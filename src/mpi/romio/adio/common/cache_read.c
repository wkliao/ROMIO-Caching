/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *

IMPORTANT:  READ BEFORE DOWNLOADING, COPYING, INSTALLING OR USING.
By downloading, copying, installing or using the software you agree
to this license.  If you do not agree to this license, do not download,
install, copy or use the software.


Copyright (c) 2006 Northwestern University
All rights reserved.

Redistribution of the software in source and binary forms,
with or without modification, is permitted provided that the
following conditions are met:

1       Redistributions of source code must retain the above copyright
        notice, this list of conditions and the following disclaimer.

2       Redistributions in binary form must reproduce the above copyright
        notice, this list of conditions and the following disclaimer in the
        documentation and/or other materials provided with the distribution.

3       Neither the name of Northwestern University nor the names of its
        contributors may be used to endorse or promote products derived
        from this software without specific prior written permission.

This software was authored by:
Northwestern University
Wei-keng Liao:  (847) 491-2916; e-mail: wkliao@ece.northwestern.edu
Department of Electrical Engineering and Computer Science
Northwestern University, Evanston, IL 60208

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS ``AS
IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY, NON-INFRINGEMENT AND
FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
NORTHWESTERN UNIVERSITY OR ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#include "adio.h"

#ifdef WKL_DEBUG
extern double readTime, lockReadTime;
extern int hits, miss, empty, nReadMigrates;
#endif

/*----< migration_local() >-------------------------------------------------*/
/* send a cache page migration request to a remote processes and reveive
   the migrated data */
int migration_local(cache_md  *md,
                    const int  len,    /* request size, assuming len < 2^31 */
                    const int  owner)
{
    int            dirtyInfo[2];
    MPI_Request    commReq;
    MPI_Status     status;
    cache_file    *fptr = md->fptr;
    cache_req_msg  remote_req;

    /* make a request to a remote process */
    remote_req.gfid      = fptr->gfid;
    remote_req.req_type  = CACHE_MIGRATE;
    remote_req.disp      = md->offset;   /* already aligned with block_size */
    remote_req.count     = len;          /* multiple of block_size */

    /* send out the request message */
    MPI_Isend(&remote_req, sizeof(cache_req_msg), MPI_BYTE, owner,
              CACHE_TAG_REQ+fptr->gfid, fptr->comm, &commReq);
    MPI_Request_free(&commReq);

    /* receive metadata isDirty and end status of the migrated blocks */
    MPI_Irecv(dirtyInfo, 2, MPI_INT, owner, CACHE_MIGRATE_INFO,
              fptr->comm, &commReq);
    MPI_Request_free(&commReq);

    /* receive the data for blocks j to i-1 of size len */
    MPI_Irecv(md->buf, len, MPI_BYTE, owner, CACHE_MIGRATE, fptr->comm, &commReq);

    /* must wait until message received, in order to copy to local buffer */
    POLLING(commReq, status, "migration_local()");

    md->isDirty = dirtyInfo[0];
    md->end     = dirtyInfo[1];

    return 1;
}


/*----< read_caching() >------------------------------------------------------*/
/* A contiguous segment of blocks (from j to i-1) has been locked and the
   caching status of these blocks has the same ownership: either never been
   cached or cached by the same remote/local process. Therefore, reading
   these blocks can be placed in one call */
static
int read_caching(cache_file        *fptr,
                 char *restrict     buf,           /* read buffer */
                 const ADIO_Offset  offset,        /* start offset of read */
                 const int          rem_len,       /* read request size,
                                                      assuming < 2^31 */
                 const int          j,
                 const int          i,
                 int               *numCommReq,
                 lock_status       *lock_st,       /* [numBlocks+1] */
                 int               *unlock_types,  /* [numBlocks] */
                 cache_req_msg     *remote_req,    /* [numBlocks] */
                 MPI_Request       *get_req)       /* [numBlocks] */
{
    int         k, err;
    cache_list *caches        = &shared_v.caches;
    int         block_size    = caches->block_size;
    int         front_extra   = offset % block_size;/* front extra of block j */
    int         ij_block_size = (i-j)*block_size;
    int         req_len       = MIN(ij_block_size - front_extra, rem_len);

    if (lock_st[j].owner == TABLE_EMPTY) {  /* no one own this -------------*/
#ifdef WKL_DEBUG
double curT = MPI_Wtime();
#endif
        /* allocate pages from cache pool and initialize their metadata */
        cache_md *md = cache_alloc(i-j); /* allocate (i-j) blocks */

#ifdef WKL_DEBUG
empty++;
#endif
        if (md == NULL) {
            /* This can be 2 cases: a) request pages too large or
               b) could not find available empty space, i.e. no
               contiguous unlocked pages large enough.
               So, have it directly read from file and don't cache
               any data */
printf("cache full ---- direct read offset=%lld len=%d\n",offset,req_len);


            if (fptr->do_block_align_io)
                /* problem if buf is not aligned with system page size,
                   buf is user-allocate buffer passed from the application
                   if O_DIRECT is enabled and buf is not aligned, erro
                   will occur ! */
                err = sys_aligned_read(fptr->file_system, fptr->fid,
                                       fptr->fsize,
                                       buf, offset, req_len);
            else
                err = sys_read_at(fptr->file_system, fptr->fid, offset, buf,
                                  req_len, fptr->do_locking_io, 0);

            /* need to reset global metadata to be empty again later */
            for (k=j; k<i; k++) unlock_types[k] = CACHE_TABLE_RESET;

            if (err == -1) return -1;
        }
        else {  /* md != NULL, md->buf has been allocated */
            md->fptr      = fptr;
            md->isDirty   = 0;   /* because this is read operation */
            md->numBlocks = i-j;
            md->offset    = offset - front_extra;
            md->reference = MPI_Wtime();
            md->end       = 0;   /* high water mark for dirty data in md->buf */

            /* read full cache pages from file to cache buffer, md->buf */
            if (fptr->do_block_align_io)
                err = sys_aligned_read(fptr->file_system, fptr->fid,
                                       fptr->fsize,
                                       md->buf, md->offset, ij_block_size);
            else
                err = sys_read_at(fptr->file_system, fptr->fid, md->offset,
                                  md->buf, ij_block_size, fptr->do_locking_io, 0);

            if (err == -1) return -1;

            /* copy from cache pages to read buffer */
            memcpy(buf, md->buf + front_extra, req_len);

            /* zero padding is not necessary any more, because md->buf
               is allocated with calloc() */

            /* add this new metadata md to the end of caches->list */
            ADD_METADATA_TO_CACHES_LIST(md);
            ADIOI_Free(md);
        }
#ifdef WKL_DEBUG
lockReadTime += MPI_Wtime() - curT;
#endif
    }  /* end of if (lock_st[j].owner == TABLE_EMPTY) */
    else {  /*---------------------------------------------------------------*/
        /* blocks j to i-1 are currently cached by the same proc (local or
           remote) */
        if (lock_st[j].owner == fptr->rank) { /* cached by self proc -------*/
#ifdef WKL_DEBUG
double curT = MPI_Wtime();
#endif
            ADIO_Offset start, end;
            int         rem_req_len = req_len;

#ifdef WKL_DEBUG
hits++;
#endif
            /* blocks j to i-1 can be located cross multiple caches->list[] */
            for (k=0; k<caches->numLinks; k++) {
                cache_md *md = caches->list + k;

                if (md->fptr != fptr) continue;

                start = MAX(offset, md->offset);
                end   = MIN(offset + req_len,
                            md->offset + md->numBlocks * block_size);
                if (start < end) {
                    /* list[k] intersects with get request, copy piece by
                       piece: requested data may be located non-contiguously
                       in cache pool list */
                    memcpy(buf + (start-offset), md->buf + (start - md->offset),
                           end - start);
                    md->reference = MPI_Wtime();
                    rem_req_len  -= end - start;
                    if (rem_req_len == 0) break; /* break loop k */
                }
            }
            if (rem_req_len != 0) {
                fprintf(stderr,"%d: Error: local GET rem_req_len(%d) != 0\n",
                        fptr->rank,rem_req_len);
            }
#ifdef WKL_DEBUG
lockReadTime += MPI_Wtime() - curT;
#endif
        }
        else { /* blocks j to i-1 are cached by remote proc -----------------*/
#ifdef WKL_DEBUG
double curT = MPI_Wtime();
#endif
            cache_md *md        = NULL;
            int       doMigrate = 0;

#ifdef WKL_DEBUG
miss++;
#endif
            if (fptr->do_page_migration) {
                /* check if any of blocks j to i-1 is accessed by self the last
                   time. If yes, perform migration for these blocks */
                for (k=j; k<i; k++) {
                    if (lock_st[k].last == fptr->rank) {
                        doMigrate = 1;
                        break;
                    }
                }
                if (doMigrate) md = cache_alloc(i-j);
            }

            if (fptr->do_page_migration && md != NULL) {
#ifdef WKL_DEBUG
{printf("read  migrating %d blocks (j=%d..i-1=%d) block %d offset=%lld, len=%d from proc %d (lock by %d)\n",i-j,j,i-1,(int)((offset-front_extra)/block_size),offset,len,owner,lock_st[j].lock);fflush(stdout);}

nReadMigrates += i-j;
#endif
                /* now, cache buffer has been allocated and will do migration
                   for blocks j to i-1 from lock_st[j].owner */
                md->fptr      = fptr;
                md->numBlocks = i-j;
                md->offset    = offset-front_extra;
                md->reference = MPI_Wtime();
                /* md->isDirty and md->end to be determined by remote proc */

                migration_local(md, ij_block_size, lock_st[j].owner);

                /* md is a newly created cache_md object due to the cache
                   pages j to i-1 migrated from remote proc lock_st[j].owner */

                /* during unlock, change the ownership of blocks j to i-1 to
                   self proc */
                for (k=j; k<i; k++) unlock_types[k] = CACHE_TABLE_UNLOCK_SWITCH;

                /* copy from local cache to read buffer */
                memcpy(buf, md->buf+front_extra, req_len);

                /* add this new metadata md to the end of caches->list */
                ADD_METADATA_TO_CACHES_LIST(md);
                ADIOI_Free(md);
            }
            else { /* migration was not performed, just proceed remote get */
                MPI_Request  commReq;

                /* make a request to a remote process */
                remote_req[*numCommReq].gfid      = fptr->gfid;
                remote_req[*numCommReq].req_type  = CACHE_TAG_GET;
                remote_req[*numCommReq].disp      = offset;
                remote_req[*numCommReq].count     = req_len;

                /* send out the request message */
                MPI_Isend(&remote_req[*numCommReq], sizeof(cache_req_msg),
                          MPI_BYTE, lock_st[j].owner, CACHE_TAG_REQ+fptr->gfid,
                          fptr->comm, &commReq);
                MPI_Request_free(&commReq);

                /* receive the GET data */
                MPI_Irecv(buf, req_len, MPI_BYTE, lock_st[j].owner,
                          CACHE_TAG_GET, fptr->comm,get_req+(*numCommReq));
                (*numCommReq)++;
            }
#ifdef WKL_DEBUG
lockReadTime += MPI_Wtime() - curT;
#endif
        }
    }
    return req_len;
}


/*----< cache_file_read() >---------------------------------------------------*/
int cache_file_read(void)
{
    cache_file    *fptr;
    int            i, j;
    int            start_block_id, numBlocks, numCommReq;
    int            len=0, rem_len, req_len, block_size;
    char          *buf;
    ADIO_Offset    offset, original_offset;
    lock_status   *lock_st;
    int           *restrict unlock_types; /* operations for remote proc
                                             during unlock */
    MPI_Status    *restrict get_st;
    MPI_Request   *restrict get_req;
    cache_req_msg *restrict remote_req;

printf("spawn I/O thread calls %s to read file\n",__func__);

    fptr            = shared_v.fptr;
    req_len         = shared_v.len;
    buf             = shared_v.buf;
    original_offset = shared_v.offset;
    block_size      = shared_v.caches.block_size;

    /* get file size, fptr->fsize, from proc 0. This prevents reading beyond
       EOF */
    cache_fsize(CACHE_GET_FSIZE, fptr, original_offset + req_len);

    /* update read request size, make sure not read beyond EOF */
    req_len = MIN(fptr->fsize - original_offset, req_len);

    if (req_len <= 0) { /* read beyond EOF */
        /* signal main thread that read operation has completed -------------*/
        MUTEX_LOCK(shared_v.mutex);
        shared_v.req = CACHE_REQ_NULL;
        shared_v.len = 0; /* bytes actually read */
        COND_SIGNAL(shared_v.cond);
        MUTEX_UNLOCK(shared_v.mutex);
        return 1;
    }
    /* now, read request range is not beyond the end of file (EOF) */

    start_block_id  = original_offset/block_size;
    numBlocks       = (original_offset+req_len-1)/block_size -
                      start_block_id + 1;
    lock_st = (lock_status*) ADIOI_Malloc((numBlocks+1)*sizeof(lock_status));
    lock_st[numBlocks].owner = TABLE_END;  /* end search condition */
    unlock_types = (int*) ADIOI_Malloc(numBlocks*sizeof(int));
    for (i=0; i<numBlocks; i++) unlock_types[i] = CACHE_TABLE_UNLOCK;

    get_st     = (MPI_Status*)    ADIOI_Malloc(numBlocks * sizeof(MPI_Status));
    get_req    = (MPI_Request*)   ADIOI_Malloc(numBlocks * sizeof(MPI_Request));
    remote_req = (cache_req_msg*) ADIOI_Malloc(numBlocks * sizeof(cache_req_msg));
    numCommReq = 0;
    offset     = original_offset;
    rem_len    = req_len;

    if (fptr->do_pipeline_io) {
        /* issue lock requests to all file blocks to be read, once a
           contiguous set of blocks are locked, read is performed */
        j = 0;
        for (i=0; i<=numBlocks; i++) {
            if (i < numBlocks) { /* only lock blocks 0,1,...,numBlocks-1 */
                metadata_lock(fptr, (start_block_id + i) % fptr->np,
                                    (start_block_id + i) / fptr->np,
                              CACHE_READ_LOCK, lock_st+i);
            }

            /* read operation: ----------------------------------------------*/
            if (i > 0) { /* only runs for i = 1, 2, ..., numBlocks */
                /* caching is performed only when locks from file blocks j to
                   i-1 are granted and belong to the same owner */
                if (lock_st[j].owner != lock_st[i].owner) {
                    len = read_caching(fptr,
                                       buf,
                                       offset,
                                       rem_len,
                                       j,
                                       i,
                                       &numCommReq,
                                       lock_st,
                                       unlock_types,
                                       remote_req,
                                       get_req);
                    if (len == -1) break;  /* error during read() */

                    /* advance to new offset, remaining read length for next
                       segment */
                    j        = i;
                    buf     += len;
                    offset  += len;
                    rem_len -= len;
                }
            }
        }
    }
    else { /* issue lock requests to all file blocks to be read and wait for
              all to be granted (reads only start when all blocks are locked */
#ifdef WKL_DEBUG
double curT = MPI_Wtime();
#endif
        for (i=0; i<numBlocks; i++) {
            metadata_lock(fptr, (start_block_id + i) % fptr->np,
                                (start_block_id + i) / fptr->np,
                          CACHE_READ_LOCK, lock_st+i);
        }
#ifdef WKL_DEBUG
lockReadTime += MPI_Wtime() - curT;
#endif
        /* at this point, all locks to file blocks are granted */

        /* read operation: --------------------------------------------------*/
        j = 0;
        for (i=1; i<numBlocks+1; i++) {
            if (lock_st[j].owner != lock_st[i].owner) {
                /* blocks j, ... i-1 are cached by the same proc */
                len = read_caching(fptr,
                                   buf,
                                   offset,
                                   rem_len,
                                   j,
                                   i,
                                   &numCommReq,
                                   lock_st,
                                   unlock_types,
                                   remote_req,
                                   get_req);
                if (len == -1) break;  /* error during read() */

                /* advance to new offset, remaining read length for next
                   segment */
                j        = i;
                buf     += len;
                offset  += len;
                rem_len -= len;
            }
        }  /* end of loop i */
    }

#ifdef WKL_DEBUG
{ double curT = MPI_Wtime();
#endif
    if (numCommReq > 0)
        WAITALL(numCommReq, get_req, get_st, "cache_read wait for all gets");
#ifdef WKL_DEBUG
lockReadTime += MPI_Wtime() - curT; }
#endif

#ifdef WKL_DEBUG
{printf("(t) READ is complete, to unlock all read pages, numCommReq=%d numBlocks=%d\n",numCommReq,numBlocks);fflush(stdout);}
#endif

    ADIOI_Free(remote_req);
    ADIOI_Free(get_req);
    ADIOI_Free(get_st);

    /* read is complete, unlock the global caching status table at once -----*/
#ifdef WKL_DEBUG
if(unlock_types[0]==CACHE_TABLE_UNLOCK_SWITCH) {printf("unlock_types[%d]=",numBlocks);for(i=0;i<numBlocks;i++)printf(" %d",unlock_types[i]);printf("\n");fflush(stdout);}
#endif

#ifdef WKL_DEBUG
{ double curT = MPI_Wtime();
#endif
    metadata_unlock(fptr, numBlocks, start_block_id, unlock_types,
                    "cache_file_read");
#ifdef WKL_DEBUG
lockReadTime += MPI_Wtime() - curT; }
#endif

    /* move the file pointer shared_v.len forward ---------------------------*/
    sys_lseek(fptr->file_system, fptr->fid, original_offset+req_len, SEEK_SET);

    ADIOI_Free(lock_st);
    ADIOI_Free(unlock_types);

#ifdef WKL_DEBUG
{printf("(t) READ ------------------------- done\n");fflush(stdout);}
#endif

    /* signal main thread that read operation has completed -----------------*/
    MUTEX_LOCK(shared_v.mutex);
    shared_v.req = CACHE_REQ_NULL;
    shared_v.len = req_len;  /* bytes actually read */
    if (len == -1) shared_v.len = -1;  /* error during read() */
    COND_SIGNAL(shared_v.cond);
    MUTEX_UNLOCK(shared_v.mutex);

    return 1;
}


/*----< cache_read() >------------------------------------------------------*/
int cache_read(cache_file        *fptr,
               char              *buf,  /* I/O buffer */
               const ADIO_Offset  offset, /* current file offset */
               const int          len)  /* read length in byte */
{
    int readBytes;
#ifdef WKL_DEBUG
double curT = MPI_Wtime();
#endif

    /* if len >= 0.5 * cache size, no caching is performed */
    /* read from file system directly and return */

    MUTEX_LOCK(shared_v.mutex);

    shared_v.req    = CACHE_FILE_READ;
    shared_v.fptr   = fptr;
    shared_v.buf    = buf;
    shared_v.len    = len;
    shared_v.offset = offset;

    /* wait for 2nd thread to complete the read job */
    COND_WAIT(shared_v.cond, shared_v.mutex);

    readBytes    = shared_v.len;
    shared_v.req = CACHE_REQ_NULL;
    MUTEX_UNLOCK(shared_v.mutex);

#ifdef WKL_DEBUG
readTime += MPI_Wtime() - curT;
#endif
    return readBytes;
}

