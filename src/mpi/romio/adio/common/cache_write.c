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
extern double writeTime, lockWriteTime;
extern int hits, miss, empty, nWriteMigrates;
#endif

/*----< write_caching() >-----------------------------------------------------*/
/* A contiguous segment of blocks (from j to i-1) has been locked and the
   caching status of these blocks has the same ownership: either never been
   cached or cached by the same remote/local process. Therefore, writing
   these blocks can be placed in one call */
static
int write_caching(cache_file        *fptr,
                  char              *buf,          /* write buffer */
                  const ADIO_Offset  offset,       /* start offset of write */
                  const int          rem_len,      /* write request size,
                                                      assuming < 2^31 */
                  const int          j,
                  const int          i,
                  int               *numCommReq,
                  lock_status       *lock_st,      /* [numBlocks+1] */
                  int               *unlock_types, /* [numBlocks] */
                  cache_req_msg     *remote_req,   /* [numBlocks] */
                  MPI_Request       *put_req)      /* [numBlocks] */
{
    int         k, err;
    cache_list *caches        = &shared_v.caches;
    int         block_size    = caches->block_size;
    int         front_extra   = offset % block_size;/* front extra of block j */
    int         ij_block_size = (i-j)*block_size;
    int         req_len       = MIN(ij_block_size - front_extra, rem_len);
                                /* rem_len may go beyond (i-j) blocks */

#ifdef WKL_DEBUG
if (MAX(offset,40960) < MIN(49152,offset+req_len))printf("WRITE [45124]=%c req_len=%d\n",buf[45124-offset],req_len);
#endif
    if (lock_st[j].owner == TABLE_EMPTY) {  /* no one own this -------------*/
#ifdef WKL_DEBUG
double curT = MPI_Wtime();
#endif
        /* allocate pages from caches and initialize their metadata */
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
#ifdef WKL_DEBUG
printf("cache full ---- direct write offset=%lld len=%d\n",offset,req_len);

if (MAX(offset,507904) < MIN(516096,offset+req_len)) printf("WRITE [514600]=%c cache full ---- direct write offset=%lld len=%d fsize=%lld\n",buf[514600-offset],offset,req_len,fptr->fsize);
#endif

            if (fptr->do_block_align_io)
                /* problem if buf is not aligned with system page size,
                   buf is user-allocate buffer passed from the application
                   if O_DIRECT is enabled and buf is not aligned, erro
                   will occur ! */
                err = sys_aligned_write(fptr->file_system, fptr->fid,
                                        fptr->fsize, buf, offset, req_len);
            else
                err = sys_write_at(fptr->file_system, fptr->fid, offset, buf,
                                   req_len, fptr->do_locking_io, 0);
            /* need to reset global metadata to be empty again later */
            for (k=j; k<i; k++) unlock_types[k] = CACHE_TABLE_RESET;

#ifdef WKL_DEBUG
#include <errno.h>
extern int errno;
if (err == -1) fprintf(stderr,"Error: write offset=%lld len=%d errno=%d EINVAL=%d\n",offset,req_len,errno,EINVAL);
#endif
            if (err == -1) return -1;
        }
        else {  /* md != NULL, md->buf has been allocated */
            md->fptr      = fptr;
            md->isDirty   = 1;
            md->numBlocks = i-j;
            md->offset    = offset - front_extra;
            md->reference = MPI_Wtime();
            md->end       = front_extra + req_len;
            /* set end position high water mark for dirty data in md->buf */

#ifdef WKL_DEBUG
if (MAX(offset,40960) < MIN(49152,offset+req_len)) printf("PREPARE CACHING [45124] offset=%lld len=%d md->offset=%lld fsize=%lld, front_extra=%d\n",offset,req_len,md->offset,fptr->fsize,front_extra);
#endif
            /* if the offset is not aligned with the start of the first block,
               need to read the front extra bytes from file to cache -------*/
            if (md->offset < fptr->fsize && front_extra > 0) {
                if (fptr->do_block_align_io) {
                    err = sys_aligned_read(fptr->file_system, fptr->fid,
                                           fptr->fsize,
                                           md->buf, md->offset, front_extra);
#ifdef WKL_DEBUG
if (MAX(offset,40960) < MIN(49152,offset+req_len)) printf("READ CACHING [45124] offset=%lld len=%d\n",md->offset,block_size);
#endif
                }
                else {
                    err = sys_read_at(fptr->file_system, fptr->fid,
                                      md->offset, md->buf,
                                      front_extra, fptr->do_locking_io, 0);
#ifdef WKL_DEBUG
if (MAX(offset,40960) < MIN(49152,offset+req_len)) printf("READ HEAD CACHING [45124] offset=%lld len=%d\n",md->offset,front_extra);
#endif
                }
                if (err == -1) return -1;
            }

            /* if the last byte to be accessed is not aligned with the end
               of the last block, need to read the end extra bytes from
               file to cache --------------------------------------------*/
            if ((offset + req_len) % block_size > 0) {
                ADIO_Offset  end_offset = offset + req_len;
                int          end_rem;   /* end remain of the last block */
                int          end_extra; /* end extra  of the last block */

                end_rem   = end_offset % block_size;
                end_extra = block_size - end_rem;  /* end_rem must > 0 */

                if (fptr->do_block_align_io) {
#ifdef WKL_DEBUG
if (MAX(md->offset,40960) < MIN(49152,md->offset+(i-j)*block_size)) printf("READ TAIL CACHING page [45124]=%c md(offset=%lld, numBlocks=%d) front_extra=%d i-j=%d fsize=%lld, end_offset-end_rem=%lld\n",md->buf[45124-md->offset],md->offset,md->numBlocks,front_extra,i-j,fptr->fsize,end_offset-end_rem);
#endif
                    if (end_offset-end_rem < fptr->fsize &&
                        ((i-j == 1 && front_extra == 0) || (i-j  > 1))) {
                        /* first block is not read due to front_extra == 0 or
                           alread read if (i-j)==1 and front_extra > 0 */
                        err = sys_aligned_read(fptr->file_system, fptr->fid,
                                               fptr->fsize,
                                               md->buf + (ij_block_size - block_size),
                                               end_offset - end_rem, block_size - end_rem);
#ifdef WKL_DEBUG
if (md->offset<=45124 && 45125 <= md->offset+(i-j)*block_size)printf("READ TAIL after CACHING page [45124]=%c md(offset=%lld, numBlocks=%d) front_extra=%d read offset=%lld len=%d ij_block_size=%d readBytes=%d\n",md->buf[45124-md->offset],md->offset,md->numBlocks,front_extra,end_offset - end_rem,block_size,ij_block_size,err);
#endif
                    }
                }
                else if (end_offset < fptr->fsize)
                    err = sys_read_at(fptr->file_system, fptr->fid, end_offset,
                                      md->buf + (ij_block_size - end_extra),
                                      end_extra, fptr->do_locking_io, 0);
                if (err == -1) return -1;
            }

            /* copy write buffer to cache pages only */
            memcpy(md->buf + front_extra, buf, req_len);

            /* zero padding is not necessary any more, because md->buf
               is allocated with calloc() */

            /* add this new metadata md to the end of caches->list */
            ADD_METADATA_TO_CACHES_LIST(md);
            ADIOI_Free(md);
        }
#ifdef WKL_DEBUG
lockWriteTime += MPI_Wtime() - curT;
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

if (MAX(offset,40960) < MIN(49152,offset+req_len)) printf("PREPARE SELF CACHING [45124] offset=%lld len=%d\n",offset,req_len);
#endif
            /* blocks j to i-1 can be located cross multiple caches->list[] */
            for (k=0; k<caches->numLinks; k++) {
                cache_md *md = caches->list + k;

                if (md->fptr != fptr) continue;

                start = MAX(offset, md->offset);
                end   = MIN(offset + req_len,
                            md->offset + md->numBlocks * block_size);
                if (start < end) {
                    /* list[k] intersects with put request, copy piece by
                       piece: requested data may be located non-contiguously
                       in cache pool list */
                    memcpy(md->buf + (start - md->offset), buf + (start-offset),
                           end - start);
                    md->reference = MPI_Wtime();
                    rem_req_len  -= end - start;
                    md->end       = MAX(md->end, end - md->offset);
                    md->isDirty   = 1;
                    if (rem_req_len == 0) break; /* break loop k */
                }
            }
            if (rem_req_len != 0) {
                fprintf(stderr,"%d: Error: local PUT rem_req_len(%d) != 0 req disp=%lld (block %lld) count=%d caches->numLinks=%d k=%d\n",fptr->rank,rem_req_len,offset,offset/block_size,req_len,caches->numLinks,k);
                printf("Error: caches->list: ");
                for (k=0;k<caches->numLinks+1;k++)
                    printf("[%d](%lld,%lld) ",k,caches->list[k].offset,caches->list[k].offset+caches->list[k].numBlocks * block_size);
                printf("\n");
                printf("Error: md table: ");
                for (k=0;k<fptr->fsize/block_size/fptr->np + 1;k++)
                    printf("[%d](%d,%d) ",k,fptr->table[k].owner,fptr->table[k].lock);
                printf("\n");fflush(stdout);
                MPI_Abort(MPI_COMM_WORLD, 1);
            }
#ifdef WKL_DEBUG
lockWriteTime += MPI_Wtime() - curT;
#endif
        }
        else { /* cached by remote proc -------------------------------------*/
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
{printf("write migrating %d blocks (j=%d..i-1=%d) block %d offset=%lld, req_len=%d from proc %d (lock by %d)\n",i-j,j,i-1,(int)((offset-front_extra)/block_size),offset,req_len,lock_st[j].owner,lock_st[j].lock);fflush(stdout);}

nWriteMigrates += i-j;
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

                /* copy from write buffer to local cache */
                memcpy(md->buf+front_extra, buf, req_len);

                md->isDirty = 1;
                md->end     = MAX(md->end, front_extra + req_len);

                /* add this new metadata md to the end of caches->list */
                ADD_METADATA_TO_CACHES_LIST(md);
                ADIOI_Free(md);
            }
            else { /* migration was not performed, just proceed remote put */
                MPI_Request  commReq;

#ifdef WKL_DEBUG
if (MAX(offset,40960) < MIN(49152,offset+req_len)) printf("PREPARE REMOTE PUT to P%d [45124]=%c offset=%lld len=%d\n",lock_st[j].owner,buf[45124-offset],offset,req_len);
#endif
                /* make a request to a remote process */
                remote_req[*numCommReq].gfid      = fptr->gfid;
                remote_req[*numCommReq].req_type  = CACHE_TAG_PUT;
                remote_req[*numCommReq].disp      = offset;
                remote_req[*numCommReq].count     = req_len;

                /* send out the request message */
                MPI_Isend(&remote_req[*numCommReq], sizeof(cache_req_msg),
                          MPI_BYTE, lock_st[j].owner, CACHE_TAG_REQ+fptr->gfid,
                          fptr->comm, &commReq);
                MPI_Request_free(&commReq);

                /* send out the data, use Issend to ensure the compleletion
                   and safe to unlock the block. MPICH can implement MPI_Isend
                   by copying data to a system buffer and return at the calling
                   of MPI_Wait(), but remote PUT is actually notyet complete.
                   To prevent such situation, use MPI_Issend() instead */
                MPI_Issend(buf, req_len, MPI_BYTE, lock_st[j].owner,
                           CACHE_TAG_PUT, fptr->comm, put_req+(*numCommReq));
                (*numCommReq)++;
            }
#ifdef WKL_DEBUG
lockWriteTime += MPI_Wtime() - curT;
#endif
        }
    }
#ifdef WKL_DEBUG
if (MAX(offset,40960) < MIN(49152,offset+req_len)) printf("RETURN WRITE [45124]=%c req_len=%d\n",buf[45124-offset],req_len);
#endif
    return req_len;
}

/*----< cache_file_write() >--------------------------------------------------*/
int cache_file_write(void)
{
    cache_file    *fptr;
    int            i, j, err;
    int            start_block_id, numBlocks, numCommReq;
    int            len=0, rem_len, req_len, block_size;
    char          *buf;
    ADIO_Offset    offset, original_offset=0;
    lock_status   *lock_st;
    int           *restrict unlock_types; /* operations for remote proc
                                             during unlock */
    MPI_Status    *restrict put_st;
    MPI_Request   *restrict put_req;
    cache_req_msg *restrict remote_req;

printf("spawn I/O thread calls %s to write file\n",__func__);

    fptr            = shared_v.fptr;
    req_len         = shared_v.len;
    buf             = shared_v.buf;
    original_offset = shared_v.offset;
    block_size      = shared_v.caches.block_size;

    /* if write goes beyond the current file size, get the up-to-date
       file size, fptr->fsize, which is used to determnine if read() in
       the read-modify-write is necessary ------------------------------*/
    if (fptr->fsize < original_offset+req_len)
        cache_fsize(CACHE_GET_FSIZE, fptr, fptr->fsize + 1);

    start_block_id  = original_offset/block_size;
    numBlocks       = (original_offset+req_len-1)/block_size -
                      start_block_id + 1;
    lock_st = (lock_status*)ADIOI_Malloc((numBlocks+1)*sizeof(lock_status));
    lock_st[numBlocks].owner = TABLE_END;  /* end search condition */
    unlock_types = (int*) ADIOI_Malloc(numBlocks*sizeof(int));
    for (i=0; i<numBlocks; i++) unlock_types[i] = CACHE_TABLE_UNLOCK;

    put_st     = (MPI_Status*)    ADIOI_Malloc(numBlocks * sizeof(MPI_Status));
    put_req    = (MPI_Request*)   ADIOI_Malloc(numBlocks * sizeof(MPI_Request));
    remote_req = (cache_req_msg*) ADIOI_Malloc(numBlocks * sizeof(cache_req_msg));
    numCommReq = 0;
    offset     = original_offset;
    rem_len    = req_len;

    if (fptr->do_pipeline_io) {
        /* issue lock requests to all file blocks to be read and wait for all
           to be granted ----------------------------------------------------*/
        j = 0;
        for (i=0; i<=numBlocks; i++) {
            if (i < numBlocks) { /* only lock blocks 0,1,...,numBlocks-1 */
                err = metadata_lock(fptr, (start_block_id + i) % fptr->np,
                                          (start_block_id + i) / fptr->np,
                                    CACHE_WRITE_LOCK, lock_st+i);
/*
if(err==-1)printf("metadata_lock: original_offset=%lld req_len=%d start_block_id=%d numBlocks=%d i=%d\n",original_offset,req_len,start_block_id,numBlocks,i);
*/
            }

            /* read operation: ----------------------------------------------*/
            if (i > 0) { /* only runs for i = 1, 2, ..., numBlocks */
                /* caching is performed only when locks from file blocks j to
                   i-1 are granted and belong to the same owner */
                if (lock_st[j].owner != lock_st[i].owner) {
                    len = write_caching(fptr,
                                        buf,
                                        offset,
                                        rem_len,
                                        j,
                                        i,
                                        &numCommReq,
                                        lock_st,
                                        unlock_types,
                                        remote_req,
                                        put_req);
                    if (len == -1) break;  /* error during write() */

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
    else { /* issue lock requests to all file blocks to be wriiten and wait for
              all to be granted (writes only start when all blocks are locked */
#ifdef WKL_DEBUG
double curT = MPI_Wtime();
#endif
        for (i=0; i<numBlocks; i++) {
            err = metadata_lock(fptr, (start_block_id + i) % fptr->np,
                                      (start_block_id + i) / fptr->np,
                                CACHE_WRITE_LOCK, lock_st+i);
        }
#ifdef WKL_DEBUG
lockWriteTime += MPI_Wtime() - curT;
#endif
        /* at this point, all locks to file blocks are granted */

        /* read operation: --------------------------------------------------*/
        j = 0;
        for (i=1; i<numBlocks+1; i++) {
            if (lock_st[j].owner != lock_st[i].owner) {
                /* blocks j, ... i-1 are cached by the same proc */
                len = write_caching(fptr,
                                    buf,
                                    offset,
                                    rem_len,
                                    j,
                                    i,
                                    &numCommReq,
                                    lock_st,
                                    unlock_types,
                                    remote_req,
                                    put_req);
                if (len == -1) break;  /* error during write() */

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
        WAITALL(numCommReq, put_req, put_st, "cache_write wait for all puts");
#ifdef WKL_DEBUG
lockWriteTime += MPI_Wtime() - curT; }
#endif

#ifdef WKL_DEBUG
{printf("(t) WRITE is complete, now to unlock all written pages, numCommReq=%d numBlocks=%d\n",numCommReq,numBlocks);fflush(stdout);}
#endif

    ADIOI_Free(remote_req);
    ADIOI_Free(put_req);
    ADIOI_Free(put_st);

    /* write is complete, unlock the global caching status table at once ----*/
#ifdef WKL_DEBUG
if(unlock_types[0]==CACHE_TABLE_UNLOCK_SWITCH) {printf("unlock_types[%d]=",numBlocks);for(i=0;i<numBlocks;i++)printf(" %d",unlock_types[i]);printf("\n");fflush(stdout);}
#endif

    if (len >= 0) {  /* no error during write */
        /* move the file pointer shared_v.len forward -----------------------*/
        sys_lseek(fptr->file_system, fptr->fid, original_offset+req_len,
                  SEEK_SET);

        /* update file size, fptr->fsize, with proc 0 -----------------------*/
        cache_fsize(CACHE_SET_FSIZE, fptr, original_offset + req_len);
    }
#ifdef WKL_DEBUG
if (original_offset==45124) printf("set fsize to %lld\n", original_offset + req_len);
#endif

#ifdef WKL_DEBUG
{ double curT = MPI_Wtime();
#endif
    metadata_unlock(fptr, numBlocks, start_block_id, unlock_types,
                    "cache_file_write");
#ifdef WKL_DEBUG
lockWriteTime += MPI_Wtime() - curT; }
#endif

    ADIOI_Free(lock_st);
    ADIOI_Free(unlock_types);

#ifdef WKL_DEBUG
{printf("(t) WRITE ------------------------ done\n");fflush(stdout);}
#endif

    /* signal main thread that write operation has completed ----------------*/
    MUTEX_LOCK(shared_v.mutex);
    shared_v.req = CACHE_REQ_NULL;
    shared_v.len = req_len;  /* bytes actually written */
    if (len == -1) shared_v.len = -1;  /* error during write() */
    COND_SIGNAL(shared_v.cond);
    MUTEX_UNLOCK(shared_v.mutex);

    return 1;
}


/*----< cache_write() >------------------------------------------------------*/
int cache_write(cache_file        *fptr,
                char              *buf,
                const ADIO_Offset  offset,
                const int          len)  /* in byte */
{
    int writeBytes;
#ifdef WKL_DEBUG
double curT = MPI_Wtime();
#endif
    /* if len >= 0.5 * cache size, no caching is performed */
    /* write to file system directly and return */
/*
printf("cache_write offset=%12lld   len=%9d\n",offset,len);
*/

    MUTEX_LOCK(shared_v.mutex);

    shared_v.req    = CACHE_FILE_WRITE;
    shared_v.fptr   = fptr;
    shared_v.buf    = buf;
    shared_v.len    = len;
    shared_v.offset = offset;

    /* wait for 2nd thread to complete the write job */
    COND_WAIT(shared_v.cond, shared_v.mutex);

    writeBytes   = shared_v.len;
    shared_v.req = CACHE_REQ_NULL;
    MUTEX_UNLOCK(shared_v.mutex);

#ifdef WKL_DEBUG
writeTime += MPI_Wtime() - curT;
#endif
    return writeBytes;
}

