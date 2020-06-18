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


/* cache_alloc() return space with all meta data clean, the update of
   meta data should be done outside cache_alloc()
*/

struct _evict_lock {
    int                 dest;      /* dest proc rank */
    cache_file         *fptr;
    ADIO_Offset         disp;      /* displacement (offset) of the block */
    struct _evict_lock *next;
};
typedef struct _evict_lock _elock;


static int double_compare(const void *p1, const void *p2)
{
    double i = ((cache_md *)p1)->reference;
    double j = ((cache_md *)p2)->reference;

    /* descreasing sorting */
    if (i > j) return (-1);
    if (i < j) return (1);
    return (0);
}


#if 0
/*----< metadata_unlock_list() >--------------------------------------------*/
static
int metadata_unlock_list(const int   numBlocks,
                         _elock     *head,
                         const int   unlock_type,  /* CACHE_TABLE_UNLOCK or
                                                      CACHE_TABLE_RESET */
                         const char *msg)
{
    int            numCommReq;
    MPI_Status    *restrict status;
    MPI_Request   *restrict comm_req;
    cache_req_msg *restrict remote_req;

    status     = (MPI_Status*)    ADIOI_Malloc(numBlocks * sizeof(MPI_Status));
    comm_req   = (MPI_Request*)   ADIOI_Malloc(numBlocks * sizeof(MPI_Request));
    remote_req = (cache_req_msg*) ADIOI_Malloc(numBlocks * sizeof(cache_req_msg));
    numCommReq = 0;

    while (head != NULL) {  /* go through the linked list to unlock */
        _elock *toFree;

        if (head->dest != head->fptr->rank) { /* metadata is remote */
            remote_req[numCommReq].gfid      = head->fptr->gfid;
            remote_req[numCommReq].disp      = head->disp;
            remote_req[numCommReq].count     = 1;
            remote_req[numCommReq].req_type  = unlock_type;

            MPI_Issend(&remote_req[numCommReq], sizeof(cache_req_msg), MPI_BYTE,
                       head->dest, CACHE_TAG_REQ+head->fptr->gfid,
                       head->fptr->comm, &comm_req[numCommReq]);
            numCommReq++;
        }
        else {  /* global caching metadata is in self proc */
            update_lock_queue(head->fptr, head->disp, unlock_type,
                              head->fptr->rank);
        }

        toFree = head;
        head   = head->next;
        ADIOI_Free(toFree);
    }

    /* waiting for the unlock requests (async send) to complete */
    if (numCommReq > 0)
        WAITALL(numCommReq, comm_req, status, msg);

    ADIOI_Free(remote_req);
    ADIOI_Free(comm_req);
    ADIOI_Free(status);

    return 1;
}
#endif

/*----< metadata_unlock_list() >--------------------------------------------*/
static
int metadata_unlock_list(const int   numBlocks,
                         _elock     *head,
                         const int   unlock_type,  /* CACHE_TABLE_UNLOCK or
                                                      CACHE_TABLE_RESET */
                         const char *msg)
{
    int *dest_v, *indx_v, *numUnlocks;

    dest_v = (int*) ADIOI_Malloc(numBlocks * sizeof(int));
    indx_v = (int*) ADIOI_Malloc(numBlocks * sizeof(int));

    while (head != NULL) {  /* go through the linked list to unlock */
        int         i, total_numUnlocks, is_head_changed;
        _elock     *p, *q;
        cache_file *fptr = head->fptr;

        numUnlocks = (int*) ADIOI_Calloc(fptr->np, sizeof(int));
        total_numUnlocks = 0;
        is_head_changed  = 0;

        /* send out unlock requests once per fptr, the while loop below
           search all linked list with the same fptr and combine the
           unlock requests to the same dest into a single message */
        p = q = head;
        while (q != NULL) { /* go through the entire linked list */
            if (q->fptr != fptr) {
                if (is_head_changed == 0) {
                    is_head_changed = 1;
                    head = q;
                }
                p = q;
                q = q->next;
            }
            else {
                if (q->dest == fptr->rank) {
                    /* global caching metadata is in self proc */
                    update_lock_queue(fptr, q->disp, unlock_type, fptr->rank);
                }
                else { /* prepare outgoing message */
                    dest_v[total_numUnlocks] = q->dest;
                    indx_v[total_numUnlocks] = q->disp;
                    numUnlocks[q->dest]++;
                    total_numUnlocks++;
                }
                if (p == q) { /* p is the previous link of q */
                    p = q->next;
                    ADIOI_Free(q);
                    q = p;
                }
                else {
                    p->next = q->next;
                    ADIOI_Free(q);
                    q = p->next;
                }
            }
        }
        if (total_numUnlocks > 0) { /* this block is the same as the one in
            metadata_unlock() in cache_metadata.c: sending unlock requests to
            remote proc */
            int             numCommReq;
            MPI_Status     *restrict status;
            MPI_Request    *restrict comm_req;
            cache_req_msg  *restrict remote_req;
            unlock_msg    **unlock_v;

            status   = (MPI_Status*)   ADIOI_Malloc(fptr->np*sizeof(MPI_Status));
            comm_req = (MPI_Request*)  ADIOI_Malloc(fptr->np*sizeof(MPI_Request));
            remote_req=(cache_req_msg*)ADIOI_Malloc(fptr->np*sizeof(cache_req_msg));
            unlock_v = (unlock_msg**)ADIOI_Malloc(fptr->np*sizeof(unlock_msg*));
            numCommReq = 0;
            for (i=0; i<fptr->np; i++) {
                if (numUnlocks[i] > 0) {
                    remote_req[i].gfid     = fptr->gfid;
                    remote_req[i].count    = numUnlocks[i];
                    remote_req[i].req_type = CACHE_TABLE_UNLOCK;

                    if (numUnlocks[i] == 1) { /* if only one unlock request */
                        int j;                /* send one message only */
                        for (j=0; j<total_numUnlocks; j++)
                            if (dest_v[j] == i) {
                                remote_req[i].disp     = indx_v[j];
                                remote_req[i].req_type = unlock_type;
                                break;
                            }
                    }
                    /* using Issend to ensure it's completion after WAITALL */
                    MPI_Issend(&remote_req[i], sizeof(cache_req_msg), MPI_BYTE,
                               i, CACHE_TAG_REQ+fptr->gfid, fptr->comm,
                               &comm_req[numCommReq]);

                    if (numUnlocks[i] == 1) {
                        unlock_v[i] = NULL;
                        numCommReq++;
                    }
                    else { /* only 2 or more unlock requests need an array */
                        unlock_v[i] = (unlock_msg*) ADIOI_Malloc(numUnlocks[i] *
                                                           sizeof(unlock_msg));
                        MPI_Request_free(&comm_req[numCommReq]);
                    }
                }
                numUnlocks[i] = 0;
            }
            /* packing the unlock requests into single message for each dest */
            for (i=0; i<total_numUnlocks; i++) {
                int dest = dest_v[i];  /* dest != fptr->rank */
                if (unlock_v[dest] == NULL) continue;
                unlock_v[dest][numUnlocks[dest]].indx       = indx_v[i];
                unlock_v[dest][numUnlocks[dest]].unlockType = unlock_type;
                numUnlocks[dest]++;
            }
            for (i=0; i<fptr->np; i++) {
                /* only 2 or more unlock requests need to send an array */
                if (numUnlocks[i] > 1) {
                    MPI_Issend(unlock_v[i], numUnlocks[i]*sizeof(unlock_msg),
                               MPI_BYTE, i, CACHE_TABLE_UNLOCK_V, fptr->comm,
                               &comm_req[numCommReq]);
                    numCommReq++;
                }
            }
            /* waiting for the unlock requests (async send) to complete */
            if (numCommReq > 0)
                WAITALL(numCommReq, comm_req, status, msg);

            for (i=0; i<fptr->np; i++)
                if (numUnlocks[i] > 1) ADIOI_Free(unlock_v[i]);
            ADIOI_Free(unlock_v);
            ADIOI_Free(remote_req);
            ADIOI_Free(comm_req);
            ADIOI_Free(status);
        }
        ADIOI_Free(numUnlocks);
        if (is_head_changed == 0) head = NULL;
    }
    ADIOI_Free(dest_v);
    ADIOI_Free(indx_v);

    return 1;
}

/*----< cache_alloc() >-------------------------------------------------------*/
cache_md* cache_alloc(const int num) /* number of cache pages */
{
    int       i, j, k;
    int       numEvictPages, numNeedPages;
    int       doEvict, evictStart;
    int       num_elocks = 0;
    cache_md *md;                 /* to be return */
    _elock   *elock_list = NULL;  /* linked list: all granted locks to evict */
    _elock   *elock_tail = NULL;  /* tail of the linked list */

    cache_list *caches     = &shared_v.caches;
    int         block_size = caches->block_size;

    if (num * block_size > caches->upperLimit) {/* requested size too large, don't cache */
#ifdef WKL_DEBUG
printf("(t) cache_alloc request %d pages upperLimit=%d\n", num,caches->upperLimit);
#endif
        return NULL;
    }

    if ((num + caches->numBlocksAlloc) * block_size <= caches->upperLimit) {
#ifdef WKL_DEBUG
printf("(t) cache_alloc request %d pages allowed (already allocated = %d pages)\n",num,caches->numBlocksAlloc);
#endif

        /* still have enough empty space -----------------------------------*/
        /* simply malloc a space of size num pages */
        /* using calloc to ensure the zeros beyond md->end */
        md = (cache_md*) ADIOI_Malloc(sizeof(cache_md));
        md->buf = (char*) ADIOI_Calloc(num * block_size, 1);

        if (md->buf != NULL) {
            return md;
        }
        else if (num > caches->numBlocksAlloc) {
            /* Since calloc() returns NULL, heap memory is full. Also,
               requesting size is larger than already allocated size, there
               is no memory space enough to cache this request, even by
               evicting all existing cached pages */
            ADIOI_Free(md);
            return NULL;
        }
        /* now, we need to evict some pages to accommodate this request */
    }
    /* now, there is no enough empty space left, need evict some pages */
    doEvict  = 1;

    /* sort caches->list based on the least recent used time stamps ----------*/
    qsort(caches->list, caches->numLinks, sizeof(cache_md), double_compare);

    /* calculate the number of pages is needed */
    numEvictPages = 0;
    numNeedPages  = num - (caches->upperLimit / block_size - caches->numBlocksAlloc);

    for (i=caches->numLinks-1; i>=0; i--) {
        numEvictPages += caches->list[i].numBlocks;
        numNeedPages  -= caches->list[i].numBlocks;
        if (numNeedPages <= 0) break;
    }
    evictStart = i;
    /* need to evict list[i] ... list[numLinks-1] */

#ifdef WKL_DEBUG
printf("(t) evicting cache evictStart=%d [%lld, %d] (numLinks=%d)\n",evictStart,caches->list[evictStart].offset,caches->list[evictStart].numBlocks,caches->numLinks);
#endif

    /* try_lock global cache metadata for those blocks to be evicted -------*/
    /* try_lock does NOT require in an increasing order, like two-phase
       locking, since if one try_lock fails, the whole eviction fails */
    for (i=evictStart; i<caches->numLinks; i++) {
        MPI_Status     *status;
        MPI_Request    *commReq;
        lock_status    *lock_st;
        int             numCommReq;
        int             numBlocks = caches->list[i].numBlocks;
        int             block_id  = caches->list[i].offset / block_size;
        cache_file     *fptr      = caches->list[i].fptr;
        cache_req_msg  *remote_req;

        /* find the file pointer for list[i] */
        status     = (MPI_Status*) ADIOI_Malloc(numBlocks *sizeof(MPI_Status));
        commReq    = (MPI_Request*)ADIOI_Malloc(numBlocks *sizeof(MPI_Request));
        lock_st    = (lock_status*)ADIOI_Malloc(numBlocks *sizeof(lock_status));
        remote_req = (cache_req_msg*)ADIOI_Malloc(numBlocks *sizeof(cache_req_msg));
        numCommReq = 0;

        /* each list[i] may contain multiple file blocks */
        for (k=0; k<numBlocks; k++) {
            int dest = (block_id + k) % fptr->np;
            int id   = (block_id + k) / fptr->np;

#ifdef WKL_DEBUG
printf("(t) evict try lock proc=%d index=%d -----------------------------\n",dest,id);
#endif

            if (dest != fptr->rank) { /* global cache metadata is not local */
                remote_req[k].gfid      = fptr->gfid;
                remote_req[k].disp      = id;
                remote_req[k].count     = 1;
                remote_req[k].req_type  = CACHE_TABLE_TRY_LOCK;

                MPI_Isend(&remote_req[k], sizeof(cache_req_msg), MPI_BYTE, dest,
                          CACHE_TAG_REQ+fptr->gfid, fptr->comm,
                          &commReq[numCommReq]);

                /* this send will have a recv reply */
                MPI_Request_free(&commReq[numCommReq]);

                MPI_Irecv(lock_st+k, sizeof(lock_status), MPI_BYTE, dest,
                          CACHE_TABLE_TRY_LOCK, fptr->comm,
                          &commReq[numCommReq++]);
            }
            else {  /* global caching metadata is held locally
                       fptr->table[id].owner always == self here */
                if (id >= fptr->table_size) { /* expand the table size */
                    int old_size = fptr->table_size;
                    fptr->table_size = (id / CACHE_FILE_TABLE_CHUNK + 1)
                                     * CACHE_FILE_TABLE_CHUNK;
                    fptr->table = (block_status*) ADIOI_Realloc(fptr->table,
                                  fptr->table_size * sizeof(block_status));
                    for (j=old_size; j<fptr->table_size; j++) {
                        fptr->table[j].owner         = TABLE_EMPTY;
                        fptr->table[j].lock          = TABLE_LOCK_FREE;
                        fptr->table[j].lockType      = TABLE_LOCK_FREE;
                        fptr->table[j].numReadLocks  = 0;
                        fptr->table[j].last          = TABLE_EMPTY;
                        fptr->table[j].qsize         = 0;
                        fptr->table[j].qcount        = 0;
                        fptr->table[j].queue         = NULL;
                        fptr->table[j].qType         = NULL;
                    }
                }
                if (fptr->table[id].lockType == TABLE_LOCK_FREE) {
                    /* this block is cached and lock-free */
                    fptr->table[id].lock     = fptr->rank;
                    fptr->table[id].lockType = CACHE_WRITE_LOCK;
                    lock_st[k].lock          = fptr->rank;
                }
                else {  /* this page is currently locked by other process */
                    lock_st[k].lock = TABLE_LOCKED;
                    doEvict = 0;
                    break; /* of loop k */
                }
            }
        }  /* loop k */

        /* wait for all remote try locks to complete */
        WAITALL(numCommReq, commReq, status, "cache_alloc()");

        /* build a linked list of all granted locks to pages to be evicted */
        for (j=0; j<k; j++) {  /* only first k elements have values */
            if (lock_st[j].lock != fptr->rank)  /* not granted */
                doEvict = 0;
            else {  /* granted locks: append to the evict lock linked list.
                       only those granted locks need to be freed */
                int dest = (block_id + j) % fptr->np;
                int id   = (block_id + j) / fptr->np;
                if (elock_list == NULL) {
                    elock_list = (_elock*) ADIOI_Malloc(sizeof(_elock));
                    elock_tail = elock_list;
                }
                else {
                    elock_tail->next = (_elock*) ADIOI_Malloc(sizeof(_elock));
                    elock_tail = elock_tail->next;
                }
                elock_tail->dest = dest;
                elock_tail->fptr = fptr;
                elock_tail->disp = id;
                elock_tail->next = NULL;
                num_elocks++;
            }
        } /* end of loop j */

        ADIOI_Free(remote_req);
        ADIOI_Free(lock_st);
        ADIOI_Free(commReq);
        ADIOI_Free(status);

        if (doEvict == 0) break;  /* of loop i */
    }

    /* if there is any unsuccessful try locks, reset all granted locks to
       their locking status (unlock) and simply return NULL -----------------*/
    if (doEvict == 0) {
        /* release locks for caches->list[evictStart] ... caches->list[i] */

#ifdef WKL_DEBUG
printf("(t) evicting pages TRY_LOCK UN-successful from %d to %d ------------------------------- aborting caching\n",evictStart,i);
#endif

        metadata_unlock_list(num_elocks, elock_list, CACHE_TABLE_UNLOCK,
                             "Evict release try lock");

        return NULL;
    }

    /* Now, doEvict == 1, i.e. all locks are granted. Need to evict locked
       pages, reset global metadata -----------------------------------------*/
    for (i=evictStart; i<caches->numLinks; i++) {
#ifdef WKL_DEBUG
extern int eNTraces;
eNTraces += caches->list[i].numBlocks;
#endif

        /* check if dirty, if yes, flush to file system */
        if (caches->list[i].isDirty) {
            ADIO_Offset pos; /* get the current file position */
            pos = sys_lseek(caches->list[i].fptr->file_system,
                            caches->list[i].fptr->fid, 0, SEEK_CUR);

            if (caches->list[i].fptr->do_block_align_io)
                sys_aligned_write(caches->list[i].fptr->file_system,
                                  caches->list[i].fptr->fid,
                                  caches->list[i].fptr->fsize,
                                  caches->list[i].buf,
                                  caches->list[i].offset,
                                  caches->list[i].end);
            else
                sys_write_at(caches->list[i].fptr->file_system,
                             caches->list[i].fptr->fid,
                             caches->list[i].offset,
                             caches->list[i].buf,
                             caches->list[i].end,
                             caches->list[i].fptr->do_locking_io,
                             0);
#ifdef WKL_DEBUG
printf("%2d: do_block_align_io=%d evict offset=%lld, len=%d\n",caches->list[i].fptr->rank,caches->list[i].fptr->do_block_align_io,caches->list[i].offset,caches->list[i].end);
if (MAX(caches->list[i].offset,40960) < MIN(49152,caches->list[i].offset+caches->list[i].end)) printf("Evict {45124] offset=%lld, len=%d\n",caches->list[i].offset,caches->list[i].end);
#endif
            /* restore the original file position */
            sys_lseek(caches->list[i].fptr->file_system,
                      caches->list[i].fptr->fid, pos, SEEK_SET);
        }
        /* free up list[i] */
        ADIOI_Free(caches->list[i].buf);
        caches->list[i].buf = NULL;

        /* decrease the blocks allocated */
        caches->numBlocksAlloc -= caches->list[i].numBlocks;

#ifdef WKL_DEBUG
{int numBlocks = caches->list[i].numBlocks; int block_id  = caches->list[i].offset / block_size; printf("(t) evicting pages from %12d to %12d ------------------------------- \n",block_id,block_id+numBlocks-1); }
#endif
    }
    caches->numLinks = evictStart;

    /* reset the global cache metadata for evicted blocks */
    metadata_unlock_list(num_elocks, elock_list, CACHE_TABLE_RESET,"Evict RESET");

    /* create a new buffer for cache page allocaton request */
    /* using calloc to ensure the zeros beyond md->end */
    md = (cache_md*) ADIOI_Malloc(sizeof(cache_md));
    md->buf = (char*) ADIOI_Calloc(num * block_size, 1);
    md->end = 0;

#ifdef WKL_DEBUG
printf("(t) evicting pages successful ------------------------------- done\n");
#endif

    return md;
}

