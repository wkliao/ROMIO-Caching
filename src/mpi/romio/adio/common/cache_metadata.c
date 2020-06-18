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

static int self_unlock_wait;

/*----< metadata_lock() >-----------------------------------------------------*/
/* this subroutine issues a lock request to the metadata of the file block
   pointed by fptr->table[indx] at remote/local process, dest                 */
int metadata_lock(cache_file   *fptr,
                  const int     dest,     /* proc id holding the metadata */
                  const int     indx,     /* metadata array index at dest */
                        int     lockType, /* CACHE_READ_LOCK or CACHE_WRITE_LOCK */
                  lock_status *lock_st) /* return caching status */
{
    if (dest == fptr->rank) { /* metadata is in stored locally --------------*/
        if (indx >= fptr->table_size) { /* expand the table size */
            int i, old_size = fptr->table_size;
            fptr->table_size = (indx / CACHE_FILE_TABLE_CHUNK + 1)
                             * CACHE_FILE_TABLE_CHUNK;
            fptr->table = (block_status*) ADIOI_Realloc(fptr->table,
                          fptr->table_size * sizeof(block_status));
            for (i=old_size; i<fptr->table_size; i++) {
                fptr->table[i].owner         = TABLE_EMPTY;
                fptr->table[i].lock          = TABLE_LOCK_FREE;
                fptr->table[i].lockType      = TABLE_LOCK_FREE;
                fptr->table[i].numReadLocks  = 0;
                fptr->table[i].last          = TABLE_EMPTY;
                fptr->table[i].qsize         = 0;
                fptr->table[i].qcount        = 0;
                fptr->table[i].queue         = NULL;
                fptr->table[i].qType         = NULL;
            }
        }

        if (fptr->do_page_migration == 1 &&
            lockType == CACHE_READ_LOCK &&
            fptr->table[indx].last  == fptr->rank &&
            fptr->table[indx].owner != fptr->rank) {
            /* under these conditions, it is possbile this page will be
               migrated, make sure the lock is exclusive to prevent remote
               read before this block is migrated */
#ifdef WKL_DEBUG
printf("%d: READ_LOCK -> WRITE_LOCK dest=%d, indx=%d last=%d owner=%d\n",fptr->rank,dest,indx,fptr->table[indx].last,fptr->table[indx].owner);
#endif
            lockType = CACHE_WRITE_LOCK;
        }

        if (fptr->table[indx].owner == TABLE_EMPTY) {
#ifdef WKL_DEBUG
if (indx*fptr->np+fptr->rank == 11) printf("block 11 is locked/cached by %d\n",fptr->rank);
printf("metadata server (self): grant lock (EMPTY) of block %d from owner %d (lock by %d) to owner %d (lock by %d)\n",indx*fptr->np+fptr->rank,fptr->table[indx].owner,fptr->table[indx].lock,fptr->rank,fptr->rank);
#endif
            /* this block is currently not cached anywhere, it will be
               cached and locked by fptr->rank once return */
            fptr->table[indx].owner    = fptr->rank;
            fptr->table[indx].lock     = fptr->rank;
            fptr->table[indx].last     = TABLE_EMPTY;
            fptr->table[indx].lockType = CACHE_WRITE_LOCK;
            /* use CACHE_WRITE_LOCK to ensure no one can access the pages
               before it is cached */

            /* return lock status */
            lock_st->owner = TABLE_EMPTY;
            lock_st->lock  = fptr->rank;
            lock_st->last  = TABLE_EMPTY;
        }
        else if (fptr->table[indx].lock == TABLE_LOCK_FREE) {
#ifdef WKL_DEBUG
if (indx*fptr->np+fptr->rank == 11) printf("block 11 is locked by %d\n",fptr->rank);
printf("metadata server (self): grant lock (FREE) of block %d from owner %d (lock by %d) to owner %d (lock by %d)\n",indx*fptr->np+fptr->rank,fptr->table[indx].owner,fptr->table[indx].lock,fptr->table[indx].owner,fptr->rank);
#endif
            /* this block is already cached and lock-free, grant the lock
               to fptr->rank */
            fptr->table[indx].lock     = fptr->rank;
            fptr->table[indx].lockType = lockType;
            if (lockType == CACHE_READ_LOCK)
                fptr->table[indx].numReadLocks++;

            /* return lock status */
            lock_st->owner = fptr->table[indx].owner;
            lock_st->lock  = fptr->table[indx].lock;
            lock_st->last  = fptr->table[indx].last;
        }
        else if (fptr->table[indx].lockType == CACHE_READ_LOCK &&
                                   lockType == CACHE_READ_LOCK &&
                 fptr->table[indx].qcount == 0) {
            /* this block is read locked by someone, read locks can be shared */
            fptr->table[indx].numReadLocks++;

            /* return lock status */
            lock_st->owner = fptr->table[indx].owner;
            lock_st->lock  = fptr->rank;
            lock_st->last  = fptr->table[indx].last;
        }
        else { /* 1) fptr->table[indx].lockType == CACHE_WRITE_LOCK
                  2) fptr->table[indx].lockType == CACHE_READ_LOCK and
                     lockType == CACHE_WRITE_LOCK
                  3) fptr->table[indx].lockType == CACHE_READ_LOCK and
                     lockType == CACHE_READ_LOCK but queue is not empty */
#ifdef WKL_DEBUG
if (fptr->table[indx].lockType == CACHE_READ_LOCK) printf("Insert a READ lock into Queue\n");
extern int nReadQueued, nWriteQueued;
if (fptr->table[indx].lockType == CACHE_WRITE_LOCK) nWriteQueued++;
else nReadQueued++;

int wkl=fptr->table[indx].lock;
#endif
            self_unlock_wait = 1;
            INSERT_LOCK_QUEUE(fptr, indx, fptr->rank, lockType);
            while (self_unlock_wait) { /* watching a local variable */
                cache_file *ptr = shared_v.cache_file_list;
                while (ptr != NULL) {
                    probe_incoming_cache(ptr, -1, "metadata_lock()");
                    ptr = ptr->next;
                }
            }
            /* lock request is granted */
            lock_st->owner = fptr->table[indx].owner;
            lock_st->lock  = fptr->rank;
            lock_st->last  = fptr->table[indx].last;

            if (fptr->table[indx].owner == TABLE_EMPTY)
                /* this block has previously been evicted */
                fptr->table[indx].owner = fptr->rank;
#ifdef WKL_DEBUG
printf("metadata server (self): grant lock (UNLOCK by other) of block %d from owner %d (lock by %d) to owner %d (lock by %d)\n",indx*fptr->np+fptr->rank,fptr->table[indx].owner,wkl,fptr->table[indx].owner,fptr->rank);
#endif
        }
    }
    else {  /* metadata is stored remotely ----------------------------------*/
        MPI_Status    status;
        MPI_Request   comm_req;
        cache_req_msg lock_req;

        /* make a request to a remote process */
        lock_req.gfid      = fptr->gfid;
        lock_req.req_type  = lockType;
        lock_req.disp      = indx;
        lock_req.count     = 1;

        /* send out lock request message */
        MPI_Isend(&lock_req, sizeof(cache_req_msg), MPI_BYTE, dest,
                  CACHE_TAG_REQ+fptr->gfid, fptr->comm, &comm_req);
        MPI_Request_free(&comm_req);

        /* gets the current caching status replied from dest */
        MPI_Irecv(lock_st, sizeof(lock_status), MPI_BYTE, dest,
                  CACHE_GRANT_LOCK, fptr->comm, &comm_req);

        /* wait until the lock request is granted -- this is to ensure
           locks are granted in increasing order for consistency reason */
        POLLING(comm_req, status, "metadata_lock()");
    }
    /* now, lock for block indx should have been obtained */
    assert(lock_st->lock == fptr->rank);

#ifdef WKL_DEBUG
if (lock_st->lock != fptr->rank) printf("Error - lock fails: lock_st->lock(%d)!=SELF(%d) dest=%d indx=%d\n", lock_st->lock,fptr->rank,dest,indx);
#endif

    return 1;
}


/*----< metadata_unlock() >-------------------------------------------------*/
int metadata_unlock(cache_file *fptr,
                    const int   numBlocks,
                    const int   startBlockID,
                    const int  *unlock_types,   /* [numBlocks] */
                    const char *msg)
{
    int           i, total_numUnlocks;
    int          *dest_v, *indx_v, *type_v, *numUnlocks;
    dest_v     = (int*)  ADIOI_Malloc(numBlocks * sizeof(int));
    indx_v     = (int*)  ADIOI_Malloc(numBlocks * sizeof(int));
    type_v     = (int*)  ADIOI_Malloc(numBlocks * sizeof(int));
    numUnlocks = (int*)  ADIOI_Calloc(fptr->np,   sizeof(int));
    total_numUnlocks = 0;

    for (i=0; i<numBlocks; i++) {
        int  dest, id;
        dest = (startBlockID + i) % fptr->np; /* dest proc holds block */
        id   = (startBlockID + i) / fptr->np; /* block id local to dest */

        if (dest == fptr->rank) { /* global cache metadata is stored locally */
#ifdef WKL_DEBUG
if(unlock_types[i]==CACHE_TABLE_UNLOCK_SWITCH) printf("SELF metadata of block %d SWITCH from %d to %d\n",startBlockID+i,fptr->table[id].owner,fptr->rank);
#endif
            if (fptr->table[id].lockType == CACHE_READ_LOCK) {
                /* this is read-shared unlocking */
                fptr->table[id].numReadLocks--;
                if (fptr->table[id].numReadLocks == 0)
                    update_lock_queue(fptr, id, unlock_types[i], fptr->rank);
            }
            else /* this is write-exclusive unlocking */
                update_lock_queue(fptr, id, unlock_types[i], fptr->rank);
        }
        else {  /* global caching metadata is stored in a remote proc */
            dest_v[total_numUnlocks] = dest;
            indx_v[total_numUnlocks] = id;
            type_v[total_numUnlocks] = unlock_types[i];
            numUnlocks[dest]++;
            total_numUnlocks++;
        }
    }
    /* pack remote unlock requests into a single message ! */
    if (total_numUnlocks > 0) {
        int             numCommReq;
        MPI_Status     *restrict status;
        MPI_Request    *restrict comm_req;
        cache_req_msg  *restrict remote_req;
        unlock_msg    **unlock_v;

        status     = (MPI_Status*)    ADIOI_Malloc(fptr->np*sizeof(MPI_Status));
        comm_req   = (MPI_Request*)   ADIOI_Malloc(fptr->np*sizeof(MPI_Request));
        remote_req = (cache_req_msg*) ADIOI_Malloc(fptr->np*sizeof(cache_req_msg));
        unlock_v   = (unlock_msg**)   ADIOI_Malloc(fptr->np*sizeof(unlock_msg*));
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
                            remote_req[i].req_type = type_v[j];
                            break;
                        }
                }
                /* using Issend to ensure it's completion after WAITALL */
                MPI_Issend(&remote_req[i], sizeof(cache_req_msg), MPI_BYTE, i,
                           CACHE_TAG_REQ+fptr->gfid, fptr->comm,
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
            unlock_v[dest][numUnlocks[dest]].unlockType = type_v[i];
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
    ADIOI_Free(dest_v);
    ADIOI_Free(indx_v);
    ADIOI_Free(type_v);
    ADIOI_Free(numUnlocks);

    return 1;
}


/*----< update_lock_queue() >------------------------------------------------*/
int update_lock_queue(cache_file *fptr,
                      int         id,
                      int         req,
                      int         unlock_proc)
{
    int i, j, newHead;

    if (fptr->table[id].qcount == 0) {
        /* lock queue is empty, no one needs to be informed */
#ifdef WKL_DEBUG
int wkl=fptr->table[id].lock;
#endif
        fptr->table[id].lock     = TABLE_LOCK_FREE;
        fptr->table[id].last     = unlock_proc;
        fptr->table[id].lockType = TABLE_LOCK_FREE;
        if (req == CACHE_TABLE_RESET) {
#ifdef WKL_DEBUG
printf("metadata server (remote req from %d): un-lock (RESET) of block %d from owner %d (lock by %d) ---- Free NOW\n",unlock_proc,id*fptr->np+fptr->rank,fptr->table[id].owner,wkl);
#endif
            /* this file block is no more cached */
            fptr->table[id].owner = TABLE_EMPTY;
            fptr->table[id].last  = TABLE_EMPTY;
        }
        else if (req == CACHE_TABLE_UNLOCK_SWITCH) {
#ifdef WKL_DEBUG
printf("metadata server (remote req from %d): un-lock (SWITCH) of block %d from owner %d (lock by %d) to owner %d ---- Free NOW\n",unlock_proc,id*fptr->np+fptr->rank,fptr->table[id].owner,wkl,unlock_proc);
#endif
            /* the owner of this file block has changed */
            fptr->table[id].owner = unlock_proc;
        }
/*
else
printf("metadata server (remote req from %d): un-lock (UNLOCK) of block %d from owner %d (lock by %d) ---- Free NOW\n",unlock_proc,id*fptr->np+fptr->rank,fptr->table[id].owner,wkl);
*/
        return 1;
    }

    /* Now, someone is waiting in the lock queue, transfer the lock to the
       queue head. Here, unlock_proc should != (fptr)->table[id].queue[0] */
    assert(unlock_proc != fptr->table[id].queue[0]);

    if (req == CACHE_TABLE_RESET) { /* page has been evicted */
#ifdef WKL_DEBUG
printf("metadata server (remote req from %d): transfer lock (RESET) of block %d from P%d (owner=%d lockType=%d) to P%d (owner=%d lockType=%d)\n",unlock_proc,id*fptr->np+fptr->rank,fptr->table[id].lock,fptr->table[id].owner,fptr->table[id].lockType,fptr->table[id].queue[0],fptr->table[id].queue[0],fptr->table[id].qType[0]);
#endif
        /* grant lock to the head queue only, even if the head has read lock,
           because the head will have to cache this page first, so lockType
           is set to CACHE_WRITE_LOCK */
        fptr->table[id].owner    = fptr->table[id].queue[0];
        fptr->table[id].lock     = fptr->table[id].queue[0];
        fptr->table[id].last     = TABLE_EMPTY;
        fptr->table[id].lockType = CACHE_WRITE_LOCK;

        /* if unlock_proc == self, self is watching local variable,
           self_unlock_wait, to become 0 */
        if (fptr->table[id].queue[0] == fptr->rank) {
            fptr->table[id].owner = TABLE_EMPTY;
            self_unlock_wait = 0;
        }
        else {
            /* notify and grant lock to the proc at queue head,
               there is already an Irecv() waiting in remote proc */
            lock_status ret;
            ret.owner = TABLE_EMPTY;
            ret.lock  = fptr->table[id].queue[0];
            ret.last  = TABLE_EMPTY;
            MPI_Send(&ret, sizeof(lock_status), MPI_BYTE,
                     fptr->table[id].queue[0], CACHE_GRANT_LOCK,
                     fptr->comm);
        }
        newHead = 1;
    }
    else { /* req == CACHE_TABLE_UNLOCK_SWITCH or CACHE_TABLE_UNLOCK */
        if (req == CACHE_TABLE_UNLOCK_SWITCH) {
            /* ownership of this block has changed, i.e. this block
               has been migrated to unlock_proc */
#ifdef WKL_DEBUG
printf("metadata server (remote req from %d): migrate lock (SWITCH) of block %d from P%d (owner=%d lockType=%d) to P%d (owner=%d lockType=%d)\n",unlock_proc,id*fptr->np+fptr->rank,fptr->table[id].lock,fptr->table[id].owner,fptr->table[id].lockType,fptr->table[id].queue[0],unlock_proc,fptr->table[id].qType[0]);
#endif
            fptr->table[id].owner = unlock_proc;
        }
        /* else case is req == CACHE_TABLE_UNLOCK */
#ifdef WKL_DEBUG
else printf("metadata server (remote req from %d): transfer lock (UNLOCK) of block %d from P%d (owner=%d lockType=%d) to P%d (owner=%d lockType=%d)\n",unlock_proc,id*fptr->np+fptr->rank,fptr->table[id].lock,fptr->table[id].owner,fptr->table[id].lockType,fptr->table[id].queue[0],fptr->table[id].owner,fptr->table[id].qType[0]);
#endif
        fptr->table[id].lock     = fptr->table[id].queue[0];
        fptr->table[id].lockType = fptr->table[id].qType[0];
        fptr->table[id].last     = unlock_proc;

        /* grant contious read lock requests as many as possible */
        for (i=0; i<fptr->table[id].qcount; i++)
            if (fptr->table[id].qType[i] == CACHE_WRITE_LOCK)
                break;

        newHead = (i == 0) ? 1 : i;
        for (i=0; i<newHead; i++) {
            /* grant a read shared lock */
            if (fptr->table[id].queue[i] == fptr->rank) {
                /* self is watching self_unlock_wait to become 0 */
                fptr->table[id].lock = fptr->rank;
                self_unlock_wait = 0;
            }
            else {
                /* notify and grant lock to the proc in queue for shared read,
                   or write lock (newHead == 1 and grant the head only)
                   there is already an Irecv() waiting in remote proc */
                lock_status ret;
                ret.owner = fptr->table[id].owner;
                ret.lock  = fptr->table[id].queue[i];
                ret.last  = unlock_proc;
                MPI_Send(&ret, sizeof(lock_status), MPI_BYTE,
                         fptr->table[id].queue[i], CACHE_GRANT_LOCK,
                         fptr->comm);
            }
            if (fptr->table[id].qType[i] == CACHE_READ_LOCK)
                fptr->table[id].numReadLocks++;
        }
    }
    /* move the rest of the queue ahead */
    for (i=newHead, j=0; i<fptr->table[id].qcount; i++,j++) {
        fptr->table[id].queue[j] = fptr->table[id].queue[i];
        fptr->table[id].qType[j] = fptr->table[id].qType[i];
    }
    fptr->table[id].qcount -= newHead;

    return 1;
}

