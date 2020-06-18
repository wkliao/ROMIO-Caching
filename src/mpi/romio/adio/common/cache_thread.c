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

thread_shared_varaibles shared_v;


/*----< migration_remote() >------------------------------------------------*/
/* handle cache page migration request from a emote process */
static
int migration_remote(cache_file          *fptr,
                     const cache_req_msg  remote_req,
                     const int          req_node)
{
    int          i, k, req_len, freeMigrateBuf=0;
    char        *migrateBuf = NULL;
    cache_list  *caches     = &shared_v.caches;
    int          block_size = caches->block_size;
    int          dirtyInfo[2];
    ADIO_Offset  reqEnd;

    /* remote_req.offset is aligned with block_size and
       remote_req.count  is multiple of block_size */
    dirtyInfo[0] = dirtyInfo[1] = 0;
    reqEnd       = remote_req.disp + remote_req.count;
    req_len      = remote_req.count;

    /* go through all cache pages to find an intersect page with the
       migration request, which can be located across different
       caches->list[] */
    for (i=0; i<caches->numLinks && req_len > 0; i++) {
        ADIO_Offset  start, end, mdEnd;
        cache_md    *md = caches->list + i;

        if (md->fptr != fptr) continue;

        mdEnd = md->offset + md->numBlocks * block_size;
        start = MAX(remote_req.disp, md->offset);
        end   = MIN(reqEnd, mdEnd);
        if (start >= end) continue; /* no intersect with migrate request */

        /* Now, list[i] intersects with migrate request */
        if (remote_req.disp == md->offset && reqEnd == mdEnd) {
            /* if migrate request is entirely the same as md */
#ifdef WKL_DEBUG
{printf("migration_remote(%lld, %d) from P%d is entirely the same as md caches->numLinks=%d i=%d\n",remote_req.disp,remote_req.count,req_node,caches->numLinks,i);fflush(stdout);}
{if (caches->numLinks == 2) printf("(t) caches->list[0].offset = %lld, caches->list[1].offset = %lld\n",caches->list[0].offset,caches->list[1].offset); fflush(stdout);}
#endif
            dirtyInfo[0] = md->isDirty;
            dirtyInfo[1] = md->end;
            migrateBuf   = md->buf;

            /* deallocate md and coalessce caches->list array */
            caches->numBlocksAlloc -= md->numBlocks;
            for (k=i+1; k<caches->numLinks; k++)
                caches->list[k-1] = caches->list[k];
            caches->numLinks--;

            req_len = 0;
            break;  /* break loop i */
        }
        else if (migrateBuf == NULL) {
            /* remote req is splitted in different cache pages and
               migrateBuf has never been allocated yet */
            migrateBuf = (char*) ADIOI_Malloc(remote_req.count);
            freeMigrateBuf = 1;
        }

        if (md->offset <= remote_req.disp && reqEnd <= mdEnd) {
            /* if migrate request is entirely inside md */
            int front_extra = remote_req.disp - md->offset;
            int end_extra   = mdEnd - reqEnd;

#ifdef WKL_DEBUG
{printf("migration_remote(%lld, %d) from P%d is entirely inside md(%lld, %d) caches->numLinks=%d i=%d\n",remote_req.disp,remote_req.count,req_node,md->offset,md->numBlocks*block_size,caches->numLinks,i);fflush(stdout);}
#endif
            memcpy(migrateBuf, md->buf + front_extra, remote_req.count);
            if (md->isDirty) {
                dirtyInfo[0] = (front_extra < md->end) ? 1 : 0;
                dirtyInfo[1] = MIN(remote_req.count, md->end - front_extra);
            }

            if (front_extra > 0) {
                if (end_extra > 0) { /* create a new md for end_extra */
                    int      len;
                    cache_md new_md;

                    new_md.fptr      = md->fptr;
                    new_md.numBlocks = end_extra / block_size;
                    new_md.offset    = reqEnd;
                    new_md.reference = md->reference;
                    len              = md->offset + md->end - reqEnd;
                    new_md.isDirty   = (len > 0) ?   1 : 0;
                    new_md.end       = (len > 0) ? len : 0;
                    new_md.buf       = (char*) ADIOI_Malloc(end_extra);
                    memcpy(new_md.buf, md->buf+(front_extra+remote_req.count),
                           end_extra);

                    /* add the new cache_md into caches->list */
                    ADD_METADATA_TO_CACHES_LIST(&new_md);
                    /* in ADD_METADATA_TO_CACHES_LIST(), new_md.numBlocks is
                       added to caches->numBlocksAlloc, but it is already
                       counted in caches->numBlocksAlloc before */
                    caches->numBlocksAlloc -= new_md.numBlocks;
                }
                /* reduce md to front_extra by cutting tail */
                md->buf       = (char*) ADIOI_Realloc(md->buf, front_extra);
                md->numBlocks = front_extra / block_size;
                md->end       = MIN(md->end, front_extra);
                /* md->offset and md->isDirty are not changed */
            }
            else { /* front_extra == 0 and then end_extra must > 0 */
                /* reduce md size by cutting front */
                char *buf = (char*) ADIOI_Malloc(end_extra);
                memcpy(buf, md->buf + remote_req.count, end_extra);
                ADIOI_Free(md->buf);
                md->buf       = buf;
                md->numBlocks = end_extra / block_size;
                if (md->isDirty) {
                    int len = md->offset + md->end - reqEnd;
                    md->isDirty = (len > 0) ?   1 : 0;
                    md->end     = (len > 0) ? len : 0;
                }
                md->offset = reqEnd;
            }
            /* reduce the total numBlocksAlloc in caches */
            caches->numBlocksAlloc -= remote_req.count / block_size;

            req_len = 0;
            break;  /* break loop i */
        }

        if (remote_req.disp <= md->offset && mdEnd <= reqEnd) {
            /* if md is entirely inside migrate request */
            int front_extra = md->offset - remote_req.disp;
            int mdLen       = md->numBlocks * block_size;
            memcpy(migrateBuf + front_extra, md->buf, mdLen);
            ADIOI_Free(md->buf);

#ifdef WKL_DEBUG
{printf("migration_remote md(%lld, %d) is entirely inside migration(%lld, %d) from P%d caches->numLinks=%d i=%d\n",md->offset,md->numBlocks*block_size,remote_req.disp,remote_req.count,req_node,caches->numLinks,i);fflush(stdout);}
#endif
            if (md->isDirty) {
                dirtyInfo[0] = 1;
                dirtyInfo[1] = MAX(dirtyInfo[1], front_extra + md->end);
            }

            /* deallocate md and coalessce caches->list array */
            caches->numBlocksAlloc -= md->numBlocks;
            for (k=i+1; k<caches->numLinks; k++)
                caches->list[k-1] = caches->list[k];
            caches->numLinks--;

            req_len -= mdLen;
            i--;    /* cache->list[i] has been removed and coalessced */
        }
        else if (remote_req.disp < md->offset) {
            /* if migrate request overlaps with front of md.
               Here, reqEnd must < mdEnd.
               Reduce md size by cutting the front overlap */
            int   end_extra = mdEnd - reqEnd;
            int   migrateLen = md->numBlocks * block_size - end_extra;
            char *buf = (char*) ADIOI_Malloc(end_extra);
#ifdef WKL_DEBUG
{printf("migration_remote(%lld, %d) from P%d overlaps front of md(%lld, %d) caches->numLinks=%d i=%d\n",remote_req.disp,remote_req.count,req_node,md->offset,md->numBlocks*block_size,caches->numLinks,i);fflush(stdout);}
#endif
            /* copy end of md->buf to migrateBuf */
            memcpy(migrateBuf + (md->offset - remote_req.disp),
                   md->buf, migrateLen);
            /* reduce size of md->buf */
            memcpy(buf, md->buf + migrateLen, end_extra);
            ADIOI_Free(md->buf);
            md->buf       = buf;
            md->numBlocks = end_extra / block_size;
            if (md->isDirty) {
                int len = md->offset + md->end - reqEnd;
                int dirtyEnd;
                md->isDirty = (len > 0) ?   1 : 0;
                md->end     = (len > 0) ? len : 0;
                dirtyEnd    = remote_req.count;
                if (len < 0) dirtyEnd += len;
                dirtyInfo[0] = 1;
                dirtyInfo[1] = MAX(dirtyInfo[1], dirtyEnd);
            }
            md->offset = reqEnd;

            /* reduce the total numBlocksAlloc in caches */
            caches->numBlocksAlloc -= migrateLen / block_size;
            req_len -= migrateLen;
        }
        else if (md->offset < remote_req.disp) {
            /* if migrate request overlaps with end of md.
               Here, mdEnd must < reqEnd.
               Reduce md size by cutting the end overlap */
            int   front_extra = remote_req.disp - md->offset;
            int   migrateLen = md->numBlocks * block_size - front_extra;
#ifdef WKL_DEBUG
{printf("migration_remote(%lld, %d) from P%d overlaps end of md(%lld, %d) caches->numLinks=%d i=%d\n",remote_req.disp,remote_req.count,req_node,md->offset,md->numBlocks*block_size,caches->numLinks,i);fflush(stdout);}
#endif
            /* copy end of md->buf to migrateBuf */
            memcpy(migrateBuf, md->buf + front_extra, migrateLen);
            /* reduce size of md->buf */
            md->buf       = (char*) ADIOI_Realloc(md->buf, front_extra);
            md->numBlocks = front_extra / block_size;
            /* md->offset and md->isDirty are not changed */
            if (md->isDirty) {
                int dirtyEnd = (md->end <= front_extra) ? 0 : front_extra - md->end;
                dirtyInfo[0] = (dirtyEnd > 0) ? 1 : 0;
                dirtyInfo[1] = MAX(dirtyInfo[1], dirtyEnd);
                md->end      = MIN(md->end, front_extra);
            }

            /* reduce the total numBlocksAlloc in caches */
            caches->numBlocksAlloc -= migrateLen / block_size;
            req_len -= migrateLen;
        }
    }
    if (req_len != 0)
        fprintf(stderr,"Error: migrate_remote req_len(%d) != 0\n",req_len);

    /* send isDirty and end about dirty data in cache pages to be migrated */
    MPI_Send(dirtyInfo, 2, MPI_INT, req_node, CACHE_MIGRATE_INFO, fptr->comm);

#ifdef WKL_DEBUG
{printf("(t) migrate[%d bytes] migrateBuf = %c%c%c%c%c%c%c%c%c%c|%c%c%c%c%c%c%c%c%c%c\n",remote_req.count,migrateBuf[0],migrateBuf[1],migrateBuf[2],migrateBuf[3],migrateBuf[4],migrateBuf[5],migrateBuf[6],migrateBuf[7],migrateBuf[8],migrateBuf[9],migrateBuf[10],migrateBuf[11],migrateBuf[12],migrateBuf[13],migrateBuf[14],migrateBuf[15],migrateBuf[16],migrateBuf[17],migrateBuf[18],migrateBuf[19]);fflush(stdout);}
#endif

    /* send back data in a contiguous buffer */
    MPI_Send(migrateBuf, remote_req.count, MPI_BYTE, req_node, CACHE_MIGRATE,
             fptr->comm);

    if (freeMigrateBuf) ADIOI_Free(migrateBuf);

    return 1;
}


/*----< probe_incoming_cache() >-----------------------------------------------*/
int probe_incoming_cache(cache_file *ptr, int req_node, const char *location)
{
    static int   fair = 0;
    int          i;
    int          incoming;
    cache_list  *caches     = &shared_v.caches;
    int          block_size = caches->block_size;
    MPI_Status   probe_status, recv_status;
    cache_req_msg  remote_req;

#ifdef WKL_DEBUG
    {
        static int counter = 0;
        counter++;
        if (counter % 1000000 == 0) {
            char stdout_str[1024];
            sprintf(stdout_str, "(t) probe -- %d times at %s shared_v.req=%d table=",
                    counter,location,shared_v.req);
            for (i=0; i<ptr->table_size; i++) {
                if (ptr->table[i].owner != TABLE_EMPTY) {
                    char str[32];
                    sprintf(str,"[%2d](o=%d, k=%d, l=%d) ", i*ptr->np+ptr->rank,
                            ptr->table[i].owner, ptr->table[i].lock,
                            ptr->table[i].last);
                    if (strlen(stdout_str) > 1024) break;
                    strcat(stdout_str, str);
                }
            }
            printf("%s\n",stdout_str); fflush(stdout);
        }
    }
#endif

    /* check if a request came from remote processor ------------------------*/
    MUTEX_LOCK(shared_v.mutex);
    /* Using MPI_ANY_SOURCE may not be fair for multiple remote requests!
    MPI_Iprobe(MPI_ANY_SOURCE, CACHE_TAG_REQ+ptr->gfid, ptr->comm,
               &incoming, &probe_status);
    req_node = probe_status.MPI_SOURCE;
    */
    incoming = 0;
    if (req_node < 0) {
        for (i=0; i<ptr->np; i++) {
            req_node = (fair + i) % ptr->np;
            if (req_node == ptr->rank) continue;
            MPI_Iprobe(req_node, CACHE_TAG_REQ+ptr->gfid, ptr->comm,
                       &incoming, &probe_status);
            if (incoming) break;
        }
    }
    else {
        MPI_Iprobe(req_node, CACHE_TAG_REQ+ptr->gfid, ptr->comm,
                   &incoming, &probe_status);
    }
    MUTEX_UNLOCK(shared_v.mutex);

    if (! incoming) return 0;  /* no incoming request */
    fair = req_node + 1;

    /* NOW, there is a remote CACHE request -----------------------------------*/
    /* receive the CACHE request from remote process */
    MPI_Recv(&remote_req, sizeof(cache_req_msg), MPI_BYTE, req_node,
             CACHE_TAG_REQ+ptr->gfid, ptr->comm, &recv_status);

#ifdef WKL_DEBUG
{
char *typeStr="";
if(remote_req.req_type==CACHE_TAG_GET)typeStr="CACHE_TAG_GET";
if(remote_req.req_type==CACHE_TAG_PUT)typeStr="CACHE_TAG_PUT";
if(remote_req.req_type==CACHE_BARRIER)typeStr="CACHE_BARRIER";
if(remote_req.req_type==CACHE_TERMINATE)typeStr="CACHE_TERMINATE";
if(remote_req.req_type==CACHE_ALL_REDUCE)typeStr="CACHE_ALL_REDUCE";
if(remote_req.req_type==CACHE_TABLE_LOCK)typeStr="CACHE_TABLE_LOCK";
if(remote_req.req_type==CACHE_TABLE_UNLOCK)typeStr="CACHE_TABLE_UNLOCK";
if(remote_req.req_type==CACHE_TABLE_RESET)typeStr="CACHE_TABLE_RESET";
if(remote_req.req_type==CACHE_TABLE_TRY_LOCK)typeStr="CACHE_TABLE_TRY_LOCK";
if(remote_req.req_type==CACHE_FILE_OPEN)typeStr="CACHE_FILE_OPEN";
if(remote_req.req_type==CACHE_FILE_CLOSE)typeStr="CACHE_FILE_CLOSE";
if(remote_req.req_type==CACHE_FILE_READ)typeStr="CACHE_FILE_READ";
if(remote_req.req_type==CACHE_FILE_WRITE)typeStr="CACHE_FILE_WRITE";
if(remote_req.req_type==CACHE_GET_FSIZE)typeStr="CACHE_GET_FSIZE";
if(remote_req.req_type==CACHE_SET_FSIZE)typeStr="CACHE_SET_FSIZE";
if(remote_req.req_type==CACHE_MIGRATE)typeStr="CACHE_MIGRATE";
if(remote_req.req_type==CACHE_MIGRATE_INFO)typeStr="CACHE_MIGRATE_INFO";
if(remote_req.req_type==CACHE_TABLE_UNLOCK_SWITCH)typeStr="CACHE_TABLE_UNLOCK_SWITCH";
printf("(t) probe from proc %d -- gfid=%d req_type=%s disp=%lld count=%d\n",req_node,remote_req.gfid,typeStr,remote_req.disp,remote_req.count); fflush(stdout);}
#endif

    if (remote_req.req_type == CACHE_READ_LOCK ||
        remote_req.req_type == CACHE_WRITE_LOCK) {  /*-------------------------*/
        int          tableNUM = remote_req.disp;
        int          lockType = remote_req.req_type;
        lock_status  ret;

        if (tableNUM >= ptr->table_size) { /* expand the table size */
            int i, old_size = ptr->table_size;
            ptr->table_size = (tableNUM / CACHE_FILE_TABLE_CHUNK + 1)
                             * CACHE_FILE_TABLE_CHUNK;
            ptr->table = (block_status*) ADIOI_Realloc(ptr->table,
                          ptr->table_size * sizeof(block_status));
            for (i=old_size; i<ptr->table_size; i++) {
                ptr->table[i].owner         = TABLE_EMPTY;
                ptr->table[i].lock          = TABLE_LOCK_FREE;
                ptr->table[i].lockType      = TABLE_LOCK_FREE;
                ptr->table[i].numReadLocks  = 0;
                ptr->table[i].last          = TABLE_EMPTY;
                ptr->table[i].qsize         = 0;
                ptr->table[i].qcount        = 0;
                ptr->table[i].queue         = NULL;
                ptr->table[i].qType         = NULL;
            }
        }

        if (ptr->do_page_migration == 1 &&
            ptr->table[tableNUM].last  == req_node &&
            ptr->table[tableNUM].owner != req_node) {
            /* it is possbile this page will be migrated, make sure the lock
               is exclusive to prevent concurrent read */
            lockType = CACHE_WRITE_LOCK;
        }

#ifdef WKL_DEBUG
{printf("(t): CACHE_TABLE_LOCK from proc %d table[%d].owner=%d lock=%d\n",req_node,tableNUM,ptr->table[tableNUM].owner,ptr->table[tableNUM].lock);fflush(stdout);}
#endif
        /* return the original current caching status, then update the status */
        if (ptr->table[tableNUM].owner == TABLE_EMPTY) {
            /* this block is currently not cached anywhere, so grant lock to
               req_node which will cache it (set lockType to CACHE_WRITE_LOCK) */
            ptr->table[tableNUM].owner    = req_node;
            ptr->table[tableNUM].lock     = req_node;
            ptr->table[tableNUM].last     = TABLE_EMPTY;
            ptr->table[tableNUM].lockType = CACHE_WRITE_LOCK;
            ret.owner = TABLE_EMPTY;
            ret.lock  = req_node;
            ret.last  = TABLE_EMPTY;
            /* reply the caching status to the requester */
            MPI_Send(&ret, sizeof(lock_status), MPI_BYTE, req_node,
                     CACHE_GRANT_LOCK, ptr->comm);
#ifdef WKL_DEBUG
printf("metadata server (remote req from %d): grant lock (EMPTY) of block %d to proc %d\n",req_node,tableNUM*ptr->np+ptr->rank,req_node);
#endif
        }
        else if (ptr->table[tableNUM].lock == TABLE_LOCK_FREE) {
#ifdef WKL_DEBUG
{printf("(t) granting CACHE_TABLE_LOCK request to %d, disp=%d\n",req_node,tableNUM);fflush(stdout);}
#endif
            /* this block is cached and lock-free */
            ptr->table[tableNUM].lock     = req_node;
            ptr->table[tableNUM].lockType = lockType;
            if (lockType == CACHE_READ_LOCK)
                ptr->table[tableNUM].numReadLocks++;

            ret.owner = ptr->table[tableNUM].owner;
            ret.lock  = ptr->table[tableNUM].lock;
            ret.last  = ptr->table[tableNUM].last;
            /* reply the caching status to the requester */
            MPI_Send(&ret, sizeof(lock_status), MPI_BYTE, req_node,
                     CACHE_GRANT_LOCK, ptr->comm);
#ifdef WKL_DEBUG
printf("metadata server (remote req from %d): grant lock (FREE) of block %d to proc %d\n",req_node,tableNUM*ptr->np+ptr->rank,req_node);
#endif
        }
        else if (ptr->table[tableNUM].lockType == CACHE_READ_LOCK &&
                                      lockType == CACHE_READ_LOCK &&
                 ptr->table[tableNUM].qcount == 0) {
            /* this block is read locked by someone, read locks can be shared */
            ptr->table[tableNUM].numReadLocks++;
            ret.owner = ptr->table[tableNUM].owner;
            ret.lock  = req_node;
            ret.last  = ptr->table[tableNUM].last;
            MPI_Send(&ret, sizeof(lock_status), MPI_BYTE, req_node,
                     CACHE_GRANT_LOCK, ptr->comm);
        }
        else { /* this block is exclusively-locked by someone */
#ifdef WKL_DEBUG
if (remote_req.req_type == CACHE_READ_LOCK) printf("Insert a READ_LOCK(%d) into Queue lock=%d lockType=%d qcount=%d\n",CACHE_READ_LOCK,ptr->table[tableNUM].lock,lockType,ptr->table[tableNUM].qcount);
extern int nReadQueued, nWriteQueued;
if (remote_req.req_type == CACHE_WRITE_LOCK) nWriteQueued++;
else nReadQueued++;

{printf("(t) remote proc %d's CACHE_TABLE_LOCK request to table[%d] is current locked by %d, lock queue count=%d\n",req_node,tableNUM,ptr->table[tableNUM].lock,ptr->lockQ[tableNUM].count);fflush(stdout);}
#endif
            INSERT_LOCK_QUEUE(ptr, tableNUM, req_node, lockType);
        }
    }
    else if (remote_req.req_type == CACHE_TABLE_TRY_LOCK) {  /*----------------*/
        int          tableNUM = remote_req.disp;
        lock_status  ret;
        /* return the original current caching status, then update the status */
        ret.owner = ptr->table[tableNUM].owner;
        ret.lock  = ptr->table[tableNUM].lock;
        ret.last  = ptr->table[tableNUM].last;

#ifdef WKL_DEBUG
{printf("(t): CACHE_TABLE_TRY_LOCK from proc %d table[%d].owner=%d lock=%d\n",req_node,tableNUM,ptr->table[tableNUM].owner,ptr->table[tableNUM].lock);fflush(stdout);}
#endif
        /* TRY_LOCK request comes only from the owner proc, so, here
           ptr->table[tableNUM].owner always == req_node */

        if (ptr->table[tableNUM].lockType == TABLE_LOCK_FREE) {
            /* this block is cached and lock-free */
            ptr->table[tableNUM].lock     = req_node;
            ptr->table[tableNUM].lockType = CACHE_WRITE_LOCK;
            ret.lock                      = req_node;
        }
        else /* this block is currently locked by someone */
            ret.lock = TABLE_LOCKED;

        /* reply the caching status to the requester */
        MPI_Send(&ret, sizeof(lock_status), MPI_BYTE, req_node,
                 CACHE_TABLE_TRY_LOCK, ptr->comm);
    }
#if 0
    else if (remote_req.req_type == CACHE_TABLE_UNLOCK || /*-------------------*/
             remote_req.req_type == CACHE_TABLE_UNLOCK_SWITCH ||
             remote_req.req_type == CACHE_TABLE_RESET) {
        int  tableNUM = remote_req.disp;
        if (ptr->table[tableNUM].numReadLocks > 0) {
            ptr->table[tableNUM].numReadLocks--;
            if (ptr->table[tableNUM].numReadLocks == 0)
                update_lock_queue(ptr, tableNUM, remote_req.req_type, req_node);
            /* when in read-shared lock mode, ptr->table[tableNUM].lock may
               != req_node */
        }
        else if (ptr->table[tableNUM].lock == req_node)
            update_lock_queue(ptr, tableNUM, remote_req.req_type, req_node);
        else
            fprintf(stderr, "%d: TO_BE_REMOVED Error release md[%d].lock=%d not locked by %d\n", ptr->rank,tableNUM,ptr->table[tableNUM].lock,req_node);
    }
#endif
    else if (remote_req.req_type == CACHE_TABLE_UNLOCK || /*-------------------*/
             remote_req.req_type == CACHE_TABLE_UNLOCK_SWITCH ||
             remote_req.req_type == CACHE_TABLE_RESET) {
        if (remote_req.count == 1) {
            /* if there is only one unlock request from dest */
            int tableNUM = remote_req.disp;
            if (ptr->table[tableNUM].lockType == CACHE_READ_LOCK) {
                ptr->table[tableNUM].numReadLocks--;
                if (ptr->table[tableNUM].numReadLocks == 0)
                    update_lock_queue(ptr, tableNUM, remote_req.req_type,
                                      req_node);
                /* when in read-shared lock mode, ptr->table[tableNUM].lock
                    may != req_node */
            }
            else /* write-exclisive lock */
                update_lock_queue(ptr, tableNUM, remote_req.req_type, req_node);
        }
        else { /* remote_req.count > 1: multiple unlock requests from dest.
               prepare to receive an array of unlock types */
            unlock_msg *unlock_v = ADIOI_Malloc(remote_req.count *
                                                    sizeof(unlock_msg));
            MPI_Recv(unlock_v, remote_req.count*sizeof(unlock_msg), MPI_BYTE,
                     req_node, CACHE_TABLE_UNLOCK_V, ptr->comm, &recv_status);
            for (i=0; i<remote_req.count; i++) {
                int tableNUM = unlock_v[i].indx;
                if (ptr->table[tableNUM].lockType == CACHE_READ_LOCK) {
                    ptr->table[tableNUM].numReadLocks--;
                    if (ptr->table[tableNUM].numReadLocks == 0)
                        update_lock_queue(ptr, tableNUM, unlock_v[i].unlockType,
                                          req_node);
                    /* when in read-shared lock mode, ptr->table[tableNUM].lock
                        may != req_node */
                }
                else
                    update_lock_queue(ptr, tableNUM, unlock_v[i].unlockType,
                                      req_node);
            }
            ADIOI_Free(unlock_v);
        }
    }
    else if (remote_req.req_type == CACHE_TAG_PUT) { /*------------------------*/
        int            doFreeTmpBuf = 0;
        int            req_len      = remote_req.count;
        char *restrict putBuf       = NULL;

#ifdef WKL_DEBUG
{printf("(t) CACHE_TAG_PUT from %d gfid=%d disp=%lld len=%d caches->numLinks=%d\n",req_node,remote_req.gfid,remote_req.disp,remote_req.count,caches->numLinks); fflush(stdout);}
#endif

        /* the cache buffer for storing remote PUT request data can be
           located across multiple different caches->list[] */
        for (i=0; i<caches->numLinks && req_len > 0; i++) {
            /* go through all cache pages to find an intersect page with the
               put request */
            ADIO_Offset  start, end;
            cache_md    *md = caches->list + i;

            if (md->fptr != ptr) continue;

            start = MAX(remote_req.disp, md->offset);
            end   = MIN(remote_req.disp + remote_req.count,
                        md->offset + md->numBlocks * block_size);
            if (start >= end) continue; /* no intersect with put request */

            /* list[i] intersects with put request */
            req_len     -= end - start;
            md->isDirty  = 1;
            md->end      = MAX(md->end, end - md->offset);
            if (putBuf == NULL) {
                if (req_len == 0) {
                    /* all requested data are contiguous in cache */
                    putBuf = md->buf + (start - md->offset);
                }
                else { /* requested data is stored non-contiguously */
                    putBuf = (char*) ADIOI_Malloc(remote_req.count);
                    doFreeTmpBuf = 1;
                }
                MPI_Recv(putBuf, remote_req.count, MPI_BYTE, req_node,
                         CACHE_TAG_PUT, ptr->comm, &recv_status);
#ifdef WKL_DEBUG
if (MAX(remote_req.disp,40960) < MIN(49152,remote_req.disp+remote_req.count)) printf("PUT from p%d [45124]=%c req offset=%lld, len=%d (req_len=%d md offset=%lld numBlocks=%d, start=%lld)\n",req_node,md->buf[45124-md->offset],remote_req.disp,remote_req.count,req_len,md->offset,md->numBlocks,start);

{printf("(t) CACHE_TAG_PUT from %d gfid=%d disp=%lld len=%d ---------- done\n",req_node,remote_req.gfid,remote_req.disp,remote_req.count); fflush(stdout);}
#endif
                if (req_len == 0) break; /* break loop i */
            }
            /* only non-contiguous case will run this: copy piece by piece
               from putBuf to md->buf */
            memcpy(md->buf + (start - md->offset),
                   putBuf  + (start - remote_req.disp),
                   end - start);
            if (req_len == 0) break; /* break loop i */
        }
        if (doFreeTmpBuf) ADIOI_Free(putBuf);
        if (req_len != 0) {
            fprintf(stderr,"Error: remote PUT from node %d req_len(%d) != 0 req disp=%lld (block %lld, block_size=%d) count=%d caches->numLinks=%d i=%d\n",req_node,req_len,remote_req.disp,remote_req.disp/block_size,block_size,remote_req.count,caches->numLinks,i);
            printf("Error: caches->list: ");
            for (i=0;i<caches->numLinks;i++)
                printf("[%d](%lld,%lld) ",i,caches->list[i].offset,caches->list[i].offset+caches->list[i].numBlocks * block_size);
            printf("\n");
            printf("Error: ptr->table: ");
            for (i=0;i<ptr->fsize/block_size/ptr->np + 1;i++)
                printf("[%d](%d,%d) ",i,ptr->table[i].owner,ptr->table[i].lock);
            printf("\n");fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }
    else if (remote_req.req_type == CACHE_TAG_GET) {  /*-----------------------*/
        int            doFreeTmpBuf = 0;
        int            req_len      = remote_req.count;
        char *restrict getBuf       = NULL;

        /* the cache buffer for storing remote GET request data can be
           located across multiple different caches->list[] */
        for (i=0; i<caches->numLinks && req_len > 0; i++) {
            /* go through all cache pages to find an intersect page with the
               get request */
            ADIO_Offset  start, end;
            cache_md    *md = caches->list + i;

            if (md->fptr != ptr) continue;

            start = MAX(remote_req.disp, md->offset);
            end   = MIN(remote_req.disp + remote_req.count,
                        md->offset + md->numBlocks * block_size);
            if (start >= end) continue; /* no intersect with put request */

            /* list[i] intersects with put request */
            req_len -= end - start;
            if (getBuf == NULL) {
                if (req_len == 0) {
                    /* all requested data are contiguous in cache */
                    getBuf = md->buf + (start - md->offset);
                    break;  /* break loop i */
                }
                else { /* requested data is located non-contiguously */
                    getBuf = (char*) ADIOI_Malloc(remote_req.count);
                    doFreeTmpBuf = 1;
                }
            }
            /* only non-contiguous case will run this: copy piece by piece
               from md->buf to getBuf */
            memcpy(getBuf  + (start - remote_req.disp),
                   md->buf + (start - md->offset),
                   end - start);
            if (req_len == 0) break;
        }

        /* send back data in a contiguous buffer */
        MPI_Send(getBuf, remote_req.count, MPI_BYTE, req_node, CACHE_TAG_GET,
                 ptr->comm);

        if (doFreeTmpBuf) ADIOI_Free(getBuf);
        if (req_len != 0) {
            fprintf(stderr,"Error: remote GET req_len(%d) != 0\n",req_len);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }
    else if (remote_req.req_type == CACHE_MIGRATE) {  /*-----------------------*/
        migration_remote(ptr, remote_req, req_node);
    }
    else if (remote_req.req_type == CACHE_GET_FSIZE) {  /*---------------------*/
        /* only proc 0 should run this */
        /* reply the file size to the requester */
        MPI_Send(&ptr->fsize, sizeof(ADIO_Offset), MPI_BYTE, req_node,
                 CACHE_GET_FSIZE, ptr->comm);
    }
    else if (remote_req.req_type == CACHE_SET_FSIZE) {  /*---------------------*/
        /* only proc 0 should run this */
        /* once updated, reply the file size to the requester */
        ptr->fsize = MAX(ptr->fsize, remote_req.disp);
        MPI_Send(&ptr->fsize, sizeof(ADIO_Offset), MPI_BYTE, req_node,
                 CACHE_SET_FSIZE, ptr->comm);
    }
    else {
        fprintf(stderr,"Error: no such req_type %d in probe_incoming_cache from proc %d\n", remote_req.req_type, req_node);
#ifdef WKL_DEBUG
{printf("(t) Error proc %d -- gfid=%d req_type=%d disp=%lld count=%d\n",req_node,remote_req.gfid,remote_req.req_type,remote_req.disp,remote_req.count); fflush(stdout);}
#endif
    }
    return 1;
}

/*----< flush_cache_page() >-------------------------------------------------*/
static
void flush_cache_page(void)
{
    int         i, err;
    cache_list *caches = &shared_v.caches;
    static int  fair = 0;

    for (i=0; i<caches->numLinks; i++) {
        int j = (fair + i) % caches->numLinks;

        if (caches->list[j].end     == caches->block_size &&
            caches->list[j].isDirty == 1) {

            ADIO_Offset pos; /* get the current file position */
            pos = sys_lseek(caches->list[j].fptr->file_system,
                            caches->list[j].fptr->fid, 0, SEEK_CUR);

            if (caches->list[j].fptr->do_block_align_io)
                err = sys_aligned_write(caches->list[j].fptr->file_system,
                                        caches->list[j].fptr->fid,
                                        caches->list[j].fptr->fsize,
                                        caches->list[j].buf,
                                        caches->list[j].offset,
                                        caches->list[j].end);
            else
                err = sys_write_at(caches->list[j].fptr->file_system,
                                   caches->list[j].fptr->fid,
                                   caches->list[j].offset,
                                   caches->list[j].buf,
                                   caches->list[j].end,
                                   caches->list[j].fptr->do_locking_io,
                                   0);

            /* restore the original file position */
            sys_lseek(caches->list[j].fptr->file_system,
                      caches->list[j].fptr->fid, pos, SEEK_SET);

            caches->list[j].end     = 0;
            caches->list[j].isDirty = 0;

            fair = j; /* next time start with j, to be fair */
            break;
        }

    }
}

/*----< th_loops() >--------------------------------------------------------*/
static void* th_loops(void *args)
{
    while (1) {
        int       reqType, err, is_req_handled;
        cache_file *ptr;

        /* check if a CACHE request from a remote processor ----------------*/
        /* since one process may open multiple files with different MPI
           communicators, probe must check all open files for incoming
           CACHE requests using different communicators */
        ptr = shared_v.cache_file_list;
        is_req_handled = 0;
        while (ptr != NULL) {
            is_req_handled += probe_incoming_cache(ptr, -1, "th_loops");
            ptr = ptr->next;
            /* pthread_yield(); */
        }

        /* check if there is a CACHE request from local process -------------*/
        reqType = CACHE_REQ_NULL;
        err = MUTEX_TRYLOCK(shared_v.mutex);
        if (err == 0) { /* lock succeed */
            reqType = shared_v.req;
            ptr     = shared_v.fptr;
#if defined(USE_THREAD_IMPL) && (USE_THREAD_IMPL == MPICH_THREAD_IMPL_GLOBAL_MUTEX)
            pthread_mutex_unlock(&MPIR_Process.global_mutex);
#else
            MUTEX_UNLOCK(shared_v.mutex);
#endif
        }
        else { /* mutex try lock failed */
            pthread_yield(); /* give the main thread a chance to complete */
            continue;
        }

        switch (reqType) {
            case CACHE_REQ_NULL:  /* pthread_yield(); */
/* perform only when two-phase flushing is disable
*/
                                if (is_req_handled == 0) flush_cache_page();
                                break;

            case CACHE_FILE_OPEN: cache_file_open();
                                break;

            case CACHE_FILE_CLOSE: cache_file_close();
                                break;

            case CACHE_FILE_WRITE: cache_file_write();
                                break;

            case CACHE_FILE_READ: cache_file_read();
                                break;

            /* mutex lock and unlock are used here is because
               cache_thread_barrier() can be called in cache_file_close() which
               need no COND_SINGAL() to main thread */
            case CACHE_BARRIER: cache_thread_barrier(ptr);
                                MUTEX_LOCK(shared_v.mutex);
                                shared_v.req = CACHE_REQ_NULL;
                                COND_SIGNAL(shared_v.cond);
                                MUTEX_UNLOCK(shared_v.mutex);
                                break;

            case CACHE_TERMINATE: /* a terminate singal for the 2nd thread */
                                MUTEX_LOCK(shared_v.mutex);
                                COND_SIGNAL(shared_v.cond);
                                MUTEX_UNLOCK(shared_v.mutex);
                                pthread_exit((void*)0);
                                return 0;
            default: break;
        }
    }
    return 0;
}

/*----< init_cache() >--------------------------------------------------------*/
int init_cache(void) {
    int err;

    err = pthread_cond_init(&shared_v.cond, NULL);
    assert(err == 0);

    err = MUTEX_INIT(shared_v.mutex);
    assert(err == 0);

    /* create a new thread */
    err = pthread_create(&shared_v.thread_id, NULL, th_loops, 0);
    assert(err == 0);

    /* make sure thread starts before return
    pthread_yield(); */

    return 1;
}

/*----< finalize_cache() >--------------------------------------------------*/
int finalize_cache(void) {
    int err;

    /* thread termination can be done when the last file close: i.e.
       in cache_close(), check if cache_file_list == NULL, if yes,
           in I/O  thread: call pthread_exit()
           in main thread: call pthread_join()
       Here, I keep termination separate just for future improvement case
    */
    MUTEX_LOCK(shared_v.mutex);
    shared_v.req = CACHE_TERMINATE;
    COND_WAIT(shared_v.cond, shared_v.mutex);
    MUTEX_UNLOCK(shared_v.mutex);

    /* waiting for 2nd thread exit */
    err = pthread_join(shared_v.thread_id, NULL);
    assert(err == 0);

    err = MUTEX_DESTORY(shared_v.mutex);
    assert(err == 0);

    err = pthread_cond_destroy(&shared_v.cond);
    assert(err == 0);

    shared_v.req         = CACHE_REQ_NULL;
    shared_v.comm        = 0;
    shared_v.file_system = 0;
    shared_v.fid         = -1;
    shared_v.oflag       = 0;

    return 1;
}


/*----< cache_lseek() >------------------------------------------------------*/
/* this should be called by the I/O thread ONLY to make thread safe: updating
   file point atomically */
ADIO_Offset cache_lseek(cache_file          *fptr,
                      const ADIO_Offset  offset,
                      const int          whence)
{
    ADIO_Offset cur_offset = -1;

    cur_offset = sys_lseek(fptr->file_system, fptr->fid, offset, whence);

    return cur_offset;
}

