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
extern double writeTime, readTime, lockWriteTime, lockReadTime;
extern int hits, miss, empty, nunexp_io;
extern int nReadQueued, nWriteQueued, nReadMigrates, nWriteMigrates;
static int numPageAlloc, numBuffers;
#endif

typedef struct {
    ADIO_Offset  offset;
    char        *buf;
    int          len;
} offset_buffer;

typedef struct {
    ADIO_Offset offset;
    int         len;
} offset_len;

static int offset_compare(const void *p1, const void *p2)
{
    ADIO_Offset i = *((ADIO_Offset *)p1);
    ADIO_Offset j = *((ADIO_Offset *)p2);

    /* inscreasing sorting */
    if (i > j) return (1);
    if (i < j) return (-1);
    return (0);
}

static int md_offset_compare(const void *p1, const void *p2)
{
    ADIO_Offset i = ((cache_md *)p1)->offset;
    ADIO_Offset j = ((cache_md *)p2)->offset;

    /* inscreasing sorting */
    if (i > j) return (1);
    if (i < j) return (-1);
    return (0);
}

static int offset_buffer_compare(const void *p1, const void *p2)
{
    /* sort by offset */
    ADIO_Offset i = ((offset_buffer *)p1)->offset;
    ADIO_Offset j = ((offset_buffer *)p2)->offset;
/* sort by buf address
    ADIO_Offset i = (ADIO_Offset)(((offset_buffer *)p1)->buf);
    ADIO_Offset j = (ADIO_Offset)(((offset_buffer *)p2)->buf);
*/
    /* inscreasing sorting */
    if (i > j) return (1);
    if (i < j) return (-1);
    return (0);
}

/*----< two_phase_flushing() >----------------------------------------------*/
static
int two_phase_flushing(cache_file *fptr, cache_list *caches)
{
    cache_list     closeCaches;
    int            i, j, k, rem, avg, nStages;
    int            globalNumPages, numWriteLinks;
    int           *recvNum, *displs;
    ADIO_Offset   *globalOffsets;
    offset_buffer *writeList;
    double         flushT, ioT;

#ifdef WKL_DEBUG
FILE *fp;char filename[16]; sprintf(filename,"%d",fptr->rank); fp=fopen(filename,"w");
for (i=0; i<caches->numLinks; i++) {
if (caches->list[i].fptr == fptr) {
fprintf(fp,"%16lld %16d\n",caches->list[i].offset,fptr->rank);
fprintf(fp,"%16lld %16d\n\n",caches->list[i].offset+caches->list[i].numBlocks*caches->block_size,fptr->rank);
}}
fclose(fp);
    flushT = MPI_Wtime();
#endif

    /* a new cache page list for the closing file */
    closeCaches.numBlocksAlloc = 0;
    closeCaches.numLinks       = 0;
    closeCaches.list = (cache_md*) ADIOI_Malloc(caches->numLinks * sizeof(cache_md));
#ifdef WKL_DEBUG
    numPageAlloc = 0;
    numBuffers   = 0;
#endif
    /* move all cache pages associated with the closing file to closeCaches */
    for (i=0, j=0; i<caches->numLinks; i++) {
        if (caches->list[i].fptr == fptr) {
#ifdef WKL_DEBUG
            numPageAlloc += caches->list[i].numBlocks;
            numBuffers++;
#endif
            if (caches->list[i].isDirty) {
/* TODO: check caches->list[i].end to reduce size (still multiple of block_size) for flushing (using realloc(buf)), but cache cannot be re-used */
                closeCaches.list[closeCaches.numLinks] = caches->list[i];
                closeCaches.list[closeCaches.numLinks].isDirty = caches->list[i].numBlocks * caches->block_size;
                /* isDirty is now used to track the number of bytes in
                   this cache buffer that will be freed */
                closeCaches.numLinks++;
                closeCaches.numBlocksAlloc += caches->list[i].numBlocks;
            }
            else {
                ADIOI_Free(caches->list[i].buf);
            }
            caches->numBlocksAlloc -= caches->list[i].numBlocks;
        }
        else { /* coalesce the cache list for non-closing files */
            if (i != j) caches->list[j] = caches->list[i];
            j++;  /* j is the currect length of caches->list */
        }
    }
    caches->numLinks = j;

#ifdef WKL_DEBUG
printf("%d: caches->numLinks=%d caches->numBlocksAlloc=%d closeCaches.numLinks=%d, closeCaches.numBlocksAlloc=%d closeCaches.[list[%d].numBlocks=%d\n",fptr->rank,caches->numLinks,caches->numBlocksAlloc,closeCaches.numLinks, closeCaches.numBlocksAlloc,closeCaches.numLinks-1,closeCaches.list[closeCaches.numLinks-1].numBlocks);
#endif
    /* sort closeCaches.list based on cache pages' offsets ------------------*/
    qsort(closeCaches.list, closeCaches.numLinks, sizeof(cache_md), md_offset_compare);

    /* the number of offsets to be received from all processes */
    recvNum = (int*) ADIOI_Malloc(fptr->np * sizeof(int));
    MPI_Allgather(&(closeCaches.numBlocksAlloc), 1, MPI_INT,
                  recvNum,                       1, MPI_INT, fptr->comm);

#ifdef WKL_DEBUG
printf("%d: recvNum[] %3d %3d %3d %3d\n",fptr->rank,recvNum[0],recvNum[1],recvNum[2],recvNum[3]);
#endif

    globalNumPages = 0;
    for (i=0; i<fptr->np; i++) globalNumPages += recvNum[i];

    /* displs[] cotains the start indices of recv buffer for each proc */
    displs  = (int*) ADIOI_Malloc(fptr->np * sizeof(int));
    j = fptr->np - 1;
    displs[j] = globalNumPages - recvNum[j];
    for (i=j-1; i>=0; i--) displs[i] = displs[i+1] - recvNum[i];

#ifdef WKL_DEBUG
printf("%d: displs[] %3d %3d %3d %3d\n",fptr->rank,displs[0],displs[1],displs[2],displs[3]);
#endif
    /* get all cache page offsets from all processes */
    globalOffsets = (ADIO_Offset*) ADIOI_Malloc((globalNumPages) * sizeof(ADIO_Offset));

    /* prepare local send buffer for offsets */
    for (i=0, j=0; i<closeCaches.numLinks; i++) {
        ADIO_Offset offset = closeCaches.list[i].offset;
        for (k=0; k<closeCaches.list[i].numBlocks; k++) {
            globalOffsets[displs[fptr->rank]+j] = offset;
            offset += caches->block_size;
            j++;
        }
    }

    /* gather all cache offsets from all other processes */
    MPI_Allgatherv(globalOffsets+displs[fptr->rank], recvNum[fptr->rank],
                   MPI_LONG_LONG, globalOffsets, recvNum, displs,
                   MPI_LONG_LONG, fptr->comm);
    ADIOI_Free(displs);
    ADIOI_Free(recvNum);

    /* sort globalOffsets[] based on the offsets */
    qsort(globalOffsets, globalNumPages, sizeof(ADIO_Offset), offset_compare);

    /* see if globalNumPages is divisible by np */
    rem = globalNumPages % fptr->np;
    avg = globalNumPages / fptr->np;
    numWriteLinks = 0;
    writeList = (offset_buffer*) ADIOI_Malloc((avg+1) * sizeof(offset_buffer));

    nStages = 1;
    while (nStages < fptr->np) nStages *= 2;

    for (i=0; i<nStages; i++) {
        int          dest = fptr->rank ^ i;
        int          numSend, numRecv, sIndex, eIndex;
        ADIO_Offset  sOffset, eOffset; /* dest's domain */
        offset_len  *send_offset_len, *recv_offset_len;
        MPI_Request *commReq, *sendReq, *recvReq;
        MPI_Status   status, *statusV;

        if (dest >= fptr->np) continue;

        /* evenly divide the cache page domain among all clients */
        sIndex = dest * avg;
        if (rem > 0) {
            if (dest < rem) sIndex += dest;
            else            sIndex += rem;
        }
        eIndex = sIndex + avg;
        if (rem > 0 && dest < rem) eIndex++;

        /* [sOffset ... eOffset) is the range of caches for dest */
        sOffset = globalOffsets[sIndex];
        eOffset = globalOffsets[eIndex-1] + caches->block_size;

        /* find the index range in closeCaches.list[] which cache pages intersect with dest's range */
        sIndex = -1;
        for (eIndex=0; eIndex<closeCaches.numLinks; eIndex++) {
            cache_md    *md = &(closeCaches.list[eIndex]);
            ADIO_Offset  start, end;

            if (eOffset <= md->offset) break; /* loop eIndex */
            if (md->buf == NULL) continue;

            /* check if intersect */
            start = MAX(sOffset, md->offset);
            end   = MIN(eOffset, md->offset + md->numBlocks * caches->block_size);
            if (start < end && sIndex == -1) sIndex = eIndex;
        }
        /* closeCaches.list[sIndex...eIndex-1] has data belong to dest */
        if (sIndex >= 0) numSend = eIndex - sIndex;
        else             numSend = 0;

        /* take care of closeCaches.list[] that belong to self */
        if (dest == fptr->rank) {
            if (numSend == 0) continue; /* for loop i */
            for (j=sIndex; j<eIndex; j++) {
                cache_md    *md = &(closeCaches.list[j]);
                ADIO_Offset  start, end;

                /* intersect range is [start ... end) */
                start = MAX(sOffset, md->offset);
                end   = MIN(eOffset, md->offset + md->numBlocks * caches->block_size);
                md->isDirty -= end - start;
                if (start == md->offset && md->isDirty == 0) {
                    /* move md->buf to the end of writeList */
                    if (md->numBlocks * caches->block_size > end-start)
                        /* md->buf's tail belong to another proc, but is already processed */
                        md->buf = (char*) ADIOI_Realloc(md->buf, end-start);
                    writeList[numWriteLinks].buf    = md->buf;
                    writeList[numWriteLinks].offset = md->offset;
                    md->buf = NULL;
                }
                else {
                    /* either md->buf's front belong to another proc or
                       md->buf's tail belongs to another and not yet free-able */
                    writeList[numWriteLinks].buf    = (char*) ADIOI_Malloc(end-start);
                    writeList[numWriteLinks].offset = start;
                    if (md->isDirty == 0) {
                        /* this must be the case md->buf's front belong to another proc */
                        ADIOI_Free(md->buf);
                        md->buf = NULL;
                    }
                }
                writeList[numWriteLinks].len       = end-start;
                numWriteLinks++;
            }
            continue; /* of loop i */
        } /* if (dest == fptr->rank) */

        /* from now on dest != fptr->rank */
        MPI_Sendrecv(&numSend, 1, MPI_INT, dest, 0,
                     &numRecv, 1, MPI_INT, dest, 0, fptr->comm, &status);

        if (numSend == 0 && numRecv == 0) continue; /* loop i */

        /* from now on, numSend + numRecv > 0 */
        statusV = (MPI_Status*)  ADIOI_Malloc((numSend + numRecv) * sizeof(MPI_Status));
        commReq = (MPI_Request*) ADIOI_Malloc((numSend + numRecv) * sizeof(MPI_Request));
        sendReq = commReq;
        recvReq = commReq + numSend;

        if (numSend > 0) {  /* prepare offset-length message for dest */
            send_offset_len = (offset_len*) ADIOI_Malloc(numSend * sizeof(offset_len));
            for (j=sIndex; j<eIndex; j++) {
                ADIO_Offset  start, end;
                cache_md    *md = &(closeCaches.list[j]);

                start = MAX(sOffset, md->offset);
                end   = MIN(eOffset, md->offset + md->numBlocks * caches->block_size);
                send_offset_len[j-sIndex].offset = start;
                send_offset_len[j-sIndex].len    = end - start;
                md->isDirty -= send_offset_len[j-sIndex].len;
                /* post send requests for sending cache pages over to dest */
                MPI_Isend(md->buf+(start - md->offset), send_offset_len[j-sIndex].len,
                          MPI_BYTE, dest, j-sIndex+i, fptr->comm, &sendReq[j-sIndex]);
            }
        }
        if (numRecv > 0)
            recv_offset_len = (offset_len*) ADIOI_Malloc(numRecv * sizeof(offset_len));

        MPI_Sendrecv(send_offset_len, numSend*sizeof(offset_len), MPI_BYTE, dest, 0,
                     recv_offset_len, numRecv*sizeof(offset_len), MPI_BYTE, dest, 0, fptr->comm, &status);

        /* post receive requests */
        if (numRecv > 0) {
#define _COMBINE_RECV_BUFFERS_
#ifdef _COMBINE_RECV_BUFFERS_
            char *buf;
            int   g, h = 0;
            /* Combine contiguous buffers into single ones as possible */
            while (h < numRecv) {
                writeList[numWriteLinks].offset    = recv_offset_len[h].offset;
                writeList[numWriteLinks].len       = recv_offset_len[h].len;
                /* check for contiguous cache pages */
                for (j=h+1; j<numRecv; j++) {
                    if (recv_offset_len[j].offset == writeList[numWriteLinks].offset + writeList[numWriteLinks].len)
                        writeList[numWriteLinks].len += recv_offset_len[j].len;
                    else
                        break;
                }
                /* allocate a single large memory space */
                buf = (char*) ADIOI_Malloc(writeList[numWriteLinks].len);
                writeList[numWriteLinks].buf = buf;
                /* receive messages from h to j-1 */
                for (g=h; g<j; g++) {
                    MPI_Irecv(buf, recv_offset_len[g].len, MPI_BYTE, dest, g+i, fptr->comm, &recvReq[g]);
                    buf += recv_offset_len[g].len;
                }
                numWriteLinks++;
                h = j;
            }
#else
            int n;
            for (n=0; n<numRecv; n++) {
                char *buf = (char*) ADIOI_Malloc(recv_offset_len[n].len);
                MPI_Irecv(buf, recv_offset_len[n].len, MPI_BYTE, dest, n+i, fptr->comm, &recvReq[n]);
                writeList[numWriteLinks].buf    = buf;
                writeList[numWriteLinks].offset = recv_offset_len[n].offset;
                writeList[numWriteLinks].len    = recv_offset_len[n].len;
                numWriteLinks++;
            }
#endif
        }
        MPI_Waitall(numSend + numRecv, commReq, statusV);
        ADIOI_Free(commReq);
        ADIOI_Free(statusV);
        if (numSend > 0) ADIOI_Free(send_offset_len);
        if (numRecv > 0) ADIOI_Free(recv_offset_len);

        /* free up cache page buffer that already migrated */
        if (numSend > 0) {
            for (j=sIndex; j<eIndex; j++) {
                if (closeCaches.list[j].isDirty == 0) {
                    ADIOI_Free(closeCaches.list[j].buf);
                    closeCaches.list[j].buf = NULL;
                }
            }
        }
    }

    /* closeCaches.list should be all empty now */
    for (i=0; i<closeCaches.numLinks; i++) if (closeCaches.list[i].buf != NULL) printf("Error: closeCaches.list[%d].buf != NULL numBlocks=%d isDirty=%d offset=%lld\n",i,closeCaches.list[i].numBlocks,closeCaches.list[i].isDirty,closeCaches.list[i].offset);

    ADIOI_Free(globalOffsets);
    ADIOI_Free(closeCaches.list);

    /* sort all buffers in writeList based on the offsets */
    qsort(writeList, numWriteLinks, sizeof(offset_buffer), offset_buffer_compare);

#ifdef WKL_DEBUG
{extern int rNTraces,wNTraces,eNTraces;
printf("%2d: before two-phase cache_size=%dKB fs_ssize=%dKB align_io=%d nRead=%d nWrite=%d nPageAlloc=%d numEvict=%d\n",
fptr->rank, shared_v.caches.block_size/1024,fptr->fs_ssize/1024,fptr->do_block_align_io,rNTraces,wNTraces,numPageAlloc,eNTraces);}
printf("---------------- flushing to pvfs ---------------------------\n");

ioT = MPI_Wtime();
j = k = 0;
#endif
    /* Now, all cache pages are globally aligned. proceed to file flush */
    for (i=0; i<numWriteLinks; i++) {
#ifdef WKL_DEBUG
j += writeList[i].len / caches->block_size;
k++;
#endif
        if (fptr->do_block_align_io)
            sys_aligned_write(fptr->file_system, fptr->fid,
                              fptr->fsize,
                              writeList[i].buf,
                              writeList[i].offset,
                              writeList[i].len);
        else
            sys_write_at(fptr->file_system,
                         fptr->fid,
                         writeList[i].offset,
                         writeList[i].buf,
                         writeList[i].len,
                         fptr->do_locking_io,
                         0);
        ADIOI_Free(writeList[i].buf);
/*
printf("%2d %lld %d\n",fptr->rank,writeList[i].offset,writeList[i].len);
fprintf(fp,"%2d w %lld %d\n",fptr->rank,writeList[i].offset,writeList[i].len);
*/
    }
#ifdef WKL_DEBUG
/*
fclose(fp);
*/
/*
sys_fsync(fptr->file_system, fptr->fid);
*/
ioT    = MPI_Wtime() - ioT;
flushT = MPI_Wtime() - flushT;

if (j>0) printf("%2d: two-phase flushing = %6.2f sec (IO part = %6.2f sec, numPages=%d numBuffers=%d) start=%16lld end=%16lld\n",fptr->rank,flushT,ioT,j,k,writeList[0].offset,writeList[numWriteLinks-1].offset+writeList[numWriteLinks-1].len);
else printf("%2d: two-phase flushing = %6.2f sec (IO part = %6.2f sec, numPages=%d numBuffers=%d) start=XXX end=XXX\n",fptr->rank,flushT,ioT,j,k);
#endif
    ADIOI_Free(writeList);

    return 1;
}

/*----< cache_file_close() >--------------------------------------------------*/
/* this subroutine is run by the I/O thread */
int cache_file_close(void) {
    int         i, j, err=-1;
    cache_file   *fptr;
    cache_list *caches = &shared_v.caches;
    double      flushT;

#ifdef WKL_DEBUG
    unexp_io = 0;

FILE *fp;char filename[16]; sprintf(filename,"%d",shared_v.fptr->rank); fp=fopen(filename,"w");
#endif

printf("spawn I/O thread calls %s to close file\n",__func__);

    fptr = shared_v.fptr;

    cache_thread_barrier(fptr);  /* wait all remote req to this file complete */

    /* find the file to be closed from cache_file_list linked list */
    if (shared_v.cache_file_list == fptr) {  /* is the head of list */
        shared_v.cache_file_list = shared_v.cache_file_list->next;
    }
    else {
        cache_file *ptr = shared_v.cache_file_list;
        while (ptr->next != NULL && ptr->next != fptr) ptr = ptr->next;
        if (ptr->next == NULL) {
            fprintf(stderr,
            "Error: invalid global file id %d (cache_file_close)\n", fptr->gfid);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        ptr->next = ptr->next->next;
    }

#ifdef WKL_DEBUG
    {
        double len;
        int    numPageAlloc = 0;
        for (i=0; i<caches->numLinks; i++)
            if (caches->list[i].fptr == fptr)
                numPageAlloc += caches->list[i].numBlocks;
        len  = (double)numPageAlloc * caches->block_size;
        len /= 1048576;
        printf("Cache space allocated = %10.2f MB\n", len);
    }
#endif

/*
#define _USE_TWO_PHASE_FLUSHING_
*/
#ifdef _USE_TWO_PHASE_FLUSHING_
    two_phase_flushing(fptr, caches);
#else
#ifdef WKL_DEBUG
    flushT = MPI_Wtime();
#endif
    /* sort caches->list based on offsets ----------------------------------*/
    qsort(caches->list, caches->numLinks, sizeof(cache_md), md_offset_compare);

    /* flush out the dirty cache for this file -----------------------------*/
    j = 0;
#ifdef WKL_DEBUG
    numPageAlloc = 0;
    numBuffers   = 0;
#endif
    for (i=0; i<caches->numLinks; i++) {
        if (caches->list[i].fptr == fptr) {
#ifdef WKL_DEBUG
            numPageAlloc += caches->list[i].numBlocks;
            numBuffers++;

fprintf(fp,"%16lld %16d\n",caches->list[i].offset,fptr->rank);
fprintf(fp,"%16lld %16d\n\n",caches->list[i].offset+caches->list[i].numBlocks*caches->block_size,fptr->rank);
#endif
            if (caches->list[i].isDirty) { /* only need to flush dirty data */

#ifdef WKL_DEBUG
if (caches->list[i].end % caches->block_size > 0) printf("[%d]: not aligned with block_size end=%d -> %d\n",i,caches->list[i].end,caches->list[i].end + caches->block_size - caches->list[i].end % caches->block_size);
#endif
                if (fptr->do_block_align_io)
                    err = sys_aligned_write(fptr->file_system, fptr->fid,
                                            fptr->fsize,
                                            caches->list[i].buf,
                                            caches->list[i].offset,
                                            caches->list[i].end);
                else
                    err = sys_write_at(fptr->file_system,
                                       fptr->fid,
                                       caches->list[i].offset,
                                       caches->list[i].buf,
                                       caches->list[i].end,
                                       fptr->do_locking_io,
                                       0);
#ifdef WKL_DEBUG
if (caches->list[i].offset<=514600 && 514601 <= caches->list[i].offset+caches->list[i].end)printf("CLOSE [514600]=%c\n",caches->list[i].buf[514600-caches->list[i].offset]);

if (err == -1) printf("Error: sys_write_at() offset=%lld len=%d\n",caches->list[i].offset,caches->list[i].end);
#endif
            }
            ADIOI_Free(caches->list[i].buf);
            caches->numBlocksAlloc -= caches->list[i].numBlocks;
        }
        else {  /* coalesce cache linked list, move successors forward */
            if (i > j) caches->list[j] = caches->list[i];
            j++;
        }
    }
    caches->numLinks = j;
    /* fsync(fptr->fid); Do we need fsync() here? */

#ifdef WKL_DEBUG
    flushT = MPI_Wtime() - flushT ;
    printf("%2d: Time for data flushing = %6.2f sec\n",fptr->rank,flushT);
    fclose(fp);
#endif
#endif

    if (fptr->do_block_align_io) {
        /* because align write may increase file size, we need to truncate
           the file to its real size */
        cache_thread_barrier(fptr);  /* wait all file flushing to complete */
        if (fptr->rank == FSIZE_OWNER) {
            if ((fptr->oflag & O_WRONLY) || (fptr->oflag & O_RDWR))
                sys_ftruncate(fptr->file_system, fptr->fid, fptr->fsize);
        }
    }

    /* system file close call is done in ad_close.c */
    err = 1;

#ifdef WKL_DEBUG
printf("READ: lockTime=%5.2f readTime=%5.2f WRITE: lockTime=%5.2f writeTime=%5.2f\n",lockReadTime,readTime,lockWriteTime,writeTime);
printf("hits=%3d, miss=%3d, empty=%3d (READ: nQueued=%3d nMigrates=%3d) (WRITE: nQueued=%3d nMigrates=%3d)\n",hits,miss,empty,nReadQueued,nReadMigrates,nWriteQueued,nWriteMigrates);
    {
        int  maxIndx = 0;
        char stdout_str[1024];
        sprintf(stdout_str,"(t) table[*] =");
        for (i=0; i<fptr->table_size; i++) {
            if (fptr->table[i].owner != TABLE_EMPTY) {
                char str[32];
                sprintf(str," [%2d] (o=%d, k=%d, l=%d)", i*fptr->np+fptr->rank,
                        fptr->table[i].owner, fptr->table[i].lock,
                        fptr->table[i].last);
                maxIndx = i*fptr->np+fptr->rank;
                if (strlen(stdout_str) < 1024) strcat(stdout_str, str);
            }
        }
        printf("%s maxIndx=%d\n",stdout_str,maxIndx);
        fflush(stdout);
    }
{extern FILE *ftrace; extern int rNTraces,wNTraces,eNTraces; fclose(ftrace); printf("%2d: numReadBlocks=%d numWriteBlocks=%d numPageAlloc=%d numEvictPages=%d\n",fptr->rank,rNTraces,wNTraces,numPageAlloc,eNTraces);}
{extern int rNTraces,wNTraces,eNTraces; printf("%2d: numReadBlocks=%d numWriteBlocks=%d numPageAlloc=%d numEvictPages=%d\n",fptr->rank,rNTraces,wNTraces,numPageAlloc,eNTraces);}
{extern FILE *ftrace; fclose(ftrace);}

{extern int rNTraces,wNTraces,eNTraces;
printf("%2d: cache_size=%dKB fs_ssize=%dKB align_io=%d lock_io=%d nRead=%d nWrite=%d nPageAlloc=%d numBuffers=%d numEvict=%d\n",
fptr->rank, shared_v.caches.block_size/1024,fptr->fs_ssize/1024,fptr->do_block_align_io,fptr->do_locking_io,rNTraces,wNTraces,numPageAlloc,numBuffers,eNTraces);}
#endif
    /* free up the space allocated for the global file metadata table ------*/
    for (i=0; i<fptr->table_size; i++) {
        if (fptr->table[i].qsize > 0) {
            ADIOI_Free(fptr->table[i].queue);
            ADIOI_Free(fptr->table[i].qType);
        }
    }
    ADIOI_Free(fptr->table);
    ADIOI_Free(fptr);

    if (shared_v.cache_file_list == NULL) {
        /* free cache space if last file close -----------------------------*/
        if (caches->numLinks != 0 || caches->numBlocksAlloc != 0)
            fprintf(stderr,
                    "Error: caches->numLinks(%d), numBlocksAlloc(%d) !=0\n",
                    caches->numLinks,caches->numBlocksAlloc);
        ADIOI_Free(caches->list);
    }

    /* signal main thread in cache_open() that the process is done */
    MUTEX_LOCK(shared_v.mutex);
    shared_v.req = CACHE_REQ_NULL;
    shared_v.err = err;
    COND_SIGNAL(shared_v.cond);
    MUTEX_UNLOCK(shared_v.mutex);

    return 1;
}


/*----< cache_close() >-------------------------------------------------------*/
/* this subroutine is called by the main thread */
int cache_close(cache_file *fptr) {

    MUTEX_LOCK(shared_v.mutex);
    shared_v.req  = CACHE_FILE_CLOSE;
    shared_v.fptr = fptr;

    COND_WAIT(shared_v.cond, shared_v.mutex);

    shared_v.req  = CACHE_REQ_NULL;
    MUTEX_UNLOCK(shared_v.mutex);

    if (shared_v.cache_file_list == NULL) {
        /* if no open file left, terminate the thread */
        finalize_cache();
    }

    return shared_v.err;
}

