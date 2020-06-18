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
double writeTime, readTime, lockWriteTime, lockReadTime;
int hits, miss, empty, unexp_io;
int nReadQueued, nWriteQueued, nReadMigrates, nWriteMigrates;
FILE *ftrace;
int rNTraces, wNTraces, eNTraces, stripe_size, SELF, NPROC;
#endif

#include <unistd.h>
int _system_page_size;  /* from getpagesize() */


/*----< cache_file_open() >---------------------------------------------------*/
/* this subroutine is run by the I/O thread */
int cache_file_open(void)
{
    int         i, oflag;
    cache_file *newFile;

printf("spawn I/O thread calls %s to open file\n",__func__);
    /* allocate and initialize a new file handler ---------------------------*/
    newFile = (cache_file*) ADIOI_Malloc(sizeof(cache_file));
    assert(newFile != NULL);

    MPI_Comm_rank(shared_v.comm, &newFile->rank);
    MPI_Comm_size(shared_v.comm, &newFile->np);
    newFile->fid         = shared_v.fid;
    newFile->gfid        = shared_v.max_gfid;
    newFile->comm        = shared_v.comm;
    newFile->file_system = shared_v.file_system;
    newFile->oflag       = shared_v.oflag;

    newFile->fsize       = 0;
    if (newFile->rank == FSIZE_OWNER) {
        /* only proc FSIZE_OWNER keeps the most recent file size, so every
           read/write will try to update its local fsize with FSIZE_OWNER.
           This is used to prevent reading beyond current file size */
        newFile->fsize = sys_lseek(shared_v.file_system, newFile->fid, 0,
                                   SEEK_END);
        sys_lseek(shared_v.file_system, newFile->fid, 0, SEEK_SET);
    }

    /* allocate global caching status (metadata) table (unique to each file)
       CACHE_FILE_TABLE_CHUNK is defined in cache_thread.h
       table_size should be able to dynamically increase due to the increase
       of the file size ... TODO */
    newFile->table_size = CACHE_FILE_TABLE_CHUNK;
    newFile->table = (block_status*) ADIOI_Malloc(newFile->table_size *
                                                  sizeof(block_status));
    assert(newFile->table != NULL);

    for (i=0; i<newFile->table_size; i++) {
        newFile->table[i].owner         = TABLE_EMPTY;
        newFile->table[i].lock          = TABLE_LOCK_FREE;
        newFile->table[i].lockType      = TABLE_LOCK_FREE;
        newFile->table[i].numReadLocks  = 0;
        newFile->table[i].last          = TABLE_EMPTY;
        newFile->table[i].qsize         = 0;
        newFile->table[i].qcount        = 0;
        newFile->table[i].queue         = NULL;
        newFile->table[i].qType         = NULL;
    }
    /* do_pipeline_io, do_page_migration, do_block_align_io, do_locking_io
       should be an MPI_Info input from user */
    newFile->do_pipeline_io    = 1;

    newFile->do_page_migration = 0;

    newFile->do_block_align_io = 0;
    newFile->fs_ssize = sys_get_block_size(newFile->file_system,newFile->fid);

    /* if FS does not perform client-side caching (e.g. PVFS), there is no
       need to wrap byte-range file locking around all read()/write() */
    newFile->do_locking_io     = 0;
         if (shared_v.file_system == ADIO_PVFS2) newFile->do_locking_io = 0;
    else if (shared_v.file_system == ADIO_NFS)   newFile->do_locking_io = 1;

    newFile->next = NULL;

#ifdef WKL_DEBUG
{
/*
char outFileName[256];
sprintf(outFileName, "trace%d", newFile->rank);
ftrace = fopen(outFileName, "w");
*/
rNTraces = wNTraces = eNTraces = 0;
unexp_io = 0;
SELF = newFile->rank;
NPROC = newFile->np;
if (newFile->do_block_align_io) stripe_size = newFile->fs_ssize;
else                            stripe_size = 1;
}
#endif

    if (shared_v.cache_file_list != NULL) {
        /* if this is NOT the 1st file open, add the new cache_file obj at
           the end of linked list */
        cache_file  *endPtr;
        endPtr = shared_v.cache_file_list;
        while (endPtr->next != NULL) endPtr = endPtr->next;
        endPtr->next = newFile;
    }
    else { /* cache_file_list == NULL, allocate cache pool at 1st file open -*/

        /* Note: cache page pool is shared by all files. Cache page size,
           block_size, can be given through MPI file hint. Its default value
           is set to be the file block size obtained from the underlying
           file system. Once block_size is set, it will not change, even
           a file on a different file system is open. That is, all files
           open successively use the same cache page size. This setting can
           only be done once per MPI program run. Since there is no way to
           know if all files in all processes are closed and cache is a
           shared resource among all files and cache page size should be
           consistent at all time, block_size can only be set once.

           Here, I let all file use the same cache page size for easy
           local cache pool management. In the future, different type files
           may desire different cache page size. block_size, will be moved
           into cache_file structure. In this case, cache pools become per file
           basis and the cache pool management code, cache.c, will be
           more complex. */

        /* if MPI hint did not provide, use file system's block size */
        if (shared_v.caches.block_size == 0)
            shared_v.caches.block_size = newFile->fs_ssize;

        /* if file system could not provide info about block size, use
           default size */
        if (shared_v.caches.block_size == 0)
            shared_v.caches.block_size = CACHE_DEFAULT_CACHE_PAGE_SIZE;

        /* initialize local cache pool -------------------------------------*/
        shared_v.caches.list = (cache_md*) ADIOI_Malloc(CACHE_MD_SEGMENT *
                                                        sizeof(cache_md));
        shared_v.caches.numLinksAlloc  = CACHE_MD_SEGMENT;
        shared_v.caches.numLinks       = 0;
        shared_v.caches.numBlocksAlloc = 0;
        shared_v.caches.upperLimit     = CACHE_CACHE_LIMIT;
        shared_v.cache_file_list       = newFile;
    }

    /* open is a collective call: sync before return to main thread,
       but this synchronization has been done when calling all_reduce_int()
       above. There is no need for calling cache_thread_barrier(newFile);
    */

    shared_v.fptr = newFile;

    /* signal main thread in cache_open() that the process is done */
    MUTEX_LOCK(shared_v.mutex);
    shared_v.req = CACHE_REQ_NULL;
    COND_SIGNAL(shared_v.cond);
    MUTEX_UNLOCK(shared_v.mutex);

    return 1;
}


/*----< cache_open() >--------------------------------------------------------*/
/* this subroutine is called by the main thread */
cache_file* cache_open(const MPI_Comm  comm,
                       const int       file_system,
                       const int       fid,           /* file is already opened */
                       const int       oflag,         /* file open flag */
                       const int       cache_page_size)
{
    static int isFirstCalled = 1;
    int        gfid, max_gfid;

    /* first time to open a file -------------------------------------------*/
    if (isFirstCalled) {
        /* makes sure cache_file_list initialized to NULL. Hereafter,
           cache_file_list should be NULL when all files are closed. */
        shared_v.cache_file_list   = NULL;
        shared_v.max_gfid          = 0;
        /* set block_size if provided through user hint from MPI_Info */
        shared_v.caches.block_size = cache_page_size;
        isFirstCalled              = 0;

#ifdef WKL_DEBUG
writeTime = readTime = lockWriteTime = lockReadTime = 0.0;
hits = miss = empty = unexp_io = 0;
nReadQueued = nWriteQueued = nReadMigrates = nWriteMigrates = 0;
#endif
    }

    /* initialize CACHE thread -----------------------------------------------*/
    if (shared_v.cache_file_list == NULL) {
        /* may not be the true first file open: either first open or
           the open after all previous files are closed */

        _system_page_size = getpagesize();
/* set manually
_system_page_size = 65536;
_system_page_size = 16384;
*/
        shared_v.fptr = NULL;
        shared_v.req  = CACHE_REQ_NULL;
        init_cache();
    }

    /* set up some values to pass to the I/O thread */
    shared_v.comm        = comm;
    shared_v.file_system = file_system;
    shared_v.fid         = fid;
    shared_v.oflag       = oflag;

    /* set up global file id, newFile->gfid, (same across all proc) to be used
       to identify the same file (because file id is only unique locally)
       across all processes.
       Max gfid is used in case multiple files are open with different
       MPI communicators */
    gfid = shared_v.max_gfid;
    gfid++;

    MPI_Allreduce(&gfid, &max_gfid, 1, MPI_INT, MPI_MAX, shared_v.comm);
    shared_v.max_gfid = max_gfid;

    MUTEX_LOCK(shared_v.mutex);

    shared_v.req = CACHE_FILE_OPEN; /* only shared_v.req needs to protected */

    /* let I/O thread open the file and main thread waits for completion */
    COND_WAIT(shared_v.cond, shared_v.mutex);

    MUTEX_UNLOCK(shared_v.mutex);

    return shared_v.fptr;
}

