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

#ifndef _H_CACHE_THREAD
#define _H_CACHE_THREAD

#include <stdio.h>
#ifndef __USE_GNU
#define __USE_GNU
#endif
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <stdarg.h>
#include <assert.h>


#include <mpi.h>

#ifndef restrict
#define restrict __restrict
#endif

/* types of requests, also used as message tags in MPI communication */
#define CACHE_REQ_NULL           0

#define CACHE_TAG_GET          1002001
#define CACHE_TAG_PUT          1002002
#define CACHE_BARRIER          1002003
#define CACHE_TERMINATE        1002004
#define CACHE_ALL_REDUCE       1002005

#define CACHE_READ_LOCK        1002006
#define CACHE_WRITE_LOCK       1002007
#define CACHE_GRANT_LOCK       1002008
#define CACHE_TABLE_UNLOCK     1002009
#define CACHE_TABLE_UNLOCK_V   1002010
#define CACHE_TABLE_RESET      1002011
#define CACHE_TABLE_TRY_LOCK   1002012

#define CACHE_FILE_OPEN        1002013
#define CACHE_FILE_CLOSE       1002014
#define CACHE_FILE_READ        1002015
#define CACHE_FILE_WRITE       1002016
#define CACHE_GET_FSIZE        1002017
#define CACHE_SET_FSIZE        1002018

#define CACHE_MIGRATE             1002019
#define CACHE_MIGRATE_INFO        1002020
#define CACHE_TABLE_UNLOCK_SWITCH 1002021

#define CACHE_TAG_REQ          1002100   /* this should be the largest one, since
                                       CACHE_TAG_REQ + gfid is the message tag
                                       to tell apart the req to different files
                                    */

/* the followings are used for global cache metadata ownership */
#define TABLE_EMPTY          -1
#define TABLE_END            -2
#define TABLE_LOCK_SELF      -3

/* TABLE_LOCK_FREE is used for table[].lock */
#define TABLE_LOCK_FREE      -1
#define TABLE_LOCKED         -1

/* the proc id that holds the up-to-date file size, can be any proc
   to reduce communication traffic to 0 */
#define FSIZE_OWNER           0


/* cache_req_msg is used to convey the status of requests */
typedef struct {
    int         gfid;      /* global file descriptor */
    int         req_type;
    ADIO_Offset disp;      /* displacement (offset) of the block */
    int         count;     /* number of bytes to be accessed, assuming
                              count < 2^31 */
} cache_req_msg;

/* unlock_msg is used to convey different types of unlock in a single msg */
typedef struct {
    int  indx;      /* local table index */
    int  unlockType;
} unlock_msg;

/* max number of table entries for global cache metadata */
#define CACHE_FILE_TABLE_CHUNK 1024

typedef struct {
    int owner;    /* the rank id of the owner processor */
    int lock;     /* the proc id locks the block */
    int last;     /* TABLE_EMPTY or the rank id lastest locked this block */
} lock_status;

typedef struct {
    int  owner;    /* TABLE_EMPTY or the rank id of the owner processor */
    int  lock;     /* TABLE_LOCK_FREE or the proc id locks the page */
    int  lockType; /* CACHE_READ_LOCK (shared) or CACHE_WRITE_LOCK (exclusive) */
    int  numReadLocks;  /* number of clients sharing the lock */
    int  last;     /* TABLE_EMPTY or the rank id lastest locked this block */

    int  qsize;   /* length of space allocated for the queue */
    int  qcount;  /* the number of processors waiting in the queue */
    int *queue;   /* [size]: lis of rank ids waiting for the lock */
    int *qType;   /* [size]: type of locks: CACHE_READ_LOCK or CACHE_WRITE_LOCK */
} block_status;

typedef struct _cache_file {
    int           fid;   /* file id return by UNIX open() */
    int           gfid;  /* global id same in all processes used in CACHE */
    ADIO_Offset   fsize; /* current file size */
    int           file_system;
    int           fs_ssize; /* file system stripe size */
    int           oflag; /* for truncate file size if align I/O is used */

    int           table_size;
    block_status *table;       /* [table_size]: caching status table */

    /* information of the processor group particpated in the CACHE */
    int           rank;       /* self proc id */
    int           np;         /* number of proc in comm */
    MPI_Comm      comm;       /* MPI communicator */

    /* a optimization flag that determines whether perform pipeline lock for
       read and write operations -- shall be able to set value by user,
       like through MPI_info */
    int do_pipeline_io;         /* default 0 */

    /* to indicate if byte-range locking is required for each read/write
       call. Some file systems perform its own client-side caching. Byte-
       rang locking flush out cached data to prevent incoherence */
    int do_locking_io;          /* default 1 */

    /* align system read/write amount to be multiple of block_size.
       Therefore, all reads/writes are aligned with block_size boundary */
    int do_block_align_io;      /* default 1 */

    /* perform cache page migration if a remote cache page is accessed by
       the same process consecutively twice */
    int do_page_migration;      /* default 1 */

    /* linked list to next opened file */
    struct _cache_file  *next;
} cache_file;


/* metadata for each cache page */
typedef struct {
    /* int       fd;           file descriptor (replaced by fptr) */
    cache_file    *fptr;
    int          isDirty;   /* 0 or 1 */
    int          numBlocks; /* number of pages (blocks) */
    char        *buf;       /* cache buffer */
    ADIO_Offset  offset;    /* offset of the page to the file */
    int          end;       /* end byte (high watermark) of buf that are
                               dirty, assuming allocated buf size < 2^31 */
    double       reference; /* time stamp: to determine if evict */
} cache_md;

typedef struct {
    cache_md *list;
    int       numLinks;        /* no. of linked list of cache_md objects */
    int       numLinksAlloc;   /* no. of linked list of cache_md objects */
    int       numBlocksAlloc;  /* no. total cache pages currently allocated */
    int       upperLimit;      /* max total cache pages allowed */

    /* cache page size, can be set to file system block size, must be
       initialized to zero at first file open time, its value should be able
       to be provided by user through MPI_Info, its default size is set to
       the file system file block size */
    int       block_size;
} cache_list;

/* default cach page size if not from user input or system block size */
#define CACHE_DEFAULT_CACHE_PAGE_SIZE 65536

/* upper limit of cache can be allocated locally = 128 MB */
#define CACHE_CACHE_LIMIT   33554432
/*
#define CACHE_CACHE_LIMIT   536870912
#define CACHE_CACHE_LIMIT   268435456
#define CACHE_CACHE_LIMIT   134217728
#define CACHE_CACHE_LIMIT   67108864
#define CACHE_CACHE_LIMIT   4194304
#define CACHE_CACHE_LIMIT   2097152
#define CACHE_CACHE_LIMIT   65536
#define CACHE_CACHE_LIMIT   32768
*/

#define CACHE_MD_SEGMENT 1024


typedef struct {
    pthread_t        thread_id;
    pthread_mutex_t  mutex;
    pthread_cond_t   cond;      /* condition for general thread operation */
    pthread_attr_t   attr;      /* thread attribute */

    /* variables for main thread to communicate with I/O thread */
    int              req;       /* request type, when main thread communicates
                                   with 2nd thread */
    cache_file        *fptr;      /* requesting file pointer */

    /* attribute for file open and close */
    int              max_gfid;
    int              file_system;
    int              fid;
    int              oflag;
    MPI_Comm         comm;

    /* read/write buffer info */
    char            *buf;
    int              len;       /* assume < 2^31 */
    int              err;       /* return error code */
    ADIO_Offset      offset;    /* current file pointer offset */

    /* the followings are used by the I/O thread only */
    cache_list       caches;    /* a local cache pool (shared by all files) */

    cache_file        *cache_file_list;  /* linked list for all opened files */

} thread_shared_varaibles;  /* used for communication between 2 threads */


/* GLOBAL variables --------------------------------------------------------*/
/* variables that used to communicate between main and I/O threads */
extern thread_shared_varaibles shared_v;
extern int _system_page_size;

/* defined MACRO -----------------------------------------------------------*/
#ifndef MAX
#define MAX(x, y) ((x > y) ? (x) : (y))
#endif

#ifndef MIN
#define MIN(x, y) ((x < y) ? (x) : (y))
#endif

// #include "mpiimpl.h"
// #include "mpir_process.h"
// extern MPICH_PerProcess_t MPIR_Process;
// extern MPIR_Process_t MPIR_Process;

/*
#if (MPICH_THREAD_LEVEL == MPI_THREAD_MULTIPLE)
*/
#if defined(USE_THREAD_IMPL) && (USE_THREAD_IMPL == MPICH_THREAD_IMPL_GLOBAL_MUTEX)
#define MUTEX_INIT(a)    0
#define MUTEX_LOCK(a)
#define MUTEX_TRYLOCK(a) pthread_mutex_trylock(&MPIR_Process.global_mutex)
#define MUTEX_UNLOCK(a)
#define MUTEX_DESTORY(a) 0
#define COND_WAIT(a,b)   pthread_cond_wait(&(a), &MPIR_Process.global_mutex)
#define COND_SIGNAL(a)  {                                \
    pthread_mutex_lock(&MPIR_Process.global_mutex);      \
    pthread_cond_signal(&(a));                           \
    pthread_mutex_unlock(&MPIR_Process.global_mutex);    \
}
#else
#define MUTEX_INIT(a)    pthread_mutex_init(&(a), NULL)
#define MUTEX_LOCK(a)    pthread_mutex_lock(&(a))
#define MUTEX_TRYLOCK(a) pthread_mutex_trylock(&(a))
#define MUTEX_UNLOCK(a)  pthread_mutex_unlock(&(a))
#define MUTEX_DESTORY(a) pthread_mutex_destroy(&(a))
#define COND_WAIT(a,b)   pthread_cond_wait(&(a), &(b))
#define COND_SIGNAL(a)   pthread_cond_signal(&(a));
#endif


#define POLLING(req,status,location) {                                        \
    int isDone;                                                               \
    MPI_Test(&(req), &isDone, &(status));                                     \
    while (isDone == 0) {                                                     \
        cache_file *ptr = shared_v.cache_file_list;                               \
        while (ptr != NULL) {                                                 \
            probe_incoming_cache(ptr, -1, location);                            \
            ptr = ptr->next;                                                  \
        }                                                                     \
        MPI_Test(&(req), &isDone, &(status));                                 \
    }                                                                         \
}

#define WAITALL(num,req,status,location) {                                    \
    int isDone;                                                               \
    MPI_Testall(num, req, &isDone, status);                                   \
    while (isDone == 0) {                                                     \
        cache_file *ptr = shared_v.cache_file_list;                               \
        while (ptr != NULL) {                                                 \
            probe_incoming_cache(ptr, -1, location);                            \
            ptr = ptr->next;                                                  \
        }                                                                     \
        MPI_Testall(num, req, &isDone, status);                               \
    }                                                                         \
}

#define INSERT_LOCK_QUEUE(fptr,id,reqNode,reqType) {                          \
    if ((fptr)->table[id].qsize == 0) {  /* queue never been used */          \
        (fptr)->table[id].queue = (int*)ADIOI_Malloc((fptr)->np *sizeof(int));\
        (fptr)->table[id].qType = (int*)ADIOI_Malloc((fptr)->np *sizeof(int));\
        (fptr)->table[id].qsize  = (fptr)->np;                                \
        (fptr)->table[id].qcount = 0;                                         \
    }                                                                         \
    else if ((fptr)->table[id].qcount % (fptr)->np == 0) {                    \
        /* queue is full, need to augment its space, this should not happen   \
           because a proc cannot be in the queue twice for the same block */  \
        (fptr)->table[id].qsize += (fptr)->np;                                \
        (fptr)->table[id].queue = (int*)ADIOI_Realloc((fptr)->table[id].queue,\
                                       (fptr)->table[id].qsize*sizeof(int));  \
        (fptr)->table[id].qType = (int*)ADIOI_Realloc((fptr)->table[id].qType,\
                                       (fptr)->table[id].qsize*sizeof(int));  \
    }                                                                         \
    (fptr)->table[id].queue[(fptr)->table[id].qcount] = reqNode;              \
    (fptr)->table[id].qType[(fptr)->table[id].qcount] = reqType;              \
    (fptr)->table[id].qcount++;                                               \
}

#define ADD_METADATA_TO_CACHES_LIST(md) {                                     \
    /* add this new metadata md to the end of caches->list */                 \
    if (caches->numLinks == caches->numLinksAlloc) {                          \
        /* extend allocation of cache.list array size */                      \
        caches->numLinksAlloc += CACHE_MD_SEGMENT;                            \
        caches->list = (cache_md*) ADIOI_Realloc(caches->list,                \
                                  caches->numLinksAlloc * sizeof(cache_md));  \
    }                                                                         \
    caches->list[caches->numLinks] = *(md);                                   \
    caches->numBlocksAlloc += (md)->numBlocks;                                \
    caches->numLinks++;                                                       \
}

/* function declarations ----------------------------------------------------*/
int probe_incoming_cache(cache_file*, int, const char*);

cache_md* cache_alloc(const int);

int init_cache(void);
int finalize_cache(void);

int cache_thread_barrier(cache_file*);
int cache_barrier(cache_file*);

int       cache_file_open(void);
cache_file* cache_open(const MPI_Comm, const int, const int, const int, const int);

int cache_file_close(void);
int cache_close(cache_file*);

/* functions lseek, ftruncate, fsync should be called by the I/O thread
   due to thread-safety, main thread should not call them */
ADIO_Offset cache_lseek(cache_file*, const ADIO_Offset, const int);
ADIO_Offset cache_ftruncate(cache_file*, const ADIO_Offset);
ADIO_Offset cache_fsync(cache_file*);

ADIO_Offset sys_lseek(const int, int, const ADIO_Offset, const int);
int sys_ftruncate(const int, int, const ADIO_Offset);
int sys_fsync(const int, int);

int cache_file_write(void);
int cache_write(cache_file*, char*, const ADIO_Offset, const int);

int cache_file_read(void);
int cache_read(cache_file*, char*, const ADIO_Offset, const int);

int  sys_read_at(const int, int, const ADIO_Offset, void*, const int,const int,const int);
int sys_write_at(const int, int, const ADIO_Offset, void*, const int,const int,const int);
int sys_get_block_size(const int, int);
int sys_aligned_write(const int, const int, const int, char *restrict, ADIO_Offset, int);
int sys_aligned_read(const int, const int, const int, char *restrict, ADIO_Offset, int);


int metadata_lock(cache_file*, const int, const int, int, lock_status*);
int metadata_unlock(cache_file*, const int, const int, const int*, const char*);
int update_lock_queue(cache_file*, int, int, int);

int cache_fsize(const int, cache_file*, const ADIO_Offset);

int migration_local(cache_md*, const int, const int);

#endif
