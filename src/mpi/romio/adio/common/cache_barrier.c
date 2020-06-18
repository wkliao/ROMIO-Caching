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


/*----< cache_thread_barrier() >----------------------------------------------*/
/* this function is called by the I/O thread only                            */
int cache_thread_barrier(cache_file *fptr)
{
    int          i, dest;
    char         dummy;
    MPI_Status  *restrict status;
    MPI_Request *restrict comm_req;

    status   = (MPI_Status*)  ADIOI_Malloc(2*fptr->np * sizeof(MPI_Status));
    comm_req = (MPI_Request*) ADIOI_Malloc(2*fptr->np * sizeof(MPI_Request));

    comm_req[fptr->rank] = comm_req[fptr->rank + fptr->np] = MPI_REQUEST_NULL;

    for (i=1; i<fptr->np; i++) {
        dest = (fptr->rank + i) % fptr->np;
        MPI_Irecv(&dummy, 1, MPI_BYTE, dest, CACHE_BARRIER, fptr->comm,
                  &comm_req[dest]);
        MPI_Issend(&dummy, 1, MPI_BYTE, dest, CACHE_BARRIER, fptr->comm,
                   &comm_req[dest+fptr->np]);
    }
    WAITALL(2*fptr->np, comm_req, status, "cache_barrier_req");

    ADIOI_Free(comm_req);
    ADIOI_Free(status);

    /* At this point, all processes should finish any remote Isend posting.
       All pending Isend should be received here */
    while (1 == probe_incoming_cache(fptr, -1, "cache_thread_barrier"));
#if 0
    for (i=0; i<fptr->np; i++) {
        dest = (fptr->rank + i) % fptr->np;
        while (1 == probe_incoming_cache(fptr, dest, "cache_thread_barrier"));
        /* keep checking until no pending communication */
    }
#endif

    return 1;
}


/*----< cache_barrier() >------------------------------------------------------*/
/* this function is called by the main thread only                           */
int cache_barrier(cache_file *fptr)
{
    MUTEX_LOCK(shared_v.mutex);
    shared_v.req  = CACHE_BARRIER;
    shared_v.fptr = fptr;

    COND_WAIT(shared_v.cond, shared_v.mutex);
    shared_v.req  = CACHE_REQ_NULL;
    MUTEX_UNLOCK(shared_v.mutex);

    return 1;
}
