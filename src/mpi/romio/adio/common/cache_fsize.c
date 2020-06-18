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

/*----< cache_fsize() >-------------------------------------------------------*/
int cache_fsize(const int          req_type,/* CACHE_GET_FSIZE or CACHE_SET_FSIZE */
                cache_file        *fptr,
                const ADIO_Offset  req_end_offset)
{
    if (fptr->rank == FSIZE_OWNER) {
        if (req_type == CACHE_SET_FSIZE)
            fptr->fsize = MAX(fptr->fsize, req_end_offset);
    }
    else if (fptr->fsize < req_end_offset) {
        /* only needed when the fsize stored locally is smaller than the end
           of requesting offset, get/set the most up-to-date file size
           from/to proc 0 */
        MPI_Request  comm_req;
        MPI_Status   status;
        cache_req_msg  fsize_req;

        fsize_req.gfid      = fptr->gfid;
        fsize_req.req_type  = req_type;
        fsize_req.count     = 1;
        fsize_req.disp      = req_end_offset;

        /* send out request message to proc 0 */
        MPI_Isend(&fsize_req, sizeof(cache_req_msg), MPI_BYTE, FSIZE_OWNER,
                  CACHE_TAG_REQ+fptr->gfid, fptr->comm, &comm_req);
        MPI_Request_free(&comm_req);

        /* gets the current fle size from proc 0 */
        MPI_Irecv(&fptr->fsize, sizeof(ADIO_Offset), MPI_BYTE, FSIZE_OWNER,
                  req_type, fptr->comm, &comm_req);

        POLLING(comm_req, status, "cache_fsize");
    }
#ifdef WKL_DEBUG
printf("(t) req EOF = %lld obtain EOF = %lld\n",req_end_offset,fptr->fsize);
#endif
    return 1;
}
