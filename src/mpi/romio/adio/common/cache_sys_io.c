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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>

#include "adio.h"
#ifdef HAVE_PVFS_H
#include <pvfs.h>      /* pvfs functions */
#endif

extern int _system_page_size;  /* from getpagesize() */

/*
#define _USE_LUSTRE_
*/

#ifdef __linux__
    #include <sys/ioctl.h>                            /* necessary for: */
    #ifndef __USE_GNU
        #define __USE_GNU                             /* O_DIRECT and */
    #endif
    #include <fcntl.h>                                /* IO operations */
    #undef __USE_GNU
    #ifdef _USE_LUSTRE_
        #include <lustre/lustre_user.h>
    #endif
#endif /* __linux__ */


#include "adio.h"


#ifdef WKL_DEBUG
extern int rNTraces, wNTraces, stripe_size, unexp_io;
extern int SELF;
#endif

#ifdef _GPFS
#define FLOCK flock64
#define SETLKW F_SETLKW64
#else
#define FLOCK flock
#define SETLKW F_SETLKW
#endif

/*   wraps byte-range locking around each read and write calls */
#define BYTE_RANGE_LOCK(type,offset,len) {                              \
    int err;                                                            \
    struct FLOCK lock;                                                  \
                                                                        \
    lock.l_type   = type;                                               \
    lock.l_start  = offset;                                             \
    lock.l_whence = SEEK_SET;                                           \
    lock.l_len    = len;                                                \
                                                                        \
    do {                                                                \
        err = fcntl(fd, SETLKW, &lock);                                 \
    } while (err && (errno == EINTR));                                  \
    if (err && (errno != EBADF))                                        \
        perror("Error when locking");                                   \
}


/*----< sys_read_at() >------------------------------------------------------*/
/* like pread BUT file pointer will advance len bytes from offset            */
int sys_read_at(const int          file_sys_type,
                int                fd,
                const ADIO_Offset  offset,
                void              *buf,
                const int          len,
                const int          do_locking_io,
                const int          do_aligned_io)
{
    int readBytes = -1;

#ifdef WKL_DEBUG
if (unexp_io) printf("unexp_io: sys_read_at() offset=%lld len=%d\n",offset,len);
#endif

#ifdef HAVE_PVFS_H
    if (file_sys_type == ADIO_PVFS2) {
        struct stat st;
        pvfs_fstat(fd, &st);  /* get current file size, st.st_size */
        pvfs_lseek64(fd, offset, SEEK_SET);
        if (st.st_size <= offset) readBytes = 0;
        else                      readBytes = pvfs_read(fd, buf, len);
/*
if (readBytes>0) printf("%d: st.st_size=%d read() (%lld %d) readBytes=%d\n",SELF,(int)(st.st_size),offset,len,readBytes);
readBytes = len; pvfs_lseek64(fd, len, SEEK_CUR);
*/
    }
#endif
    if (file_sys_type == ADIO_NFS) {
        struct stat st;
        BYTE_RANGE_LOCK(F_RDLCK, offset, len);
        fstat(fd, &st);  /* get current file size, st.st_size */
        lseek(fd, offset, SEEK_SET);
        if (st.st_size <= offset) readBytes = 0;
        else                      readBytes = read(fd, buf, len);
        BYTE_RANGE_LOCK(F_UNLCK, offset, len);
    }
    else if (file_sys_type == ADIO_UFS) {
        struct stat st;
        if (do_locking_io) BYTE_RANGE_LOCK(F_RDLCK, offset, len);
        fstat(fd, &st);  /* get current file size, st.st_size */
        lseek(fd, offset, SEEK_SET);
        if (st.st_size <= offset) readBytes = 0;
        else {
            char *new_buf = buf;
            if (do_aligned_io) {
#ifdef WKL_DEBUG
if (offset%_system_page_size > 0 || len%_system_page_size > 0) printf("Warning: sys_read_at offset=%lld(%lld) len=%d(%d) not align _system_page_size(%d) !!!\n",offset,offset%_system_page_size,len,len%_system_page_size,_system_page_size);
#endif
                if ((long)buf & (_system_page_size-1)) {
printf("----  read buf(%ld) is not aligned: do malloc(%d) and memcpy ! --------------\n",(long)buf,len);
                    new_buf = (char*) ADIOI_Malloc(len);
                }
            }
            readBytes = read(fd, new_buf, len);
            if (new_buf != buf) {
                memcpy(buf, new_buf, len);
                ADIOI_Free(new_buf);
            }
        }
        if (do_locking_io) BYTE_RANGE_LOCK(F_UNLCK, offset, len);
    }

    if (file_sys_type != ADIO_PVFS2 && file_sys_type != ADIO_NFS  &&
        file_sys_type != ADIO_UFS)
        fprintf(stderr,
                "Error: sys_read_at() file system type (%d) is not supported\n",
                file_sys_type);

#ifdef WKL_DEBUG
{if (stripe_size > 1 && readBytes>0) rNTraces += len /stripe_size;}
{extern FILE *ftrace; if (readBytes>0) fprintf(ftrace,"%2d r %lld %d\n",SELF,offset,len);}
if (readBytes>0) printf("r %lld %d readBytes=%d\nr %lld %d readBytes=%d\n\n",offset/stripe_size,ntrace,readBytes,(offset+len)/stripe_size,ntrace,readBytes);ntrace++;

if (readBytes == -1) printf("%2d: Error: sys_read_at() readBytes == -1 offset=%lld, len=%d, errno=%d strerror=%s\n",SELF,offset,len,errno,strerror(errno));
#endif

    if (readBytes == -1) printf("Error: sys_read_at() readBytes == -1 offset=%lld, len=%d, errno=%d strerror=%s\n",offset,len,errno,strerror(errno));

    return readBytes;
}

/*----< sys_aligned_read() >-------------------------------------------------*/
/* makes sure read offset is aligned with system page size                   */
int sys_aligned_read(const int      file_sys_type,
                     const int      fd,       /* file descriptor */
                     const int      fsize,    /* current file size */
                     char *restrict buf,      /* read buffer */
                     ADIO_Offset    offset,   /* start offset of write */
                     int            req_len)  /* read request size,
                                                      assuming < 2^31 */
{
    /* offset, req_len, buffer address may not be aligned with system page
       size */
    int err;
    int align_front_extra = offset % _system_page_size;
    int align_end_rem     = (offset+req_len) % _system_page_size;
    int align_start_page  = offset / _system_page_size;
    int align_end_page    = (offset + req_len - 1) / _system_page_size;
    int num_pages         = align_end_page - align_start_page + 1;
    char *tmpBuf;

    ADIO_Offset  lock_offset = offset-align_front_extra;
    int          lock_range  = num_pages * _system_page_size;

    /* if read beyond EOF */
    if (fsize <= offset - align_front_extra) return 0;

/*
if (file_sys_type != ADIO_PVFS2)    BYTE_RANGE_LOCK(F_RDLCK, lock_offset, lock_range);
*/
    if (num_pages <= 2) {
        offset -= align_front_extra;
        if (align_front_extra > 0 || align_end_rem > 0) {
            tmpBuf = (char*) ADIOI_Calloc(num_pages*_system_page_size, 1);
            err = sys_read_at(file_sys_type, fd, offset, tmpBuf, num_pages*_system_page_size, 0, 1);
            memcpy(buf, tmpBuf + align_front_extra, req_len);
            ADIOI_Free(tmpBuf);
        }
        else /* align_front_extra == 0 && align_end_rem == 0  &&
                req_len == num_pages*_system_page_size */
            err = sys_read_at(file_sys_type, fd, offset, buf, req_len, 0, 1);

/*
if (file_sys_type != ADIO_PVFS2)        BYTE_RANGE_LOCK(F_UNLCK, lock_offset, lock_range);
*/
        return err;
    }
    /* now, num_pages >= 3 */

    if (align_front_extra > 0 || align_end_rem > 0)
        tmpBuf = (char*) ADIOI_Calloc(_system_page_size, 1);

    if (align_front_extra > 0) { /* read front_extra of 1st page */
        int align_front_rem = _system_page_size - align_front_extra;

        offset -= align_front_extra;
        err = sys_read_at(file_sys_type, fd, offset, tmpBuf, _system_page_size, 0, 1);
        memcpy(buf, tmpBuf + align_front_extra, align_front_rem);
        buf     += align_front_rem;
        req_len -= align_front_rem;
        offset  += _system_page_size;
    }
    /* read the middle pages of buffer */
    err = sys_read_at(file_sys_type, fd, offset, buf, req_len-align_end_rem, 0, 1);
    offset += req_len - align_end_rem;
    buf    += req_len - align_end_rem;

    if (align_end_rem > 0) { /* read end remain of last page */
        if (align_front_extra > 0) bzero(tmpBuf, _system_page_size);
        err = sys_read_at(file_sys_type, fd, offset, tmpBuf, _system_page_size, 0, 1);
        memcpy(buf, tmpBuf, align_end_rem);
    }

    if (align_front_extra > 0 || align_end_rem > 0)
        ADIOI_Free(tmpBuf);

/*
if (file_sys_type != ADIO_PVFS2)    BYTE_RANGE_LOCK(F_UNLCK, lock_offset, lock_range);
*/

    return err;
}
/*----< sys_write_at() >-----------------------------------------------------*/
/* like pwrite BUT file pointer will advance len bytes from offset           */
int sys_write_at(const int          file_sys_type,
                 int                fd,
                 const ADIO_Offset  offset,
                 void              *buf,
                 const int          len,
                 const int          do_locking_io,
                 const int          do_aligned_io)
{
    int writeBytes = -1;

#ifdef WKL_DEBUG
if (unexp_io) printf("unexp_io: sys_write_at() offset=%lld len=%d\n",offset,len);
if (stripe_size > 1) wNTraces += len /stripe_size;
{extern FILE *ftrace; fprintf(ftrace,"%2d w %lld %d\n",SELF,offset,len);}
printf("w %lld %d\nw %lld %d\n\n",offset/stripe_size,ntrace,(offset+len)/stripe_size,ntrace);ntrace++;
#endif

#ifdef HAVE_PVFS_H
    if (file_sys_type == ADIO_PVFS2) {
        pvfs_lseek64(fd, offset, SEEK_SET);
        writeBytes = pvfs_write(fd, buf, len);
/*
writeBytes = len; pvfs_lseek64(fd, len, SEEK_CUR);
*/
if (do_aligned_io) if ((long)buf & 4095) printf("TESTING ----  write buf(%ld) is not aligned: do malloc(%d) and memcpy ! --------------\n",(long)buf,len);
    }
#endif
    if (file_sys_type == ADIO_NFS) {
        BYTE_RANGE_LOCK(F_WRLCK, offset, len);
        lseek(fd, offset, SEEK_SET);
        writeBytes = write(fd, buf, len);
        BYTE_RANGE_LOCK(F_UNLCK, offset, len);
    }
    else if (file_sys_type == ADIO_UFS) {
        char *new_buf = buf;
        if (do_aligned_io) {
#ifdef WKL_DEBUG
if (offset%_system_page_size > 0 || len%_system_page_size > 0) printf("Warning: sys write offset=%lld(%lld) len=%d(%d) not align _system_page_size(%d) !!!\n",offset,offset%_system_page_size,len,len%_system_page_size,_system_page_size);
#endif
            if ((long)buf & (_system_page_size-1)) {
printf("----  write buf(%ld) is not aligned: do malloc(%d) and memcpy ! --------------\n",(long)buf,len);
                new_buf = (char*) ADIOI_Malloc(len);
                memcpy(new_buf, buf, len);
            }
        }
        if (do_locking_io) BYTE_RANGE_LOCK(F_WRLCK, offset, len);
        lseek(fd, offset, SEEK_SET);
/* write one file block at a time
{
        char *buf_ptr;
        int i, block_size = shared_v.caches.block_size;
        buf_ptr = new_buf;
        for (i=0; i<len/block_size; i++) {
            write(fd, buf_ptr, block_size);
            buf_ptr += block_size;
        }
        if (len % block_size > 0)
            write(fd, buf_ptr, len%block_size);

        writeBytes = len;
}
*/
        writeBytes = write(fd, new_buf, len);
        if (do_locking_io) BYTE_RANGE_LOCK(F_UNLCK, offset, len);
        if (buf != new_buf) ADIOI_Free(new_buf);
    }

    if (file_sys_type != ADIO_PVFS2 && file_sys_type != ADIO_NFS  &&
        file_sys_type != ADIO_UFS)
        fprintf(stderr,
               "Error: sys_write_at() file system type (%d) is not supported\n",
               file_sys_type);

if (writeBytes == -1) printf("Error: sys_write_at() writeBytes == -1 offset=%lld, len=%d, errno=%d strerror=%s\n",offset,len,errno,strerror(errno));
#ifdef WKL_DEBUG
if (writeBytes == -1) printf("%2d: Error: sys_write_at() writeBytes == -1 offset=%lld, len=%d, errno=%d strerror=%s\n",SELF,offset,len,errno,strerror(errno));
#endif

    return writeBytes;
}

/*----< sys_aligned_write() >-------------------------------------------------*/
/* makes sure write offset is aligned with system page size                   */
int sys_aligned_write(const int      file_sys_type,
                      const int      fd,       /* file descriptor */
                      const int      fsize,    /* current file size */
                      char *restrict buf,      /* read buffer */
                      ADIO_Offset    offset,   /* start offset of write */
                      int            req_len)  /* read request size,
                                                      assuming < 2^31 */
{
    int err;
    int align_front_extra = offset % _system_page_size;
    int align_end_rem     = (offset+req_len) % _system_page_size;
    int align_start_page  = offset / _system_page_size;
    int align_end_page    = (offset + req_len - 1) / _system_page_size;
    int num_pages         = align_end_page - align_start_page + 1;
    char *tmpBuf;

/* !!! PVFS supports file locking ??? */
/*
    ADIO_Offset  lock_offset = offset-align_front_extra;
    int          lock_range  = num_pages * _system_page_size;
if (file_sys_type != ADIO_PVFS2)    BYTE_RANGE_LOCK(F_WRLCK, lock_offset, lock_range);
*/
    if (num_pages <= 2) {
        tmpBuf  = buf;
        offset -= align_front_extra;

        if (align_front_extra > 0 || align_end_rem > 0) {
            tmpBuf = (char*) ADIOI_Calloc(num_pages*_system_page_size, 1);
            /* read-modify-write only when not reading beyond EOF */
            if (offset < fsize) {
                /* read whole 1 or 2 pages */
                if (num_pages == 1 || (num_pages == 2 &&
                    align_front_extra > 0 && align_end_rem > 0))
                    err = sys_read_at(file_sys_type, fd, offset,
                                      tmpBuf, num_pages*_system_page_size, 0, 1);
                else if (align_front_extra > 0) { /* read 1st page :
                    num_pages == 2 && align_end_rem == 0 */
                    err = sys_read_at(file_sys_type, fd, offset,
                                      tmpBuf, _system_page_size, 0, 1);
                }
                else { /* read 2nd page: Here, align_end_rem > 0 &&
                          num_pages == 2 && align_front_extra == 0 */
                    err = sys_read_at(file_sys_type, fd, offset+_system_page_size,
                                      tmpBuf+_system_page_size, _system_page_size, 0, 1);
                }
            }
            memcpy(tmpBuf + align_front_extra, buf, req_len);
        }
        err = sys_write_at(file_sys_type, fd, offset,
                           tmpBuf, num_pages*_system_page_size, 0, 1);
        if (tmpBuf != buf) ADIOI_Free(tmpBuf);
/*
if (file_sys_type != ADIO_PVFS2)        BYTE_RANGE_LOCK(F_UNLCK, lock_offset, lock_range);
*/
        return err;
    }

    if (align_front_extra > 0 || align_end_rem > 0)
        tmpBuf = (char*) ADIOI_Calloc(_system_page_size, 1);

    /* now, num_pages >= 3 */
    if (align_front_extra > 0) { /* read-modify-write for 1st page */
        int   align_front_rem = _system_page_size - align_front_extra;

        offset -= align_front_extra;
        if (offset < fsize)
            err = sys_read_at(file_sys_type, fd, offset, tmpBuf, _system_page_size, 0, 1);
        memcpy(tmpBuf + align_front_extra, buf, align_front_rem);
        err = sys_write_at(file_sys_type, fd, offset, tmpBuf, _system_page_size, 0, 1);

        buf     += align_front_rem;
        req_len -= align_front_rem;
        offset  += _system_page_size;
    }

    /* write the middle pages of buffer */
    err = sys_write_at(file_sys_type, fd, offset, buf, req_len-align_end_rem, 0, 1);
    offset += req_len - align_end_rem;
    buf    += req_len - align_end_rem;

    if (align_end_rem > 0) { /* read-modify-write for last page */
        if (align_front_extra > 0) bzero(tmpBuf, _system_page_size);
        if (offset < fsize)
            err = sys_read_at(file_sys_type, fd, offset, tmpBuf, _system_page_size, 0, 1);
        memcpy(tmpBuf, buf, align_end_rem);
        err = sys_write_at(file_sys_type, fd, offset, tmpBuf, _system_page_size, 0, 1);
    }

    if (align_front_extra > 0 || align_end_rem > 0)
        ADIOI_Free(tmpBuf);
/*
if (file_sys_type != ADIO_PVFS2)    BYTE_RANGE_LOCK(F_UNLCK, lock_offset, lock_range);
*/

    return err;
}

/*----< sys_lseek() >--------------------------------------------------------*/
inline
ADIO_Offset sys_lseek(const int         file_sys_type,
                      int               fd,
                      const ADIO_Offset offset,
                      const int         whence)
{
#ifdef HAVE_PVFS_H
    if (file_sys_type == ADIO_PVFS2)
        return pvfs_lseek64(fd, offset, whence);
#endif
    if (file_sys_type == ADIO_NFS || file_sys_type == ADIO_UFS)
        return lseek(fd, offset, whence);

    fprintf(stderr,
            "Error: sys_lseek() file system type (%d) is not supported\n",
            file_sys_type);

    return -1;
}

/*----< sys_fsync() >--------------------------------------------------------*/
inline
int sys_fsync(const int file_sys_type, int fd)
{
#ifdef HAVE_PVFS_H
    if (file_sys_type == ADIO_PVFS2) return pvfs_fsync(fd);
#endif
    if (file_sys_type == ADIO_NFS || file_sys_type == ADIO_UFS)
        return fsync(fd);

    fprintf(stderr,
            "Error: sys_fsync() file system type (%d) is not supported\n",
            file_sys_type);

    return -1;
}

/*----< sys_get_block_size() >-----------------------------------------------*/
int sys_get_block_size(const int file_sys_type, int fd)
{
    int block_size = -1;

#ifdef HAVE_PVFS_H
    if (file_sys_type == ADIO_PVFS2) {
        /* pvfs_stat64 seems not working !
        struct stat64 buf;
        if (pvfs_stat64(filename, &buf) < 0)
            fprintf(stderr, "Warning: pvfs_stat64 %s (%s)\n",
                    strerror(errno), filename);
        else
            block_size = buf.st_blksize;
        */
        pvfs_filestat buf;
        if (pvfs_ioctl(fd, GETMETA, &buf) < 0)
            fprintf(stderr, "Warning: pvfs_ioctl %s\n", strerror(errno));
        else
            block_size = buf.ssize;
        return block_size;
    }
#endif
    if (file_sys_type == ADIO_UFS) { /* for Lustre FS */
#ifdef _USE_LUSTRE_
        struct lov_user_md lum = { 0 };
        /* get file striping information */
        lum.lmm_magic = LOV_USER_MAGIC;
        if (ioctl(fd, LL_IOC_LOV_GETSTRIPE, (void *) &lum) < 0)
            fprintf(stderr, "Warning: Lustre ioctl() %s\n", strerror(errno));
        else
            block_size = lum.lmm_stripe_size;
        return block_size;
#else
        struct stat buf;
        if (fstat(fd, &buf) < 0)
            fprintf(stderr, "Warning: stat %s\n", strerror(errno));
        else
            block_size = buf.st_blksize;
        return block_size;
#endif
    }
    else if (file_sys_type == ADIO_NFS) {
        struct stat buf;
        if (fstat(fd, &buf) < 0)
            fprintf(stderr, "Warning: stat %s\n", strerror(errno));
        else
            block_size = buf.st_blksize;
        return block_size;
    }

    fprintf(stderr,
         "Error: sys_get_block_size() file system type (%d) is not supported\n",
         file_sys_type);

    return -1;
}

/*----< sys_ftruncate() >----------------------------------------------------*/
inline
int sys_ftruncate(const int file_sys_type, int fd, const ADIO_Offset fsize)
{
#ifdef HAVE_PVFS_H
    if (file_sys_type == ADIO_PVFS2) return pvfs_ftruncate64(fd, fsize);
#endif
    if (file_sys_type == ADIO_NFS || file_sys_type == ADIO_UFS)
        return ftruncate(fd, fsize);

    fprintf(stderr,
            "Error: sys_ftruncate() file system type (%d) is not supported\n",
            file_sys_type);

    return -1;
}

