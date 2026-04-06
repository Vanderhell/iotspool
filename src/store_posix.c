/* store_posix.c – POSIX (and ESP-IDF VFS) file storage backend.
 * SPDX-License-Identifier: MIT */

#include "store_posix.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#if defined(_WIN32)
#include <io.h>
#include <windows.h>
#endif
#if defined(__linux__) || defined(__APPLE__)
#include <unistd.h>
#endif

typedef struct {
    FILE    *fp;
    uint32_t size;   /* cached size to avoid fseek on every append */
} posix_ctx_t;

static iotspool_err_t posix_append(void *ctx, const uint8_t *data, uint32_t len) {
    posix_ctx_t *c = (posix_ctx_t *)ctx;
    if (fwrite(data, 1, len, c->fp) != len) return IOTSPOOL_EIO;
    c->size += len;
    return IOTSPOOL_OK;
}

static iotspool_err_t posix_read_at(void *ctx, uint32_t off,
                                     uint8_t *out, uint32_t cap,
                                     uint32_t *out_len)
{
    posix_ctx_t *c = (posix_ctx_t *)ctx;
    if (fseek(c->fp, (long)off, SEEK_SET) != 0) return IOTSPOOL_EIO;
    *out_len = (uint32_t)fread(out, 1, cap, c->fp);
    return IOTSPOOL_OK;
}

static iotspool_err_t posix_sync(void *ctx) {
    posix_ctx_t *c = (posix_ctx_t *)ctx;
    if (fflush(c->fp) != 0) return IOTSPOOL_EIO;
    /* fsync for real durability on Linux */
#if defined(__linux__) || defined(__APPLE__)
    {
        int fd = fileno(c->fp);
        if (fd >= 0) fsync(fd);
    }
#endif
    return IOTSPOOL_OK;
}

static uint32_t posix_size(void *ctx) {
    return ((posix_ctx_t *)ctx)->size;
}

static iotspool_err_t posix_truncate(void *ctx, uint32_t new_size) {
    posix_ctx_t *c = (posix_ctx_t *)ctx;
#if defined(__linux__) || defined(__APPLE__)
    if (fflush(c->fp) != 0) return IOTSPOOL_EIO;
    int fd = fileno(c->fp);
    if (ftruncate(fd, (off_t)new_size) != 0) return IOTSPOOL_EIO;
    fseek(c->fp, 0, SEEK_END);
    c->size = new_size;
    return IOTSPOOL_OK;
#elif defined(_WIN32)
    int fd;
    intptr_t osfh;
    HANDLE handle;
    LARGE_INTEGER offset;

    if (fflush(c->fp) != 0) return IOTSPOOL_EIO;
    fd = _fileno(c->fp);
    if (fd < 0) return IOTSPOOL_EIO;
    osfh = _get_osfhandle(fd);
    if (osfh == -1) return IOTSPOOL_EIO;
    handle = (HANDLE)osfh;
    offset.QuadPart = (LONGLONG)new_size;
    if (!SetFilePointerEx(handle, offset, NULL, FILE_BEGIN)) return IOTSPOOL_EIO;
    if (!SetEndOfFile(handle)) return IOTSPOOL_EIO;
    if (fseek(c->fp, 0, SEEK_END) != 0) return IOTSPOOL_EIO;
    c->size = new_size;
    return IOTSPOOL_OK;
#else
    (void)c;
    (void)new_size;
    return IOTSPOOL_EIO; /* not supported on this platform */
#endif
}

iotspool_err_t store_posix_open(const char *path, iotspool_store_t *store) {
    if (!path || !store) return IOTSPOOL_EINVAL;

    posix_ctx_t *c = (posix_ctx_t *)calloc(1, sizeof(*c));
    if (!c) return IOTSPOOL_ENOMEM;

    /* Open for read+write, create if missing */
    c->fp = fopen(path, "a+b");
    if (!c->fp) { free(c); return IOTSPOOL_EIO; }

    /* Determine existing size */
    fseek(c->fp, 0, SEEK_END);
    long sz = ftell(c->fp);
    c->size = (sz > 0) ? (uint32_t)sz : 0;
    /* Leave file position at end (for appends) */

    store->ctx         = c;
    store->append      = posix_append;
    store->read_at     = posix_read_at;
    store->sync        = posix_sync;
    store->size_bytes  = posix_size;
    store->truncate_to = posix_truncate;
    return IOTSPOOL_OK;
}

void store_posix_close(iotspool_store_t *store) {
    if (!store || !store->ctx) return;
    posix_ctx_t *c = (posix_ctx_t *)store->ctx;
    if (c->fp) { fflush(c->fp); fclose(c->fp); }
    free(c);
    store->ctx = NULL;
}
