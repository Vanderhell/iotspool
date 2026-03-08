/* store_posix.h – POSIX file-based storage backend.
 * Works on Linux/macOS and via ESP-IDF VFS on ESP32.
 * SPDX-License-Identifier: MIT */
#pragma once
#include "../include/iotspool.h"

/**
 * Open (or create) a spool file at `path` and fill `store` with the
 * vtable and an internally-allocated context.
 *
 * Call store_posix_close() when done to flush and free resources.
 */
iotspool_err_t store_posix_open (const char *path, iotspool_store_t *store);
void           store_posix_close(iotspool_store_t *store);
