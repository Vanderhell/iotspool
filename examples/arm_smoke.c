/*
 * arm_smoke.c - Minimal bare-metal link smoke for Cortex-M builds.
 *
 * The target exists to prove the library links under the selected ARM
 * toolchain profile; it is not executed in CI.
 *
 * SPDX-License-Identifier: MIT
 */

#include "iotspool.h"

#include <string.h>

typedef struct {
    uint8_t  data[1024];
    uint32_t size;
} smoke_store_t;

static iotspool_err_t smoke_append(void *ctx, const uint8_t *data, uint32_t len) {
    smoke_store_t *s = (smoke_store_t *)ctx;
    if (len > sizeof(s->data) - s->size) return IOTSPOOL_EFULL;
    memcpy(&s->data[s->size], data, len);
    s->size += len;
    return IOTSPOOL_OK;
}

static iotspool_err_t smoke_read_at(void *ctx, uint32_t off, uint8_t *out,
                                    uint32_t cap, uint32_t *out_len) {
    smoke_store_t *s = (smoke_store_t *)ctx;
    if (off >= s->size) {
        if (out_len) *out_len = 0;
        return IOTSPOOL_OK;
    }
    uint32_t avail = s->size - off;
    uint32_t n = (avail < cap) ? avail : cap;
    memcpy(out, &s->data[off], n);
    if (out_len) *out_len = n;
    return IOTSPOOL_OK;
}

static iotspool_err_t smoke_sync(void *ctx) {
    (void)ctx;
    return IOTSPOOL_OK;
}

static uint32_t smoke_size(void *ctx) {
    return ((smoke_store_t *)ctx)->size;
}

static iotspool_err_t smoke_truncate(void *ctx, uint32_t new_size) {
    smoke_store_t *s = (smoke_store_t *)ctx;
    if (new_size > s->size) return IOTSPOOL_EINVAL;
    s->size = new_size;
    return IOTSPOOL_OK;
}

static iotspool_err_t smoke_replace(void *ctx, const uint8_t *data, uint32_t len) {
    smoke_store_t *s = (smoke_store_t *)ctx;
    if (len > sizeof(s->data)) return IOTSPOOL_EFULL;
    memcpy(s->data, data, len);
    s->size = len;
    return IOTSPOOL_OK;
}

int main(void) {
    smoke_store_t store_ctx = {{0}, 0};
    iotspool_store_t store = {
        .ctx = &store_ctx,
        .append = smoke_append,
        .read_at = smoke_read_at,
        .sync = smoke_sync,
        .size_bytes = smoke_size,
        .truncate_to = smoke_truncate,
        .replace = smoke_replace
    };

    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 4;
    cfg.max_store_bytes = sizeof(store_ctx.data);
    cfg.max_topic_bytes = 16;
    cfg.max_payload_bytes = 16;

    iotspool_t spool;
    iotspool_entry_t entries[4];
    uint8_t scratch[128];
    if (iotspool_init_inplace(&spool, entries, 4, scratch, sizeof(scratch), &cfg, &store) != IOTSPOOL_OK) {
        return 1;
    }
    if (iotspool_recover(&spool) != IOTSPOOL_OK) {
        return 2;
    }
    iotspool_msg_t msg = {
        .topic = "arm/smoke",
        .payload = (const uint8_t *)"ok",
        .payload_len = 2,
        .qos = 0,
        .retain = false
    };
    if (iotspool_enqueue(&spool, &msg, NULL) != IOTSPOOL_OK) {
        return 3;
    }
    (void)iotspool_compact(&spool);
    iotspool_deinit(&spool);
    return 0;
}
