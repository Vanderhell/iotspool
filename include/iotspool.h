/**
 * iotspool.h  –  Persistent store-and-forward queue for MQTT publish.
 *
 * Survives power loss and reboots. Runs on Linux, ESP32, STM32.
 * No RTOS required. C99. Zero dynamic dependencies.
 *
 * SPDX-License-Identifier: MIT
 */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define IOTSPOOL_VERSION_MAJOR 0
#define IOTSPOOL_VERSION_MINOR 1
#define IOTSPOOL_VERSION_PATCH 0

/* ── Error codes ──────────────────────────────────────────────────────────── */
typedef enum {
    IOTSPOOL_OK         = 0,
    IOTSPOOL_EINVAL     = 1,
    IOTSPOOL_EIO        = 2,
    IOTSPOOL_ECORRUPT   = 3,
    IOTSPOOL_EFULL      = 4,
    IOTSPOOL_ENOTFOUND  = 5,
    IOTSPOOL_ESTATE     = 6,
    IOTSPOOL_ENOMEM     = 7,
} iotspool_err_t;

const char *iotspool_strerror(iotspool_err_t err);

typedef uint32_t iotspool_msg_id_t;
#define IOTSPOOL_MSG_ID_INVALID UINT32_MAX

/* ── Message ──────────────────────────────────────────────────────────────── */
typedef struct {
    const char    *topic;
    const uint8_t *payload;
    uint32_t       payload_len;
    uint8_t        qos;    /* 0 or 1 */
    bool           retain;
} iotspool_msg_t;

/* ── Storage backend (vtable) ─────────────────────────────────────────────── */
typedef struct {
    void *ctx;
    iotspool_err_t (*append)     (void *ctx, const uint8_t *data, uint32_t len);
    iotspool_err_t (*read_at)    (void *ctx, uint32_t off, uint8_t *out,
                                  uint32_t cap, uint32_t *out_len);
    iotspool_err_t (*sync)       (void *ctx);
    uint32_t       (*size_bytes) (void *ctx);
    iotspool_err_t (*truncate_to)(void *ctx, uint32_t new_size); /* may be NULL */
} iotspool_store_t;

/* ── Configuration ────────────────────────────────────────────────────────── */
typedef struct {
    uint32_t max_pending_msgs;
    uint32_t max_store_bytes;
    uint32_t max_payload_bytes;
    uint32_t max_topic_bytes;
    uint32_t min_retry_ms;
    uint32_t max_retry_ms;
    bool     enable_sha256;
    bool     drop_oldest_on_full;
} iotspool_cfg_t;

static inline iotspool_cfg_t iotspool_cfg_default(void) {
    iotspool_cfg_t c = {0};
    c.max_pending_msgs   = 64;
    c.max_store_bytes    = 256 * 1024;
    c.max_payload_bytes  = 4096;
    c.max_topic_bytes    = 256;
    c.min_retry_ms       = 1000;
    c.max_retry_ms       = 60000;
    c.enable_sha256      = false;
    c.drop_oldest_on_full = false;
    return c;
}

/* ── Opaque handle ────────────────────────────────────────────────────────── */
typedef struct iotspool iotspool_t;

/* ── Lifecycle ────────────────────────────────────────────────────────────── */
iotspool_err_t iotspool_init    (iotspool_t **out, const iotspool_cfg_t *cfg,
                                  const iotspool_store_t *store);
iotspool_err_t iotspool_recover (iotspool_t *s);
void           iotspool_deinit  (iotspool_t *s);

/* ── Enqueue ──────────────────────────────────────────────────────────────── */
iotspool_err_t iotspool_enqueue (iotspool_t *s, const iotspool_msg_t *m,
                                  iotspool_msg_id_t *out_id);

/* ── Publish path ─────────────────────────────────────────────────────────── */
iotspool_err_t iotspool_peek_ready      (iotspool_t *s, uint32_t now_ms,
                                          iotspool_msg_t *out,
                                          iotspool_msg_id_t *out_id);
iotspool_err_t iotspool_ack             (iotspool_t *s, iotspool_msg_id_t id);
void           iotspool_on_publish_fail (iotspool_t *s, uint32_t now_ms);

/* ── Diagnostics ──────────────────────────────────────────────────────────── */
typedef struct {
    uint32_t pending_count;
    uint32_t store_bytes;
    uint32_t enqueued_total;
    uint32_t acked_total;
    uint32_t dropped_total;
    uint32_t corrupt_records;
} iotspool_stats_t;

void iotspool_stats(const iotspool_t *s, iotspool_stats_t *stats);

#ifdef __cplusplus
}
#endif
