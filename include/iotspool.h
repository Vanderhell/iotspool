/*
 * iotspool.h - Persistent store-and-forward queue for MQTT publish.
 *
 * Public API focuses on bounded caller-owned state for embedded use.
 * Host convenience wrappers may allocate, but the core API does not require it.
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

typedef enum {
    IOTSPOOL_OK        = 0,
    IOTSPOOL_EINVAL    = 1,
    IOTSPOOL_EIO       = 2,
    IOTSPOOL_ECORRUPT  = 3,
    IOTSPOOL_EFULL     = 4,
    IOTSPOOL_ENOTFOUND = 5,
    IOTSPOOL_ESTATE    = 6,
    IOTSPOOL_ENOMEM    = 7,
    IOTSPOOL_EAGAIN    = 8,
    IOTSPOOL_EBUSY     = 9
} iotspool_err_t;

const char *iotspool_strerror(iotspool_err_t err);

typedef uint64_t iotspool_msg_id_t;
#define IOTSPOOL_MSG_ID_INVALID UINT64_MAX

typedef enum {
    IOTSPOOL_STATE_UNINITIALIZED = 0,
    IOTSPOOL_STATE_INITIALIZED   = 1,
    IOTSPOOL_STATE_RECOVERING    = 2,
    IOTSPOOL_STATE_READY         = 3,
    IOTSPOOL_STATE_COMPACTING    = 4,
    IOTSPOOL_STATE_FAILED        = 5
} iotspool_state_t;

typedef struct {
    const char    *topic;
    const uint8_t *payload;
    uint32_t       payload_len;
    uint8_t        qos;     /* 0 or 1 */
    bool           retain;
} iotspool_msg_t;

typedef struct {
    void *ctx;
    iotspool_err_t (*append)(void *ctx, const uint8_t *data, uint32_t len);
    iotspool_err_t (*read_at)(void *ctx, uint32_t off, uint8_t *out,
                              uint32_t cap, uint32_t *out_len);
    iotspool_err_t (*sync)(void *ctx);
    uint32_t       (*size_bytes)(void *ctx);
    iotspool_err_t (*truncate_to)(void *ctx, uint32_t new_size);
    iotspool_err_t (*replace)(void *ctx, const uint8_t *data, uint32_t len);
} iotspool_store_t;

typedef iotspool_err_t (*iotspool_lock_fn)(void *ctx);

typedef struct {
    uint32_t max_pending_msgs;
    uint32_t max_store_bytes;
    uint32_t max_payload_bytes;
    uint32_t max_topic_bytes;
    uint32_t min_retry_ms;
    uint32_t max_retry_ms;
    bool     enable_sha256;
    bool     drop_oldest_on_full;
    bool     allow_concurrent_acquire;
    void    *lock_ctx;
    iotspool_lock_fn lock;
    iotspool_lock_fn unlock;
} iotspool_cfg_t;

static inline iotspool_cfg_t iotspool_cfg_default(void) {
    iotspool_cfg_t c = {0};
    c.max_pending_msgs = 64;
    c.max_store_bytes = 256u * 1024u;
    c.max_payload_bytes = 4096;
    c.max_topic_bytes = 256;
    c.min_retry_ms = 1000;
    c.max_retry_ms = 60000;
    c.enable_sha256 = false;
    c.drop_oldest_on_full = false;
    c.allow_concurrent_acquire = false;
    c.lock_ctx = NULL;
    c.lock = NULL;
    c.unlock = NULL;
    return c;
}

typedef struct {
    iotspool_msg_id_t id;
    uint32_t          generation;
    uint32_t          record_offset;
    uint32_t          record_len;
    uint32_t          topic_len;
    uint32_t          payload_len;
    uint8_t           qos;
    bool              retain;
} iotspool_entry_t;

typedef struct {
    iotspool_msg_id_t id;
    uint32_t          generation;
    uint16_t          mqtt_packet_id;
    uint8_t           qos;
    bool              retain;
    const char       *topic;
    const uint8_t    *payload;
    uint32_t          payload_len;
} iotspool_inflight_t;

typedef struct {
    uint32_t pending_count;
    uint32_t store_bytes;
    uint32_t enqueued_total;
    uint32_t acked_total;
    uint32_t dropped_total;
    uint32_t corrupt_records;
} iotspool_stats_t;

typedef struct iotspool {
    iotspool_cfg_t   cfg;
    iotspool_store_t store;
    iotspool_state_t state;
    iotspool_entry_t *entries;
    uint32_t         entry_cap;
    uint32_t         entry_count;
    uint32_t         head;
    uint32_t         tail;
    uint64_t         next_id;
    uint32_t         generation;
    uint32_t         superblock_size;
    uint8_t         *scratch;
    size_t           scratch_cap;
    bool             inflight_active;
    uint32_t         inflight_epoch;
    iotspool_inflight_t inflight;
    iotspool_stats_t stats;
    uint32_t         retry_deadline_ms;
    uint32_t         retry_failures;
    uint32_t         retry_seed;
    uint32_t         backoff_cap_ms;
    uint32_t         backoff_retry_after_ms;
    uint32_t         backoff_rng_state;
    bool             initialized;
    bool             owns_workspace;
    void            *owned_workspace;
    void            *owned_index;
    void            *owned_scratch;
} iotspool_t;

size_t iotspool_required_state_bytes(void);
size_t iotspool_required_index_bytes(const iotspool_cfg_t *cfg);
size_t iotspool_required_scratch_bytes(const iotspool_cfg_t *cfg);
size_t iotspool_required_workspace_bytes(const iotspool_cfg_t *cfg);
size_t iotspool_required_compaction_bytes(const iotspool_cfg_t *cfg);

iotspool_err_t iotspool_init_inplace(iotspool_t *s,
                                     iotspool_entry_t *entries,
                                     uint32_t entry_cap,
                                     uint8_t *scratch,
                                     size_t scratch_cap,
                                     const iotspool_cfg_t *cfg,
                                     const iotspool_store_t *store);
iotspool_err_t iotspool_init(iotspool_t **out,
                             const iotspool_cfg_t *cfg,
                             const iotspool_store_t *store);
iotspool_err_t iotspool_recover(iotspool_t *s);
void           iotspool_deinit(iotspool_t *s);

iotspool_err_t iotspool_enqueue(iotspool_t *s, const iotspool_msg_t *m,
                                iotspool_msg_id_t *out_id);
iotspool_err_t iotspool_acquire_ready(iotspool_t *s, uint32_t now_ms,
                                      iotspool_inflight_t *out);
iotspool_err_t iotspool_publish_confirmed(iotspool_t *s,
                                          const iotspool_inflight_t *token);
iotspool_err_t iotspool_publish_failed(iotspool_t *s,
                                       const iotspool_inflight_t *token,
                                       uint32_t now_ms);
iotspool_err_t iotspool_release_or_timeout(iotspool_t *s,
                                           const iotspool_inflight_t *token,
                                           uint32_t now_ms);
iotspool_err_t iotspool_ack(iotspool_t *s, iotspool_msg_id_t id);
iotspool_err_t iotspool_drop_oldest(iotspool_t *s, uint32_t now_ms);
iotspool_err_t iotspool_compact(iotspool_t *s);

void iotspool_stats(const iotspool_t *s, iotspool_stats_t *stats);

/* Legacy convenience wrapper.
 * Concurrency contract:
 * - If lock/unlock are not configured, the caller must serialize all public
 *   stateful operations externally.
 * - If lock/unlock are configured, every public operation that touches shared
 *   state acquires the lock before calling back into the store backend.
 *   Those callbacks must not re-enter iotspool.
 * - ISR context is not supported unless the configured lock callback rejects
 *   it explicitly with IOTSPOOL_EBUSY or IOTSPOOL_ESTATE.
 */
iotspool_err_t iotspool_peek_ready(iotspool_t *s, uint32_t now_ms,
                                   iotspool_msg_t *out,
                                   iotspool_msg_id_t *out_id);
void iotspool_on_publish_fail(iotspool_t *s, uint32_t now_ms);

#ifdef __cplusplus
}
#endif
