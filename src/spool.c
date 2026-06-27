/*
 * spool.c - Core spool engine.
 *
 * This implementation uses caller-owned state/index/scratch buffers for the
 * canonical path. Host convenience allocation is isolated in iotspool_init().
 *
 * SPDX-License-Identifier: MIT
 */

#include "../include/iotspool.h"
#include "record.h"

#include <stdlib.h>
#include <string.h>

enum {
    INITIAL_GENERATION = 1u
};

static void zero_stats(iotspool_stats_t *stats) {
    if (stats) memset(stats, 0, sizeof(*stats));
}

static uint32_t topic_len_bounded(const char *topic, uint32_t max_len) {
    if (!topic) return UINT32_MAX;
    for (uint32_t i = 0; i <= max_len; ++i) {
        if (topic[i] == '\0') return i;
    }
    return UINT32_MAX;
}

static bool cfg_valid(const iotspool_cfg_t *cfg) {
    if (!cfg) return false;
    if (cfg->max_pending_msgs == 0) return false;
    if (cfg->max_topic_bytes == 0) return false;
    if (cfg->max_store_bytes < record_superblock_size() + record_min_record_size(IOTSPOOL_REC_TYPE_ENQUEUE)) {
        return false;
    }
    if (cfg->max_payload_bytes > UINT32_MAX - cfg->max_topic_bytes) {
        return false;
    }
    if (cfg->min_retry_ms > cfg->max_retry_ms) return false;
    if ((cfg->lock == NULL) != (cfg->unlock == NULL)) return false;
#ifndef IOTSPOOL_ENABLE_SHA256
    if (cfg->enable_sha256) return false;
#endif
    return true;
}

static bool store_valid(const iotspool_store_t *store) {
    if (!store || !store->ctx || !store->append || !store->read_at ||
        !store->sync || !store->size_bytes || !store->replace) {
        return false;
    }
    return true;
}

static iotspool_err_t spool_lock_state(iotspool_t *s) {
    if (!s || !s->cfg.lock || !s->cfg.unlock) return IOTSPOOL_OK;
    return s->cfg.lock(s->cfg.lock_ctx);
}

static iotspool_err_t spool_unlock_state(iotspool_t *s) {
    if (!s || !s->cfg.lock || !s->cfg.unlock) return IOTSPOOL_OK;
    return s->cfg.unlock(s->cfg.lock_ctx);
}

static bool store_has_room(const iotspool_t *s, uint32_t need_bytes) {
    if (!s) return false;
    uint64_t used = (uint64_t)s->store.size_bytes(s->store.ctx);
    uint64_t need = (uint64_t)need_bytes;
    uint64_t total = used + need;
    return total <= (uint64_t)s->cfg.max_store_bytes;
}

static void clear_inflight(iotspool_t *s) {
    if (!s) return;
    s->inflight_active = false;
    memset(&s->inflight, 0, sizeof(s->inflight));
}

static void clear_entries(iotspool_t *s) {
    if (!s) return;
    s->entry_count = 0;
    s->head = 0;
    s->tail = 0;
}

static iotspool_entry_t *entry_at(iotspool_t *s, uint32_t idx) {
    if (!s || !s->entries || idx >= s->entry_count) return NULL;
    return &s->entries[idx];
}

static iotspool_entry_t *find_entry(iotspool_t *s, iotspool_msg_id_t id, uint32_t *idx_out) {
    if (!s || !s->entries) return NULL;
    for (uint32_t i = 0; i < s->entry_count; ++i) {
        if (s->entries[i].id == id) {
            if (idx_out) *idx_out = i;
            return &s->entries[i];
        }
    }
    return NULL;
}

static void remove_entry_index(iotspool_t *s, uint32_t idx) {
    if (!s || idx >= s->entry_count) return;
    if (idx + 1u < s->entry_count) {
        memmove(&s->entries[idx], &s->entries[idx + 1u],
                (size_t)(s->entry_count - idx - 1u) * sizeof(s->entries[0]));
    }
    --s->entry_count;
    s->head = 0;
    s->tail = (s->entry_count == 0) ? 0 : (s->entry_count - 1u);
}

static iotspool_err_t write_superblock(iotspool_t *s, uint32_t committed_pos) {
    uint8_t buf[32];
    iotspool_superblock_t sb;
    memset(&sb, 0, sizeof(sb));
    sb.magic = IOTSPOOL_STORE_MAGIC;
    sb.version = IOTSPOOL_STORE_VERSION;
    sb.record_version = IOTSPOOL_RECORD_VERSION;
    sb.generation = s->generation;
    sb.active_identity = 0;
    sb.configured_size = s->cfg.max_store_bytes;
    sb.committed_pos = committed_pos;
    if (record_encode_superblock(buf, sizeof(buf), &sb) != IOTSPOOL_OK) {
        return IOTSPOOL_EINVAL;
    }
    iotspool_err_t err = s->store.append(s->store.ctx, buf, (uint32_t)sizeof(buf));
    if (err != IOTSPOOL_OK) return err;
    return s->store.sync(s->store.ctx);
}

static iotspool_err_t append_record_and_sync(iotspool_t *s, const uint8_t *buf, uint32_t len) {
    if (!store_has_room(s, len)) return IOTSPOOL_EFULL;
    iotspool_err_t err = s->store.append(s->store.ctx, buf, len);
    if (err != IOTSPOOL_OK) return err;
    err = s->store.sync(s->store.ctx);
    if (err != IOTSPOOL_OK) return err;
    return IOTSPOOL_OK;
}

static void backoff_reset_local(iotspool_t *s);
static iotspool_err_t iotspool_drop_oldest_locked(iotspool_t *s, uint32_t now_ms);
static iotspool_err_t iotspool_release_or_timeout_locked(iotspool_t *s,
                                                         const iotspool_inflight_t *token,
                                                         uint32_t now_ms);

static iotspool_err_t compact_to_live_generation(iotspool_t *s) {
    if (!s) return IOTSPOOL_EINVAL;
    if (s->state != IOTSPOOL_STATE_READY) return IOTSPOOL_ESTATE;
    s->state = IOTSPOOL_STATE_COMPACTING;

    size_t snap_cap = s->cfg.max_store_bytes;
    uint8_t *snapshot = (uint8_t *)malloc(snap_cap);
    iotspool_entry_t *new_entries = NULL;
    uint32_t *new_offsets = NULL;
    uint32_t *new_lengths = NULL;
    iotspool_err_t result = IOTSPOOL_OK;
    if (!snapshot) {
        s->state = IOTSPOOL_STATE_READY;
        return IOTSPOOL_ENOMEM;
    }
    new_entries = (iotspool_entry_t *)malloc((size_t)s->entry_count * sizeof(*new_entries));
    new_offsets = (uint32_t *)malloc((size_t)s->entry_count * sizeof(*new_offsets));
    new_lengths = (uint32_t *)malloc((size_t)s->entry_count * sizeof(*new_lengths));
    if ((!new_entries && s->entry_count > 0) || (!new_offsets && s->entry_count > 0) ||
        (!new_lengths && s->entry_count > 0)) {
        result = IOTSPOOL_ENOMEM;
        goto cleanup_compact;
    }

    iotspool_superblock_t sb;
    memset(&sb, 0, sizeof(sb));
    sb.magic = IOTSPOOL_STORE_MAGIC;
    sb.version = IOTSPOOL_STORE_VERSION;
    sb.record_version = IOTSPOOL_RECORD_VERSION;
    sb.generation = s->generation + 1u;
    sb.active_identity = 0;
    sb.configured_size = s->cfg.max_store_bytes;
    sb.committed_pos = (uint32_t)record_superblock_size();
    if (record_encode_superblock(snapshot, (uint32_t)snap_cap, &sb) != IOTSPOOL_OK) {
        result = IOTSPOOL_EINVAL;
        goto cleanup_compact;
    }

    uint32_t pos = (uint32_t)record_superblock_size();
    for (uint32_t i = 0; i < s->entry_count; ++i) {
        iotspool_entry_t *e = &s->entries[i];
        if (e->record_len > s->scratch_cap) {
            result = IOTSPOOL_ENOMEM;
            goto cleanup_compact;
        }

        uint32_t got = 0;
        iotspool_err_t err = s->store.read_at(s->store.ctx, e->record_offset,
                                              s->scratch, e->record_len, &got);
        if (err != IOTSPOOL_OK || got < e->record_len) {
            result = (err != IOTSPOOL_OK) ? err : IOTSPOOL_EIO;
            goto cleanup_compact;
        }

        record_enqueue_t enq = {0};
        uint8_t type = 0;
        uint32_t consumed = 0;
        iotspool_decode_result_t d = record_decode(s->scratch, got, &type,
                                                    &enq, NULL, NULL, &consumed);
        if (d != IOTSPOOL_DEC_VALID || type != IOTSPOOL_REC_TYPE_ENQUEUE) {
            result = IOTSPOOL_ECORRUPT;
            goto cleanup_compact;
        }
        iotspool_msg_t msg = {
            .topic = NULL,
            .payload = enq.payload,
            .payload_len = enq.payload_len,
            .qos = enq.qos,
            .retain = enq.retain
        };
        char *topic_copy = (char *)malloc((size_t)enq.topic_len + 1u);
        if (!topic_copy) {
            result = IOTSPOOL_ENOMEM;
            goto cleanup_compact;
        }
        memcpy(topic_copy, enq.topic, enq.topic_len);
        topic_copy[enq.topic_len] = '\0';
        msg.topic = topic_copy;
        uint32_t rec_len = record_encode_enqueue(snapshot + pos,
                                                 (uint32_t)(snap_cap - pos),
                                                 &msg, e->id, sb.generation,
                                                 enq.timestamp_ms,
                                                 s->cfg.enable_sha256);
        free(topic_copy);
        if (rec_len == 0) {
            result = IOTSPOOL_EFULL;
            goto cleanup_compact;
        }
        new_entries[i] = *e;
        new_entries[i].generation = sb.generation;
        new_offsets[i] = pos;
        new_lengths[i] = rec_len;
        pos += rec_len;
    }

    sb.committed_pos = pos;
    if (record_encode_superblock(snapshot, (uint32_t)snap_cap, &sb) != IOTSPOOL_OK) {
        result = IOTSPOOL_EINVAL;
        goto cleanup_compact;
    }

    iotspool_err_t err = s->store.replace(s->store.ctx, snapshot, pos);
    if (err != IOTSPOOL_OK) {
        result = err;
        goto cleanup_compact;
    }
    err = s->store.sync(s->store.ctx);
    if (err != IOTSPOOL_OK) {
        result = err;
        goto cleanup_compact;
    }

    for (uint32_t i = 0; i < s->entry_count; ++i) {
        s->entries[i] = new_entries[i];
        s->entries[i].record_offset = new_offsets[i];
        s->entries[i].record_len = new_lengths[i];
    }

    s->generation = sb.generation;
    clear_inflight(s);
    backoff_reset_local(s);
cleanup_compact:
    free(snapshot);
    free(new_entries);
    free(new_offsets);
    free(new_lengths);
    s->state = IOTSPOOL_STATE_READY;
    return result;
}

static iotspool_err_t iotspool_compact_locked(iotspool_t *s) {
    return compact_to_live_generation(s);
}

static iotspool_err_t ensure_space_for(iotspool_t *s, uint32_t need_bytes, bool allow_drop) {
    if (!s) return IOTSPOOL_EINVAL;
    if (store_has_room(s, need_bytes)) return IOTSPOOL_OK;

    iotspool_err_t err = compact_to_live_generation(s);
    if (err == IOTSPOOL_OK) {
        if (store_has_room(s, need_bytes)) return IOTSPOOL_OK;
    }

    if (!allow_drop) return IOTSPOOL_EFULL;
    while (s->entry_count > 0) {
        err = iotspool_drop_oldest_locked(s, 0);
        if (err != IOTSPOOL_OK) return err;
        if (store_has_room(s, need_bytes)) return IOTSPOOL_OK;
        err = compact_to_live_generation(s);
        if (err != IOTSPOOL_OK) return err;
        if (store_has_room(s, need_bytes)) return IOTSPOOL_OK;
    }
    return IOTSPOOL_EFULL;
}

static iotspool_err_t encode_record_into_scratch(iotspool_t *s, const iotspool_msg_t *m,
                                                 iotspool_msg_id_t id, uint32_t generation,
                                                 uint32_t timestamp_ms, uint32_t *out_len)
{
    if (!s || !m || !out_len) return IOTSPOOL_EINVAL;
    if (!s->scratch || s->scratch_cap < 1u) return IOTSPOOL_ENOMEM;
    uint32_t len = record_encode_enqueue(s->scratch, (uint32_t)s->scratch_cap,
                                         m, id, generation, timestamp_ms,
                                         s->cfg.enable_sha256);
    if (len == 0) return IOTSPOOL_EINVAL;
    *out_len = len;
    return IOTSPOOL_OK;
}

static iotspool_err_t encode_ack_into_scratch(iotspool_t *s, iotspool_msg_id_t id, uint32_t *out_len) {
    if (!s || !out_len) return IOTSPOOL_EINVAL;
    uint32_t len = record_encode_ack(s->scratch, (uint32_t)s->scratch_cap, id, s->generation);
    if (len == 0) return IOTSPOOL_EINVAL;
    *out_len = len;
    return IOTSPOOL_OK;
}

static iotspool_err_t encode_drop_into_scratch(iotspool_t *s, iotspool_msg_id_t id, uint32_t *out_len) {
    if (!s || !out_len) return IOTSPOOL_EINVAL;
    uint32_t len = record_encode_drop(s->scratch, (uint32_t)s->scratch_cap, id, s->generation);
    if (len == 0) return IOTSPOOL_EINVAL;
    *out_len = len;
    return IOTSPOOL_OK;
}

static uint32_t xorshift32(uint32_t *state) {
    uint32_t x = *state ? *state : 0x9e3779b9u;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x;
    return x;
}

static bool time_after_eq(uint32_t now_ms, uint32_t deadline_ms) {
    return (int32_t)(now_ms - deadline_ms) >= 0;
}

static void backoff_init_local(iotspool_t *s) {
    if (!s) return;
    s->backoff_cap_ms = s->cfg.min_retry_ms;
    s->backoff_retry_after_ms = 0;
    s->backoff_rng_state = 0x243f6a88u;
    s->retry_deadline_ms = 0;
    s->retry_failures = 0;
    s->retry_seed = s->backoff_rng_state;
}

static void backoff_reset_local(iotspool_t *s) {
    if (!s) return;
    s->backoff_cap_ms = s->cfg.min_retry_ms;
    s->backoff_retry_after_ms = 0;
    s->retry_deadline_ms = 0;
    s->retry_failures = 0;
}

static void backoff_fail_local(iotspool_t *s, uint32_t now_ms) {
    if (!s) return;
    uint32_t cap = s->backoff_cap_ms;
    if (cap == 0) cap = s->cfg.min_retry_ms;
    if (cap > s->cfg.max_retry_ms) cap = s->cfg.max_retry_ms;
    uint32_t jitter = (cap == 0) ? 0 : (xorshift32(&s->backoff_rng_state) % (cap + 1u));
    if (jitter < s->cfg.min_retry_ms) jitter = s->cfg.min_retry_ms;
    if (UINT32_MAX - now_ms < jitter) s->backoff_retry_after_ms = UINT32_MAX;
    else s->backoff_retry_after_ms = now_ms + jitter;
    s->retry_deadline_ms = s->backoff_retry_after_ms;
    ++s->retry_failures;
    if (s->backoff_cap_ms < s->cfg.max_retry_ms / 2u) {
        s->backoff_cap_ms *= 2u;
    } else {
        s->backoff_cap_ms = s->cfg.max_retry_ms;
    }
    s->retry_seed = s->backoff_rng_state;
}

static bool backoff_ready_local(const iotspool_t *s, uint32_t now_ms) {
    if (!s) return false;
    return time_after_eq(now_ms, s->backoff_retry_after_ms);
}

static iotspool_err_t remove_by_id(iotspool_t *s, iotspool_msg_id_t id) {
    uint32_t idx = 0;
    if (!find_entry(s, id, &idx)) return IOTSPOOL_ENOTFOUND;
    remove_entry_index(s, idx);
    return IOTSPOOL_OK;
}

static iotspool_err_t ensure_ready_state(const iotspool_t *s) {
    if (!s) return IOTSPOOL_EINVAL;
    if (s->state != IOTSPOOL_STATE_READY) return IOTSPOOL_ESTATE;
    return IOTSPOOL_OK;
}

size_t iotspool_required_state_bytes(void) {
    return sizeof(iotspool_t);
}

size_t iotspool_required_index_bytes(const iotspool_cfg_t *cfg) {
    if (!cfg) return 0;
    return (size_t)cfg->max_pending_msgs * sizeof(iotspool_entry_t);
}

size_t iotspool_required_scratch_bytes(const iotspool_cfg_t *cfg) {
    if (!cfg) return 0;
    return (size_t)record_header_size() + (size_t)cfg->max_topic_bytes +
           (size_t)cfg->max_payload_bytes + 32u + 4u;
}

size_t iotspool_required_workspace_bytes(const iotspool_cfg_t *cfg) {
    return iotspool_required_state_bytes() + iotspool_required_index_bytes(cfg) +
           iotspool_required_scratch_bytes(cfg);
}

size_t iotspool_required_compaction_bytes(const iotspool_cfg_t *cfg) {
    if (!cfg) return 0;
    return (size_t)cfg->max_store_bytes;
}

const char *iotspool_strerror(iotspool_err_t err) {
    switch (err) {
        case IOTSPOOL_OK:        return "OK";
        case IOTSPOOL_EINVAL:    return "Invalid argument";
        case IOTSPOOL_EIO:       return "I/O error";
        case IOTSPOOL_ECORRUPT:  return "Store corruption detected";
        case IOTSPOOL_EFULL:     return "Queue or store full";
        case IOTSPOOL_ENOTFOUND: return "Message not found";
        case IOTSPOOL_ESTATE:    return "Invalid lifecycle state";
        case IOTSPOOL_ENOMEM:    return "Out of memory";
        case IOTSPOOL_EAGAIN:    return "Try again later";
        case IOTSPOOL_EBUSY:     return "Busy";
        default:                 return "Unknown error";
    }
}

iotspool_err_t iotspool_init_inplace(iotspool_t *s,
                                     iotspool_entry_t *entries,
                                     uint32_t entry_cap,
                                     uint8_t *scratch,
                                     size_t scratch_cap,
                                     const iotspool_cfg_t *cfg,
                                     const iotspool_store_t *store)
{
    if (!s || !entries || !scratch || !cfg || !store) return IOTSPOOL_EINVAL;
    if (!cfg_valid(cfg) || !store_valid(store)) return IOTSPOOL_EINVAL;
    if (entry_cap < cfg->max_pending_msgs) return IOTSPOOL_EINVAL;
    if (scratch_cap < iotspool_required_scratch_bytes(cfg)) return IOTSPOOL_EINVAL;

    memset(s, 0, sizeof(*s));
    s->cfg = *cfg;
    s->store = *store;
    s->state = IOTSPOOL_STATE_INITIALIZED;
    s->entries = entries;
    s->entry_cap = entry_cap;
    s->scratch = scratch;
    s->scratch_cap = scratch_cap;
    s->generation = INITIAL_GENERATION;
    s->next_id = 1u;
    s->superblock_size = (uint32_t)record_superblock_size();
    s->retry_seed = 0x9e3779b9u;
    backoff_init_local(s);
    zero_stats(&s->stats);
    s->initialized = true;
    s->owns_workspace = false;
    return IOTSPOOL_OK;
}

iotspool_err_t iotspool_init(iotspool_t **out,
                             const iotspool_cfg_t *cfg,
                             const iotspool_store_t *store)
{
    if (!out) return IOTSPOOL_EINVAL;
    *out = NULL;
    if (!cfg_valid(cfg) || !store_valid(store)) return IOTSPOOL_EINVAL;

    iotspool_t *s = (iotspool_t *)calloc(1, sizeof(*s));
    if (!s) return IOTSPOOL_ENOMEM;

    size_t index_bytes = iotspool_required_index_bytes(cfg);
    size_t scratch_bytes = iotspool_required_scratch_bytes(cfg);
    s->entries = (iotspool_entry_t *)calloc(1, index_bytes);
    s->scratch = (uint8_t *)calloc(1, scratch_bytes);
    if (!s->entries || !s->scratch) {
        free(s->entries);
        free(s->scratch);
        free(s);
        return IOTSPOOL_ENOMEM;
    }

    s->entry_cap = cfg->max_pending_msgs;
    s->scratch_cap = scratch_bytes;
    s->cfg = *cfg;
    s->store = *store;
    s->state = IOTSPOOL_STATE_INITIALIZED;
    s->generation = INITIAL_GENERATION;
    s->next_id = 1u;
    s->superblock_size = (uint32_t)record_superblock_size();
    s->owns_workspace = true;
    s->owned_workspace = s;
    s->owned_index = s->entries;
    s->owned_scratch = s->scratch;
    backoff_init_local(s);
    s->initialized = true;

    *out = s;
    return IOTSPOOL_OK;
}

void iotspool_deinit(iotspool_t *s) {
    if (!s) return;
    if (s->owns_workspace) {
        free(s->owned_index);
        free(s->owned_scratch);
        free(s->owned_workspace);
        return;
    }
    memset(s, 0, sizeof(*s));
}

static iotspool_err_t rebuild_from_store(iotspool_t *s, uint32_t size) {
    uint32_t off = s->superblock_size;
    uint32_t clean_end = off;
    iotspool_superblock_t sb;
    uint8_t sbuf[32];
    uint32_t got = 0;

    if (size == 0) {
        iotspool_err_t err = write_superblock(s, s->superblock_size);
        if (err != IOTSPOOL_OK) return err;
        clear_entries(s);
        clear_inflight(s);
        s->state = IOTSPOOL_STATE_READY;
        return IOTSPOOL_OK;
    }

    if (size < s->superblock_size) return IOTSPOOL_ECORRUPT;

    iotspool_err_t err = s->store.read_at(s->store.ctx, 0, sbuf, sizeof(sbuf), &got);
    if (err != IOTSPOOL_OK || got < sizeof(sbuf)) return IOTSPOOL_EIO;
    iotspool_decode_result_t d = record_decode_superblock(sbuf, got, &sb);
    if (d != IOTSPOOL_DEC_VALID) return (d == IOTSPOOL_DEC_UNSUPPORTED_VERSION) ? IOTSPOOL_EINVAL : IOTSPOOL_ECORRUPT;

    s->generation = sb.generation;
    clear_entries(s);
    clear_inflight(s);
    s->next_id = 1u;
    s->stats.corrupt_records = 0;

    off = s->superblock_size;
    while (off < size) {
        uint32_t remaining = size - off;
        uint32_t read_cap = (remaining < s->scratch_cap) ? remaining : (uint32_t)s->scratch_cap;
        err = s->store.read_at(s->store.ctx, off, s->scratch, read_cap, &got);
        if (err != IOTSPOOL_OK) return err;
        if (got == 0) break;

        uint8_t type = 0;
        record_enqueue_t enq = {0};
        record_ack_t ack = {0};
        record_drop_t drop = {0};
        uint32_t consumed = 0;
        d = record_decode(s->scratch, got, &type, &enq, &ack, &drop, &consumed);

        if (d == IOTSPOOL_DEC_INCOMPLETE_TAIL || d == IOTSPOOL_DEC_CLEAN_END) {
            clean_end = off;
            break;
        }
        if (d != IOTSPOOL_DEC_VALID) {
            s->stats.corrupt_records++;
            s->state = IOTSPOOL_STATE_FAILED;
            return (d == IOTSPOOL_DEC_UNSUPPORTED_VERSION) ? IOTSPOOL_EINVAL : IOTSPOOL_ECORRUPT;
        }
        if (consumed == 0 || consumed > got) return IOTSPOOL_ECORRUPT;
        if (enq.generation != s->generation &&
            type == IOTSPOOL_REC_TYPE_ENQUEUE) {
            s->state = IOTSPOOL_STATE_FAILED;
            return IOTSPOOL_ECORRUPT;
        }

        if (type == IOTSPOOL_REC_TYPE_ENQUEUE) {
            if (s->entry_count >= s->entry_cap) return IOTSPOOL_EFULL;
            iotspool_entry_t *e = &s->entries[s->entry_count++];
            e->id = enq.msg_id;
            e->generation = enq.generation;
            e->record_offset = off;
            e->record_len = consumed;
            e->topic_len = enq.topic_len;
            e->payload_len = enq.payload_len;
            e->qos = enq.qos;
            e->retain = enq.retain;
            s->next_id = (enq.msg_id >= s->next_id) ? (enq.msg_id + 1u) : s->next_id;
            s->stats.enqueued_total++;
        } else if (type == IOTSPOOL_REC_TYPE_ACK || type == IOTSPOOL_REC_TYPE_DROP) {
            uint32_t idx = 0;
            if (find_entry(s, (type == IOTSPOOL_REC_TYPE_ACK) ? ack.msg_id : drop.msg_id, &idx)) {
                remove_entry_index(s, idx);
                if (type == IOTSPOOL_REC_TYPE_ACK) s->stats.acked_total++;
                else s->stats.dropped_total++;
            }
        }

        off += consumed;
        clean_end = off;
    }

    if (clean_end < size) {
        if (!s->store.truncate_to) return IOTSPOOL_EIO;
        err = s->store.truncate_to(s->store.ctx, clean_end);
        if (err != IOTSPOOL_OK) return err;
        err = s->store.sync(s->store.ctx);
        if (err != IOTSPOOL_OK) return err;
    }

    s->state = IOTSPOOL_STATE_READY;
    return IOTSPOOL_OK;
}

static iotspool_err_t iotspool_recover_locked(iotspool_t *s) {
    if (!s) return IOTSPOOL_EINVAL;
    if (s->state == IOTSPOOL_STATE_READY) return IOTSPOOL_ESTATE;
    s->state = IOTSPOOL_STATE_RECOVERING;
    uint32_t size = s->store.size_bytes(s->store.ctx);
    iotspool_err_t err = rebuild_from_store(s, size);
    if (err != IOTSPOOL_OK) {
        s->state = IOTSPOOL_STATE_FAILED;
        return err;
    }
    return IOTSPOOL_OK;
}

static iotspool_err_t iotspool_enqueue_locked(iotspool_t *s, const iotspool_msg_t *m,
                                iotspool_msg_id_t *out_id)
{
    if (!s || !m) return IOTSPOOL_EINVAL;
    iotspool_err_t st = ensure_ready_state(s);
    if (st != IOTSPOOL_OK) return st;
    if (m->qos > 1u) return IOTSPOOL_EINVAL;
    if (m->payload_len > 0 && !m->payload) return IOTSPOOL_EINVAL;

    uint32_t topic_len = topic_len_bounded(m->topic, s->cfg.max_topic_bytes);
    if (topic_len == 0 || topic_len == UINT32_MAX) return IOTSPOOL_EINVAL;
    if (m->payload_len > s->cfg.max_payload_bytes) return IOTSPOOL_EINVAL;
    if (s->entry_count >= s->entry_cap) {
        if (!s->cfg.drop_oldest_on_full) return IOTSPOOL_EFULL;
        st = iotspool_drop_oldest_locked(s, 0);
        if (st != IOTSPOOL_OK) return st;
    }
    if ((uint32_t)s->store.size_bytes(s->store.ctx) >= s->cfg.max_store_bytes) {
        if (!s->cfg.drop_oldest_on_full) return IOTSPOOL_EFULL;
        st = iotspool_drop_oldest_locked(s, 0);
        if (st != IOTSPOOL_OK) return st;
    }

    uint32_t enc_len = 0;
    iotspool_msg_id_t id = s->next_id;
    st = encode_record_into_scratch(s, m, id, s->generation, 0, &enc_len);
    if (st != IOTSPOOL_OK) return st;
    st = ensure_space_for(s, enc_len, s->cfg.drop_oldest_on_full);
    if (st != IOTSPOOL_OK) return st;
    st = append_record_and_sync(s, s->scratch, enc_len);
    if (st != IOTSPOOL_OK) return st;
    s->next_id = id + 1u;

    iotspool_entry_t *e = &s->entries[s->entry_count++];
    e->id = id;
    e->generation = s->generation;
    e->record_offset = s->store.size_bytes(s->store.ctx) - enc_len;
    e->record_len = enc_len;
    e->topic_len = topic_len;
    e->payload_len = m->payload_len;
    e->qos = m->qos;
    e->retain = m->retain;
    s->head = 0;
    s->tail = s->entry_count - 1u;
    s->stats.enqueued_total++;
    if (out_id) *out_id = id;
    return IOTSPOOL_OK;
}

static iotspool_err_t iotspool_drop_oldest_locked(iotspool_t *s, uint32_t now_ms) {
    (void)now_ms;
    if (!s) return IOTSPOOL_EINVAL;
    iotspool_err_t st = ensure_ready_state(s);
    if (st != IOTSPOOL_OK) return st;
    if (s->entry_count == 0) return IOTSPOOL_ENOTFOUND;

    iotspool_entry_t victim = s->entries[0];
    uint32_t enc_len = 0;
    st = encode_drop_into_scratch(s, victim.id, &enc_len);
    if (st != IOTSPOOL_OK) return st;
    st = ensure_space_for(s, enc_len, false);
    if (st != IOTSPOOL_OK) return st;
    st = append_record_and_sync(s, s->scratch, enc_len);
    if (st != IOTSPOOL_OK) return st;
    remove_entry_index(s, 0);
    s->stats.dropped_total++;
    return IOTSPOOL_OK;
}

static iotspool_err_t iotspool_acquire_ready_locked(iotspool_t *s, uint32_t now_ms,
                                      iotspool_inflight_t *out)
{
    if (!s || !out) return IOTSPOOL_EINVAL;
    iotspool_err_t st = ensure_ready_state(s);
    if (st != IOTSPOOL_OK) return st;
    if (s->entry_count == 0) return IOTSPOOL_ENOTFOUND;
    if (s->inflight_active && !s->cfg.allow_concurrent_acquire) return IOTSPOOL_EBUSY;
    if (!backoff_ready_local(s, now_ms)) return IOTSPOOL_EAGAIN;

    iotspool_entry_t *e = entry_at(s, 0);
    if (!e) return IOTSPOOL_ECORRUPT;
    if (e->record_len > s->scratch_cap) return IOTSPOOL_ENOMEM;

    uint32_t got = 0;
    st = s->store.read_at(s->store.ctx, e->record_offset, s->scratch, e->record_len, &got);
    if (st != IOTSPOOL_OK) return st;
    if (got < e->record_len) return IOTSPOOL_EIO;

    record_enqueue_t enq = {0};
    uint8_t type = 0;
    uint32_t consumed = 0;
    iotspool_decode_result_t d = record_decode(s->scratch, got, &type, &enq, NULL, NULL, &consumed);
    if (d != IOTSPOOL_DEC_VALID || type != IOTSPOOL_REC_TYPE_ENQUEUE || consumed != e->record_len) {
        return IOTSPOOL_ECORRUPT;
    }

    s->inflight_active = true;
    s->inflight_epoch++;
    s->inflight.id = e->id;
    s->inflight.generation = s->inflight_epoch;
    s->inflight.mqtt_packet_id = (uint16_t)(e->id & 0xffffu);
    s->inflight.qos = e->qos;
    s->inflight.retain = e->retain;
    s->inflight.topic = enq.topic;
    s->inflight.payload = enq.payload;
    s->inflight.payload_len = enq.payload_len;
    *out = s->inflight;
    return IOTSPOOL_OK;
}

static bool inflight_matches(const iotspool_t *s, const iotspool_inflight_t *token) {
    return s && token && s->inflight_active &&
           s->inflight.id == token->id &&
           s->inflight.generation == token->generation;
}

static iotspool_err_t iotspool_publish_confirmed_locked(iotspool_t *s,
                                          const iotspool_inflight_t *token)
{
    if (!s || !token) return IOTSPOOL_EINVAL;
    iotspool_err_t st = ensure_ready_state(s);
    if (st != IOTSPOOL_OK) return st;
    if (!inflight_matches(s, token)) return IOTSPOOL_ESTATE;

    uint32_t enc_len = 0;
    st = encode_ack_into_scratch(s, token->id, &enc_len);
    if (st != IOTSPOOL_OK) return st;
    st = append_record_and_sync(s, s->scratch, enc_len);
    if (st != IOTSPOOL_OK) return st;

    st = remove_by_id(s, token->id);
    if (st != IOTSPOOL_OK) return st;
    clear_inflight(s);
    backoff_reset_local(s);
    s->stats.acked_total++;
    return IOTSPOOL_OK;
}

static iotspool_err_t iotspool_publish_failed_locked(iotspool_t *s,
                                       const iotspool_inflight_t *token,
                                       uint32_t now_ms)
{
    if (!s || !token) return IOTSPOOL_EINVAL;
    if (!inflight_matches(s, token)) return IOTSPOOL_ESTATE;
    backoff_fail_local(s, now_ms);
    return IOTSPOOL_OK;
}

static iotspool_err_t iotspool_release_or_timeout_locked(iotspool_t *s,
                                           const iotspool_inflight_t *token,
                                           uint32_t now_ms)
{
    if (!s || !token) return IOTSPOOL_EINVAL;
    if (!inflight_matches(s, token)) return IOTSPOOL_ESTATE;
    if (!backoff_ready_local(s, now_ms)) {
        return IOTSPOOL_EAGAIN;
    }
    clear_inflight(s);
    return IOTSPOOL_OK;
}

static iotspool_err_t iotspool_ack_locked(iotspool_t *s, iotspool_msg_id_t id) {
    if (!s) return IOTSPOOL_EINVAL;
    iotspool_err_t st = ensure_ready_state(s);
    if (st != IOTSPOOL_OK) return st;

    uint32_t idx = 0;
    iotspool_entry_t *e = find_entry(s, id, &idx);
    if (!e) return IOTSPOOL_ENOTFOUND;

    uint32_t enc_len = 0;
    st = encode_ack_into_scratch(s, id, &enc_len);
    if (st != IOTSPOOL_OK) return st;
    st = ensure_space_for(s, enc_len, false);
    if (st != IOTSPOOL_OK) return st;
    st = append_record_and_sync(s, s->scratch, enc_len);
    if (st != IOTSPOOL_OK) return st;

    remove_entry_index(s, idx);
    if (s->inflight_active && s->inflight.id == id) clear_inflight(s);
    backoff_reset_local(s);
    s->stats.acked_total++;
    (void)e;
    return IOTSPOOL_OK;
}

static void iotspool_stats_locked(const iotspool_t *s, iotspool_stats_t *stats) {
    if (!s || !stats) return;
    *stats = s->stats;
    stats->pending_count = s->entry_count;
    stats->store_bytes = s->store.size_bytes ? s->store.size_bytes(s->store.ctx) : 0u;
}

static iotspool_err_t iotspool_peek_ready_locked(iotspool_t *s, uint32_t now_ms,
                                   iotspool_msg_t *out,
                                   iotspool_msg_id_t *out_id)
{
    if (!s || !out || !out_id) return IOTSPOOL_EINVAL;
    iotspool_inflight_t token = {0};
    iotspool_err_t st = iotspool_acquire_ready_locked(s, now_ms, &token);
    if (st != IOTSPOOL_OK) return st;
    out->topic = token.topic;
    out->payload = token.payload;
    out->payload_len = token.payload_len;
    out->qos = token.qos;
    out->retain = token.retain;
    *out_id = token.id;
    (void)iotspool_release_or_timeout_locked(s, &token, now_ms);
    return IOTSPOOL_OK;
}

static void iotspool_on_publish_fail_locked(iotspool_t *s, uint32_t now_ms) {
    if (!s) return;
    if (!s->inflight_active) return;
    (void)now_ms;
    backoff_fail_local(s, now_ms);
}

iotspool_err_t iotspool_compact(iotspool_t *s) {
    iotspool_err_t err = spool_lock_state(s);
    if (err != IOTSPOOL_OK) return err;
    err = iotspool_compact_locked(s);
    iotspool_err_t unlock_err = spool_unlock_state(s);
    if (unlock_err != IOTSPOOL_OK) return (err == IOTSPOOL_OK) ? unlock_err : err;
    return err;
}

iotspool_err_t iotspool_recover(iotspool_t *s) {
    iotspool_err_t err = spool_lock_state(s);
    if (err != IOTSPOOL_OK) return err;
    err = iotspool_recover_locked(s);
    iotspool_err_t unlock_err = spool_unlock_state(s);
    if (unlock_err != IOTSPOOL_OK) return (err == IOTSPOOL_OK) ? unlock_err : err;
    return err;
}

iotspool_err_t iotspool_enqueue(iotspool_t *s, const iotspool_msg_t *m,
                                iotspool_msg_id_t *out_id)
{
    iotspool_err_t err = spool_lock_state(s);
    if (err != IOTSPOOL_OK) return err;
    err = iotspool_enqueue_locked(s, m, out_id);
    iotspool_err_t unlock_err = spool_unlock_state(s);
    if (unlock_err != IOTSPOOL_OK) return (err == IOTSPOOL_OK) ? unlock_err : err;
    return err;
}

iotspool_err_t iotspool_drop_oldest(iotspool_t *s, uint32_t now_ms) {
    iotspool_err_t err = spool_lock_state(s);
    if (err != IOTSPOOL_OK) return err;
    err = iotspool_drop_oldest_locked(s, now_ms);
    iotspool_err_t unlock_err = spool_unlock_state(s);
    if (unlock_err != IOTSPOOL_OK) return (err == IOTSPOOL_OK) ? unlock_err : err;
    return err;
}

iotspool_err_t iotspool_acquire_ready(iotspool_t *s, uint32_t now_ms,
                                      iotspool_inflight_t *out)
{
    iotspool_err_t err = spool_lock_state(s);
    if (err != IOTSPOOL_OK) return err;
    err = iotspool_acquire_ready_locked(s, now_ms, out);
    iotspool_err_t unlock_err = spool_unlock_state(s);
    if (unlock_err != IOTSPOOL_OK) return (err == IOTSPOOL_OK) ? unlock_err : err;
    return err;
}

iotspool_err_t iotspool_publish_confirmed(iotspool_t *s,
                                          const iotspool_inflight_t *token)
{
    iotspool_err_t err = spool_lock_state(s);
    if (err != IOTSPOOL_OK) return err;
    err = iotspool_publish_confirmed_locked(s, token);
    iotspool_err_t unlock_err = spool_unlock_state(s);
    if (unlock_err != IOTSPOOL_OK) return (err == IOTSPOOL_OK) ? unlock_err : err;
    return err;
}

iotspool_err_t iotspool_publish_failed(iotspool_t *s,
                                       const iotspool_inflight_t *token,
                                       uint32_t now_ms)
{
    iotspool_err_t err = spool_lock_state(s);
    if (err != IOTSPOOL_OK) return err;
    err = iotspool_publish_failed_locked(s, token, now_ms);
    iotspool_err_t unlock_err = spool_unlock_state(s);
    if (unlock_err != IOTSPOOL_OK) return (err == IOTSPOOL_OK) ? unlock_err : err;
    return err;
}

iotspool_err_t iotspool_release_or_timeout(iotspool_t *s,
                                           const iotspool_inflight_t *token,
                                           uint32_t now_ms)
{
    iotspool_err_t err = spool_lock_state(s);
    if (err != IOTSPOOL_OK) return err;
    err = iotspool_release_or_timeout_locked(s, token, now_ms);
    iotspool_err_t unlock_err = spool_unlock_state(s);
    if (unlock_err != IOTSPOOL_OK) return (err == IOTSPOOL_OK) ? unlock_err : err;
    return err;
}

iotspool_err_t iotspool_ack(iotspool_t *s, iotspool_msg_id_t id) {
    iotspool_err_t err = spool_lock_state(s);
    if (err != IOTSPOOL_OK) return err;
    err = iotspool_ack_locked(s, id);
    iotspool_err_t unlock_err = spool_unlock_state(s);
    if (unlock_err != IOTSPOOL_OK) return (err == IOTSPOOL_OK) ? unlock_err : err;
    return err;
}

void iotspool_stats(const iotspool_t *s, iotspool_stats_t *stats) {
    if (!s || !stats) return;
    iotspool_t *mutable_s = (iotspool_t *)s;
    if (spool_lock_state(mutable_s) != IOTSPOOL_OK) return;
    iotspool_stats_locked(s, stats);
    (void)spool_unlock_state(mutable_s);
}

iotspool_err_t iotspool_peek_ready(iotspool_t *s, uint32_t now_ms,
                                   iotspool_msg_t *out,
                                   iotspool_msg_id_t *out_id)
{
    iotspool_err_t err = spool_lock_state(s);
    if (err != IOTSPOOL_OK) return err;
    err = iotspool_peek_ready_locked(s, now_ms, out, out_id);
    iotspool_err_t unlock_err = spool_unlock_state(s);
    if (unlock_err != IOTSPOOL_OK) return (err == IOTSPOOL_OK) ? unlock_err : err;
    return err;
}

void iotspool_on_publish_fail(iotspool_t *s, uint32_t now_ms) {
    if (!s) return;
    if (spool_lock_state(s) != IOTSPOOL_OK) return;
    iotspool_on_publish_fail_locked(s, now_ms);
    (void)spool_unlock_state(s);
}
