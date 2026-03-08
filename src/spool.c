/* spool.c – Core spool engine: RAM index, enqueue, peek, ack, recovery.
 * SPDX-License-Identifier: MIT */

#include "../include/iotspool.h"
#include "record.h"
#include "backoff.h"
#include <stdlib.h>
#include <string.h>

/* ── Internal pending entry ────────────────────────────────────────────── */
typedef struct pending_entry {
    iotspool_msg_id_t  id;
    char              *topic;      /* heap-owned copy */
    uint8_t           *payload;    /* heap-owned copy */
    uint32_t           payload_len;
    uint8_t            qos;
    bool               retain;
    uint32_t           timestamp_ms;
    struct pending_entry *next;
} pending_entry_t;

/* ── Spool state ────────────────────────────────────────────────────────── */
typedef enum { STATE_INIT, STATE_READY } spool_state_t;

struct iotspool {
    iotspool_cfg_t   cfg;
    iotspool_store_t store;
    backoff_t        backoff;
    spool_state_t    state;

    /* Singly-linked pending queue (FIFO via tail pointer) */
    pending_entry_t *head;
    pending_entry_t *tail;
    uint32_t         pending_count;

    /* Next message ID to assign */
    iotspool_msg_id_t next_id;

    /* Stats */
    iotspool_stats_t stats;
};

/* ── Helpers ────────────────────────────────────────────────────────────── */
static pending_entry_t *find_entry(iotspool_t *s, iotspool_msg_id_t id) {
    for (pending_entry_t *e = s->head; e; e = e->next)
        if (e->id == id) return e;
    return NULL;
}

static void free_entry(pending_entry_t *e) {
    if (!e) return;
    free(e->topic);
    free(e->payload);
    free(e);
}

static pending_entry_t *remove_head(iotspool_t *s) {
    if (!s->head) return NULL;
    pending_entry_t *e = s->head;
    s->head = e->next;
    if (!s->head) s->tail = NULL;
    s->pending_count--;
    return e;
}

/* ── Public API ─────────────────────────────────────────────────────────── */

const char *iotspool_strerror(iotspool_err_t err) {
    switch (err) {
        case IOTSPOOL_OK:        return "OK";
        case IOTSPOOL_EINVAL:    return "Invalid argument";
        case IOTSPOOL_EIO:       return "I/O error";
        case IOTSPOOL_ECORRUPT:  return "Store corruption detected";
        case IOTSPOOL_EFULL:     return "Queue or store full";
        case IOTSPOOL_ENOTFOUND: return "Message ID not found";
        case IOTSPOOL_ESTATE:    return "Invalid lifecycle state";
        case IOTSPOOL_ENOMEM:    return "Out of memory";
        default:                 return "Unknown error";
    }
}

iotspool_err_t iotspool_init(iotspool_t **out,
                              const iotspool_cfg_t   *cfg,
                              const iotspool_store_t *store)
{
    if (!out || !cfg || !store) return IOTSPOOL_EINVAL;
    if (!store->append || !store->read_at || !store->sync || !store->size_bytes)
        return IOTSPOOL_EINVAL;
    if (cfg->max_pending_msgs == 0) return IOTSPOOL_EINVAL;

    iotspool_t *s = (iotspool_t *)calloc(1, sizeof(iotspool_t));
    if (!s) return IOTSPOOL_ENOMEM;

    s->cfg     = *cfg;
    s->store   = *store;
    s->state   = STATE_INIT;
    s->next_id = 1;
    backoff_init(&s->backoff, cfg->min_retry_ms, cfg->max_retry_ms);

    *out = s;
    return IOTSPOOL_OK;
}

void iotspool_deinit(iotspool_t *s) {
    if (!s) return;
    pending_entry_t *e = s->head;
    while (e) {
        pending_entry_t *next = e->next;
        free_entry(e);
        e = next;
    }
    free(s);
}

/* ── Recovery ───────────────────────────────────────────────────────────── */
iotspool_err_t iotspool_recover(iotspool_t *s) {
    if (!s) return IOTSPOOL_EINVAL;
    if (s->state != STATE_INIT) return IOTSPOOL_ESTATE;

    uint32_t store_size = s->store.size_bytes(s->store.ctx);
    uint32_t offset     = 0;

    /* Read buffer – one record at a time via sliding window */
    uint8_t  hdr_buf[RECORD_ENQ_HDR_SIZE];
    uint8_t *rec_buf   = NULL;
    uint32_t rec_cap   = 0;

    /* First pass: build two lists: enqueued IDs and acked IDs */
    /* We re-build the pending list on the fly */

    while (offset < store_size) {
        uint32_t got = 0;
        /* Read minimal header to determine record size */
        iotspool_err_t err = s->store.read_at(s->store.ctx, offset,
                                               hdr_buf, sizeof(hdr_buf), &got);
        if (err != IOTSPOOL_OK || got == 0) break;
        if (got < 4) break; /* incomplete – tail truncation */

        if (hdr_buf[0] != RECORD_MAGIC) {
            s->stats.corrupt_records++;
            break; /* corrupted – stop scan, discard tail */
        }

        uint8_t type = hdr_buf[1];
        uint32_t rec_size = 0;

        if (type == RECORD_ACK) {
            rec_size = RECORD_ACK_SIZE;
        } else if (type == RECORD_ENQ) {
            if (got < RECORD_ENQ_HDR_SIZE) break;
            uint32_t tl = ((uint32_t)hdr_buf[12])       |
                          ((uint32_t)hdr_buf[13] << 8)   |
                          ((uint32_t)hdr_buf[14] << 16)  |
                          ((uint32_t)hdr_buf[15] << 24);
            uint32_t pl = ((uint32_t)hdr_buf[16])       |
                          ((uint32_t)hdr_buf[17] << 8)   |
                          ((uint32_t)hdr_buf[18] << 16)  |
                          ((uint32_t)hdr_buf[19] << 24);
            uint8_t flags = hdr_buf[3];
            bool has_sha  = (flags & RECORD_FLAG_SHA256) != 0;
            rec_size = RECORD_ENQ_HDR_SIZE + tl + pl +
                       (has_sha ? RECORD_SHA256_SIZE : 0) + RECORD_CRC_SIZE;
        } else {
            s->stats.corrupt_records++;
            break;
        }

        if (offset + rec_size > store_size) break; /* incomplete tail */

        /* Load full record into buffer */
        if (rec_size > rec_cap) {
            free(rec_buf);
            rec_buf = (uint8_t *)malloc(rec_size);
            if (!rec_buf) { s->state = STATE_READY; return IOTSPOOL_ENOMEM; }
            rec_cap = rec_size;
        }
        err = s->store.read_at(s->store.ctx, offset, rec_buf, rec_size, &got);
        if (err != IOTSPOOL_OK || got < rec_size) break;

        uint8_t      decoded_type = 0;
        record_enq_t enq = {0};
        record_ack_t ack = {0};
        uint32_t consumed = record_decode(rec_buf, rec_size,
                                          &decoded_type, &enq, &ack);
        if (consumed == 0) {
            s->stats.corrupt_records++;
            break; /* bad record – stop */
        }

        if (decoded_type == RECORD_ENQ) {
            /* Add to pending queue if not already there */
            if (!find_entry(s, enq.msg_id) &&
                s->pending_count < s->cfg.max_pending_msgs)
            {
                pending_entry_t *e = (pending_entry_t *)calloc(1, sizeof(*e));
                if (!e) break;
                e->id          = enq.msg_id;
                e->payload_len = enq.payload_len;
                e->qos         = (enq.flags & RECORD_FLAG_QOS1) ? 1 : 0;
                e->retain      = (enq.flags & RECORD_FLAG_RETAIN) != 0;
                e->timestamp_ms= enq.timestamp_ms;

                e->topic = (char *)malloc(enq.topic_len + 1);
                if (!e->topic) { free(e); break; }
                memcpy(e->topic, enq.topic, enq.topic_len);
                e->topic[enq.topic_len] = '\0';

                if (enq.payload_len > 0) {
                    e->payload = (uint8_t *)malloc(enq.payload_len);
                    if (!e->payload) { free(e->topic); free(e); break; }
                    memcpy(e->payload, enq.payload, enq.payload_len);
                }

                /* Append to tail */
                if (s->tail) s->tail->next = e;
                else         s->head = e;
                s->tail = e;
                s->pending_count++;
                s->stats.enqueued_total++;

                /* Track next_id */
                if (enq.msg_id >= s->next_id)
                    s->next_id = enq.msg_id + 1;
            }
        } else if (decoded_type == RECORD_ACK) {
            /* Remove matching pending entry */
            if (s->head && s->head->id == ack.msg_id) {
                pending_entry_t *e = remove_head(s);
                free_entry(e);
                s->stats.acked_total++;
            } else {
                /* Scan list */
                for (pending_entry_t *prev = s->head; prev && prev->next; prev = prev->next) {
                    if (prev->next->id == ack.msg_id) {
                        pending_entry_t *e = prev->next;
                        prev->next = e->next;
                        if (s->tail == e) s->tail = prev;
                        s->pending_count--;
                        free_entry(e);
                        s->stats.acked_total++;
                        break;
                    }
                }
            }
        }

        offset += rec_size;
    }

    free(rec_buf);
    s->state = STATE_READY;
    return IOTSPOOL_OK;
}

/* ── Enqueue ─────────────────────────────────────────────────────────────── */
iotspool_err_t iotspool_enqueue(iotspool_t *s, const iotspool_msg_t *m,
                                 iotspool_msg_id_t *out_id)
{
    if (!s || !m || !m->topic) return IOTSPOOL_EINVAL;
    if (s->state != STATE_READY) return IOTSPOOL_ESTATE;
    if (m->qos > 1) return IOTSPOOL_EINVAL;

    uint32_t tl = (uint32_t)strlen(m->topic);
    if (tl == 0 || tl > s->cfg.max_topic_bytes)   return IOTSPOOL_EINVAL;
    if (m->payload_len > s->cfg.max_payload_bytes) return IOTSPOOL_EINVAL;

    /* Capacity check */
    if (s->pending_count >= s->cfg.max_pending_msgs ||
        (s->cfg.max_store_bytes > 0 &&
         s->store.size_bytes(s->store.ctx) >= s->cfg.max_store_bytes))
    {
        if (s->cfg.drop_oldest_on_full && s->head) {
            pending_entry_t *e = remove_head(s);
            free_entry(e);
            s->stats.dropped_total++;
        } else {
            return IOTSPOOL_EFULL;
        }
    }

    /* Encode to store */
    uint32_t enc_cap = RECORD_ENQ_HDR_SIZE + tl + m->payload_len +
                       RECORD_SHA256_SIZE + RECORD_CRC_SIZE;
    uint8_t *buf = (uint8_t *)malloc(enc_cap);
    if (!buf) return IOTSPOOL_ENOMEM;

    iotspool_msg_id_t id = s->next_id++;
    uint32_t enc_len = record_encode_enq(buf, enc_cap, m, id, 0,
                                          s->cfg.enable_sha256);
    if (enc_len == 0) { free(buf); return IOTSPOOL_EINVAL; }

    iotspool_err_t err = s->store.append(s->store.ctx, buf, enc_len);
    free(buf);
    if (err != IOTSPOOL_OK) return err;
    s->store.sync(s->store.ctx);

    /* Add to RAM index */
    pending_entry_t *e = (pending_entry_t *)calloc(1, sizeof(*e));
    if (!e) return IOTSPOOL_ENOMEM;
    e->id          = id;
    e->payload_len = m->payload_len;
    e->qos         = m->qos;
    e->retain      = m->retain;

    e->topic = (char *)malloc(tl + 1);
    if (!e->topic) { free(e); return IOTSPOOL_ENOMEM; }
    memcpy(e->topic, m->topic, tl);
    e->topic[tl] = '\0';

    if (m->payload_len > 0) {
        e->payload = (uint8_t *)malloc(m->payload_len);
        if (!e->payload) { free(e->topic); free(e); return IOTSPOOL_ENOMEM; }
        memcpy(e->payload, m->payload, m->payload_len);
    }

    if (s->tail) s->tail->next = e;
    else         s->head = e;
    s->tail = e;
    s->pending_count++;
    s->stats.enqueued_total++;

    if (out_id) *out_id = id;
    return IOTSPOOL_OK;
}

/* ── Peek ────────────────────────────────────────────────────────────────── */
iotspool_err_t iotspool_peek_ready(iotspool_t *s, uint32_t now_ms,
                                    iotspool_msg_t *out,
                                    iotspool_msg_id_t *out_id)
{
    if (!s || !out || !out_id) return IOTSPOOL_EINVAL;
    if (s->state != STATE_READY) return IOTSPOOL_ESTATE;
    if (!s->head) return IOTSPOOL_ENOTFOUND;
    if (!backoff_is_ready(&s->backoff, now_ms)) return IOTSPOOL_ENOTFOUND;

    pending_entry_t *e = s->head;
    out->topic       = e->topic;
    out->payload     = e->payload;
    out->payload_len = e->payload_len;
    out->qos         = e->qos;
    out->retain      = e->retain;
    *out_id          = e->id;
    return IOTSPOOL_OK;
}

/* ── ACK ─────────────────────────────────────────────────────────────────── */
iotspool_err_t iotspool_ack(iotspool_t *s, iotspool_msg_id_t id) {
    if (!s) return IOTSPOOL_EINVAL;
    if (s->state != STATE_READY) return IOTSPOOL_ESTATE;

    /* Write ACK record to store */
    uint8_t ack_buf[RECORD_ACK_SIZE];
    uint32_t len = record_encode_ack(ack_buf, sizeof(ack_buf), id);
    if (len == 0) return IOTSPOOL_EINVAL;
    iotspool_err_t err = s->store.append(s->store.ctx, ack_buf, len);
    if (err != IOTSPOOL_OK) return err;
    s->store.sync(s->store.ctx);

    /* Remove from RAM index */
    if (s->head && s->head->id == id) {
        pending_entry_t *e = remove_head(s);
        free_entry(e);
        s->stats.acked_total++;
        backoff_reset(&s->backoff);
        return IOTSPOOL_OK;
    }
    for (pending_entry_t *prev = s->head; prev && prev->next; prev = prev->next) {
        if (prev->next->id == id) {
            pending_entry_t *e = prev->next;
            prev->next = e->next;
            if (s->tail == e) s->tail = prev;
            s->pending_count--;
            free_entry(e);
            s->stats.acked_total++;
            backoff_reset(&s->backoff);
            return IOTSPOOL_OK;
        }
    }
    return IOTSPOOL_ENOTFOUND;
}

/* ── Publish fail ────────────────────────────────────────────────────────── */
void iotspool_on_publish_fail(iotspool_t *s, uint32_t now_ms) {
    if (!s) return;
    backoff_on_fail(&s->backoff, now_ms);
}

/* ── Stats ───────────────────────────────────────────────────────────────── */
void iotspool_stats(const iotspool_t *s, iotspool_stats_t *stats) {
    if (!s || !stats) return;
    *stats = s->stats;
    stats->pending_count = s->pending_count;
    stats->store_bytes   = s->store.size_bytes(s->store.ctx);
}
