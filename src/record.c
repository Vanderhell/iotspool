/* record.c – Binary log record encode/decode.
 * SPDX-License-Identifier: MIT */

#include "record.h"
#include "sha256.h"
#include <string.h>

/* ── Little-endian helpers ─────────────────────────────────────────────── */
static void put_u8 (uint8_t *b, uint8_t  v)               { b[0]=v; }
static void put_u32(uint8_t *b, uint32_t v) {
    b[0]=(uint8_t)v; b[1]=(uint8_t)(v>>8);
    b[2]=(uint8_t)(v>>16); b[3]=(uint8_t)(v>>24);
}
static uint32_t get_u32(const uint8_t *b) {
    return (uint32_t)b[0]|((uint32_t)b[1]<<8)|
           ((uint32_t)b[2]<<16)|((uint32_t)b[3]<<24);
}

/* ── ENQ encode ────────────────────────────────────────────────────────── */
uint32_t record_encode_enq(uint8_t *buf, uint32_t cap,
                            const iotspool_msg_t *m,
                            uint32_t msg_id, uint32_t ts_ms,
                            bool sha256_en)
{
    if (!buf || !m || !m->topic) return 0;
    uint32_t tl = (uint32_t)strlen(m->topic);
    uint32_t pl = m->payload_len;
    uint32_t total = RECORD_ENQ_HDR_SIZE + tl + pl +
                     (sha256_en ? RECORD_SHA256_SIZE : 0) + RECORD_CRC_SIZE;
    if (total > cap) return 0;

    uint8_t flags = 0;
    if (sha256_en)   flags |= RECORD_FLAG_SHA256;
    if (m->retain)   flags |= RECORD_FLAG_RETAIN;
    if (m->qos == 1) flags |= RECORD_FLAG_QOS1;

    uint8_t *p = buf;
    put_u8(p, RECORD_MAGIC);    p++;
    put_u8(p, RECORD_ENQ);      p++;
    put_u8(p, RECORD_VERSION);  p++;
    put_u8(p, flags);           p++;
    put_u32(p, msg_id);         p+=4;
    put_u32(p, ts_ms);          p+=4;
    put_u32(p, tl);             p+=4;
    put_u32(p, pl);             p+=4;
    memcpy(p, m->topic, tl);    p+=tl;
    if (pl && m->payload) { memcpy(p, m->payload, pl); }
    p += pl;

    if (sha256_en) {
        /* hash = SHA-256(header[0..HDR-1] || topic || payload) */
        sha256_ctx_t ctx;
        sha256_init(&ctx);
        sha256_update(&ctx, buf, (size_t)(p - buf));
        sha256_final(&ctx, p);
        p += RECORD_SHA256_SIZE;
    }

    uint32_t crc = crc32(buf, (uint32_t)(p - buf));
    put_u32(p, crc);

    return total;
}

/* ── ACK encode ────────────────────────────────────────────────────────── */
uint32_t record_encode_ack(uint8_t *buf, uint32_t cap, uint32_t msg_id)
{
    if (cap < RECORD_ACK_SIZE) return 0;
    uint8_t *p = buf;
    put_u8(p, RECORD_MAGIC);   p++;
    put_u8(p, RECORD_ACK);     p++;
    put_u8(p, RECORD_VERSION); p++;
    put_u8(p, 0);              p++;
    put_u32(p, msg_id);        p+=4;
    uint32_t crc = crc32(buf, 8);
    put_u32(p, crc);
    return RECORD_ACK_SIZE;
}

/* ── Decode ────────────────────────────────────────────────────────────── */
uint32_t record_decode(const uint8_t *buf, uint32_t avail,
                       uint8_t *type_out,
                       record_enq_t *enq_out,
                       record_ack_t *ack_out)
{
    if (!buf || avail < 4) return 0;
    if (buf[0] != RECORD_MAGIC) return 0;
    uint8_t type    = buf[1];
    uint8_t version = buf[2];
    (void)version; /* reserved for future compat check */

    if (type_out) *type_out = type;

    if (type == RECORD_ACK) {
        if (avail < RECORD_ACK_SIZE) return 0;
        uint32_t stored_crc = get_u32(buf + 8);
        if (crc32(buf, 8) != stored_crc) return 0;
        if (ack_out) ack_out->msg_id = get_u32(buf + 4);
        return RECORD_ACK_SIZE;
    }

    if (type == RECORD_ENQ) {
        if (avail < RECORD_ENQ_HDR_SIZE) return 0;
        uint8_t  flags  = buf[3];
        uint32_t msg_id = get_u32(buf + 4);
        uint32_t ts_ms  = get_u32(buf + 8);
        uint32_t tl     = get_u32(buf + 12);
        uint32_t pl     = get_u32(buf + 16);
        bool     has_sha = (flags & RECORD_FLAG_SHA256) != 0;

        uint32_t total = RECORD_ENQ_HDR_SIZE + tl + pl +
                         (has_sha ? RECORD_SHA256_SIZE : 0) + RECORD_CRC_SIZE;
        if (avail < total) return 0;

        /* Check CRC */
        uint32_t body_len = total - RECORD_CRC_SIZE;
        uint32_t stored_crc = get_u32(buf + body_len);
        if (crc32(buf, body_len) != stored_crc) return 0;

        /* Optionally verify SHA-256 */
        if (has_sha) {
            const uint8_t *sha_field = buf + RECORD_ENQ_HDR_SIZE + tl + pl;
            uint8_t computed[32];
            sha256(buf, RECORD_ENQ_HDR_SIZE + tl + pl, computed);
            if (memcmp(computed, sha_field, 32) != 0) return 0;
        }

        if (enq_out) {
            enq_out->type        = type;
            enq_out->flags       = flags;
            enq_out->msg_id      = msg_id;
            enq_out->timestamp_ms= ts_ms;
            enq_out->topic_len   = tl;
            enq_out->payload_len = pl;
            enq_out->topic       = (const char *)(buf + RECORD_ENQ_HDR_SIZE);
            enq_out->payload     = buf + RECORD_ENQ_HDR_SIZE + tl;
            if (has_sha)
                memcpy(enq_out->sha256,
                       buf + RECORD_ENQ_HDR_SIZE + tl + pl, 32);
        }
        return total;
    }

    return 0; /* unknown type */
}
