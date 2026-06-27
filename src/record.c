/*
 * record.c - Binary log encode/decode for iotspool.
 *
 * Wire format is explicit little-endian and versioned.
 *
 * SPDX-License-Identifier: MIT
 */

#include "record.h"

#include <string.h>

#ifdef IOTSPOOL_ENABLE_SHA256
#include "sha256.h"
#endif

enum {
    RECORD_FLAG_SHA256 = 1u << 0,
    RECORD_FLAG_RETAIN  = 1u << 1,
    RECORD_FLAG_QOS1    = 1u << 2
};

enum {
    RECORD_HEADER_SIZE = 32u,
    RECORD_CRC_SIZE = 4u,
    SUPERBLOCK_SIZE = 32u
};

static void zero_bytes(void *dst, size_t len) {
    uint8_t *p = (uint8_t *)dst;
    if (!p) return;
    for (size_t i = 0; i < len; ++i) {
        p[i] = 0;
    }
}

static void copy_bytes(void *dst, const void *src, size_t len) {
    uint8_t *d = (uint8_t *)dst;
    const uint8_t *s = (const uint8_t *)src;
    if (!d || !s) return;
    for (size_t i = 0; i < len; ++i) {
        d[i] = s[i];
    }
}

static void put_u16(uint8_t *p, uint16_t v) {
    p[0] = (uint8_t)(v & 0xffu);
    p[1] = (uint8_t)((v >> 8) & 0xffu);
}

static void put_u32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v & 0xffu);
    p[1] = (uint8_t)((v >> 8) & 0xffu);
    p[2] = (uint8_t)((v >> 16) & 0xffu);
    p[3] = (uint8_t)((v >> 24) & 0xffu);
}

static void put_u64(uint8_t *p, uint64_t v) {
    for (unsigned i = 0; i < 8; ++i) {
        p[i] = (uint8_t)((v >> (8u * i)) & 0xffu);
    }
}

static uint16_t get_u16(const uint8_t *p) {
    return (uint16_t)((uint16_t)p[0] | ((uint16_t)p[1] << 8));
}

static uint32_t get_u32(const uint8_t *p) {
    return (uint32_t)p[0] |
           ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) |
           ((uint32_t)p[3] << 24);
}

static uint64_t get_u64(const uint8_t *p) {
    uint64_t v = 0;
    for (unsigned i = 0; i < 8; ++i) {
        v |= ((uint64_t)p[i]) << (8u * i);
    }
    return v;
}

bool record_checked_add_u32(uint32_t a, uint32_t b, uint32_t *out) {
    uint64_t v = (uint64_t)a + (uint64_t)b;
    if (v > UINT32_MAX) return false;
    if (out) *out = (uint32_t)v;
    return true;
}

bool record_checked_add_u64(uint64_t a, uint64_t b, uint64_t *out) {
    if (UINT64_MAX - a < b) return false;
    if (out) *out = a + b;
    return true;
}

bool record_checked_mul_u32(uint32_t a, uint32_t b, uint32_t *out) {
    uint64_t v = (uint64_t)a * (uint64_t)b;
    if (v > UINT32_MAX) return false;
    if (out) *out = (uint32_t)v;
    return true;
}

size_t record_superblock_size(void) {
    return SUPERBLOCK_SIZE;
}

size_t record_header_size(void) {
    return RECORD_HEADER_SIZE;
}

size_t record_min_record_size(iotspool_record_type_t type) {
    switch (type) {
        case IOTSPOOL_REC_TYPE_ENQUEUE:
            return RECORD_HEADER_SIZE + RECORD_CRC_SIZE;
        case IOTSPOOL_REC_TYPE_ACK:
        case IOTSPOOL_REC_TYPE_DROP:
            return RECORD_HEADER_SIZE + RECORD_CRC_SIZE;
        default:
            return 0;
    }
}

static uint32_t record_encode_common(uint8_t *buf, uint32_t cap,
                                     iotspool_record_type_t type,
                                     uint64_t msg_id,
                                     uint32_t generation,
                                     uint8_t flags,
                                     uint32_t topic_len,
                                     uint32_t payload_len,
                                     uint32_t timestamp_ms,
                                     const uint8_t *topic,
                                     const uint8_t *payload,
                                     bool enable_sha256)
{
    uint32_t total = 0;
    uint32_t var_len = 0;
    uint32_t sha_len = 0;

    if (!buf) return 0;
    if (enable_sha256) {
#ifdef IOTSPOOL_ENABLE_SHA256
        flags = (uint8_t)(flags | RECORD_FLAG_SHA256);
        sha_len = 32u;
#else
        return 0;
#endif
    }

    if (!record_checked_add_u32(topic_len, payload_len, &var_len)) return 0;
    if (!record_checked_add_u32(RECORD_HEADER_SIZE, var_len, &total)) return 0;
    if (!record_checked_add_u32(total, sha_len, &total)) return 0;
    if (!record_checked_add_u32(total, RECORD_CRC_SIZE, &total)) return 0;
    if (total > cap) return 0;

    uint8_t *p = buf;
    p[0] = IOTSPOOL_STORE_MAGIC & 0xffu;
    p[1] = (uint8_t)((IOTSPOOL_STORE_MAGIC >> 8) & 0xffu);
    p[2] = (uint8_t)((IOTSPOOL_STORE_MAGIC >> 16) & 0xffu);
    p[3] = (uint8_t)((IOTSPOOL_STORE_MAGIC >> 24) & 0xffu);
    p[4] = (uint8_t)type;
    p[5] = (uint8_t)IOTSPOOL_RECORD_VERSION;
    p[6] = flags;
    p[7] = 0;
    put_u64(p + 8, msg_id);
    put_u32(p + 16, generation);
    put_u32(p + 20, topic_len);
    put_u32(p + 24, payload_len);
    put_u32(p + 28, timestamp_ms);

    p += RECORD_HEADER_SIZE;
    if (topic_len && topic) {
        copy_bytes(p, topic, topic_len);
    }
    p += topic_len;
    if (payload_len && payload) {
        copy_bytes(p, payload, payload_len);
    }
    p += payload_len;

#ifdef IOTSPOOL_ENABLE_SHA256
    if (sha_len) {
        uint8_t digest[32];
        sha256(buf, (size_t)(p - buf), digest);
        copy_bytes(p, digest, sizeof(digest));
        p += sizeof(digest);
    }
#endif

    uint32_t crc = crc32(buf, (uint32_t)(p - buf));
    put_u32(p, crc);
    return total;
}

uint32_t record_encode_enqueue(uint8_t *buf, uint32_t cap,
                               const iotspool_msg_t *m,
                               uint64_t msg_id,
                               uint32_t generation,
                               uint32_t timestamp_ms,
                               bool enable_sha256)
{
    if (!buf || !m) return 0;
    if (m->qos > 1u) return 0;
    if (!m->topic) return 0;

    uint32_t topic_len = 0;
    while (topic_len <= UINT32_MAX) {
        if (m->topic[topic_len] == '\0') break;
        ++topic_len;
    }
    if (topic_len == 0) return 0;
    if (m->payload_len > 0 && !m->payload) return 0;

    uint8_t flags = 0;
    if (m->retain) flags |= RECORD_FLAG_RETAIN;
    if (m->qos == 1u) flags |= RECORD_FLAG_QOS1;

    return record_encode_common(buf, cap, IOTSPOOL_REC_TYPE_ENQUEUE,
                                msg_id, generation, flags,
                                topic_len, m->payload_len, timestamp_ms,
                                (const uint8_t *)m->topic, m->payload,
                                enable_sha256);
}

uint32_t record_encode_ack(uint8_t *buf, uint32_t cap,
                           uint64_t msg_id, uint32_t generation)
{
    return record_encode_common(buf, cap, IOTSPOOL_REC_TYPE_ACK,
                                msg_id, generation, 0,
                                0, 0, 0, NULL, NULL, false);
}

uint32_t record_encode_drop(uint8_t *buf, uint32_t cap,
                            uint64_t msg_id, uint32_t generation)
{
    return record_encode_common(buf, cap, IOTSPOOL_REC_TYPE_DROP,
                                msg_id, generation, 0,
                                0, 0, 0, NULL, NULL, false);
}

iotspool_err_t record_encode_superblock(uint8_t *buf, uint32_t cap,
                                        const iotspool_superblock_t *sb)
{
    if (!buf || !sb || cap < SUPERBLOCK_SIZE) return IOTSPOOL_EINVAL;

    zero_bytes(buf, SUPERBLOCK_SIZE);
    put_u32(buf + 0, sb->magic);
    put_u16(buf + 4, sb->version);
    put_u16(buf + 6, sb->record_version);
    put_u32(buf + 8, sb->generation);
    put_u32(buf + 12, sb->active_identity);
    put_u32(buf + 16, sb->configured_size);
    put_u32(buf + 20, sb->committed_pos);
    uint32_t crc = crc32(buf, 24u);
    put_u32(buf + 24, crc);
    copy_bytes(buf + 28, sb->reserved, sizeof(sb->reserved));
    return IOTSPOOL_OK;
}

iotspool_decode_result_t record_decode_superblock(const uint8_t *buf,
                                                  uint32_t avail,
                                                  iotspool_superblock_t *sb)
{
    if (!buf || !sb) return IOTSPOOL_DEC_IO_ERROR;
    if (avail < SUPERBLOCK_SIZE) return IOTSPOOL_DEC_INCOMPLETE_TAIL;

    zero_bytes(sb, sizeof(*sb));
    sb->magic = get_u32(buf + 0);
    sb->version = get_u16(buf + 4);
    sb->record_version = get_u16(buf + 6);
    sb->generation = get_u32(buf + 8);
    sb->active_identity = get_u32(buf + 12);
    sb->configured_size = get_u32(buf + 16);
    sb->committed_pos = get_u32(buf + 20);
    sb->crc32 = get_u32(buf + 24);
    copy_bytes(sb->reserved, buf + 28, sizeof(sb->reserved));

    if (sb->magic != IOTSPOOL_STORE_MAGIC) return IOTSPOOL_DEC_CORRUPT;
    if (sb->version != IOTSPOOL_STORE_VERSION) return IOTSPOOL_DEC_UNSUPPORTED_VERSION;
    if (sb->record_version != IOTSPOOL_RECORD_VERSION) return IOTSPOOL_DEC_UNSUPPORTED_VERSION;
    if (sb->crc32 != crc32(buf, 24u)) return IOTSPOOL_DEC_CORRUPT;
    for (size_t i = 0; i < sizeof(sb->reserved); ++i) {
        if (sb->reserved[i] != 0) return IOTSPOOL_DEC_CORRUPT;
    }
    return IOTSPOOL_DEC_VALID;
}

iotspool_decode_result_t record_decode(const uint8_t *buf, uint32_t avail,
                                       uint8_t *type_out,
                                       record_enqueue_t *enq_out,
                                       record_ack_t *ack_out,
                                       record_drop_t *drop_out,
                                       uint32_t *consumed_out)
{
    if (!buf) return IOTSPOOL_DEC_IO_ERROR;
    if (avail == 0) return IOTSPOOL_DEC_CLEAN_END;
    if (avail < RECORD_HEADER_SIZE) return IOTSPOOL_DEC_INCOMPLETE_TAIL;

    uint32_t magic = get_u32(buf + 0);
    if (magic != IOTSPOOL_STORE_MAGIC) return IOTSPOOL_DEC_CORRUPT;

    uint8_t type = buf[4];
    uint8_t version = buf[5];
    uint8_t flags = buf[6];

    if (type_out) *type_out = type;
    if (version != IOTSPOOL_RECORD_VERSION) return IOTSPOOL_DEC_UNSUPPORTED_VERSION;
    if ((flags & ~(RECORD_FLAG_SHA256 | RECORD_FLAG_RETAIN | RECORD_FLAG_QOS1)) != 0) {
        return IOTSPOOL_DEC_CORRUPT;
    }
    if (buf[7] != 0) return IOTSPOOL_DEC_CORRUPT;

    uint64_t msg_id = get_u64(buf + 8);
    uint32_t generation = get_u32(buf + 16);
    uint32_t topic_len = get_u32(buf + 20);
    uint32_t payload_len = get_u32(buf + 24);
    uint32_t timestamp_ms = get_u32(buf + 28);
    bool has_sha = (flags & RECORD_FLAG_SHA256) != 0;

    if (topic_len > UINT32_MAX - payload_len) return IOTSPOOL_DEC_INVALID_LENGTH;
    uint32_t body_len = 0;
    if (!record_checked_add_u32(RECORD_HEADER_SIZE, topic_len, &body_len)) {
        return IOTSPOOL_DEC_INVALID_LENGTH;
    }
    if (!record_checked_add_u32(body_len, payload_len, &body_len)) {
        return IOTSPOOL_DEC_INVALID_LENGTH;
    }
#ifdef IOTSPOOL_ENABLE_SHA256
    if (has_sha) {
        if (!record_checked_add_u32(body_len, 32u, &body_len)) {
            return IOTSPOOL_DEC_INVALID_LENGTH;
        }
    }
#else
    if (has_sha) return IOTSPOOL_DEC_UNSUPPORTED_VERSION;
#endif
    if (!record_checked_add_u32(body_len, RECORD_CRC_SIZE, &body_len)) {
        return IOTSPOOL_DEC_INVALID_LENGTH;
    }
    if (avail < body_len) return IOTSPOOL_DEC_INCOMPLETE_TAIL;

    uint32_t stored_crc = get_u32(buf + body_len - RECORD_CRC_SIZE);
    if (stored_crc != crc32(buf, body_len - RECORD_CRC_SIZE)) {
        return IOTSPOOL_DEC_CORRUPT;
    }

#ifdef IOTSPOOL_ENABLE_SHA256
    if (has_sha) {
        const uint8_t *sha_field = buf + RECORD_HEADER_SIZE + topic_len + payload_len;
        uint8_t digest[32];
        sha256(buf, RECORD_HEADER_SIZE + topic_len + payload_len, digest);
        if (memcmp(digest, sha_field, sizeof(digest)) != 0) {
            return IOTSPOOL_DEC_CORRUPT;
        }
    }
#endif

    if (type == IOTSPOOL_REC_TYPE_ENQUEUE) {
        if (enq_out) {
            enq_out->msg_id = msg_id;
            enq_out->generation = generation;
            enq_out->topic_len = topic_len;
            enq_out->payload_len = payload_len;
            enq_out->timestamp_ms = timestamp_ms;
            enq_out->qos = (flags & RECORD_FLAG_QOS1) ? 1u : 0u;
            enq_out->retain = (flags & RECORD_FLAG_RETAIN) != 0;
            enq_out->topic = (const char *)(buf + RECORD_HEADER_SIZE);
            enq_out->payload = buf + RECORD_HEADER_SIZE + topic_len;
        }
        if (consumed_out) *consumed_out = body_len;
        return IOTSPOOL_DEC_VALID;
    }
    if (type == IOTSPOOL_REC_TYPE_ACK) {
        if (topic_len != 0 || payload_len != 0) return IOTSPOOL_DEC_INVALID_LENGTH;
        if (ack_out) ack_out->msg_id = msg_id;
        if (consumed_out) *consumed_out = body_len;
        return IOTSPOOL_DEC_VALID;
    }
    if (type == IOTSPOOL_REC_TYPE_DROP) {
        if (topic_len != 0 || payload_len != 0) return IOTSPOOL_DEC_INVALID_LENGTH;
        if (drop_out) drop_out->msg_id = msg_id;
        if (consumed_out) *consumed_out = body_len;
        return IOTSPOOL_DEC_VALID;
    }

    return IOTSPOOL_DEC_UNKNOWN_TYPE;
}
