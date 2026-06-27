/*
 * record.h - Versioned on-disk format for iotspool.
 *
 * The log begins with a fixed superblock followed by records.
 * All multi-byte fields are little-endian.
 *
 * SPDX-License-Identifier: MIT
 */

#pragma once

#include <stdbool.h>
#include <stdint.h>
#include "../include/iotspool.h"

#define IOTSPOOL_STORE_MAGIC 0x50534F49u /* "IOSP" */
#define IOTSPOOL_STORE_VERSION 1u
#define IOTSPOOL_RECORD_VERSION 1u

typedef enum {
    IOTSPOOL_REC_TYPE_ENQUEUE = 1u,
    IOTSPOOL_REC_TYPE_ACK     = 2u,
    IOTSPOOL_REC_TYPE_DROP    = 3u
} iotspool_record_type_t;

typedef enum {
    IOTSPOOL_DEC_VALID = 0,
    IOTSPOOL_DEC_CLEAN_END,
    IOTSPOOL_DEC_INCOMPLETE_TAIL,
    IOTSPOOL_DEC_CORRUPT,
    IOTSPOOL_DEC_UNSUPPORTED_VERSION,
    IOTSPOOL_DEC_UNKNOWN_TYPE,
    IOTSPOOL_DEC_INVALID_LENGTH,
    IOTSPOOL_DEC_IO_ERROR,
    IOTSPOOL_DEC_GENERATION_MISMATCH
} iotspool_decode_result_t;

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t record_version;
    uint32_t generation;
    uint32_t active_identity;
    uint32_t configured_size;
    uint32_t committed_pos;
    uint32_t crc32;
    uint8_t  reserved[4];
} iotspool_superblock_t;

typedef struct {
    uint64_t msg_id;
    uint32_t generation;
    uint32_t topic_len;
    uint32_t payload_len;
    uint32_t timestamp_ms;
    uint8_t  qos;
    bool     retain;
    const char    *topic;
    const uint8_t *payload;
} record_enqueue_t;

typedef struct {
    uint64_t msg_id;
} record_ack_t;

typedef struct {
    uint64_t msg_id;
} record_drop_t;

size_t record_superblock_size(void);
size_t record_header_size(void);
size_t record_min_record_size(iotspool_record_type_t type);

iotspool_decode_result_t record_decode(const uint8_t *buf, uint32_t avail,
                                       uint8_t *type_out,
                                       record_enqueue_t *enq_out,
                                       record_ack_t *ack_out,
                                       record_drop_t *drop_out,
                                       uint32_t *consumed_out);

uint32_t record_encode_enqueue(uint8_t *buf, uint32_t cap,
                               const iotspool_msg_t *m,
                               uint64_t msg_id,
                               uint32_t generation,
                               uint32_t timestamp_ms,
                               bool enable_sha256);
uint32_t record_encode_ack(uint8_t *buf, uint32_t cap,
                           uint64_t msg_id, uint32_t generation);
uint32_t record_encode_drop(uint8_t *buf, uint32_t cap,
                            uint64_t msg_id, uint32_t generation);

iotspool_err_t record_encode_superblock(uint8_t *buf, uint32_t cap,
                                        const iotspool_superblock_t *sb);
iotspool_decode_result_t record_decode_superblock(const uint8_t *buf,
                                                  uint32_t avail,
                                                  iotspool_superblock_t *sb);

bool record_checked_add_u32(uint32_t a, uint32_t b, uint32_t *out);
bool record_checked_add_u64(uint64_t a, uint64_t b, uint64_t *out);
bool record_checked_mul_u32(uint32_t a, uint32_t b, uint32_t *out);

uint32_t crc32(const uint8_t *data, uint32_t len);
