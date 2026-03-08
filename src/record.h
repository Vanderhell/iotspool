/**
 * record.h – Binary log record format (internal).
 *
 * Wire layout (little-endian):
 *
 *  ENQ record:
 *  [0]     magic    u8  = 0xE7
 *  [1]     type     u8  = RECORD_ENQ (0x01)
 *  [2]     version  u8  = RECORD_VERSION (0x01)
 *  [3]     flags    u8  (bit0=sha256_present, bit1=retain, bit2-3=qos)
 *  [4..7]  msg_id   u32
 *  [8..11] timestamp_ms u32 (low 32 bits of enqueue time)
 *  [12..15] topic_len u32
 *  [16..19] payload_len u32
 *  [20 .. 20+topic_len-1]       topic bytes (no null terminator in file)
 *  [20+topic_len .. +payload_len-1] payload bytes
 *  [optional 32 bytes]          SHA-256 if flags bit0 set
 *  [last 4 bytes]               CRC32 of all preceding bytes
 *
 *  ACK record:
 *  [0]     magic    u8  = 0xE7
 *  [1]     type     u8  = RECORD_ACK (0x02)
 *  [2]     version  u8  = RECORD_VERSION
 *  [3]     reserved u8  = 0
 *  [4..7]  msg_id   u32
 *  [8..11] CRC32
 */

#pragma once
#include <stdint.h>
#include <stdbool.h>
#include "../include/iotspool.h"

#define RECORD_MAGIC    0xE7u
#define RECORD_VERSION  0x01u

#define RECORD_ENQ 0x01u
#define RECORD_ACK 0x02u

#define RECORD_FLAG_SHA256 (1u << 0)
#define RECORD_FLAG_RETAIN (1u << 1)
#define RECORD_FLAG_QOS1   (1u << 2)

/* Fixed header size before variable-length fields */
#define RECORD_ENQ_HDR_SIZE  20u
#define RECORD_ACK_SIZE      12u
#define RECORD_SHA256_SIZE   32u
#define RECORD_CRC_SIZE       4u

typedef struct {
    uint8_t  type;
    uint8_t  flags;
    uint32_t msg_id;
    uint32_t timestamp_ms;
    uint32_t topic_len;
    uint32_t payload_len;
    /* Pointers into caller-owned buffer after decode */
    const char    *topic;
    const uint8_t *payload;
    uint8_t  sha256[32]; /* valid only if RECORD_FLAG_SHA256 set */
} record_enq_t;

typedef struct {
    uint32_t msg_id;
} record_ack_t;

/**
 * Encode an ENQ record into buf (caller must provide enough space).
 * Returns total bytes written, or 0 on error.
 */
uint32_t record_encode_enq(uint8_t *buf, uint32_t buf_cap,
                            const iotspool_msg_t *m,
                            uint32_t msg_id, uint32_t timestamp_ms,
                            bool enable_sha256);

/**
 * Encode an ACK record. Returns bytes written (always RECORD_ACK_SIZE), 0 on err.
 */
uint32_t record_encode_ack(uint8_t *buf, uint32_t buf_cap, uint32_t msg_id);

/**
 * Decode one record starting at buf[0].
 * Returns total record size (so caller can advance), 0 on corrupt/incomplete.
 * type_out is set to RECORD_ENQ or RECORD_ACK.
 */
uint32_t record_decode(const uint8_t *buf, uint32_t available,
                       uint8_t *type_out,
                       record_enq_t *enq_out,   /* may be NULL */
                       record_ack_t *ack_out);  /* may be NULL */

/** Compute CRC32 (ISO 3309 / Ethernet polynomial). */
uint32_t crc32(const uint8_t *data, uint32_t len);
