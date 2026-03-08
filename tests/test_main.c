/* test_main.c – Minimal test runner (no external framework needed).
 * SPDX-License-Identifier: MIT */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "../include/iotspool.h"
#include "../src/record.h"
#include "../src/store_posix.h"
#include "../src/sha256.h"

/* ── Test helpers ────────────────────────────────────────────────────────── */
static int g_tests = 0, g_passed = 0, g_failed = 0;
#define TEST(name) static void test_##name(void)
#define RUN(name)  do { g_tests++; printf("  %-45s", #name); \
                        test_##name(); \
                        printf("PASS\n"); g_passed++; } while(0)
#define ASSERT(cond) do { if(!(cond)) { \
    printf("FAIL\n  ASSERT FAILED: %s  (%s:%d)\n", #cond, __FILE__, __LINE__); \
    g_failed++; return; } } while(0)
#define ASSERT_EQ(a,b) ASSERT((a)==(b))

/* ── SHA-256 known-answer tests (FIPS 180-4 / NIST CAVP) ────────────────── */
TEST(sha256_empty) {
    /* SHA-256("") = e3b0c442 98fc1c14 9afbf4c8 996fb924 27ae41e4 649b934c a495991b 7852b855 */
    uint8_t digest[32];
    sha256(NULL, 0, digest);
    const uint8_t expected[32] = {
        0xe3,0xb0,0xc4,0x42,0x98,0xfc,0x1c,0x14,
        0x9a,0xfb,0xf4,0xc8,0x99,0x6f,0xb9,0x24,
        0x27,0xae,0x41,0xe4,0x64,0x9b,0x93,0x4c,
        0xa4,0x95,0x99,0x1b,0x78,0x52,0xb8,0x55
    };
    ASSERT(memcmp(digest, expected, 32) == 0);
}

TEST(sha256_abc) {
    /* SHA-256("abc") NIST FIPS 180-4 */
    const uint8_t msg[] = {'a','b','c'};
    uint8_t digest[32];
    sha256(msg, 3, digest);
    const uint8_t expected[32] = {
        0xba,0x78,0x16,0xbf,0x8f,0x01,0xcf,0xea,
        0x41,0x41,0x40,0xde,0x5d,0xae,0x22,0x23,
        0xb0,0x03,0x61,0xa3,0x96,0x17,0x7a,0x9c,
        0xb4,0x10,0xff,0x61,0xf2,0x00,0x15,0xad
    };
    ASSERT(memcmp(digest, expected, 32) == 0);
}

/* ── CRC32 ───────────────────────────────────────────────────────────────── */
TEST(crc32_known) {
    /* CRC32("123456789") = 0xCBF43926 */
    const uint8_t data[] = "123456789";
    ASSERT_EQ(crc32(data, 9), 0xCBF43926u);
}

/* ── Record encode/decode round-trip ─────────────────────────────────────── */
TEST(record_enq_roundtrip) {
    uint8_t buf[512];
    iotspool_msg_t m = {
        .topic       = "sensors/temp",
        .payload     = (const uint8_t *)"23.5",
        .payload_len = 4,
        .qos         = 1,
        .retain      = false
    };
    uint32_t len = record_encode_enq(buf, sizeof(buf), &m, 42, 1000, false);
    ASSERT(len > 0);

    uint8_t type = 0;
    record_enq_t enq = {0};
    uint32_t consumed = record_decode(buf, len, &type, &enq, NULL);
    ASSERT_EQ(consumed, len);
    ASSERT_EQ(type, RECORD_ENQ);
    ASSERT_EQ(enq.msg_id, 42u);
    ASSERT_EQ(enq.payload_len, 4u);
    ASSERT(strncmp(enq.topic, "sensors/temp", 12) == 0);
    ASSERT(memcmp(enq.payload, "23.5", 4) == 0);
}

TEST(record_ack_roundtrip) {
    uint8_t buf[16];
    uint32_t len = record_encode_ack(buf, sizeof(buf), 99);
    ASSERT_EQ(len, (uint32_t)RECORD_ACK_SIZE);

    uint8_t type = 0;
    record_ack_t ack = {0};
    uint32_t consumed = record_decode(buf, len, &type, NULL, &ack);
    ASSERT_EQ(consumed, len);
    ASSERT_EQ(type, RECORD_ACK);
    ASSERT_EQ(ack.msg_id, 99u);
}

TEST(record_corrupt_crc) {
    uint8_t buf[512];
    iotspool_msg_t m = { .topic="t", .payload=NULL, .payload_len=0, .qos=0 };
    uint32_t len = record_encode_enq(buf, sizeof(buf), &m, 1, 0, false);
    ASSERT(len > 0);
    buf[len - 1] ^= 0xFF; /* corrupt CRC */
    uint8_t type = 0;
    uint32_t consumed = record_decode(buf, len, &type, NULL, NULL);
    ASSERT_EQ(consumed, 0u);
}

/* ── Full spool lifecycle (enqueue → recover → ack) ─────────────────────── */
#define TMP_STORE "/tmp/iotspool_test.bin"

static void cleanup_store(void) { remove(TMP_STORE); }

TEST(spool_enqueue_and_ack) {
    cleanup_store();
    iotspool_store_t store = {0};
    ASSERT_EQ(store_posix_open(TMP_STORE, &store), IOTSPOOL_OK);

    iotspool_cfg_t cfg = iotspool_cfg_default();
    iotspool_t *s = NULL;
    ASSERT_EQ(iotspool_init(&s, &cfg, &store), IOTSPOOL_OK);
    ASSERT_EQ(iotspool_recover(s), IOTSPOOL_OK);

    iotspool_msg_t m = {
        .topic = "home/sensor/1",
        .payload = (const uint8_t *)"{\"t\":22}",
        .payload_len = 8, .qos = 1
    };
    iotspool_msg_id_t id = IOTSPOOL_MSG_ID_INVALID;
    ASSERT_EQ(iotspool_enqueue(s, &m, &id), IOTSPOOL_OK);
    ASSERT(id != IOTSPOOL_MSG_ID_INVALID);

    iotspool_msg_t out = {0};
    iotspool_msg_id_t out_id = 0;
    ASSERT_EQ(iotspool_peek_ready(s, 9999999, &out, &out_id), IOTSPOOL_OK);
    ASSERT_EQ(out_id, id);
    ASSERT(strcmp(out.topic, "home/sensor/1") == 0);

    ASSERT_EQ(iotspool_ack(s, id), IOTSPOOL_OK);
    ASSERT_EQ(iotspool_peek_ready(s, 9999999, &out, &out_id), IOTSPOOL_ENOTFOUND);

    iotspool_deinit(s);
    store_posix_close(&store);
}

TEST(spool_recover_after_restart) {
    cleanup_store();
    iotspool_store_t store = {0};
    ASSERT_EQ(store_posix_open(TMP_STORE, &store), IOTSPOOL_OK);

    iotspool_cfg_t cfg = iotspool_cfg_default();
    iotspool_t *s = NULL;
    ASSERT_EQ(iotspool_init(&s, &cfg, &store), IOTSPOOL_OK);
    ASSERT_EQ(iotspool_recover(s), IOTSPOOL_OK);

    iotspool_msg_t m = { .topic="a/b", .payload=(const uint8_t*)"x",
                         .payload_len=1, .qos=0 };
    iotspool_msg_id_t id = 0;
    ASSERT_EQ(iotspool_enqueue(s, &m, &id), IOTSPOOL_OK);

    /* Simulate restart: deinit + close, then reopen */
    iotspool_deinit(s); s = NULL;
    store_posix_close(&store);

    ASSERT_EQ(store_posix_open(TMP_STORE, &store), IOTSPOOL_OK);
    ASSERT_EQ(iotspool_init(&s, &cfg, &store), IOTSPOOL_OK);
    ASSERT_EQ(iotspool_recover(s), IOTSPOOL_OK);

    iotspool_msg_t out = {0};
    iotspool_msg_id_t out_id = 0;
    ASSERT_EQ(iotspool_peek_ready(s, 9999999, &out, &out_id), IOTSPOOL_OK);
    ASSERT_EQ(out_id, id);

    iotspool_deinit(s);
    store_posix_close(&store);
}

TEST(recover_ignores_partial_enq) {
    cleanup_store();
    iotspool_store_t store = {0};
    ASSERT_EQ(store_posix_open(TMP_STORE, &store), IOTSPOOL_OK);

    iotspool_cfg_t cfg = iotspool_cfg_default();
    iotspool_t *s = NULL;
    ASSERT_EQ(iotspool_init(&s, &cfg, &store), IOTSPOOL_OK);
    ASSERT_EQ(iotspool_recover(s), IOTSPOOL_OK);

    iotspool_msg_t m = { .topic="x/y", .payload=(const uint8_t*)"hello",
                         .payload_len=5, .qos=1 };
    ASSERT_EQ(iotspool_enqueue(s, &m, NULL), IOTSPOOL_OK);

    uint32_t full_size = store.size_bytes(store.ctx);
    iotspool_deinit(s); s = NULL;
    store_posix_close(&store);

    /* Truncate to simulate power-loss mid-write */
    ASSERT_EQ(store_posix_open(TMP_STORE, &store), IOTSPOOL_OK);
    if (store.truncate_to)
        store.truncate_to(store.ctx, full_size / 2);
    store_posix_close(&store);

    /* Recover: incomplete record should be silently ignored */
    ASSERT_EQ(store_posix_open(TMP_STORE, &store), IOTSPOOL_OK);
    ASSERT_EQ(iotspool_init(&s, &cfg, &store), IOTSPOOL_OK);
    ASSERT_EQ(iotspool_recover(s), IOTSPOOL_OK);

    iotspool_stats_t st = {0};
    iotspool_stats(s, &st);
    ASSERT_EQ(st.pending_count, 0u); /* partial record must be gone */

    iotspool_deinit(s);
    store_posix_close(&store);
}

TEST(store_full_backpressure) {
    cleanup_store();
    iotspool_store_t store = {0};
    ASSERT_EQ(store_posix_open(TMP_STORE, &store), IOTSPOOL_OK);

    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 3;
    cfg.drop_oldest_on_full = false;
    iotspool_t *s = NULL;
    ASSERT_EQ(iotspool_init(&s, &cfg, &store), IOTSPOOL_OK);
    ASSERT_EQ(iotspool_recover(s), IOTSPOOL_OK);

    iotspool_msg_t m = { .topic="t", .payload=NULL, .payload_len=0, .qos=0 };
    ASSERT_EQ(iotspool_enqueue(s, &m, NULL), IOTSPOOL_OK);
    ASSERT_EQ(iotspool_enqueue(s, &m, NULL), IOTSPOOL_OK);
    ASSERT_EQ(iotspool_enqueue(s, &m, NULL), IOTSPOOL_OK);
    ASSERT_EQ(iotspool_enqueue(s, &m, NULL), IOTSPOOL_EFULL);

    iotspool_deinit(s);
    store_posix_close(&store);
}

TEST(ack_idempotent) {
    cleanup_store();
    iotspool_store_t store = {0};
    ASSERT_EQ(store_posix_open(TMP_STORE, &store), IOTSPOOL_OK);

    iotspool_cfg_t cfg = iotspool_cfg_default();
    iotspool_t *s = NULL;
    ASSERT_EQ(iotspool_init(&s, &cfg, &store), IOTSPOOL_OK);
    ASSERT_EQ(iotspool_recover(s), IOTSPOOL_OK);

    iotspool_msg_t m = { .topic="t", .payload=NULL, .payload_len=0, .qos=1 };
    iotspool_msg_id_t id = 0;
    ASSERT_EQ(iotspool_enqueue(s, &m, &id), IOTSPOOL_OK);
    ASSERT_EQ(iotspool_ack(s, id), IOTSPOOL_OK);
    /* Second ack: ENOTFOUND is acceptable (not a crash) */
    iotspool_err_t r2 = iotspool_ack(s, id);
    ASSERT(r2 == IOTSPOOL_OK || r2 == IOTSPOOL_ENOTFOUND);

    iotspool_deinit(s);
    store_posix_close(&store);
}

/* ── main ────────────────────────────────────────────────────────────────── */
int main(void) {
    printf("\niotspool test suite\n");
    printf("===================\n");

    RUN(sha256_empty);
    RUN(sha256_abc);
    RUN(crc32_known);
    RUN(record_enq_roundtrip);
    RUN(record_ack_roundtrip);
    RUN(record_corrupt_crc);
    RUN(spool_enqueue_and_ack);
    RUN(spool_recover_after_restart);
    RUN(recover_ignores_partial_enq);
    RUN(store_full_backpressure);
    RUN(ack_idempotent);

    printf("\n%d/%d passed", g_passed, g_tests);
    if (g_failed) printf(", %d FAILED", g_failed);
    printf("\n");
    cleanup_store();
    return g_failed ? 1 : 0;
}
