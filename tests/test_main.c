/*
 * test_main.c - Self-contained verification for iotspool.
 *
 * SPDX-License-Identifier: MIT
 */

#include "../include/iotspool.h"
#include "../src/record.h"
#include "../src/store_posix.h"
#ifdef IOTSPOOL_ENABLE_SHA256
#include "../src/sha256.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_tests = 0;
static int g_failed = 0;

#define TEST_ASSERT(expr) do { \
    if (!(expr)) { \
        printf("FAIL: %s (%s:%d)\n", #expr, __FILE__, __LINE__); \
        g_failed++; \
        return; \
    } \
} while (0)

#define TEST_ASSERT_EQ(a, b) TEST_ASSERT((a) == (b))

#define RUN_TEST(fn) do { \
    printf("%s\n", #fn); \
    g_tests++; \
    fn(); \
    if (!g_failed) printf("PASS\n"); \
} while (0)

typedef struct {
    uint8_t *buf;
    uint32_t cap;
    uint32_t size;
    uint32_t append_calls;
    uint32_t read_calls;
    uint32_t sync_calls;
    uint32_t truncate_calls;
    uint32_t replace_calls;
    int fail_append_after;
    int fail_read_after;
    int fail_sync_after;
    int fail_truncate_after;
    int fail_replace_after;
    bool partial_append;
    bool fail_size;
} fi_store_t;

typedef struct {
    bool held;
    uint32_t lock_calls;
    uint32_t unlock_calls;
} lock_ctx_t;

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

static void move_u64_left(uint64_t *values, uint32_t count) {
    if (!values || count == 0) return;
    for (uint32_t i = 0; i + 1u < count; ++i) {
        values[i] = values[i + 1u];
    }
}

static iotspool_err_t fi_append(void *ctx, const uint8_t *data, uint32_t len) {
    fi_store_t *s = (fi_store_t *)ctx;
    ++s->append_calls;
    if (s->fail_append_after >= 0 && (int)s->append_calls > s->fail_append_after) {
        if (s->partial_append && len > 0u && s->size < s->cap) {
            uint32_t copy = len / 2u;
            if (copy > s->cap - s->size) copy = s->cap - s->size;
            copy_bytes(s->buf + s->size, data, copy);
            s->size += copy;
        }
        return IOTSPOOL_EIO;
    }
    if (len > s->cap - s->size) return IOTSPOOL_EFULL;
    copy_bytes(s->buf + s->size, data, len);
    s->size += len;
    return IOTSPOOL_OK;
}

static iotspool_err_t fi_read_at(void *ctx, uint32_t off, uint8_t *out,
                                 uint32_t cap, uint32_t *out_len) {
    fi_store_t *s = (fi_store_t *)ctx;
    ++s->read_calls;
    if (s->fail_read_after >= 0 && (int)s->read_calls > s->fail_read_after) {
        return IOTSPOOL_EIO;
    }
    if (off >= s->size) {
        if (out_len) *out_len = 0;
        return IOTSPOOL_OK;
    }
    uint32_t n = s->size - off;
    if (n > cap) n = cap;
    copy_bytes(out, s->buf + off, n);
    if (out_len) *out_len = n;
    return IOTSPOOL_OK;
}

static iotspool_err_t fi_sync(void *ctx) {
    fi_store_t *s = (fi_store_t *)ctx;
    ++s->sync_calls;
    if (s->fail_sync_after >= 0 && (int)s->sync_calls > s->fail_sync_after) {
        return IOTSPOOL_EIO;
    }
    return IOTSPOOL_OK;
}

static uint32_t fi_size(void *ctx) {
    fi_store_t *s = (fi_store_t *)ctx;
    return s->fail_size ? 0u : s->size;
}

static iotspool_err_t fi_truncate(void *ctx, uint32_t new_size) {
    fi_store_t *s = (fi_store_t *)ctx;
    ++s->truncate_calls;
    if (s->fail_truncate_after >= 0 && (int)s->truncate_calls > s->fail_truncate_after) {
        return IOTSPOOL_EIO;
    }
    if (new_size > s->size) return IOTSPOOL_EINVAL;
    s->size = new_size;
    return IOTSPOOL_OK;
}

static iotspool_err_t fi_replace(void *ctx, const uint8_t *data, uint32_t len) {
    fi_store_t *s = (fi_store_t *)ctx;
    ++s->replace_calls;
    if (s->fail_replace_after >= 0 && (int)s->replace_calls > s->fail_replace_after) {
        return IOTSPOOL_EIO;
    }
    if (len > s->cap) return IOTSPOOL_EFULL;
    copy_bytes(s->buf, data, len);
    s->size = len;
    return IOTSPOOL_OK;
}

static void make_fi_store(fi_store_t *s, uint8_t *buf, uint32_t cap) {
    zero_bytes(s, sizeof(*s));
    s->buf = buf;
    s->cap = cap;
    s->fail_append_after = -1;
    s->fail_read_after = -1;
    s->fail_sync_after = -1;
    s->fail_truncate_after = -1;
    s->fail_replace_after = -1;
}

static void bind_fi_store(iotspool_store_t *store, fi_store_t *fi) {
    store->ctx = fi;
    store->append = fi_append;
    store->read_at = fi_read_at;
    store->sync = fi_sync;
    store->size_bytes = fi_size;
    store->truncate_to = fi_truncate;
    store->replace = fi_replace;
}

static iotspool_err_t lock_cb(void *ctx) {
    lock_ctx_t *lock = (lock_ctx_t *)ctx;
    ++lock->lock_calls;
    if (lock->held) return IOTSPOOL_EBUSY;
    lock->held = true;
    return IOTSPOOL_OK;
}

static iotspool_err_t unlock_cb(void *ctx) {
    lock_ctx_t *lock = (lock_ctx_t *)ctx;
    ++lock->unlock_calls;
    lock->held = false;
    return IOTSPOOL_OK;
}

static iotspool_err_t make_spool(iotspool_t *spool, iotspool_entry_t *entries,
                                 uint8_t *scratch, size_t scratch_cap,
                                 fi_store_t *fi, iotspool_store_t *store,
                                 const iotspool_cfg_t *cfg) {
    bind_fi_store(store, fi);
    iotspool_err_t err = iotspool_init_inplace(spool, entries, cfg->max_pending_msgs,
                                                scratch, scratch_cap, cfg, store);
    if (err != IOTSPOOL_OK) return err;
    return iotspool_recover(spool);
}

static void test_config_validation(void) {
    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 0;
    uint8_t raw[4096];
    fi_store_t fi;
    make_fi_store(&fi, raw, sizeof(raw));
    iotspool_store_t store;
    bind_fi_store(&store, &fi);
    iotspool_t spool;
    iotspool_entry_t entries[4];
    uint8_t scratch[512];
    TEST_ASSERT_EQ(iotspool_init_inplace(&spool, entries, 4, scratch, sizeof(scratch), &cfg, &store),
                   IOTSPOOL_EINVAL);

#ifndef IOTSPOOL_ENABLE_SHA256
    cfg = iotspool_cfg_default();
    cfg.enable_sha256 = true;
    TEST_ASSERT_EQ(iotspool_init_inplace(&spool, entries, 4, scratch, sizeof(scratch), &cfg, &store),
                   IOTSPOOL_EINVAL);
#endif

    cfg = iotspool_cfg_default();
    cfg.max_topic_bytes = 0;
    TEST_ASSERT_EQ(iotspool_init_inplace(&spool, entries, 4, scratch, sizeof(scratch), &cfg, &store),
                   IOTSPOOL_EINVAL);
}

static void test_null_inputs(void) {
    iotspool_cfg_t cfg = iotspool_cfg_default();
    uint8_t raw[4096];
    fi_store_t fi;
    make_fi_store(&fi, raw, sizeof(raw));
    iotspool_store_t store;
    bind_fi_store(&store, &fi);
    iotspool_t spool;
    iotspool_entry_t entries[4];
    uint8_t scratch[512];
    TEST_ASSERT_EQ(iotspool_init_inplace(NULL, entries, 4, scratch, sizeof(scratch), &cfg, &store),
                   IOTSPOOL_EINVAL);
    TEST_ASSERT_EQ(iotspool_init_inplace(&spool, NULL, 4, scratch, sizeof(scratch), &cfg, &store),
                   IOTSPOOL_EINVAL);
    TEST_ASSERT_EQ(iotspool_init_inplace(&spool, entries, 4, NULL, sizeof(scratch), &cfg, &store),
                   IOTSPOOL_EINVAL);
}

static void test_insufficient_buffers(void) {
    iotspool_cfg_t cfg = iotspool_cfg_default();
    uint8_t raw[4096];
    fi_store_t fi;
    make_fi_store(&fi, raw, sizeof(raw));
    iotspool_store_t store;
    bind_fi_store(&store, &fi);
    iotspool_t spool;
    iotspool_entry_t entries[1];
    uint8_t scratch[16];
    TEST_ASSERT_EQ(iotspool_init_inplace(&spool, entries, 1, scratch, sizeof(scratch), &cfg, &store),
                   IOTSPOOL_EINVAL);
}

static void test_topic_and_payload_validation(void) {
    uint8_t raw[4096];
    fi_store_t fi;
    make_fi_store(&fi, raw, sizeof(raw));
    iotspool_store_t store;
    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 4;
    cfg.max_topic_bytes = 16;
    cfg.max_payload_bytes = 16;
    iotspool_t spool;
    iotspool_entry_t entries[4];
    uint8_t scratch[128];
    TEST_ASSERT_EQ(make_spool(&spool, entries, scratch, sizeof(scratch), &fi, &store, &cfg),
                   IOTSPOOL_OK);

    iotspool_msg_t m = { .topic = "", .payload = NULL, .payload_len = 0, .qos = 0, .retain = false };
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, NULL), IOTSPOOL_EINVAL);

    char long_topic[32];
    for (size_t i = 0; i < sizeof(long_topic); ++i) long_topic[i] = 'a';
    long_topic[sizeof(long_topic) - 1] = '\0';
    m.topic = long_topic;
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, NULL), IOTSPOOL_EINVAL);

    m.topic = "ok";
    m.payload = (const uint8_t *)"x";
    m.payload_len = 1;
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, NULL), IOTSPOOL_OK);
    iotspool_deinit(&spool);
}

static void test_record_codec_lengths(void) {
    iotspool_msg_t m = {
        .topic = "topic",
        .payload = (const uint8_t *)"payload",
        .payload_len = 7,
        .qos = 1,
        .retain = true
    };
    uint8_t buf[256];
    uint32_t len = record_encode_enqueue(buf, sizeof(buf), &m, 7, 3, 9, false);
    TEST_ASSERT(len > 0);
    for (uint32_t i = 0; i < len; ++i) {
        record_enqueue_t enq = {0};
        uint8_t type = 0;
        uint32_t consumed = 0;
        iotspool_decode_result_t r = record_decode(buf, i, &type, &enq, NULL, NULL, &consumed);
        TEST_ASSERT(r == IOTSPOOL_DEC_INCOMPLETE_TAIL || r == IOTSPOOL_DEC_CLEAN_END);
    }
    record_enqueue_t enq = {0};
    uint8_t type = 0;
    uint32_t consumed = 0;
    TEST_ASSERT_EQ(record_decode(buf, len, &type, &enq, NULL, NULL, &consumed), IOTSPOOL_DEC_VALID);
    TEST_ASSERT_EQ(type, IOTSPOOL_REC_TYPE_ENQUEUE);
    TEST_ASSERT_EQ(consumed, len);
    TEST_ASSERT_EQ(enq.topic_len, 5u);
    TEST_ASSERT_EQ(enq.payload_len, 7u);
}

#ifdef IOTSPOOL_ENABLE_SHA256
static void test_sha256_known_answers(void) {
    uint8_t got[32];
    sha256((const uint8_t *)"", 0, got);
    const uint8_t empty_expected[32] = {
        0xe3,0xb0,0xc4,0x42,0x98,0xfc,0x1c,0x14,
        0x9a,0xfb,0xf4,0xc8,0x99,0x6f,0xb9,0x24,
        0x27,0xae,0x41,0xe4,0x64,0x9b,0x93,0x4c,
        0xa4,0x95,0x99,0x1b,0x78,0x52,0xb8,0x55
    };
    TEST_ASSERT(memcmp(got, empty_expected, sizeof(got)) == 0);

    sha256((const uint8_t *)"abc", 3, got);
    const uint8_t abc_expected[32] = {
        0xba,0x78,0x16,0xbf,0x8f,0x01,0xcf,0xea,
        0x41,0x41,0x40,0xde,0x5d,0xae,0x22,0x23,
        0xb0,0x03,0x61,0xa3,0x96,0x17,0x7a,0x9c,
        0xb4,0x10,0xff,0x61,0xf2,0x00,0x15,0xad
    };
    TEST_ASSERT(memcmp(got, abc_expected, sizeof(got)) == 0);
}

static void test_sha256_record_flag_when_enabled(void) {
    uint8_t raw[4096];
    fi_store_t fi;
    make_fi_store(&fi, raw, sizeof(raw));
    iotspool_store_t store;
    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 4;
    cfg.max_topic_bytes = 16;
    cfg.max_payload_bytes = 16;
    cfg.enable_sha256 = true;
    iotspool_t spool;
    iotspool_entry_t entries[4];
    uint8_t scratch[256];
    TEST_ASSERT_EQ(make_spool(&spool, entries, scratch, sizeof(scratch), &fi, &store, &cfg), IOTSPOOL_OK);

    iotspool_msg_t m = { .topic = "sha", .payload = (const uint8_t *)"x", .payload_len = 1, .qos = 0, .retain = false };
    iotspool_msg_id_t id;
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, &id), IOTSPOOL_OK);

    uint32_t got = 0;
    record_enqueue_t enq = {0};
    uint8_t type = 0;
    TEST_ASSERT_EQ(record_decode(raw + record_superblock_size(), fi.size - (uint32_t)record_superblock_size(),
                                 &type, &enq, NULL, NULL, &got), IOTSPOOL_DEC_VALID);
    TEST_ASSERT_EQ(type, IOTSPOOL_REC_TYPE_ENQUEUE);
    TEST_ASSERT_EQ((uint32_t)got > 0u, true);
    iotspool_deinit(&spool);
}
#endif

static void test_lock_callbacks_and_busy_path(void) {
    uint8_t raw[4096];
    fi_store_t fi;
    make_fi_store(&fi, raw, sizeof(raw));
    iotspool_store_t store;
    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 4;
    cfg.max_topic_bytes = 16;
    cfg.max_payload_bytes = 16;
    lock_ctx_t lock = {0};
    cfg.lock_ctx = &lock;
    cfg.lock = lock_cb;
    cfg.unlock = unlock_cb;
    iotspool_t spool;
    iotspool_entry_t entries[4];
    uint8_t scratch[256];
    TEST_ASSERT_EQ(make_spool(&spool, entries, scratch, sizeof(scratch), &fi, &store, &cfg), IOTSPOOL_OK);

    iotspool_msg_t m = { .topic = "a", .payload = (const uint8_t *)"b", .payload_len = 1, .qos = 0, .retain = false };
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, NULL), IOTSPOOL_OK);
    TEST_ASSERT(lock.lock_calls > 0u);
    TEST_ASSERT(lock.unlock_calls > 0u);

    lock.held = true;
    TEST_ASSERT_EQ(iotspool_ack(&spool, 1u), IOTSPOOL_EBUSY);
    TEST_ASSERT_EQ(iotspool_compact(&spool), IOTSPOOL_EBUSY);
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, NULL), IOTSPOOL_EBUSY);
    iotspool_deinit(&spool);
}

static void test_ack_unknown_does_not_grow_store(void) {
    uint8_t raw[4096];
    fi_store_t fi;
    make_fi_store(&fi, raw, sizeof(raw));
    iotspool_store_t store;
    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 4;
    cfg.max_topic_bytes = 16;
    cfg.max_payload_bytes = 16;
    iotspool_t spool;
    iotspool_entry_t entries[4];
    uint8_t scratch[256];
    TEST_ASSERT_EQ(make_spool(&spool, entries, scratch, sizeof(scratch), &fi, &store, &cfg), IOTSPOOL_OK);

    iotspool_msg_t m = { .topic = "a", .payload = (const uint8_t *)"b", .payload_len = 1, .qos = 0, .retain = false };
    iotspool_msg_id_t id;
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, &id), IOTSPOOL_OK);
    uint32_t before = fi.size;
    TEST_ASSERT_EQ(iotspool_ack(&spool, id + 1u), IOTSPOOL_ENOTFOUND);
    TEST_ASSERT_EQ(fi.size, before);
    TEST_ASSERT_EQ(iotspool_ack(&spool, id), IOTSPOOL_OK);
    before = fi.size;
    TEST_ASSERT_EQ(iotspool_ack(&spool, id), IOTSPOOL_ENOTFOUND);
    TEST_ASSERT_EQ(fi.size, before);
    iotspool_deinit(&spool);
}

static void test_persistent_drop_survives_reboot(void) {
    uint8_t raw[4096];
    fi_store_t fi;
    make_fi_store(&fi, raw, sizeof(raw));
    iotspool_store_t store;
    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 2;
    cfg.max_topic_bytes = 16;
    cfg.max_payload_bytes = 16;
    cfg.drop_oldest_on_full = false;
    iotspool_t spool;
    iotspool_entry_t entries[2];
    uint8_t scratch[256];
    TEST_ASSERT_EQ(make_spool(&spool, entries, scratch, sizeof(scratch), &fi, &store, &cfg), IOTSPOOL_OK);

    iotspool_msg_t m = { .topic = "a", .payload = (const uint8_t *)"b", .payload_len = 1, .qos = 0, .retain = false };
    iotspool_msg_id_t id1, id2;
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, &id1), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, &id2), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_drop_oldest(&spool, 0), IOTSPOOL_OK);
    iotspool_deinit(&spool);

    iotspool_t spool2;
    iotspool_entry_t entries2[2];
    uint8_t scratch2[256];
    TEST_ASSERT_EQ(iotspool_init_inplace(&spool2, entries2, 2, scratch2, sizeof(scratch2), &cfg, &store), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_recover(&spool2), IOTSPOOL_OK);
    iotspool_stats_t st;
    iotspool_stats(&spool2, &st);
    TEST_ASSERT_EQ(st.pending_count, 1u);
    iotspool_deinit(&spool2);
}

static void test_partial_enqueue_and_sync_failure(void) {
    uint8_t raw[4096];
    fi_store_t fi;
    make_fi_store(&fi, raw, sizeof(raw));
    iotspool_store_t store;
    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 4;
    cfg.max_topic_bytes = 16;
    cfg.max_payload_bytes = 16;
    iotspool_t spool;
    iotspool_entry_t entries[4];
    uint8_t scratch[256];
    TEST_ASSERT_EQ(make_spool(&spool, entries, scratch, sizeof(scratch), &fi, &store, &cfg), IOTSPOOL_OK);

    fi.fail_append_after = 0;
    fi.partial_append = true;
    iotspool_msg_t m = { .topic = "a", .payload = (const uint8_t *)"b", .payload_len = 1, .qos = 0, .retain = false };
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, NULL), IOTSPOOL_EIO);
    iotspool_deinit(&spool);

    TEST_ASSERT_EQ(iotspool_init_inplace(&spool, entries, 4, scratch, sizeof(scratch), &cfg, &store), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_recover(&spool), IOTSPOOL_OK);
    iotspool_stats_t st;
    iotspool_stats(&spool, &st);
    TEST_ASSERT_EQ(st.pending_count, 0u);
    TEST_ASSERT_EQ(fi.size, (uint32_t)record_superblock_size());
    iotspool_deinit(&spool);

    make_fi_store(&fi, raw, sizeof(raw));
    TEST_ASSERT_EQ(make_spool(&spool, entries, scratch, sizeof(scratch), &fi, &store, &cfg), IOTSPOOL_OK);
    iotspool_msg_id_t id;
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, &id), IOTSPOOL_OK);
    fi.fail_sync_after = 1;
    TEST_ASSERT_EQ(iotspool_ack(&spool, id), IOTSPOOL_EIO);
    iotspool_stats(&spool, &st);
    TEST_ASSERT_EQ(st.pending_count, 1u);
    iotspool_deinit(&spool);

    TEST_ASSERT_EQ(iotspool_init_inplace(&spool, entries, 4, scratch, sizeof(scratch), &cfg, &store), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_recover(&spool), IOTSPOOL_OK);
    iotspool_stats(&spool, &st);
    TEST_ASSERT_EQ(st.pending_count, 0u);
    iotspool_deinit(&spool);
}

static void test_partial_drop_and_tail_repair(void) {
    uint8_t raw[4096];
    fi_store_t fi;
    make_fi_store(&fi, raw, sizeof(raw));
    iotspool_store_t store;
    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 4;
    cfg.max_topic_bytes = 16;
    cfg.max_payload_bytes = 16;
    iotspool_t spool;
    iotspool_entry_t entries[4];
    uint8_t scratch[256];
    TEST_ASSERT_EQ(make_spool(&spool, entries, scratch, sizeof(scratch), &fi, &store, &cfg), IOTSPOOL_OK);

    iotspool_msg_t m = { .topic = "a", .payload = (const uint8_t *)"b", .payload_len = 1, .qos = 0, .retain = false };
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, NULL), IOTSPOOL_OK);
    fi.fail_append_after = 1;
    fi.partial_append = true;
    TEST_ASSERT_EQ(iotspool_drop_oldest(&spool, 0), IOTSPOOL_EIO);
    iotspool_stats_t st;
    iotspool_stats(&spool, &st);
    TEST_ASSERT_EQ(st.pending_count, 1u);
    iotspool_deinit(&spool);

    TEST_ASSERT_EQ(iotspool_init_inplace(&spool, entries, 4, scratch, sizeof(scratch), &cfg, &store), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_recover(&spool), IOTSPOOL_OK);
    iotspool_stats(&spool, &st);
    TEST_ASSERT_EQ(st.pending_count, 1u);
    iotspool_deinit(&spool);
}

static void test_compaction_empty_store(void) {
    uint8_t raw[4096];
    fi_store_t fi;
    make_fi_store(&fi, raw, sizeof(raw));
    iotspool_store_t store;
    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 2;
    cfg.max_topic_bytes = 16;
    cfg.max_payload_bytes = 16;
    iotspool_t spool;
    iotspool_entry_t entries[2];
    uint8_t scratch[256];
    TEST_ASSERT_EQ(make_spool(&spool, entries, scratch, sizeof(scratch), &fi, &store, &cfg), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_compact(&spool), IOTSPOOL_OK);
    TEST_ASSERT_EQ(fi.size, (uint32_t)record_superblock_size());
    TEST_ASSERT_EQ(fi.replace_calls, 1u);
    iotspool_deinit(&spool);
}

static void test_compaction_mixed_live_dead_records(void) {
    uint8_t raw[8192];
    fi_store_t fi;
    make_fi_store(&fi, raw, sizeof(raw));
    iotspool_store_t store;
    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 4;
    cfg.max_topic_bytes = 16;
    cfg.max_payload_bytes = 16;
    cfg.max_store_bytes = 512;
    iotspool_t spool;
    iotspool_entry_t entries[4];
    uint8_t scratch[512];
    TEST_ASSERT_EQ(make_spool(&spool, entries, scratch, sizeof(scratch), &fi, &store, &cfg), IOTSPOOL_OK);

    iotspool_msg_t m = { .topic = "a", .payload = (const uint8_t *)"b", .payload_len = 1, .qos = 0, .retain = false };
    iotspool_msg_id_t id1, id2, id3;
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, &id1), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, &id2), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, &id3), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_ack(&spool, id2), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_drop_oldest(&spool, 0), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_compact(&spool), IOTSPOOL_OK);

    iotspool_stats_t st;
    iotspool_stats(&spool, &st);
    TEST_ASSERT_EQ(st.pending_count, 1u);
    TEST_ASSERT(st.store_bytes <= cfg.max_store_bytes);

    iotspool_deinit(&spool);
    TEST_ASSERT_EQ(iotspool_init_inplace(&spool, entries, 4, scratch, sizeof(scratch), &cfg, &store), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_recover(&spool), IOTSPOOL_OK);
    iotspool_stats(&spool, &st);
    TEST_ASSERT_EQ(st.pending_count, 1u);
    iotspool_deinit(&spool);
}

static void test_compaction_abort_and_commit(void) {
    uint8_t raw[8192];
    fi_store_t fi;
    make_fi_store(&fi, raw, sizeof(raw));
    iotspool_store_t store;
    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 4;
    cfg.max_topic_bytes = 16;
    cfg.max_payload_bytes = 16;
    cfg.max_store_bytes = 512;
    iotspool_t spool;
    iotspool_entry_t entries[4];
    uint8_t scratch[512];
    TEST_ASSERT_EQ(make_spool(&spool, entries, scratch, sizeof(scratch), &fi, &store, &cfg), IOTSPOOL_OK);

    iotspool_msg_t m = { .topic = "a", .payload = (const uint8_t *)"b", .payload_len = 1, .qos = 0, .retain = false };
    iotspool_msg_id_t id1, id2;
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, &id1), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, &id2), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_ack(&spool, id1), IOTSPOOL_OK);

    fi.fail_replace_after = 0;
    TEST_ASSERT_EQ(iotspool_compact(&spool), IOTSPOOL_EIO);
    iotspool_stats_t st;
    iotspool_stats(&spool, &st);
    TEST_ASSERT_EQ(st.pending_count, 1u);

    fi.fail_replace_after = -1;
    TEST_ASSERT_EQ(iotspool_compact(&spool), IOTSPOOL_OK);
    iotspool_deinit(&spool);

    TEST_ASSERT_EQ(iotspool_init_inplace(&spool, entries, 4, scratch, sizeof(scratch), &cfg, &store), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_recover(&spool), IOTSPOOL_OK);
    iotspool_stats(&spool, &st);
    TEST_ASSERT_EQ(st.pending_count, 1u);
    TEST_ASSERT(fi.replace_calls >= 2u);
    iotspool_deinit(&spool);
}

static void test_drop_policy_persists_and_store_bound(void) {
    uint8_t raw[8192];
    fi_store_t fi;
    make_fi_store(&fi, raw, sizeof(raw));
    iotspool_store_t store;
    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 2;
    cfg.max_topic_bytes = 16;
    cfg.max_payload_bytes = 16;
    cfg.max_store_bytes = 512;
    cfg.drop_oldest_on_full = true;
    iotspool_t spool;
    iotspool_entry_t entries[2];
    uint8_t scratch[256];
    TEST_ASSERT_EQ(make_spool(&spool, entries, scratch, sizeof(scratch), &fi, &store, &cfg), IOTSPOOL_OK);

    iotspool_msg_t m = { .topic = "a", .payload = (const uint8_t *)"b", .payload_len = 1, .qos = 0, .retain = false };
    iotspool_msg_id_t id1, id2, id3;
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, &id1), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, &id2), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, &id3), IOTSPOOL_OK);
    iotspool_stats_t st;
    iotspool_stats(&spool, &st);
    TEST_ASSERT_EQ(st.pending_count, 2u);
    TEST_ASSERT(st.store_bytes <= cfg.max_store_bytes);
    iotspool_deinit(&spool);

    TEST_ASSERT_EQ(iotspool_init_inplace(&spool, entries, 2, scratch, sizeof(scratch), &cfg, &store), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_recover(&spool), IOTSPOOL_OK);
    iotspool_stats(&spool, &st);
    TEST_ASSERT_EQ(st.pending_count, 2u);
    TEST_ASSERT(st.store_bytes <= cfg.max_store_bytes);
    iotspool_deinit(&spool);
}

static void test_inflight_token_semantics(void) {
    uint8_t raw[4096];
    fi_store_t fi;
    make_fi_store(&fi, raw, sizeof(raw));
    iotspool_store_t store;
    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 4;
    cfg.max_topic_bytes = 16;
    cfg.max_payload_bytes = 16;
    iotspool_t spool;
    iotspool_entry_t entries[4];
    uint8_t scratch[256];
    TEST_ASSERT_EQ(make_spool(&spool, entries, scratch, sizeof(scratch), &fi, &store, &cfg), IOTSPOOL_OK);

    iotspool_msg_t m = { .topic = "a", .payload = (const uint8_t *)"b", .payload_len = 1, .qos = 1, .retain = false };
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, NULL), IOTSPOOL_OK);

    iotspool_inflight_t token;
    TEST_ASSERT_EQ(iotspool_acquire_ready(&spool, 0, &token), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_acquire_ready(&spool, 0, &token), IOTSPOOL_EBUSY);
    iotspool_inflight_t bad = token;
    bad.generation++;
    TEST_ASSERT_EQ(iotspool_publish_confirmed(&spool, &bad), IOTSPOOL_ESTATE);
    TEST_ASSERT_EQ(iotspool_publish_confirmed(&spool, &token), IOTSPOOL_OK);
    iotspool_deinit(&spool);
}

static void test_tail_repair_and_reboot(void) {
    uint8_t raw[4096];
    fi_store_t fi;
    make_fi_store(&fi, raw, sizeof(raw));
    iotspool_store_t store;
    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 4;
    cfg.max_topic_bytes = 16;
    cfg.max_payload_bytes = 16;
    iotspool_t spool;
    iotspool_entry_t entries[4];
    uint8_t scratch[512];
    TEST_ASSERT_EQ(make_spool(&spool, entries, scratch, sizeof(scratch), &fi, &store, &cfg), IOTSPOOL_OK);

    iotspool_msg_t m = { .topic = "a", .payload = (const uint8_t *)"b", .payload_len = 1, .qos = 0, .retain = false };
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, NULL), IOTSPOOL_OK);
    uint32_t before = fi.size;
    fi.size = before - 2u;
    iotspool_deinit(&spool);

    iotspool_t spool2;
    iotspool_entry_t entries2[4];
    uint8_t scratch2[512];
    TEST_ASSERT_EQ(iotspool_init_inplace(&spool2, entries2, 4, scratch2, sizeof(scratch2), &cfg, &store), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_recover(&spool2), IOTSPOOL_OK);
    TEST_ASSERT_EQ(fi.size, (uint32_t)record_superblock_size());
    TEST_ASSERT_EQ(fi.truncate_calls, 1u);
    iotspool_deinit(&spool2);
}

static void test_randomized_model(void) {
    unsigned seed = 12345u;
    uint8_t raw[8192];
    fi_store_t fi;
    make_fi_store(&fi, raw, sizeof(raw));
    iotspool_store_t store;
    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 8;
    cfg.max_topic_bytes = 16;
    cfg.max_payload_bytes = 16;
    iotspool_t spool;
    iotspool_entry_t entries[8];
    uint8_t scratch[512];
    TEST_ASSERT_EQ(make_spool(&spool, entries, scratch, sizeof(scratch), &fi, &store, &cfg), IOTSPOOL_OK);

    uint64_t model[8];
    uint32_t model_count = 0;
    for (int i = 0; i < 200; ++i) {
        uint32_t now_ms = (uint32_t)i * 10u;
        seed = seed * 1103515245u + 12345u;
        unsigned op = (seed >> 16) % 6u;
        if (op == 0 && model_count < 8) {
            char topic[8];
            topic[0] = 't';
            topic[1] = (char)('0' + ((seed >> 8) & 7u));
            topic[2] = '\0';
            iotspool_msg_t m = { .topic = topic, .payload = (const uint8_t *)"x", .payload_len = 1, .qos = 0, .retain = false };
            iotspool_msg_id_t id = 0;
            if (iotspool_enqueue(&spool, &m, &id) == IOTSPOOL_OK) {
                model[model_count++] = id;
            }
        } else if (op == 1 && model_count > 0) {
            iotspool_inflight_t token;
            if (iotspool_acquire_ready(&spool, now_ms, &token) == IOTSPOOL_OK) {
                TEST_ASSERT_EQ(token.id, model[0]);
                TEST_ASSERT_EQ(iotspool_publish_confirmed(&spool, &token), IOTSPOOL_OK);
                move_u64_left(model, model_count);
                --model_count;
            }
        } else if (op == 2 && model_count > 0) {
            TEST_ASSERT_EQ(iotspool_ack(&spool, model[0]), IOTSPOOL_OK);
            move_u64_left(model, model_count);
            --model_count;
        } else if (op == 3 && model_count > 0) {
            TEST_ASSERT_EQ(iotspool_drop_oldest(&spool, now_ms), IOTSPOOL_OK);
            move_u64_left(model, model_count);
            --model_count;
        } else if (op == 4) {
            TEST_ASSERT_EQ(iotspool_recover(&spool), IOTSPOOL_ESTATE);
        }
    }
    iotspool_deinit(&spool);
}

static void test_posix_backend_roundtrip(void) {
    const char *path = "iotspool_test.log";
    remove(path);

    iotspool_store_t store;
    iotspool_err_t open_err = store_posix_open(path, &store);
    TEST_ASSERT_EQ(open_err, IOTSPOOL_OK);
    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.max_pending_msgs = 4;
    cfg.max_topic_bytes = 16;
    cfg.max_payload_bytes = 16;
    iotspool_t spool;
    iotspool_entry_t entries[4];
    uint8_t scratch[256];
    TEST_ASSERT_EQ(iotspool_init_inplace(&spool, entries, 4, scratch, sizeof(scratch), &cfg, &store), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_recover(&spool), IOTSPOOL_OK);

    iotspool_msg_t m = { .topic = "posix", .payload = (const uint8_t *)"x", .payload_len = 1, .qos = 0, .retain = false };
    TEST_ASSERT_EQ(iotspool_enqueue(&spool, &m, NULL), IOTSPOOL_OK);
    iotspool_deinit(&spool);
    store_posix_close(&store);

    TEST_ASSERT_EQ(store_posix_open(path, &store), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_init_inplace(&spool, entries, 4, scratch, sizeof(scratch), &cfg, &store), IOTSPOOL_OK);
    TEST_ASSERT_EQ(iotspool_recover(&spool), IOTSPOOL_OK);
    iotspool_stats_t st;
    iotspool_stats(&spool, &st);
    TEST_ASSERT_EQ(st.pending_count, 1u);
    iotspool_deinit(&spool);
    store_posix_close(&store);
    remove(path);
}

int main(void) {
    RUN_TEST(test_config_validation);
    RUN_TEST(test_null_inputs);
    RUN_TEST(test_insufficient_buffers);
    RUN_TEST(test_topic_and_payload_validation);
    RUN_TEST(test_record_codec_lengths);
#ifdef IOTSPOOL_ENABLE_SHA256
    RUN_TEST(test_sha256_known_answers);
    RUN_TEST(test_sha256_record_flag_when_enabled);
#endif
    RUN_TEST(test_lock_callbacks_and_busy_path);
    RUN_TEST(test_ack_unknown_does_not_grow_store);
    RUN_TEST(test_persistent_drop_survives_reboot);
    RUN_TEST(test_partial_enqueue_and_sync_failure);
    RUN_TEST(test_partial_drop_and_tail_repair);
    RUN_TEST(test_inflight_token_semantics);
    RUN_TEST(test_tail_repair_and_reboot);
    RUN_TEST(test_compaction_empty_store);
    RUN_TEST(test_compaction_mixed_live_dead_records);
    RUN_TEST(test_compaction_abort_and_commit);
    RUN_TEST(test_drop_policy_persists_and_store_bound);
    RUN_TEST(test_randomized_model);
    RUN_TEST(test_posix_backend_roundtrip);

    printf("%d tests run\n", g_tests);
    if (g_failed) {
        printf("%d tests failed\n", g_failed);
        return 1;
    }
    return 0;
}
