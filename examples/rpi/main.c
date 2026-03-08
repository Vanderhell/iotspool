/**
 * example: Raspberry Pi / Linux host demo
 *
 * Simulates an IoT device that:
 *  - Spools 5 sensor readings
 *  - Goes "offline" (skips publish)
 *  - Comes back online and drains the queue
 *
 * Build:  cmake --build build && ./build/example_rpi
 * SPDX-License-Identifier: MIT
 */

#include <stdio.h>
#include <string.h>
#include <time.h>
#include "iotspool.h"
#include "../../src/store_posix.h"

#define SPOOL_FILE "/tmp/demo_spool.bin"

static uint32_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint32_t)(ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
}

/* Fake MQTT publish – returns 0 on success */
static int fake_mqtt_publish(const iotspool_msg_t *m) {
    printf("  [MQTT] publish  topic=%-25s  payload=%.*s  qos=%d\n",
           m->topic, (int)m->payload_len, (const char *)m->payload, m->qos);
    return 0; /* always succeeds in this demo */
}

int main(void) {
    printf("=== iotspool demo (Linux/Raspberry Pi) ===\n\n");

    /* ── Open store ───────────────────────────────────────────────────── */
    iotspool_store_t store = {0};
    if (store_posix_open(SPOOL_FILE, &store) != IOTSPOOL_OK) {
        fprintf(stderr, "Failed to open spool file %s\n", SPOOL_FILE);
        return 1;
    }

    /* ── Init + recover ───────────────────────────────────────────────── */
    iotspool_cfg_t cfg = iotspool_cfg_default();
    cfg.min_retry_ms = 100;  /* short backoff for demo */
    cfg.max_retry_ms = 2000;

    iotspool_t *spool = NULL;
    iotspool_init(&spool, &cfg, &store);
    iotspool_recover(spool);

    iotspool_stats_t st;
    iotspool_stats(spool, &st);
    printf("Recovered %u pending message(s) from store.\n\n", st.pending_count);

    /* ── Enqueue some sensor readings ─────────────────────────────────── */
    const char *topics[] = {
        "factory/line1/temp",
        "factory/line1/humidity",
        "factory/line2/temp",
        "factory/line2/pressure",
        "factory/alarm/smoke"
    };
    const char *payloads[] = {
        "{\"v\":72.3}", "{\"v\":45.1}", "{\"v\":68.9}",
        "{\"v\":1013}", "{\"v\":0}"
    };

    printf("Enqueueing 5 sensor readings...\n");
    for (int i = 0; i < 5; i++) {
        iotspool_msg_t m = {
            .topic       = topics[i],
            .payload     = (const uint8_t *)payloads[i],
            .payload_len = (uint32_t)strlen(payloads[i]),
            .qos         = 1,
            .retain      = false
        };
        iotspool_msg_id_t id;
        iotspool_err_t err = iotspool_enqueue(spool, &m, &id);
        printf("  enqueue %-25s  id=%u  %s\n",
               topics[i], id, iotspool_strerror(err));
    }

    /* ── Drain queue ──────────────────────────────────────────────────── */
    printf("\nDraining queue (simulated online)...\n");
    for (int attempt = 0; attempt < 20; attempt++) {
        iotspool_msg_t out = {0};
        iotspool_msg_id_t out_id;
        iotspool_err_t err = iotspool_peek_ready(spool, now_ms(), &out, &out_id);
        if (err == IOTSPOOL_ENOTFOUND) break;
        if (err != IOTSPOOL_OK) {
            fprintf(stderr, "peek error: %s\n", iotspool_strerror(err));
            break;
        }

        if (fake_mqtt_publish(&out) == 0) {
            iotspool_ack(spool, out_id);
        } else {
            iotspool_on_publish_fail(spool, now_ms());
        }
    }

    iotspool_stats(spool, &st);
    printf("\nStats: pending=%u  enqueued=%u  acked=%u  store=%u bytes\n",
           st.pending_count, st.enqueued_total,
           st.acked_total, st.store_bytes);

    iotspool_deinit(spool);
    store_posix_close(&store);
    remove(SPOOL_FILE);
    return 0;
}
