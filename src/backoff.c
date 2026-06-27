/*
 * backoff.c - Exponential backoff with per-context PRNG.
 *
 * SPDX-License-Identifier: MIT
 */

#include "backoff.h"

static uint32_t xorshift32(uint32_t *state) {
    uint32_t x = *state;
    if (x == 0) x = 0x6d2b79f5u;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x;
    return x;
}

static bool time_after_eq(uint32_t now_ms, uint32_t deadline_ms) {
    return (int32_t)(now_ms - deadline_ms) >= 0;
}

static uint32_t clamp_add_u32(uint32_t a, uint32_t b) {
    if (UINT32_MAX - a < b) return UINT32_MAX;
    return a + b;
}

void backoff_init(backoff_t *b, uint32_t min_ms, uint32_t max_ms,
                  uint32_t seed)
{
    if (!b) return;
    b->min_ms = min_ms;
    b->max_ms = (max_ms < min_ms) ? min_ms : max_ms;
    b->cap_ms = min_ms;
    b->retry_after_ms = 0;
    b->failure_count = 0;
    b->rng_state = seed ? seed : 0x9e3779b9u;
}

void backoff_reset(backoff_t *b) {
    if (!b) return;
    b->cap_ms = b->min_ms;
    b->retry_after_ms = 0;
    b->failure_count = 0;
}

void backoff_on_fail(backoff_t *b, uint32_t now_ms) {
    if (!b) return;
    uint32_t cap = b->cap_ms;
    if (cap == 0) cap = b->min_ms;
    if (cap > b->max_ms) cap = b->max_ms;

    uint32_t jitter = (cap == 0) ? 0 : (xorshift32(&b->rng_state) % (cap + 1u));
    if (jitter < b->min_ms) jitter = b->min_ms;
    b->retry_after_ms = clamp_add_u32(now_ms, jitter);
    ++b->failure_count;

    if (b->cap_ms < b->max_ms / 2u) {
        b->cap_ms *= 2u;
        if (b->cap_ms < b->min_ms) b->cap_ms = b->max_ms;
    } else {
        b->cap_ms = b->max_ms;
    }
}

bool backoff_is_ready(const backoff_t *b, uint32_t now_ms) {
    if (!b) return false;
    return time_after_eq(now_ms, b->retry_after_ms);
}

uint32_t backoff_next_deadline(const backoff_t *b) {
    return b ? b->retry_after_ms : 0;
}
