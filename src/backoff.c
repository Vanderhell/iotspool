/* backoff.c – Exponential backoff with full jitter ("Full Jitter" strategy).
 *
 * sleep = random_between(0, min(cap, max))
 * cap doubles on each failure until max is reached.
 *
 * SPDX-License-Identifier: MIT */

#include "backoff.h"

/* Simple LCG for jitter – no stdlib dependency needed. */
static uint32_t s_seed = 0xdeadbeef;
static uint32_t lcg_rand(void) {
    s_seed = s_seed * 1664525u + 1013904223u;
    return s_seed;
}

void backoff_init(backoff_t *b, uint32_t min_ms, uint32_t max_ms) {
    b->min_ms         = min_ms;
    b->max_ms         = max_ms;
    b->current_cap_ms = min_ms;
    b->retry_after_ms = 0;
}

void backoff_reset(backoff_t *b) {
    b->current_cap_ms = b->min_ms;
    b->retry_after_ms = 0;
}

void backoff_on_fail(backoff_t *b, uint32_t now_ms) {
    uint32_t cap = b->current_cap_ms;
    if (cap > b->max_ms) cap = b->max_ms;

    /* Full jitter: sleep in [0, cap] */
    uint32_t sleep_ms = (cap > 0) ? (lcg_rand() % (cap + 1)) : 0;
    if (sleep_ms < b->min_ms) sleep_ms = b->min_ms;

    b->retry_after_ms = now_ms + sleep_ms;

    /* Double cap for next failure */
    if (b->current_cap_ms < b->max_ms / 2)
        b->current_cap_ms *= 2;
    else
        b->current_cap_ms = b->max_ms;
}

int backoff_is_ready(const backoff_t *b, uint32_t now_ms) {
    return (now_ms >= b->retry_after_ms);
}

uint32_t backoff_next_ms(const backoff_t *b) {
    return b->retry_after_ms;
}
