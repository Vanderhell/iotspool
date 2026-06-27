/*
 * backoff.h - Per-context exponential backoff with jitter.
 *
 * SPDX-License-Identifier: MIT
 */

#pragma once

#include <stdbool.h>
#include <stdint.h>

typedef struct {
    uint32_t min_ms;
    uint32_t max_ms;
    uint32_t cap_ms;
    uint32_t retry_after_ms;
    uint32_t failure_count;
    uint32_t rng_state;
} backoff_t;

void     backoff_init(backoff_t *b, uint32_t min_ms, uint32_t max_ms,
                      uint32_t seed);
void     backoff_reset(backoff_t *b);
void     backoff_on_fail(backoff_t *b, uint32_t now_ms);
bool     backoff_is_ready(const backoff_t *b, uint32_t now_ms);
uint32_t backoff_next_deadline(const backoff_t *b);
