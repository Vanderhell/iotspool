/* backoff.h – Exponential backoff with full jitter.
 * SPDX-License-Identifier: MIT */
#pragma once
#include <stdint.h>

typedef struct {
    uint32_t min_ms;
    uint32_t max_ms;
    uint32_t current_cap_ms; /* current ceiling, doubles each failure */
    uint32_t retry_after_ms; /* absolute timestamp of next allowed retry */
} backoff_t;

void     backoff_init    (backoff_t *b, uint32_t min_ms, uint32_t max_ms);
void     backoff_reset   (backoff_t *b);
void     backoff_on_fail (backoff_t *b, uint32_t now_ms);
int      backoff_is_ready(const backoff_t *b, uint32_t now_ms);
uint32_t backoff_next_ms (const backoff_t *b); /* ms until next retry */
