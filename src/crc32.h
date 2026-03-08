/**
 * crc32.h - CRC-32/ISO-HDLC (polynomial 0xEDB88320, reflected)
 *
 * Standard CRC32 as used in zlib, gzip, Ethernet.
 * Lookup table generated at compile time via X-macro trick.
 *
 * SPDX-License-Identifier: MIT
 */

#pragma once
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Update a running CRC32 checksum.
 *
 * Usage:
 *   uint32_t crc = 0xFFFFFFFFu;
 *   crc = crc32_update(crc, data, len);
 *   crc = crc32_update(crc, more, more_len);
 *   uint32_t final = crc ^ 0xFFFFFFFFu;
 */
uint32_t crc32_update(uint32_t crc, const uint8_t *data, size_t len);

/**
 * Convenience: compute CRC32 of a single buffer.
 */
static inline uint32_t crc32_calc(const uint8_t *data, size_t len)
{
    return crc32_update(0xFFFFFFFFu, data, len) ^ 0xFFFFFFFFu;
}

#ifdef __cplusplus
}
#endif
