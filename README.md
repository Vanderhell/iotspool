# iotspool

> **Persistent store-and-forward queue for MQTT publish.**  
> Survives power loss and reboots. Runs on Linux, ESP32, STM32.  
> No RTOS required. C99. Zero dynamic dependencies.

```
Device reboots at 3am. WiFi is down. You lose no telemetry.
That's iotspool.
```

[![CI](https://github.com/Vanderhell/iotspool/actions/workflows/ci.yml/badge.svg)](https://github.com/Vanderhell/iotspool/actions)
![C99](https://img.shields.io/badge/C-99-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Platforms](https://img.shields.io/badge/platform-Linux%20%7C%20ESP32%20%7C%20STM32-lightgrey)

---

## The problem

Every IoT device eventually loses connectivity or power. The naive solution – 
publish directly when data arrives – drops messages silently. Rolling your own 
buffer is fragile and untested under power-loss. iotspool is the missing piece.

## What it does

- **Enqueue** MQTT publish messages to a crash-safe append-only log
- **Recover** the queue automatically after power loss or reboot
- **Retry** with exponential backoff + jitter when the broker is unreachable
- **Acknowledge** successful delivery (QoS 0 and QoS 1)
- Works with **any MQTT client** – coreMQTT, Paho, mosquitto, your own

## Quickstart (Linux / Raspberry Pi)

```c
#include "iotspool.h"
#include "store_posix.h"   /* included in src/ */

/* 1. Open store */
iotspool_store_t store = {0};
store_posix_open("/var/spool/mqtt.bin", &store);

/* 2. Init + recover pending queue from disk */
iotspool_cfg_t cfg = iotspool_cfg_default();
iotspool_t *spool = NULL;
iotspool_init(&spool, &cfg, &store);
iotspool_recover(spool);   /* safe to call on empty store */

/* 3. Enqueue a message (persisted before this returns) */
iotspool_msg_t m = {
    .topic       = "factory/sensor/temp",
    .payload     = (const uint8_t *)"{\"v\":72.3}",
    .payload_len = 10,
    .qos         = 1,
};
iotspool_msg_id_t id;
iotspool_enqueue(spool, &m, &id);

/* 4. In your publish loop */
iotspool_msg_t out;
iotspool_msg_id_t out_id;
if (iotspool_peek_ready(spool, now_ms(), &out, &out_id) == IOTSPOOL_OK) {
    if (mqtt_publish(out.topic, out.payload, out.payload_len) == 0)
        iotspool_ack(spool, out_id);
    else
        iotspool_on_publish_fail(spool, now_ms()); /* triggers backoff */
}
```

Build:
```bash
cmake -S . -B build && cmake --build build -j
```

## Supported targets

| Platform | Storage backend | Notes |
|---|---|---|
| **Linux SBC** (Raspberry Pi, etc.) | POSIX file (`store_posix`) | Included |
| **ESP32** (ESP-IDF) | VFS file via `store_posix` | Same backend, VFS mount |
| **STM32** / bare-metal | Custom via vtable callbacks | Provide your own flash/FS adapter |

## Storage backends

The library uses a simple vtable (`iotspool_store_t`) with four required callbacks:
`append`, `read_at`, `sync`, `size_bytes`. Provide your own to target any storage.

**POSIX / ESP-IDF VFS** (included):
```c
iotspool_store_t store = {0};
store_posix_open("/spiffs/spool.bin", &store);  /* same API on ESP32 via VFS */
```

**Custom (e.g. raw flash)**:
```c
iotspool_store_t store = {
    .ctx        = &my_flash_ctx,
    .append     = my_flash_append,
    .read_at    = my_flash_read_at,
    .sync       = my_flash_sync,
    .size_bytes = my_flash_size,
};
```

## QoS semantics

| QoS | When to call `iotspool_ack()` |
|-----|-------------------------------|
| 0   | After the transport layer confirms the packet was sent |
| 1   | After receiving `PUBACK` from the broker |

QoS 2 is outside the current scope. The library guarantees **at-least-once** 
delivery for QoS 1 across reboots.

## Crash recovery and integrity

Every record in the log carries a **CRC32** checksum. An incomplete tail 
(power-loss mid-write) is detected and silently trimmed during `iotspool_recover()`.

Optionally enable **SHA-256** per record for stronger corruption detection:
```c
cfg.enable_sha256 = true;
```
Note: SHA-256 here detects silent data corruption, not adversarial tampering.
For authentication, add a MAC layer on top.

## Configuration

```c
iotspool_cfg_t cfg = iotspool_cfg_default();
cfg.max_pending_msgs   = 128;      /* RAM index limit           */
cfg.max_store_bytes    = 512*1024; /* 512 KiB store cap         */
cfg.min_retry_ms       = 1000;     /* backoff floor             */
cfg.max_retry_ms       = 60000;    /* backoff ceiling           */
cfg.drop_oldest_on_full = true;    /* evict old data when full  */
```

## Building and CI

```bash
# Host (Linux) – build + test
cmake -S . -B build -DIOTSPOOL_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure

# Cortex-M compile check
cmake -S . -B build-stm32 \
  -DCMAKE_TOOLCHAIN_FILE=cmake/toolchains/arm-none-eabi-gcc.cmake \
  -DIOTSPOOL_BUILD_TESTS=OFF
cmake --build build-stm32 -j
```

CI runs on every push: gcc + clang + AddressSanitizer + arm-none-eabi compile check.

## Testing

The test suite (`tests/test_main.c`) covers:
- SHA-256 NIST FIPS 180-4 known-answer vectors
- CRC32 known value (`123456789` → `0xCBF43926`)
- Record encode/decode round-trip
- CRC corruption detection
- Full lifecycle: enqueue → persist → simulated reboot → recover → ack
- Power-loss simulation: truncated store tail is safely ignored
- Backpressure: `IOTSPOOL_EFULL` returned when queue is full
- Idempotent ACK

## License

MIT – free for commercial and personal use.
