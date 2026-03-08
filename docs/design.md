# iotspool – Design Document

## Record binary format

All integers are **little-endian**.

### ENQ record

```
Offset  Size  Field
------  ----  -----
0       1     magic        = 0xE7
1       1     type         = 0x01 (ENQ)
2       1     version      = 0x01
3       1     flags        bit0=sha256_present  bit1=retain  bit2=qos1
4       4     msg_id       u32
8       4     timestamp_ms u32  (low 32 bits of monotonic clock)
12      4     topic_len    u32
16      4     payload_len  u32
20      N     topic        (no null terminator)
20+N    M     payload
[opt]   32    sha256       (only if flags bit0 set)
last    4     crc32        (covers all preceding bytes)
```

### ACK record

```
Offset  Size  Field
------  ----  -----
0       1     magic    = 0xE7
1       1     type     = 0x02 (ACK)
2       1     version  = 0x01
3       1     reserved = 0x00
4       4     msg_id   u32
8       4     crc32    (covers bytes 0..7)
```

## Crash-consistency guarantees

- Every write is followed by `sync()` (fsync on Linux).
- An incomplete tail record (power-loss mid-write) is detected by:
  1. EOF before the expected record end, OR
  2. CRC32 mismatch on the last record.
- Recovery silently discards any incomplete tail and continues with all
  fully-committed records.

## Backoff algorithm

Full Jitter exponential backoff:

```
sleep = random(0, min(cap, max_ms))
cap   = min(cap * 2, max_ms)  on each failure
```

Reset to `min_retry_ms` on successful ACK.

## Forward compatibility

- `version` field is present in every record header.
- `reserved` / `flags` bytes provide room for future extensions.
- A reader MUST skip (not reject) records with unknown `type` values.

## Threat model limitations

- CRC32 and SHA-256 detect **accidental** corruption, not adversarial tampering.
- For authenticated integrity, add HMAC-SHA256 with a device key in a higher layer.
- The store file is not encrypted; use filesystem-level encryption if confidentiality is required.
