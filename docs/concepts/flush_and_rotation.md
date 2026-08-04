# Flush and rotation

Capture buffers rows in memory and flushes complete Parquet chunks (chunked mode)
or appends to a segment file until seal (segment mode). Tuning is global under
`[output]` today.

## Defaults (general live)

```toml
[output]
flush_rows = 5000
flush_interval_ms = 1000
max_buffer_bytes = 33554432   # 32 MiB
queue_capacity = 10000
```

## Profiles

| Profile | Use | `flush_rows` | `flush_interval_ms` | `max_buffer_bytes` |
|---------|-----|--------------|---------------------|--------------------|
| A General live | quotes/trades | 5000 | 1000 | 32 MiB |
| B Unattended / universe | mixed greeks | 5000 | 5000 | 64 MiB |
| C Book deltas | high rate | 20000 | 1000 | 128 MiB |
| D Sparse / request poll | low rate | 1000 | 10000 | 16 MiB |
| E Smoke only | force chunks | 3 | 250 | 64 KiB |

## Per-family guidance (dominant family)

| Family | `flush_rows` | `flush_interval_ms` | `max_buffer_bytes` |
|--------|--------------|---------------------|--------------------|
| quotes / trades | 5k–20k | 1–2s | 32–64 MiB |
| order_book_deltas | 10k–50k | 0.5–1s | 64–128 MiB |
| bars | 500–2k | 5–30s | 16–32 MiB |
| option_greeks | 1k–5k | 2–5s | 32–64 MiB |
| funding / status | 100–1k | 30–300s | 8–16 MiB |
| custom request | 500–5k | ≥ poll interval | 16–64 MiB |

Watch `/metrics`: `dropped_items`, `active_partitions`, `flush_reasons`,
`catalog_capture_custom_data_request_*`.

Segment seal (daily files) is separate — see [segment lifecycle](segment_lifecycle.md).
