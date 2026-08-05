# Flush and rotation

Capture buffers rows in memory and flushes into open segment parts by **default**
(`mode = segment`), or into complete catalog parquet files when you opt into
`mode = chunked` (smoke only).

TOML still exposes a **baseline** `flush_rows` / `row_group_rows`. At runtime the actor
applies a **per-family flush profile** (`catalog_capture_core::flush_profile`) so L2 and
BookSummary do not share one number.

## Defaults (general live)

```toml
[output]
flush_rows = 5000
flush_interval_ms = 1000
max_buffer_bytes = 33554432   # 32 MiB
queue_capacity = 10000        # ingress queue depth — not the row flush threshold
```

## Is 20 000 “the” flush threshold?

| Knob | Role | ~20k reasonable? |
|------|------|------------------|
| `queue_capacity = 20000` | Items waiting for the family worker | **Yes** for multi-venue (L2 bursts) |
| `row_group_rows` / family profile for **book_deltas** | Memory → open `.part` batch size | **Yes** (~20k) — high rate |
| Same 20k for **BookSummary / mark / quotes** | Would hold many polls/ticks in RAM | **No** — custom uses **~1k**, marks **~100**, quotes **~500** |

## Runtime family profile (segment multi-venue)

| Family | Effective rows | Notes |
|--------|----------------|--------|
| book_deltas | max(configured, 20k) capped 50k | Profile C |
| trades | 2 000 | Medium |
| quotes | 500 | Sparse outcomes |
| mark / funding / status | 100 | Very sparse |
| **custom (BookSummary)** | **1 000** | ≈ one poll; append `.part`, not 1 file/s |
| instruments | 50 | Always chunked |

Smoke configs with `flush_rows` / `row_group_rows` **&lt; 200** are left unchanged.

## Segment interval

With `mode = segment`, every `durability.sync_interval_ms` the worker:

1. **Interval-flush** memory → append open `*.parquet.part`
2. **Tick** fsync that part

Seal (e.g. 06:00 UTC) renames the part to a catalog parquet — it is not the first write.

## Profiles (dominant-family TOML when not multi-stream)

| Profile | Use | baseline rows | interval | max_buffer |
|---------|-----|---------------|----------|------------|
| A General live | quotes/trades | 5k | 1s | 32 MiB |
| B Unattended | mixed | 5k | 5s | 64 MiB |
| C Book deltas | L2 only | 20k | 1s | 128 MiB |
| D Sparse / request | poll | 1k | ≥ poll | 16 MiB |
| E Smoke | force flush | 3 | 250ms | 64 KiB |

Watch `/metrics`: `dropped_items`, `active_partitions`, `flush_reasons`,
`catalog_capture_custom_data_request_*`.

Segment seal details — [segment lifecycle](segment_lifecycle.md).
