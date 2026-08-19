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

| Family | Memory flush | Parquet row group | Notes |
|--------|--------------|-------------------|--------|
| book_deltas | max(configured, 20k) capped 50k | same | Profile C |
| trades | 2 000 | same | Medium |
| quotes | 500 | same | Sparse outcomes |
| mark / funding / status | 100 | same | Very sparse |
| **custom (BookSummary)** | **1 000** | **50 000** | Poll often; many polls per RG (≪ 32k RG/file/day) |
| instruments | 50 | n/a | Always chunked |

Smoke configs with `flush_rows` / `row_group_rows` **&lt; 200** are left unchanged (both knobs stay tiny).

## Segment interval

With `mode = segment`, every `durability.sync_interval_ms` the worker:

1. **Interval-flush** memory → append open `*.parquet.part`
2. **Tick** fsync that part only (does **not** call `ArrowWriter::flush` / seal a row group)

Seal (e.g. 06:00 UTC) renames the part to a catalog parquet — it is not the first write.
If an open part approaches **30 000** flushed row groups (soft cap =
`i16::MAX − 2767`), the sink seals and reopens mid-day so parquet’s **32 767** hard
limit cannot abort the job. Derivation and cloud-rate checks live in
`row_group_capacity` unit tests.

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
