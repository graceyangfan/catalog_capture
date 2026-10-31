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
| book_depth | 512 | same | Full snapshots; many levels per row |
| trades | 2 000 | same | Medium |
| quotes | 500 | same | Sparse outcomes |
| mark / funding / status | 100 | same | Very sparse |
| **custom (BookSummary)** | **1 000** | **50 000** | Poll often; many polls per RG (≪ 32k RG/file/day) |
| instruments | 50 | n/a | Always chunked |

Smoke configs with `flush_rows` / `row_group_rows` **&lt; 200** are left unchanged (both knobs stay tiny).

## Segment interval

With `mode = segment`, the worker uses independent absolute deadlines (a
continuously busy family queue cannot starve either timer):

1. Every `flush_interval_ms`, **interval-flush** family buffers to the active
   Arrow parquet writer; row and byte thresholds may flush sooner.
2. Every positive `durability.sync_interval_ms`, **tick** fsync bytes the writer
   has already emitted (does **not** call `ArrowWriter::flush` / seal a row group).
   Zero disables the periodic sync tick, without disabling interval flush.

Changing the fsync interval does not change the memory-flush deadline. Timer
execution still waits for the current worker batch to finish; these intervals
are scheduling targets, not hard latency bounds.

Arrow may retain an incomplete row group in memory, so a sparse stream's active
`.parquet.part` can exist at 0 bytes even while rows are being accepted. The part is
not catalog-queryable. A full row group writes earlier; seal (or graceful shutdown)
closes the final row group and renames the part to a catalog parquet.

The Predict profiles use two-second flush and ten-second periodic fsync. This
reduces periodic sync passes fivefold relative to two-second fsync without changing
buffer flush or market cadence. It is a provisional throughput choice; a single
long sync can still exhaust a finite queue. Seal always syncs the completed file.

An fsync is not a Parquet commit: the current recovery reader also requires the
file footer written by close. An abrupt exit can leave an entire active part
unreadable, including its already-written row groups. Thus the fsync interval is
not a bound on crash data loss. See the
[durability and replay audit](../developer_guide/realtime-adapters.md#durability-and-resource-interpretation).

If an open part approaches **30 000** flushed row groups (soft cap =
`i16::MAX − 2767`), the sink seals and reopens mid-day so parquet’s **32 767** hard
limit cannot abort the job. Derivation and cloud-rate checks live in
`row_group_capacity` unit tests.

## Rotation policy

Use **daily UTC 06:00** sealing for general, option-universe, custom-data, and
multi-venue strategy captures. It keeps file counts and boundary work small
while keeping active writer memory bounded. An abrupt process or host failure can
make the active, unclosed part unrecoverable by the current reader; graceful
shutdown and a supervisor do not replace a defined crash-durability contract.

Use **hourly sealing only for high-rate L2 order-book profiles** when each hour
must be independently replayable. Such a profile retains one marked managed-book initialization prefix at each
hour boundary in the standard `book_deltas` file. The window-aware reader skips
intermediate prefixes during continuous replay. Do not make
hourly rotation a global default: it adds 24 seal events and snapshot batches
per instrument per day without improving sparse quotes, trades, Greeks, or
custom-data capture.

The reference profiles follow this split: `capture.multi-venue-mainnet.toml`
and the option-universe/operator profiles use daily UTC 06:00 defaults, while
`capture.binance-lighter-btc-sol-perp-books.toml` is the explicit hourly L2
profile.

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

For backlog diagnosis, also inspect per-family `oldest_queued_seconds`,
`in_flight_items`, `in_flight_bytes`, `worker_busy_seconds`, `active_writers` and
`operation_seconds_{count,sum,max}` (operations: `queue_wait`, `sink_write`,
`sink_sync`, `sink_seal`). Queue draining does not wait for file rotation. Partition
flush passes data to Arrow; incomplete row groups can still occupy encoder memory.
An hourly seal is not an hourly memory flush, and shortening seals is not a fix
for input bursts. See the [pipeline observations](../developer_guide/realtime-adapters.md#pipeline-observations-and-partition-failures)
for timing and bounded fault-history semantics.

## Self-contained hourly book segments

For a book file that can be replayed without the preceding hour, configure
`seal.interval_secs = 3600`. The actor seals the old part before submitting one
complete managed-book initialization batch to the new part. Its prefix row count
is recorded in Parquet schema metadata; the schema remains standard Native
`order_book_deltas`. One grouped message is admitted atomically. Use the
window-aware reader to skip intermediate prefixes during full replay or initialize
from the first retained prefix. Ordinary catalog concatenation ignores these
metadata and does not implement the same callback tape. Known gaps always require
separate fresh-engine backtests.

Segment seal details — [segment lifecycle](segment_lifecycle.md).

The family ingress queue enforces its resolved `max_bytes` limit on estimated
payload bytes and its resolved `capacity` limit on messages. Configure them through
`output.queues.<family>`; omitted fields inherit `queue_capacity` and
`queue_max_bytes`. Without an explicit queue byte limit, the legacy fallback is
`max_total_buffer_bytes`. This is separate from the aggregate partition-buffer
cap. A worker can hold one drained queue budget while the
producer fills the next queue. Parquet's native `set_max_row_group_bytes` also uses
`max_buffer_bytes` as its estimated encoded row-group target, alongside the family
row-count limit. Encoder overhead and Native engine queues are outside these caps. Monitor `catalog_capture_queued_bytes` and process RSS as well
as row-buffer metrics.
