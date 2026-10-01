# Segment lifecycle

Long-running jobs can append into an active temp file and **seal** on a wall-clock
boundary into Nautilus catalog-readable parquet names.

## Modes

| Mode | Behavior |
|------|----------|
| **`segment` (default)** | Append under stable dirs; seal uses `timestamps_to_filename` |
| `chunked` (opt-in smoke) | Each flush → new catalog parquet |

Implementation:

- `lifecycle/segment_support` — `ActivePart` (open/write/tick/seal), orphan recovery, path helpers  
- `SegmentCaptureSink` — market encode (`EncodeToRecordBatch`)  
- `SegmentCustomDataSink` — custom encode (`prepare_custom_data_batch`)

## What uses segment when `mode = segment`

| Family | Segment (`.part` + seal) | Notes |
|--------|--------------------------|--------|
| quotes, trades, bars, book_deltas, mark/index/funding, greeks, status, closes | **Yes** | Market series |
| **custom data** (subscribe + request, e.g. `DeribitBookSummary`) | **Yes** | `data/custom/{Type}/{id}/…` — no per-second catalog files |
| instruments | **No** (always chunked) | Sparse definitions |

### Defaults (production-oriented)

Omitting `[output.lifecycle]` now means:

- `mode = "segment"`
- `seal.enabled = true`, `schedule = "06:00"`, `timezone = "UTC"`, daily interval

### Chunked = smoke only (not production)

If you **explicitly** set `mode = "chunked"` **and** the plan includes custom data:

- each flush may create a **new catalog parquet** (file explosion under 1s BookSummary polls);
- `validate` / `run` emit a **WARNING** (config remains valid);
- startup logs the same advisory.

Prefer leaving defaults (segment) for real capture.

## Config sketch

```toml
[output.lifecycle]
mode = "segment"

[output.lifecycle.segment]
row_group_rows = 5000

[output.lifecycle.durability]
sync_interval_ms = 1000

[output.lifecycle.seal]
enabled = true
schedule = "06:00"
timezone = "UTC"
interval_secs = 86400
```

## Files (Nautilus layout)

- Active (not catalog-queryable):
  - Market: `data/{type}/{instrument_id}/{open_ts}.parquet.part`
  - Custom: `data/custom/{TypeName}/{identifier}/{open_ts}.parquet.part`
- Sealed: `{start}_{end}.parquet` via `timestamps_to_filename` (same clock for market + custom)

Memory flushes into the active Arrow writer when buffer / family flush limits hit; the
writer packs rows into row groups up to `row_group_rows`. Until a row group is full or
the segment is sealed, the corresponding `.part` can remain at 0 bytes because those
rows are still inside ArrowWriter. **Sizing is model-driven** (see
`lifecycle/row_group_capacity.rs`), not free-hand:

| Constant | Value | Source |
|----------|------:|--------|
| Hard RG limit | **32 767** | parquet/arrow-rs `i16::MAX` (cloud error: `currently: 32768`) |
| Soft capacity roll | **30 000** | hard − 2 767 headroom (~91.5 %) |
| Cloud BookSummary rate | **~830 rows/s** | ~800–1000 rows/poll × 1 s poll (observed blow-up) |
| Custom memory flush | **1 000** | one poll |
| Custom parquet RG | **50 000** | ≥ min for 10× slack vs soft roll over 24 h @ 830 r/s → ~1 435 RGs/day |

Durability tick only **fsyncs** bytes already emitted to disk; it cannot make an
unfinished in-memory row group crash-durable. It must **not** finalize a row group each
second (that was the cloud 1 RG/s path: hard fail in ~9.1 h). Wall-clock seal
(e.g. 06:00 UTC) closes the day file and opens the next part. Soft capacity roll seals
and reopens near 30 k RGs if misconfig/rate ever approaches the hard cap (same seal path
as day roll; same UTC day may contain multiple catalog parquets).

Keep a single stable `catalog_uri` for the job. Examples:

- `examples/capture.hyperliquid-perp-daily.toml` — perp day files at 06:00 UTC  
- `examples/capture.hyperliquid-hip4-btc-daily.toml` — **HIP-4 daily** instrument
  refresh + **same 06:00 UTC seal** as contract day boundary  
- `examples/capture.multi-venue-mainnet.toml` — HL + Binance L2 + Deribit BookSummary day segments  
- `examples/operator/*-unattended.toml` — long option-universe runs  

HIP-4: universe poll (which YES/NO) is separate from seal (file day). See
[HIP-4 capture](../how_to/hip4_capture.md).

## Order book checkpoints

When `book_deltas` is enabled, the capture actor writes the live canonical
`OrderBookDeltas` stream and uses Nautilus' managed `OrderBook` cache to create
one boundary checkpoint with `OrderBook::to_deltas`. There is no second periodic
book-snapshot subscription. At each segment boundary the actor:

1. drains and seals the old segment;
2. submits one complete snapshot batch to the book runtime;
3. continues with live delta batches in FIFO order.

The snapshot is the first book batch of the new segment, not an extra exchange
REST request. The queue transports the snapshot as one `OrderBookDeltas` item;
the segment sink expands it in bounded chunks into the standard
`order_book_deltas` Parquet rows. This preserves `F_SNAPSHOT`/`F_LAST` while
avoiding one queue item and one copy per snapshot delta. An empty or
never-updated book is skipped because it cannot provide a valid state snapshot.
The segment sink also records the snapshot source-sequence high-water per
instrument. A later ordinary batch at or below that high-water is already
represented by the snapshot and is discarded; an unsequenced batch (`sequence =
0`) is retained. This mirrors Nautilus adapter recovery without reimplementing
venue-specific REST recovery.

The sink keeps each venue message atomic in time. If the local `ts_init` clock
rolls back at the handoff, it advances only the persisted `ts_init` watermark;
`ts_event`, source sequence, actions, and record flags are unchanged. This keeps
ParquetCatalog's timestamp ordering consistent with the FIFO snapshot/delta
order while preserving the source book semantics. Actual source sequence gaps
remain the adapter's responsibility and must not be hidden by this timestamp
normalization.

Example for directly replayable hourly segments:

```toml
[output.lifecycle]
mode = "segment"

[output.lifecycle.seal]
enabled = true
schedule = "00:00"
timezone = "UTC"
interval_secs = 3600
```
