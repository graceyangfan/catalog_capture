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
  - Market: `data/{type}/{instrument_id}/{open_ts}-{uuid}.parquet.part`
  - Custom: `data/custom/{TypeName}/{identifier}/{open_ts}-{uuid}.parquet.part`
- Sealed: `{start}_{end}_{identity}.parquet` using Native timestamps and an allowed
  alphanumeric identity suffix. Equal-time file ranges cannot overwrite one another.

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
unfinished in-memory row group crash-durable. Even completed row groups need the
file footer before the current recovery reader can read the part, so the sync
interval does not bound crash data loss. It must **not** finalize a row group each
second (that was the cloud 1 RG/s path: hard fail in ~9.1 h). Wall-clock seal
(e.g. 06:00 UTC) closes the day file; the next event opens the next part lazily. This
prevents retired or sparse partitions from accumulating empty `.part` files. Soft capacity
roll seals and reopens near 30 k RGs if misconfig/rate ever approaches the hard cap (same
UTC day may contain multiple catalog parquets).

Keep a single stable `catalog_uri` for the job. Examples:

- `examples/capture.hyperliquid-perp-daily.toml` — perp day files at 06:00 UTC
- `examples/capture.hyperliquid-hip4-btc-daily.toml` — **HIP-4 daily** instrument
  refresh + **same 06:00 UTC seal** as contract day boundary
- `examples/capture.multi-venue-mainnet.toml` — HL + Binance L2 + Deribit BookSummary day segments
- `examples/operator/*-unattended.toml` — long option-universe runs

HIP-4: universe poll (which YES/NO) is separate from seal (file day). See
[HIP-4 capture](../how_to/hip4_capture.md).

## Order book initialization and replay windows

Source `OrderBookDeltas` use the standard Native `order_book_deltas` schema.
Source fields, sequence numbers and publication order are preserved.
The recorder does not filter source events covered by a cached snapshot or advance
source `ts_init`. Buffered venue recovery can publish an acquired snapshot before
older received diffs; this time decrease is not itself a capture gap. The raw
window reader retains it. Native backtest input still requires an explicit
initialization/research start when source creation times are not monotonic.

For hourly L2 rotation, the actor enqueues the seal, then one complete Native
managed-book snapshot from `OrderBook::to_deltas`, then later source frames.
No second venue snapshot subscription or REST request is made. The worker writes
that snapshot as the next physical file's **marked initialization prefix**, counted
by `capture_initialization_rows`. It is not published as a live strategy event.
A never-updated book is skipped; a valid updated empty book is an initialization.

Retaining this prefix makes later hours usable after the original baseline file
is removed by daily cleanup. The window-aware reader skips every intermediate
prefix during continuous replay. Starting from a retained hour uses its prefix
only for initialization, with a source-sequence/event anchor when cache state is
ahead of the first queued updates. Initialization may require warmup; it does not
represent missing market events. Unanchored L2 windows fail the caller's
`has_book_baseline()` admission check.

Every standard market segment includes `capture_window_id` and
`capture_segment_index`. Hourly file rotation keeps one logical window. A known
admission/write gap, source L2 reinitialization, retirement or process restart
starts another. Missing middle file ordinals also split replay. Thus
`x..A + gap + C..y` is **two backtests**, each with a fresh Native engine, strategy,
account and book. Initializing at C cannot repair the missing events after A.

Use `read_book_replay_windows` to decode through Native codecs and reconstruct
`F_LAST` frames before BacktestEngine sees them. Use `read_replay_windows` for
TradeTick and full Depth, and align all required feeds within a common healthy
range. Ordinary Native catalog queries still read these files but ignore the
window/prefix metadata; raw concatenation does not implement this replay contract.
Legacy files lack certified boundaries and are rejected by this reader.

Seal closes the footer, syncs the completed file, publishes without overwriting,
then syncs the directory on Unix. The Native filename suffix is derived from the
part identity, allowing retry after publication but before removal of the part.
This does not recover a part interrupted before its footer was written.

Hourly schedule:

```toml
[output.lifecycle]
mode = "segment"

[output.lifecycle.seal]
enabled = true
schedule = "00:00"
timezone = "UTC"
interval_secs = 3600
```

See the [replay entry and evidence](../developer_guide/realtime-adapters.md#faithful-replay-across-file-boundaries).
