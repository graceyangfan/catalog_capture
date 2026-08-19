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

Memory flushes into the open `.part` when buffer / family flush limits hit; the parquet
writer packs rows into row groups up to `row_group_rows`. **Sizing is model-driven**
(see `lifecycle/row_group_capacity.rs`), not free-hand:

| Constant | Value | Source |
|----------|------:|--------|
| Hard RG limit | **32 767** | parquet/arrow-rs `i16::MAX` (cloud error: `currently: 32768`) |
| Soft capacity roll | **30 000** | hard − 2 767 headroom (~91.5 %) |
| Cloud BookSummary rate | **~830 rows/s** | ~800–1000 rows/poll × 1 s poll (observed blow-up) |
| Custom memory flush | **1 000** | one poll |
| Custom parquet RG | **50 000** | ≥ min for 10× slack vs soft roll over 24 h @ 830 r/s → ~1 435 RGs/day |

Durability tick only **fsyncs** bytes already on disk — it must **not** finalize a row
group each second (that was the cloud 1 RG/s path: hard fail in ~9.1 h). Wall-clock seal
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
