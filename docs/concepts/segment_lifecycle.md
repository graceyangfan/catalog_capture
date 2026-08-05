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

Memory flushes into the open `.part` when `row_group_rows` / buffer limits hit; durability
tick only **fsyncs** the part. Wall-clock seal (e.g. 06:00 UTC) closes the day file and
opens the next part — it is **not** “hold all day in RAM then write once”.

Keep a single stable `catalog_uri` for the job. Examples:

- `examples/capture.hyperliquid-perp-daily.toml` — perp day files at 06:00 UTC  
- `examples/capture.hyperliquid-hip4-btc-daily.toml` — **HIP-4 daily** instrument
  refresh + **same 06:00 UTC seal** as contract day boundary  
- `examples/capture.multi-venue-mainnet.toml` — HL + Binance L2 + Deribit BookSummary day segments  
- `examples/operator/*-unattended.toml` — long option-universe runs  

HIP-4: universe poll (which YES/NO) is separate from seal (file day). See
[HIP-4 capture](../how_to/hip4_capture.md).
