# Segment Lifecycle (Track S)

## Purpose

Long-running capture jobs (perpetual futures, HIP-4 daily, unattended daemons) need:

- **continuous append** into the current recording segment
- **scheduled seal** at a wall-clock boundary (e.g. 06:00 UTC daily)
- **sealed catalog parquet** that can be copied and loaded directly for backtest via `ParquetDataCatalog`

This is **independent** from universe refresh (HIP-4 `outcomeMeta`, option universe). Those change *what* is subscribed; segment lifecycle changes *how output files are produced*.

## Concepts

| Term | Meaning |
|------|---------|
| **Chunked** (Phase 1) | Each flush writes a new immutable parquet chunk |
| **Segment** (Track S) | One open `.part.parquet` per partition; row groups appended until seal |
| **Batch** | Micro: memory buffer → append row group (same file) |
| **Sync** | Durability: `fsync` active file (same file) |
| **Seal** | Macro: close + rename to `{min_ts}_{max_ts}.parquet` (new file for next period) |

## Architecture

```
CatalogCaptureActor
  └─ BackgroundCaptureRuntime (per family)
       └─ CaptureRuntime
            └─ CatalogSink
                 ├─ Chunked → NautilusCatalogSink (default)
                 └─ Segment → SegmentCaptureSink
```

Timers:

- `SEGMENT_SEAL` — wall-clock seal (orthogonal to `HIP4_UNIVERSE_REFRESH`)
- Worker interval — sync (segment) or interval flush (chunked)

## Configuration

```toml
[output]
catalog_uri = "file:///data/hl-btc-perp-capture"
flush_rows = 5000
flush_interval_ms = 1000

[output.lifecycle]
mode = "segment"   # default: "chunked"

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

## File layout

- **Active**: `data/{family}/{instrument_id}/{open_ts}.part.parquet` (ignored by catalog query)
- **Sealed**: `data/{family}/{instrument_id}/{start}_{end}.parquet` (catalog-readable)

Single stable `catalog_uri` for the whole job. Days are distinguished by sealed filename time range, not by changing the catalog root.

## Backtest workflow

1. Run capture unattended (`capture_seconds = 0`).
2. At each seal boundary, new sealed parquet files appear under `data/`.
3. Copy sealed files (or catalog subtree) to research storage.
4. Load with PyO3 `ParquetDataCatalog` — no conversion step.

## Implementation roadmap (Track S)

| Milestone | Deliverable | Status |
|-----------|-------------|--------|
| S0 | `LifecycleConfig` + TOML parsing | done |
| S1 | `SegmentCaptureSink` + unit tests | done |
| S2 | `CatalogSink` enum; chunked default unchanged | done |
| S3 | Runtime/background tick + seal dispatch | done |
| S4 | Actor `SEGMENT_SEAL` timer + shutdown seal | done |
| S5 | Recovery for orphan `.part` + metrics | done |
| S6 | Production example + readback validation | done |

### Examples

- `examples/capture.hyperliquid-perp-daily.toml` — perpetual futures with daily 06:00 UTC seal
- `crates/catalog-capture-runtime-adapter/examples/segment_quote_roundtrip.rs` — segment seal + catalog readback

```bash
# Segment capture via product CLI (single binary)
cargo run -p catalog-capture-cli -- run --config examples/capture.hyperliquid-perp-seal-quick.toml
cargo run -p catalog-capture-cli -- run --config examples/capture.hyperliquid-perp-daily.toml
python tests/probe_segment_seal_readback.py /path/to/catalog BTC-USD-PERP.HYPERLIQUID
```

Seal boundaries are computed from `schedule`, `timezone`, and `interval_secs` only. There is no
relative “seal in N minutes” override — production jobs rely on the configured wall-clock rotation.

## Related docs

- [flush-rotation-policy.md](flush-rotation-policy.md) — Phase 1 chunking; Track S supersedes Phase 2 active-writer section
- [production-architecture.md](production-architecture.md) — layered policies
