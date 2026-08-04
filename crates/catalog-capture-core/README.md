# catalog-capture-core

Core **library** for catalog capture: plans, buffers, sinks, layout helpers, and
metadata. No live-node wiring (that lives in `catalog-capture-runtime-adapter` +
`catalog-capture-cli`).

## Stable surface (Track C5)

Prefer these types/functions as the supported integration surface. Other `pub`
items may tighten toward `pub(crate)` later.

### Config & plan

| Item | Role |
|------|------|
| `CaptureConfig` | Catalog URI, flush thresholds, lifecycle, layout |
| `LayoutCompatibility` | Only `RustCanonicalOnly` |
| `CapturePlan` | Declared families to capture |
| `CustomDataCaptureSpec` / `CustomDataRequestCaptureSpec` | Subscribe vs request custom |
| `validate_capture_config` | Config sanity checks |

### Write path

| Item | Role |
|------|------|
| `PartitionKey` / `CaptureItem` | Partition identity for buffers |
| `NautilusCatalogSink` / `ChunkedCatalogSink` | Write complete Parquet chunks via Rust catalog |
| `SegmentCaptureSink` | Segment lifecycle (active `.part` + seal) |
| `BackgroundCaptureRuntime` | Bounded queue + worker flush |

### Layout & run metadata

| Item | Role |
|------|------|
| `catalog_layout::{market_data_dir, custom_data_dir, path_is_under_*}` | Path contract helpers |
| `write_capture_run_record` / `CaptureRunRecord` | `metadata/capture_run.json` |
| `append_option_universe_resolution_records` | Universe lineage JSONL |
| `append_hip4_universe_resolution_records` | HIP-4 lineage JSONL |

### Metrics

| Item | Role |
|------|------|
| `CaptureMetrics` / `CaptureMetricsSnapshot` | Flush/queue metrics + request-job counters |
| `render_prometheus` / `render_json` | Export helpers for the CLI metrics server |

### Option / HIP-4 pure logic

Resolution pure functions under `option_universe` / `hip4` (no HTTP). Live discovery
stays in the CLI.

## Not stable

- Module-private helpers and test-only utilities  
- Exact error strings (may improve without a major version)  
- Anything under `pub use` that is not listed above may still move  

## Tests

```bash
cargo test -p catalog-capture-core --lib
# layout write + Rust readback
cargo test -p catalog-capture-core --lib catalog_layout
```

## Related

- Product binary: `catalog-capture-cli` + `examples/*.toml`  
- How-to: [rust_backtest_from_catalog](../../docs/how_to/rust_backtest_from_catalog.md)  
- Flush policy: [flush_and_rotation](../../docs/concepts/flush_and_rotation.md)  

