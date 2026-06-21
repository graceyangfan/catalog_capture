# CLI reference

Binary: `catalog-capture-cli` (package `catalog-capture-cli`).

## Capture

```bash
cargo run -p catalog-capture-cli -- run --config <path.toml>
```

Post-run option-universe report runs automatically unless skipped via flags documented in
`--help`.

## Config

```bash
cargo run -p catalog-capture-cli -- validate --config <path.toml>
cargo run -p catalog-capture-cli -- print-effective-config --config <path.toml>
```

## Option universe validation

| Subcommand | Purpose |
|------------|---------|
| `inspect-option-universe` | Summarize resolution lineage |
| `validate-option-universe-metadata` | Check `option_universe_resolutions.jsonl` |
| `validate-option-universe-readback` | ParquetDataCatalog readback |
| `validate-option-universe-catalog` | On-disk parquet checks |
| `validate-option-universe` | Unified suite (metadata + readback + catalog) |

Example:

```bash
cargo run -p catalog-capture-cli -- validate-option-universe \
  --config examples/capture.deribit-btc-universe-autorefresh.toml \
  --catalog-uri file:///path/to/catalog \
  --option-universe-format text
```

## Runtime fields (common)

| Field | Description |
|-------|-------------|
| `runtime.capture_seconds` | Duration in seconds; `0` = until shutdown signal |
| `runtime.option_universe_refresh.enabled` | Enable rolling universe refresh |
| `output.catalog_uri` | `file://` URI for catalog root |
| `output.flush_rows` | Rows before throughput flush |
| `output.flush_interval_ms` | Max interval before flush |

See example TOMLs under `examples/` for venue-specific fields.