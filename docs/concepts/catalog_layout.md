# Catalog layout

Aligned with Nautilus Trader Rust `ParquetDataCatalog` only — no alternate layout.

## Official tree

```text
{catalog_root}/
  data/
    instruments/{instrument_id}/{start}_{end}.parquet
    quotes/{instrument_id}/{start}_{end}.parquet
    trades/{instrument_id}/{start}_{end}.parquet
    order_book_deltas/{instrument_id}/{start}_{end}.parquet
    mark_prices/{instrument_id}/{start}_{end}.parquet
    index_prices/…  funding_rate_update/…  bars/…  option_greeks/…
    custom/{TypeName}/[{identifier}/]/{start}_{end}.parquet
```

- `{start}_{end}` = Nautilus `timestamps_to_filename` (ISO-8601 filesystem-safe).
- `{instrument_id}` = `urisafe_instrument_id` (e.g. `BTCUSDT-PERP.BINANCE`).
- Type folder names come from `CatalogPathPrefix` on the model type
  (`quotes`, `trades`, `order_book_deltas`, `mark_prices`, …).

## How this project writes

| Mode | Writer | Final files |
|------|--------|-------------|
| **chunked** | `write_to_parquet` / `write_instruments` / `write_custom_data_batch` | Official paths only |
| **segment** (market + custom when `mode=segment`) | Append `*.parquet.part`, seal via `timestamps_to_filename` | Active part not queryable; sealed files match catalog names |
| **instruments** | Always chunked | Sparse defs; no day segment |
| **segment** (optional lifecycle) | Append under same `make_path` dirs; seal renames with `timestamps_to_filename` | Same final layout; active temp is `*.parquet.part` (not queried) |

Config:

```toml
[output]
catalog_uri = "file:///path/to/catalog"
layout_compatibility = "rust_canonical_only"  # only accepted value
```

## Not part of the catalog data contract

Operator-only files (optional) under `{catalog_root}/metadata/` — lineage / run
records. Backtest loaders use `ParquetDataCatalog` on `data/` only.

## Verify

```bash
cargo test -p catalog-capture-core --lib catalog_layout
# roundtrip: write quotes → ParquetDataCatalog::quote_ticks
```
