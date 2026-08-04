# Catalog layout

## Contract

Only Nautilus Trader **Rust** `ParquetDataCatalog` paths are written.

```toml
[output]
catalog_uri = "file:///path/to/catalog"
# layout_compatibility = "rust_canonical_only"  # default; only accepted value
```

```text
{catalog_uri}/
  data/
    instruments/  quotes/  trades/
    mark_prices/  index_prices/  funding_rate_update/
    option_greeks/  order_book_deltas/  bars/
    custom/{TypeName}/[{identifier}/]…
  metadata/
    capture_run.json
    …
```

Python legacy path mirroring is **not** supported.

## Proof

```bash
cargo test -p catalog-capture-core --lib catalog_layout
```

Load the same URI with Nautilus Trader Rust `ParquetDataCatalog` (see
[Rust backtest from catalog](../how_to/rust_backtest_from_catalog.md)).
