# Use a captured catalog in Nautilus Trader Rust backtest

## Goal

Load assets written by **catalog-capture** with **Nautilus Trader Rust**
`ParquetDataCatalog` / backtest paths **without** Feather conversion.

Capture writes **only** the Nautilus Trader **Rust canonical** catalog layout.
Python legacy directory mirrors are **not** supported.

## Layout contract

### TOML

```toml
[output]
catalog_uri = "file:///path/to/catalog"
compression = "snappy"
layout_compatibility = "rust_canonical_only"   # default; only accepted value
```

Omitting `layout_compatibility` is equivalent. Any other value (including the
former `rust_canonical_with_python_legacy_mirror`) is rejected at config validate.

### Directory shape (Rust canonical)

```text
{catalog_uri}/
  data/
    instruments/...
    quotes/...
    trades/...
    mark_prices/...
    index_prices/...
    funding_rate_update/...
    option_greeks/...
    order_book_deltas/...
    bars/...
    custom_*/...          # adapter custom types (Nautilus Rust naming)
  metadata/               # capture extensions (universe resolution, etc.)
```

Exact family prefixes follow Nautilus Trader Rust `ParquetDataCatalog` /
`CatalogPathPrefix` for the pinned `nautilus_trader` revision.

## Workflow

1. Run capture (default layout is Rust-only).
2. Point Rust backtest / research code at the same `catalog_uri`.
3. Load instruments and market data through Nautilus Rust catalog APIs.
4. Optional: Python probes in this repo may still be used as smoke tools if your
   environment can read the **same Rust paths** — they are not a second layout.

## Validation checklist

- [ ] Parquet files exist under `data/<rust_family>/...`
- [ ] Rust `ParquetDataCatalog` queries at least one family (e.g. quotes)
- [ ] Backtest or catalog example runs without conversion steps
- [ ] No dependency on `quote_tick` / `trade_tick` legacy mirror directories

## Related

- [Refactor and optimization plan — Track L](../refactor-optimization-plan.md)
- [Architecture](../architecture.md)
- [Smoke and soak](smoke_and_soak.md)
