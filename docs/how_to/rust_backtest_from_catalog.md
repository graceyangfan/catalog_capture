# Use a captured catalog in Nautilus Trader Rust backtest

## Goal

Prove the product loop:

```text
catalog-capture-cli (TOML config) → Rust Parquet catalog
                                 → ParquetDataCatalog / BacktestNode load
```

No Feather conversion. No second layout. Same pattern used by research backtests
(e.g. `cjp_mm_rs` catalog helpers: `ParquetDataCatalog::new` + query / `BacktestDataConfig`).

## Product shape (Nautilus-like)

| Piece | Role |
|-------|------|
| **One binary** | `catalog-capture-cli` |
| **Configs** | `examples/*.toml` only — not cargo example crates |
| **Catalog** | `rust_canonical_only` under `file://…` |

```bash
cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-dvol.toml
```

## Layout contract

```toml
[output]
catalog_uri = "file:///path/to/catalog"
compression = "snappy"
# layout_compatibility = "rust_canonical_only"  # default; only accepted value
```

```text
{catalog_uri}/
  data/
    instruments/…
    quotes/…
    trades/…
    mark_prices/…  index_prices/…  funding_rate_update/…
    option_greeks/…  order_book_deltas/…  bars/…
    custom/{TypeName}/[{identifier}/]…   # subscribe + request custom
  metadata/
    capture_run.json
    …
```

## Offline proof (this repo)

Write with the capture sink, read with Nautilus catalog APIs (no network):

```bash
cargo test -p catalog-capture-core --lib catalog_layout
```

Key test: `write_quotes_then_query_with_parquet_data_catalog` — capture write then
`ParquetDataCatalog::quote_ticks(...)`.

## Load pattern (research / backtest)

Same idea as `cjp_mm_rs` `catalog.rs`:

```rust
use nautilus_persistence::backend::catalog::ParquetDataCatalog;
use std::path::Path;

fn load_quotes(root: &Path, instrument_id: &str) -> anyhow::Result<usize> {
    let mut catalog = ParquetDataCatalog::new(root, None, None, None, None);
    let rows = catalog.quote_ticks(Some(vec![instrument_id.to_string()]), None, None)?;
    ParquetDataCatalog::check_ascending_timestamps(&rows, "quotes")?;
    Ok(rows.len())
}
```

For a full backtest node, point `BacktestDataConfig` / `BacktestRunConfig` at the
**same** catalog path (see Nautilus backtest docs and projects like `cjp_mm_rs`
`run_config` / `data_config` builders). Capture does not embed a backtest runner;
it only guarantees the on-disk contract those loaders expect.

## Workflow

1. Capture with CLI + TOML → `catalog_uri`.
2. Open the same path with `ParquetDataCatalog` (or backtest data config).
3. Query instruments / quotes / trades / custom as needed.
4. Optional: Python probes only if they read the **same Rust paths**.

## Validation checklist

- [ ] Parquet under `data/<family>/…` (or `data/custom/<Type>/…`)
- [ ] Rust `ParquetDataCatalog` returns rows for at least one family
- [ ] Timestamps ascending
- [ ] No dependency on Python legacy mirror dirs

## Related

- [examples/README.md](../../examples/README.md) — configs + single CLI only  
- [custom-data-contract.md](../custom-data-contract.md) — custom path table  
- [refactor-optimization-plan.md](../refactor-optimization-plan.md) — Track L  
