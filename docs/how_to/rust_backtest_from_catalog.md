# Use a captured catalog in Rust backtest

```text
catalog-capture-cli → file://catalog  →  ParquetDataCatalog / BacktestNode
```

No conversion step. Same layout Nautilus Trader Rust loaders expect.

## Capture

```bash
cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-dvol.toml
```

Layout: [catalog layout](../concepts/catalog_layout.md).

## Offline proof (this repo)

```bash
cargo test -p catalog-capture-core --lib catalog_layout
```

## Load pattern

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

Point `BacktestDataConfig` at the **same** catalog path for a full backtest node
(see Nautilus Trader backtest docs).

## Checklist

- [ ] Parquet under `data/<family>/…` or `data/custom/<Type>/…`
- [ ] `ParquetDataCatalog` returns rows
- [ ] Timestamps ascending
- [ ] No Python legacy mirror dirs required
