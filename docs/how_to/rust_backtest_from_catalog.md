# Use a captured catalog in Rust backtest

```text
catalog-capture-cli → file://catalog  →  ParquetDataCatalog / BacktestNode
```

No conversion step. Same layout Nautilus Trader Rust loaders expect.

For catalogs written before the Nautilus v0.65 persistence refactor, run
`nautilus catalog migrate-parquet` into a separate empty destination first. Do not point a
v0.66 reader at a mixed old/new catalog.

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

## Binance Spot SBE L2 + trades

The Spot SBE recorder writes the ordinary Nautilus families; it does not need a
custom-data decoder. Read the same `BTCUSDT.BINANCE` identity from both
families:

```rust
fn load_spot_l2_and_trades(root: &Path, instrument_id: &str) -> anyhow::Result<(usize, usize)> {
    let mut catalog = ParquetDataCatalog::new(root, None, None, None, None);
    let deltas = catalog.order_book_deltas(Some(vec![instrument_id.to_string()]), None, None)?;
    let trades = catalog.trade_ticks(Some(vec![instrument_id.to_string()]), None, None)?;
    ParquetDataCatalog::check_ascending_timestamps(&deltas, "order_book_deltas")?;
    ParquetDataCatalog::check_ascending_timestamps(&trades, "trade_ticks")?;
    Ok((deltas.len(), trades.len()))
}
```

For [`capture.binance-spot-sbe-btc.toml`](../../examples/capture.binance-spot-sbe-btc.toml),
pass `"BTCUSDT.BINANCE"`. Every sealed UTC hour begins with a canonical book
snapshot, so an individual hour can initialize an L2 replay without reading the
preceding hour.

Point `BacktestDataConfig` at the **same** catalog path for a full backtest node
(see Nautilus Trader backtest docs).

## Checklist

- [ ] Parquet under `data/<family>/…` or `data/custom/<Type>/…`
- [ ] `ParquetDataCatalog` returns rows
- [ ] Timestamps ascending
- [ ] No Python legacy mirror dirs required
