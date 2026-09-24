# CLI reference

Binary: `catalog-capture-cli`.

## Capture

```bash
cargo run -p catalog-capture-cli -- run --config <path.toml>
```

## Config

```bash
cargo run -p catalog-capture-cli -- validate --config <path.toml>
cargo run -p catalog-capture-cli -- print-effective-config --config <path.toml>
```

## Option universe

| Subcommand | Purpose |
|------------|---------|
| `inspect-option-universe` | Summarize resolution lineage |
| `validate-option-universe-metadata` | Check resolution metadata |
| `validate-option-universe-readback` | Catalog readback |
| `validate-option-universe-catalog` | On-disk parquet checks |
| `validate-option-universe` | Full suite |

```bash
cargo run -p catalog-capture-cli -- validate-option-universe \
  --config examples/capture.deribit-btc-universe-autorefresh.toml \
  --option-universe-format text
```

Use `--help` on any subcommand for flags.

## Order-book replay validation

Validate that canonical order-book Parquet data can be rebuilt by Nautilus'
Rust `OrderBook` implementation. The default also requires every sealed
segment to contain an `F_SNAPSHOT` reset. The catalog query is timestamp
ordered, so delayed rows may appear before that reset even though the writer
submitted the reset first.

```bash
./bin/catalog-capture-cli validate-order-book \
  --catalog-uri file://./data/binance-lighter-btc-sol-perp-books \
  --instrument-id BTCUSDT-PERP.BINANCE \
  --instrument-id SOLUSDT-PERP.BINANCE \
  --instrument-id SOL-PERP.LIGHTER \
  --format text
```

Use `--allow-missing-segment-snapshots` only for older chunked catalogs that
were not written as independently replayable segments.
