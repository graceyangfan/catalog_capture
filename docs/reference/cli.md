# CLI reference

Binary: `catalog-capture-cli`.

## Capture

```bash
./scripts/cargo.sh run -p catalog-capture-cli -- run --config <path.toml>
```

Useful preflight flags:

| Flag | Purpose |
|------|---------|
| `--dry-run-resolve` | Resolve dynamic option/HIP-4 universes, print the report, then exit |
| `--print-option-universe` | Print resolved universe reports before continuing capture |
| `--skip-post-run-report` | Skip automatic option-universe inspection after a bounded run |

## Config

```bash
./scripts/cargo.sh run -p catalog-capture-cli -- validate --config <path.toml>
./scripts/cargo.sh run -p catalog-capture-cli -- print-effective-config --config <path.toml>
```

Capture families include native `[[capture.book_depth]]` (instrument ID, L2 book
type and optional depth) and `[[capture.trades]]`. Rolling Predict profiles select
these through `record_depth` and `record_trades` metadata; `record_snapshot="false"`
suppresses custom snapshot files. See [Predict capture](../how_to/predict-market-data.md).

Prophet profiles use `kind = "prophet"` and `id = "PROPHET"`. Select
`<marketId>-0.PROPHET` in `[[capture.book_depth]]` with `book_type = "L2_MBP"`
and omit the depth limit. Use `ProphetLastTradePrice` in
`[[capture.custom_data]]` with the decimal market ID as its identifier.
See [Prophet capture](../how_to/prophet-capture.md).

Matched sports profiles configure `[capture.matched_sports]` instead of fixed
instrument IDs. See [matched sports capture](../how_to/matched-sports-capture.md)
for matching, bilateral activity selection and event retirement. `validate` checks
configuration only; it does not discover a live paired universe.

## Option universe

| Subcommand | Purpose |
|------------|---------|
| `resolve-option-universe` | Resolve live venue metadata without starting capture |
| `inspect-option-universe` | Summarize resolution lineage |
| `validate-option-universe-metadata` | Check resolution metadata |
| `validate-option-universe-readback` | Catalog readback |
| `validate-option-universe-catalog` | On-disk parquet checks |
| `validate-option-universe` | Full suite |

```bash
./scripts/cargo.sh run -p catalog-capture-cli -- validate-option-universe \
  --catalog-uri file://./data/deribit-btc-universe-autorefresh \
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
