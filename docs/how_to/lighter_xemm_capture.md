# Lighter XEMM Capture

Use [`capture.lighter-xemm-openai-anthropic.toml`](../../examples/capture.lighter-xemm-openai-anthropic.toml)
to record the same perpetual symbols from the Lighter and Robinhood Chain
deployments for cross-venue market making.

## Venue separation

Both deployments use the upstream `nautilus-lighter` adapter. Keep one
`[[venues]]` entry per deployment:

```toml
[[venues]]
id = "lighter_main"
kind = "lighter"
deployment = "lighter"
environment = "mainnet"

[[venues]]
id = "lighter_robinhood_main"
kind = "lighter"
deployment = "robinhood"
environment = "mainnet"
```

The adapter assigns distinct Nautilus venues and therefore distinct IDs, for
example `OPENAI-PERP.LIGHTER` and `OPENAI-PERP.LIGHTER_ROBINHOOD`.

## Recommended data

The XEMM baseline records only native Nautilus data:

| Data | Capture family | Purpose |
|------|----------------|---------|
| L2 snapshot and deltas | `book_deltas` | Reconstruct executable depth and spread |
| Trade ticks | `trades` | Observe fills and aggressor flow |
| Mark price | `mark_prices` | Compare venue mark and execution reference |
| Index price | `index_prices` | Compare oracle/index basis |
| Funding rate | `funding_rates` | Estimate carry and cross-venue funding edge |
| Instrument definition | `instruments` | Preserve precision, contract and venue metadata |

No CustomData is needed for these streams. The Lighter adapter sends the
initial book snapshot followed by deltas, while Nautilus' capture actor also
maintains the hourly book checkpoint.

The upstream Lighter market-stats payload also contains open interest and
daily volume, but it is not currently exposed as a standard Nautilus `Data`
variant. Add a dedicated custom type only when the XEMM signal explicitly
needs those fields; do not duplicate funding, mark, index, trades, or books in
a project-local schema.

## Verify

```bash
make build-release-capture
./bin/catalog-capture-cli validate \
  --config examples/capture.lighter-xemm-openai-anthropic.toml
./bin/catalog-capture-cli run \
  --config examples/capture.lighter-xemm-openai-anthropic.toml
```

After a short live run, read the catalog with the Nautilus
`ParquetDataCatalog`, querying each fully qualified instrument ID separately.
The two venues must never be combined under one instrument ID.
