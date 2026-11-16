# Changelog

## Unreleased

### Compatibility

- Require Rust 1.100.0 and the quantity-commission fork pinned in
  [installation](docs/getting_started/installation.md).
- Use official develop for Hyperliquid instrument-request completion, partial
  metadata refreshes and Binance book replay timestamp ordering.
- Remove Binance Prophet (`nautilus-binance-prediction`, `venue-prophet`),
  including its configuration, custom data type and examples.
- Require explicit HIP-4 rolling stream selectors with `hip4_rolling = true`.
- Import Predict rules from `nautilus_predict::common`, data clients from
  `nautilus_predict::data`, HTTP models from `nautilus_predict::http::models`,
  and use `PredictEnvironment` for signer/transport configuration.

### Capture behavior

- Preserve source observations through bounded recording queues, independent
  flush/sync deadlines and sealed Native Parquet segments.
- Record continuity gaps and managed-book initialization prefixes for independent
  replay windows. Release expired market state after its last owner retires.
- Resolve Predict rolling windows using UTC category epochs and New York clock
  rules, with bounded successor discovery.

## 0.1.0

Single Rust CLI with TOML configuration, optional venue adapters, Native
`ParquetDataCatalog` output, custom subscriptions/requests, capture metadata,
metrics and unattended service tooling. Python legacy catalog mirroring and
additional product binaries are unsupported. See [documentation](docs/index.md)
for current operation and [Git history](https://github.com/graceyangfan/catalog_capture/commits/main/)
for implementation changes.
