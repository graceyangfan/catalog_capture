# Quickstart

After [installation](installation.md), from the **repository root**:

```bash
make bootstrap-deps
make build-release-capture

./target/release/catalog-capture-cli validate \
  --config examples/capture.multi-venue-mainnet.toml
```

Offline layout proof (no network):

```bash
cargo test -p catalog-capture-core --lib catalog_layout
```

## First live run (mainnet)

Public market data; no API keys required.

```bash
# Short smoke (~2 minutes)
CAPTURE_SECONDS=120 ./scripts/run-mainnet-capture.sh

# Or long-running (until Ctrl+C / SIGTERM)
./scripts/run-mainnet-capture.sh
# default: examples/capture.multi-venue-mainnet.toml
```

Data under `./data/` (gitignored). Layout:
[catalog layout](../concepts/catalog_layout.md).

## Other starters

| Goal | Config |
|------|--------|
| Multi-venue mainnet | `examples/capture.multi-venue-mainnet.toml` |
| HL universe only | `examples/capture.hyperliquid-hip4-btc-daily.toml` |
| Deribit BookSummary | `examples/capture.deribit-btc-book-summary.toml` |

See [examples/README.md](../../examples/README.md).

## Next

- [Cloud capture](../how_to/cloud_capture.md)
- [Rust backtest from catalog](../how_to/rust_backtest_from_catalog.md)
- [Unattended capture](../how_to/unattended_capture.md)
