# Catalog Capture

Independent, community-maintained capture tool
([catalog_capture](https://github.com/graceyangfan/catalog_capture)) that writes live
market data into [Nautilus Trader](https://github.com/nautechsystems/nautilus_trader)
**Rust `ParquetDataCatalog`** layouts for direct backtest use.

Not affiliated with or endorsed by Nautech Systems Pty Ltd.
See [TRADEMARK.md](TRADEMARK.md) and [NOTICE](NOTICE).

```text
venues → catalog-capture-cli + TOML
      → {catalog}/data/{type}/{instrument_id}/…parquet
      → ParquetDataCatalog / Rust backtest
```

## Features

- **One product binary** — `catalog-capture-cli` (TOML configs only)
- **Rust catalog layout only** — `rust_canonical_only` (Nautilus `ParquetDataCatalog`)
- **Venues** — Binance Futures, Deribit, Bybit, OKX, Hyperliquid (`venue-*` features)
- **Universe refresh** — e.g. HIP-4 style outcome roll (unsub old / sub new)
- **Mainnet-oriented examples** — public data by default (no keys in TOML)

## Quick start

Requires Rust **1.97.1** and sibling `../nautilus_trader`. Run from the **repo root**.

```bash
make bootstrap-deps

# Cloud / multi-venue capture: only link venues you need (smaller, faster)
make build-release-capture
# features: venue-binance,venue-deribit,venue-hyperliquid

./target/release/catalog-capture-cli validate \
  --config examples/capture.multi-venue-mainnet.toml

# Long-running mainnet capture (logs under ./logs/)
./scripts/run-mainnet-capture.sh
# default config: examples/capture.multi-venue-mainnet.toml
```

Short smoke:

```bash
CAPTURE_SECONDS=120 ./scripts/run-mainnet-capture.sh
```

Data: `./data/…` (gitignored). Catalog layout: [docs/concepts/catalog_layout.md](docs/concepts/catalog_layout.md).

## Recommended configs

| Config | Use |
|--------|-----|
| **`examples/capture.multi-venue-mainnet.toml`** | HL rolling universe + Binance L2 d20 + Deribit BookSummary |
| `examples/capture.hyperliquid-hip4-btc-daily.toml` | Hyperliquid universe only |
| `examples/capture.deribit-btc-book-summary.toml` | Deribit BookSummary only (1s poll) |

More: [examples/README.md](examples/README.md).

## Build options

| Command | When |
|---------|------|
| `make build-release-capture` | **Preferred** for multi-venue mainnet capture |
| `make build-release` | All venues (`all-venues`) |
| `make build-slim FEATURES=venue-deribit` | Single-venue debug |
| `make build-release-small` | Smaller binary, slower compile |
| `make clean` / `make clean-all-targets` | Free disk (`clean-all` also clears `../nautilus_trader/target`) |

Build size notes: [docs/how_to/build_size.md](docs/how_to/build_size.md).

## Cloud / unattended

```bash
# Full runbook
# docs/how_to/cloud_capture.md

nohup ./scripts/run-mainnet-capture.sh \
  examples/capture.multi-venue-mainnet.toml \
  > logs/nohup.out 2>&1 &

# Metrics (if enabled in TOML): http://127.0.0.1:9108/metrics
# Stop: kill -TERM <pid>
```

## Documentation

| Section | Path |
|---------|------|
| Doc map | [docs/index.md](docs/index.md) |
| Install | [docs/getting_started/installation.md](docs/getting_started/installation.md) |
| Cloud capture | [docs/how_to/cloud_capture.md](docs/how_to/cloud_capture.md) |
| Catalog layout | [docs/concepts/catalog_layout.md](docs/concepts/catalog_layout.md) |
| Multi-venue / HIP-4 style | [docs/how_to/hip4_capture.md](docs/how_to/hip4_capture.md) |
| Build size | [docs/how_to/build_size.md](docs/how_to/build_size.md) |
| CLI | [docs/reference/cli.md](docs/reference/cli.md) |

## Credentials

Public capture by default (unset API env vars). Optional key+secret in env only —
see [credentials](docs/how_to/credentials.md).

## Development

```bash
make bootstrap-deps
make install-tools
pip install pre-commit && pre-commit install
make test && make clippy && make cargo-deny
```

See [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), [ROADMAP.md](ROADMAP.md).

## License

[LGPL-3.0-or-later](LICENSE). Links against Nautilus Trader (LGPL). [NOTICE](NOTICE).
