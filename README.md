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

## Status

**0.1.x — early open source, research / operator oriented.**

- Multi-venue mainnet capture (public market data) has been run unattended for
  multi-day windows with scheduled segment sealing and Parquet readback.
- APIs and TOML may still evolve; pin a commit for production jobs.
- Best-effort support — see [SECURITY.md](SECURITY.md) and [ROADMAP.md](ROADMAP.md).

## Features

- **One product binary** — `catalog-capture-cli` (TOML configs only)
- **Rust catalog layout only** — `rust_canonical_only` (Nautilus `ParquetDataCatalog`)
- **Venue adapters** — Binance Futures/Spot SBE, Bybit, Deribit, OKX,
  Hyperliquid, Lighter/Robinhood Chain, Extended, and Predict (`venue-*` features)
- **Lighter binary options** — fixed markets and automatic 15m capture via
  [nautilus-lighter-ext](crates/nautilus-lighter-ext/README.md) (`venue-lighter-ext`);
  [operator guide](docs/how_to/lighter-binary-capture.md) and
  [rolling profile](examples/capture.lighter-updown-15m.toml)
- **Dynamic products** — option universes, Hyperliquid HIP-4, and Predict
  crypto Up/Down selectors discover and rotate instruments without restarting
- **Order book checkpoints** — managed Nautilus books checkpointed hourly and at
  segment boundaries
- **Segment lifecycle** — append `*.parquet.part`, then seal on the configured
  schedule; rolling Predict products seal on their own interval boundaries
- **Mainnet-oriented examples** — public data by default (no keys in TOML)

## Quick start

Requires Rust **1.99.1** and sibling `../nautilus_trader_quantity_commission`. Run from the **repo root**.

```bash
# Clone + checkout the Nautilus revision pinned by CI (recommended)
make bootstrap-deps

# Standard public multi-venue profile: link only the adapters it uses
make build-release-capture
# default CAPTURE_FEATURES: venue-binance,venue-deribit,venue-hyperliquid,venue-lighter

./bin/catalog-capture-cli validate \
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
| **`examples/capture.multi-venue-mainnet.toml`** | Multi-venue instruments, quotes, trades and order-book data |
| `examples/capture.predict-native.toml` | Predict BTC 5m native YES depth and trades |
| `examples/capture.predict-crypto-updown.toml` | BTC/ETH/BNB 5m/15m Predict depth/trades plus Binance Spot SBE |
| `examples/capture.extended-btc-perp.toml` | Extended BTC perpetual public market data |
| `examples/capture.hyperliquid-hip4-btc-daily.toml` | Hyperliquid rolling-universe example |
| `examples/capture.deribit-btc-book-summary.toml` | Deribit BookSummary only (1s poll) |

More: [examples/README.md](examples/README.md).

## Build options

| Command | When |
|---------|------|
| `make build-release-capture` | Standard mainnet feature set; override `CAPTURE_FEATURES` for another profile |
| `make build-release` | All venues (`all-venues`) |
| `make build-slim FEATURES=venue-deribit` | Single-venue debug |
| `make build-release-small` | Smaller binary, slower compile |
| `make clean` / `make clean-all-targets` | Free disk (`clean-all` also clears `../nautilus_trader_quantity_commission/target`) |

Build size notes: [docs/how_to/build_size.md](docs/how_to/build_size.md).

## Cloud / unattended

```bash
# Full runbook
# docs/how_to/cloud_capture.md

mkdir -p logs
nohup env NAUTILUS_LOG='stdout=Info;is_colored=false' \
  ./bin/catalog-capture-cli run \
  --config examples/capture.multi-venue-mainnet.toml \
  > logs/capture.log 2>&1 </dev/null &
echo $! > logs/capture.pid

# Metrics (if enabled in TOML): http://127.0.0.1:9108/metrics
# Stop gracefully: kill -TERM "$(cat logs/capture.pid)"
```

Start the binary directly so `logs/capture.pid` is the recorder PID. Do not
background `scripts/run-capture-service.sh`; it is a foreground build/logging
helper. See [docs/how_to/unattended_capture.md](docs/how_to/unattended_capture.md)
for the stale-PID guard and shutdown wait.

## Documentation

| Section | Path |
|---------|------|
| Doc map | [docs/index.md](docs/index.md) |
| Install | [docs/getting_started/installation.md](docs/getting_started/installation.md) |
| Cloud capture | [docs/how_to/cloud_capture.md](docs/how_to/cloud_capture.md) |
| Catalog layout | [docs/concepts/catalog_layout.md](docs/concepts/catalog_layout.md) |
| Dynamic universe / subscription lifecycle | [docs/how_to/hip4_capture.md](docs/how_to/hip4_capture.md) |
| Build size | [docs/how_to/build_size.md](docs/how_to/build_size.md) |
| CLI | [docs/reference/cli.md](docs/reference/cli.md) |

## Credentials

Most venue profiles use public market data. Predict and Binance Spot SBE require
dedicated local credentials; secrets never belong in capture TOML. See
[credentials](docs/how_to/credentials.md).

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
