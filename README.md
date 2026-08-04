# Nautilus Catalog Capture

**Unofficial, community-maintained** capture tooling for [Nautilus Trader](https://github.com/nautechsystems/nautilus_trader).  
Not affiliated with or endorsed by Nautech Systems Pty Ltd. See [TRADEMARK.md](TRADEMARK.md) and [NOTICE](NOTICE).

Record live (and runtime-generated) market data **directly** into Nautilus Trader’s
**Rust `ParquetDataCatalog` layout**, then load the same catalog in Rust backtest —
no feather→convert step, no Python legacy path mirror.

## What this is / is not

| This project **does** | This project **does not** |
|----------------------|---------------------------|
| Write catalog-native Parquet (`rust_canonical_only`) | Fork or replace Nautilus Trader |
| Drive capture from **one CLI** + declarative TOML | Be a trading engine, query DB, or ML pipeline |
| Support multi-venue adapters (Binance Futures, Deribit, Bybit, OKX, Hyperliquid) | Ship multiple product binaries or cargo “demo bins” |
| Own flush/rotation, universe, and ops policy | Mirror Python legacy catalog layouts |

**Layout:** only Nautilus Rust catalog paths under `file://…/data/…`.  
Backtest how-to: [docs/how_to/rust_backtest_from_catalog.md](docs/how_to/rust_backtest_from_catalog.md).

## Quick start (3 happy paths)

Requires Rust **1.97.1** (`rust-toolchain.toml`) and a sibling `../nautilus_trader` tree.

### 1) Bootstrap dependencies

```bash
# Prefer existing local ../nautilus_trader; if missing, clone upstream develop
make bootstrap-deps
```

Optional: match CI’s fixed pin → `./scripts/bootstrap-deps.sh --pin-ci`  
Details: [docs/getting_started/installation.md](docs/getting_started/installation.md).

### 2) Validate a config (offline)

```bash
make build
cargo run -p catalog-capture-cli -- validate --config examples/capture.toml
cargo run -p catalog-capture-cli -- print-effective-config --config examples/capture.toml
```

### 3) Short live capture (network)

```bash
# Example: Deribit DVOL (subscribe custom data) — adjust catalog_uri in the TOML first
cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-dvol.toml
```

Other starter configs: `examples/capture.binance-perp-trades.toml`,  
`examples/capture.deribit-btc-universe.toml`, `examples/capture.hyperliquid-open-interest.toml`.  
Operator / unattended: `examples/operator/`.

```bash
# Slim build (one venue only)
cargo build -p catalog-capture-cli --no-default-features --features venue-deribit
```

## Product surface

- **One product binary:** `catalog-capture-cli` (Nautilus-style single entrypoint).
- **Libraries only:** `catalog-capture-core`, `catalog-capture-runtime-adapter` (`rlib`).
- **Configs:** `examples/*.toml` — not cargo `[[example]]` binaries.
- **Custom data:**  
  - stream → `[[capture.custom_data]]`  
  - poll/request → `[[capture.custom_data_requests]]`  
  (strict separation; wrong channel fails validation.)

```bash
make build
make build-release   # target/release/catalog-capture-cli
make clean-debug
```

## Credentials (optional)

Public data by default. For authenticated venues, set env vars (never TOML):
see [docs/how_to/credentials.md](docs/how_to/credentials.md) and `.env.example`.

## Operations (optional)

```bash
# Smoke / soak (network)
python3 tests/probe_option_universe_smoke.py --venue deribit-autorefresh --seconds 30 --cleanup

# Unattended until SIGTERM
make build-release
./scripts/run-capture-service.sh \
  --config examples/operator/capture.deribit-btc-universe-unattended.toml \
  --release
```

Deploy templates: `deploy/launchd/`, `deploy/systemd/`.  
Metrics (when enabled in TOML): `http://…/metrics`.

## Development

```bash
make bootstrap-deps
make install-tools
pip install pre-commit && pre-commit install
make test && make clippy && make cargo-deny
```

Execution plan: [docs/refactor-optimization-plan.md](docs/refactor-optimization-plan.md)  
Doc map: [docs/index.md](docs/index.md)

## License & trademark

- **License:** [LGPL-3.0-or-later](https://www.gnu.org/licenses/lgpl-3.0.en.html) — see [LICENSE](LICENSE).
- Links against Nautilus Trader (LGPL-3.0-or-later). Third-party notices: [NOTICE](NOTICE).
- “NautilusTrader” / “Nautilus Trader” are trademarks of Nautech Systems Pty Ltd.
  This project is independent; use those names only for compatibility statements.
  Policy: [TRADEMARK.md](TRADEMARK.md).

## Documents

| Doc | Topic |
|-----|--------|
| [docs/index.md](docs/index.md) | Full map |
| [docs/architecture.md](docs/architecture.md) | Design |
| [docs/custom-data-contract.md](docs/custom-data-contract.md) | Custom subscribe/request |
| [docs/how_to/rust_backtest_from_catalog.md](docs/how_to/rust_backtest_from_catalog.md) | Rust backtest from capture |
| [docs/how_to/smoke_and_soak.md](docs/how_to/smoke_and_soak.md) | Live validation |
| [docs/refactor-optimization-plan.md](docs/refactor-optimization-plan.md) | Active roadmap |
