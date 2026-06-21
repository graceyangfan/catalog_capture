# Nautilus Catalog Capture

A standalone Rust-first project for direct runtime-to-catalog capture.

## Purpose

This project implements a deployment-owned capture path where:

- online/runtime-generated market time-series data is written directly as catalog-readable Parquet assets
- backtest can consume those assets after rollover without a `feather -> convert -> parquet` step
- capture policy, batching, and operations stay in this repository

## Project boundary

This project is intentionally **write-focused**.

Its job is to record runtime data into parquet assets that Nautilus Trader can read natively.

It should not become a parallel query or backtest framework.

The preferred validation path is:

- this project writes
- Nautilus Trader PyO3 `ParquetDataCatalog` reads
- Nautilus Trader backtest consumes

## Design stance

This repository intentionally does **not** try to become a forked trading engine.

It is organized as:

- `catalog-capture-core`: capture config, capture plan, partitioning, batching, and direct catalog sink primitives
- `catalog-capture-runtime-adapter`: actor-centric runtime integration for a dedicated `CatalogCaptureActor`
- `catalog-capture-plugin-adapter`: optional plugin-facing shell for stock `LiveNode` deployments
- `catalog-capture-cli`: TOML-driven runner for validation and live capture
- `tests/`: Python-native readback probes and smoke tests
- `examples/`: end-to-end build, synthetic, fixture, and live-capture flows
- `docs/`: RFC, architecture, rollout, and integration strategy documents

## Scope

This repository is capture-focused: it records runtime data and writes catalog assets.
It is not a trading engine, query layer, or backtest framework.

The implementation reuses Nautilus model and persistence primitives as libraries and
owns deployment-specific capture policy here.

## Initial implementation strategy

Phase 1 is intentionally simple:

- chunked direct Parquet writes
- a dedicated capture actor, separate from strategy logic
- canonical Rust catalog path semantics
- a local-file compatibility mirror for Nautilus Trader legacy Python path discovery when needed
- default `high-precision` builds to match typical Nautilus Python environments
- bounded background queues, partition buffers, and timed flush worker skeleton
- online-written data becomes backtest-readable after rollover

Phase 2 may introduce active `.part` writers and row-group append if Phase 1 proves insufficient.

## Design center

The design center is **not** "let every strategy write its own parquet files".

The design center is:

- strategies stay focused on trading decisions
- a dedicated capture actor owns runtime recording
- the capture actor subscribes to a declared `CapturePlan`
- the capture actor writes catalog-native Parquet assets directly
- instrument metadata can be recorded alongside market data so Python readback and backtest setup stay straightforward

That keeps data capture explicit, reusable, and compatible with direct backtest reuse.

## Current compatibility status

The project now validates the workflow that matters most:

- this project writes instrument metadata, quotes, mark prices, instrument statuses, instrument closes, and option greeks through the capture actor path
- this project also writes index prices and funding rates through the same actor path
- Nautilus Trader PyO3 `ParquetDataCatalog` reads them back directly
- this project also writes adapter-compatible Rust custom data through the capture actor path
- Nautilus Trader PyO3 `ParquetDataCatalog` reads those custom assets back directly
- the first P0 targeted-derivatives custom-data path now validates `HyperliquidOpenInterest`
- legacy Python `ParquetDataCatalog` remains a compatibility target, not the primary surface

Today there is one practical PyO3 edge to be aware of:

- `FundingRateUpdate` parquet assets are written correctly and are discoverable in the catalog
- but the PyO3 catalog surface does not yet expose a dedicated funding-rate query helper like it does for quotes, marks, and index prices
- this project therefore validates funding capture today through file discovery and catalog type discovery, while keeping PyO3 direct readback as the target contract

Reference smoke tests (run from the repository root; require a PyO3-capable sibling
`../nautilus_trader` checkout):

- `tests/pyo3_market_readback_smoke.py`
- `tests/python_custom_readback_smoke.py`
- `tests/python_hyperliquid_open_interest_smoke.py`
- `tests/python_readback_smoke.py`

## CLI

The repository now includes a first TOML-driven CLI:

- validate a config:
  - `cargo run -p catalog-capture-cli -- validate --config examples/capture.toml`
- print the effective config:
  - `cargo run -p catalog-capture-cli -- print-effective-config --config examples/capture.toml`
- run a capture session:
  - `cargo run -p catalog-capture-cli -- run --config examples/capture.toml`

The initial CLI focuses on TOML and currently supports a `binance_futures` venue kind.

Useful configs:

- baseline: `examples/capture.toml`
- low-threshold validation profile: `examples/capture.low-threshold.toml`

## Prerequisites

Build requires a sibling dependency checkout (see [installation](docs/getting_started/installation.md))
and Rust `1.96.0` (`rust-toolchain.toml`).

## Operations

### Smoke and soak validation

```bash
# Quick per-venue smoke (30s)
python3 tests/probe_option_universe_smoke.py --venue deribit-autorefresh --seconds 30 --cleanup

# Daily-live soak across Deribit, OKX, and Bybit (180s)
python3 tests/probe_option_universe_soak.py --preset daily-live --seconds 180 --cleanup
```

Use `--require-refresh-change` on longer rolling-live runs when ATM drift is expected.

### Unattended long-running capture

Set `runtime.capture_seconds = 0` to run until `SIGTERM` or `Ctrl+C`. Production-shaped configs live under `examples/operator/`.

```bash
make build-release
./scripts/run-capture-service.sh \
  --config examples/operator/capture.deribit-btc-universe-unattended.toml \
  --release
```

Deployment templates: `deploy/launchd/`, `deploy/systemd/`. See `examples/operator/README.md`.

### Cleanup temp artifacts

```bash
make cleanup-tmp
# or
./scripts/cleanup-tmp-captures.sh /tmp
```

### Post-run validation

```bash
cargo run -p catalog-capture-cli -- validate-option-universe \
  --config examples/capture.deribit-btc-universe-autorefresh.toml \
  --catalog-uri file:///path/to/catalog \
  --option-universe-format text
```

## License

This project is licensed under the [GNU Lesser General Public License v3.0 or later](https://www.gnu.org/licenses/lgpl-3.0.en.html).
It links against [Nautilus Trader](https://github.com/nautechsystems/nautilus_trader) (LGPL-3.0-or-later).
See `NOTICE` and `TRADEMARK.md`.

## Development

```bash
make install-tools
pip install pre-commit && pre-commit install
make test && make clippy && make cargo-deny
```

Documentation map: [docs/index.md](docs/index.md).

## Documents

- [docs/index.md](docs/index.md)
- `docs/rfc.md`
- `docs/architecture.md`
- `docs/production-architecture.md`
- `docs/flush-rotation-policy.md`
- `docs/implementation-plan.md`
- `docs/custom-data-contract.md`
- `docs/native-custom-data-targets.md`
- `docs/integration-strategy.md`
- `docs/live-validation.md`
- `docs/pyo3-surface.md`
