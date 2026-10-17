# Documentation

Catalog Capture writes **Nautilus Trader Rust `ParquetDataCatalog`** layouts.
Operator documentation follows the Divio split (getting started / concepts /
how-to / reference); protocol design records are listed separately.

## Getting started

| Doc | Purpose |
|-----|---------|
| [Installation](getting_started/installation.md) | Sibling `nautilus_trader`, toolchain, build |
| [Quickstart](getting_started/quickstart.md) | Validate + first run |
| [Examples](../examples/README.md) | TOML profiles |

## How-to (operators)

| Doc | Purpose |
|-----|---------|
| [Cloud capture](how_to/cloud_capture.md) | Clone, build, unattended mainnet, monitor |
| [Multi-venue / HIP-4 style](how_to/hip4_capture.md) | Streams, rotation clocks, BookSummary rate |
| [Lighter XEMM capture](how_to/lighter_xemm_capture.md) | Lighter and Robinhood Chain L2, trades and funding |
| [Predict orderbook capture](predict-orderbook-capture-design.md) | YES-only snapshots, rotation, catalog layout, and optional Binance SBE L2 |
| [Predict private API calibration](how_to/predict-private-api-tests.md) | Local TOML, automatic discovery, SDK parity, and native read-only calibration |
| [Predict Rust strategy and LiveNode](how_to/predict-rust-strategy-calibration.md) | Custom snapshot signals, execution calibration and supervision |
| [Build size](how_to/build_size.md) | Why `target/` is large; slim release |
| [Unattended capture](how_to/unattended_capture.md) | Long-running process |
| [Credentials](how_to/credentials.md) | Public vs env keys |
| [Rust backtest from catalog](how_to/rust_backtest_from_catalog.md) | Load with `ParquetDataCatalog` |
| [Smoke and soak](how_to/smoke_and_soak.md) | Live probes |

## Concepts

| Doc | Topic |
|-----|--------|
| [Architecture](concepts/architecture.md) | Layers and product boundary |
| [Catalog layout](concepts/catalog_layout.md) | Official `data/{type}/{id}/…parquet` |
| [Custom data](concepts/custom_data.md) | Subscribe vs request |
| [Flush and rotation](concepts/flush_and_rotation.md) | Buffer / flush profiles |
| [Segment lifecycle](concepts/segment_lifecycle.md) | Seal at wall-clock boundary |

## Design records

These documents capture protocol evidence, invariants, and implementation
boundaries. They are not operator runbooks.

| Doc | Scope |
|-----|-------|
| [Extended public adapter](extended-public-capture-design.md) | RPC v2 protocol and realtime lifecycle |
| [Predict execution adapter](predict-execution-adapter-design.md) | Orders, wallet events, reconciliation, and execution lifecycle |

## Developer guide

| Doc | Topic |
|-----|--------|
| [Environment setup](developer_guide/environment_setup.md) | Local workflow |
| [Pre-commit](developer_guide/pre_commit.md) | Hooks |

## Reference

- [CLI](reference/cli.md)
- [Predict adapter tests](../crates/nautilus-predict/tests/README.md)
- [Predict live evidence](reference/predict-live-evidence.md)
- [Inventory-unit commissions](reference/quantity-commission.md)
- [Predict work plan](predict-execution-work-plan.md)
- Core API: [crates/catalog-capture-core/README.md](../crates/catalog-capture-core/README.md)

## Scripts

| Script | Role |
|--------|------|
| `scripts/bootstrap-deps.sh` | Sibling `nautilus_trader` |
| `scripts/run-mainnet-capture.sh` | Build slim release + run (default multi-venue) |
| `scripts/run-capture-service.sh` | Generic long-running run + log |
| `scripts/cleanup-tmp-captures.sh` | Clear `./data` or `/tmp` smoke dirs |
| `scripts/optional-user-service.sh` | Optional user systemd/launchd |
