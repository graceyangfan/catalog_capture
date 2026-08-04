# Documentation

Catalog Capture writes **Nautilus Trader Rust `ParquetDataCatalog`** layouts.
Docs follow the Divio split (getting started / concepts / how-to / reference).

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

## Developer guide

| Doc | Topic |
|-----|--------|
| [Environment setup](developer_guide/environment_setup.md) | Local workflow |
| [Pre-commit](developer_guide/pre_commit.md) | Hooks |

## Reference

- [CLI](reference/cli.md)
- Core API: [crates/catalog-capture-core/README.md](../crates/catalog-capture-core/README.md)

## Scripts

| Script | Role |
|--------|------|
| `scripts/bootstrap-deps.sh` | Sibling `nautilus_trader` |
| `scripts/run-mainnet-capture.sh` | Build slim release + run (default multi-venue) |
| `scripts/run-capture-service.sh` | Generic long-running run + log |
| `scripts/cleanup-tmp-captures.sh` | Clear `./data` or `/tmp` smoke dirs |
| `scripts/optional-user-service.sh` | Optional user systemd/launchd |
