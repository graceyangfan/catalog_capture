# Documentation

Catalog Capture is an independent capture tool that writes Nautilus Trader Rust
catalog layouts. Structure follows the same Divio split as Nautilus Trader docs.

## Getting started

1. [Installation](getting_started/installation.md) — sibling deps, toolchain, build
2. [Quickstart](getting_started/quickstart.md) — validate and first run
3. [Examples](../examples/README.md) — TOML profiles

## Concepts

| Doc | Topic |
|-----|--------|
| [Architecture](concepts/architecture.md) | Layers, actor, product boundary |
| [Catalog layout](concepts/catalog_layout.md) | On-disk contract for backtest |
| [Custom data](concepts/custom_data.md) | Subscribe vs request |
| [Flush and rotation](concepts/flush_and_rotation.md) | Buffer / flush profiles |
| [Segment lifecycle](concepts/segment_lifecycle.md) | Continuous capture + seal |

## How-to

| Doc | Task |
|-----|------|
| [Credentials](how_to/credentials.md) | Public vs env keys |
| [Rust backtest from catalog](how_to/rust_backtest_from_catalog.md) | Load captured data |
| [Unattended capture](how_to/unattended_capture.md) | Long-running service |
| [HIP-4 capture](how_to/hip4_capture.md) | BTC daily strategies + rotation |
| [Smoke and soak](how_to/smoke_and_soak.md) | Live validation |

## Developer guide

| Doc | Topic |
|-----|--------|
| [Environment setup](developer_guide/environment_setup.md) | Day-to-day workflow |
| [Pre-commit](developer_guide/pre_commit.md) | Hooks |

## Reference

- [CLI](reference/cli.md)
- Core library surface: [crates/catalog-capture-core/README.md](../crates/catalog-capture-core/README.md)
