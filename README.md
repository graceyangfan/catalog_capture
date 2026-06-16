# Nautilus Catalog Capture

A standalone Rust-first project for direct runtime-to-catalog capture on top of Nautilus Trader.

## Purpose

This project exists to implement a deployment-owned capture path where:

- online/runtime-generated market time-series data is written directly as catalog-readable Parquet assets
- backtest can consume those assets after rollover without a `feather -> convert -> parquet` step
- the implementation remains external to the Nautilus Trader core repository
- the architecture stays compatible with future upstream cooperation through small hooks or helper improvements

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
- `docs/`: RFC, architecture, rollout, and upstream strategy documents

## Why this repository exists outside `nautilus_trader`

The Nautilus maintainers have stated that runtime capture is considered deployment-specific and should remain user-owned rather than framework-built-in.

That makes an external project the cleanest long-term structure:

- we reuse Nautilus model and persistence primitives
- we own the operational capture policy
- we can still upstream small generic hooks or helpers later if needed

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
- the capture actor writes Nautilus-native `ParquetDataCatalog` assets directly
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
- but the current Nautilus Trader PyO3 catalog surface does not yet expose a dedicated funding-rate query helper like it does for quotes, marks, and index prices
- this project therefore validates funding capture today through file discovery and catalog type discovery, while keeping PyO3 direct readback as the target contract

Reference smoke test:

- `/Users/yfclark/nautilus_catalog_capture/tests/pyo3_market_readback_smoke.py`
- `/Users/yfclark/nautilus_catalog_capture/tests/python_custom_readback_smoke.py`
- `/Users/yfclark/nautilus_catalog_capture/tests/python_hyperliquid_open_interest_smoke.py`
- `/Users/yfclark/nautilus_catalog_capture/tests/python_readback_smoke.py`

## CLI

The repository now includes a first TOML-driven CLI:

- validate a config:
  - `cargo +1.96.0 run -p catalog-capture-cli -- validate --config /Users/yfclark/nautilus_catalog_capture/examples/capture.toml`
- print the effective config:
  - `cargo +1.96.0 run -p catalog-capture-cli -- print-effective-config --config /Users/yfclark/nautilus_catalog_capture/examples/capture.toml`
- run a capture session:
  - `cargo +1.96.0 run -p catalog-capture-cli -- run --config /Users/yfclark/nautilus_catalog_capture/examples/capture.toml`

The initial CLI focuses on TOML and currently supports a `binance_futures` venue kind.

Useful configs:

- baseline:
  - `/Users/yfclark/nautilus_catalog_capture/examples/capture.toml`
- low-threshold validation profile:
  - `/Users/yfclark/nautilus_catalog_capture/examples/capture.low-threshold.toml`

## Documents

- `docs/rfc.md`
- `docs/architecture.md`
- `docs/production-architecture.md`
- `docs/flush-rotation-policy.md`
- `docs/implementation-plan.md`
- `docs/custom-data-contract.md`
- `docs/native-custom-data-targets.md`
- `docs/upstream-strategy.md`
- `docs/live-validation.md`
- `docs/pyo3-surface.md`
