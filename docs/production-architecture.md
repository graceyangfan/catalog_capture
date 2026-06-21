# Production Architecture

## Goal

Move this project from a strong Phase 1 direct-parquet proof into a production-shaped
runtime capture framework that still feels natural in the Nautilus Trader ecosystem.

The target outcome is:

- runtime market and adapter-supported data can be recorded directly into parquet
- Nautilus Trader PyO3 `ParquetDataCatalog` reads those assets directly
- backtest reuses those assets without conversion
- users configure capture externally through a concise CLI or pyO3 surface
- hot-path work stays small and predictable
- the resulting datasets can serve as the raw research layer for options strategies, ML pipelines, and future derivatives analytics products

## Design priorities

1. Keep capture external to `nautilus_trader` core.
2. Keep readback native to Nautilus Trader.
3. Reuse `DataActor` and `ParquetDataCatalog` rather than introducing parallel abstractions.
4. Support multiple venues, instruments, and data families through a declarative capture plan.
5. Treat durability, compatibility mirrors, and file lifecycle as policies rather than hard-coded behavior.
6. Default to targeted derivatives capture rather than broad market-wide capture.

## Current Phase 1 shape

The project already has the right product boundary:

- `catalog-capture-core` owns capture config, partitioning, buffering, and direct parquet sinks
- `catalog-capture-runtime-adapter` owns the dedicated `CatalogCaptureActor`
- output is validated through Nautilus Trader PyO3 `ParquetDataCatalog`

The current implementation is intentionally simple:

- per-family runtimes
- per-family background workers with bounded queues
- per-partition in-memory buffers
- chunked direct parquet writes
- timed flush skeleton
- local filesystem Python-legacy mirror compatibility
- first-class actor capture for instruments, market data, and custom data

## Capture modes

### `targeted_derivatives`

This should be the default operating mode for the next implementation phase.

It targets:

- option chains
- hedge underlyings
- perp / futures references
- spot references
- vol / index references
- real-time OI and liquidation families when relevant

### `cross_sectional_market`

This should remain an explicit opt-in mode for:

- market-wide panels
- ranking strategies
- cross-sectional ML datasets

### `historical_backfill`

This should remain separate from the default live runtime recorder and cover request/batch style
families such as historical OI.

## What production maturity still requires

### 1. Broader data-family coverage

Today the capture plan focuses on:

- instruments
- quotes
- mark prices
- instrument status
- instrument closes
- option greeks
- trades
- bars
- book deltas
- custom data

To better match the existing Nautilus adapter ecosystem, the next layer should expand toward:

- additional venue-specific or custom typed data families exposed through Nautilus `DataActor`

For the derivatives-research roadmap, the next additions should prioritize:

- `index_prices`
- `funding_rates`
- adapter-emitted `open_interest` style custom data
- adapter-emitted `liquidations` style custom data
- adapter-emitted volatility index style data
- better CLI exposure for `custom_data`

For P0, this should be interpreted narrowly:

- capture custom families for selected derivatives underlyings
- prefer real-time OI over historical OI
- avoid enabling whole-market families by default

The right organizing idea is a **data-family registry**, not ad hoc per-example growth.

One current boundary to account for:

- not every built-in Nautilus Rust data family currently has a first-class dedicated PyO3 query helper
- for example, `FundingRateUpdate` can already be written and discovered cleanly in the catalog, but the current PyO3 catalog surface does not yet expose a dedicated funding-rate query method
- capture support should still proceed, while validation distinguishes between:
  - direct PyO3 typed readback already available today
  - catalog discoverability and file-level correctness where a dedicated query helper is still missing

### 2. Background write execution

The project now has a first worker skeleton:

- actor callback performs lightweight classification and enqueue
- bounded per-family queues absorb short bursts
- background writer workers own flush execution
- shutdown drains buffers and finalizes files

What still remains for production maturity is:

- queue sizing policy per family or venue
- richer overflow observability
- more explicit worker lifecycle metrics
- long-run soak validation under live load
- flush-reason and file-size observability

### 3. Timed flush behavior

`flush_interval_ms` is now enforced by the background worker skeleton.

Production behavior should support:

- row-threshold flush
- byte-threshold flush
- timed flush
- shutdown flush

The remaining work is to validate tuned defaults under real live workloads rather than only
fixture and smoke-test traffic.

### 4. Durability policy

Borrowing from `wuledan/quant`, durability should be configurable and layered:

- no WAL
- batch WAL
- periodic sync WAL
- stronger durability modes only when explicitly chosen

The core point is to avoid forcing every deployment into the same latency / durability tradeoff.

### 5. File lifecycle evolution

Phase 1 uses chunked direct parquet files.

That is simple and compatible, but long-running high-frequency capture may eventually benefit from:

- active `.part` files
- row-group append
- rollover on row/size/time thresholds
- close + rename finalization

This should be a later optimization, not a prerequisite for the core workflow.

See also:

- `docs/flush-rotation-policy.md`

## Recommended runtime architecture

### Layer 1: capture-core

Should own:

- configuration types
- capture plan
- data-family registry
- partition policy
- buffer policy
- flush policy
- layout compatibility policy
- durability policy
- sink contracts

### Layer 2: runtime adapters

Should own:

- `CatalogCaptureActor`
- callback-to-item translation
- subscription and unsubscription lifecycle
- runtime-specific wiring

### Layer 3: runner surfaces

Should own:

- CLI configuration loading
- TOML parsing and effective-config rendering
- venue / client setup
- process lifecycle
- logging and metrics
- pyO3 launch surfaces

## Configuration direction

The most maintainable operator experience is a declarative config model.

At minimum this should separate:

- runtime output settings
- queue and flush settings
- venue/client definitions
- capture plan definitions
- compatibility and durability policies

That allows a capture process to be launched without editing Rust code.

## Why this architecture

This evolution keeps the capture service focused because it:

- uses `DataActor` as the runtime ingress
- uses `ParquetDataCatalog` as the write/read contract
- treats PyO3 and backtest workflows as first-class consumers
- keeps long-running capture policy in this repository rather than in core trading logic

## Near-term implementation order

1. Split and stabilize module boundaries in this project.
2. Add broader data-family support in `CapturePlan`.
3. Add a runner-oriented external config model.
4. Introduce background write execution and timed flush.
5. Add optional durability layers.
6. Revisit active `.part` files only if production usage proves they are needed.
