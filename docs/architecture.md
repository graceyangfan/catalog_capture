# Architecture

## Design principles

1. Keep the core independent from plugin ABI.
2. Reuse Nautilus model and catalog primitives instead of copying engine models.
3. Keep `Catalog` facts separate from `EventStore` and state/snapshot concerns.
4. Keep hot-path work limited to filtering, partitioning, and batching decisions.
5. Prefer a dedicated capture actor over strategy-owned recording logic.
6. Prefer a phased path: chunked direct parquet first, active `.part` writers later if truly needed.

## Primary runtime model

The primary runtime model is a **dedicated `CatalogCaptureActor`**.

That actor:

- is added to the `LiveNode` alongside strategies
- subscribes to a declared `CapturePlan`
- receives the same runtime market-data families as strategies
- writes direct `ParquetDataCatalog` assets without going through Feather

This is the most natural fit for a deployment where many strategies may subscribe to the same data but capture policy should remain explicit and centralized.

## Reader boundary

This project is write-focused. Readback and backtest validation happen through the same
catalog surfaces strategies already use.

That means:

- this project owns runtime capture and parquet writing
- **Nautilus Trader Rust** `ParquetDataCatalog` / Rust backtest is the primary read path
- the writer writes **only** Rust-canonical catalog layout (no Python legacy path mirror)
- backtest reuse uses the same catalog URI without conversion

## Layering

### Layer 1: `catalog-capture-core`

Responsible for:

- `CaptureConfig`
- `CapturePlan`
- `CaptureItem`
- `PartitionKey`
- `PartitionBuffer`
- `OverflowPolicy`
- runtime metrics
- sink contract

Not responsible for:

- plugin ABI
- deployment control plane
- event replay
- state persistence

### Layer 2: runtime adapters

Responsible for:

- exposing a dedicated capture actor
- translating actor callbacks into `CaptureItem`
- feeding the core runtime
- keeping capture separate from strategy logic

The current implementation keeps a single runtime/actor adapter surface. If another deployment
surface is needed later, it should stay a thin shell around the same actor-oriented model rather
than becoming a parallel architecture layer.

### Layer 3: deployment integration

Responsible for:

- concrete storage URIs
- operational defaults
- metrics export
- rollout fallback selection

## Phase 1 file model

Phase 1 uses direct chunk files:

- accumulate a partition buffer
- flush one chunk into one canonical parquet file
- rely on existing catalog semantics for path layout and interval disjointness
- write only Rust-canonical catalog paths (no Python legacy mirror)

This is intentionally simpler than active `.part` append writers.

For the tuning and production tradeoffs around chunk sizing, flush cadence, and when to evolve beyond this model, see:

- `docs/flush-rotation-policy.md`

## Data flow

The intended flow is:

1. `LiveNode` starts strategies and a `CatalogCaptureActor`.
2. The capture actor subscribes according to a declared `CapturePlan`.
3. `on_quote`, `on_trade`, `on_bar`, and `on_book_deltas` callbacks translate runtime events into `CaptureItem`s.
4. The core runtime batches by partition.
5. Reaching `flush_rows` or `max_buffer_bytes` writes a canonical parquet chunk through `ParquetDataCatalog`.
6. Rust backtest / Rust `ParquetDataCatalog` read those assets directly from the same URI.

This keeps capture explicit without requiring strategies to own persistence policy.

## Phase 2 candidate evolution

If Phase 1 proves insufficient, Phase 2 may introduce:

- active `.part` files
- row-group append
- close + rename rollover
- stronger FD / writer lifecycle control
