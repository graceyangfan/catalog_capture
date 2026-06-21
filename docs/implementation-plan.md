# Implementation Plan

## Phase 1: Minimal direct catalog capture

### Goal

Produce backtest-readable catalog assets directly from a dedicated runtime capture actor without any Feather conversion step.

### Scope

- build `capture-core`
- define partitioning, batching, and capture-plan contracts
- implement chunked direct parquet output
- expose a dedicated `CatalogCaptureActor`
- verify backtest can read produced files directly

### Deliverables

- `CaptureConfig`
- `CapturePlan`
- `CaptureItem`
- `PartitionKey`
- `PartitionBuffer`
- direct catalog sink
- `CatalogCaptureActor`
- benchmark harness
- integration example
- live validation plan for Binance Futures `QuoteTick`

### Validation

- files are readable by Rust `ParquetDataCatalog`
- rollover boundaries produce non-overlapping intervals
- backtest consumes written data directly
- strategies remain decoupled from persistence policy

## Phase 2: Data-family expansion and external configuration

### Goal

Cover more of the adapter-supported data surface and make capture easier to launch without editing Rust code, with derivatives research and ML dataset construction as the primary use case.

### Deliverables

- broaden `CapturePlan` toward more adapter-supported data families
- formalize data-family capture naming and partition policy
- add runner-oriented config design
- shape CLI and pyO3 launch surfaces

### Immediate derivatives-research priorities

- define the default capture mode as:
  - targeted derivatives first
  - cross-sectional second
  - historical backfill third
- promote `custom_data` into the CLI surface
- inventory and prioritize adapter-native custom families:
  - `docs/native-custom-data-targets.md`
- add venue-oriented data families required for derivatives research:
  - `index_prices`
  - `funding_rates`
  - adapter-emitted `open_interest` style custom data
  - adapter-emitted `liquidations` style custom data
  - adapter-emitted volatility index style custom data
- support declarative multi-venue plans for:
  - options instruments
  - hedge underlyings
  - reference spot / perp / index markets

### P0 target set

The first implementation pass should stay narrow and business-driven:

- specific underlyings that actually have options or derivatives products we care about
- their hedge / reference legs
- real-time OI rather than historical OI

That means the initial custom-data order should be:

1. `HyperliquidOpenInterest` ✅
2. `DeribitVolatilityIndex` ✅
3. **Step 6** — built-in `trades` / selective `book_deltas` / `bars` — done (see `docs/stepwise-capture-roadmap.md`)
4. **Step 7–8** — HTTP `RequestCustomData` / historical backfill — **skipped** for now
5. **Step 9 next** — option-universe polish + offline DM derivation jobs (see Track R below)

Binance Futures custom families below are **deferred until upstream Arrow support** ([nautilus_trader#4297](https://github.com/nautechsystems/nautilus_trader/issues/4297)):

- `BinanceFuturesLiquidation`
- `BinanceFuturesTicker`
- `BinanceFuturesOpenInterest`

And explicitly **not**:

- whole-market capture by default
- historical OI as a primary runtime feature

### Why these families matter

They provide the minimal raw substrate needed for:

- options surface reconstruction
- basis and carry analysis
- GEX / skew / term-structure style offline derivation
- cross-venue microstructure and lead-lag studies
- ML feature generation that is not locked to a single venue panel format

## Phase 3: Adapter maturity and runtime surfaces

### Goal

Support two integration modes cleanly and make the project easier to run in real deployments.

### Deliverables

- actor wiring examples
- adapter traits where still useful
- plugin-based deployment demo
- pyO3 surface implementation start

### Derivatives product focus

Phase 3 should move beyond single-venue validation and make the runner feel like a real derivatives capture process:

- one config can describe multiple venues
- one config can describe both option legs and hedge legs
- one config can declare venue custom families explicitly
- the resulting catalog can be treated as the raw research layer for strategy and ML work

This should still default to targeted derivatives bundles such as:

- one options venue + one hedge venue
- one underlying family such as `BTC` or `ETH`
- one volatility index family when available

## Phase 4: Operational hardening (Track R)

### Goal

Improve long-running behavior without changing the core product promise. Complete **Track R**
before claiming small-VM support for heavy capture profiles.

### Track R deliverables (priority order)

1. **R1 — Per-family memory / partition budget**
   - `max_total_buffer_bytes` cap per family `CaptureRuntime` (not a single process-global pool)
   - `max_active_partitions` with eviction or forced flush of oldest partitions
   - plan-time summed peak estimate: Σ min(partitions × `max_buffer_bytes`, `max_total_buffer_bytes`)
   - startup warning when summed estimate exceeds `runtime.resource_budget_bytes`

2. **R2 — Lazy background workers**
   - create `BackgroundCaptureRuntime` only for families present in the effective `CapturePlan`
   - remove unconditional 12 worker threads at `CatalogCaptureActor::new()`
   - optional shared worker pool per IO class (quotes/trades vs status/metadata) — later

3. **R3 — Metrics export** (done)
   - `runtime.metrics.enabled` serves Prometheus at `/metrics`, JSON at `/metrics.json`
   - per-family labels plus aggregated totals; `process_rss_bytes` when available
   - soak dashboards: `dropped_items`, `active_partitions`, `queued_items`, flush reasons

4. **R4 — Tiered soak acceptance**
   - see `docs/how_to/smoke_and_soak.md` for profiles and pass/fail gates

### Already in place (Phase 4 baseline)

- per-family bounded queueing and background writer workers
- timed flush and segment sync (`flush_interval_ms`, `sync_interval_ms`)
- `FlushReason` observability including seal
- Track S segment lifecycle (S0–S6)

### Candidate items (after R1–R4)

- family-specific queue / flush defaults
- idle-close / reopen policies
- durability options (layered WAL — see `production-architecture.md`, `wuledan/quant` lineage)
- CLI crate split (operator surface only; not blocking soak)

### Derivatives-load validation

Operational hardening should be validated under data shapes that resemble real target products:

- many option instruments
- sparse-but-wide options chains
- high-frequency hedge-leg quotes and trades
- venue custom data with mixed cadence
- cross-venue concurrent capture

**VM guidance (R1/R2 landed — tune budgets before heavy):**

| Profile | VM | Capture scope |
|---------|-----|---------------|
| rolling | 4C8G | single venue, small strike window, no `book_deltas` / full-chain |
| research | 4C16G | multi-venue rolling, Deribit/Bybit/OKX without heavy `book_deltas` |
| heavy | 8C+ with budget tuning | full-chain + selective `book_deltas`; verify startup buffer estimate |

## storage-engine integration (optional, cross-repo)

[wuledan/storage-engine](https://github.com/wuledan/storage-engine) provides Online IO scheduling
(io_uring SQPOLL, libaio, SPDK) and Offline NUMA work-stealing. It does **not** implement
Nautilus catalog layout; integration stays optional.

| Tier | When | Action |
|------|------|--------|
| L0 | Now | Apply design patterns in Rust (capacity budget, lazy workers, tiered soak) |
| L1 | 9b CPU-bound | Separate derive process; optional Offline worker pool for panel jobs |
| L2 | Segment IO-saturated | Evaluate io_uring-backed fsync/seal path via FFI — only if profiling proves benefit |
| L3 | Engine mature | Compaction/tiering over sealed assets — requires readback contract review |

Capture hot path remains: `DataActor` → `CaptureItem` → `ParquetDataCatalog`.

## Step 9b — Offline derivation jobs

Goal: DM-style panels from raw catalog without polluting capture hot path.

| Panel | Raw inputs |
|-------|------------|
| IV term / surface | `option_greeks` + `instruments` |
| GEX / max pain | `option_greeks` (gamma × OI) |
| Basis / carry | `index_prices` + `mark_prices` + `funding_rates` |

Deliverable location: `research/` directory or separate repository. Read path: PyO3
`ParquetDataCatalog` only. `online_option_metrics` remains stdout sanity check, not truth source.

Completion: reproducible jobs producing at least IV term, GEX, and basis panels from one
sealed catalog window.

## Phase 5: File lifecycle optimization

### Goal

Optimize long-run parquet behavior only if production usage proves Phase 1 chunk files are insufficient.

The file-lifecycle tuning criteria for that decision are documented in:

- `docs/flush-rotation-policy.md`

### Candidate items

- active `.part` files
- row-group append
- rollover by row/size/time
- close + rename finalization

## Deferred items

These are intentionally deferred:

- Binance Futures `BinanceFuturesLiquidation`, `BinanceFuturesTicker`, and `BinanceFuturesOpenInterest` custom capture until [nautilus_trader#4297](https://github.com/nautechsystems/nautilus_trader/issues/4297) merges
- built-in framework capture service
- forced object-store semantics support
- compaction as a required feature
- immediate removal of Feather fallback paths
