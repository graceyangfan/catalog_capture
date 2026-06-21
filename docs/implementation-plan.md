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
5. **Step 9 next** — option-universe polish + offline DM derivation jobs

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

## Phase 4: Operational hardening

### Goal

Improve long-running behavior without changing the core product promise.

### Candidate items

- per-family bounded queueing and background writer workers
- queue-capacity tuning and observability
- timed flush execution validation under live load
- writer lifecycle limits
- idle-close / reopen policies
- richer metrics
- durability options

### Derivatives-load validation

Operational hardening should be validated under data shapes that resemble real target products:

- many option instruments
- sparse-but-wide options chains
- high-frequency hedge-leg quotes and trades
- venue custom data with mixed cadence
- cross-venue concurrent capture

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
