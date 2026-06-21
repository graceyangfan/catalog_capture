# Native custom data targets

## Goal

Identify adapter-emitted custom data families that are strong candidates for capture in this
project **without introducing any project-local schema invention**.

This document answers:

- which custom types already exist in venue adapters
- whether they are stream-like or request-like
- whether they are Arrow/catalog friendly today
- how useful they are for research, backtest enrichment, and ML datasets

## Recording modes

Before choosing a custom data family, classify the intended business mode.

### `targeted_derivatives`

This is the default mode for the current project stage.

Use it when recording data for:

- one options chain
- one or a few derivatives underlyings
- a perp / futures strategy tied to specific symbols
- a vol, skew, carry, or basis research workflow around one underlying family

Typical capture set:

- option instruments
- hedge underlyings
- spot / perp / futures references
- mark prices
- index prices
- funding rates
- real-time open interest
- optional liquidation flow
- optional volatility index

### `cross_sectional_market`

Use this only when the strategy or research workflow is intentionally market-wide.

Typical use cases:

- cross-sectional ranking
- panel ML
- market-wide lead/lag
- broad venue-state or contagion studies

Typical custom families:

- all-market mids
- venue-wide asset context snapshots
- all-market liquidation streams

### `historical_backfill`

Use this for request/batch-oriented historical enrichment rather than live runtime capture.

Typical use cases:

- bootstrap a research dataset
- fill missing history
- pre-load slow moving reference panels

Typical families:

- historical open interest
- request-style aggregate snapshots

## Current default stance

For now, this project should prioritize **`targeted_derivatives`**.

That means:

- prefer instrument-scoped or underlying-scoped custom families
- do not default to all-market feeds
- treat historical request families as follow-on backfill work rather than the main runtime path

## Selection rules

A custom data family is a good target when all of the following are true:

- the type is already emitted by a venue adapter
- the adapter already uses `CustomData` / `DataType`
- the data carries real research or strategy value
- the family can be described declaratively in `CapturePlan` / CLI config
- the resulting parquet should be meaningful to read later through catalog readback surfaces

## Upstream-blocked (deferred)

The following Binance Futures adapter custom types are **not implemented in this project** until
`nautilus-binance` registers them for Arrow/Parquet encoding upstream:

| Type | Planned step | Tracker |
|---|---|---|
| `BinanceFuturesLiquidation` | Step 5b (WS) | [nautilus_trader#4297](https://github.com/nautechsystems/nautilus_trader/issues/4297) |
| `BinanceFuturesTicker` | backlog (WS) | same |
| `BinanceFuturesOpenInterest` | Step 7 (HTTP request) | same |

Runtime subscribe/request may work in Nautilus today, but direct `ParquetDataCatalog` capture
requires Arrow registration. CLI configs referencing these `type_name` values are rejected at
startup. **Step 6 WS families are done**; **Step 7–8 HTTP capture/backfill are skipped**; focus
**Step 9** (see `docs/stepwise-capture-roadmap.md`).

## P0 targets

These are the best next targets for direct capture support in **`targeted_derivatives`** mode.

### `BinanceFuturesLiquidation`（deferred → #4297）

- Adapter: Binance Futures
- Source shape: live stream
- Why it matters:
  - direct liquidation flow is useful for crowding, stress, and momentum studies
  - naturally complements quotes, trades, mark prices, and funding
- Reference (sibling dependency tree):
  - `crates/adapters/binance/src/futures/data.rs`
  - `crates/adapters/binance/src/data_types.rs`
- Notes:
  - already partitioned by instrument through `DataType` metadata when subscribed per instrument
  - stream-native and fits the runtime capture model
  - should be used primarily for selected underlyings, not as the default all-market stream
- Status:
  - **deferred** until [nautilus_trader#4297](https://github.com/nautechsystems/nautilus_trader/issues/4297) lands; do not implement locally

### `BinanceFuturesTicker`（deferred → #4297）

- Adapter: Binance Futures
- Source shape: live stream (`ticker`)
- Reference: `crates/adapters/binance/src/data_types.rs`
- Status:
  - same Arrow-registration gap as liquidation; tracked in #4297; **not in current scope**

### `HyperliquidOpenInterest`

- Adapter: Hyperliquid
- Source shape: live stream from asset context updates
- Why it matters:
  - strong derivatives-research value
  - adapter-native open-interest family (no local schema invention)
- Reference (sibling dependency tree):
  - `crates/adapters/hyperliquid/src/websocket/handler.rs`
  - `crates/adapters/hyperliquid/src/data_types.rs`
- Notes:
  - Arrow/catalog capable when adapter builds with Arrow support
  - ideal first “open interest” family because the type already exists in the adapter
  - this is the preferred first OI family because it is real-time and instrument-scoped
  - validated in this project with:
    - `crates/catalog-capture-runtime-adapter/examples/write_hyperliquid_open_interest_fixture.rs`
    - `tests/python_hyperliquid_open_interest_smoke.py`

### `DeribitVolatilityIndex`

- Adapter: Deribit
- Source shape: live stream
- Why it matters:
  - directly relevant to options and volatility research
  - a clean derivatives-native reference series
- Reference (sibling dependency tree):
  - `crates/adapters/deribit/src/websocket/handler.rs`
  - `crates/adapters/deribit/src/data_types.rs`
- Notes:
  - one of the most natural P0 custom families for the options roadmap
  - likely easier to validate than more complex venue-specific aggregates
  - especially well-matched to options-chain recording around specific underlyings

## P1 targets

These are strong, but slightly less central to the immediate **runtime** derivatives roadmap.

### `BinanceFuturesOpenInterest`（deferred → #4297）

- Adapter: Binance Futures
- Source shape: request/snapshot style
- Reference: `crates/adapters/binance/src/data_types.rs`
- Why it matters:
  - useful for snapshots and point-in-time OI enrichment
- Caveat:
  - request/snapshot-oriented rather than naturally streaming
  - still worth supporting, but should come after the first real-time OI family
- Status:
  - **deferred** until #4297; Step 7 work proceeds with other venues first

### `BinanceFuturesOpenInterestHist`

- Adapter: Binance Futures
- Source shape: request/batch style
- Reference: `crates/adapters/binance/src/data_types.rs`
- Why it matters:
  - good for research backfills and carry term studies
- Caveat:
  - should be treated as a `historical_backfill` family, not a default live runtime target

### `HyperliquidAllMids`

- Adapter: Hyperliquid
- Source shape: live stream
- Reference: `crates/adapters/hyperliquid/src/data_types.rs`
- Why it matters:
  - useful cross-market snapshot for lead-lag and panel studies
- Caveat:
  - more naturally a `cross_sectional_market` family than a targeted-derivatives default
  - aggregate snapshot semantics differ from per-instrument families

### `HyperliquidAllDexsAssetCtxs`

- Adapter: Hyperliquid
- Source shape: live aggregate snapshot
- Reference: `crates/adapters/hyperliquid/src/data_types.rs`
- Why it matters:
  - rich research payload: funding, open interest, oracle, mark, premium, impact prices
- Caveat:
  - more naturally a `cross_sectional_market` family than a targeted-derivatives default
  - currently `no_arrow`, JSON-backed, live-only by design
  - valuable, but should come after simpler Arrow/catalog-native families

## P2 targets

These are valid custom families, but less central for the immediate derivatives capture product.

### `DatabentoImbalance`

- Adapter: Databento
- Source shape: market microstructure feed
- Reference: `crates/adapters/databento/src/types.rs`
- Why it matters:
  - very useful for auction and microstructure research
- Caveat:
  - more equities/venue-microstructure oriented than the current derivatives-first roadmap

### `DatabentoStatistics`

- Adapter: Databento
- Source shape: market statistics feed
- Reference: `crates/adapters/databento/src/types.rs`
- Why it matters:
  - useful for generalized research datasets
- Caveat:
  - less immediately connected to options/perp capture than the P0 set

### Betfair custom families

- Adapter: Betfair
- Examples:
  - `BetfairTicker`
  - `BetfairStartingPrice`
  - `BetfairBspBookDelta`
  - `BetfairRaceRunnerData`
  - `BetfairRaceProgress`
- Reference: `crates/adapters/betfair/src/data_types.rs`
- Why it matters:
  - excellent proof that rich adapter-native custom families can be catalog-persisted cleanly
- Caveat:
  - domain is different from the near-term derivatives/ML roadmap

### `PolymarketResolveRequestSummaryData`

- Adapter: Polymarket
- Source shape: request/result summary
- Reference: `crates/adapters/polymarket/src/resolve.rs`
- Why it matters:
  - operationally informative and request-debug useful
- Caveat:
  - not really a market-data recording family for the current product goal

## Recommended P0 implementation order

To stay focused on **specific derivatives underlyings rather than all-market recording**, while
recording adapter-native payloads as emitted, the best order is:

1. `HyperliquidOpenInterest` ✅
2. `DeribitVolatilityIndex` ✅
3. **Step 6** — built-in `trades` / selective `book_deltas` / `bars` — done
4. **Step 9 next** — universe polish + offline derivation (HTTP Steps 7–8 skipped)
5. `BinanceFuturesLiquidation` — after [nautilus_trader#4297](https://github.com/nautechsystems/nautilus_trader/issues/4297)
6. `BinanceFuturesOpenInterest` — after #4297 (Step 7, skipped until re-evaluated)
7. `BinanceFuturesOpenInterestHist` — backfill mode (Step 8, skipped until re-evaluated)

## Why this order

- `HyperliquidOpenInterest` gives us a true adapter-native open-interest family without inventing a
  local schema.
- `DeribitVolatilityIndex` supports the options roadmap immediately.
- Binance liquidation / ticker / snapshot OI remain valuable, but **blocked on upstream Arrow
  registration**; the project continues with Step 6 rather than local workarounds.
- Binance real-time/snapshot open-interest is useful next, but still secondary to the first
  continuously emitted OI family.
- Binance historical open-interest should stay in the backlog as a backfill mode rather than
  diluting the first runtime implementation.

## Practical project guidance

For now, this project should do **only** the following for custom data:

- subscribe to adapter-emitted `DataType`
- record the `CustomData` payload exactly as emitted
- preserve adapter-chosen `type_name`, metadata, and identifier
- validate catalog discoverability and, where available, PyO3 typed readback

It should **not**:

- invent new research schemas
- rename custom families into project-local names
- normalize multiple venue families into one synthetic type before capture

Those may become useful later, but only after a separate design decision.