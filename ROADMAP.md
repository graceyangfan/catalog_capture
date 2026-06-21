# Roadmap

## Product direction

This repository is a standalone capture service for research-grade derivatives and options data.

The working objective is:

- use venue adapters from the sibling `../nautilus_trader` dependency as the data ingress layer
- record runtime market data directly into PyO3-readable parquet catalog assets
- make those assets immediately reusable for:
  - strategy research
  - backtest replay
  - feature engineering
  - ML dataset construction
  - future derivatives analytics products

Capture policy, batching, partitioning, and operator workflows live in this repository.
Readback and backtest consumption are validated through PyO3 `ParquetDataCatalog` and standard
backtest paths — this project writes; downstream tooling reads.

The most important downstream consumers are therefore:

- systematic options strategy research
- cross-venue derivatives monitoring
- offline ML / feature pipelines
- future product surfaces similar to derivatives flow / skew / GEX dashboards

## Phase 1

- define capture contracts
- define a declared `CapturePlan`
- connect to Rust `ParquetDataCatalog`
- expose a dedicated `CatalogCaptureActor`
- support chunked direct Parquet writes for quotes, trades, bars, and deltas
- verify online-written data is backtest-readable after rollover
- prepare a Binance Futures `QuoteTick` live-validation path

## Phase 2

- expand the capture plan from generic market-data validation into research-grade derivatives coverage
- expose a stronger external configuration surface
- benchmark hot-path overhead and chunk sizing
- keep PyO3 `ParquetDataCatalog` as the primary readback contract
- establish a clear default operating mode:
  - targeted derivatives capture first
  - cross-sectional capture second
  - historical backfill third

### Phase 2 priorities

- add first-class CLI support for `custom_data`
- add a data-family registry for venue-supported derivatives families
- prioritize adapter-native custom families using:
  - `docs/native-custom-data-targets.md`
- prepare capture support for:
  - `index_prices`
  - `funding_rates`
  - adapter-emitted `open_interest` style custom data
  - adapter-emitted `liquidations` style custom data
  - adapter-emitted volatility / derivatives index custom data
- defer Binance Futures `BinanceFuturesLiquidation`, `BinanceFuturesTicker`, and
  `BinanceFuturesOpenInterest` until upstream Arrow/Parquet registration lands
  ([nautilus_trader#4297](https://github.com/nautechsystems/nautilus_trader/issues/4297)); continue with
  Step 6 built-in WS families (`trades`, selective `book_deltas`, `bars`) — done; **Step 7–8 HTTP capture/backfill skipped**; focus **Step 9** (universe + offline derivation)
- keep P0 focused on specific underlyings / option-linked products, not whole-market capture
- make multi-venue targeted capture a first-class runtime goal before broad market-wide capture

## Phase 3

- improve venue-oriented runtime composition
- support multi-venue derivatives capture plans cleanly
- add plugin adapter shell for stock `LiveNode` where useful
- expose a first pyO3-friendly launcher surface

### Phase 3 priorities

- Deribit / Bybit / OKX / Derive-oriented capture configs
- unified instrument metadata capture for options chains and hedge legs
- cross-venue capture plans covering:
  - option instruments
  - hedge underlyings
  - reference spot / index legs
  - adapter-native venue custom data

## Track S — Segment Lifecycle (Phase 2a)

Long-running capture (perpetual futures, HIP-4 daily, unattended daemons) needs continuous
append into active segments, scheduled wall-clock seal, and catalog-readable sealed parquet for
direct backtest. See [docs/segment-lifecycle.md](docs/segment-lifecycle.md).

| Milestone | Status |
|-----------|--------|
| S0 `LifecycleConfig` + TOML | done |
| S1 `SegmentCaptureSink` + unit tests | done |
| S2 `CatalogSink` enum (chunked default) | done |
| S3 Runtime/background tick + seal dispatch | done |
| S4 Actor `SEGMENT_SEAL` timer + shutdown seal | done |
| S5 Orphan `.part` recovery + metrics | done |
| S6 Production example + readback validation | done |

Universe refresh (HIP-4 `outcomeMeta`, option universe) is **orthogonal** to segment seal.

## Ecosystem: `wuledan/storage-engine`

Sibling repo [wuledan/storage-engine](https://github.com/wuledan/storage-engine) is a C++20
coroutine IO runtime (Online priority scheduler + Offline NUMA work-stealing). It shares design
DNA with `wuledan/quant` (work-stealing, affinity primitives) but is **not** a drop-in replacement
for Nautilus `ParquetDataCatalog`.

| Layer | Owner | Contract |
|-------|-------|----------|
| Live ingest + raw parquet | this repository | `ParquetDataCatalog` write/read |
| Offline derivation (Step 9b) | `research/` or separate job | PyO3 read raw → derived panels |
| IO / CPU executor (optional) | storage-engine | L1+ integration only when 9b or seal IO needs it |

Integration tiers (see `docs/implementation-plan.md`):

- **L0 (now)** — borrow patterns only: global capacity budget, lazy workers, tiered soak, metrics
- **L1** — storage-engine Offline pool as optional 9b job executor (separate process)
- **L2+** — io_uring seal/compaction only if production proves Parquet encode is not the bottleneck

## Track R — Runtime resource governance (Phase 4a)

Expert review + storage-engine alignment: capture must not claim arbitrary VM sizes until global
memory and worker lifecycle are bounded. **Complete Track R before marketing heavy profiles**
(full-chain + `book_deltas`) on small VMs.

| Milestone | Deliverable | Status |
|-----------|-------------|--------|
| R1 | Per-family `max_total_buffer_bytes` + `max_active_partitions`; plan-time summed peak estimate + `resource_budget_bytes` startup warnings | done |
| R2 | Lazy `BackgroundCaptureRuntime` per `CapturePlan` family (remove fixed 12 OS threads at actor `new()`) | done |
| R3 | HTTP/Prometheus metrics export; RSS + `dropped_items` + `active_partitions` soak dashboards | done |
| R4 | Tiered soak acceptance (rolling / research / heavy profiles) | planned |

Universe refresh and segment seal stay orthogonal to Track R.

## Phase 4

- improve partition lifecycle management
- cap active writers / open resources
- add richer diagnostics and metrics
- formalize research-oriented capture defaults for different data families
- **complete Track R (R1–R4) before unattended heavy-profile claims**

### Phase 4 priorities

- **R1** per-family total buffer cap (`max_total_buffer_bytes`, default 512 MiB per family runtime;
  summed peak bounded by enabled families × cap; `max_buffer_bytes` remains per-partition, default 32 MiB)
- **R2** plan-driven lazy background workers
- file-count and file-size observability
- flush-reason observability (including `FlushReason::Seal`)
- tiered long-run soak validation (4C8G rolling → 4C16G research → heavy after R1)
- family-specific queue / flush defaults
- CLI modularization **after** soak stability (not blocking R1/R2)

## Phase 5

- orphan `.part` recovery and object-store-aware seal
- evaluate stronger durability modes (WAL) only if live capture risk justifies it

### Phase 5 priorities

- `.part` recovery for crash/restart continuity (S5 baseline done for local FS)
- object-store commit semantics when deployment requirements demand them

## Step 9 — Universe + offline derivation (current product focus)

Parallel to Track R; capture writes **raw** only.

| Track | Scope | Status |
|-------|-------|--------|
| 9a | `underlying` / expiry / strike policies, autorefresh, OI preflight | in progress (Deribit/OKX OI preflight open) |
| 9b | Offline jobs: IV term, GEX, basis from raw catalog via PyO3 | planned (`research/` or separate repo) |
| 9c | Binance Options / Thalex adapter gap | evaluate |

9b completion criterion: at least three derived panel families (IV term, GEX, basis) reproducible
from sealed catalog assets. Optional storage-engine Offline workers accelerate 9b CPU only.
