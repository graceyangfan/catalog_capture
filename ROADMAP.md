# Roadmap

## Product direction

This repository is no longer just proving "direct parquet capture works."

The next working objective is:

- use Nautilus Trader native adapters as the data ingress layer
- record research-grade derivatives and options data directly into PyO3-readable parquet catalog assets
- make those assets immediately reusable for:
  - strategy research
  - backtest replay
  - feature engineering
  - ML dataset construction
  - future derivatives analytics products

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
  - `/Users/yfclark/nautilus_catalog_capture/docs/native-custom-data-targets.md`
- prepare capture support for:
  - `index_prices`
  - `funding_rates`
  - adapter-emitted `open_interest` style custom data
  - adapter-emitted `liquidations` style custom data
  - adapter-emitted volatility / derivatives index custom data
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

## Phase 4

- improve partition lifecycle management
- cap active writers / open resources
- add richer diagnostics and metrics
- formalize research-oriented capture defaults for different data families

### Phase 4 priorities

- file-count and file-size observability
- flush-reason observability
- long-run soak validation under realistic derivatives data loads
- family-specific queue / flush defaults

## Phase 5

- evaluate whether active `.part` writers are worth the complexity
- evaluate stronger durability modes
- evaluate object-store-aware commit behavior

### Phase 5 priorities

- active parquet writer only if chunk files prove operationally insufficient
- WAL or stronger durability only if live capture risk justifies it
- object-store commit semantics only when deployment requirements demand them
