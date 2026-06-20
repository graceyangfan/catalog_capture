# Live Validation Plan

## Goal

Prove that runtime market data can be captured directly into Nautilus-native Parquet catalog assets and then read back without any Feather conversion step.

The first validation target should be:

- Binance Futures
- one instrument
- `QuoteTick` only
- no execution logic
- fixed-duration run
- direct catalog readback after stop

This keeps the first live validation focused on the capture path rather than on trading behavior.

## Why start with `QuoteTick`

`QuoteTick` is the cleanest first validation target because:

- it is widely available across live adapters
- it has a clear partitioning model
- it exercises the highest-value "online -> backtest" path
- it avoids the heavier write profile of book deltas in the first pass

Once `QuoteTick` is stable, the next progression should be:

1. `TradeTick`
2. `Bar`
3. `OrderBookDeltas`

## Validation stages

### Stage 1: Synthetic direct catalog round-trip

Purpose:

- validate chunked direct Parquet writing
- validate direct `ParquetDataCatalog` readback
- validate instrument metadata capture alongside market data
- validate Nautilus Trader PyO3 readback, not just local Rust readback
- validate timestamp ordering and interval handling

Expected outcome:

- one or more canonical parquet files are written
- query returns the same `QuoteTick` series in ascending order

Current reference example:

- `crates/catalog-capture-runtime-adapter/examples/synthetic_quote_roundtrip.rs`
- `crates/catalog-capture-runtime-adapter/examples/write_python_readback_fixture.rs`
- `tests/pyo3_market_readback_smoke.py`
- `tests/python_readback_smoke.py`

The current fixture-based smoke also proves that instrument metadata, mark prices,
instrument statuses, instrument closes, and option greeks can be written through the
same actor path and read back via the PyO3 `ParquetDataCatalog`.

### Stage 2: Live market-data capture only

Purpose:

- connect to Binance Futures market data
- subscribe to `QuoteTick`
- let the capture actor record data for a bounded period
- stop cleanly and flush all buffers
- read back the written parquet through Nautilus catalog APIs

Expected outcome:

- parquet files exist under the canonical quote layout
- PyO3 `ParquetDataCatalog` reads the captured ticks directly
- first and last timestamps match the observed live window

Current reference implementation:

- `crates/catalog-capture-runtime-adapter/examples/binance_futures_quote_capture.rs`
- `tests/python_catalog_probe.py`

### Stage 3: Backtest readback validation

Purpose:

- use the captured catalog as input to backtest
- verify the recorded assets are not just queryable, but truly reusable

Expected outcome:

- `BacktestNode` or equivalent backtest loading path accepts the captured catalog without conversion

### Stage 4: Long-run stability

Purpose:

- evaluate a longer live session
- measure chunk count, flush rate, file sizes, and tail flush behavior

Expected outcome:

- no interval overlaps
- stable file creation behavior
- bounded resource growth

For short validation runs where we explicitly want multiple parquet chunks quickly, use:

- `/Users/yfclark/nautilus_catalog_capture/examples/capture.low-threshold.toml`

## Proposed first live scenario

### Venue and instrument

- venue: Binance Futures
- environment: testnet or demo first, then live market-data-only if needed
- instrument: one high-liquidity perpetual, e.g. `ETHUSDT-PERP.BINANCE`

### Runtime shape

- one `LiveNode`
- zero execution logic required
- one dedicated `CatalogCaptureActor`
- one declared `CapturePlan`
- instrument capture declared alongside quote capture in the same plan

### Capture plan

For the first run:

- quotes: enabled for one instrument
- trades: disabled
- bars: disabled
- book deltas: disabled

## Success criteria

The first live validation should be considered successful only if all of the following are true:

1. Live `QuoteTick` events are observed by the capture actor.
2. Direct Parquet files are written through `ParquetDataCatalog`.
3. `ParquetDataCatalog` reads back the captured ticks without any conversion step.
4. Instrument metadata captured through the runtime path can also be read back directly.
5. The captured series is time-ordered and instrument-correct.
6. Stopping the runtime flushes the final buffered ticks cleanly.

## What to record during the test

At minimum, record:

- instrument ID
- runtime start and stop timestamps
- received tick count
- flushed batch count
- generated file paths
- first and last tick timestamps read back from catalog

## Suggested local command sequence

1. Run the capture example:

   - `CAPTURE_SECONDS=30 BINANCE_ENV=testnet cargo +1.96.0 run -p catalog-capture-runtime-adapter --example binance_futures_quote_capture`

2. Copy the printed `Catalog dir`.

3. Verify through Nautilus Trader Python:

   - `/Users/yfclark/nautilus_trader/.venv/bin/python /Users/yfclark/nautilus_catalog_capture/tests/pyo3_market_readback_smoke.py`
   - or for legacy compatibility:
   - `/Users/yfclark/nautilus_trader/.venv/bin/python /Users/yfclark/nautilus_catalog_capture/tests/python_catalog_probe.py <catalog_dir> ETHUSDT-PERP.BINANCE 1`

## Step 2: instrument metadata and contract state

Reference profile:

- `examples/capture.binance-perp.ws.toml` (includes `instrument_statuses` / `instrument_closes`)
- `tests/python_catalog_derivatives_probe.py`
  - live short runs: assert `instruments` + market-data families
  - fixture mode: add `--require-contract-state` after `pyo3_market_readback_smoke.py`

Binance Futures polls instrument status about every 3600s, so a 60s live run may legitimately produce zero status/close rows while still passing the probe.

## Step 3: Deribit perp + option greeks

Reference profile:

- `examples/capture.deribit-btc.toml`
- `tests/python_catalog_deribit_probe.py`

Run:

- `cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-btc.toml`
- `python3 tests/python_catalog_deribit_probe.py /tmp/nautilus-catalog-capture-deribit-btc`

## Step 4: Bybit, OKX, and multi-venue capture

Profiles:

- `examples/capture.bybit-btc.toml` → `tests/python_catalog_bybit_probe.py`
- `examples/capture.okx-btc.toml` → `tests/python_catalog_okx_probe.py`
- `examples/capture.multi-deribit-binance.toml` → `tests/python_catalog_multi_venue_probe.py`

OKX options require `instrument_families` (e.g. `BTC-USD`) when `instrument_types` includes `option`.

### Step 9a-lite option universe smoke

The option universe resolver can be validated with short live captures that resolve the current
nearest expiry and ATM-relative strikes, start the Nautilus live node, and assert that the expanded
per-instrument parquet families were written.

Run one venue:

- `python3 tests/probe_option_universe_smoke.py --venue deribit --seconds 30`
- `python3 tests/probe_option_universe_smoke.py --venue okx --seconds 30`
- `python3 tests/probe_option_universe_smoke.py --venue bybit --seconds 30`

Run all supported venues:

- `python3 tests/probe_option_universe_smoke.py --venue all --seconds 30`

The helper creates a temporary TOML profile under `/tmp`, rewrites `capture_seconds` and
`catalog_uri`, runs `catalog-capture-cli run --print-option-universe`, parses the resolved
`perp=` / `options=[...]` output, then checks these required parquet families:

- `instruments`
- `quotes`
- `trade_tick` or `trades` (required for OKX/Bybit short smokes; Deribit may warn if absent)
- `option_greeks`
- `mark_prices`
- `index_prices`
- `funding_rate_update`
- `metadata/forward_prices.jsonl` (P1 forward snapshots from option greeks)

If `pyarrow` is installed, it also verifies that sampled parquet files contain rows. By default,
the helper then runs `tests/python_catalog_option_universe_probe.py` to read the selected
instruments through Nautilus `ParquetDataCatalog` and assert:

- every selected option has `quotes`, `mark_prices`, and `option_greeks`
- the hedge perp/swap has `quotes`, `trade_ticks`, `mark_prices`, `index_prices`, and funding parquet rows

Research profile smoke:

- `python3 tests/probe_option_universe_smoke.py --venue deribit-research --seconds 30`
- `python3 tests/probe_option_universe_smoke.py --venue all-plus-research --seconds 30`

The research profile uses `capture.deribit-btc-universe-research.toml` and additionally
asserts `DeribitVolatilityIndex` custom data readback.

OI-ranked profile smoke (P2a):

- `python3 tests/probe_option_universe_smoke.py --venue deribit-oi-ranked --seconds 30`
- `python3 tests/probe_option_universe_smoke.py --venue all-plus-oi-ranked --seconds 30`

The OI-ranked profile uses `capture.deribit-btc-universe-oi-ranked.toml` and additionally
asserts `metadata/option_universe_resolutions.jsonl` contains
`strike_selection_mode = "oi_ranked"` and `oi_ranked_top_n`.

Use `--skip-readback-probe` for file-only validation. Use `--cleanup` to remove generated catalogs
after a successful run.

Use `--metrics-probe` when the smoke should also compute a lightweight research snapshot from
the captured parquet:

- `python3 tests/probe_option_universe_smoke.py --venue bybit --seconds 30 --metrics-probe`

The metrics probe reads the same Nautilus catalog and reports:

- latest perp quote mid
- ATM strike and ATM IV decimal
- low-put / high-call IV decimal
- rough risk-reversal and wing-richness proxies in decimal IV units
- per-option latest raw IV, normalized decimal IV, delta, quote mid, and row counts

These are sanity-check features for validating that the captured option universe can feed later
Derivatives Monkey-style surface/skew research. They are not a replacement for a full offline
surface builder or exact 25-delta interpolation.

The probe preserves `mark_iv_raw` because venues can expose IV in different scales. For cross-venue
sanity checks it also prints `mark_iv_decimal`, using a conservative heuristic that treats raw IV
values greater than `3.0` as percentages and divides them by `100`.

### Online option-metrics feasibility

For runtime-only feasibility checks, the capture CLI can also compute a lightweight
`stdout` snapshot directly from live `quotes + option_greeks`, without writing any derived
feature parquet:

```toml
[runtime.online_option_metrics]
enabled = true
snapshot_interval_secs = 3
```

Current scope:

- only activates for `capture.option_universe`
- requires resolved `perp` quotes plus per-option `quotes` and `option_greeks`
- prints ATM IV / low-put IV / high-call IV / rough risk-reversal / wing-richness
- keeps per-instrument parquet capture unchanged; this is an observer, not a new truth source

Short Bybit feasibility run:

- `cargo run -p catalog-capture-cli -- run --config /tmp/capture.bybit-btc-universe-online-metrics.toml --print-option-universe --option-universe-format text`

Representative output:

- `[online-option-metrics] venue=bybit_main underlying=BTC ... atm_iv=0.360600 ... rr=-0.071400 ... greeks_ready=6/6 quotes_ready=6/6`

Current manual smoke results:

- OKX wrote instruments, quotes, option greeks, mark prices, index prices, and funding rates for
  6 BTC options plus `BTC-USD-SWAP.OKX`.
- Deribit wrote the same required families for 6 BTC options plus `BTC-PERPETUAL.DERIBIT`.
- Bybit wrote the same required families for 6 BTC options plus `BTCUSDT-LINEAR.BYBIT`.

## Step 5: custom data live validation

### Deribit DVOL

Reference profile:

- `examples/capture.deribit-dvol.toml`
- `tests/python_catalog_deribit_dvol_probe.py`

Run:

- `cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-dvol.toml`
- `python3 tests/python_catalog_deribit_dvol_probe.py /tmp/nautilus-catalog-capture-deribit-dvol 1 --index-name btc_usd`

### Hyperliquid open interest

Reference profile:

- `examples/capture.hyperliquid-open-interest.toml`
- `tests/python_catalog_hyperliquid_open_interest_probe.py`

Run:

- `cargo run -p catalog-capture-cli -- run --config examples/capture.hyperliquid-open-interest.toml`
- `python3 tests/python_catalog_hyperliquid_open_interest_probe.py /tmp/nautilus-catalog-capture-hyperliquid-open-interest ETH-USD-PERP.HYPERLIQUID 1 --min-quotes 1`

## Step 1 follow-up: derivatives state WS families

After the first `QuoteTick` run succeeds, Step 1 adds the built-in WS families needed for basis and carry research:

- `mark_prices`
- `index_prices`
- `funding_rates`

Reference assets:

- `examples/capture.binance-perp.ws.toml`
- `crates/catalog-capture-runtime-adapter/examples/binance_futures_derivatives_state_capture.rs`
- `tests/python_catalog_derivatives_probe.py`

Suggested command sequence:

1. Validate the Step 1 TOML profile:

   - `cargo +1.96.0 run -p catalog-capture-cli -- validate --config /Users/yfclark/nautilus_catalog_capture/examples/capture.binance-perp.ws.toml`

2. Run the Step 1 live capture (CLI or example):

   - `cargo +1.96.0 run -p catalog-capture-cli -- run --config /Users/yfclark/nautilus_catalog_capture/examples/capture.binance-perp.ws.toml`
   - or:
   - `CAPTURE_SECONDS=60 BINANCE_ENV=testnet cargo +1.96.0 run -p catalog-capture-runtime-adapter --example binance_futures_derivatives_state_capture`

3. Probe all four market-data families:

   - `/Users/yfclark/nautilus_trader/.venv/bin/python /Users/yfclark/nautilus_catalog_capture/tests/python_catalog_derivatives_probe.py <catalog_dir> ETHUSDT-PERP.BINANCE 1`

Step 1 success criteria:

- quotes, mark prices, and index prices are queryable and time-ordered
- funding rate parquet is discoverable under `data/funding_rate_update/<instrument_id>/`
- instrument metadata is present for the same instrument

## Follow-up after Step 1

Once Step 1 succeeds, the next immediate follow-ups should be:

1. Add `instrument_statuses` and `instrument_closes` live validation (Step 2).
2. Add `TradeTick` capture for the same instrument (Step 6).
3. Run a minimal backtest that reads the captured catalog directly.

That will give us a complete proof of the target workflow:

- live runtime data enters Nautilus
- dedicated capture actor writes canonical Parquet
- backtest consumes the same assets directly
