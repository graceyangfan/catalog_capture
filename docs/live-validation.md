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

## Follow-up after the first successful run

Once the first Binance Futures `QuoteTick` run succeeds, the next two immediate follow-ups should be:

1. Add `TradeTick` capture for the same instrument.
2. Run a minimal backtest that reads the captured catalog directly.

That will give us a complete proof of the target workflow:

- live runtime data enters Nautilus
- dedicated capture actor writes canonical Parquet
- backtest consumes the same assets directly
