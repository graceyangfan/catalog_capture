# Examples

Current examples and planned validation paths:

- `examples/capture.toml`
  - first TOML-driven CLI configuration
  - includes `index_prices` and `funding_rates` for Step 1 basis/carry capture
  - includes a commented example for `capture.custom_data`
  - intended to be used with:
    - `cargo +1.96.0 run -p catalog-capture-cli -- validate --config /Users/yfclark/nautilus_catalog_capture/examples/capture.toml`
    - `cargo +1.96.0 run -p catalog-capture-cli -- print-effective-config --config /Users/yfclark/nautilus_catalog_capture/examples/capture.toml`
    - `cargo +1.96.0 run -p catalog-capture-cli -- run --config /Users/yfclark/nautilus_catalog_capture/examples/capture.toml`
- `examples/capture.binance-perp.ws.toml`
  - Step 1 profile: Binance Futures built-in WS families only
  - captures `quotes`, `mark_prices`, `index_prices`, `funding_rates`
  - intended to be used with:
    - `cargo +1.96.0 run -p catalog-capture-cli -- validate --config /Users/yfclark/nautilus_catalog_capture/examples/capture.binance-perp.ws.toml`
    - `cargo +1.96.0 run -p catalog-capture-cli -- run --config /Users/yfclark/nautilus_catalog_capture/examples/capture.binance-perp.ws.toml`
  - verify with:
    - `/Users/yfclark/nautilus_trader/.venv/bin/python /Users/yfclark/nautilus_catalog_capture/tests/python_catalog_derivatives_probe.py <catalog_dir> ETHUSDT-PERP.BINANCE 1`
- `examples/capture.low-threshold.toml`
  - intentionally aggressive validation profile
  - useful for forcing fast parquet chunk creation and multi-file behavior
  - intended to be used with:
    - `cargo +1.96.0 run -p catalog-capture-cli -- validate --config /Users/yfclark/nautilus_catalog_capture/examples/capture.low-threshold.toml`
    - `cargo +1.96.0 run -p catalog-capture-cli -- run --config /Users/yfclark/nautilus_catalog_capture/examples/capture.low-threshold.toml`

- `crates/catalog-capture-runtime-adapter/examples/build_capture_actor.rs`
  - builds a dedicated `CatalogCaptureActor`
  - demonstrates a declared `CapturePlan`
  - demonstrates the intended user-facing configuration shape
- `crates/catalog-capture-runtime-adapter/examples/synthetic_quote_roundtrip.rs`
  - writes synthetic `QuoteTick` data through `CatalogCaptureActor`
  - flushes direct Parquet chunks
  - reads the same data back through Nautilus `ParquetDataCatalog`
  - run with:
    - `cargo +1.96.0 run -p catalog-capture-runtime-adapter --example synthetic_quote_roundtrip`
- `crates/catalog-capture-runtime-adapter/examples/write_python_readback_fixture.rs`
  - writes instrument + quote + mark price + index price + funding rate + instrument status + instrument close + option greeks fixture data through `CatalogCaptureActor`
  - keeps reading responsibility outside this project
  - intended to be consumed by PyO3-first and legacy-compatibility smoke tests:
    - `tests/pyo3_market_readback_smoke.py`
    - `tests/python_readback_smoke.py`
- `crates/catalog-capture-runtime-adapter/examples/write_python_custom_readback_fixture.rs`
  - writes instrument + Rust custom data fixture items through `CatalogCaptureActor`
  - demonstrates `subscribe_data(...)` / `on_data(...)` capture rather than a market-data-only path
  - intended to be consumed by the Python smoke test:
    - `tests/python_custom_readback_smoke.py`
- `crates/catalog-capture-runtime-adapter/examples/write_hyperliquid_open_interest_fixture.rs`
  - writes instrument + native Hyperliquid `HyperliquidOpenInterest` custom data through `CatalogCaptureActor`
  - demonstrates the P0 targeted-derivatives custom-data path using an adapter-emitted type rather than a project-local schema
  - intended to be consumed by:
    - `tests/python_hyperliquid_open_interest_smoke.py`
- `crates/catalog-capture-runtime-adapter/examples/binance_futures_quote_capture.rs`
  - connects to the Nautilus Binance Futures data client
  - runs a market-data-only live capture for a fixed duration
  - declares instrument + quote capture through the same `CatalogCaptureActor`
  - relies on Nautilus-native adapter/runtime callbacks rather than manual protocol handling
  - verify the written catalog with:
    - `/Users/yfclark/nautilus_trader/.venv/bin/python /Users/yfclark/nautilus_catalog_capture/tests/python_catalog_probe.py <catalog_dir> <instrument_id> 1`
  - run with:
    - `CAPTURE_SECONDS=30 BINANCE_ENV=testnet cargo +1.96.0 run -p catalog-capture-runtime-adapter --example binance_futures_quote_capture`
- `crates/catalog-capture-runtime-adapter/examples/binance_futures_derivatives_state_capture.rs`
  - Step 1 live validation: quotes + mark + index + funding via WS
  - verify the written catalog with:
    - `/Users/yfclark/nautilus_trader/.venv/bin/python /Users/yfclark/nautilus_catalog_capture/tests/python_catalog_derivatives_probe.py <catalog_dir> <instrument_id> 1`
  - run with:
    - `CAPTURE_SECONDS=60 BINANCE_ENV=testnet cargo +1.96.0 run -p catalog-capture-runtime-adapter --example binance_futures_derivatives_state_capture`

Planned next examples:

- `backtest_reads_captured_catalog`
  - proves the captured files can be reused without conversion
- `plugin_capture_demo`
  - deployment shell example for stock `LiveNode`

The next major validation target is the Binance Futures `QuoteTick` capture path described in `docs/live-validation.md`.
