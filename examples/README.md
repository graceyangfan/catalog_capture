# Examples

Runnable configs, runtime-adapter demos, and catalog readback probes for this repository.

Run CLI commands from the repository root. Python verification scripts require a PyO3-capable
environment from the sibling `../nautilus_trader` checkout (see `docs/getting_started/installation.md`).

## TOML capture profiles

- `examples/capture.toml`
  - first TOML-driven CLI configuration
  - includes `index_prices` and `funding_rates` for Step 1 basis/carry capture
  - includes a commented example for `capture.custom_data`
  - intended to be used with:
    - `cargo run -p catalog-capture-cli -- validate --config examples/capture.toml`
    - `cargo run -p catalog-capture-cli -- print-effective-config --config examples/capture.toml`
    - `cargo run -p catalog-capture-cli -- run --config examples/capture.toml`
- `examples/capture.binance-perp.ws.toml`
  - Step 1–2 profile: Binance Futures built-in WS + instrument bootstrap
  - captures `quotes`, `mark_prices`, `index_prices`, `funding_rates`, `instruments`, `instrument_statuses`, `instrument_closes`
  - intended to be used with:
    - `cargo run -p catalog-capture-cli -- validate --config examples/capture.binance-perp.ws.toml`
    - `cargo run -p catalog-capture-cli -- run --config examples/capture.binance-perp.ws.toml`
  - verify with:
    - `python3 tests/python_catalog_derivatives_probe.py <catalog_dir> ETHUSDT-PERP.BINANCE 1`
    - fixture contract-state: add `--require-contract-state` after `pyo3_market_readback_smoke.py`
- `examples/capture.binance-perp-trades.toml`
  - Step 6a profile: Step 1–2 families plus `trades` on `ETHUSDT-PERP.BINANCE`
  - intended to be used with:
    - `cargo run -p catalog-capture-cli -- validate --config examples/capture.binance-perp-trades.toml`
    - `cargo run -p catalog-capture-cli -- run --config examples/capture.binance-perp-trades.toml`
  - verify with:
    - `python3 tests/python_catalog_derivatives_probe.py <catalog_dir> ETHUSDT-PERP.BINANCE 1 --min-trade-rows 1`
    - fixture smoke: `python3 tests/python_binance_trades_smoke.py`
    - live smoke (network, 3 min): `python3 tests/probe_binance_trades_smoke.py --cleanup`
- Step 6b option-universe `trades` smoke (Deribit research / Bybit / OKX, 3 min default):
  - `python3 tests/probe_option_universe_trades_smoke.py --cleanup`
  - single venue: `--venue bybit`
- `examples/capture.binance-perp-bars.toml`
  - Step 6c profile: Step 1–2 families plus `bars` on `ETHUSDT-PERP.BINANCE`
  - intended to be used with:
    - `cargo run -p catalog-capture-cli -- validate --config examples/capture.binance-perp-bars.toml`
    - `cargo run -p catalog-capture-cli -- run --config examples/capture.binance-perp-bars.toml`
  - verify with:
    - `python3 tests/python_catalog_derivatives_probe.py <catalog_dir> ETHUSDT-PERP.BINANCE 1 --bar-type ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL`
    - fixture smoke: `python3 tests/python_bars_readback_smoke.py`
    - live smoke (network, 3 min): `python3 tests/probe_binance_bars_smoke.py --cleanup`
- `examples/capture.hyperliquid-bars.toml`
  - Step 6c profile: Hyperliquid perp quotes + 1m `LAST-EXTERNAL` bars
  - intended to be used with:
    - `cargo run -p catalog-capture-cli -- validate --config examples/capture.hyperliquid-bars.toml`
    - `cargo run -p catalog-capture-cli -- run --config examples/capture.hyperliquid-bars.toml`
  - verify with:
    - `python3 tests/python_hyperliquid_bars_probe.py <catalog_dir>`
    - live smoke (network, 3 min): `python3 tests/probe_hyperliquid_bars_smoke.py --cleanup`
- Step 6c `bars` smoke (all venues, perp 1m `LAST-EXTERNAL`, 3 min default):
  - fixture: `python3 tests/python_bars_readback_smoke.py`
  - live all venues: `python3 tests/probe_bars_smoke.py --cleanup`
  - option-universe only: `python3 tests/probe_option_universe_bars_smoke.py --cleanup`
- `examples/capture.deribit-btc.toml`
  - Step 3 profile: Deribit BTC perp + near-term ATM call/put (hard-coded instrument IDs)
  - captures `instruments`, `quotes`, `mark_prices`, `index_prices`, `funding_rates`, `option_greeks`
  - intended to be used with:
    - `cargo run -p catalog-capture-cli -- validate --config examples/capture.deribit-btc.toml`
    - `cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-btc.toml`
  - verify with:
    - `python3 tests/python_catalog_deribit_probe.py <catalog_dir>`
- `examples/capture.deribit-btc-universe.toml`
  - Step 9a-lite profile: Deribit BTC option universe resolved once at startup
  - includes `instrument_statuses` and `instrument_closes` for expiry/settlement lineage
  - declares `[[capture.option_universe]]` instead of concrete option `instrument_id`s
  - resolve without running capture:
    - `cargo run -p catalog-capture-cli -- resolve-option-universe --config examples/capture.deribit-btc-universe.toml`
    - `cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-btc-universe.toml --dry-run-resolve --print-option-universe`
  - run:
    - `cargo run -p catalog-capture-cli -- validate --config examples/capture.deribit-btc-universe.toml`
    - `cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-btc-universe.toml`
- `examples/capture.deribit-btc-universe-research.toml`
  - DM-oriented research profile: rolling universe + `trades` + `forward_prices` + DVOL custom data
  - also includes `instrument_statuses` / `instrument_closes` for lifecycle traceability
  - adds `BTC-PERPETUAL.DERIBIT-1-MINUTE-LAST-EXTERNAL` bars for RV / regime baselines
  - forward prices append to `metadata/forward_prices.jsonl` (derived from option greeks)
  - open interest is available on each `option_greeks` row (`open_interest` field)
  - run: `cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-btc-universe-research.toml`
- `examples/capture.deribit-btc-universe-autorefresh.toml`
  - V1.5 profile: same logical universe as above, plus runtime refresh (Deribit/Bybit/OKX)
  - enable with `[runtime.option_universe_refresh]` (`interval_secs` controls re-resolve cadence)
  - on ATM drift or expiry rollover, the actor subscribes to new instruments and unsubscribes removed ones
  - resolve preview:
    - `cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-btc-universe-autorefresh.toml --dry-run-resolve --print-option-universe`
  - run:
    - `cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-btc-universe-autorefresh.toml`
  - refresh logs appear only when the resolved member set changes:
    - `Option universe refresh venue_id=... add=[...] remove=[...]`
  - resolution lineage is appended to:
    - `<catalog_dir>/metadata/option_universe_resolutions.jsonl`
  - inspect the latest persisted universe state:
    - `cargo run -p catalog-capture-cli -- inspect-option-universe --catalog-uri file:///tmp/nautilus-catalog-capture-deribit-btc-universe-autorefresh --option-universe-format text`
  - validate the latest persisted universe against catalog parquet families:
    - `cargo run -p catalog-capture-cli -- validate-option-universe-catalog --catalog-uri file:///tmp/nautilus-catalog-capture-deribit-btc-universe-autorefresh --option-universe-format text`
    - Post-run inspect + validate runs automatically after `run` when `capture.option_universe` is configured (use `--skip-post-run-report` to disable)
- `examples/capture.bybit-btc-universe.toml`
  - Bybit BTC option universe resolved once at startup
  - includes `instrument_statuses` and `instrument_closes`
  - includes `BTCUSDT-LINEAR.BYBIT-1-MINUTE-LAST-EXTERNAL` bars as the current Bybit research baseline
  - resolve: `cargo run -p catalog-capture-cli -- resolve-option-universe --config examples/capture.bybit-btc-universe.toml`
  - run: `cargo run -p catalog-capture-cli -- run --config examples/capture.bybit-btc-universe.toml`
- `examples/capture.bybit-btc-universe-autorefresh.toml`
  - Bybit BTC option universe with runtime refresh
  - run: `cargo run -p catalog-capture-cli -- run --config examples/capture.bybit-btc-universe-autorefresh.toml`
- `examples/capture.bybit-btc-universe-oi-ranked.toml`
  - Bybit BTC OI-ranked option universe resolved at startup
  - intended for flow/GEX-oriented recording where startup HTTP OI is available
  - run: `cargo run -p catalog-capture-cli -- run --config examples/capture.bybit-btc-universe-oi-ranked.toml`
- `examples/capture.okx-btc-universe.toml`
  - OKX BTC option universe resolved once at startup
  - includes `instrument_statuses` and `instrument_closes`
  - includes `BTC-USD-SWAP.OKX-1-MINUTE-LAST-EXTERNAL` bars as the current OKX research baseline
  - resolve: `cargo run -p catalog-capture-cli -- resolve-option-universe --config examples/capture.okx-btc-universe.toml`
  - run: `cargo run -p catalog-capture-cli -- run --config examples/capture.okx-btc-universe.toml`
- `examples/capture.okx-btc-universe-autorefresh.toml`
  - OKX BTC option universe with runtime refresh
  - run: `cargo run -p catalog-capture-cli -- run --config examples/capture.okx-btc-universe-autorefresh.toml`
- `examples/capture.okx-btc-universe-oi-ranked.toml`
  - OKX BTC OI-ranked option universe profile
  - current recommendation: use for config validation and runtime-refresh-oriented work; startup
    OI-ranked preflight is not yet supported by the OKX adapter HTTP discovery path
  - run: `cargo run -p catalog-capture-cli -- validate --config examples/capture.okx-btc-universe-oi-ranked.toml`
- `examples/capture.bybit-btc.toml`
  - Step 4b profile: Bybit linear perp + ATM call/put
  - run: `cargo run -p catalog-capture-cli -- run --config examples/capture.bybit-btc.toml`
  - verify: `python3 tests/python_catalog_bybit_probe.py /tmp/nautilus-catalog-capture-bybit-btc`
- `examples/capture.okx-btc.toml`
  - Step 4c profile: OKX BTC-USD swap + ATM call/put
  - run: `cargo run -p catalog-capture-cli -- run --config examples/capture.okx-btc.toml`
  - verify: `python3 tests/python_catalog_okx_probe.py /tmp/nautilus-catalog-capture-okx-btc`
- `examples/capture.multi-deribit-binance.toml`
  - Step 4 dual-venue: Binance testnet ETH perp + Deribit BTC perp in one job
  - verify: `python3 tests/python_catalog_multi_venue_probe.py /tmp/nautilus-catalog-capture-multi-deribit-binance`
- `examples/capture.deribit-dvol.toml`
  - Step 5a profile: Deribit BTC perp + `DeribitVolatilityIndex`
  - run: `cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-dvol.toml`
  - validate: `cargo run -p catalog-capture-cli -- validate --config examples/capture.deribit-dvol.toml`
  - verify:
    - `python3 tests/python_catalog_deribit_dvol_probe.py /tmp/nautilus-catalog-capture-deribit-dvol 1 --index-name btc_usd`
- `examples/capture.hyperliquid-open-interest.toml`
  - Step 5b profile: Hyperliquid perp + `HyperliquidOpenInterest`
  - run: `cargo run -p catalog-capture-cli -- run --config examples/capture.hyperliquid-open-interest.toml`
  - validate: `cargo run -p catalog-capture-cli -- validate --config examples/capture.hyperliquid-open-interest.toml`
  - verify:
    - `python3 tests/python_catalog_hyperliquid_open_interest_probe.py /tmp/nautilus-catalog-capture-hyperliquid-open-interest ETH-USD-PERP.HYPERLIQUID 1 --min-quotes 1`
- `examples/capture.low-threshold.toml`
  - intentionally aggressive validation profile
  - useful for forcing fast parquet chunk creation and multi-file behavior
  - intended to be used with:
    - `cargo run -p catalog-capture-cli -- validate --config examples/capture.low-threshold.toml`
    - `cargo run -p catalog-capture-cli -- run --config examples/capture.low-threshold.toml`

## Runtime-adapter examples

- `crates/catalog-capture-runtime-adapter/examples/build_capture_actor.rs`
  - builds a dedicated `CatalogCaptureActor`
  - demonstrates a declared `CapturePlan`
  - demonstrates the intended user-facing configuration shape
- `crates/catalog-capture-runtime-adapter/examples/synthetic_quote_roundtrip.rs`
  - writes synthetic `QuoteTick` data through `CatalogCaptureActor`
  - flushes direct Parquet chunks
  - reads the same data back through PyO3 `ParquetDataCatalog`
  - run with:
    - `cargo run -p catalog-capture-runtime-adapter --example synthetic_quote_roundtrip`
- `crates/catalog-capture-runtime-adapter/examples/write_python_readback_fixture.rs`
  - writes instrument + quote + mark price + index price + funding rate + instrument status + instrument close + option greeks fixture data through `CatalogCaptureActor`
  - readback validated via catalog probes and PyO3 smoke tests:
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
- `crates/catalog-capture-runtime-adapter/examples/write_deribit_dvol_fixture.rs`
  - writes native Deribit `DeribitVolatilityIndex` custom data through `CatalogCaptureActor`
  - intended to be consumed by:
    - `tests/python_deribit_dvol_smoke.py`
- `crates/catalog-capture-runtime-adapter/examples/binance_futures_quote_capture.rs`
  - connects to the Binance Futures venue adapter data client
  - runs a market-data-only live capture for a fixed duration
  - declares instrument + quote capture through the same `CatalogCaptureActor`
  - uses venue adapter runtime callbacks rather than manual protocol handling
  - verify the written catalog with:
    - `python3 tests/python_catalog_probe.py <catalog_dir> <instrument_id> 1`
  - run with:
    - `CAPTURE_SECONDS=30 BINANCE_ENV=testnet cargo run -p catalog-capture-runtime-adapter --example binance_futures_quote_capture`
- `crates/catalog-capture-runtime-adapter/examples/binance_futures_derivatives_state_capture.rs`
  - Step 1 live validation: quotes + mark + index + funding via WS
  - verify the written catalog with:
    - `python3 tests/python_catalog_derivatives_probe.py <catalog_dir> <instrument_id> 1`
  - run with:
    - `CAPTURE_SECONDS=60 BINANCE_ENV=testnet cargo run -p catalog-capture-runtime-adapter --example binance_futures_derivatives_state_capture`

## Planned next examples

- `backtest_reads_captured_catalog`
  - proves the captured files can be reused without conversion
- `plugin_capture_demo`
  - deployment shell example for stock `LiveNode`

The next major validation target is the Binance Futures `QuoteTick` capture path described in `docs/live-validation.md`.

## Option-universe profile matrix

Use these profiles by intent rather than by venue alone:

| Intent | Deribit | Bybit | OKX |
|---|---|---|---|
| Rolling live baseline | `capture.deribit-btc-universe-autorefresh.toml` | `capture.bybit-btc-universe-autorefresh.toml` | `capture.okx-btc-universe-autorefresh.toml` |
| Research-ready baseline | `capture.deribit-btc-universe-research.toml` | `capture.bybit-btc-universe.toml` | `capture.okx-btc-universe.toml` |
| OI-ranked selection | `capture.deribit-btc-universe-oi-ranked-autorefresh.toml` | `capture.bybit-btc-universe-oi-ranked.toml` | `capture.okx-btc-universe-oi-ranked.toml` |
| Full-chain batch | `capture.deribit-btc-universe-all.toml` | `capture.bybit-btc-universe-all.toml` | `capture.okx-btc-universe-all.toml` |

Notes:

- Deribit research adds `DeribitVolatilityIndex` custom data on top of the standard raw market-data families.
- Bybit and OKX base universe profiles already include `trades` and `forward_prices`, so they are the current research-ready baselines.
- Current research baselines also add 1-minute perp/swap `LAST-EXTERNAL` bars for RV / regime work without widening the main rolling-capture surface.
- Standard option-universe profiles now also include `instrument_statuses` and `instrument_closes`.
  Use `tests/probe_option_universe_smoke.py --require-contract-state` or
  `tests/probe_option_universe_soak.py --require-contract-state` on longer live runs to validate them.
- Autorefresh soak runs can also require a real runtime delta with
  `tests/probe_option_universe_soak.py --require-refresh-change` when validating rollover windows.
- Startup `oi_ranked` preflight is currently supported on Bybit. Deribit and OKX should use
  `atm_relative` or `all` at startup, then rely on runtime refresh once `option_greeks` warm up.
- For long validation runs, prefer `tests/probe_option_universe_soak.py` over ad hoc one-off commands.

## Operator configs

Production-oriented unattended capture lives under `examples/operator/`. See `examples/operator/README.md`
and `docs/how_to/` for launchd/systemd deployment.