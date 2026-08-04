# Examples — capture configs only

This directory holds **TOML configuration files** for the single product binary
`catalog-capture-cli` (same idea as Nautilus Trader’s single `nautilus` CLI).

```text
There is no second “examples binary”.
  cargo run -p catalog-capture-cli -- run --config examples/<profile>.toml
```

- **Do not** add cargo `[[bin]]` / `[[example]]` demos for product flows.
- Former Rust `[[example]]` sources live under `dev/legacy-examples/` and are
  **not** built.

## How to run

From the repository root (after `make bootstrap-deps` / `make build`):

```bash
# Validate only
cargo run -p catalog-capture-cli -- validate --config examples/capture.toml

# Short live capture (edit catalog_uri in the TOML first)
cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-dvol.toml
```

Optional slim binary:  
`cargo build -p catalog-capture-cli --no-default-features --features venue-deribit`

## Starter profiles

| Intent | Config |
|--------|--------|
| Minimal validate/run | `examples/capture.toml` |
| Deribit DVOL (subscribe custom) | `examples/capture.deribit-dvol.toml` |
| Deribit book summary (request custom) | `examples/capture.deribit-btc-book-summary.toml` |
| Binance perp WS | `examples/capture.binance-perp.ws.toml` |
| Hyperliquid open interest | `examples/capture.hyperliquid-open-interest.toml` |
| Unattended / long-running | `examples/operator/*.toml` |

Option-universe matrix (by intent):

| Intent | Deribit | Bybit | OKX |
|--------|---------|-------|-----|
| Rolling live | `capture.deribit-btc-universe-autorefresh.toml` | `capture.bybit-btc-universe-autorefresh.toml` | `capture.okx-btc-universe-autorefresh.toml` |
| Research | `capture.deribit-btc-universe-research.toml` | `capture.bybit-btc-universe.toml` | `capture.okx-btc-universe.toml` |
| OI-ranked | `capture.deribit-btc-universe-oi-ranked-autorefresh.toml` | `capture.bybit-btc-universe-oi-ranked.toml` | `capture.okx-btc-universe-oi-ranked.toml` |
| Full chain | `capture.deribit-btc-universe-all.toml` | `capture.bybit-btc-universe-all.toml` | `capture.okx-btc-universe-all.toml` |

All profiles use `layout_compatibility = "rust_canonical_only"` (or the default).

## Prove capture is backtest-readable (Rust)

Offline unit tests write with the capture sink then load with Nautilus
`ParquetDataCatalog` (same pattern as research backtests such as `cjp_mm_rs`):

```bash
cargo test -p catalog-capture-core --lib catalog_layout
```

Especially:

- `write_quotes_then_query_with_parquet_data_catalog` — write quotes → `quote_ticks()` readback  
- custom path tests — `data/custom/{TypeName}/…`

How-to: [docs/how_to/rust_backtest_from_catalog.md](../docs/how_to/rust_backtest_from_catalog.md)

## Live / Python probes (optional)

Network smokes and PyO3 probes under `tests/` are **not** product binaries; they
validate live venues or optional Python readback. See
[docs/how_to/smoke_and_soak.md](../docs/how_to/smoke_and_soak.md).

## Layout policy

| Path | Role |
|------|------|
| `examples/*.toml` | Capture configs for `catalog-capture-cli` |
| `examples/operator/` | Unattended profiles |
| `dev/legacy-examples/` | Demoted cargo examples — not in default build |
