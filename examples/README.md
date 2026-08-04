# Examples

TOML configs for `catalog-capture-cli` only (not cargo examples).
Run from the repo root. Default `catalog_uri` values use `file://./data/…`.

```bash
cargo run -p catalog-capture-cli -- validate --config examples/capture.toml
cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-dvol.toml
```

## Starters

| Intent | Config |
|--------|--------|
| Minimal | `capture.toml` |
| Deribit DVOL | `capture.deribit-dvol.toml` |
| Deribit book summary (request) | `capture.deribit-btc-book-summary.toml` |
| Binance perp WS | `capture.binance-perp.ws.toml` |
| Hyperliquid OI | `capture.hyperliquid-open-interest.toml` |
| Unattended | `operator/*.toml` |

## Option universe

| Intent | Deribit | Bybit | OKX |
|--------|---------|-------|-----|
| Rolling | `*-universe-autorefresh.toml` | same pattern | same |
| Research | `*-universe-research.toml` / `*-universe.toml` | … | … |
| OI-ranked | `*-oi-ranked*.toml` | … | … |
| Full chain | `*-universe-all.toml` | … | … |

## Prove layout (offline)

```bash
cargo test -p catalog-capture-core --lib catalog_layout
```

See [docs/how_to/rust_backtest_from_catalog.md](../docs/how_to/rust_backtest_from_catalog.md).
