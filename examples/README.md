# Examples

TOML configs for `catalog-capture-cli` only (not cargo examples).
Run from the **repository root**. Catalog roots use `file://./data/…` (gitignored).

## Rotation convention

- General, option-universe, custom-data, and multi-venue profiles omit an explicit
  seal override and use the production default: **daily at 06:00 UTC**.
- The Binance/Lighter L2 profile is the deliberate exception: **hourly UTC
  segments** with a managed snapshot at each boundary so each hour can be replayed
  independently.
- `*-smoke.toml`, `*-seal-quick.toml`, and `capture.low-threshold.toml` are test
  profiles only and are not unattended production defaults.

Layout is Nautilus Rust `ParquetDataCatalog` only — see
[docs/concepts/catalog_layout.md](../docs/concepts/catalog_layout.md).

## Recommended (mainnet)

```bash
make build-release-capture

./bin/catalog-capture-cli validate \
  --config examples/capture.multi-venue-mainnet.toml

./scripts/run-mainnet-capture.sh
# or: ./scripts/run-mainnet-capture.sh examples/capture.multi-venue-mainnet.toml
```

| Config | Content |
|--------|---------|
| **`capture.multi-venue-mainnet.toml`** | Multi-venue instruments, quotes, trades and order-book data |
| **`capture.binance-lighter-btc-sol-perp-books.toml`** | High-rate Binance/Lighter L2 books and trades, with self-contained hourly UTC segments |
| **`capture.binance-spot-sbe-btc.toml`** | Binance Spot SBE BTC/USDT full L2 + trades, with independently replayable hourly UTC segments |
| **`capture.predict-binance-spot-sbe-btc-updown.toml`** | Canonical Predict BTC 5m/15m Up/Down YES snapshots with own interval seals plus Binance Spot SBE L2/trades hourly |
| **`capture.binance-lighter-extended-btc-sol-perp-books.toml`** | Binance/Lighter BTC/SOL L2 and trades plus Extended SOL L2, trades, funding and mark/index prices |
| **`capture.lighter-xemm-openai-anthropic.toml`** | Lighter + Robinhood Chain OPENAI/ANTHROPIC L2, trades, funding, mark and index prices |
| **`capture.extended-btc-perp.toml`** | Extended public RPC v2 full L2, trades, mark/index prices and applied funding |
| `capture.hyperliquid-hip4-btc-daily.toml` | Hyperliquid universe only + 06:00 UTC seal |
| `capture.deribit-btc-book-summary.toml` | Deribit BookSummary only (`interval_secs = 1`) |

## Other starters

| Intent | Config |
|--------|--------|
| Minimal validate | `capture.toml` |
| Deribit DVOL (subscribe custom) | `capture.deribit-dvol.toml` |
| Binance perp WS | `capture.binance-perp.ws.toml` |
| Hyperliquid OI | `capture.hyperliquid-open-interest.toml` |
| Operator / unattended option universe | `operator/*.toml` |

## Option universe (Deribit / Bybit / OKX)

| Intent | Naming |
|--------|--------|
| Rolling | `*-universe-autorefresh.toml` |
| Research | `*-universe-research.toml` / `*-universe.toml` |
| OI-ranked | `*-oi-ranked*.toml` |
| Full chain | `*-universe-all.toml` |

## Offline layout proof

```bash
cargo test -p catalog-capture-core --lib catalog_layout
```

## Cleanup

```bash
./scripts/cleanup-tmp-captures.sh          # ./data
./scripts/cleanup-tmp-captures.sh /tmp     # smoke leftovers
```
