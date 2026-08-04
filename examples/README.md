# Examples

TOML configs for `catalog-capture-cli` only (not cargo examples).
Run from the **repository root**. Catalog roots use `file://./data/…` (gitignored).

Layout is Nautilus Rust `ParquetDataCatalog` only — see
[docs/concepts/catalog_layout.md](../docs/concepts/catalog_layout.md).

## Recommended (mainnet)

```bash
make build-release-capture

./target/release/catalog-capture-cli validate \
  --config examples/capture.multi-venue-mainnet.toml

./scripts/run-mainnet-capture.sh
# or: ./scripts/run-mainnet-capture.sh examples/capture.multi-venue-mainnet.toml
```

| Config | Content |
|--------|---------|
| **`capture.multi-venue-mainnet.toml`** | HL rolling universe (quotes/trades/mark) + Binance L2 d20 + trades + Deribit BookSummary 1s |
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
