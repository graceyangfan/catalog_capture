# Examples

TOML configs for `catalog-capture-cli` only (not cargo examples).
Run from the **repository root**. Catalog roots use `file://./data/…` (gitignored).

The directory contains three deliberate classes:

- long-running operator profiles (`capture.*` rows marked production below);
- focused protocol/reference profiles used by one adapter or data family;
- finite smoke fixtures referenced by `tests/probe_*.py`.

Do not copy a finite smoke duration or a hard-coded instrument into an
unattended deployment. Use dynamic universe profiles for expiring options.

## Rotation convention

- Profiles without an explicit seal override use the production default:
  **daily at 06:00 UTC**.
- High-rate native L2 profiles use **hourly UTC segments** with a managed
  snapshot at each boundary so each hour can be replayed independently.
- `capture.multi-venue-mainnet.toml` explicitly uses hourly UTC segments for
  HIP-4, Binance, and Deribit; daily HIP-4 instrument rotation remains independent.
- Predict rolling products seal each selector when its successor's first valid
  snapshot confirms the handover at/after the window boundary;
  a combined Predict/Binance profile therefore has independent 5m/15m custom
  segments and hourly Binance segments.
- `*-smoke.toml` and finite `capture_seconds` profiles are validation assets,
  not unattended production defaults.

Layout is Nautilus Rust `ParquetDataCatalog` only — see
[docs/concepts/catalog_layout.md](../docs/concepts/catalog_layout.md).

## Recommended production profiles

```bash
make build-release-capture

./bin/catalog-capture-cli validate \
  --config examples/capture.multi-venue-mainnet.toml

./scripts/run-mainnet-capture.sh
```

| Config | Required features | Content |
|--------|-------------------|---------|
| **`capture.multi-venue-mainnet.toml`** | standard `make build-release-capture` set | Public multi-venue instruments, quotes, trades, L2 and BookSummary |
| **`capture.predict-crypto-updown.toml`** | `venue-binance,venue-predict` | BTC/ETH/BNB 5m/15m Predict YES snapshots plus matching hourly Spot SBE L2/trades; separate from HIP4, no Futures |
| **`capture.binance-lighter-btc-sol-perp-books.toml`** | `venue-binance,venue-lighter` | High-rate BTC/SOL L2 and trades; replayable hourly segments |
| **`capture.binance-spot-sbe-btc.toml`** | `venue-binance` | Binance Spot SBE BTC/USDT full L2 + trades; credentials required |
| **`capture.predict-binance-spot-sbe-btc-updown.toml`** | `venue-binance,venue-predict` | Predict BTC 5m/15m plus Binance Spot SBE L2/trades |
| **`capture.binance-lighter-extended-btc-sol-perp-books.toml`** | `venue-binance,venue-lighter,venue-extended` | Binance/Lighter/Extended L2, trades and price/funding data |
| **`capture.lighter-xemm-openai-anthropic.toml`** | `venue-lighter` | Lighter + Robinhood Chain XEMM research feeds |
| **`capture.extended-btc-perp.toml`** | `venue-extended` | Extended RPC v2 BTC perpetual public data |
| `capture.hyperliquid-hip4-btc-daily.toml` | `venue-binance,venue-hyperliquid` | BTC rolling daily binary universe plus hourly BTC USD-M perpetual L2 (`depth=50`, `@depth@0ms`)/trades; no Spot SBE |
| `capture.deribit-btc-book-summary.toml` | `venue-deribit` | Deribit BookSummary request stream |

For a nonstandard profile, build the same binary with an explicit feature set:

```bash
CAPTURE_FEATURES=venue-binance,venue-predict make build-release-capture
```

## Focused reference profiles

| Intent | Config |
|--------|--------|
| Minimal validate | `capture.toml` |
| Deribit DVOL (subscribe custom) | `capture.deribit-dvol.toml` |
| Binance perp WS | `capture.binance-perp.ws.toml` |
| Hyperliquid OI | `capture.hyperliquid-open-interest.toml` |
| Minimal Predict BTC 5m selector | `capture.predict-orderbook.toml` |
| Operator / unattended option universe | `operator/*.toml` |

Finite Binance/Hyperliquid bar, trade, custom-data, and HIP-4 smoke profiles
remain at the top level because the corresponding `tests/probe_*.py` programs
load them directly. They are regression assets, not additional product entrypoints.

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
