# Multi-venue capture (HIP-4 style universe + books)

Public **mainnet** market-data recording. One process can combine Hyperliquid
outcome rotation, Binance USD-M L2, and Deribit book-summary polls.

## Subscriptions (channel-level)

| Venue | Data | How Catalog Capture maps it |
|-------|------|-----------------------------|
| Hyperliquid | Instruments for active outcomes | `[[capture.hip4_universe]]` → `instruments` |
| Hyperliquid | BBO (YES/NO) | `quotes` → QuoteTick |
| Hyperliquid | Trade ticks (YES/NO) | `trades` → TradeTick |
| Hyperliquid | Mark on USD perp | `mark_prices` (when `include_perp_mark`) |
| Binance Futures | Trade ticks | `[[capture.trades]]` |
| Binance Futures | L2 deltas | `[[capture.book_deltas]]` `L2_MBP` **`depth = 20`** |
| Deribit | Book summary | `[[capture.custom_data_requests]]` `DeribitBookSummary` |

### Binance L2

Nautilus Binance Futures L2 opens the unthrottled **`{symbol}@depth@0ms`** stream.
`depth` is the **snapshot** level count (valid: 5, 10, 20, 50, 100, 500, 1000).
If omitted, the adapter defaults to **1000**. Research capture should set:

```toml
[[capture.book_deltas]]
instrument_id = "BTCUSDT-PERP.BINANCE"
book_type = "L2_MBP"
depth = 20
```

### HIP-4 style auto rotation

When `[runtime.hip4_universe_refresh]` is enabled:

1. Poll Hyperliquid `outcomeMeta` on an adaptive schedule  
   (idle far from expiry, faster near expiry).  
2. Resolve the next matching market (e.g. BTC `priceBinary` `1d`).  
3. **Unsubscribe** the previous plan, **bootstrap** new instruments, **subscribe** the new plan.  
4. Optionally purge removed instruments from cache.

Unit tests cover selection and adaptive delay; live smoke still needs network.

## Deribit book summary rate

`get_book_summary_by_currency` is **one public HTTP call per currency** (not per strike).

| Setting | Recommendation |
|---------|----------------|
| Deribit non-matching public budget | ~**20 rps** class (shared with other REST) |
| Our floor | `interval_secs >= 1` |
| High-cadence capture | **`interval_secs = 1`**, `overlap_policy = "skip"`, `request_timeout_secs = 5` |
| Aggregate guard | Keep total request jobs ≲ ~2 rps unless you raise budget intentionally |

At 1s with a single BookSummary job, load is ~**1 rps** — headroom remains for other Deribit calls on the same IP.

## Example configs (mainnet)

```bash
# Combined multi-venue
cargo run -p catalog-capture-cli -- run \
  --config examples/capture.multi-venue-mainnet.toml

# Hyperliquid universe only
cargo run -p catalog-capture-cli -- run \
  --config examples/capture.hyperliquid-hip4-btc-daily.toml

# Deribit book summary only (1s poll)
cargo run -p catalog-capture-cli -- run \
  --config examples/capture.deribit-btc-book-summary.toml
```

Catalog root is always local (`file://./data/…`). No system install.
