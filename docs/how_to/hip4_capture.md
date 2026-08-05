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
3. **Unsubscribe** the previous plan, **bootstrap** new instruments.  
4. **Ready-then-subscribe** for quotes/trades/…: only after the new instrument is in
   the actor cache (`on_instrument`). If not ready yet, the id stays **pending** and
   we re-request with adaptive backoff (1s→…→60s poll only) until cache-ready or the
   next roll clears it — **no fixed wait-before-subscribe and no total-wait cap**.
   Work runs only while pending is non-empty (not all day).  
5. **Purge expired outcomes** when `purge_removed_instruments = true` (default): call
   Nautilus `Cache::purge_instrument`, which drops the definition **and** cache-owned
   quotes/trades/books/marks/… so RSS does not grow across daily rolls. Perp reference
   is kept.

### Option universe (Deribit / Bybit / OKX)

Same memory policy as HIP-4 after expiry / ATM / OI rolls:

- `[runtime.option_universe_refresh] purge_removed_instruments = true` (**default**)
- Unsub + forget runtime state, then `Cache::purge_instrument` on removed options
- Keeps **perp** and any id still in the active capture plan (static or other universes)
- **Does not** delete catalog parquet on disk

Unit tests cover HIP-4 roll defer → ready subscribe, HIP-4/option purge guards, and
selection/adaptive delay; live smoke still needs network.

## Deribit book summary rate

`get_book_summary_by_currency` is **one public HTTP call per currency** (not per strike).

| Setting | Recommendation |
|---------|----------------|
| Deribit non-matching public budget | ~**20 rps** class (shared with other REST) |
| Our floor | `interval_secs >= 1` |
| High-cadence capture | **`interval_secs = 1`**, `overlap_policy = "skip"`, `request_timeout_secs = 5` |
| Aggregate guard | Keep total request jobs ≲ ~2 rps unless you raise budget intentionally |

At 1s with a single BookSummary job, load is ~**1 rps** — headroom remains for other Deribit calls on the same IP.

### File layout (with `output.lifecycle.mode = segment`)

BookSummary is **custom data** but uses the **same segment lifecycle** as market series:

- Poll ~1 Hz → rows append into `data/custom/DeribitBookSummary/…/{open_ts}.parquet.part`
- **Not** one catalog parquet per second
- Seal at **06:00 UTC** (or shutdown) → official `{start}_{end}.parquet`

Default lifecycle is **`segment`** (`.part` + seal). Explicit `mode = "chunked"` is
**smoke only** — `validate` / `run` print a WARNING when custom data is also configured.
See [segment lifecycle](../concepts/segment_lifecycle.md).

## Two clocks (do not conflate)

| Clock | What it does | When |
|-------|----------------|------|
| **HIP-4 universe refresh** | HTTP outcomeMeta → new YES/NO set → **unsubscribe old / subscribe new** | Adaptive: idle ~1800s, near expiry ~10s |
| **Segment seal** | Seal active segments to Nautilus `{start}_{end}.parquet` names | Wall clock **06:00 UTC** only |

**Not** all streams “rotate content” at 06:00. Only **file segments** seal at 06:00.
Outcome **subscriptions** roll when the next daily market appears (often near that boundary).

## Lightweight runtime (memory / CPU)

Capture node defaults (in code, inspired by lean live nodes):

| Knob | Value | Why |
|------|-------|-----|
| `CacheConfig.save_market_data` | `false` | Do not retain ticks in cache; parquet actor is the sink |
| `tick_capacity` / `bar_capacity` | 2000 / 64 | Bounded if anything is cached |
| Binance `instrument_provider` | `load_all=false` + plan `load_ids` | Avoid loading entire futures universe |
| HL `update_instruments_interval_mins` | 1 | Prefer fresh instrument defs while rolling |
| `flush_rows` / `max_buffer_*` | tight in multi-venue example | Bound capture queues under L2 load |
| Metrics HTTP | optional `runtime.metrics` | RSS + dropped_items without heavy tooling |

Monitor (when metrics enabled): `http://127.0.0.1:9108/metrics` — watch
`catalog_capture_process_rss_bytes`, `dropped_items`, `active_partitions`.

## Example configs (mainnet / live)

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

Catalog root is always local (`file://./data/…`).
