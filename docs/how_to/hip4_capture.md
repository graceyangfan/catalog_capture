# HIP-4 capture for live strategies

How Catalog Capture should be configured for Hyperliquid **HIP-4 BTC daily**
(`priceBinary` / `1d`), based on:

| Project | Role |
|---------|------|
| `polyup_deribit_rs` (`Hip4BtcDailyStrategy`) | Live paper/live edge on YES/NO + HL mark + Deribit surface |
| `hyperliquid_stale_quote` / `cjp_mm_rs` | Research replay: outcome L1 + optional Binance alpha |

## What each strategy consumes

### polyup_deribit_rs (`strategy_hip4`)

| Stream | Instrument | Why |
|--------|------------|-----|
| **Quotes** | Active YES + NO `BinaryOption` | Entry/exit BBO, spread as cost (venue fee = 0) |
| **Mark prices** | `BTC-USD-PERP.HYPERLIQUID` | Spot / S for Deribit surface pricing |
| **Index** (optional) | `BTC-PERPETUAL.DERIBIT` | Fallback only if HL mark missing |
| **Custom / REST** | Deribit book summary | Volatility surface (not HIP-4 catalog) |
| **Instruments** | Cache from `outcomeMeta` | Discovery of next daily pair |

Strategy **does not** need outcome trades for the edge path; it is quote + mark driven.

### cjp_mm_rs / stale_quote research catalog

| Stream | Instrument | Why |
|--------|------------|-----|
| **Quotes** | YES/NO outcomes | L1 BBO |
| **Trades** | YES/NO outcomes | Execution / flow replay |
| **Trades + book deltas** | Binance perp | Alpha / L2 state (separate venue) |
| **Instruments** | Outcomes (+ mark perp) | Load keys for backtest |

`cjp_mm_rs` `require_layout` expects `data/{quotes,trades,order_book_deltas,instruments}/`.
HIP-4-only capture fills quotes/trades/instruments (+ mark under mark_prices).  
Binance L2 is **not** produced by `[[capture.hip4_universe]]` — run a Binance
profile (or multi-venue config) if you need that alpha path.

### stale_quote Python recorder (reference)

Also records `OrderBookDepth10`, raw `OutcomeMetaSnapshot`, and seals files with:

```text
RotationMode.SCHEDULED_DATES
rotation_interval = 1 day
rotation_time = 06:00 UTC
```

Catalog Capture maps that to **segment lifecycle seal** at `06:00` UTC.

## How they discover instruments and rotate

### polyup (cache-first)

1. HL adapter loads `outcomeMeta` → `BinaryOption` in the Nautilus cache  
   (strategy notes: connect-time load is not enough; **`request_instruments` every ~60s**).
2. `find_active_btc_daily`: scan cache for  
   `class=priceBinary`, `underlying=BTC`, `period ∈ {1d,daily,24h}`,  
   both YES and NO present, `expiry_ns > now`, pick **soonest expiry**.
3. On change: unsubscribe old quotes → subscribe new YES/NO; keep mark on BTC-PERP.

Incomplete pairs (only one side) are ignored — same discipline as capture should use.

### stale_quote / our `hip4_universe_refresh`

HTTP/outcomeMeta poll with **adaptive delay** (we already mirror this):

| Phase | When | Poll |
|-------|------|------|
| Idle | Far from expiry | `idle_poll_secs` (1800) |
| Approach | Within `pre_expiry_window_secs` (900) | Cap delay to window start |
| Active | Near / after expiry | `active_poll_secs` (10) |

This is **instrument universe rotation**, not Parquet file seal.

### Two clocks (do not conflate)

```text
Universe refresh  → which YES/NO (+ mark) we subscribe  (adaptive poll)
Segment seal      → when open .part files become day-bounded parquet  (06:00 UTC)
```

HIP-4 **daily** products expire on the **06:00 UTC** boundary. File seal should
use the same wall clock so each sealed day aligns with one contract day.

## Recommended Catalog Capture config

Use:

```bash
cargo run -p catalog-capture-cli -- run \
  --config examples/capture.hyperliquid-hip4-btc-daily.toml
```

That profile:

1. **Discovers** BTC `priceBinary` `1d` via `[[capture.hip4_universe]]`  
2. **Refreshes** with idle/active/pre-expiry polls + `purge_removed_instruments`  
3. **Records** `instruments`, `quotes`, `trades`, `mark_prices` on the active set  
4. **Seals** segments at **06:00 UTC** (`interval_secs = 86400`)

Output: `./data/hyperliquid-hip4-btc-daily/`.

### Minimal (polyup-shaped, no trades)

```toml
families = ["instruments", "quotes", "mark_prices"]
```

### Research / CJP-shaped (default daily example)

```toml
families = ["instruments", "quotes", "trades", "mark_prices"]
```

### Optional Deribit surface (polyup pricing)

Not part of HIP-4 universe expand. Add a second venue + custom request, or a
separate Deribit DVOL / book-summary config, if you need surface offline.

### Optional Binance alpha (cjp_mm_rs)

Separate `venue-binance` capture of trades + book deltas for the signal symbol;
merge catalogs only if your backtest loader supports multi-root or you co-locate
paths carefully.

## What we already learned and keep

| Idea | Source | Our status |
|------|--------|------------|
| Adaptive poll near expiry | stale_quote `rotation.py` | `next_rotation_delay_secs` + TOML |
| Require complete YES+NO | polyup discovery | Resolve only full markets |
| Periodic instrument refresh | polyup `request_instruments` | HIP-4 universe refresh loop |
| Daily file rotation 06:00 UTC | stale_quote StreamingConfig | segment seal on daily example |
| Quotes + mark for live edge | polyup | hip4 families |
| Outcome trades for research | cjp_mm_rs | `trades` family |

## Smoke

```bash
python3 tests/probe_hip4_smoke.py --seconds 60 --cleanup
# or short config:
cargo run -p catalog-capture-cli -- run --config examples/capture.hyperliquid-hip4-btc-smoke.toml
```
