# HIP-4 capture (BTC daily)

Target: one Hyperliquid capture that serves both:

1. **polyup_deribit_rs** `Hip4BtcDailyStrategy` (live edge)  
2. **cjp_mm_rs** / research catalog (replay)

without running two overlapping HIP-4 recorders.

## Can discovery / rotation run correctly?

### What we do (capture side)

```text
timer → HTTP get_outcome_meta()
      → resolve BTC priceBinary daily (nearest unexpired question)
      → expand YES/NO (+ optional BTC-USD-PERP mark)
      → subscribe delta / unsubscribe removed
      → adaptive next poll (idle 1800s / active 10s / pre-expiry 900s)
```

| Piece | Status | Notes |
|-------|--------|--------|
| Fetch `outcomeMeta` | **Supported** | `HyperliquidRawHttpClient::get_outcome_meta` (same meta polyup needs) |
| Filter `priceBinary` + BTC + daily | **Supported** | Case-insensitive; period aliases `1d`/`daily`/`24h` (polyup-compatible) |
| Pick nearest future expiry | **Supported** | Unit-tested (`resolve_selects_nearest_future_question`) |
| YES+NO instrument ids | **Supported** | `{outcomeId}-YES-OUTCOME.HYPERLIQUID` / `…-NO-…` |
| Adaptive poll | **Supported** | Mirrors `hyperliquid_stale_quote/rotation.py` |
| Plan delta subscribe | **Supported** | Actor applies add/remove plans |
| Purge old instruments | **Supported** | `purge_removed_instruments = true` |
| File seal at 06:00 UTC | **Supported** | Segment lifecycle on daily example |

### vs polyup (verified live)

polyup discovers from the **Nautilus cache** after `request_instruments` every ~60s, and only binds when **both** YES and NO are already `BinaryOption` in cache.

We discover from **HTTP outcomeMeta** (stale_quote style), then subscribe by **canonical ids**. That is the right path for a **capture service** (we own the write plan; we do not need strategy cache scan).

| Risk | Severity | Mitigation |
|------|----------|------------|
| Venue not yet listing a brand-new id | Medium | Adaptive poll near expiry; smoke requires quote rows |
| Meta field spelling drift | Low | Loose class/underlying; daily period aliases |
| HTTP failure mid-day | Low | Keep previous plan on refresh error |
| Incomplete namedOutcomes | Medium | Empty namedOutcomes skipped; binary expands YES+NO from each id |

**Conclusion:** rotation logic is sound and unit-tested; live correctness still needs a short network smoke after deploy (`probe_hip4_smoke` or daily config for a few minutes). It is **not** the same code path as polyup’s cache scan, but it is the capture-appropriate equivalent and matches the already-proven stale_quote recorder design.

## What each strategy needs vs what we record

### polyup `hip4_btc_daily` (Hyperliquid leg)

| Need | Record? | Where |
|------|---------|--------|
| YES/NO **quotes** | **Yes** | `families` → `quotes` |
| BTC-PERP **mark** | **Yes** | `include_perp_mark` + `mark_prices` |
| **Instruments** (defs) | **Yes** | `instruments` |
| Deribit **index** fallback | Optional | Not in HIP-4 block (rarely needed if HL mark healthy) |
| Deribit **book summary** surface | Optional | Separate Deribit request capture if you want offline surface |

### cjp_mm_rs / research (Hyperliquid leg)

| Need | Record? | Where |
|------|---------|--------|
| YES/NO **quotes** | **Yes** | `quotes` |
| YES/NO **trades** | **Yes** | `trades` |
| **Instruments** | **Yes** | `instruments` |
| Binance **trades + book deltas** | **No** (by design) | Separate Binance config / catalog |

### Support matrix (HIP-4 HL only)

| Data | Supported in Catalog Capture HIP-4 |
|------|-------------------------------------|
| instruments | yes |
| quotes (outcomes) | yes |
| trades (outcomes) | yes |
| mark_prices (BTC-PERP) | yes |
| order_book_deltas (HL outcomes) | no (neither strategy requires HL L2 for core path) |
| Deribit surface | yes, via other examples (not hip4_universe) |
| Binance L2 alpha | yes, via Binance venue configs (not hip4_universe) |

## One capture for both strategies (no double HIP-4)

**Do not** run two processes both on `[[capture.hip4_universe]]` BTC daily — that duplicates WS and HTTP.

**Do** run **one** unattended HIP-4 job with the **union** of HL fields:

```bash
# From repo root — single process, single catalog
cargo run -p catalog-capture-cli -- run \
  --config examples/capture.hyperliquid-hip4-btc-daily.toml
```

That profile already unions:

```toml
families = ["instruments", "quotes", "trades", "mark_prices"]
```

| Consumer | Uses from this catalog |
|----------|-------------------------|
| polyup live / research load | instruments, quotes, mark_prices |
| cjp_mm_rs HIP-4 window | instruments, quotes, trades (+ mark if needed) |

Optional add-ons (separate processes / catalogs — **not** HIP-4 duplicates):

| Add-on | When | Example |
|--------|------|---------|
| Deribit book summary / DVOL | polyup surface offline | `capture.deribit-dvol.toml` or book-summary |
| Binance trades + book | cjp alpha | `capture.binance-perp.ws.toml` style |

Recommended layout:

```text
./data/hyperliquid-hip4-btc-daily/   # one HIP-4 day stream (shared)
./data/binance-alpha/                # optional, only if cjp needs L2
./data/deribit-surface/              # optional, only if surface offline
```

## Smoke

```bash
python3 tests/probe_hip4_smoke.py --seconds 60 --cleanup
# expects: discovery metadata + quote_rows + mark_rows
```

## Related

- Example: `examples/capture.hyperliquid-hip4-btc-daily.toml`  
- Segment seal (file day): [segment lifecycle](../concepts/segment_lifecycle.md)  
- Adaptive poll unit tests: `catalog-capture-core` `hip4::rollover`  
