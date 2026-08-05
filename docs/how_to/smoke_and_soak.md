# Smoke and soak

Optional network validation. Product CI is Rust unit tests + clippy.

## Smoke (~30–120s)

```bash
python3 tests/probe_option_universe_smoke.py --venue deribit-autorefresh --seconds 30 --cleanup
python3 tests/probe_hip4_smoke.py --seconds 60 --cleanup
```

**Defaults are segment** (production). For short smoke that forces immediate catalog files,
set `mode = "chunked"` explicitly — `validate` / `run` print a **WARNING** when custom data
is present. Prefer default segment for multi-venue / forever runs.

Prefer `purge_removed_instruments = true` under `[runtime.hip4_universe_refresh]`
for long HIP-4 runs.

## Soak profiles

| Profile | VM | Scope |
|---------|-----|--------|
| rolling | 4C8G | single-venue autorefresh; no full-chain book |
| research | 4C16G | longer rolling; defer full-chain |
| segment | 4C16G | cross one seal boundary |
| heavy | 8C+ | full-chain + depth; tune buffers first |

```bash
python3 tests/probe_option_universe_soak.py --preset daily-live --seconds 180 --cleanup
make smoke-soak   # 180s daily-live preset
```

## Pass signals

| Signal | Expect |
|--------|--------|
| `dropped_items` | 0 (or documented) |
| `active_partitions` | bounded after warm-up |
| RSS | under VM budget |
| request metrics | polls/rows grow; timeouts ≈ 0 |

Enable metrics in TOML (`runtime.metrics.enabled`). See [tests/README.md](../../tests/README.md).

## Multi-venue segment checklist (`capture.multi-venue-mainnet.toml`)

After rebuild + restart of `catalog-capture-cli`:

| Check | Expect |
|-------|--------|
| BookSummary disk | Growing `data/custom/DeribitBookSummary/**/*.parquet.part` — **not** a new sealed `.parquet` every second |
| L2 / trades / quotes | Same: open `*.parquet.part` under `data/order_book_deltas|trades|quotes|mark_prices/…` |
| Metrics | family `accepted` ↑; `flushed` ↑ within seconds (interval + row profile); `dropped_items=0` |
| 06:00 UTC / shutdown | Parts rename to `{start}_{end}.parquet`; new empty parts reopen when seal schedule is on |
| Failures | No permanent `background capture worker is not running` for custom |

```bash
# Example: watch BookSummary part growth
find ./data/multi-venue-mainnet/data/custom -name '*.parquet.part' -ls
curl -s http://127.0.0.1:9108/metrics | grep catalog_capture
```
