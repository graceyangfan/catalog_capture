# Smoke and soak

Optional network validation. Product CI is Rust unit tests + clippy.

## Smoke (~30–120s)

```bash
python3 tests/probe_option_universe_smoke.py --venue deribit-autorefresh --seconds 30 --cleanup
python3 tests/probe_hip4_smoke.py --seconds 60 --cleanup
```

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
