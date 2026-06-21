# Smoke and soak validation

Validation is tiered by VM size and capture profile. Complete **Track R1/R2** before claiming
**heavy** profiles (full-chain + `book_deltas`) on small VMs. See `ROADMAP.md` (Track R) and
`docs/stepwise-capture-roadmap.md`.

## Smoke (per venue, ~30–120s)

```bash
python3 tests/probe_option_universe_smoke.py --venue deribit-autorefresh --seconds 30 --cleanup
python3 tests/probe_option_universe_smoke.py --venue okx-autorefresh --seconds 30 --cleanup
python3 tests/probe_option_universe_smoke.py --venue bybit-autorefresh --seconds 30 --cleanup
python3 tests/probe_hip4_smoke.py --seconds 60 --cleanup
```

After code changes: `cargo test` and pre-commit before any soak longer than a few minutes.

## Soak profiles

| Profile | VM | Example config | Duration | Scope |
|---------|-----|----------------|----------|-------|
| rolling | 4C8G | single-venue autorefresh, small strike | 2h+ | no `book_deltas`, no full-chain |
| research | 4C16G | `daily-live` preset or multi-venue rolling | 2h–24h | trades/bars OK; defer full-chain |
| segment | 4C16G | `examples/capture.hyperliquid-perp-daily.toml` | 2h+ (cross one seal) | segment seal + readback |
| heavy | 8C+ **after R1/R2** | `capture.*-btc-universe-all.toml` + selective depth | 4h+ | full-chain; monitor RSS |

Do not run **heavy** unattended on 4C8G until you tune `output.max_buffer_bytes`,
`output.max_total_buffer_bytes` (per-family cap), and `runtime.resource_budget_bytes` so the
startup buffer estimate fits the VM. R1 enforces per-family caps; summed peak ≈
`families × max_total_buffer_bytes`.

## Option universe soak presets

```bash
# Short integration (research VM)
python3 tests/probe_option_universe_soak.py --preset daily-live --seconds 180 --cleanup

# Longer rolling-live (optional refresh-change assertion)
python3 tests/probe_option_universe_soak.py --preset daily-live --seconds 7200 --require-refresh-change
```

Use `--require-refresh-change` only on long runs where ATM strike drift is expected.

## Segment seal soak

```bash
cargo run -p catalog-capture-cli -- run --config examples/capture.hyperliquid-perp-daily.toml
# After seal boundary or shutdown:
python3 tests/probe_segment_seal_readback.py /path/to/catalog BTC-USD-PERP.HYPERLIQUID
```

HIP-4 daily example: `examples/capture.hyperliquid-hip4-btc-daily.toml`

## Acceptance criteria (Track R4)

Record these during soak; treat failures as release blockers for unattended profiles.

| Signal | Pass (rolling / research) | Notes |
|--------|---------------------------|-------|
| `dropped_items` | 0 (or documented profile cap) | `GET /metrics` when `runtime.metrics.enabled = true` |
| `active_partitions` | stable or bounded after warm-up | spikes at universe refresh are OK if they decay |
| Process RSS | below VM budget with headroom | compare start vs 2h mark |
| Seal / flush | sealed files appear on schedule | segment mode: readback probe passes |
| PyO3 readback | smoke or probe scripts green | same contract as backtest |

## What to watch

- `queued_items` — background queue backlog per family
- `active_partitions` — in-memory buffers not yet flushed
- `flush_reasons` — row / byte / interval / seal / shutdown mix
- Universe refresh cycles — temporary partition count bumps

## Optional offline validation (Step 9b)

After soak produces sealed catalog assets:

1. Run derive job on the same window (IV term, GEX, basis).
2. Confirm panels are reproducible from raw inputs only.
3. CPU-bound derive at scale may later use [storage-engine](https://github.com/wuledan/storage-engine)
   Offline workers in a **separate process** — not required for soak pass/fail.

See [live-validation.md](../live-validation.md) for full probe matrices and flags.