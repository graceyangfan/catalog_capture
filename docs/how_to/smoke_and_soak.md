# Smoke and soak validation

## Smoke (per venue, ~30–120s)

```bash
python3 tests/probe_option_universe_smoke.py --venue deribit-autorefresh --seconds 30 --cleanup
python3 tests/probe_option_universe_smoke.py --venue okx-autorefresh --seconds 30 --cleanup
python3 tests/probe_option_universe_smoke.py --venue bybit-autorefresh --seconds 30 --cleanup
```

## Soak presets

```bash
# Daily-live: Deribit + OKX + Bybit autorefresh
python3 tests/probe_option_universe_soak.py --preset daily-live --seconds 180 --cleanup

# Longer rolling-live (optional refresh-change assertion)
python3 tests/probe_option_universe_soak.py --preset daily-live --seconds 7200 --require-refresh-change
```

Use `--require-refresh-change` only on long runs where ATM strike drift is expected.

See [live-validation.md](../live-validation.md) for full probe matrices and flags.