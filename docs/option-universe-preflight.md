# Option Universe Preflight

Option universe preflight resolves a logical options universe into the existing per-instrument
capture plan before the Nautilus live node starts. It is a 9a-lite operator feature: it removes
routine expiry/strike ID edits while keeping instruments, quotes, greeks, and related feeds as the
catalog source of truth.

It does not use `OptionChainSlice` as the primary record format. The output is still ordinary
per-instrument subscriptions that write the same parquet families as hand-written `[[capture.*]]`
entries.

## Commands

Validate the TOML without live metadata:

```bash
cargo run -p catalog-capture-cli -- validate --config examples/capture.okx-btc-universe.toml
```

Resolve and print the universe without starting capture:

```bash
cargo run -p catalog-capture-cli -- resolve-option-universe \
  --config examples/capture.okx-btc-universe.toml \
  --option-universe-format text
```

Dry-run the same resolution path used by `run`:

```bash
cargo run -p catalog-capture-cli -- run \
  --config examples/capture.okx-btc-universe.toml \
  --dry-run-resolve \
  --option-universe-format text
```

Print the resolved universe and then start capture:

```bash
cargo run -p catalog-capture-cli -- run \
  --config examples/capture.okx-btc-universe.toml \
  --print-option-universe \
  --option-universe-format text
```

When `run --print-option-universe` is used, the CLI reuses the same materialized plan for capture
instead of resolving the universe a second time.

Inspect the latest resolved state from an existing catalog:

```bash
cargo run -p catalog-capture-cli -- inspect-option-universe \
  --catalog-uri file:///tmp/nautilus-catalog-capture-deribit-btc-universe-autorefresh \
  --option-universe-format text
```

Validate the latest resolved universe against the catalog parquet families:

```bash
cargo run -p catalog-capture-cli -- validate-option-universe-catalog \
  --catalog-uri file:///tmp/nautilus-catalog-capture-deribit-btc-universe-autorefresh \
  --option-universe-format text \
  --preset rolling-autorefresh
```

Built-in presets:

| Preset | Use case |
|---|---|
| `post-capture` | Default post-run smoke: lineage + perp/options parquet rows |
| `rolling-autorefresh` | Autorefresh profiles: also require at least one refresh delta |
| `venue-trades` | Bybit/OKX profiles: also require perp trade parquet rows |
| `research` | Research profile: contract state + `BTC-PERPETUAL.DERIBIT-1-MINUTE-LAST-EXTERNAL` bars |

Explicit flags such as `--min-rows`, `--require-refresh-change`, or `--bar-type` override preset defaults.

After a normal `run`, when the profile includes `capture.option_universe`, the CLI now prints a
post-run report automatically:

1. `inspect-option-universe` lineage summary for the catalog just written
2. `validate-option-universe-catalog` using a preset inferred from the TOML profile

Disable with `--skip-post-run-report`, or override the inferred preset with
`--post-run-validation-preset rolling-autorefresh`.
```

## Output

The text output is designed for operator review:

- `expiry` is the selected option expiry in ISO8601.
- `atm_reference` is the venue HTTP reference price used for ATM-relative strike selection.
- `strikes` are the selected strikes after applying the configured strike policy.
- `perp` is the derived hedge/reference perpetual when `include_perp = true`.
- `options` are the option instruments expanded into the capture plan.
- `overlap` lists instruments already present in explicit `[[capture.*]]` entries or earlier universes.
- `new` lists instruments introduced by this universe.

For `inspect-option-universe`, the text output summarizes the persisted runtime lineage:

- `startup_at` is when the catalog first resolved that logical universe.
- `latest_event` is the most recent `startup` or `refresh` event.
- `refresh_count` shows how many runtime deltas were actually applied.
- `latest_rollover_reason` surfaces the last observed reason such as `expiry_roll` or `atm_drift`.
- `options` is the current resolved option member set from the latest lineage record.

For `validate-option-universe-catalog`, the CLI checks the latest resolved universe members against
the catalog itself:

- requires non-empty `metadata/forward_prices.jsonl`
- validates latest `perp` parquet families: quotes, mark prices, index prices, funding, and
  optionally trades
- validates latest option member parquet families: quotes, mark prices, option greeks
- optionally requires `instrument_status` / `instrument_closes`
- optionally requires at least one applied runtime refresh delta

## Venue Notes

Deribit universes require the venue `product_types` to include `option`. If `include_perp = true`,
they also require `future`; the derived perpetual is `{UNDERLYING}-PERPETUAL.DERIBIT`.

Bybit universes require `product_types = ["linear", "option"]` when `include_perp = true`, and a
`settlement_currency` such as `USDT`; the derived perpetual is `{UNDERLYING}{SETTLE}-LINEAR.BYBIT`.

OKX universes require `instrument_types = ["swap", "option"]` when `include_perp = true`, plus an
`instrument_families` entry like `BTC-USD`. In the option universe config,
`settlement_currency = "USD"` means the OKX family suffix used for discovery, not the parsed
Nautilus option settlement asset.

## Limits

V1 resolves once before the live node starts. It does not refresh, subscribe, or unsubscribe during
a running capture job. For rolling coverage, run this preflight on each scheduled capture start.

V1 uses venue HTTP metadata/ticker endpoints before startup rather than a post-connect Nautilus
cache manager. A future runtime manager can reuse the same logical universe model and add delta
subscription APIs, but that is intentionally outside this small preflight path.

`oi_ranked` startup resolution currently depends on venue HTTP access to per-contract option open
interest. At the moment:

- Bybit startup preflight supports `oi_ranked`
- Deribit runtime refresh supports `oi_ranked`, but startup preflight does not because the current
  Nautilus HTTP discovery path does not expose per-contract option OI
- OKX runtime refresh supports `oi_ranked`, but startup preflight does not because the current
  Nautilus HTTP discovery path does not expose per-contract option OI

For Deribit and OKX, use `atm_relative` or `all` for startup resolution if you need immediate live
startup today, then rely on runtime refresh once `option_greeks` warm up.
