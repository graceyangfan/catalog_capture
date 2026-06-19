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

## Output

The text output is designed for operator review:

- `expiry` is the selected option expiry in ISO8601.
- `atm_reference` is the venue HTTP reference price used for ATM-relative strike selection.
- `strikes` are the selected strikes after applying the configured strike policy.
- `perp` is the derived hedge/reference perpetual when `include_perp = true`.
- `options` are the option instruments expanded into the capture plan.
- `overlap` lists instruments already present in explicit `[[capture.*]]` entries or earlier universes.
- `new` lists instruments introduced by this universe.

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
