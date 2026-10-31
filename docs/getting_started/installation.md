# Installation

## Layout

Path dependencies expect a sibling Nautilus Trader tree:

```text
~/work/nautilus_trader_quantity_commission   # adapters + persistence
~/work/catalog_capture   # this repo
```

## Bootstrap

```bash
make bootstrap-deps          # recommended: resolve sibling + checkout CI pin
# make bootstrap-deps-local  # resolve sibling only (no pin checkout)
```

1. Uses `NAUTILUS_TRADER_PATH` if set, else existing `../nautilus_trader_quantity_commission`.
2. If missing, clones `graceyangfan/nautilus_trader` into `../nautilus_trader_quantity_commission`.
3. `make bootstrap-deps` checks out the **CI-pinned** revision (`--pin-ci`).
4. Runs a workspace check.

First-time / reproducible builds should use the pin. Use `bootstrap-deps-local` only
when you intentionally develop against a different Nautilus checkout.

### Pinned Nautilus Trader revision (CI)

| Catalog Capture | Nautilus Trader ref |
|-----------------|---------------------|
| 0.1.x / main | `abdaf5684afea9bc4f883b5d05b1f708de58c054` |

## Dependency contract

`NAUTILUS_TRADER_REF` in `.github/workflows/ci.yml` and `scripts/bootstrap-deps.sh`
select the same immutable project-fork commit. It merges official develop with
inventory-unit commissions, exact gross execution quantities, unresolved command
retention and terminal quantity normalization. Predict uses these Native APIs
directly; an unmodified upstream checkout is not interchangeable.
The current pin includes official develop `288990482b312d18cf17b9c80b0d5159d3b591c6`
(NautilusTrader 0.68.0).

See [quantity commissions](../reference/quantity-commission.md) for supported
account/fee semantics and [validation](../reference/predict-validation.md) for checks.

## Toolchain

Rust **1.99.1** (`rust-toolchain.toml`):

```bash
rustup toolchain install 1.99.1
rustup component add rustfmt clippy --toolchain 1.99.1
```

## Build

```bash
# Standard public profile (Binance + Deribit + Hyperliquid + Lighter)
make build-release-capture
# Deployable binary: ./bin/catalog-capture-cli (target/ is only build cache)

# Optional: create a self-contained cloud deployment archive
./scripts/package-cloud.sh --config examples/capture.multi-venue-mainnet.toml

# All venues
make build-release

# Single venue (debug)
make build-slim FEATURES=venue-deribit

# Smaller binary (slower compile)
make build-release-small
```

Venue features: `venue-binance`, `venue-bybit`, `venue-deribit`, `venue-okx`,
`venue-hyperliquid`, `venue-lighter`, `venue-lighter-ext`, `venue-extended`, `venue-predict`, or
`all-venues`. A slim binary must include every venue referenced by its TOML:

```bash
CAPTURE_FEATURES=venue-binance,venue-predict make build-release-capture
```

`examples/*.toml` are CLI configs, not cargo examples.

### Disk / clean

Path deps make `target/` large (see [build size](../how_to/build_size.md)).

```bash
make clean                 # this repo target/
make clean-all-targets     # + ../nautilus_trader_quantity_commission/target
```

## Python (optional)

Live probes under `tests/` may need PyO3 from the same Nautilus revision.
Not required for the Rust product path.
