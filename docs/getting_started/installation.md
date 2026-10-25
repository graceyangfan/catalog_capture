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
| 0.1.x / main | `c9bbbcd89421245fa86286caaa173c6524638461` |

Source of truth: `NAUTILUS_TRADER_REF` in `.github/workflows/ci.yml`.
This project-fork baseline includes official develop
`7cebe282353615d4f7b6fcdabde2911ba621516c` and the project's quantity-fee support.
Predict targets these Native APIs directly. See the
[upstream adaptation plan](../plans/predict-execution-lifecycle-refactor.md).

The workspace uses a pinned project fork. `execution` enables
its experimental inventory-fee mapping; this is not an unmodified upstream
develop build.

This pin contains the live reported-cash quantity-fee implementation discussed in
upstream RFC #5418. Predict execution uses the matching Native APIs directly. See the
[dependency contract](../reference/predict-execution-validation.md#two-repository-handoff).

## Toolchain

Rust **1.99.0** (`rust-toolchain.toml`):

```bash
rustup toolchain install 1.99.0
rustup component add rustfmt clippy --toolchain 1.99.0
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
