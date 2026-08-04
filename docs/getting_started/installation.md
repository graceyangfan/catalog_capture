# Installation

## Layout

Path dependencies expect a sibling Nautilus Trader tree:

```text
~/work/nautilus_trader   # adapters + persistence
~/work/catalog_capture   # this repo
```

## Bootstrap

```bash
make bootstrap-deps
```

1. Uses `NAUTILUS_TRADER_PATH` if set, else existing `../nautilus_trader`.
2. If missing, clones `nautechsystems/nautilus_trader` **develop** into `../nautilus_trader`.
3. Runs a workspace check.

Match CI pin:

```bash
./scripts/bootstrap-deps.sh --pin-ci
```

### Pinned Nautilus Trader revision (CI)

| Catalog Capture | Nautilus Trader ref |
|-----------------|---------------------|
| 0.1.x / main | `a7159b484e816a8b73388ff58db71de454253222` |

Source of truth: `NAUTILUS_TRADER_REF` in `.github/workflows/ci.yml`.

## Toolchain

Rust **1.97.1** (`rust-toolchain.toml`):

```bash
rustup toolchain install 1.97.1
rustup component add rustfmt clippy --toolchain 1.97.1
```

## Build

```bash
# Recommended for multi-venue mainnet capture (Binance + Deribit + Hyperliquid only)
make build-release-capture

# All venues
make build-release

# Single venue (debug)
make build-slim FEATURES=venue-deribit

# Smaller binary (slower compile)
make build-release-small
```

Venue features: `venue-binance`, `venue-bybit`, `venue-deribit`, `venue-okx`,
`venue-hyperliquid`, or `all-venues`.

`examples/*.toml` are CLI configs, not cargo examples.

### Disk / clean

Path deps make `target/` large (see [build size](../how_to/build_size.md)).

```bash
make clean                 # this repo target/
make clean-all-targets     # + ../nautilus_trader/target
```

## Python (optional)

Live probes under `tests/` may need PyO3 from the same Nautilus revision.
Not required for the Rust product path.
