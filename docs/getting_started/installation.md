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
| 0.1.x / main | `1f3d1b8e079c1a50c7a93a8e97862f1f348d7670` |

Fork repository: [graceyangfan/nautilus_trader](https://github.com/graceyangfan/nautilus_trader),
branch `codex/quantity-commission-live`. CI checks out the immutable commit above.

## Dependency contract

`NAUTILUS_TRADER_REF` in `.github/workflows/ci.yml` and `scripts/bootstrap-deps.sh`
select the same immutable project-fork commit. It merges official develop with
inventory-unit commissions, exact gross execution quantities, unresolved command
retention and terminal quantity normalization. Predict uses these Native APIs
directly; an unmodified upstream checkout is not interchangeable.
The current pin includes official develop `83529d1cd11d8a23179cf52d70b013b831923a32`
(NautilusTrader 0.69.0).
Hyperliquid instrument-request completion and partial metadata refreshes use the
official implementation; the fork no longer carries separate patches for them.
Binance book replay timestamp ordering also follows official develop.

See [quantity commissions](../reference/quantity-commission.md) for supported
account/fee semantics and [validation](../reference/predict-validation.md) for checks.

## Toolchain

Rust **1.100.0** (`rust-toolchain.toml`):

```bash
rustup toolchain install 1.100.0
rustup component add rustfmt clippy llvm-tools --toolchain 1.100.0
```

## Project Cargo

This project uses **huacnlee/cargo**, pinned with release checksums in
`scripts/cargo.sh`. All Make targets, build/service scripts, hooks, and CI use
this entry point. From the repository root, run direct commands as
`./scripts/cargo.sh <command>`. When invoked by its absolute path from elsewhere,
the wrapper still operates from the workspace root.

```bash
make install-cargo
./scripts/cargo.sh --version
```

Bootstrap also installs it. The binary lives under `.tools/cargo/`; automatic
updates are disabled. Cache policy is in `.cargo/config.toml`. rustup supplies
the compiler, rustfmt, and Clippy; its system installation can remain available
to other repositories.

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

Build intermediates live in `~/.cargo/targets`; binaries remain in `target/`.
See [cache management](../how_to/build_size.md).

```bash
make cache-size            # local target + all central caches
make clean                 # this project's local and central build cache
```

## Python (optional)

Catalog readback probes under `tests/` use the Python extension from the same
pinned quantity-commission checkout. Build it with the fork's Python 3.13+ project
environment, then run probes with that environment's interpreter:

```bash
(cd ../nautilus_trader_quantity_commission && \
  PYO3_PYTHON="$PWD/python/.venv/bin/python" make build-debug)
../nautilus_trader_quantity_commission/python/.venv/bin/python \
  tests/python_catalog_probe.py <catalog-dir> <instrument-id>
```

The shared import helper selects `../nautilus_trader_quantity_commission/python`.
`NAUTILUS_TRADER_ROOT` can explicitly select another checkout or its `python/`
directory. Rebuild the extension after updating the fork; changing the Python
sources alone does not update compiled APIs. Rust capture does not require Python.
