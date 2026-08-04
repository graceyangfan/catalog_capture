# Build size and compile speed

## Why tens of GB appear

Path dependencies pull a large **Nautilus Trader** crate graph into **Cargo’s
`target/`**. Space is usually:

| Location | Typical cause |
|----------|----------------|
| `../nautilus_trader/target/debug` | Building/testing full NT workspace |
| `…/debug/deps` | All rlib/object files for the graph |
| `…/debug/incremental` | Incremental caches (many GB) |
| `…/nextest` | Nextest workspaces |
| `catalog_capture/target` | Building this CLI (same graph when you `cargo build` here) |

On a machine that also develops Nautilus, **64GB+ under `nautilus_trader/target`
is normal** if `debug` + `incremental` + `nextest` accumulated. That is **not**
the size of the final `catalog-capture-cli` binary (usually tens of MB).

```bash
du -sh target ../nautilus_trader/target
du -sh ../nautilus_trader/target/* | sort -hr | head
```

## Free disk now

```bash
# This repo only
make clean
# or: rm -rf target

# Also wipe sibling NT build cache (largest win if you don't need it)
make clean-all-targets
# or: rm -rf ../nautilus_trader/target
```

Do **not** run `cargo build` / `cargo test` / nextest on the full
`nautilus_trader` workspace on a capture-only server unless required.

## Smallest + practical cloud binary (recommended)

Only link venues you capture (Binance + Deribit + Hyperliquid):

```bash
cd catalog_capture
make bootstrap-deps

# Fast release, smaller graph than all-venues
make build-release-capture
# → target/release/catalog-capture-cli

# Even smaller binary (slower compile)
make build-release-small
```

Equivalent:

```bash
cargo build --release -p catalog-capture-cli \
  --no-default-features \
  --features venue-binance,venue-deribit,venue-hyperliquid
```

`./scripts/run-mainnet-capture.sh` already uses these features.

## Faster compile on a server

```bash
# One-shot release: skip incremental caches
export CARGO_INCREMENTAL=0

# Use all cores
export CARGO_BUILD_JOBS="$(nproc 2>/dev/null || sysctl -n hw.ncpu)"

# Optional: faster linker (Linux)
# sudo apt-get install -y mold
# RUSTFLAGS="-C link-arg=-fuse-ld=mold"

make build-release-capture
```

Avoid on capture servers:

- `cargo test` with default (pulls test profile + more deps)
- Building whole `nautilus_trader` workspace
- `cargo doc` / nextest unless needed

## Profile notes (this repo)

| Profile | Role |
|---------|------|
| `dev` | `debug=false`, strip debuginfo |
| `test` | `debug=false` (was true → fat test artifacts) |
| `release` | `strip=symbols`, no LTO, `codegen-units=16` (faster link) |
| `release-small` | `opt-level=s`, thin LTO, `codegen-units=1` |

## Expected sizes (order of magnitude)

| Artifact | Order |
|----------|--------|
| `target/release/catalog-capture-cli` | tens of MB |
| Fresh `target/` after one slim release | several GB (deps once) |
| NT full workspace debug + nextest | tens of GB |

First compile is dominated by compiling Nautilus path deps; later rebuilds of
*this* crate alone are much smaller/faster if `target/` is kept.
