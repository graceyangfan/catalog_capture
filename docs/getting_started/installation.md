# Installation

## Layout

Clone this repository and its path dependencies as siblings:

```text
~/nautilus_trader          # persistence and adapter libraries
~/nautilus_catalog_capture # this project
```

`Cargo.toml` path dependencies expect `../nautilus_trader`.

### Bootstrap sibling dependency (recommended)

From this repo, prefer an existing local checkout; only clone if missing:

```bash
cd nautilus_catalog_capture
make bootstrap-deps
# or: ./scripts/bootstrap-deps.sh
```

Behavior:

1. Use `NAUTILUS_TRADER_PATH` if set, else `../nautilus_trader` when it already exists.
2. If neither is present, clone  
   `https://github.com/nautechsystems/nautilus_trader` **branch `develop`** into `../nautilus_trader`.
3. Run `cargo check -p catalog-capture-core --lib` to verify the graph.

Optional:

```bash
# Match CI pin after resolve (see NAUTILUS_TRADER_REF in .github/workflows/ci.yml)
./scripts/bootstrap-deps.sh --pin-ci

# Point at an existing checkout elsewhere (creates sibling symlink for Cargo)
NAUTILUS_TRADER_PATH=/path/to/nautilus_trader ./scripts/bootstrap-deps.sh
```

## Rust toolchain

Install Rust **1.97.1** (see `rust-toolchain.toml`):

```bash
curl https://sh.rustup.rs -sSf | sh
rustup toolchain install 1.97.1
rustup component add rustfmt clippy --toolchain 1.97.1
```

## Build (single product binary)

This project ships **one** product binary, `catalog-capture-cli` (same pattern as
Nautilus Trader’s `nautilus` CLI). Libraries are not binaries.

```bash
cd nautilus_catalog_capture
make build
# or: cargo build -p catalog-capture-cli
```

Default cargo features enable **all venues** (`all-venues`). For a slimmer adapter graph
(e.g. Deribit-only research laptop):

```bash
cargo build -p catalog-capture-cli --no-default-features --features venue-deribit
```

Venue features: `venue-binance`, `venue-bybit`, `venue-deribit`, `venue-okx`,
`venue-hyperliquid`, or `all-venues`. A venue kind in TOML that was not compiled in
fails at config load with a clear feature-rebuild message.

Release:

```bash
make build-release
```

Do **not** use `cargo build --examples` for normal work. TOML samples under `examples/`
are configs for the CLI, not cargo example crates.

## Sibling `nautilus_trader` revision

- **Local / bootstrap default:** keep your existing tree, or clone **`develop`** when missing.
- **CI:** pins `NAUTILUS_TRADER_REF` in `.github/workflows/ci.yml` (currently
  `a7159b484e816a8b73388ff58db71de454253222`) so builds are reproducible.

To align a bootstrap clone with CI after the first setup:

```bash
./scripts/bootstrap-deps.sh --pin-ci
```

## Python (optional, for readback probes)

Live validation probes use PyO3 `ParquetDataCatalog` from the Python environment built
alongside the sibling dependency checkout. Use the same checkout revision for Rust and
Python validation.

## Development tools

```bash
pip install pre-commit
pre-commit install
make install-tools       # installs cargo-deny
```

See [environment setup](../developer_guide/environment_setup.md) for the full workflow.
