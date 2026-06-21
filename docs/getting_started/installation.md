# Installation

## Layout

Clone this repository and its path dependencies as siblings:

```text
~/nautilus_trader          # persistence and adapter libraries
~/nautilus_catalog_capture # this project
```

`Cargo.toml` path dependencies expect `../nautilus_trader`.

## Rust toolchain

Install Rust **1.96.0** (see `rust-toolchain.toml`):

```bash
curl https://sh.rustup.rs -sSf | sh
rustup toolchain install 1.96.0
rustup component add rustfmt clippy --toolchain 1.96.0
```

## Build

`rust-toolchain.toml` pins Rust 1.96.0 for this repository, so plain `cargo` commands work
from the repo root:

```bash
cd nautilus_catalog_capture
cargo build -p catalog-capture-cli
```

Release binary:

```bash
make build-release
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