# Installation

## Layout

Clone both repositories as siblings:

```text
~/nautilus_trader
~/nautilus_catalog_capture
```

Path dependencies in `Cargo.toml` expect `../nautilus_trader`.

## Rust toolchain

Install Rust **1.96.0** (see `rust-toolchain.toml`):

```bash
curl https://sh.rustup.rs -sSf | sh
rustup toolchain install 1.96.0
rustup component add rustfmt clippy --toolchain 1.96.0
```

## Build

```bash
cd nautilus_catalog_capture
cargo +1.96.0 build -p catalog-capture-cli
```

Release binary:

```bash
make build-release
```

## Python (optional, for readback probes)

Live validation probes use Nautilus PyO3 bindings from your `nautilus_trader` Python
environment. Point probes at the same `nautilus_trader` checkout you built against.

## Development tools

```bash
pip install pre-commit   # or: cargo install prek --locked
pre-commit install
make install-tools       # installs cargo-deny
```

See [environment setup](../developer_guide/environment_setup.md) for the full workflow.