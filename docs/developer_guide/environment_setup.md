# Environment setup

## Prerequisites

- Sibling `nautilus_trader` checkout (see [installation](../getting_started/installation.md))
- Rust 1.96.0 with `rustfmt` and `clippy`
- Network access for live smoke/soak probes

## One-time setup

```bash
cd nautilus_catalog_capture
rustup toolchain install 1.96.0
rustup component add rustfmt clippy --toolchain 1.96.0
make install-tools
pip install pre-commit
pre-commit install
cargo +1.96.0 build -p catalog-capture-cli
```

## Daily workflow

```bash
make test          # unit tests
make fmt           # rustfmt
make clippy        # clippy -D warnings
make pre-commit    # all pre-commit hooks
```

Before opening a PR, ensure `make test`, `make clippy`, and `make cargo-deny` pass.

## Forked nautilus_trader

Some capture features depend on patches in a forked `nautilus_trader` branch. Keep your
local sibling checkout on the branch documented in `README.md` or project issues. CI
checks out upstream `develop` by default; pin `NAUTILUS_TRADER_REF` in CI when testing
against a fork.