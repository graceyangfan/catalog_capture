# Environment setup

## Prerequisites

- Sibling dependency checkout (see [installation](../getting_started/installation.md))
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
cargo build -p catalog-capture-cli
```

## Daily workflow

```bash
make test          # unit tests
make fmt           # rustfmt
make clippy        # clippy -D warnings
make pre-commit    # all pre-commit hooks
```

Before opening a PR, ensure `make test`, `make clippy`, and `make cargo-deny` pass.

## Dependency version

Keep your local sibling checkout on the revision documented in project issues or your
team's deployment notes. CI uses `NAUTILUS_TRADER_REF` in `.github/workflows/ci.yml`
to pin the checkout used for builds.