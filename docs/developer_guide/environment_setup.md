# Environment setup

## Prerequisites

- Sibling dependency checkout (see [installation](../getting_started/installation.md))
- Rust **1.97.1** with `rustfmt` and `clippy` (`rust-toolchain.toml`)
- Network access for live smoke/soak probes

## One-time setup

```bash
cd nautilus_catalog_capture
rustup toolchain install 1.97.1
rustup component add rustfmt clippy --toolchain 1.97.1
make bootstrap-deps   # prefer local ../nautilus_trader; else clone upstream develop
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

| Mode | Behavior |
|------|----------|
| Day-to-day / bootstrap | Prefer local `../nautilus_trader`; if missing, clone **develop** via `make bootstrap-deps` |
| CI / reproducible builds | Fixed `NAUTILUS_TRADER_REF` in `.github/workflows/ci.yml` |

Current CI pin: `a7159b484e816a8b73388ff58db71de454253222`.

```bash
# One-shot align with CI after bootstrap
./scripts/bootstrap-deps.sh --pin-ci
```

Do **not** let CI float on `develop` tip; local research clones may track develop, then pin when validating against CI.
