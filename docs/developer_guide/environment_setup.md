# Environment setup

## Prerequisites

- Sibling `../nautilus_trader` — [installation](../getting_started/installation.md)
- Rust 1.97.1 with rustfmt + clippy
- Optional network for live probes

## One-time

```bash
make bootstrap-deps
make install-tools
pip install pre-commit && pre-commit install
make build
```

## Daily

```bash
make test
make fmt
make clippy
make pre-commit
```

Before a PR: `make test`, `make clippy`, `make cargo-deny`.

## Dependency pin

| Mode | Behavior |
|------|----------|
| Local | Prefer existing sibling; bootstrap may clone `develop` |
| CI | Fixed `NAUTILUS_TRADER_REF` in `.github/workflows/ci.yml` |

```bash
./scripts/bootstrap-deps.sh --pin-ci
```
