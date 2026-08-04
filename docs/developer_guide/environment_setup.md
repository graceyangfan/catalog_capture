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
make build-release-capture   # or: make build (all-venues debug)
```

## Daily

```bash
make test
make fmt
make clippy
make pre-commit
```

Before a PR: `make test`, `make clippy`, `make cargo-deny`.

Disk: `make clean` or `make clean-all-targets` — see [build size](../how_to/build_size.md).

## Dependency pin

| Mode | Behavior |
|------|----------|
| Local | Prefer existing sibling; bootstrap may clone `develop` |
| CI | Fixed pin — [installation](../getting_started/installation.md) |

```bash
./scripts/bootstrap-deps.sh --pin-ci
```
