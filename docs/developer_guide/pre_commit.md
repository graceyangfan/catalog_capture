# Pre-commit

This project uses [pre-commit](https://pre-commit.com/) with a focused hook set aligned
with Nautilus Trader conventions (without upstream-only hooks for PyO3/Cap'n Proto).

## Install

```bash
pip install pre-commit
pre-commit install
```

## Run manually

```bash
make pre-commit
# or
pre-commit run --all-files
```

## Hooks

| Hook | Scope |
|------|-------|
| trailing-whitespace, eof-fixer | rust, python, markdown, toml, yaml |
| taplo-format | TOML |
| shfmt, shellcheck | `scripts/`, `deploy/`, `.pre-commit-hooks/` |
| actionlint | `.github/workflows/` |
| typos | spelling |
| cargo fmt --check | Rust formatting |
| cargo clippy | workspace lint (requires `../nautilus_trader`) |

Clippy is skipped locally when the sibling `nautilus_trader` path is missing; CI always
runs clippy after checking out both repositories.