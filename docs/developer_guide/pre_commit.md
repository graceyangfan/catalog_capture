# Pre-commit

This project uses [pre-commit](https://pre-commit.com/) for formatting, linting, and
basic repository hygiene before each commit.

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
| cargo fmt --check (stable, via `cargo_fmt_stable.sh`) | Rust formatting |
| cargo clippy | workspace lint (requires sibling dependency checkout) |

Clippy is skipped locally when the path dependency checkout is missing; CI checks out
both repositories before running hooks.
