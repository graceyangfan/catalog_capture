# Pre-commit

Hooks follow the same practical subset as Nautilus Trader: general hygiene,
`shfmt` / `shellcheck`, `taplo`, `typos`, `actionlint`, plus local `cargo fmt`
and `cargo clippy`.

```bash
pip install pre-commit
pre-commit install
pre-commit run --all-files
# or
make pre-commit
```

`cargo fmt` uses stable rustfmt with an empty config path so nightly-only options
in `rustfmt.toml` (`group_imports`, `imports_granularity`) do not break the check —
same approach as Nautilus Trader.

Clippy requires a sibling `../nautilus_trader` checkout.
