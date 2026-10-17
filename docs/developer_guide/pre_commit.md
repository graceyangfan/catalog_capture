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

Clippy requires the pinned sibling `../nautilus_trader_quantity_commission` checkout.


## Secret scan before publishing

Local operator TOML and `.env` files must remain ignored. Review the staged file
list and scan the outgoing commit range with Gitleaks:

```bash
gitleaks git --redact=100 --log-opts="origin/main..HEAD"
```

The repository configuration extends the default rules. It permits only the
public Anvil account-0 development key and exact source-hash metadata shapes in
retained validation summaries. Do not exclude entire fixture directories or
create a blanket baseline for unexplained findings. Keep scan reports local and
redacted. A clean pattern scan complements, but does not replace, collector
redaction and review of captured data.
