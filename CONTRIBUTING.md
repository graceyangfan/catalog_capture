# Contributing

Thanks for helping improve **Catalog Capture** — an independent, unofficial tool
compatible with Nautilus Trader catalog layouts. See [TRADEMARK.md](TRADEMARK.md)
and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Principles

- Keep capture contracts and deployment policy in this repository.
- Prefer Nautilus model and persistence primitives over copied code.
- One product binary: `catalog-capture-cli`. Libraries stay `rlib` only.
- Examples are **TOML configs**, not cargo `[[example]]` / extra product bins.
- Do not brand contributions as official Nautilus products.

## License

Contributions are accepted under **LGPL-3.0-or-later**. See `LICENSE` and `NOTICE`.

## Prerequisites

```bash
# Prefer the CI-pinned Nautilus revision (reproducible builds)
make bootstrap-deps
# Power users with an existing editable ../nautilus_trader_quantity_commission tree:
#   make bootstrap-deps-local

make install-tools
pip install pre-commit && pre-commit install
```

Use `make` or `./scripts/cargo.sh` for all Cargo commands. The project pins
huacnlee/cargo; `make install-cargo` installs it without changing other projects.
Rust **1.100.0** (`rust-toolchain.toml`). Details:
[docs/getting_started/installation.md](docs/getting_started/installation.md).

## Workflow

1. Design non-trivial changes in `docs/` first when needed.
2. Put contracts in `catalog-capture-core`; adapter behavior only after the contract is clear.
3. Add unit tests in the crate that owns the behavior.
4. Run local checks before opening a PR.

## Local checks

```bash
make build
make test
make fmt
make clippy
make cargo-deny
make pre-commit
```

Live smoke (network):

```bash
python3 tests/probe_option_universe_smoke.py --venue deribit-autorefresh --seconds 30 --cleanup
```

## Product surface (keep it simple)

- Venue adapters are optional cargo features (`venue-*` / `all-venues`).
- Slim build: `make build-slim` or
  `./scripts/cargo.sh build -p catalog-capture-cli --no-default-features --features venue-deribit`
- CI pins Nautilus Trader — see [docs/getting_started/installation.md](docs/getting_started/installation.md).

## Documentation and local artifacts

- Maintain documentation in English and describe the current supported behavior.
- Keep setup, configuration, contracts and reproducible validation commands;
  summarize decisions instead of retaining session logs or step-by-step work diaries.
- Remove retired experiments together with their scripts, templates and links.
- Keep probe outputs, benchmark reports and local credentials outside versioned
  documentation. Use ignored `data/` for operational outputs and `env/` for secrets.
  Retain versioned regression fixtures when tests depend on them.
- Review staged paths and run the [secret scan](docs/developer_guide/pre_commit.md#secret-scan-before-publishing)
  before committing. Never force-add a local credential file.

## Source headers

New Rust files must include the LGPL header block used in existing `crates/**/*.rs`.

## Commit style

- One logical change per commit.
- Complete sentences in commit messages.
- Prefer Conventional Commits when practical (`fix:`, `docs:`, `test:`).
