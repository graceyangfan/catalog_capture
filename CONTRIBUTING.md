# Contributing

## Principles

- Keep capture contracts and deployment policy in this repository.
- Prefer reuse of Nautilus model and persistence primitives over copied code.
- Add adapter behavior only after the core contract is clear.
- Keep Phase 1 simple: chunked direct Parquet first.

## License

Contributions are accepted under the **GNU Lesser General Public License v3.0 or later**
(LGPL-3.0-or-later). See `LICENSE` and `NOTICE`.

## Prerequisites

See [installation](docs/getting_started/installation.md). Use Rust `1.97.1`
(`rust-toolchain.toml`).

```bash
make bootstrap-deps   # local sibling first; else clone nautechsystems/nautilus_trader@develop
```

## Product surface (keep it simple)

- **One product binary:** `catalog-capture-cli` (same idea as Nautilus Trader’s `nautilus` CLI).
- **Libraries are `rlib` only** — do not add `[[bin]]` to core/runtime-adapter.
- **Do not add cargo `[[example]]` binaries** for demos; use `examples/*.toml` + the CLI, or unit tests.
- **Venue adapters are optional cargo features** (`venue-*` / `all-venues`; default is all venues).
  Slim build: `make build-slim` or
  `cargo build -p catalog-capture-cli --no-default-features --features venue-deribit`.
- Sibling `nautilus_trader`: prefer local; clone `develop` if missing; CI uses a fixed pin
  (`./scripts/bootstrap-deps.sh --pin-ci` to match).
- Execution plan: [docs/refactor-optimization-plan.md](docs/refactor-optimization-plan.md) (Track P / L).

## Workflow

1. Design in `docs/` first for non-trivial changes.
2. Add or adjust contracts in `catalog-capture-core`.
3. Add adapter behavior only after the core contract is clear.
4. Add unit tests in the crate that owns the behavior.
5. Run local checks before opening a PR.

## Local checks

```bash
make bootstrap-deps
make install-tools
pip install pre-commit && pre-commit install
make build          # product CLI only
make test
make fmt
make clippy
make cargo-deny
make pre-commit
```

If `target/debug` grows large: `make clean-debug` (or `make clean`).
Live smoke (requires network):

```bash
python3 tests/probe_option_universe_smoke.py --venue deribit-autorefresh --seconds 30 --cleanup
```

Documentation map: [docs/index.md](docs/index.md).

## Source headers

New Rust files must include the LGPL header block used in existing `crates/**/*.rs` files.

## Module boundaries

Keep generic helpers separate from deployment-specific capture policy. If a change is
reusable outside this service, isolate it in a small, well-bounded module rather than
mixing it into operator defaults.

## Commit style

- One logical change per commit.
- Complete sentences in commit messages.
- Reference docs or issue context when the behavior is operator-facing.
