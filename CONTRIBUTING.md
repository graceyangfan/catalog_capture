# Contributing

## Principles

- Keep the project external to `nautilus_trader` core.
- Prefer reuse of Nautilus model and persistence primitives over copied code.
- Keep deployment-specific policy outside anything that might someday be upstreamed.
- Keep Phase 1 simple: chunked direct Parquet first.

## License

Contributions are accepted under the **GNU Lesser General Public License v3.0 or later**
(LGPL-3.0-or-later), consistent with Nautilus Trader. See `LICENSE` and `NOTICE`.

## Prerequisites

Clone this repository next to a compatible `nautilus_trader` checkout:

```text
~/nautilus_trader
~/nautilus_catalog_capture
```

Path dependencies in `Cargo.toml` expect `../nautilus_trader`. Use Rust `1.96.0`
(`rust-toolchain.toml`).

## Workflow

1. Design in `docs/` first for non-trivial changes.
2. Add or adjust contracts in `catalog-capture-core`.
3. Add adapter behavior only after the core contract is clear.
4. Add unit tests in the crate that owns the behavior.
5. Run local checks before opening a PR.

## Local checks

```bash
make install-tools
pip install pre-commit && pre-commit install
make test
make fmt
make clippy
make cargo-deny
make pre-commit
```

Live smoke (requires network):

```bash
python3 tests/probe_option_universe_smoke.py --venue deribit-autorefresh --seconds 30 --cleanup
```

Documentation map: [docs/index.md](docs/index.md).

## Source headers

New Rust files must include the LGPL header block used in existing `crates/**/*.rs` files.

## Upstream mindset

If a change might belong upstream later, isolate it early:

- helper
- hook
- compatibility improvement
- example

Do not couple core capture policy to potential upstream patches.

## Commit style

- One logical change per commit.
- Complete sentences in commit messages.
- Reference docs or issue context when the behavior is operator-facing.
