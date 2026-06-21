# Contributing

## Principles

- Keep the project external to `nautilus_trader` core.
- Prefer reuse of Nautilus model and persistence primitives over copied code.
- Keep deployment-specific policy outside anything that might someday be upstreamed.
- Keep Phase 1 simple: chunked direct Parquet first.

## Prerequisites

Clone this repository next to a compatible `nautilus_trader` checkout:

```text
~/nautilus_trader
~/nautilus_catalog_capture
```

Path dependencies in `Cargo.toml` expect `../nautilus_trader`. Use Rust `1.96.0` (`rust-toolchain.toml`).

## Workflow

1. Design in `docs/` first for non-trivial changes.
2. Add or adjust contracts in `catalog-capture-core`.
3. Add adapter behavior only after the core contract is clear.
4. Add unit tests in the crate that owns the behavior.
5. Prefer benchmarks and integration checks before broadening scope.

## Local checks

```bash
make test
make fmt
make clippy
```

Live smoke (requires network):

```bash
python3 tests/probe_option_universe_smoke.py --venue deribit-autorefresh --seconds 30 --cleanup
python3 tests/probe_option_universe_soak.py --preset daily-live --seconds 180 --cleanup
```

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