# Contributing

## Principles

- Keep the project external to `nautilus_trader` core.
- Prefer reuse of Nautilus model and persistence primitives over copied code.
- Keep deployment-specific policy outside anything that might someday be upstreamed.
- Keep Phase 1 simple: chunked direct Parquet first.

## Workflow

1. Design in `docs/` first.
2. Add or adjust contracts in `catalog-capture-core`.
3. Add adapter behavior only after the core contract is clear.
4. Prefer benchmarks and integration checks before broadening scope.

## Upstream mindset

If a change might belong upstream later, isolate it early:

- helper
- hook
- compatibility improvement
- example

Do not couple core capture policy to potential upstream patches.
