# Upstream Strategy

## Main idea

This project should be maintained as an external, Nautilus-compatible capture layer.

The goal is not to upstream the whole operational service.

The goal is to upstream only small, generic improvements if needed.

The external project should look and feel like a normal Nautilus deployment component:

- a dedicated actor
- a capture plan
- direct use of `ParquetDataCatalog`

That is easier for maintainers to reason about than a new built-in runtime capture subsystem.

## Likely upstreamable items

- stable runtime hooks
- path compatibility helpers
- custom data path consistency fixes
- example recipes for direct catalog writing from a dedicated actor
- small generic writer/finalization helpers

## Items that should remain external

- batching policy
- overflow policy
- deployment-specific storage defaults
- rollout fallback behavior
- capture orchestration and service lifecycle
- the concrete capture actor implementation

## Why this is maintainable

This keeps the ownership boundary clean:

- Nautilus owns core primitives
- this project owns deployment capture policy

That matches maintainer guidance and reduces long-term fork pressure.
