# Architecture

## Product boundary

Write-focused capture service:

- ingress via Nautilus Trader venue adapters (`../nautilus_trader`)
- one product binary: `catalog-capture-cli`
- declarative TOML plans
- output: **Rust-canonical** `ParquetDataCatalog` layout only

Does not fork Nautilus Trader, run strategies, or mirror Python legacy catalog paths.

## Principles

1. Capture policy lives here; reuse Nautilus models and catalog primitives.
2. Dedicated capture actor — not strategy-owned recording.
3. Hot path: filter, partition, batch.
4. Libraries are `rlib` only; one product binary.
5. Subscribe vs request custom data stay separate in config.

## Runtime

```text
TOML → catalog-capture-cli
    → LiveNode + CatalogCaptureActor
    → catalog-capture-core sink
    → {catalog_uri}/data/...
```

## Layers

| Crate | Role |
|-------|------|
| `catalog-capture-core` | Plan, buffers, flush, Parquet write, `capture_run` metadata |
| `catalog-capture-runtime-adapter` | Live actor, universe hooks, request polling, venue wiring |
| `catalog-capture-cli` | Config load/validate, credentials, runner, metrics HTTP |

## Out of scope

- Offline ML features on the hot path
- Query engine / multi-tenant platform
- Extra product binaries for demos
