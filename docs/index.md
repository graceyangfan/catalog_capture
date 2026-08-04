# Documentation map

Catalog Capture records runtime market data directly into catalog-native Parquet assets.
**Default catalog layout is Nautilus Trader Rust canonical** so assets can be loaded by Rust
backtest without conversion. Documentation follows the
[Divio system](https://docs.divio.com/documentation-system/):

| Type | Purpose | Location |
|------|---------|----------|
| Getting started | Install and first smoke run | [getting_started/](getting_started/) |
| Developer guide | Environment, pre-commit, doc style | [developer_guide/](developer_guide/) |
| Concepts | Architecture and design rationale | See list below |
| How-to | Operator workflows | [how_to/](how_to/) |
| Reference | CLI and TOML fields | [reference/](reference/) |

## Active execution plan

- **[Refactor and optimization plan](refactor-optimization-plan.md)** — open-source delivery,
  Rust catalog / backtest (Track L), **single product binary** (Track P, Nautilus-style),
  venue features, config split, docs IA

## How-to highlights

- [Rust backtest from catalog](how_to/rust_backtest_from_catalog.md)
- [Smoke and soak](how_to/smoke_and_soak.md)
- [Credentials (optional env keys)](how_to/credentials.md)
- [Installation](getting_started/installation.md) (`make bootstrap-deps`)

## Concepts (design documents)

- [Architecture](architecture.md)
- [Production architecture](production-architecture.md)
- [Flush and rotation policy](flush-rotation-policy.md) — **per-family flush table (R3)**
- [Custom data contract](custom-data-contract.md)
- Core library surface: [crates/catalog-capture-core/README.md](../crates/catalog-capture-core/README.md) **(C5)**
- [Segment lifecycle](segment-lifecycle.md)
- [Live validation](live-validation.md)
- [Option universe preflight](option-universe-preflight.md)
- [Option universe manager design](option-universe-manager-design.md)
- [Integration strategy](integration-strategy.md)

### Historical / detailed planning (do not treat as current TODO)

- [RFC](rfc.md)
- [Implementation plan](implementation-plan.md)
- [Stepwise capture roadmap](stepwise-capture-roadmap.md)
- [Options ML data capture plan](options-ml-data-capture-plan.md)
- [Native custom data targets](native-custom-data-targets.md)
- [PyO3 surface](pyo3-surface.md)

## How-to

- [Smoke and soak](how_to/smoke_and_soak.md)
- [Unattended capture](how_to/unattended_capture.md)
- [Rust backtest from catalog](how_to/rust_backtest_from_catalog.md) — **layout contract for Rust replay**

## Build dependency

The workspace links against Nautilus persistence and adapter libraries from a sibling
checkout at `../nautilus_trader`. See [installation](getting_started/installation.md).
License obligations for that dependency are described in `NOTICE`.
Pin the sibling revision used by CI when building for production or open-source release
(see [refactor-optimization-plan.md](refactor-optimization-plan.md) Track O).
