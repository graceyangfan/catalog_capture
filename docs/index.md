# Documentation map

Catalog Capture records runtime market data directly into catalog-native Parquet assets. Documentation follows the [Divio system](https://docs.divio.com/documentation-system/):

| Type | Purpose | Location |
|------|---------|----------|
| Getting started | Install and first smoke run | [getting_started/](getting_started/) |
| Developer guide | Environment, pre-commit, doc style | [developer_guide/](developer_guide/) |
| Concepts | Architecture and design rationale | See list below |
| How-to | Operator workflows | [how_to/](how_to/) |
| Reference | CLI and TOML fields | [reference/](reference/) |

## Concepts (design documents)

- [RFC](rfc.md)
- [Architecture](architecture.md)
- [Production architecture](production-architecture.md)
- [Flush and rotation policy](flush-rotation-policy.md)
- [Implementation plan](implementation-plan.md)
- [Live validation](live-validation.md)
- [Option universe preflight](option-universe-preflight.md)
- [Option universe manager design](option-universe-manager-design.md)
- [Integration strategy](integration-strategy.md)

## Build dependency

The workspace links against Nautilus persistence and adapter libraries from a sibling
checkout at `../nautilus_trader`. See [installation](getting_started/installation.md).
License obligations for that dependency are described in `NOTICE`.