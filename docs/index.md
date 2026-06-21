# Documentation map

Nautilus Catalog Capture records runtime market data directly into Nautilus-native
Parquet catalog assets. Documentation follows the [Divio system](https://docs.divio.com/documentation-system/):

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
- [Upstream strategy](upstream-strategy.md)

## External dependency

This project requires a sibling [Nautilus Trader](https://github.com/nautechsystems/nautilus_trader)
checkout at `../nautilus_trader`. See [installation](getting_started/installation.md).