# Architecture

## Data flow

```text
TOML → catalog-capture-cli → LiveNode + CatalogCaptureActor
                                  ↑                 ↓
                         venue data adapters   catalog-capture-core
                                                    ↓
                                       ParquetDataCatalog + run metadata
```

The CLI composes one product binary. Its optional `predict-node` command hosts
Rust strategies separately from the capture actor. Both use the pinned Nautilus
fork; see [installation](../getting_started/installation.md) and the
[Predict execution contract](../predict-execution-adapter-design.md).

## Ownership

| Component | Owns | Does not own |
| --- | --- | --- |
| Venue adapters (`nautilus-predict`, `nautilus-extended`, `nautilus-lighter-ext`, upstream adapters) | Venue transport, wire models, instrument conversion and exchange commands/events | Capture files, strategy decisions or a second Native order ledger |
| `catalog-capture-core` | Capture plans, pure universe-selection rules, buffering, partitioning, flushing and catalog metadata | Venue transport or adapter model aliases |
| `catalog-capture-runtime-adapter` | Nautilus data actor, subscriptions, polling and dynamic universe changes | Parquet encoding or exchange protocol |
| `catalog-capture-cli` | Configuration, credentials, node assembly and operator commands | Duplicate capture or venue state |
| Nautilus Engine, Cache, Portfolio and RiskEngine | Order lifecycle, positions, account state and risk decisions | Venue-specific signing and transport |

The runtime actor consumes Nautilus events and passes capture items to the core
sink. Strategies receive execution events through Native; the capture actor is
not in their order path. Subscribe and request custom data remain separate
because they are distinct Native commands.

The core plan currently recognizes the `PredictCryptoUpDown` custom-data name
to budget instrument definitions for a rolling market with no static instrument
ID. This is a plan-specific exception; it does not justify importing Predict
transport or model aliases into the capture core.

## Simplification rule

Remove pass-through APIs and duplicate state before adding another layer. Keep
wire facts in the venue adapter, capture policy in the actor/core boundary, and
order/position authority in Native. The hot capture path filters, partitions and
batches; asynchronous discovery and exchange queries stay outside it.
