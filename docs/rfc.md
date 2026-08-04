> **Historical document.** Current priorities live in [refactor-optimization-plan.md](refactor-optimization-plan.md) and [ROADMAP.md](../ROADMAP.md).
>

# RFC: Direct catalog capture

## Summary

This project implements a deployment-owned direct catalog capture service.

The target outcome is simple:

- runtime/online-generated data is written directly into catalog-readable Parquet assets
- backtest can consume those assets after rollover without a `feather -> convert -> parquet` step
- capture policy, batching, and operations remain in this repository

The primary runtime shape is a **dedicated capture actor**, not strategy-owned recording logic.

## Scope

Catalog capture is operational infrastructure: batching thresholds, storage layout, rollover
policy, and long-running service lifecycle belong here rather than in strategy code.

This repository:

- reuses persistence and adapter primitives from the sibling dependency checkout
- owns capture configuration and deployment behavior
- keeps generic trading logic out of the capture path

## Problem framing

In real deployments, strategies may subscribe to a large amount of runtime data.

If direct backtest reuse matters, it is not enough to let each strategy "record what it saw":

- capture becomes coupled to strategy business logic
- multiple strategies can duplicate writes
- recorded coverage becomes an accidental by-product of strategy subscriptions
- hot-path write concerns leak into trading logic

That makes strategy-owned recording the wrong default boundary.

## Target architecture

- `catalog-capture-core`: capture config, capture plan, partitioning, buffering, and sink contracts
- `catalog-capture-runtime-adapter`: a dedicated `CatalogCaptureActor` that subscribes to a declared capture plan

The default shape is:

- strategies remain ordinary `DataActor` or `Strategy` instances
- capture is performed by a separate actor
- the actor writes directly to `ParquetDataCatalog`
- backtest reads the resulting assets without conversion

## Implementation stance

Phase 1 should use **chunked direct Parquet** rather than immediately building appendable active parquet writers.

This keeps the first version:

- simple
- testable
- easy to maintain
- easy to mount beside existing strategies without inventing new framework services

## Long-term stance

- `StreamingFeatherWriter` can remain a fallback or migration aid
- direct catalog capture is the target architecture
- the dedicated capture actor is the primary runtime model
