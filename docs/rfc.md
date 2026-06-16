# RFC: External Rust-first Catalog Capture for Nautilus Trader

## Summary

This project implements a deployment-owned direct catalog capture path outside the Nautilus Trader core repository.

The target outcome is simple:

- runtime/online-generated data is written directly into catalog-readable Parquet assets
- backtest can consume those assets after rollover without a `feather -> convert -> parquet` step
- the implementation remains compatible with Nautilus Trader's Rust-first direction without requiring a built-in framework service

The primary runtime shape is a **dedicated capture actor**, not strategy-owned recording logic.

## Why external

Nautilus maintainers have explicitly stated that runtime capture is operational/deployment-specific and should remain user-owned.

That makes an external project the right boundary:

- reuse Nautilus primitives
- own the capture policy externally
- upstream only minimal generic hooks or helpers if needed

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
- `catalog-capture-plugin-adapter`: optional shell for stock `LiveNode` integration when plugin deployment is preferred

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
- easy to discuss with maintainers later
- easy to mount beside existing strategies without inventing new framework services

## Long-term stance

- `StreamingFeatherWriter` can remain a fallback or migration aid
- direct catalog capture is the target architecture
- plugin is a deployment adapter, not the core design center
- the dedicated capture actor is the primary runtime model
