# Integration strategy

## Main idea

This repository is a standalone capture service that integrates with existing persistence and
live-runtime primitives through normal library dependencies.

The service should feel like a focused deployment component:

- a dedicated capture actor
- a declarative capture plan
- direct use of `ParquetDataCatalog`

That keeps responsibilities clear: trading decisions stay in strategies; recording policy stays
here.

## What belongs in this repository

- capture plan schema and validation
- partitioning, buffering, and flush policy
- option-universe resolve and refresh logic
- CLI and operator workflows (smoke, soak, unattended capture)
- deployment-specific defaults (catalog paths, rotation, health checks)
- the concrete `CatalogCaptureActor` implementation used by this service

## What belongs in dependency libraries

- instrument and market-data model types
- venue adapters and websocket clients
- parquet catalog read/write primitives
- live node, data engine, and actor runtime

We consume those as libraries. We do not fork or reimplement them inside this repository.

## Optional shared improvements

When a change is genuinely generic (path compatibility helper, catalog write utility, small
runtime hook), keep it isolated in a bounded module here first. Promote it to a shared library
only after the capture use case is stable and the API is narrow.

## Why this split is maintainable

- dependency libraries own core model and persistence contracts
- this project owns deployment capture policy and operator surface
- upgrades to adapter or persistence crates flow through normal path dependencies
- capture behavior can evolve on its own release cadence

See `NOTICE` for license obligations on linked components.