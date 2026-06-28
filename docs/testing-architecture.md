# Testing Architecture

This repository validates capture in three layers:

## 1. Rust unit tests

Use crate-local `mod tests` for pure logic:

- capture-plan transforms
- budget estimation
- option-universe resolution
- runtime bookkeeping

These tests should not require live adapters, network, or Python.

## 2. Fixture readback tests

Use Rust fixture writers plus Python readback probes for stable end-to-end validation:

- Rust writes canonical parquet through `CatalogCaptureActor`
- Python reads the catalog through Nautilus `ParquetDataCatalog`

This is the preferred regression layer for custom data because it avoids network instability while
still validating the Rust -> catalog -> Python path.

## 3. Live smoke tests

Use short, bounded live scripts only for critical venue paths:

- connection succeeds
- subscriptions succeed
- parquet is written
- readback succeeds after shutdown

Live smokes should stay sparse. They are expensive and should verify a narrow production path, not
replace fixture tests.

## Python layout

Python probes under `tests/` should follow these roles:

- `catalog_probe_common.py`: shared helpers for catalog loading, instrument checks, and monotonicity
- `python_smoke_common.py`: shared helpers for fixture example execution and temp catalog lifecycle
- `python_catalog_*_probe.py`: readback assertions against an existing catalog
- `python_*_smoke.py`: fixture-driven end-to-end tests
- `probe_*_smoke.py`: live capture launchers

Avoid duplicating helper logic across probes. New probes should import shared helpers first.

## Why `target/debug/deps` is huge

`target/debug/deps/` is Cargo's build cache. In a Rust workspace with PyO3, Arrow, Parquet, and
multiple Nautilus adapter crates, it will contain:

- object files (`*.o`)
- dependency metadata (`*.d`)
- test binaries
- example binaries
- incremental rebuild artifacts

This directory is not project structure; it is compiler output. A large file count here is normal.

What matters is reducing unnecessary compile surface:

- keep workspace dependency features narrow
- keep the workspace default member on the production CLI path
- gate runtime-adapter examples behind an explicit feature
- avoid turning fixtures into always-built product binaries
- prefer shared test helpers over many near-duplicate executables
- run focused package tests during development instead of rebuilding the entire workspace

## Practical development workflow

- Fast logic iteration: `cargo test -p catalog-capture-core`
- CLI validation iteration: `cargo test -p catalog-capture-cli`
- Fixture validation: run targeted `python_*_smoke.py`
- Live validation: run one `probe_*_smoke.py` at a time

If disk usage grows too large, use `cargo clean` deliberately, but do not treat `target/debug/deps`
as a source tree to organize.
