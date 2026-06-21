# Flush and Rotation Policy

## Goal

Define a production-shaped file lifecycle policy for this project that:

- stays natural for catalog-native backtest and research workflows
- preserves direct PyO3 `ParquetDataCatalog` readability
- avoids unbounded in-memory buffering
- reduces small-file proliferation
- keeps runtime hot-path work small and predictable

This document explains:

- how the current Phase 1 chunking model works
- why it is reasonable today
- where it becomes insufficient
- how to tune it for production-like capture
- when to evolve toward active `.part` writers

## Current Phase 1 behavior

The current system does **not** keep one long-lived active parquet file open.

Instead it works like this:

1. A `CatalogCaptureActor` subscribes to declared data families.
2. Runtime callbacks enqueue normalized model objects into bounded background queues.
3. Background workers partition data by family and identifier.
4. Each partition accumulates in-memory rows and estimated bytes.
5. A flush occurs when one of the configured flush conditions is met.
6. That flush writes one **complete immutable parquet chunk file** through `ParquetDataCatalog`.
7. If legacy compatibility mirroring is enabled, the completed file is mirrored into the Python legacy discovery layout on local filesystems.

This means the current system uses **flush-driven chunking**, not active-file rotation.

## Why the current model is reasonable

For the current stage of the project, flush-driven chunking is a good choice because it:

- proves the direct online-to-parquet path with low implementation risk
- writes standard parquet files that catalog readback can consume immediately
- avoids active writer lifecycle complexity
- avoids `.part` file recovery semantics in the first production iteration
- keeps the read contract simple: each completed chunk is already catalog-readable

It is the simplest model that satisfies the core project promise:

> runtime capture writes parquet assets that catalog readback can consume directly.

## What the current model is not

The current model is **not yet**:

- date-based rotation
- final-file-size-based rotation
- active `.part` row-group append
- close-and-rename parquet finalization
- WAL-backed recovery-oriented capture

That is intentional.

The current design chooses compatibility and simplicity first, then plans to add stronger file lifecycle control only if production evidence justifies it.

## Why small files can happen

Because each flush produces one new complete parquet file, small files appear when:

- `flush_rows` is set too low
- `flush_interval_ms` is too short for a low-volume partition
- `max_buffer_bytes` is too small
- many partitions are cold or sporadically active
- low-frequency families flush on the same cadence as high-frequency families

The current system therefore trades off:

- simpler writer state
- immediate catalog readability

against:

- potentially more parquet chunk files
- potentially worse compression on tiny batches
- potentially more catalog scan overhead

## Production principle

The project should treat file lifecycle as a **policy**, not a hard-coded writer behavior.

That means production tuning should answer:

- how much data to keep in memory before flushing
- how long to keep a cold partition open in memory
- which data families deserve aggressive flushing
- which data families should wait longer to avoid tiny files
- what file-size band is operationally healthy

## Recommended Phase 1.5 policy

Before introducing active `.part` writers, the preferred next step is to make the current chunking model more deliberate.

### 1. Tune by data family, not globally only

Different families should not share the exact same practical defaults.

Suggested policy direction:

- `QuoteTick`, `TradeTick`, `OrderBookDeltas`
  - prefer larger `flush_rows`
  - allow shorter `flush_interval_ms`
  - prioritize throughput and compression
- `MarkPriceUpdate`
  - moderate thresholds
  - moderate flush cadence
- `InstrumentStatus`, `InstrumentClose`, `OptionGreeks`
  - lower row volume
  - longer `flush_interval_ms`
  - prioritize avoiding tiny files
- `InstrumentAny`
  - very low frequency
  - may flush mainly on interval or shutdown
- `CustomData`
  - family-specific tuning depending on expected throughput

The current configuration is global. The next production step should introduce either:

- per-family defaults, or
- optional per-family overrides

without making basic operator configuration too complex.

### 2. Prefer target file bands over arbitrary tiny chunks

Even before active `.part` writers exist, operational tuning should aim for a healthy file-size range.

For local filesystem workloads, a practical target band is usually:

- avoid repeated files below roughly `1 MB` unless the family is genuinely sparse
- aim more commonly for files in the `4 MB` to `64 MB` range
- avoid very large monolithic files for hot partitions

This is not a strict contract. It is an operating guideline.

The project should measure:

- average file size
- p50 / p95 file size
- files per hour
- files per partition per hour

before deciding whether the file lifecycle is healthy.

### 3. Distinguish tail flush from throughput flush

Two flush reasons are operationally different:

- **throughput flush**
  - triggered by rows or bytes
  - expected to produce normal chunk sizes
- **tail flush**
  - triggered by interval or shutdown
  - may legitimately produce smaller files

This distinction matters for observability and tuning.

Operators should be able to see whether small files are mostly caused by:

- overly small throughput thresholds
- overly frequent interval flushes
- sparse instruments
- shutdown tails

### 4. Make testing intentionally low-threshold, but keep production defaults conservative

For development and validation, it is useful to set:

- very small `flush_rows`
- short `flush_interval_ms`

to force multiple files and prove chunking behavior.

That is a **test profile**, not a production profile.

Example validation-oriented profile:

```toml
[output]
flush_rows = 3
flush_interval_ms = 250
max_buffer_bytes = 65536
queue_capacity = 100
```

This kind of profile is excellent for proving:

- multiple parquet chunk creation
- shutdown tail flush
- direct PyO3 readback after several chunk writes

It should not become the default for long-running live capture.

## Recommended production defaults

These are suggested starting points, not final hard-coded values.

### General-purpose live market capture

```toml
[output]
flush_rows = 5000
flush_interval_ms = 1000
max_buffer_bytes = 33554432
queue_capacity = 10000
```

This profile is intentionally moderate:

- large enough to avoid many tiny files in common quote/trade flows
- small enough to bound memory
- frequent enough to make trailing data visible reasonably quickly

### Lower-frequency operational families

If per-family overrides are introduced later, the following direction is recommended:

- `instrument_status`
  - longer interval
  - small row thresholds are acceptable
- `instrument_close`
  - longer interval
  - shutdown flush is often sufficient
- `option_greeks`
  - depends heavily on venue cadence
  - default more conservatively than quotes

## Observability that must exist before we judge the policy

File lifecycle decisions should be backed by measurements, not intuition.

The system should expose at least:

- queued items by family
- dropped items by family and overflow reason
- flushed rows by family
- flush count by family
- flush reason counts:
  - row threshold
  - byte threshold
  - interval
  - shutdown
- average file size by family
- files written by family and partition

Without those measurements, operators can see that files exist, but they cannot easily tell whether the chosen policy is healthy.

## When the current model is still good enough

The current chunking model remains a good default when:

- capture volume is moderate
- the number of partitions is bounded
- file counts remain manageable
- average file size is reasonable
- PyO3 catalog readback remains smooth
- backtest load times remain acceptable

In that regime, introducing active `.part` writers too early would add complexity without much return.

## When to move to active `.part` writers

The next lifecycle model should only be introduced when production evidence shows the current one is insufficient.

Recommended triggers:

- file count becomes operationally noisy
- too many partitions repeatedly produce tiny files
- compression efficiency is materially poor
- PyO3 catalog scans or backtest loads degrade due to file fragmentation
- object-store usage makes small-file economics unacceptable

At that point, the preferred evolution is:

1. open an active `.part` file per hot partition
2. append row groups into that active file
3. rollover on row / byte / open-time thresholds
4. finalize by close + rename

That should be treated as **Phase 2 file lifecycle optimization**, not the baseline contract.

## Relationship to the old Feather workflow

The old streaming path behaved more like a continuously written stream file with explicit rotation.

The current capture runtime is intentionally different:

- it writes immutable parquet chunks
- each completed chunk is already catalog-readable
- it avoids a separate conversion stage

That difference is important.

The new project should not try to mimic the old workflow mechanically. It should produce a file lifecycle that is natural for direct parquet and still operationally sane.

## Track S — Segment Lifecycle (implemented)

For long-running unattended capture, the project now supports an optional **segment** lifecycle
mode. See [segment-lifecycle.md](segment-lifecycle.md) for the full design.

Three orthogonal policies:

| Policy | Meaning |
|--------|---------|
| **Batch** | Memory buffer → append row group to same `.part` file |
| **Sync** | Periodic `fsync` on the active file |
| **Seal** | Close + rename to `{min_ts}_{max_ts}.parquet`; open new `.part` |

Default remains **chunked** (flush-driven immutable chunks) for CI and regression. Enable segment
mode via `[output.lifecycle]` in TOML.

Segment seal (e.g. daily 06:00 UTC) is **independent** from HIP-4 universe refresh or option
universe rollover — those change subscriptions; segment lifecycle changes how files are produced.

## Recommended near-term implementation order

1. Keep chunked mode as the default for tests and smoke runs.
2. Use segment mode for unattended perpetual / daily-seal production profiles.
3. Add flush reason metrics and file-size metrics (including `FlushReason::Seal`).
4. Introduce per-family default tuning.
5. Run segment roundtrip validation (`segment_quote_roundtrip` example).
6. Run longer live soak tests with production-like thresholds.

## Practical conclusion

Chunked mode remains appropriate for Phase 1 validation and low-volume capture.

Segment mode is the production-shaped path for long-running jobs that need:

- fewer files per partition per day
- scheduled seal boundaries for backtest handoff
- continuous append without per-flush new files

Both modes write catalog-readable sealed/finalized parquet; segment mode defers readability until
seal (`.part` files are not catalog-queryable).
