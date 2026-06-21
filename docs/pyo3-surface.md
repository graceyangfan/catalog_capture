# pyO3 Integration Plan

## Goal

Expose the capture configuration surface through pyO3 so that Python-side users can declare what runtime data should be recorded without embedding persistence logic inside strategies.

The desired user experience is:

- users configure capture from Python when they want to
- users configure capture declaratively
- the runtime implementation remains Rust-first
- the same capture concepts can be reused across live, paper, and backtest-oriented workflows
- the same PyO3 catalog surface becomes the primary readback path

## Design stance

pyO3 should expose the **configuration surface**, not the internal batching machinery.

That means Python users should be able to say:

- record quotes for these instruments
- record trades for these instruments
- record bars for these bar types
- record book deltas for these instruments

But they should not need to care about:

- partition buffer internals
- chunk writer internals
- active writer lifecycle details

## Phase split

### Phase 1

Keep pyO3 out of the critical path while the Rust capture core and actor are still stabilizing.

### Phase 2

Expose the user-facing configuration layer through pyO3:

- `CaptureConfig`
- `CapturePlan`
- `QuoteCaptureSpec`
- `TradeCaptureSpec`
- `BarCaptureSpec`
- `BookDeltasCaptureSpec`
- `CatalogCaptureActorConfig`

### Phase 3

If the runtime integration matures enough, expose a higher-level Python-friendly way to attach capture to a node or deployment recipe.

## Why pyO3 matters here

Even when the capture service stays Rust-first, many deployments will still want to:

- configure capture from Python
- reuse Python-side venue and strategy orchestration
- keep live, paper, and backtest workflows consistent

That makes pyO3 an important surface for adoption, even if it is not the first implementation milestone.

## Proposed Python-facing concepts

### `CaptureConfig`

Python should be able to configure:

- `enabled`
- `catalog_uri`
- `flush_rows`
- `flush_interval_ms`
- `max_buffer_bytes`
- `compression`
- `overflow_policy`

### `CapturePlan`

Python should be able to declare a plan explicitly, for example:

- quote capture specs
- trade capture specs
- bar capture specs
- book-deltas capture specs

This is the key to making capture reproducible and auditable.

### `CatalogCaptureActorConfig`

Python should be able to bind:

- actor identity
- capture config
- capture plan

That would make the dedicated capture actor feel like a first-class runtime component rather than an opaque sidecar.

## Why not infer capture from strategy subscriptions

It can be tempting to say:

- "if the strategy subscribes to it, record it"

But that is the wrong long-term interface because:

- recording becomes an accidental side effect
- multi-strategy deployments can duplicate intent
- changing strategy logic silently changes recorded coverage
- backtest inputs become less explicit

The pyO3 surface should reinforce the opposite design:

- capture is declared explicitly
- strategies remain strategy-focused
- persistence policy remains deployment-owned

## Validation goals for pyO3

When the pyO3 surface is introduced, validate:

1. Python can construct a `CapturePlan` without dropping to Rust internals.
2. Python configuration maps cleanly to the Rust actor configuration.
3. The configured capture actor records the intended data families only.
4. The resulting Parquet catalog remains directly readable by standard backtest workflows.

## Expected outcome

If this surface is done well, Python users should gain freedom in what they record without reintroducing the old `StreamingFeatherWriter` model.

The implementation stays Rust-first.

The configuration stays user-friendly.

The captured assets stay directly reusable.
