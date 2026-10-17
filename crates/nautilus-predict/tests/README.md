# Predict adapter tests

Normal tests use saved protocol fixtures and in-process HTTP, WebSocket and RPC
services. They do not load operator credentials or submit funded orders.
Explicit network probes and capture exporters are ignored by default.

## Test layers

| Location | Boundary |
| --- | --- |
| `unit/` | Private parsing, precision, resource and lifecycle helpers, included with `cfg(test)` |
| `http_security.rs`, `private_http/`, `private_ws.rs`, `blockchain.rs` | Wire formats, authentication, pagination and chain reads |
| `execution/mock_venue.rs` | Shared private venue routes, configurable responses and request observations |
| `execution.rs`, `execution/` | Adapter commands/reports through Native ExecutionEngine and Portfolio |
| `execution/node_host.rs`, `execution/snapshot_multi.rs` | Product factories, Rust strategies, custom snapshots, RiskEngine and LiveNode |
| `support/` | Reusable test configuration, strategies and node assembly |
| `data_client/`, `instrument_requests.rs` | Subscriptions, reconnects, requests and Native data callbacks |
| `capture_rollover.rs`, `catalog_readback.rs`, `quote_rollover.rs` | Discovery, expiry handover and persisted public data |
| `private_api_live.rs` | Explicitly selected read-only or supervised order-write probes |

These layers have different purposes. Direct-client admission tests intentionally
bypass RiskEngine to exercise adapter-specific resource bounds. Strategy/node
tests exercise Native risk, routing and callbacks. Neither replaces the other.

## Run offline tests

From the repository root:

```bash
CARGO_INCREMENTAL=0 cargo test -p nautilus-predict --features execution --locked -- --test-threads=4
CARGO_INCREMENTAL=0 cargo test -p nautilus-predict --features native-quantity-commission --locked -- --test-threads=4
CARGO_INCREMENTAL=0 cargo clippy -p nautilus-predict --features native-quantity-commission --all-targets --locked -- -D warnings
```

The quantity-commission feature requires the pinned Nautilus fork configured in
the workspace. Ordinary execution uses a different position-reconciliation
mapping; its PnL results must not be presented as the same accounting path.
Focused examples:

```bash
cargo test -p nautilus-predict --features execution --locked --test execution multi_admission::
cargo test -p nautilus-predict --features execution --locked --test execution multiple_active_orders_replace
cargo test -p nautilus-predict --features native-quantity-commission --locked --lib execution::resources::
```

## Cancellation coverage

`execution/node_cancellations.rs` reuses the product host, shared batch strategy
and configurable loopback venue. Each test owns a distinct protocol or lifecycle
boundary:

| Boundary | Assertions |
| --- | --- |
| Removal result partitions | Per-ID removed/noop/rejected handling, including mixed batches and preflight failures |
| Wallet gaps | Definitive single/batch rejection reaches the strategy; explicit later cancellation succeeds |
| Native recovery | QueryOrder and periodic reports resolve missing wallet feedback without repeating writes |
| HTTP deadlines | Actual client timeouts preserve pending orders; single and multi-ID terminal recovery retains correct reservations |
| Duplicate reports | REST and repeated wallet reports produce one terminal strategy callback per order |
| Native exhaustion | OPEN or failed queries can lead to a local Canceled while the venue order remains OPEN; characterize the dependency limit |

The [adapter contract](../../../docs/predict-execution-adapter-design.md#native-cancellation-recovery-limit)
describes the exhaustion limitation. Synthetic scenarios do not establish live
outage or removal-lock behavior. Private HTTP tests separately replay unchanged
real single/batch removal receipts and check their numeric IDs. MARKET signing
vectors and REST envelope tests remain separate from lifecycle coverage.

## Add a scenario

1. Select the smallest existing layer that owns the behavior.
2. Reuse the shared venue and Native setup helpers. Keep transport routes in the
   mock venue and scenario-specific response mutations in the scenario.
3. Dispatch a command, then wait for observable state or a causal event. Avoid
   arbitrary sleeps and assertions that can pass before the work has run.
4. Assert venue requests and Native state/events, including identity, quantity,
   fees or balances where relevant. For money-sensitive failures, assert zero
   writes and a valid control case.
5. Keep protocol field matrices at unit level. Use one integration scenario for
   the lifecycle invariant rather than exporting another copy of existing suites.

For example, `multi_admission` verifies that an unaccountable OPEN snapshot
rejects a new command before POST, and that a separate command can be accepted
and canceled after the snapshot is repaired. `unit/resources.rs` covers signed
amounts versus cumulative fills, minimum positive units, rounding and token scope.
The adapter does not convert an existing OPEN order into Rejected.

## Fixtures and captures

`fixtures/evidence_manifest.json` records SHA-256, size and provenance. Run:

```bash
uv run --python 3.12 python tests/predict_evidence.py
```

| Tier | Meaning |
| --- | --- |
| `primary_capture` | Credential-redacted messages and logs actually observed from the venue |
| `derived` | Scenarios constructed from recorded economic facts |
| `synthetic` | Loopback responses for controlled protocol and failure cases |
| `reference` | SDK vectors and other reference inputs |
| `review` | Analysis artifacts, not raw venue evidence |

Keep captures immutable and give new recordings distinct paths. Validate the
manifest before collecting, stop the collector before refreshing it, and review
the tier assignment. Refresh refuses replacement of registered primary captures.
Do not update expected values merely to make tests pass. Generated transcripts
are not proof that a venue accepts a request, and historical responses combined
with derived local state are not newly observed order activity.

Cancellation exporters write run-specific JSON under the Git-ignored
`data/predict-validation/<run-id>/` directory. These are diagnostic outputs, not
regression inputs. Run only a named exporter; never run all ignored tests:

```bash
CARGO_INCREMENTAL=0 cargo test -p nautilus-predict --features execution --test execution node_host::cancellations::record_batch_http_timeout_probe --locked -- --ignored --exact
```

Version a synthetic fixture only when a test consumes it as a reproducible input
or it supplies reference evidence not covered by the test itself. Existing
retained captures remain immutable; exporters use `create_new`.

Real read-only session and query evidence is retained under
[`live_readonly_resources_20261017T100325Z`](fixtures/private_api/live_readonly_resources_20261017T100325Z/).
It covers early JWT replacement, WS resubscription, QueryAccount and QueryOrder.
The observed account was empty. Historical canceled-order repair uses a derived
local context; it does not verify newly submitted orders or nonempty startup.
Funded startup and exit evidence is documented separately in the
[exit-only report](../../../docs/reference/predict-live-evidence.md).

For operator configuration and explicitly selected network probes, use the
[private API guide](../../../docs/how_to/predict-private-api-tests.md).
