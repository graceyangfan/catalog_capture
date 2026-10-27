# Predict adapter tests

Ordinary tests use immutable fixtures and loopback HTTP/WS/RPC, without credentials
or venue orders. The [architecture](../../../docs/predict-execution-adapter-design.md)
defines behavior; the [validation matrix](../../../docs/reference/predict-execution-validation.md)
links sources, real JSON and known limits. Use the matching
[Native dependency](../../../docs/reference/predict-execution-validation.md#two-repository-handoff).

## Run offline tests

```bash
cargo test -p nautilus-predict --features execution --locked -- --test-threads=1
cargo clippy -p nautilus-predict --features execution --all-targets --locked -- -D warnings
cargo check -p nautilus-predict --no-default-features --features execution --locked --lib
cargo check --workspace --all-targets --features catalog-capture-cli/predict-execution --locked
python3 tests/predict_evidence.py
```

Serial full-suite scheduling isolates process-global fixtures; HTTP/WS/strategy
concurrency still runs inside scenarios. Set NO_PROXY=127.0.0.1,localhost if local
proxies interfere. Execution has one fee contract, including no-default-feature
builds. Ignored tests are not part of this baseline: never run the entire ignored set.

## Layers and focused replays

| Location | Boundary |
| --- | --- |
| unit/ | Private protocol/evidence, resources, routing, nonce and mirror retention |
| private_http/, private_ws.rs, blockchain.rs, http_security.rs | Wire schemas, authentication, pagination and canonical chain reads |
| execution/ | Native execution/Portfolio, order/report application and subscriber-visible state |
| execution/node_host.rs and support/ | Factories, RiskEngine, Rust Strategy and LiveNode callbacks |
| data_client/, instrument_requests.rs | Discovery/subscriptions, current request API, both transports and reconnect |
| capture_rollover.rs, catalog_readback.rs, quote_rollover.rs | Expiry handover and persisted public data |
| private_api_live.rs | Explicit operator probes, ignored by default |

Direct-client tests intentionally bypass RiskEngine for protocol fault cases.
LiveNode scenarios separately exercise Native risk and strategy flow. Neither
layer substitutes for the other. Select focused replay without exporting new JSON:

```bash
cargo test -p nautilus-predict --features execution --test market_recorded_fill --locked
cargo test -p nautilus-predict --features execution --test execution recorded_startup:: --locked
cargo test -p nautilus-predict --features execution --test execution batch_recorded:: --locked
cargo test -p nautilus-predict --features execution --test execution terminal_recovery:: --locked
cargo test -p nautilus-predict --features execution --test execution expiry_scope:: --locked
cargo test -p nautilus-predict --features execution --test execution feedback_delivery:: --locked
```

| Focused area | Assertions |
| --- | --- |
| Recorded startup, strategy lifecycle and batch replay | Captured entry/fees, adoption, removal and exits through Native Strategy/LiveNode |
| terminal_recovery | Atomic remainder, pending commands, partial/late settlements, duplicate replay and terminal observations while history is held |
| feedback_delivery | Subscriber-visible applied state despite held IO, missing ACK, route anomalies, expiry or closed optional channels |
| expiry_scope | Historical residuals and expired definitions remain independent of active-market admission |
| submissions nonce controls | Contract-local invalidation, concurrent markets, independent RPC deadlines, preserved successful slots, cancellation and generation isolation |
| feed_discovery | Exact query/path encoding and bounded cursor-cycle rejection |

Subscriber checks assert exact gross/net quantities, original fees and applied
Cache state. No replay schedule may fabricate or void an execution. Identity,
time, signing and delivery-order changes are labeled derived; they do not
establish additional venue behavior. Fixture provenance is documented in the
[validation matrix](../../../docs/reference/predict-execution-validation.md).
Pending-command retention and terminal quantity normalization require its
[paired framework revision](../../../docs/reference/predict-execution-validation.md#two-repository-handoff).

## Pre-live checks without venue orders

Run the ordinary suite first, then select these additional offline integrations:

```bash
cargo test -p nautilus-predict --features execution --locked --test execution restart_live::native_restart_ -- --ignored --test-threads=1
cargo test -p nautilus-predict --features execution --locked --test private_ws repeated_reconnects_deliver_each_epoch_once_and_close_the_session -- --ignored --exact --test-threads=1
```

Restart tests launch an isolated loopback Redis and use new LiveNode/Cache/strategy
instances; they do not simulate an OS crash or Redis data loss. A repeated order
query reads one status and, for a matched order, its trade history. Final Native
trade IDs, quantities, cash and exit counts must remain unchanged by duplicate
recovery. Snapshot signals use distinct venue millisecond timestamps.

The reconnect check exercises 16 connections on each supported transport, stale
ACK rejection, per-epoch delivery and close. It preserves Native's reconnect
backoff, including its 10-second cap, and takes several minutes.

`continuous::extended_callbacks_with_suspended_recovery` reuses the existing
Strategy/LiveNode replay for 1,000 maker replacements with 1,000 retained active
orders, and 500 simulated taker commands, each with a baseline and a held-query
case. It checks callbacks, request counts, final state and telemetry loss. This is
a finite synthetic workload, not production latency or unattended uptime evidence.
Actual read-only authentication/query probes are documented in the
[probe guide](../../../docs/how_to/predict-private-api-tests.md); real matching
and settlement remain separate supervised validation.

## Evidence and Python controls

[fixtures/evidence_manifest.json](fixtures/evidence_manifest.json) records bytes,
SHA-256 and evidence tiers. Primary captures are immutable. Synthetic faults,
derived replay, SDK vectors and analysis remain separately labeled. Add only
completed distinct captures with `python3 tests/predict_evidence.py --refresh`.
Review a completed resting recording with --run <directory>; do not rerun its
live supervisor to inspect old evidence.

Python supervisor/evidence controls are offline; recorder tests use localhost:

```bash
PYTHONPATH=tests uv run --python 3.12 --with requests --with websocket-client --with websockets --with python-dotenv --with predict-sdk==0.0.22 python -m unittest test_predict_evidence test_predict_private_http_recorder test_predict_resting_live test_predict_batch_live
```

The manifest verifier needs only the standard library. Supervisors require Python
3.11+. Live operators should consult the
[probe guide](../../../docs/how_to/predict-private-api-tests.md) and
[existing venue evidence](../../../docs/reference/predict-live-evidence.md) before
requesting a new funded case. No-submit preparation is not fill evidence.

## Benchmarks and explicit JSON exporters

Benchmarks use local services and do not submit venue orders:

```bash
cargo bench -p nautilus-predict --features execution --locked --bench routing
cargo bench -p nautilus-predict --features execution --locked --bench execution_entry
cargo bench -p nautilus-predict --features execution --locked --bench latency
```

For local submission registration, use the existing execution harness and a fresh
output path. Run the prebuilt optimized binary without other builds/tests on the
host when interpreting timings:

```bash
PREDICT_REGISTRATION_PATH=data/registration.json cargo test --release -p nautilus-predict --features execution --locked --test execution entry_benchmark::benchmark_submission_registration -- --ignored --exact --test-threads=1
```

The exporter uses eight-command batches, 0/1,000 retained Native orders and normal
or held cash RPC. It verifies POSTs and Native wallet acceptance, reports telemetry
loss and keeps the last 128 measurements after 32 warmup commands. Account-lock
wait and registration are measured directly; the latter includes nested lock waits
and shared-index updates; immutable snapshot construction precedes the lock.
The normal registration regression checks facts
and sample counts, with no latency threshold. These are finite loopback timings,
not live Strategy-to-venue latency or a contention guarantee.

For report CPU costs, measure collection, synchronous finalization and result
disposal separately, using a fresh output file:

```bash
mkdir -p data
PREDICT_REPORT_STAGES_PATH=data/report-stages.json cargo test --release -p nautilus-predict --features execution --locked --test execution matches::benchmark_report_collection_finish_and_drop -- --ignored --exact --test-threads=1
```

This replay covers 1 through 100,000 fills, first collection and duplicate replay,
without a retained Native order. It asserts exact report facts and no execution
event application. The in-service exporter separately includes retained Native
histories and real Strategy callbacks. Use release results for optimized CPU
comparisons: the repository's debug/test profile deliberately leaves local code
unoptimized. Collection wall time includes loopback HTTP and is not core-thread
CPU time.

For an ignored exporter, select its exact test and a fresh output path; names
below are under execution::<module>::tests in --lib:

| Module | Ignored test | Output environment variable |
| --- | --- | --- |
| account | benchmark_open_owner_retirement | PREDICT_OPEN_RETIRE_PATH |
| account | benchmark_pending_account_projection | PREDICT_DEFERRED_PROJECTION_PATH |
| fills | benchmark_wallet_fill_history | PREDICT_FILL_BENCH_PATH |
| fills | benchmark_wallet_pending_work | PREDICT_PENDING_PATH |
| fills | benchmark_native_order_snapshot_copy | PREDICT_SNAPSHOT_COPY_PATH |
| fills | benchmark_native_worker_observation | PREDICT_OBSERVER_BENCH_PATH |
| fills | benchmark_lifecycle_raw_history | PREDICT_RAW_BENCH_PATH |
| fills | benchmark_advancing_wallet_prefix | PREDICT_ADVANCING_PREFIX_PATH |
| fills | benchmark_applied_fill_inspection | PREDICT_APPLIED_INSPECTION_PATH |
| maintenance | benchmark_native_lookup_retirement | PREDICT_CLEANUP_BENCH_PATH |
| maintenance | benchmark_cold_instrument_cleanup | PREDICT_INSTRUMENT_CLEANUP_PATH |
| maintenance | benchmark_large_history_cleanup | PREDICT_HISTORY_CLEANUP_PATH |
| maintenance | benchmark_native_purge_and_adapter_retirement | PREDICT_NATIVE_PURGE_PATH |
| maintenance | benchmark_steady_native_purge_cadence | PREDICT_STEADY_PURGE_PATH |
| maintenance | benchmark_verified_owner_retirement_phases | PREDICT_RETIREMENT_PHASE_PATH |
| maintenance | benchmark_native_position_purge_history | PREDICT_POSITION_PURGE_PATH |
| maintenance | benchmark_native_timer_evidence_sweeps | PREDICT_TIMER_EVIDENCE_PATH |


Actual Strategy exporters under --test execution include:

| Test under node_host | Output environment variable |
| --- | --- |
| continuous::benchmark_warmed_callback_immediate_maker_and_taker | PREDICT_CONTINUOUS_PATH |
| continuous::extended_callbacks_with_suspended_recovery | PREDICT_SUSTAINED_PATH |
| continuous::benchmark_active_order_callback_scaling | PREDICT_ACTIVE_PATH |
| in_service::benchmark_real_timer_evidence_strategy_delivery | PREDICT_LIVE_TIMER_PATH |
| in_service::benchmark_in_service_strategy_pauses | PREDICT_INSERVICE_PAUSE_PATH |

Prebuild revisions and alternate matched executables in ABBA order with warmups
excluded. Keep Native source, logger, harness, features and output policy fixed;
avoid concurrent builds/tests during measurement. PREDICT_CONTINUOUS_LOG_LEVEL
allows matched INFO/WARN controls. CPU exporters exclude different work from
end-to-end callbacks: preserve each output's declared boundary, report losses,
and never sum independent percentiles. Cold-history and purge stress tests do
not establish normal HFT p99 or constant-time disposal. Exporters assert delivery
and final state, not a latency budget: a passing exporter can still reveal an
unacceptable pause. The timer exporter labels report collection/finalization,
wallet validation and retirement windows separately; overlapping backlog is not
CPU time attributable to the timer. See
[telemetry](../../../docs/predict-execution-adapter-design.md#optional-latency-telemetry).

## Add a scenario

Reuse the smallest owning harness and causal barriers. Assert request fields/counts,
actual Native state and subscriber callbacks. Preserve captured economic facts and
label transformations. Money-sensitive failures need zero writes and a valid
nonempty control. Do not add a production accessor, second harness/state machine,
or change historical expectations simply to make a regression pass.
