# Predict adapter tests

Normal tests use immutable fixtures and loopback HTTP/WS/RPC. They do not load
operator credentials or submit venue orders. Run only named ignored exporters or
probes; never run the entire ignored set. Current behavior is defined in the
[architecture](../../../docs/predict-execution-adapter-design.md); accepted results
and source/evidence limits are in the
[validation record](../../../docs/reference/predict-execution-validation.md).

## Test layers

| Location | Boundary |
| --- | --- |
| unit/ | Private precision, resources, account, routing, settlement and lifecycle helpers via cfg(test) |
| private_http/, private_ws.rs, blockchain.rs, http_security.rs | Wire schema, authentication, pagination, canonical chain reads |
| execution/mock_venue.rs | Shared local venue, configurable faults and observed requests |
| execution.rs, execution/ | Native execution/Portfolio, commands/reports and ownership |
| execution/node_host.rs and support/ | Product factories, LiveNode, Rust strategies and actual callbacks |
| data_client/, instrument_requests.rs | Discovery, subscriptions, reconnect and data responses |
| capture_rollover.rs, catalog_readback.rs, quote_rollover.rs | Expiry handover and persisted public data |
| private_api_live.rs | Explicit read-only or supervised live probes; ignored by default |

Direct-client tests deliberately bypass RiskEngine for protocol/ambiguous-outcome
coverage. LiveNode tests assert real Native risk/state and strategy callbacks.
Neither substitutes for the other. Benchmark/exporter tests are reproducible tools,
not temporary files merely because they are ignored by default.

## Run offline tests

Run from the repository root against the matching repaired Native sibling tree.
See the [dependency handoff](../../../docs/reference/predict-execution-validation.md#two-repository-handoff).
Serial scheduling is the accepted full-suite baseline; business-internal WS/HTTP/
strategy concurrency still runs within these scenarios.

```bash
cargo test -p nautilus-predict --features native-quantity-commission --locked -- --test-threads=1
cargo clippy -p nautilus-predict --features native-quantity-commission --all-targets --locked -- -D warnings
cargo check -p nautilus-predict --no-default-features --features native-quantity-commission --locked --lib
cargo check -p catalog-capture-cli --no-default-features --features predict-execution,nautilus-predict/native-quantity-commission --locked
```

Legacy reconciliation can be checked separately with --features execution. It is
not the same inventory-fee mapping or the current maker/taker validation target.
Set NO_PROXY=127.0.0.1,localhost if local proxies interfere with loopback services.
Do not use a full ignored run as a replacement for either offline command.

Python supervisor/evidence tests are offline (HTTP recorder tests use localhost):

```bash
PYTHONPATH=tests uv run --python 3.12 --with requests --with websocket-client --with websockets --with python-dotenv --with predict-sdk==0.0.22 python -m unittest test_predict_evidence test_predict_private_http_recorder test_predict_resting_live test_predict_batch_live
python3 tests/predict_evidence.py
```

The supervisor tests require Python 3.11+; the command selects 3.12 and its
dependencies explicitly rather than macOS system Python. The manifest verifier
alone needs only the standard library. No live supervisor entry point is invoked.

## Execution regressions

| Area | Tests and observable contract |
| --- | --- |
| Submission and risk | submissions, multi_admission, node_host: no funds/OPEN preflight, venue rejection forwarded, Native notional denial before POST, independent maker/taker intent |
| MARKET preparation | market_preparation, market_taker, market_orders: exact strategy reference, BUY/SELL/YES/NO amounts, one slippage application, no book GET, cold/expired readiness denial, TTL-zero rejection |
| Wallet lifecycle | wallet_lifecycle: direct Native events, typed reasons, Cache applied before callback, duplicate/queued acceptance suppression, one terminal callback |
| Precision and fees | unit/precision, execution/precision, quantity_commission, quantity_fee_cost: exact raw wei, cumulative rounding, net BUY shares, SELL collateral fee, atomic conflict checks |
| Early matching | unit/settlement and node_host/continuous: actual on_data before Success, Submitted/Failed without economic fill, scoped routing, duplicate/late callbacks |
| Accounts | unit/account, snapshot_multi, continuous: held RPC does not block lifecycle, per-order projection, tight-cash callback replacement, no stale relocking or cross-strategy release |
| Recovery | query_recovery, bulk_reports, recovery_live, strategy_lifecycle: explicit QueryAccount, missing WS, terminal late fills, stable IDs, bounded owner scope, post-IO fences |
| Retention | unit/lookup and in_service: Native purge, pending/unapplied owner protection, cutoff identity, bounded retirement, actual timer and strategy delivery |

Confirmed cash refresh is asynchronous; no ordinary Accepted/confirmed cancel
forces OPEN/RPC. Synthetic Native cancellation cannot release a real remote OPEN
row. Freshness, precision, ownership and batch atomicity checks remain. Unit work
counters/negative controls establish removed copies/scans; short continuous orders
alone do not prove long-history behavior.

Focused examples:

```bash
cargo test -p nautilus-predict --features native-quantity-commission --locked --test execution market_preparation::
cargo test -p nautilus-predict --features native-quantity-commission --locked --test execution wallet_lifecycle::
cargo test -p nautilus-predict --features native-quantity-commission --locked --test execution node_host::continuous:: -- --test-threads=1
cargo test -p nautilus-predict --features native-quantity-commission --locked --test execution node_host::in_service:: -- --test-threads=1
```

### Cancellation coverage

node_cancellations covers removed/noop/rejected partitions, wallet gaps, real HTTP
timeouts, QueryOrder/periodic recovery, duplicate reports and Native retry exhaustion.
Noop is not a cancellation receipt; unknown write outcomes are not automatically
resent. Read the [Native cancellation limit](../../../docs/predict-execution-adapter-design.md#native-cancellation-recovery-limit)
before interpreting timeout-derived Canceled as proof of venue removal.

### Recovery and retirement controls

- unresolved_commands_prevent_automatic_readiness_capture: installed VirtualClock
  timer queues nothing for three unresolved ticks, then resumes when resolved.
- automatic_gap_with_unacknowledged_order_avoids_io_but_explicit_account_query_refreshes:
  actual reconnect makes no futile automatic IO; explicit query still reads/publishes.
- reset_preserves_unresolved_cancel_ownership_until_reconciled: restored order without
  local submit context retains ambiguous cancellation across rejected reset.
- cold_membership_does_not_hold_account_or_order_registry_locks and
  changed_worker_view_between_proof_and_retirement_is_retained: worker interleaving
  probes real guard ownership and preserves a concurrently replaced owner.
- retirement_membership_cursor_does_not_substitute_zero_raw_economics: membership
  proof never supplies zero monetary normalization.
- Position purge tests establish both forward links through Native add_position_id
  before checking their removal; empty links cannot make the assertion pass.

## Benchmarks and explicit JSON exporters

Criterion-compatible benchmarks use local services, without venue credentials:

```bash
cargo bench -p nautilus-predict --features native-quantity-commission --locked --bench routing
cargo bench -p nautilus-predict --features native-quantity-commission --locked --bench execution_entry -- execution/market_to_post_strategy_price
cargo bench -p nautilus-predict --features native-quantity-commission --locked --bench execution_entry -- execution/cancel_to_reported_cash_rpc_5ms
cargo bench -p nautilus-predict --features native-quantity-commission --locked --bench latency
```

Named ignored release tests write create_new JSON. Choose a fresh output file under
ignored data/predict-validation/<run-id>/; an existing file must fail, not overwrite
evidence. Do not run performance exporters alongside compilation/full suites.

```bash
mkdir -p data/predict-validation/my-review
PREDICT_TIMER_EVIDENCE_PATH=data/predict-validation/my-review/timer.json cargo test --release -p nautilus-predict --features native-quantity-commission --locked --lib execution::lifecycle::tests::benchmark_native_timer_evidence_sweeps -- --ignored --exact --test-threads=1
PREDICT_LIVE_TIMER_PATH=data/predict-validation/my-review/live-timer.json cargo test --release -p nautilus-predict --features native-quantity-commission --locked --test execution node_host::in_service::benchmark_real_timer_evidence_strategy_delivery -- --ignored --exact --test-threads=1
PREDICT_CONTINUOUS_PATH=data/predict-validation/my-review/callbacks.json cargo test --release -p nautilus-predict --features native-quantity-commission --locked --test execution node_host::continuous::benchmark_warmed_callback_immediate_maker_and_taker -- --ignored --exact --test-threads=1
```

The timer CPU fixture measures cold/verified 1×100,000 and 32×9,999-fill owners,
including destructors. The actual LiveClock fixture imports 100,000 REST facts,
receives independent WS books and observes Rust strategy callbacks. Periodic account
IO participates; its waiting window is not timer CPU. The continuous fixture reports
paired actual Filled/cancel-replacement spans separately from setup and shutdown.
Every sent book must arrive; the timer evidence replay permits zero create/remove
writes. None measures venue network latency or proves a hard pause deadline.

Retained unit exporter index (each name is under execution::<module>::tests in --lib):

| Module | Ignored test | Output environment variable |
| --- | --- | --- |
| account | benchmark_open_owner_retirement | PREDICT_OPEN_RETIRE_PATH |
| account | benchmark_readiness_account_capture | PREDICT_ACCOUNT_CAPTURE_PATH |
| account | benchmark_pending_account_projection | PREDICT_DEFERRED_PROJECTION_PATH |
| account | benchmark_retained_readiness_construction_and_release | PREDICT_RETAINED_READINESS_PATH |
| precision | benchmark_wallet_fill_history | PREDICT_FILL_BENCH_PATH |
| precision | benchmark_native_order_snapshot_copy | PREDICT_SNAPSHOT_COPY_PATH |
| precision | benchmark_native_worker_observation | PREDICT_OBSERVER_BENCH_PATH |
| precision | benchmark_lifecycle_raw_history | PREDICT_RAW_BENCH_PATH |
| precision | benchmark_advancing_wallet_prefix | PREDICT_ADVANCING_PREFIX_PATH |
| precision | benchmark_recovery_match_fence | PREDICT_RECOVERY_FENCE_PATH |
| precision | benchmark_applied_fill_inspection | PREDICT_APPLIED_INSPECTION_PATH |
| lifecycle | benchmark_native_lookup_retirement | PREDICT_CLEANUP_BENCH_PATH |
| lifecycle | benchmark_cold_instrument_cleanup | PREDICT_INSTRUMENT_CLEANUP_PATH |
| lifecycle | benchmark_large_history_cleanup | PREDICT_HISTORY_CLEANUP_PATH |
| lifecycle | benchmark_native_purge_and_adapter_retirement | PREDICT_NATIVE_PURGE_PATH |
| lifecycle | benchmark_steady_native_purge_cadence | PREDICT_STEADY_PURGE_PATH |
| lifecycle | benchmark_verified_owner_retirement_phases | PREDICT_RETIREMENT_PHASE_PATH |
| lifecycle | benchmark_native_position_purge_history | PREDICT_POSITION_PURGE_PATH |
| lifecycle | benchmark_native_timer_evidence_sweeps | PREDICT_TIMER_EVIDENCE_PATH |

Other strategy exporters under --test execution:

| Test under node_host | Output variable |
| --- | --- |
| continuous::benchmark_active_order_callback_scaling | PREDICT_ACTIVE_PATH |
| in_service::benchmark_in_service_strategy_pauses | PREDICT_INSERVICE_PAUSE_PATH |

The purge/retirement CPU exporters include complete cleanup, whereas copy/normalization
exporters exclude setup and Native application. Keep each JSON's declared boundaries;
a repeated cold capture is a stress control, not normal live frequency. The
[telemetry contract](../../../docs/predict-execution-adapter-design.md#optional-latency-telemetry)
requires nonblocking collection, actual callback correlation and explicit losses.

## Fixtures and captures

[evidence_manifest.json](fixtures/evidence_manifest.json) records SHA-256, size and
provenance. python3 tests/predict_evidence.py verifies it without network or mutation.

| Tier | Meaning |
| --- | --- |
| primary_capture | Sanitized venue messages actually observed |
| derived | Scenarios constructed from recorded facts |
| synthetic | Controlled local protocol/failure cases |
| reference | SDK vectors and other reference inputs |
| review | Analysis, not raw venue evidence |

Primary captures are immutable. Refresh adds distinct captures and refuses to replace
registered primary bytes. Operational recordings/logs/source archives stay ignored
under data/predict-validation/ and are not automatically promoted to fixtures.
Review completed resting captures offline using python3 tests/predict_evidence.py
--run <directory>; rerunning the supervisor would place new live orders. Consult
[existing coverage](../../../docs/reference/predict-live-evidence.md#choose-tests-without-repeating-funded-runs)
before repeating a funded case. Read-only/private probes are documented in the
[operator guide](../../../docs/how_to/predict-private-api-tests.md).

## Add a scenario

Reuse the existing venue and Native helpers in the smallest owning layer. Prefer
observable events/barriers to fixed sleeps. Assert actual request fields, state and
subscriber callbacks; money-sensitive failures need zero writes and a nonempty valid
control. Preserve two-owner, duplicate, reset, late-fill and conflict coverage where
relevant. Do not export another production API, clone a whole suite or change recorded
expectations merely to pass. A mock request is not evidence of venue acceptance.
