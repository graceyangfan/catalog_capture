# Predict adapter validation

This reference links the [behavioral contract](../predict-execution-adapter-design.md)
to executable checks and immutable evidence. The [test guide](../../crates/nautilus-predict/tests/README.md)
contains commands. Correct offline replay does not establish new venue behavior,
production latency or unattended operating duration.

## Two-repository handoff

Cargo uses the sibling `nautilus_trader_quantity_commission` checkout. CI and
bootstrap select the project fork
[graceyangfan/nautilus_trader](https://github.com/graceyangfan/nautilus_trader)
at [acd6c83048](https://github.com/graceyangfan/nautilus_trader/commit/acd6c830484c91815fa3bee14ed2fc090bf8b006).
The immutable revision contains the merged official framework, quantity
commissions, opt-in pending-command retention and terminal quantity normalization.
These are downstream extensions, not a claim of an unmodified official release.
Use the pinned APIs directly; no legacy fee mode or compatibility branch exists.

| Capability | Native owner |
| --- | --- |
| Predict retains unresolved cancel/modify commands and emits CommandRecoveryExhausted | ExecutionClient contract, live manager and node/msgbus |
| Confirmed FILLED quantity normalization while a command is pending | Execution reconciliation and regression tests |

CI and bootstrap use the same immutable revision. Build releases from clean sibling
checkouts, and keep the installation reference aligned with the CI pin. Local
uncommitted changes are not a reproducible dependency version.

Native owns quantity commission, exact gross/order scale alignment, checked report
application and portfolio accounting. See [inventory-unit commissions](quantity-commission.md).
The current adapter uses ExecutionReportTask collection/finish and batch
instrument_ids APIs; single-member endpoints validate through single_instrument_id().

## Comparison basis

Comparisons use the pinned Native fork and official TS SDK 1.3.8 at
[5ff2b4a](https://github.com/PredictDotFun/sdk/tree/5ff2b4a1c54cbbeb0fd661e17246cd12a9af8486).
They are specific contracts, not assertions that adapters have identical protocols.

| Source path in Native / SDK | Relevant rule applied to Predict |
| --- | --- |
| Native adapters/bybit/src/websocket/parse.rs | FillReport uses actual quantity, price, maker/taker and fee; local post-only intent cannot rewrite a fill |
| Native adapters/bybit/src/websocket/parse.rs, Rejected status mapping | PartiallyFilled cannot transition to Rejected; its venue-specific Canceled conversion does not establish Predict duplicate/partial semantics |
| Native adapters/deribit/src/websocket/parse.rs | Canceled/Expired directly express venue lifecycle; no trade-history completeness wait |
| Native adapters/polymarket/src/execution/ | Separate transport receipts, execution facts, reports and explicit cancellation; Native owns final state |
| Native adapters/bybit and adapters/okx command parsing | Ordinary Native fields plus typed venue-specific parameters; unsupported operations fail explicitly |
| Native common/src/cache and execution reconciliation | Retention/purge and late-fill application belong to framework lifecycle |
| Native adapters/hyperliquid/src/signing/nonce.rs | Clock-based nonce is venue-specific; do not copy its freshness policy to Predict |
| Native adapters/lighter/src/signing/nonce.rs and execution.rs | Sequential nonces belong to an account/API key; Predict caches contract state and does not allocate a nonce per order |
| SDK src/OrderBuilder.ts and src/abis/CTFExchange.json | salt, expiration and nonce are separate; nonces(owner)/incrementNonce/InvalidNonce are verifying-contract facts |

Predict therefore caches contract nonces without a clock TTL and invalidates only
the disproved contract entry. Native API integration and SDK arithmetic have
different authority: the SDK does not define Cache, portfolio or callback semantics.

## Recorded JSON replay matrix

Fixture paths are relative to
[private_api](../../crates/nautilus-predict/tests/fixtures/private_api/).
Original sources remain unchanged. Rebinding identity, signing or time to a local
venue creates derived replay, even when all economic fields come from real fills.

| Captured input | Executable consumer | Observable result |
| --- | --- | --- |
| live_resting_fixed_20261015/ | maker_lifecycle; live_node | Native maker acceptance/cancel/replacement and callbacks |
| live_resting_batch-restart_20261016T153745Z/ and live_resting_http-batch_20261016T160926Z/ | batch_recorded | Empty-Cache adoption, scoped multi-ID remove, per-order terminal callbacks |
| live_fill_20261016T123240Z_independent/calibration.json plus admission.json | cold_start; recorded_startup | Old flat cycles do not become active orders; actual BUY/fee/TP/cancel/SELL economics pass through Strategy/LiveNode |
| live_fill_20261016T122233Z_independent/calibration.json | strategy_lifecycle | Real fill facts inside restart/late-fill and strategy exit scenarios |
| native_market_fill_post_only_rejected_20261025.json | market_recorded_fill; feedback_delivery; market_taker | Actual 3.5 gross/.07 fee/3.43 net and distinct rejection/SDK-exit provenance |
| native_non_fok_full_plan_two_settlements_20261025.json | market_recorded_fill | Exact two-settlement gross/net, mint evidence and cash arithmetic |
| native_market_filled_atomic_remainder_20261025.json | terminal_recovery | Native FILLED recovery, one-wei remainder, pending commands, exact fees, replay and original signed terms |
| live_chain_atomic_rounding_20261025.json | chain_recorded_rounding | Public transaction inputs/receipts reproduce the one-wei split; chain remaining uses maker-asset units and does not override CLOB FILLED |
| live_native_zero_fill_20261025/ | Explicit Native live probes; source index and immutable manifests | LIMIT accepted/canceled twice; MARKET FOK/GTC accepted then noMarketMatch/Rejected; zero fills, no residual OPEN; LIMIT collector incomplete and independently checked afterward |

The [MARKET](predict-immediate-order-validation.md) and
[maker](predict-maker-order-validation.md) references delimit the corresponding
live observations. Saved outputs that merely assert a result are not substitutes
for replaying source responses through the current adapter and Native.

## Fault and independence checks

| Test area | Required invariant |
| --- | --- |
| feedback_delivery | Accepted/terminal/fills survive absent ACK, held POST/history, route anomalies, expiry, clock skew and wallet reconnect; optional metadata receiver closure and a held earlier QueryOrder cannot suppress current callbacks |
| market_fok per-order slippage; node_host product profile | Omitted/zero/nonzero command params control signed amounts; Native owns risk/recovery policy without host veto; disabled startup recovery skips account discovery |
| submissions nonce controls | One rejected exchange does not invalidate another; nonce IO/cash IO independent; no normal-command RPC or POST replay |
| cancellations; maker_lifecycle | Known route avoids GET; a required GET already proving terminal avoids remove/matches; held/rejected/noop results are distinct |
| expiry_scope; recorded_startup | Historical residuals do not block reports/restart/new market; active unknown holdings remain visible errors; owned late fills survive expiry |
| ingestion_contract; factual_precision | Cache/positions are applied before subscriber callbacks; exact facts and position-snapshot coverage survive duplicate/gap recovery |
| recovery_live; feedback_delivery | Incremental REST preserves Native-applied and queued wallet fills; no fake FillVoided from an incomplete snapshot |
| node_host::cancellations | Native exhaustion diagnostic retains uncertain pending state; other orders and subsequent genuine resolution continue |
| unit lookup/lifecycle; in_service | Native retention owns mirrors; bounded retirement protects unapplied evidence |
| restart_live, explicitly selected with isolated Redis | Fresh Native caches restore persisted orders, positions and strategy intent; offline partial/full fills and duplicate history do not create another entry or exit |
| continuous extended replay; private_ws repeated reconnects | Sustained callback-driven commands continue across suspended cash/history IO; repeated connections require their own ACK and retain Native backoff |

These include synthetic timing/fault inputs. Passing them demonstrates local
interaction and query counts, not an observed exchange outage or hard p99 bound.

For pre-live validation, run the ordinary suite and the selected offline Redis,
reconnect and callback exporters in the [test guide](../../crates/nautilus-predict/tests/README.md#pre-live-checks-without-venue-orders).
Measure Native purge and large-owner disposal separately from command IO: a
nonblocking network path does not make synchronous lifecycle work constant-time.
Keep fixture setup, compilation and concurrent tests outside measured runs.
Read-only venue probes establish authentication, ACK and applied account/report
delivery; they do not establish matching, partial-tail or delayed-payout behavior.

## Rejection and terminal evidence

Wallet rejection observations, REST lifecycle status and confirmed settlements have
separate authority. Duplicate/unknown rejection or a rejection accompanied by
execution does not independently close an accepted original. The existing timer
sends a coalesced Native QueryOrder; genuine cancellation, expiry and execution
feedback remain independent. Zero-match wallet rejection followed by REST CANCELLED
preserves the already-applied rejection for both MARKET GTC and FOK.

| Retained source | What it establishes | Limit |
| --- | --- | --- |
| market_parameter_matrix_no_match_20261024.json | Valid requests with no executable match; Accepted, noMarketMatch and terminal REST observations | Does not establish termination of a partially executed remainder or successful FOK enforcement |
| live_unexecutable_rejections_20261025.json | Gateway expiry/signature validation and exact machine-error mapping | No valid signature or matching execution; validation order for valid orders is unknown |
| live_crate_signed_parameter_rejections_20261025.json | Signed expired-only requests through the crate; explicit unsupported LIMIT FOK and MARKET post-only errors | Expiry masked other options; amount mismatch was not sent; no Native strategy live run |
| Native atomic-remainder capture and derived terminal_recovery schedules | Exact gross/net inventory, pending-command recovery, replay and late real fills after a newer terminal | Combined partial/rejection schedules are synthetic, not additional venue observations |

`PostOnlyUnsupported` is distinct from a crossing post-only rejection. Real-response
replays assert Native rejection callbacks against the already-updated Cache, zero
fabricated fills and no automatic POST retry. The
[MARKET evidence](predict-immediate-order-validation.md) explains capture provenance,
terminal ownership and the remaining matching/settlement gaps.

## Evidence integrity

[evidence_manifest.json](../../crates/nautilus-predict/tests/fixtures/evidence_manifest.json)
indexes bytes and SHA-256 for primary, derived, synthetic, reference and review
inputs. Verify with `python3 tests/predict_evidence.py`. New completed fixtures
can be indexed with --refresh; registered primary bytes cannot be replaced.
Raw operational logs/source archives stay under ignored data/predict-validation/.
Published summaries do not imply those archives are distributed test inputs.

## Measurement requirements

Performance claims require matched executable revisions, Native dependency,
features, logging and workload, with compilation and other tests excluded.
Use the [explicit exporters](../../crates/nautilus-predict/tests/README.md#benchmarks-and-explicit-json-exporters)
for reproducible CPU and Strategy measurements. Keep collection wall time,
synchronous finalization, disposal and end-to-end callbacks separate. Overlapping
percentiles and sampling counts cannot be added.

Debug/test profiles leave local code unoptimized. Production CPU comparisons use
release builds. Large-history report conversion and Native physical purge remain
linear; a successful delivery assertion is not an HFT latency guarantee. Full
local logs, executable hashes and source snapshots belong under ignored
`data/predict-validation/`, rather than release fixtures or development timelines
in this reference.

## Public snapshot and native depth delivery

Tests cover exact JSON decimals, CustomSnapshot/depth/BBO subscription sharing,
managed book/quote state before Strategy callbacks, both transports, reconnect,
reset, rollover and persisted readback. Offline fault schedules establish those
local boundaries; read-only venue observations do not prove a forced outage,
natural rollover, production latency or unattended operating duration.

Event/init timestamps remain schema fields; execution telemetry is independently
opt-in. Credentials, raw operator recordings, profiler binaries and scratch output
are excluded from version control. Frozen fixture bytes and their evidence tiers
remain unchanged when documentation or code is reorganized.
