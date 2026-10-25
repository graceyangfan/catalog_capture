# Predict execution internals

The [adapter contract](predict-execution-adapter-design.md) defines supported
behavior. This reference describes internal ownership and implementation limits.
See the [test guide](../crates/nautilus-predict/tests/README.md) for executable
coverage and the [validation reference](reference/predict-execution-validation.md)
for measurements.

## Module responsibilities

| Modules | Responsibility |
| --- | --- |
| mod, factory, types | Native client, trait delegation, construction and shared dependencies |
| orders, submitter, order_builder | Command handling, async transmission and signed-amount conversion |
| cancellations | Single/batch removal and unknown-outcome classification |
| receipt | Transient LIMIT receipt metadata on the existing Native data channel |
| runtime | Wallet transport consumption, association and lifecycle/settlement dispatch |
| context | Native order context and borrowed immutable history |
| routing | Signed hash to numeric REST endpoint ID, retained with its owner |
| precision, settlement | Confirmed economic evidence and provisional settlement notifications |
| account, resources | Asynchronous cash reporting, local reservation projection and exact arithmetic |
| reports, reconciliation, matches, recovery, external | Scoped collection/conversion and Native own/external report delivery |
| inventory_scope, reconnect | Active inventory recovery scope, account refresh and Native timer coordination |
| metadata, authentication | Background market preparation and signer/session continuity |

Native Engine, Cache and Portfolio own order and position state. Adapter context
observes that state; it does not advance a parallel order model. The signed hash
and numeric REST ID remain separate because they serve different venue endpoints.
Provisional settlement notices and confirmed raw evidence likewise have different
lifetimes and economic meaning.

## Retained state and its authority

| State | Why it belongs in the adapter |
| --- | --- |
| Native worker views indexed by client ID/hash | Async workers cannot borrow Native's core-thread Cache; both indexes share immutable Native-derived views, not independently advanced order state |
| Signed hash and numeric REST ID | Native/exchange signing uses the hash; Predict removal uses the numeric endpoint ID |
| Ambiguous wallet observation | One-shot query intent dispatched to Native on the existing timer; no adapter retry loop and no delay of confirmed facts |
| Pending transmission/deferred cancel | Correlate a write and resolve routing before ACK; Native owns order status and subsequent recovery |
| Raw/queued fill evidence | Exact wei, fees and pre-application deduplication; Native remains the applied order/position authority |
| Settlement notices | Submitted/Failed are venue facts outside Native economic fills; provisional notifications never create inventory |
| Reported account baseline/locks | Convert actual chain cash and OPEN reservations for a reported account; no second risk budget or available-funds admission policy |
| Nonces and market signing metadata | Protocol inputs keyed by account/environment/contract or instrument definition; they are not per-strategy settings |

Native schedules order-query frequency and retries. Every explicit QueryOrder is
executed: an earlier query waiting for trade history cannot suppress a later
query's current terminal observation. The adapter retains no per-order query
suppression set. This follows the Bybit/Polymarket command/report boundary.

Background account refresh and explicit gap QueryAccount share collection and
projection code but have different transport conditions: the former requires an
acknowledged wallet, while the latter can verify authenticated REST/chain cash
during a wallet gap. Neither is an order-recovery loop or a submission gate.
Combining these entry points requires preserving that distinction and timely local
lock publication; method similarity alone is not evidence that one can be deleted.

## Shared-state boundaries

- Wallet normalization borrows applied/queued trade indexes instead of copying
  an order's complete event history. Cold or replaced histories are fully validated.
- Order/evidence sampling follows the Native observer's publication lock order.
  Account locks are released before normalization; evidence remains protected
  through event enqueue and queued marking.
- Retirement membership proof is not an economic summary. Disposal rechecks current
  ownership and preserves pending/unapplied facts, including ambiguous cancellations.
- Account queries refresh cash and OPEN reservations independently of unresolved
  commands. Missing contract nonces are prepared in an independent async branch of
  the same managed task; valid entries are not polled on cash or WS refresh. Historical order/holding equality
  is not a submission gate. Client-generation cancellation fences late HTTP results; a WS epoch change does not discard them. See the
  [lifecycle plan](plans/predict-execution-lifecycle-refactor.md).
- Closed-owner cleanup follows Native retention. Large final references are released
  outside shared guards; no separate disposer or cleanup scheduler is maintained.

## Implementation limits

| Area | Cost or constraint |
| --- | --- |
| Cold evidence | First complete validation scales with retained fill history; fill-report finalization still constructs/commits reports on the Native core thread |
| Physical purge | Native purge and one oversized owner's final destruction remain synchronous and linear |
| Recovery/projection | Full captures scale with relevant owners, facts and external OPEN rows |
| Instrument ownership and OPEN snapshots | Scans and copy-on-write can be linear while old readers remain |
| Settlement facts | Some immutable fields exist in provisional and confirmed records to preserve Submitted/Failed/late Success semantics |
| Bulk reports | Preparation scales with requested scope; batch atomicity and position-only recovery coverage remain required |

The bounded retirement scan is not a hard callback-pause guarantee or a promise
of constant memory for unresolved activity. Prefer measured removal of repeated
work over additional ownership indexes, funds ledgers or background management.
Worker-based HTTP collection does not isolate the synchronous finalization phase.
Large-history replay must measure callback backlog across report finalization,
wallet validation and disposal separately from the recovery timer waiting window.
Atomic amounts use checked integer-to-Decimal conversion, preserving the original
exact domain and scale. Replayed facts are checked against their committed entries;
only new IDs need a temporary batch index or insertion into retained indexes.

## Public and test surfaces

Client/config/factory types, protocol converters, custom data and opt-in execution
telemetry are intentional Rust APIs. Worker, precision, account and recovery
modules stay private. Test strategies, mock venues and recording utilities remain
under tests/ or benches/ and are not installed in production clients. Credentials
are supplied by the host; tests do not require production accessors.
