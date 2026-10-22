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
| runtime | Wallet transport consumption, association and lifecycle/settlement dispatch |
| context | Native order context and borrowed immutable history |
| routing | Signed hash to numeric REST endpoint ID, retained with its owner |
| precision, settlement | Confirmed economic evidence and provisional settlement notifications |
| account, resources | Asynchronous cash reporting, local reservation projection and exact arithmetic |
| reports, reconciliation, matches, recovery, external | Scoped collection/conversion and Native own/external report delivery |
| inventory_scope, readiness, reconnect | Recovery coverage, post-IO verification and Native timer coordination |
| metadata, authentication | Background market preparation and signer/session continuity |

Native Engine, Cache and Portfolio own order and position state. Adapter context
observes that state; it does not advance a parallel order model. The signed hash
and numeric REST ID remain separate because they serve different venue endpoints.
Provisional settlement notices and confirmed raw evidence likewise have different
lifetimes and economic meaning.

## Shared-state boundaries

- Wallet normalization borrows applied/queued trade indexes instead of copying
  an order's complete event history. Cold or replaced histories are fully validated.
- Order/evidence sampling follows the Native observer's publication lock order.
  Account locks are released before normalization; evidence remains protected
  through event enqueue and queued marking.
- Retirement membership proof is not an economic summary. Disposal rechecks current
  ownership and preserves pending/unapplied facts, including ambiguous cancellations.
- Explicit account queries refresh independently of automatic readiness eligibility.
  Network reads are followed by the required state checks before applying results.
- Closed-owner cleanup follows Native retention. Large final references are released
  outside shared guards; no separate disposer or cleanup scheduler is maintained.

## Implementation limits

| Area | Cost or constraint |
| --- | --- |
| Cold evidence | First complete validation scales with the retained fill history |
| Physical purge | Native purge and one oversized owner's final destruction remain synchronous and linear |
| Recovery/projection | Full captures scale with relevant owners, facts and external OPEN rows |
| Instrument ownership and OPEN snapshots | Scans and copy-on-write can be linear while old readers remain |
| Settlement facts | Some immutable fields exist in provisional and confirmed records to preserve Submitted/Failed/late Success semantics |
| Bulk reports | Preparation scales with requested scope; batch atomicity and position-only recovery coverage remain required |

The bounded retirement scan is not a hard callback-pause guarantee or a promise
of constant memory for unresolved activity. Prefer measured removal of repeated
work over additional ownership indexes, funds ledgers or background management.

## Public and test surfaces

Client/config/factory types, protocol converters, custom data and opt-in execution
telemetry are intentional Rust APIs. Worker, precision, account and recovery
modules stay private. Test strategies, mock venues and recording utilities remain
under tests/ or benches/ and are not installed in production clients. Credentials
are supplied by the host; tests do not require production accessors.
