# Predict execution structure review

This review describes current ownership and remaining implementation costs. See the
[architecture](predict-execution-adapter-design.md) for the API contract,
[development status](predict-execution-work-plan.md) for priorities and
[validation record](reference/predict-execution-validation.md) for measurements.

## Execution boundaries

| Path | Current boundary |
| --- | --- |
| Submit | Prepared local context, protocol validation, signing and one create POST |
| Cancel | Known numeric route and one remove POST; missing IDs/noop/unknown outcomes use targeted recovery |
| Own lifecycle WS | Native events for Accepted/Canceled/Rejected/Expired |
| Confirmed settlement | Raw precision validation and Native fill/report application |
| Early settlement | Account/strategy-scoped CustomData, without economic state changes |
| Accounts | Local reservation projection; asynchronous remote cash/OPEN verification |
| Explicit queries | Requested order/report scope, without mandatory full-account audit or cleanup |
| Recovery | Native scheduling, scoped facts and post-IO verification |
| Retirement | Existing timer, Native owner lifetime and retained unapplied facts |

Per-command funds/OPEN audits, cumulative attempted-BUY limits, full-map REST route
insertion, per-event order-history clones and fixed 10,000-entry trading caps have
been removed. They are not outstanding refactoring tasks.

## Module ownership

| Modules | Responsibility and boundary |
| --- | --- |
| mod, factory, types | Native client, trait delegation, construction and shared dependencies |
| orders, submitter, order_builder | Command handling, async transmission and pure signed-amount conversion |
| cancellations | Single/batch removal and unknown outcome classification; no synthetic exchange amendment |
| runtime | Transport loop plus the existing per-message WalletReportContext method |
| context | Native order context and immutable borrowed history; no independently advanced order state |
| routing | Signed hash to numeric REST endpoint ID; lifetime follows the owner |
| precision, settlement | Final raw economic evidence versus provisional settlement notification and deduplication |
| account, resources | Asynchronous cash/local locks and exact resource arithmetic; no strategy admission budget |
| reports, reconciliation, matches, recovery, external | Target/bulk collection and known/external Native report conversion |
| inventory_scope, readiness, reconnect | Inventory-related recovery coverage, post-IO verification and existing timer scheduling |
| metadata, authentication | Background market preparation and signer/session continuity |

Polymarket also separates context, fills, settlement, commands and reports.
Bybit/OKX retain protocol/routing validation and async error classification. Predict's
SDK specifies signing/amount conversion, not Native Cache/Portfolio ownership.
The number of files alone does not establish duplication.

The hash and numeric-ID indexes have distinct protocol identities. Raw evidence
and early settlement notices have distinct final/provisional lifetimes. Sharing
immutable facts may be useful, but deleting either responsibility or introducing
another ledger would change the contract.

## Public API and test boundaries

Client/config/factory types, protocol conversion functions, custom-data types and
optional host telemetry are intentional Rust APIs. Internal context, account,
precision, recovery and worker modules remain private; public methods inside an
inaccessible module are not external exports. Native state is changed through
its engine APIs rather than adapter test hooks.

Private helper tests use cfg(test). Strategy/mock implementations and benchmark
assembly remain under tests/ or benches/. Documentation and test convenience must
not introduce production-only accessors, alternate state models or credential
loading. An API key is a credential, distinct from the adapter's public Rust API.

## Remaining costs

| Area | Current limitation | Direction |
| --- | --- | --- |
| Cold evidence and destruction | First validation and one large owner's final release remain O(N); physical Native purge is synchronous | Measure full cleanup and actual callbacks before changing scheduling |
| Full recovery/projection | Work still scales with retained owners/facts and external OPEN | Reuse collected inputs; keep these operations separate from ordinary commands |
| Instrument checks and OPEN COW | Ownership checks and snapshots with old readers may require linear work | Remove proven repetition without a permanent parallel ownership/funds index |
| Settlement/evidence fields | Some immutable raw fields remain in both provisional and final records | Preserve Submitted/Failed/late Success semantics before considering sharing |
| Runtime/report orchestration | Dense branches and bulk/context preparation remain | Simplify actual message paths, retain batch atomicity and position-only protection |

Automatic recovery checks unresolved commands before enqueue/capture, while explicit
QueryAccount still refreshes. Retirement reuses complete membership proof in the
existing prefix slot; a membership-only cursor is not a monetary zero summary.
Proof releases shared account/order locks, and deletion rechecks current ownership.
Reset retains ambiguous cancellations, including restored orders.

The evidence demonstrates less repeated work, not universal speedup: a verified
multi-owner sweep regressed in the latest observation, and one 100,000-fill cold
owner still pauses for about 23 ms. Keep quantitative results in the validation
record; do not substitute short-order replay for long-history measurements.
