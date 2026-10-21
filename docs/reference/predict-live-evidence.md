# Predict live execution evidence

This index describes observed mainnet behavior, not permission to run funded tests.
Frozen regression JSON/JSONL and Native journals remain under
[`tests/fixtures/private_api`](../../crates/nautilus-predict/tests/fixtures/private_api/).
The [manifest](../../crates/nautilus-predict/tests/fixtures/evidence_manifest.json)
records hashes and provenance. Derived replays and synthetic fault cases are
separate from actual exchange observations.
New operational recordings belong under Git-ignored `data/predict-validation/`.
They are not added to the frozen manifest unless a regression needs them.

## Coverage

| Capture | Observed behavior | Limit |
| --- | --- | --- |
| [Maker matrix](../../crates/nautilus-predict/tests/fixtures/private_api/maker_live_matrix_20261015.json) | YES/NO x GTC/GTD, eight accepted post-only LIMIT orders, modify rejection, cancel/replacement, zero fills, final locks/open orders zero | No atomic amend or natural GTD expiry claim |
| [HTTP batch](../../crates/nautilus-predict/tests/fixtures/private_api/live_resting_http-batch_20261016T160926Z/) | Two orders recovered into empty Cache; one multi-ID removal request; two Accepted and Canceled callbacks, zero fills, final OPEN zero | Actual WS confirmations preceded REST completion; opposite ordering is replay coverage |
| [Funded entry](../../crates/nautilus-predict/tests/fixtures/private_api/live_fill_20261017T051620Z/) | BUY gross 32 at 0.040; fee 0.64 SHARES; net inventory 31.36; maker TP left active | Entry and interruption evidence, not a profitable strategy result |
| [Read-only adoption](../../crates/nautilus-predict/tests/fixtures/private_api/exit_only_restart_20261017T052850Z/) | Empty-Cache startup restores the holding and existing TP without adding orders | Exchange reconciliation, not Redis restoration |
| [Exit-only execution](../../crates/nautilus-predict/tests/fixtures/private_api/exit_only_restart_20261017T053156Z/) | Existing TP canceled; MARKET SELL 31.36 filled; PositionClosed and final flat account | One full exit, not partial-exit or long-outage coverage |
| [Independent exit collection](../../crates/nautilus-predict/tests/fixtures/private_api/exit_only_restart_20261017T053156Z_independent/) | Wallet, matches and chain transfers corroborate exit economics | Separate collector, with its own provenance |
| [Read-only session and queries](../../crates/nautilus-predict/tests/fixtures/private_api/live_readonly_resources_20261017T100325Z/) | Early JWT replacement, WS resubscription, QueryAccount/readiness and QueryOrder; 101 and 52 successful requests, zero order/chain writes | Empty account; canceled-order repair combines a real historical response with derived stale local state |
| Operator-run YES/GTD, 2026-10-20 | Two Accepted, two ModifyRejected with unchanged terms, two Canceled; independent wallet confirmation; zero new matches, cash/holdings unchanged, final OPEN zero | Revalidates one existing maker case; does not extend the matrix or prove callback latency |

Capture-directory timestamps use UTC. Earlier successful and failed captures are
retained; the table lists representative complete runs rather than every attempt.

## Latest revalidation and request costs

The operator-run YES/GTD case completed on 2026-10-20 at 17:14:08 Asia/Shanghai
(09:14:08 UTC). Its ten files were moved unchanged from transient fixtures to
`data/predict-validation/live_resting_yes-gtd_20261020T091408Z/`; all file hashes
were checked before and after the move. The offline report is
`data/predict-validation/resting-review-20261020.json`.

The recorded relay saw 26 HTTP requests: two create POSTs, two remove POSTs,
16 OPEN-order GETs, two market GETs, two account GETs and one authentication
challenge/JWT pair. The supervisor, independent collector and public book
confirmation use additional connections; 26 is not the whole-run request count.

| Recorded operation | First order | Replacement |
| --- | ---: | ---: |
| Create HTTP duration | 422.753 ms | 407.783 ms |
| Remove HTTP duration | 525.169 ms | 383.578 ms |

These are local relay wall-clock intervals through upstream response. They
include relay overhead and do not measure WS arrival or strategy callback delay.
The resting harness sets `account_refresh_interval_secs = 1`; each account refresh
queries OPEN orders before reading chain balances. Startup, per-submit resource
admission and final cleanup also query OPEN orders. The transcript has no
per-request caller tag, so it cannot assign all 16 reads to those callers exactly.
Do not interpret them as 16 pre-submit checks or remove resource checks on that basis.

The harness deliberately waits about two seconds after each modify rejection
before requesting cancellation and independently refreshes a public book about
every 500 ms. Those are test observation/safety costs, not evidence of slow WS
execution feedback. No adapter performance change is justified by these timings.

Post-outage read-only probes on the same day passed with 42 query requests and
83 session requests, including ten wallet frames and two early JWT renewals.
The longer public observation suffered repeated DNS failures and remains
inconclusive for uninterrupted rollover/data delivery. Bounded cache samples
during a stalled feed do not establish long-run cache safety.

The latest recording did not include a source/features metadata file. Keep that
provenance gap explicit rather than retroactively filling it from today's files.
Future resting runs record the selected features, source commit, tracked-diff
hash, lockfile and relevant harness hashes in `run_metadata.json`.

## Choose tests without repeating funded runs

| Changed boundary | First check | Reason to repeat mainnet |
| --- | --- | --- |
| Documentation, evidence review, archive paths | Offline helper tests and frozen manifest verification | None |
| Parsing, units, signing, numeric REST IDs | Existing protocol/SDK vectors and recorded replay | Changed venue contract or unresolved disagreement with captured facts |
| Authentication, queries, wallet recovery | Session/query offline tests, then one named read-only probe | Changed credentials, transport or authentication behavior |
| Submit/cancel routing, Native dependency update | Execution/Native tests and one representative resting case | Changed behavior has no current venue evidence; operator starts the funded runner |
| YES/NO or GTC/GTD mapping | Existing four-case matrix and precision assertions | The specific mapping or venue expiry rule changed |
| Batch or empty-Cache recovery | Existing batch captures and loopback lifecycle tests | The batch/recovery boundary changed; do not repeat it after unrelated changes |
| Filled accounting and fees | Existing entry/exit captures and offline quantity-commission regressions | Defer new fills to the separately planned strategy validation |

Order writes alone do not justify a whole matrix run. Select the case that
exercises the changed boundary, retain failed attempts as well as successes,
and require a concrete unresolved question before another funded run.

Use the existing evidence tool to recheck a completed resting directory:

```bash
python3 tests/predict_evidence.py --run data/predict-validation/live_resting_yes-gtd_20261020T091408Z
python3 tests/predict_evidence.py
```

`--run` uses only the Python standard library. It reads the same validation
functions used by the supervisor, verifies the independent final checks, prints
request counts/timings and file hashes, and never opens a network connection,
loads credentials or changes captured files. Missing final checks fail review;
the intermediate `zero open orders` message alone is not completion.

## Startup, fill and exit flow

The funded restart began without persisted Native Cache. InstrumentProvider
loaded markets required by effective holdings and OPEN orders. Native mass-status
reconciliation restored the entry's required settlement prefix, net holding,
reported cash and existing TP. The strategy registered external claims before
startup and adopted that state in `on_start`.

The first adoption run submitted nothing and received no new runtime Filled or
PositionOpened callbacks for historical activity. During the separately authorized
exit run, a relay prohibited BUY from the outset. The strategy canceled the
existing TP, waited for a Native terminal state, then submitted the inventory-backed
MARKET SELL. The observed callback sequence was:

```text
on_order_pending_cancel -> on_order_canceled
-> on_order_submitted -> on_order_accepted -> on_order_filled
-> on_position_closed
```

Canceled and Filled each occurred once. REST `removed` and wallet cancellation
referred to the same order without duplicate strategy transitions. During the
SELL Filled callback, Native Order was FILLED with leaves zero and cached net
inventory was already zero. The subsequent PositionClosed also showed zero.
A complete exit does not require PositionChanged; restored inventory does not
require a new runtime PositionOpened.

## Account and PnL observations

SELL execution price was 0.039. The reported collateral fee was 0.02446080 USDT,
so gross proceeds of 1.22304000 became net proceeds of 1.19857920 USDT.
The final independent account check showed no open orders or effective holdings.

The account refresh followed Filled asynchronously. The strategy observed the
new balance through applied Native Cache state, rather than a guaranteed new
account-topic callback. No adapter-owned balance ledger or synthetic notification
was introduced.

| Measure | Value |
| --- | ---: |
| Entry collateral spent | 1.28000000 USDT |
| Exit net proceeds | 1.19857920 USDT |
| Round-trip cash difference | -0.08142080 USDT |
| Native quote realized PnL | -0.05582080 USDT |
| Entry inventory fee at entry price | 0.02560000 USDT |

The difference is consistent with the existing Native base-commission convention:
quote realized PnL is not an all-fee cash return. See
[inventory-unit commissions](quantity-commission.md). These values describe one
recorded cycle and are not a fee schedule or return forecast.

## Protocol findings and evidence limits

- The maker matrix retained a real `create_order_expiry_too_soon` response: venue
  arrival must leave at least two minutes of validity. The SDK's future-time
  check alone did not establish this venue constraint. Current admission rejects
  insufficient local validity without silently extending the user's expiry.
- Modify rejection is a standard local response to an unsupported amend operation.
  Subsequent cancel/replacement was verified; atomic amendment was not.
- Batch removal uses numeric REST IDs after hash verification. `removed` and
  `noop` are per-item results, not an atomic batch guarantee. Mixed failures and
  REST-first duplicate handling have offline coverage, not new live observations.
- Session probes used genuine authentication and two early renewals. They did
  not wait for natural JWT expiry or establish prolonged network-outage behavior.
- Real partial SELL, cancellation/fill races and competing strategies' inventory
  remain outside these live runs. Deterministic offline scenarios cover their
  intended Native behavior.

The exit-only helper deliberately adopts one entry and at most one existing SELL
LIMIT. Ambiguous ownership is rejected; this is not a generic multi-strategy
recovery framework. Unsupported historical post-only metadata remains unknown.


## Optional latency pilot, 2026-10-20

The single release run at
`data/predict-validation/live_resting_yes-gtd_20261020T113531080073Z/`
created and canceled two distant post-only GTD BUY orders, each 1 USDT. Both
independent wallet facts and Native callbacks showed no fills; matches, cash,
token balances and positions were unchanged. Complete independent REST OPEN
queries after capture and again after the read-only RPC comparison returned zero.
No fallback removal was needed. Raw facts, monotonic metrics, request journal,
`latency_analysis.json`, `rpc_batch_comparison.json` and the final
`optimization_decision.json` remain together. Review this run offline rather than
submitting another pair of orders.

| Interval | p50 ms | p95 / p99 ms | Completed samples |
| --- | ---: | ---: | ---: |
| Decision → create call | 437.150 | 801.477 | 2 |
| Per-submit funding checks | 436.247 | 451.186 | 2 |
| Create client call | 186.813 | 220.035 | 2 |
| Cancel client call | 176.650 | 190.700 | 2 |
| Cancel decision → strategy callback | 177.176 | 191.396 | 2 |
| WS handler → actual strategy callback | 0.338 | 0.878 | 2 |

Two samples make p95/p99 maxima, not production tail estimates. The matched WS
callbacks were Accepted events. REST-first cancellation and later WS confirmation
do not require duplicate strategy transitions. Four unmatched cancellation marks
remain explicit; one interrupted background OPEN span at shutdown is excluded.
No samples were dropped and no interval was invalid. Native runner queues were
empty at the shutdown snapshot; this does not establish peak queue depth.

The execution probe counted 51 RPC calls, 16 OPEN page attempts, two creates and
two removals; the private HTTP relay recorded 27 requests including setup and
cleanup. Independent observers and public book/discovery reads are outside these
counts. The one-second account refresh is a calibration setting; production's
existing default is 30 seconds plus event-driven wakeups. Metadata had one cold
miss and one warm hit (50% overall); the cold GET cost 347.628 ms. Funding explains
approximately 54% of the two measured submit-through-HTTP-ACK intervals and nearly
all of warm preparation. HTTP client spans include the safety relay; separately
recorded upstream durations expose that boundary.

A subsequent read-only comparison ran three paired trials at a fresh canonical
block per pair, reading identical balance, allowance and nonce values. The three
parallel requests had median 163.165 ms; one batch had median 162.421 ms. Means
were 162.891 and 163.588 ms respectively, with no clear speed improvement. These
are Python transport measurements, not a Native Rust batch benchmark. Batch
changes three HTTP requests to one for that stage (five to three for complete
per-order funding checks), while retaining three serial dependency stages.

The chosen follow-up is a read-only comparison of Native RPC transport/endpoint
and deployment RTT. Production batching is not enabled. Batching remains a quota
candidate if needed; each submission must still read a fresh block, use that
exact hash for all funds/nonce reads, recheck canonicality, account for Native
reservations, and retain final fill/registration and recovery fences. No funds,
nonce or allowance cache across orders was added.
