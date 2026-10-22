# Predict live execution evidence

This reference distinguishes actual venue observations from synthetic and derived
regressions. Frozen sanitized captures are indexed by the
[evidence manifest](../../crates/nautilus-predict/tests/fixtures/evidence_manifest.json).
New operator recordings stay under Git-ignored data/predict-validation/ unless a
specific regression requires an immutable sanitized fixture. Capture names use UTC.
A historical capture does not establish behavior for a different source revision.

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


## Interpretation and limits

The [adapter contract](../predict-execution-adapter-design.md) defines current
behavior. Historical recordings may contain funds/OPEN preflights, cold market
GETs or one-second account refreshes that are absent from the current normal submit
path. Their request counts cannot be used to infer current per-order work.

The representative resting revalidation contains two create/two remove POSTs,
zero fills and complete independent final OPEN checks. Its relay observed 26 HTTP
requests; supervisor, independent collector and public discovery/book connections
were outside that count. Relay HTTP intervals include its forwarding overhead,
not WS-to-strategy latency. That capture lacks source/features metadata, so it
cannot support a source-matched performance claim. New runs write source/features,
tracked-diff, lockfile and harness hashes in run_metadata.json.

Read-only session probes used genuine authentication and early JWT renewals; they
did not wait for natural expiry. A DNS-stalled observation is not evidence of
uninterrupted rollover or bounded long-running cache growth. Public read-only
5m/15m subscription/depth verification is documented separately in
[public-data validation](predict-execution-validation.md#public-snapshot-and-native-depth-delivery).

## Choose tests without repeating funded runs

| Changed boundary | First check | Reason for another venue run |
| --- | --- | --- |
| Documentation/evidence organization | Offline helper tests and manifest verification | None |
| Parsing, units, signing and IDs | Protocol/SDK vectors and recorded replay | Unresolved disagreement with venue facts |
| Authentication, queries and renewal | Offline session/query tests, then a named read-only probe | Changed transport or authentication behavior |
| Submit/cancel or dependency update | Native execution tests and existing resting evidence | The changed boundary lacks venue evidence; use one bounded authorized case |
| YES/NO, GTC/GTD or batch mapping | Existing matrix, batch and ownership replays | A specific venue rule changed |
| Fees, fills and inventory | Recorded entry/exit and quantity-commission regressions | A separately authorized funded strategy validation |

Review a completed resting capture without trading:

```bash
python3 tests/predict_evidence.py --run data/predict-validation/<completed-run>
python3 tests/predict_evidence.py
```

The review is offline and uses the supervisor's validation functions. It checks
independent final facts, request counts and hashes without loading credentials or
changing evidence. An intermediate zero-open-orders message is insufficient if
final checks are missing. Preserve failed attempts as well as complete captures.

## Confirmed fill and inventory semantics

The recorded startup reconstructed effective holdings, required settlement history,
reported cash and an existing take-profit order into an empty Native Cache. Native
external claims established strategy ownership before startup; adoption submitted
nothing and did not generate new runtime fills for historical activity.

The exit-only run prohibited BUY, canceled the existing take-profit order and then
sold the remaining inventory. Its observed callback sequence was:

```text
on_order_pending_cancel → on_order_canceled
→ on_order_submitted → on_order_accepted → on_order_filled
→ on_position_closed
```

Cancellation and Filled each occurred once. Cache showed the filled order with
zero leaves and zero net inventory inside the SELL Filled callback. A complete
exit need not emit PositionChanged; restored inventory need not emit a new runtime
PositionOpened. Reported cash refreshed asynchronously.

| Recorded economic quantity | Value |
| --- | ---: |
| BUY gross quantity / share fee / net inventory | 32 / 0.64 / 31.36 shares |
| Entry collateral spent | 1.28000000 USDT |
| SELL price / collateral fee | 0.039 / 0.02446080 USDT |
| Exit gross / net proceeds | 1.22304000 / 1.19857920 USDT |
| Flat-cycle cash change | -0.08142080 USDT |
| Native quote realized PnL | -0.05582080 USDT |

These are one capture's reported amounts, not a fee schedule or return forecast.
The difference between cash change and Native quote PnL follows the documented
[inventory-unit commission convention](quantity-commission.md); do not deduct the
valuation of a share fee twice. Fee asset/amount remain venue facts.

## Historical execution timing pilot

A two-order distant post-only GTD pilot recorded zero fills, unchanged cash/holdings
and complete independent final OPEN queries. Observed create-client intervals were
186.813–220.035 ms, cancel-client intervals 176.650–190.700 ms and matched Accepted
WS-handler-to-strategy intervals 0.338–0.878 ms. Client intervals include the test
relay and network. With only two samples, p95/p99 are maxima, not tail estimates.

That revision performed per-submit funds checks: 436.247–451.186 ms, accounting
for almost all warm preparation. Its 51 RPC calls, 16 OPEN attempts, one cold
metadata miss and one hit describe the historical harness, not current submission.
A separate read-only Python comparison found no clear batching benefit: three
parallel reads had median 163.165 ms versus 162.421 ms for one batch. No production
batching or per-order funds cache was introduced. Current local execution/callback
comparisons and their boundaries are in the
[validation reference](predict-execution-validation.md).

## Venue contract observations

- The maker matrix includes an actual create_order_expiry_too_soon response.
  Venue arrival required at least two minutes of remaining validity; local checks
  do not silently extend the requested expiry.
- Modify is an explicit local rejection. Cancel/replacement was observed; atomic
  amendment was not.
- Batch removed/noop results are per-item facts, not an atomic guarantee. Mixed
  failures and REST-first duplicates have offline coverage.
- Partial SELL, cancel/fill races and competing strategies' inventory were not
  established by these funded runs; deterministic replays cover their intended
  Native behavior.
- Exit-only adoption supports one entry and at most one existing SELL LIMIT.
  Ambiguous ownership is rejected; it is not a general multi-strategy recovery API.
