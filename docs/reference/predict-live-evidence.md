# Predict live execution evidence

This index describes observed mainnet behavior, not permission to run funded tests.
Raw credential-redacted JSON/JSONL and Native journals remain under
[`tests/fixtures/private_api`](../../crates/nautilus-predict/tests/fixtures/private_api/).
The [manifest](../../crates/nautilus-predict/tests/fixtures/evidence_manifest.json)
records hashes and provenance. Derived replays and synthetic fault cases are
separate from actual exchange observations.

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

Capture-directory timestamps use UTC. Earlier successful and failed captures are
retained; the table lists representative complete runs rather than every attempt.

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
