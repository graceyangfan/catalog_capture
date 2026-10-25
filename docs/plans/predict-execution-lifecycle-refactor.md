# Predict execution lifecycle acceptance criteria

This document defines the refactor's acceptance boundary. Supported behavior is
specified by the [adapter contract](../predict-execution-adapter-design.md),
ownership by [execution internals](../predict-execution-structure-review.md), and
reproducible checks by the [validation reference](../reference/predict-execution-validation.md).

## Ownership

Strategies select active markets, exposure, exits and replacement policy.
RiskEngine enforces configured trading limits. Native owns order transitions,
positions, recovery scheduling, callbacks and retention. The adapter translates
Predict authentication, signing, identifiers, venue facts and account reports.
Confirmed exchange feedback takes precedence over local intent and recovery
completeness.

## Data-flow requirements

| Path | Required behavior |
| --- | --- |
| Order intent | MARKET reference and extra slippage come from order params; omitted extra slippage is SDK zero; no client-level default |
| Native configuration | The host passes risk, reconciliation and position-check settings without additional policy |
| Submission | Prepared signing inputs and one POST; no cash, book, history, nonce or market-definition preflight |
| Cancellation | Known numeric route uses one remove; missing routes require a hash lookup; confirmed removal is delivered immediately; noop remains unresolved |
| Fills | Confirmed Success retains actual quantity, price and fees; no historical query or intent-based veto |
| Callbacks | Accepted, Canceled and Expired do not wait for history; optional CustomData cannot block execution events |
| Reconnect | Account reporting and Native recovery remain independent; a WS epoch change does not discard confirmed HTTP results |
| Nonce | Cache by account, environment and contract; invalidate only the rejected entry and replenish in the existing background branch |
| Expired inventory | Exclude known expired static balances; retain actual OPEN orders, unresolved commands and late fills for owned orders |
| Restart | Startup and position reports use the same inventory scope; exclusions create no synthetic closure, zero balance or payout |

## Expiry and retention

1. The strategy stops opening risk in the old instrument and selects its successor.
2. Expiry does not establish cancellation, completed settlement or a zero balance.
   Confirmed venue feedback still reaches Native.
3. Native purge retains definitions referenced by nonterminal orders or open
   positions. Otherwise it removes definitions and market-data caches while
   preserving historical order and position records.
4. Adapter mirrors follow Native retention. Retained definitions do not consume
   strategy active-market capacity or gate unrelated submissions.
5. Cash and actual reservations remain reported; old static tokens do not become
   an account-wide readiness condition.

A missing instrument is not evidence of expiry. Recovery may query the market
window once for unknown, nonzero, nonfinal inventory; both outcomes share that
result. Ordinary submit/cancel commands and wallet callbacks perform no such IO.

## Integration and validation

Use the pinned Native batch-instrument, collection/finish and quantity-commission
APIs directly. Native pending-command retention and terminal quantity
normalization preserve unresolved ownership and real fills. The
[dependency contract](../reference/predict-execution-validation.md#two-repository-handoff)
defines the paired repository revision.

Replay immutable venue captures through loopback HTTP/WS/RPC, Native
Engine/Portfolio and actual Strategy/LiveNode callbacks. Label identity or time
rebinding as derived replay. Cover delayed queries, lost ACKs, reconnects,
duplicate/late fills, contract-local nonce rejection and expired-inventory
restart. Assert callback-visible applied state, exact economics and request
counts. Normal tests never invoke ignored live probes or rewrite captures.

Do not introduce an adapter OMS, funds budget, query polling loop or global
readiness gate. Cold-history validation, Native purge and large-object disposal
remain linear costs; offline correctness does not establish production latency.
