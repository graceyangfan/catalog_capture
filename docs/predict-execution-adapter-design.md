# Predict adapter architecture

Predict translates venue commands and authenticated feedback into Native execution
messages. Strategy, risk and portfolio policy remain in NautilusTrader. This is
the behavioral contract; [internals](reference/predict-execution-internals.md) describes
module ownership and [validation](reference/predict-execution-validation.md)
links executable checks and evidence. Supported Native APIs are documented in the
[dependency handoff](reference/predict-execution-validation.md#two-repository-handoff).

## Ownership

| Component | Responsibility |
| --- | --- |
| Strategy | Active market selection, reference prices, exposure, exits, replacement and treatment of residual inventory |
| RiskEngine | Configured trading state, rate/notional/funds checks and in-flight reservations |
| Native LiveNode / execution manager | Startup/runtime recovery admission, deadlines, retries and exhaustion diagnostics |
| Native ExecutionEngine / Cache / Portfolio | Order transitions, report application, balances, positions, deduplication, callbacks and retention |
| Predict adapter | Authentication, signing, protocol conversion, identity/routing, actual fee facts and asynchronous account reporting |
| Host | Credentials, node configuration, persistence, monitoring and telemetry collection |

The adapter has no account-wide historical-position readiness gate, cumulative
BUY budget, replacement policy or second order/position ledger. Connection health
means authenticated running transport. Cash reporting and recovery continue
independently; their progress does not redefine connection health.

## Input authority

| Input | Single owner / entry point |
| --- | --- |
| MARKET reference and extra slippage | Strategy SubmitOrder.params; absent extra slippage is SDK zero |
| Quantity / quote budget, TIF, expiry, post-only | Native order fields; no parallel HTTP params |
| Exposure, replacement, stop-loss and residual policy | Strategy and configured Native risk/execution algorithms |
| Risk bypass, recovery scope/cadence and position checks | Native node config; host forwards it without extra admission rules |
| Signing permission | Optional submission_signer; independent of auth_signer for read-only/session renewal |
| Pagination, timeouts, metadata TTL | Adapter protocol/resource bounds, not strategy risk budgets |

Product TOML uses predict.submission_enabled (default false); credentials alone
never enable submission. Rust configuration injects submission_signer directly;
there is no single-field submission-policy wrapper or client trading-policy schema.
Disabling Native startup reconciliation also skips host account-market discovery.

## Commands and identity

```text
Strategy -> RiskEngine -> ExecutionEngine -> prepared context -> sign -> create POST
CancelOrder -> cached numeric route -> remove POST -> Native event -> strategy
Wallet execution -> FillReport -> Native Engine/Cache/Portfolio -> strategy callback
Native recovery/account request -> asynchronous HTTP/RPC collection -> Native application
```

ClientOrderId is Native identity. VenueOrderId is the signed order hash. A numeric
REST order ID is only endpoint routing, learned from receipts, wallet messages or
a targeted lookup. Optional route observation cannot veto an otherwise valid
execution fact; actual routing still rejects conflicting ownership.

| Command | Mapping |
| --- | --- |
| LIMIT BUY/SELL | GTC/GTD; Native post-only; optional LIMIT STP |
| MARKET BUY/SELL, base quantity | GTC or FOK; explicit strategy reference and resolved slippage |
| MARKET BUY, quote quantity | Native quote budget; GTC/FOK; zero resolved extra slippage |
| CancelOrder | One remove POST with a cached route; one hash GET first only if the numeric route is absent |
| BatchCancelOrders / CancelAllOrders | Select owned orders, group routes and interpret individual removal results |
| ModifyOrder | ModifyRejected; strategy explicitly cancels then submits a new order |
| BatchModifyOrders | Native default fanout to ModifyOrder; each child receives ModifyRejected, with no venue amendment |
| OrderList, reduce-only, IOC, LIMIT FOK, quote SELL | Unsupported; no silent intent conversion |

Warm submit performs local preparation and one create POST. It performs no book,
cash, OPEN, holdings, match-history, metadata or nonce read. Cold signing inputs
fail promptly and wake existing background preparation. Checks retain ownership,
protocol legality, current definition, authentication and task lifetime; they do
not duplicate risk budgets. Ambiguous transmitted writes retain correlation and
are never automatically resubmitted.

### MARKET strategy input

Supply an exact decimal string in `SubmitOrder.params["market_price"]`, using the
selected outcome's price. Native MarketOrder has no price field. The strategy
owns reference freshness and executable depth; the adapter does not fetch a book.
`market_order_slippage_bps` is a per-order parameter, using the Native Hyperliquid
name. Omission means zero extra slippage, as in the official Predict SDK. There is
no client-level slippage setting. Slippage adjusts signed
amounts once. Both base-share and quote-budget paths send `isMinAmountOut=false`.
This is an internal HTTP protocol constant, not a strategy parameter or planned
order capability. Explicit `isMinAmountOut` / `is_min_amount_out` params are
rejected; quote-budget BUY is expressed through Native quote_quantity.

For 100 shares at .020 and 100 bps, BUY signs at most 2.02 USDT for 100 shares;
SELL signs 100 shares for at least 1.98 USDT before fees. These signed terms do
not promise available liquidity. SDK input constraints include three significant
price digits, five significant quantity digits, .01-share minimum and 1 USDT
signed collateral minimum. Execution precision is independent of order-entry
rounding. LIMIT rejects terms the SDK would silently truncate.

`TimeInForce::Fok` on MARKET maps to `isFillOrKill=true`; GTC omits it. Neither
FOK nor post-only is a camel-case HTTP command parameter. GTC omission does not
establish resting-remainder behavior. MARKET signs a five-minute expiration;
LIMIT GTC uses the SDK's 2100 fallback and GTD uses the Native expiry. Signed order
expiry, instrument expiry and recovery timeout are separate concepts.

### MARKET BUY quote sizing

`quote_quantity=true, quantity=X` means maximum principal budget X USDT. At the
strategy reference, SDK value-mode arithmetic derives shares with five-significant-
digit flooring. Signed collateral must be representable, at least 1 USDT and no
more than X. Positive resolved slippage is unsupported for quote sizing.

Before Submitted, Native OrderUpdated converts the working quantity to base
shares and clears the quote flag. Initialized retains the original budget;
worker views and restart validation reuse that history. Subsequent order progress
uses gross shares. No separate budget registry is maintained. Reservations use
the immutable signed budget or conservatively the persisted quote cap. Sizing
from one reference does not guarantee full budget consumption. See
[SDK and MARKET evidence](reference/predict-immediate-order-validation.md).

### Maker STP and receipt metadata

Native post-only and GTC/GTD remain ordinary order fields. LIMIT accepts
`params["self_trade_prevention"]`: CANCEL_MAKER, CANCEL_TAKER or CANCEL_BOTH.
Omission preserves venue defaults; MARKET STP is unsupported. STP is an HTTP
option outside the signed terms and cannot be inferred from a recovered hash.

Subscribe to `predict_limit_order_receipt_data_type(account, strategy)` with no
client selector for `PredictLimitOrderReceiptUpdate`. It contains submitted
options and the optional exact removal-lock deadline. Missing means unknown;
metadata is transient and not reconstructed after restart. Its delivery failure
does not change execution outcome. See the [maker contract](reference/predict-maker-order-validation.md).

### Cancellation

A definitive `removed` partition or wallet cancellation publishes Canceled without
waiting for fills or account refresh. A lock-window `rejected` partition publishes
CancelRejected and preserves the order. `noop`, timeout and lost responses leave
outcome unresolved. HTTP 200 alone does not prove cancellation.

Submitted-order cancellation uses existing correlation until the route/ownership
is available. The adapter adds no follow-up polling loop. Native recovery queries
resolve uncertain outcomes. Predict opts into `retain_unresolved_commands`: Native
budget exhaustion publishes `CommandRecoveryExhausted` and retains pending state,
instead of inventing venue cancellation. This opt-in requires the paired Native
implementation; other clients retain their default behavior. A later actual fact
or explicit Native query may resolve the order. Unrelated orders continue.

Rust hosts can subscribe to `reconciliation.CommandRecoveryExhausted` on the Native
msgbus and filter by client, account and strategy. This diagnostic is not an order
event: it ends the automatic retry budget without changing quantity, confirmed
fills or pending risk ownership. It does not assert that the venue removed an order.

## Wallet feedback and settlement

| Venue fact | Native effect |
| --- | --- |
| Create 201 receipt | Routing/optional receipt metadata; no acceptance assertion |
| Owned Accepted | Immediate Accepted |
| Owned Canceled / Expired | Immediate terminal lifecycle event; no trade-history wait |
| noMarketMatch / rejectedPostOnly, no confirmed/reported fill or pending settlement | Rejected with the venue reason |
| Rejection with confirmed/reported/pending execution, any rejectedDuplicate or unknown reason | Immediate rejection observation; retain Native state and request independent order status |
| Transaction Submitted / Failed | Optional settlement CustomData; no economic fill |
| Transaction Success | Confirmed FillReport with actual execution and fee facts |

Native applies events/reports before publishing strategy callbacks. Typed handlers
run before `on_order_event`. A command issued inside a typed handler can advance
the Cache synchronously, so a generic observer may see a newer pending state than
the event it is recording. Do not restore an older state to make them equal.
A cancellation callback does not assert that all fills have arrived. Late confirmed fills remain
applicable after cancellation or instrument expiry. Missing acceptance required
for hash binding or the Native Expired transition uses the existing queued proof.
Known cancellation/expiry and real executions never wait for unrelated history or
account collection. An ambiguous rejection does not itself prove original-order closure.

`orderNotAccepted` with confirmed or reported execution does not independently
establish cancellation of the remainder. An outstanding
OrderTransactionSubmitted also prevents a false zero-fill rejection when the display
cumulative is still zero; it confirms matching intent, not final inventory. Any rejectedDuplicate
or unrecognized rejection likewise preserves the original order, including a zero-fill
accepted original. Independent wallet cancellation/expiry or REST terminal evidence
must establish closure. Confirmed settlements remain independently applicable.

Subscribe to `predict_order_not_accepted_data_type(account, strategy)` for
`PredictOrderNotAcceptedUpdate`. This immediate observation includes the reason
classification, displayed cumulative quantity and Native-applied quantity/status.
It is neither an order terminal nor an economic fill.
Known reasons retain their wire spelling; unrecognized strings are classified as
`unknown` rather than echoed. Duplicate observations may repeat this transient metadata
without duplicating Native order or position events.

The existing client recovery timer coalesces each affected client-order ID and sends
one ordinary Native QueryOrder on its next tick (default one second). It maintains
query intent only, with no parallel order status or retry loop. Native owns collection,
application and later retries. Explicit terminals retire a queued request. Shutdown
fences the timer. Hosts disabling that timer retain responsibility for ordinary Native
queries/reconciliation; a metadata subscriber need not manage another order ledger.
Optional metadata channel closure cannot suppress genuine execution delivery.

The strategy contract separates remaining order activity from confirmed economic
execution. Canceled/Expired means the order has stopped working; it does not assert
zero inventory or a complete settlement history. PartiallyFilled means an incomplete
working order, whereas a partially executed order whose remainder ended is Canceled
with its actual fills preserved. Share fees change net inventory, not gross order
progress. A later genuine settlement may update a closed order and its position.

For a zero-fill MARKET already Rejected by wallet evidence, a subsequent REST
CANCELLED preserves that Native rejection for both GTC and FOK. Without wallet
evidence, recovery uses Canceled and does not invent a no-match reason. This is an
equivalence between two terminal representations, independent of execution TIF.

The official [wallet topic reference](https://dev.predict.fun/subscription-topics-1915507m0)
defines OrderNotAccepted as rejection and enumerates reasons without explaining their
combinations with partial settlements or whether a duplicate refusal terminates an
existing original. SDK 1.3.8 builds/hashes/signs orders and supplies no wallet rejection
state resolver. Native compatibility in another adapter cannot fill this protocol gap.
The current adapter preserves uncertainty explicitly until independent status evidence.

Actual execution price, liquidity and fee asset/amount are authoritative. A fill
is not discarded for differing from limit/post-only intent or the local clock.
Identity, signed terms, raw arithmetic and conflicting duplicate checks remain.
Confirmed wallet and REST fills use Native report ingestion, preserving its fee
preflight and suppression of inventory already covered by position snapshots.

`PredictSettlementUpdate` uses the existing DataEngine/msgbus custom-data route:
subscribe to `predict_settlement_data_type(account, strategy)`. Submitted may drive
strategy hedging, but has no final fee and creates no position. Confirmed metadata
is not a second fill. Cross-channel ordering is not guaranteed. Optional metadata
receiver failure is logged and cannot suppress execution delivery.

### Quantity commissions and precision

Gross shares advance orders. Actual SHARES fees reduce received BUY inventory;
COLLATERAL fees reduce cash proceeds. Native handles quantity commissions, net
cost basis and PnL. Do not deduct a share fee's USDT valuation again. Zero-fee and
collateral-fee fills retain exact atomic quantity; actual execution prices need
not have the same precision as quote ticks. See [quantity commissions](reference/quantity-commission.md).

The adapter retains raw economic evidence and queued trade identities for exact
conversion and deduplication before Native applies them. Immutable Native history
indexes are reused on valid continuations; cold/replaced histories receive full
validation. These are protocol evidence, not a second position model. Queued wallet
fills are included in recovery bundles so lagging REST cannot void genuine fills.

A venue FILLED with complete quantitative evidence may normalize the working
quantity to confirmed gross via Native reconciliation OrderUpdated. Original signed
terms remain in history. This handles a one-wei unmatched remainder without
fabricating a Canceled event, trade or fee. Unknown `amountFilled` units alone
cannot authorize normalization.

This closing update is published as `OrderUpdated(reconciliation=true)` after the
cache has reached FILLED; it creates no extra `OrderFilled`. Strategies observing
terminal state should handle ordinary order events or updates as well as fills.
Previously projected historical fills retain their allocation and replay identity;
exact precision for new fills does not rewrite past trades or fees.

## Accounts and recovery

Venue-only MARKET BUY adoption requires a FILLED terminal, complete per-order
settlement quantity matching both REST and the signed share quantity, and
executions within the signed price bound. This equality also preserves repeated
reports after Native adoption without a separate signed-order ledger. The original
requested quantity is not guessed when these quantities differ. Active, canceled,
or differently sized external MARKET BUYs remain explicit recovery errors. Known Native orders retain
their original command terms and existing report path.

Cash and OPEN reservations are collected by one asynchronous account task. Periodic
refresh and explicit `QueryAccount` share its request counters and collector;
explicit requests can verify authenticated REST/chain state during a wallet gap.
An existing read completes or reaches its deadline before the next read starts.
Ordinary order changes
reproject Native locks without HTTP/RPC; fills request background cash refresh.
Submitted capacity remains with Native risk. Known OPEN changes use order-local
projection when provenance is available; otherwise projection uses the baseline
and current owners. Allowance is not cash, and an unconfirmed terminal state
cannot release a real venue reservation.

Native owns recovery scheduling and report admission. Predict implements the
current `ExecutionReportTask` collection/finalization API. IO runs on owned worker
inputs; Cache-dependent finalization runs on the core thread. Target collection
can publish an already verified terminal lifecycle fact through the emitter before
reading trades. It does not mutate Cache from the worker. Report collection never
serializes submit/cancel or wallet callbacks behind its network waits.

A target query uses one order GET and market-scoped matches only for nonzero
reported fills. Each Native QueryOrder executes independently; an older history
read cannot silently consume a newer query. Query scheduling and frequency limits
remain with Native. Overlapping results use current Native ownership and standard
trade deduplication; a stale OPEN snapshot cannot reopen a confirmed canceled order. No second status GET requires the venue to remain unchanged.
Applied Native quantities plus unseen real trades form cumulative progress; the
endpoint need not return an already-applied prefix again. Client shutdown fences
late results; a wallet reconnect alone does not invalidate authenticated HTTP facts.

An OPEN row is sampled before its trade-history request. If a wallet cancellation
or expiry reaches Native during that request, the collected real trades still
apply while preserving the newer terminal state. An older OPEN observation cannot
reopen the order or veto those executions. Conflicting economic facts and distinct
terminal assertions remain explicit reconciliation errors.

Bulk recovery determines scope before fetching matches: OPEN venue orders,
retained Native recovery orders and unexpired holdings, including position-only
markets. Empty scope performs no account-wide historical scan. Known orders keep
late-fill recovery after expiry. Explicit historical queries retain their requested
scope. Pagination bounds, ownership and actual economic contradictions remain
report checks, not account-wide order-admission checks.

### Prepared metadata and signing

Market/fee metadata is prepared at connection, definition updates and background
refresh, sharing YES/NO context. TTL is 1..=300 seconds. Warm commands read it
locally; stale context fails promptly and requests preparation. Wallet resubscribe
does not invalidate still-valid metadata.

Predict nonce is `nonces(owner)` for the selected verifying contract, not a clock
or instrument sequence. Cache scope is client environment/account plus one of four
exchange contracts. Only missing entries are read. InvalidNonce clears the matching
entry if it still equals the rejected value, then wakes background refresh; other
contracts remain usable. No guessed increment, time TTL or automatic POST retry.

Nonce IO runs immediately when the existing signing-metadata task starts, in a
branch independent of account collection and market HTTP. Explicit connect does
not await it: nonce errors or slow reads cannot fail an otherwise healthy account
and acknowledged wallet connection. Only a new order whose contract nonce is
missing fails locally through the existing Native rejection path, with no POST.
The first account baseline is still required in reported mode; connection setup
still performs the existing best-effort market preparation.
Missing entries retry in the same nonce branch after 1, 2, 4 seconds, with the wait
capped at 30 seconds after each completed attempt. Targeted invalidation wakes it immediately
and resets the delay. Once all entries are valid, it waits only for invalidation;
market HTTP, its refresh cadence and account collection do not schedule nonce
recovery. Each RPC attempt retains the chain client's configured timeout.
The same recovery task drives at most four concurrent contract reads. Each read
has its own deadline and canonical-block verification, then publishes that slot
without waiting for the other contracts. Errors leave only that slot missing;
successful and already-prepared slots remain usable. A recovery round completes
before the existing shared backoff schedules another round.
Preparing all four contracts uses six more block GETs than a shared batch; these
checks occur only during preparation or recovery, never on normal submission.
Automatic wallet reconnect and instrument retirement preserve valid values;
full client setup resets connection context. See the [SDK comparison](reference/predict-execution-validation.md#comparison-basis).

## Retention and public data

Instrument expiry ends new trading on that instrument; it does not cancel an
order, erase a position or prove automatic redemption has completed. Static expired
inventory is omitted from trading-position recovery, while definitions and owned
orders needed for late execution remain under Native retention.

| Ordinary Native purge condition | Effect |
| --- | --- |
| Any nonterminal order, including Initialized/Submitted/PendingCancel | Keep definition |
| Any open position, including expired dust | Keep definition |
| Only terminal orders and closed positions | Remove definition/market data; retain historical order/position records |

No adapter force-purge API or retirement registry is added. Bounded mirror cleanup
follows Native removal and protects queued/unapplied evidence. Retained old state
is not an account-wide gate on new markets; genuine shared cash reservations still
belong to risk/account reporting. An unsafe forced definition removal may prevent
Native from applying a late fill.

Position collection skips zero and final WON/LOST balances before metadata lookup.
Cached expiry requires no query; unknown nonfinal holdings use one market-window
lookup per market per collection. Missing instrument is not itself expiry. Expired
holdings need no historical instrument reconstruction. Active unknown holdings
still fail the report. `provides_bulk_position_coverage=false` means an omitted
position is not a flat assertion: no synthetic close, cash credit or payout.

Public CustomSnapshot, native depth and BBO share venue book subscriptions.
DataClient owns discovery, rollover and subscription handover; final retirement
follows Native retention. Current batch requests preserve `instrument_ids`;
single-member boundaries use `single_instrument_id()`. No older-API compatibility
branch is maintained.

## Optional latency telemetry

Telemetry is opt-in, bounded and disposable. Producers never wait for collection;
saturation drops measurements and reports loss. Hosts drain outside callbacks.
Measure command, venue feedback and actual strategy intervals separately, with API
counts and metadata hits. CPU microbenchmarks, startup stress, loopback callbacks
and live network latency are different workloads; overlapping percentiles cannot
be added. Linear cold history validation and physical purge remain implementation
limits, not claimed constant-time HFT paths.

SubmissionAccountLockWait and SubmissionRegistration directly measure the local
registration boundary; Preparation ends before that boundary. Registration spans
nested locks and shared-index updates, while CreateRequest starts later. Immutable
hash formatting and worker snapshot construction belong to Preparation before
account-lock acquisition.
The bounded probe publishes these samples only after releasing all registration
locks. A disabled probe reads no additional clock. These elapsed intervals include
scheduling effects; do not subtract their independent percentiles to infer lock
cost. The explicit loopback exporter is documented in the test guide.
