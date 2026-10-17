# Predict adapter architecture

The workspace pins Nautilus fork `587e1065efde08db19016a8eab921c1605d5d2a4`.
The opt-in quantity-commission integration is a downstream extension, not an
unmodified upstream capability. Its scope is described in
[Inventory-unit commissions](reference/quantity-commission.md).

## Ownership

| Owner | Responsibilities |
| --- | --- |
| Nautilus | DataEngine routing, Cache, order state machine, Position, Portfolio, RiskEngine, persistence, startup/runtime reconciliation and physical WS reconnection |
| Adapter | Predict HTTP/WS protocol, signatures, authentication, subscriptions/heartbeat, venue identities, reports, raw precision evidence and venue-specific admission |
| Strategy | Custom snapshot signals, trading intent, maker take-profit, cancellation before taker stop-loss, and adoption of existing business intent |
| Host | TOML, InstrumentProvider, client factories, LiveNode assembly and diagnostic collection |

The adapter does not maintain a separate position/PnL ledger or order recovery
scheduler. Strategies consume Native state rather than writing Cache or
resubmitting commands whose outcome is unknown. Polymarket execution and Deribit
authentication provide structural comparisons; their venue capabilities are not
assumed to apply to Predict.

## Source layout

| Module | Responsibility |
| --- | --- |
| `common/` | Venue identity, exchange domains, exact conversion and Up/Down rules |
| `http/` | Transport, authentication, queries and DTOs; execution metadata is validated separately from public capture DTOs |
| `websocket/` | Public and authenticated wallet protocols |
| `data/` | Subscription ownership, discovery, expiry handover and Native data publication |
| `execution/mod.rs`, `lifecycle.rs` | Public client, Native trait delegation, connection and Cache observation |
| `execution/{orders,submitter,order_builder,cancellations}.rs` | Commands and bounded unknown-submission context |
| `execution/{reports,reconciliation,matches,external,inventory_scope}.rs` | Native reports and history needed to explain current inventory |
| `execution/{account,readiness,reconnect,authentication}.rs` | Reported cash, generation checks, QueryAccount scheduling and JWT renewal |
| `execution/resources.rs`, `precision.rs` | Stateless resource interpretation and cumulative projection of raw fill/fee evidence |

Test strategies, local venues and collectors live in tests. Private unit tests
use `cfg(test)` without expanding the production API. See the
[test guide](../crates/nautilus-predict/tests/README.md).

## Orders and identity

```text
Strategy -> RiskEngine -> ExecutionEngine -> Predict command/signing/HTTP
Venue wallet/REST facts -> Native events/reports -> Order/Position/Portfolio
-> strategy order and position callbacks
```

| Native operation | Predict mapping |
| --- | --- |
| BUY LIMIT | GTC/GTD, maker or explicitly permitted non-post-only; exact signing and collateral/allowance/nonce checks |
| SELL LIMIT / MARKET | Available net inventory less active SELL reservations; MARKET GTC uses `market_slippage_bps` |
| CancelOrder | Verify the signed hash's numeric REST ID, then call `orders/remove` |
| BatchCancelOrders | Send `ids[]` together; interpret each `removed/noop/rejected` independently |
| CancelAllOrders | Select owned orders by account/instrument/side and reuse removal |
| ModifyOrder | Emit standard ModifyRejected; replacement requires explicit cancel and a new order |
| OrderList / batch modify / reduce-only | Reject unsupported operations rather than discarding intent |

### Cancellation

Single and batch cancellation share per-order preflight and outcome handling.
The signed hash is the Native VenueOrderId; its numeric REST ID is used only for
[orderbook removal](https://dev.predict.fun/remove-orders-from-the-orderbook-25326904e0).
Removal does not invalidate the signed order on-chain.

| Evidence | Adapter action |
| --- | --- |
| Proven not sent or explicitly rejected | Emit OrderCancelRejected; keep the order and its reservation active. A definitive rejection remains valid during a wallet gap. |
| Requested ID in `removed`, continuous session | Emit OrderCanceled; retain command ownership until Native applies the terminal event. |
| Requested ID in `noop` | Query the hash. A validated zero-fill CANCELLED/EXPIRED snapshot produces an OrderStatusReport; OPEN or unresolved fills do not establish cancellation. |
| Timeout, 5xx, malformed receipt or lost wallet continuity after transmission | Retain the unknown outcome for WS, Native QueryOrder and periodic reconciliation. Do not automatically repeat the removal write. |

The [receipt schema](https://dev.predict.fun/removeordersresponse-14037528d0)
partitions IDs into `removed`, `noop` and `rejected`; batch success is not atomic.
Task ownership and generation checks prevent stale responses from changing a
new session. Native Cache owns state transitions and duplicate-report handling.

The official Rust adapters at develop snapshot `7fe26c3d59` provide comparisons:

| Adapter | Unknown cancellation outcome |
| --- | --- |
| [Binance Spot](https://github.com/nautechsystems/nautilus_trader/blob/7fe26c3d59/crates/adapters/binance/src/spot/execution.rs) | Await reconciliation; emit CancelRejected only for NotSent or VenueRejected. |
| [Polymarket](https://github.com/nautechsystems/nautilus_trader/blob/7fe26c3d59/crates/adapters/polymarket/src/execution/cancellations.rs) | Await stream/reports; its cancel guard protects the HTTP task lifetime. |
| [Hyperliquid](https://github.com/nautechsystems/nautilus_trader/blob/7fe26c3d59/crates/adapters/hyperliquid/src/execution.rs) | Await reconciliation; query by client ID, then venue ID. |
| [Lighter](https://github.com/nautechsystems/nautilus_trader/blob/7fe26c3d59/crates/adapters/lighter/src/execution.rs) | Retain pending action and nonce evidence; do not reject an unknown send outcome. |

These are source comparisons, not live validation on the other venues.
Venue-specific confirmations and task ownership need not be identical.

### Native cancellation recovery limit

An unchanged OPEN snapshot maps to Accepted. It proves the order remains live,
not that a delayed cancel failed. Native deliberately preserves PendingCancel
and its inflight query tracking for this report.

In the pinned Rust ExecutionManager, an exhausted PendingCancel confirmation
budget can generate reconciliation Canceled without venue terminal evidence.
This branch and unchanged-Accepted handling match the official develop snapshot
above. The retry budget counts confirmation checks, not repeated cancel writes.
Reported account reservations continue to include venue OPEN orders even if
Native has closed their cached order.

A later OPEN snapshot cannot reopen a terminal cached order, and Native Strategy
suppresses cancellation of that closed ID. Automatic replacement must therefore
not rely on timeout-derived Canceled as proof of venue removal. Resolve the venue
state before resuming that workflow. The current adapter does not guarantee
automatic recovery from an indefinite outage; increasing retries only extends
the confirmation window. Disabling inflight checks removes normal coverage.

The adapter preserves this boundary without a second timer, synthetic rejection
or shadow order-state machine. `SubmissionRecoveryPolicy` covers unresolved
submissions, not cancellation of an already accepted order. Offline coverage is
described in the [test guide](../crates/nautilus-predict/tests/README.md#cancellation-coverage);
observed mainnet behavior is documented in [live evidence](reference/predict-live-evidence.md).

### Submission and fills

Quantity-based MARKET requests preserve the SDK's signed maker/taker amounts,
serialize `slippageBps` as a decimal string, and explicitly send
`isMinAmountOut=false`. Slippage already adjusts signed amounts; the flag must
match the SDK mode, rather than applying another price adjustment. See the
[official order guide](https://dev.predict.fun/how-to-create-or-cancel-orders-679306m0).

Builder inputs follow the supported SDK precision: up to three significant price
digits and five significant quantity digits, with the SDK's 0.01-share quantity
floor and additional venue minimum-notional checks. Fill/inventory accounting
precision is separate. A strategy must submit a legal exit size and retain any
untradeable remainder explicitly.

Native owns ClientOrderId. VenueOrderId is the signed order hash; numeric REST IDs
are endpoint parameters, not substitutes for that hash. Native external claims
assign recovered orders. REST lacks original post-only intent, so it remains
unknown rather than being inferred from expiry or strategy type.

Only confirmed settlements produce FillReport. Submission receipts, transaction
plans and failed settlements do not create extra fills. Stable settlement/trade
identity supports WS/REST deduplication; Native applies order and position state.
Both REST and WS may confirm cancellation without creating a second strategy
transition. [Orderbook removal](https://dev.predict.fun/remove-orders-from-the-orderbook-25326904e0)
is distinct from the SDK's on-chain signature invalidation operation.

Native can also synthesize an order tagged with its reserved
`RECONCILIATION_ORDER_TAG` to align cached positions with a position report.
Those local adjustments still reach Native order/position callbacks, but do not
advance Predict's chain-cash freshness requirement. Real venue fills recovered
through reconciliation do advance it; `OrderFilled.reconciliation` alone does
not distinguish these sources. Startup, event observation, snapshot retirement,
account refresh, readiness and submission use the same classification.

## Sessions

Native epoch transport, backoff and SocketStateSink own physical connections.
AuthTracker, SubscriptionState and TaskGroup support protocol and task ownership.
Predict handles subscription ACKs, JWT topics, stale generations and the
[application heartbeat](https://dev.predict.fun/heartbeats-1915508m0).
Heartbeat echo preserves the server timestamp; it is distinct from WS Ping/Pong.

Wallet ACK is not an account snapshot. New JWT topics require subscription and
account verification before submission becomes ready. With `auth_signer`, renewal
is scheduled at roughly 80% of the remaining token lifetime; identity is checked
before publishing the replacement. Trading POSTs are not retried by renewal.
Externally managed tokens without a signer remain the operator's responsibility.

| Worker/check | Purpose |
| --- | --- |
| Native inflight/open/position checks | Order outcomes and inventory reconciliation |
| Account worker, periodic + Notify | One balance-refresh loop, woken by order changes or QueryAccount; periodic reads cover external transfers |
| Gap QueryAccount timer | Queue a core-thread query while the current wallet epoch remains unverified |
| Authentication worker | Renew credentials without implementing strategy recovery |
| Public discovery deadline | Discover successor markets and hand over subscriptions |

Old-generation tasks cannot make a new connection ready. Recovery QueryAccount
still performs broader OPEN/matches/positions/chain checks; further reduction
requires an equivalent Native report-application completion contract.

## Startup and accounts

Startup discovers instruments required by current OPEN orders and effective
unexpired holdings. InstrumentProvider loads definitions; Native mass status
applies current reports and the confirmed fill prefix needed for that inventory.
Unrelated closed historical cycles do not become new running-strategy signals.
Lookback bounds discovery, without truncating required active-order evidence.
Strategies register external claims before startup and adopt Cache state in
`on_start`; the adapter does not add redemption or settlement operations.

Accounts are reported cash. Allowance is not a balance. Native Cache/Portfolio
applies AccountState; the adapter reports locks using the Native CashAccount
formula plus external/pending resource evidence. This follows the responsibility
of Polymarket's reported collateral path.

Account refresh is asynchronous with Filled. The pinned fork's reported-account
endpoint updates Cache without guaranteeing a new account-topic publication for
every refresh. Strategies use available notifications and read the applied Cache
state; the adapter does not fabricate an `on_account` event.

### Resource interpretation

| Source | Reported cash lock | Admission bound |
| --- | --- | --- |
| Known LIMIT BUY | Native gross leaves x limit | Exact leaves notional when not represented by REST |
| REST OPEN BUY | Full signed maker budget for unknown/recovering orders, rounded up to Money precision | Full signed budget plus existing fee-rate headroom |
| Recovering MARKET BUY without signed budget | Conservative binary cap of 1 USDT/share | Same cap applied to local leaves |
| SELL | No USDT lock | Full signed shares for the matching token, or unrepresented local leaves |

Admission headroom is not a charged cash fee. SHARES fees remain inventory fees.
REST and local evidence form a union by exchange hash; they are not summed twice.
Unknown transmitted writes retain their resource claim.

The supported OPEN resource view requires USDT, OPEN status, hash and positive
original signed maker/taker amounts. These are not remaining quantities;
`amountFilled` does not reduce them. [OrderData](https://dev.predict.fun/orderdata-14037505d0)
defines the fields but does not prescribe a local rejection callback. If the
adapter cannot account for the snapshot, the new command fails before POST through
Native OrderRejected. Existing venue orders are not marked rejected, and the
failed command is not automatically retried.

### Retention and accounting

Complete terminal-order mirrors become eligible for lazy release only after
Native Cache has actually purged them. Existing report/account entry points do
the work. Active, unresolved or unapplied-fill contexts remain retained; the
adapter does not introduce another retention timer or tombstone table.

Raw 18-decimal fill/fee evidence and the last-applied-fill time watermark survive
mirror release. The watermark constrains chain-balance freshness. Late/duplicate
facts within Native retention follow normal report/trade-ID handling. Facts
outside retention that cannot be attributed stop that consumer and retain the
reconciliation gate; arbitrary external writers are not automatically adopted.

Orders complete on gross execution. BUY SHARES fees reduce received inventory;
SELL COLLATERAL fees reduce proceeds. Reported fee asset, amount and liquidity
are authoritative; a FeeModel is an estimator, not a replacement for actual fees.
Cumulative precision projection preserves partial-fill prefixes and residuals.
The default execution reconciliation mapping and opt-in quantity-commission path
are distinct. Native quote realized PnL is not all-fee cash PnL.

## Public data and evidence

DataClient owns selector discovery, successor-market handover and subscription
release. A successor's first valid snapshot completes handover; stale topic/epoch
frames are discarded. Native retains instrument definitions according to its
Cache policy. Custom snapshot identities and explicit YES/NO QuoteTicks route
through Native DataEngine. Strategies can compute signals directly from snapshots;
quotes serve consumers that require Native prices.

Use an explicit snapshot subscription or one rolling selector for a given market.
Mixed identities and overlapping selectors are not a supported routing contract.
RequestInstrument publishes the definition and correlated InstrumentResponse;
dates do not select historical definitions.

[Live evidence](reference/predict-live-evidence.md) distinguishes observed venue
behavior from derived and synthetic tests. Captures are credential-redacted and
hash-verified; collectors and supervised execution tools remain in tests.
Long-running unattended validation is incomplete. Raw fill evidence has an
explicit 10,000-entry bound, and unresolved external activity remains gated.
Further work is tracked in the [work plan](predict-execution-work-plan.md).
