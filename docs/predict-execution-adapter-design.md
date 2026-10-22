# Predict adapter architecture

The [execution internals](predict-execution-structure-review.md) describe module
ownership and implementation limits. [Test commands](../crates/nautilus-predict/tests/README.md)
and the [validation reference](reference/predict-execution-validation.md) document
coverage, measurements and evidence boundaries.

The workspace pins the repaired Native fork required by execution. See the
[dependency contract](reference/predict-execution-validation.md#two-repository-handoff)
for its revision and validation. Quantity commissions are a downstream extension,
described in [Inventory-unit commissions](reference/quantity-commission.md).

## Ownership

| Owner | Responsibility |
| --- | --- |
| Strategy | Maker/taker intent, price/depth freshness, size, hedge and replacement policy, exposure limits |
| Native RiskEngine | Configured notional/rate/funds checks and in-flight reservations |
| Native Engine/Cache/Portfolio | Order/position state, economic accounting, reconciliation, strategy callbacks and physical purge |
| Adapter | Predict protocol, exact units, signing, authentication, identities, factual events/reports and raw precision evidence |
| Host | TOML, factories, instrument discovery, LiveNode assembly, telemetry and supervised test budgets |

The adapter owns neither a second OMS nor a funds ledger. It has no cumulative
attempted-BUY budget. A venue rejection remains an ordinary Native rejection;
normal commands do not first audit balances, allowance or every OPEN order.

## Commands and identity

```text
Strategy -> Native risk/execution -> prepared context -> local signing -> create POST
Known-ID cancel -> remove POST -> definitive receipt -> Native event -> strategy
Own wallet fact -> Native event/report -> Engine/Cache/Portfolio -> strategy
Account and recovery queries -> separate background/explicit collection
```

Native owns ClientOrderId. VenueOrderId is the signed order hash. Numeric REST IDs
are endpoint routing values, retained in the existing short-lock lookup; insertion
does not copy the complete historical map. Conflicting identity is rejected.

| Command | Supported mapping |
| --- | --- |
| LIMIT BUY/SELL | GTC/GTD, per-order post-only intent, local signing and one create POST |
| MARKET BUY/SELL | GTC, base shares, mandatory strategy reference price and configured slippage |
| CancelOrder | Known numeric route: one remove POST; missing route: targeted hash GET first |
| BatchCancelOrders | Group numeric IDs and interpret each removed/noop/rejected result |
| CancelAllOrders | Select owned orders and reuse removal, including deferred Submitted orders |
| ModifyOrder | Native ModifyRejected; caller uses cancel then a new order |
| OrderList, batch modify, reduce-only, quote-quantity MARKET, IOC/FOK | Explicitly unsupported; no silent intent conversion |

Local checks enforce protocol legality, ownership and current prepared context.
They do not duplicate strategy risk budgets. Unknown POST outcomes retain their
existing correlation for wallet/Native recovery and are not automatically resent.

### MARKET strategy input

Native MarketOrder has no price field. Supply an exact decimal string through
`SubmitOrder.params["market_price"]`, for example `"0.020"`, for either BUY or SELL.
It is the selected outcome's reference before `market_slippage_bps`; NO uses its
own price. This parameter is consumed locally, not added to the venue payload.
Missing/invalid values produce Native OrderDenied before Submitted. There is no
orderbook fallback or implicit cached-quote selection.

Reference price must be positive, at most one and representable in wei. The
builder follows the supported SDK precision and venue signed-amount minimum;
these are protocol constraints, not discretionary risk limits. Strategy owns
executable depth, reference freshness, sizing and any tighter slippage policy.

For 100 shares at 0.020 with 100 bps slippage:

| Side | Signed maker amount | Signed taker amount |
| --- | --- | --- |
| BUY | 2.02 USDT maximum, capped at one USDT/share | 100 shares |
| SELL | 100 shares | 1.98 USDT minimum proceeds, before fees |

Slippage adjusts signed amounts once. The request serializes `slippageBps` as a
decimal string and uses `isMinAmountOut=false`, matching the quantity-based SDK
mode. A signed floor/maximum is not a guarantee of liquidity or execution.
Supported builder inputs use up to three significant price digits and five
significant quantity digits, a 0.01-share floor and the venue's 1 USDT signed
collateral minimum. Fill/inventory precision is separate; strategies retain any
untradeable remainder explicitly. See the
[pinned SDK comparison](https://github.com/PredictDotFun/sdk-python/blob/6da1708e1f3a9341f5cfd8d0b91bb66c8f43bef1/src/predict_sdk/order_builder.py).

### Prepared metadata and signing

Market definitions/fees are prepared at connect, instrument updates and background
refresh, sharing the existing slot across YES/NO. `market_metadata_ttl_secs` accepts
1..=300; zero is invalid. Cold/expired metadata denies promptly without a command
GET. Definition replacement, recovery and wallet epochs invalidate stale context.

Wallet signer, typed-data constants and exchange domains are reused. Signing nonce
preparation belongs to initialization/background refresh. Invalid-nonce rejection
requests refresh without automatically retrying the trading POST. There is no
signer pool or prepared-order grid.

### Cancellation

[Orderbook removal](https://dev.predict.fun/remove-orders-from-the-orderbook-25326904e0)
does not invalidate the signature on-chain or reverse an already matched settlement.
Single and batch cancellation share result classification:

| Evidence | Result |
| --- | --- |
| Proven not sent or explicit rejection | CancelRejected; retain active reservation |
| Requested ID in removed, current generation | Canceled, even during a wallet gap; command ownership stays until Native applies it |
| Requested ID in noop | Targeted hash query; validated terminal evidence may produce a report |
| Timeout, 5xx, malformed receipt or stale generation after transmission | Unknown outcome; retain intent for wallet/Native reconciliation |

OPEN or unresolved fills do not prove cancellation. Batches are not atomic.
Reset refuses unresolved submissions or cancellations, including restored orders
without a local submit entry. Stop/reconnect do not erase ambiguous intent.

### Native cancellation recovery limit

An unchanged OPEN report proves the order remains live. Native preserves
PendingCancel while checking confirmation. In the compared Native implementation,
retry exhaustion can synthesize Canceled without a venue terminal fact. This is
not proof of removal; the adapter keeps the remote OPEN reservation. A terminal
cached order cannot simply be reopened or canceled by the normal strategy API.
Resolve venue state before replacement after such an outage. No second order
state machine or retry scheduler is added to hide this framework limitation.

## Wallet feedback and settlement

Known own Accepted/Canceled/Rejected/Expired facts use `OrderEventFactory` and the
Native emitter directly. Applied state and one-terminal-callback rules remain
Native-owned. The once-written Accepted marker follows the order context, including
queued-before-application races; it is not a 10,000-entry FIFO.

Confirmed fills retain exact raw quantity/fee normalization on the Native
execution path. REST/external recovery uses reports. Hash, account, immutable terms, cumulative
quantity and trade identity are validated. A venue millisecond timestamp is not
rejected merely for preceding the local nanosecond submission timestamp.

| Wallet message | CustomData status | Economic state |
| --- | --- | --- |
| orderTransactionSubmitted | Submitted: matched and submitted for settlement | No Native fill or position increase |
| orderTransactionSuccess | Confirmed | Normal confirmed Native Filled path |
| orderTransactionFailed | Failed | No Filled; does not by itself cancel the whole order |

`PredictSettlementUpdate` is published through Native DataEngine/msgbus to the
owning account/strategy's `on_data(CustomData)`. Subscribe with
`predict_settlement_data_type(account_id, strategy_id)`. It contains order, trade
and settlement identity, raw economic facts, timestamp and status. No additional
REST/RPC or exchange subscription is required.

Early hedging or canceling other risk orders may start at settlement Submitted.
Track settlement_id, correct the hedge if Failed, and do not hedge twice on
Confirmed. Submitted lacks final fee facts. Final sellable inventory and PnL use
`on_order_filled`, position events and Native Cache. Confirmed CustomData is not a
second fill. The data and execution queues do not guarantee cross-channel order.
Late Filled may still arrive after cancellation. See the
[strategy callback contract](how_to/predict-rust-strategy-calibration.md#quantity-fees-and-settlement-callbacks).

### Quantity commissions and precision

For `native-quantity-commission`, orders progress on gross executed shares. BUY
SHARES fees reduce received inventory; SELL COLLATERAL fees reduce cash proceeds.
The actual reported fee asset/amount is authoritative; do not deduct the USDT
valuation of a share fee again. Native quote realized PnL is not all-fee cash PnL.
Legacy execution and quantity-commission reconciliation are different mappings;
the maker/taker strategy contract targets the latter.

The existing Native-applied fill index is borrowed by trade ID under immutable
cutoffs. Repeated live observations reuse it, rather than clone OrderAny history,
build single-element maps or repeatedly search queued fills. A weak history cursor
plus complete fact count/cutoff permits reuse of verified raw prefixes. Only newly
appended IDs need reading on a valid continuation. Cold/replaced/incomplete histories
retain full validation. Uncommitted facts are not memoized.

Pending wallet fills borrow the existing queued-ID and committed-fact indexes;
there is no per-message economic-fact Vec or lifecycle lookup map. The order view
and evidence guard are sampled under the Native observer's publication lock. That
account lock is released before normalization; the evidence guard protects the
borrow through event enqueue and queued marking. Lifecycle/account publication
occurs after releasing evidence, preserving the observer's lock order. The
transmitted hash supplies ownership even before Native applies Accepted.
Wallet dispatch constructs message-derived venue/instrument IDs once and passes
them to the internal report parser. The parser still checks Native ownership and
instrument identity. Numeric REST IDs use the shared strict decimal uint256 codec
directly, without a JSON value or a second string allocation.

Retirement may reuse a membership-only cursor with no monetary summary. It must
never substitute zero economic totals. Raw wei evidence and temporary settlement
notices have distinct purposes and owner-scoped lifetimes; neither is a position
ledger. Their former two 10,000-entry hard limits have been removed.

## Accounts and recovery

Remote collection reads cash, OPEN and required nonce context; local projection
uses the existing baseline plus Native reservations. Submitted capacity belongs to
Native risk, so it is not reported twice. Ordinary Accepted/confirmed cancellation
changes local locks without OPEN/RPC. Fills refresh cash asynchronously; periodic,
explicit account queries and gap recovery perform wider verification.

Known OPEN changes use an order-local projection when the previous revision and
AccountState are proven applied. Missing provenance falls back to full projection.
MARKET BUY retains its immutable signed budget; external OPEN rows keep full signed
budget where progress is unknown. SELL does not lock USDT. Unknown/synthetic cancel
cannot release a real venue OPEN row. Allowance is not cash.

The paired Native Runner applies AccountState through Portfolio and publishes the
standard account topic. Deferred projections flush only after their source event
is applied. Callback-child ordering preserves timely replacement funds; it does not
promise every account update is visible inside the preceding order callback. Cash
freshness remains asynchronous, with block/session/revision checks. No funds ledger,
ACK protocol or per-command balance query is introduced.

Automatic gap retries use Native inflight/open indexes and existing command
registries before enqueueing QueryAccount, then repeat eligibility before capturing
retained history. Explicit QueryAccount still refreshes when readiness is ineligible.
Recovery reuses collected account facts and observed order views, with post-IO
OPEN/match/holdings/block and revision checks where needed. It is intentionally
broader than ordinary execution. Current inventory, not unrelated closed history,
determines startup recovery scope. Native external claims preserve strategy ownership.

QueryOrder and single-order report tasks share target HTTP collection and pure
conversion. Native core-thread finalization and command report delivery remain
separate. Zero reported fills need one order GET; a nonzero amount requires
market-scoped matches and a second order GET consistency fence. Predict documents
amountFilled as a string without a share unit, so it is not compared numerically
to Native filled_qty. A Filled-only shortcut is deferred: skipping the REST matches
would also require preserving the existing complete raw-history, signed-term and
allocation checks without constructing another retained proof/cache.

Ordinary bulk reports with an explicit instrument filter matches by market ID.
Account-wide and full recovery retain all required markets, including instruments
with positions and no current order. Explicit fill-report windows and batch
validation are unchanged.

A cold capture reconstructs a Native/raw prefix at startup, full client reconnect,
missing/stale observer context or a separate recovery view. A wallet resubscribe
alone does not erase the live prefix. Startup may be slower; in-service cold
reconstruction and large disposal can still pause the Native thread.

## Retention and public data

Native owns retention. Its existing closed-order/position/account-event purge is
opt-in; the node example uses one-minute closed-order cadence and a 60-minute
buffer. Explicit user profiles are not rewritten. Already-purged terminal owners
retire on the existing recovery timer; manual mode keeps lifecycle/query fallback.
Active orders, pending notifications and unapplied evidence remain protected.

Each adapter cleanup checks at most 128 entries from each existing index and limits
selected retirement work. Membership is checked before disposal deferral, so an
unapplied fact gates readiness in the same callback. Proof releases account/client/
hash locks; selected deletion rechecks ownership/evidence. Final worker-history
references are released outside shared guards. A single oversized owner, physical
Native purge, instrument checks and OPEN copy-on-write can still be linear; this is
not a hard pause deadline or a constant-memory promise for unresolved activity.

Public CustomSnapshot, native L2 depth and explicit BBO subscriptions share one
venue book topic. CustomSnapshot is the recording representation; native YES/NO
depth is an on-demand sorted view, and quotes are an independent BBO projection.
Managed depth uses Native book/quote maintenance before ordinary strategy callbacks.
No extra venue subscription or adapter quote stage is required. Market-data timing
probes have been retired; recording event/init timestamps remain part of the schema.

DataClient owns discovery, subscription rollover and expiry handover. The first
valid successor snapshot completes handover; stale topic/epoch frames are discarded.
Use an explicit snapshot or one rolling selector per market; overlapping selector
identities are unsupported. RequestInstrument publishes a correlated response.
Final instrument retirement follows Native retention and unresolved ownership.
Public capture and strategy execution share data contracts; the strategy host does
not record data unless configured to do so.

## Optional latency telemetry

Telemetry is opt-in, bounded and disposable. Producers do not wait for a collector;
the host drains it away from trading callbacks. Saturation drops measurements and
reports that loss rather than block execution. It is not a recovery input.

Measure paired command/WS-to-Native/actual-strategy intervals, API attempts and
metadata hits/misses. Confirmed-fill timing is separate from Accepted and early
matching. REST confirmations are not relabeled as WS samples. Overlapping spans
must not be summed. Background/startup counts and per-command counts are reported
separately. CPU microbenchmarks, timer waiting windows and live network measurements
have different boundaries; see the validation record for limitations.

## Comparison basis

Maintained Native Polymarket context, dispatch, fills and reports provide the
ownership/event/report comparison; Bybit, OKX and Binance Spot provide command,
query and error-handling comparisons. Predict's SDK establishes protocol amount
and signing behavior, not Native Cache/Portfolio lifecycle. SDK on-chain cancel is
different from HTTP book removal. Comparisons use the source revisions recorded in
the validation record; they are not a claim about unfetched upstream HEAD.

[Live evidence](reference/predict-live-evidence.md) separately records observed
venue behavior. Offline replays and sanitized fixtures do not establish new venue
acceptance. Long-running unattended validation and universal latency bounds remain
outside the completed evidence.
