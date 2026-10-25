# Predict maker contract and evidence

Native LIMIT GTC/GTD, post-only and explicit cancel-then-submit are the maker
interface. Supported parameters and command flow are defined in the
[architecture](../predict-execution-adapter-design.md#maker-stp-and-receipt-metadata).
This reference separates venue behavior from offline callback validation.

## Protocol mapping

| Input or response | Adapter behavior |
| --- | --- |
| Native LimitOrder.is_post_only | HTTP isPostOnly; no separate Native command flag |
| params[self_trade_prevention] | Typed CANCEL_MAKER, CANCEL_TAKER or CANCEL_BOTH, LIMIT only |
| Omitted STP | Preserve venue default; do not reconstruct STP from hash/REST |
| Create 201 | Record numeric route; optional receipt CustomData, no fake Accepted |
| removalLockedUntil | Optional exact RFC3339 deadline; missing is unknown |
| Remove: removed | Native Canceled immediately; no match-history wait |
| Remove: rejected | CancelRejected with lock reason; retain order/reservation |
| Remove: noop / timeout | Unresolved; Native query/recovery, no automatic replacement or POST replay |
| ModifyOrder | ModifyRejected; strategy owns explicit replacement |

STP and post-only request options are separate from signed economic terms.
Invalid values/types, HTTP camel-case command aliases and unsupported MARKET STP
are rejected before POST. Receipt timestamps preserve fractional seconds/offsets.
An invalid lock timestamp after transmission is ambiguous, not an order rejection.

## Strategy receipt data

Subscribe without a data-client selector:

```rust
self.subscribe_data(
    predict_limit_order_receipt_data_type(account_id, self.strategy_id().unwrap()),
    None,
    None,
);
```

Downcast `PredictLimitOrderReceiptUpdate` in on_data. It carries owner/order IDs,
submitted options, optional removal-lock deadline, connection epoch and receipt
time. It can arrive before or after lifecycle callbacks; it is not acceptance or
proof of cancellation. Receipt metadata is transient and is not reconstructed
after restart. Closed optional receivers do not change remote order outcome.
A strategy may schedule an attempt from the lock deadline, but must await actual
cancellation facts before treating replacement as safe.

## Recorded venue evidence

The immutable
[maker capture](../../crates/nautilus-predict/tests/fixtures/private_api/maker_post_only_stp_cancel_20261024.json)
records one official Python SDK 0.0.22 passive post-only GTD BUY at .01 for 100
shares / 1 USDT, STP=CANCEL_TAKER. It received 201, wallet Accepted and REST OPEN;
a scoped remove received 200, wallet Canceled and final REST CANCELLED with zero
fills/OPEN orders. The receipt omitted removalLockedUntil.

This proves submitted-option acceptance and one removal path. It does not prove
STP matching enforcement, a positive lock window or universal immediate removal.
The later [MARKET capture](predict-immediate-order-validation.md#recorded-venue-observations)
contains a real post-only rejection; its subsequent SDK exit has separate provenance.
A bounded read-only 500-market sample exposed no lock metadata; absent fields do
not establish that no lock exists. No known positive-lock live case is claimed.

The [Native zero-fill capsule](../../crates/nautilus-predict/tests/fixtures/private_api/live_native_zero_fill_20261025/README.md)
records two post-only GTD orders, expected unsupported-modify rejections and two
confirmed cancellations. Native callbacks and follow-up REST checks showed zero
fills and zero OPEN orders, without fallback removal. The independent collector
failed on its final read-only snapshot; that capture remains incomplete.

## Native callback and replacement verification

```bash
cargo test -p nautilus-predict --features execution --test execution maker_options:: --locked
cargo test -p nautilus-predict --features execution --test execution maker_lifecycle:: --locked
cargo test -p nautilus-predict --features execution --test execution node_host::cancellations:: --locked
```

Loopback Strategy/LiveNode scenarios assert held removal keeps PendingCancel and
locked cash, rejected partitions retain the original order, and definitive removal
permits a distinct replacement without wallet confirmation. Mixed batch partitions
resolve individually. GTD, duplicate wallet races and late fills use Native state
and callbacks. Predict's Native retention opt-in preserves pending state at recovery
exhaustion and emits diagnostics; timeout alone does not become venue cancellation.

These tests validate client/framework interaction, not live STP or lock enforcement.
The [validation matrix](predict-execution-validation.md) links captured batch and
funded-order replay as well as synthetic fault controls. No adapter replacement
state machine, cancel polling task or per-callback exchange read is required.

## Comparison basis

Native Bybit/OKX separate ordinary order flags from typed venue parameters;
Polymarket separates create receipts, order reports and explicit cancellation.
Predict follows those boundaries while preserving its own three-way removal
response. Community wrappers are not protocol authority. Use the pinned official
SDK, schema and saved exchange responses for supported capabilities.

Sources: [SDK Types](https://github.com/PredictDotFun/sdk/blob/5ff2b4a1c54cbbeb0fd661e17246cd12a9af8486/src/Types.ts),
[OrderBuilder](https://github.com/PredictDotFun/sdk/blob/5ff2b4a1c54cbbeb0fd661e17246cd12a9af8486/src/OrderBuilder.ts),
[Create response](https://dev.predict.fun/createorderresponsedata-14037468d0.md),
[Remove response](https://dev.predict.fun/removeordersresponse-14037528d0.md).
