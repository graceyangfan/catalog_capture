# Inventory-unit commissions

Predict execution uses the reported-cash implementation in the
[pinned project fork](../getting_started/installation.md#dependency-contract).
Inventory-unit commissions are discussed in
[RFC #5418](https://github.com/nautechsystems/nautilus_trader/issues/5418).
The fork covers live reported-cash accounts; general FeeModel, calculated-account,
backtest and correction support is outside this contract.

## Mapping and ownership

Enable `execution` on `nautilus-predict`; the product CLI enables
it through `predict-execution`. This also enables Native model `defi` support for
exact 18-decimal fee and inventory facts.

| Venue fact | Native mapping |
| --- | --- |
| Gross executed shares and price | `FillReport.last_qty` and `last_px`; gross order progress and execution VWAP |
| Nonzero BUY SHARES fee | `quantity_commission: Some(Quantity)`, zero monetary commission |
| Zero fee | `quantity_commission: None` |
| SELL COLLATERAL fee | Existing monetary commission in USDT |
| Reported net holding | Exact atomic quantity, including positive dust |

REST matches and confirmed wallet settlements use the same quantity-fee mapping.
Missing actual fee evidence remains unresolved; a simulation estimate never
replaces it. Pending settlement notifications do not create a fill. Native owns
orders, positions, account events, duplicate classification and replay. The
adapter retains raw wei and settlement identities for protocol validation.

## Cost and PnL

For gross BUY quantity `q`, price `p` and inventory fee `f`, order completion uses
`q`, inventory increases by `q-f`, carrying cost increases by `(q-f)*p`, and Native
expenses `f*p` through its existing PnL remainder calculation. Reported account
cash remains authoritative; the valuation does not debit it again.

BUY 10 at 0.10 with fee 1, then BUY 10 at 0.20 without a fee produces gross VWAP
0.15, net inventory 19, basis 2.90 and initial expense 0.10. Selling all 19 at 0.20
produces total profit 0.80. Partial exits retain their unallocated basis.

Fee summaries derive from original fee-bearing fills. Native cost accounting
and replay state carry the facts; tests independently compare PnL with cash plus
marked inventory.

## Precision and supported scope

Quantity fees require a reported cash account with calculated balances disabled,
linear unit inventory settled in quote currency, and a BUY into long or flat
inventory. Require `0 < fee < gross quantity`; nonzero quantity and monetary fees
cannot occur on the same fill. SELL quantity fees, rebates, flips and corrections
in fee-bearing episodes remain unsupported by this first implementation.

Fees and holdings preserve all representable atomic digits through Native Quantity.
New orders retain exact gross atomic quantities for every fee denomination,
independently of the instrument display scale. Native aligns existing order quantities without
rounding before applying the fill. Unrepresentable quantities still fail before
application. Submission increments and net holdings are separate concepts: positive
dust must not be rounded into a flat position.

`execution` always enables the quantity-fee contract and Native `defi` support.
There is no alternative monetary valuation mode for SHARES fees. Monetary
COLLATERAL fees continue to use the existing exact-money/cumulative allocation path.

Raw 18-decimal evidence does not make arbitrary products or settlement notionals
exact. Native checked arithmetic, domain bounds and PnL remainders remain
authoritative. Venue integer-truncated notional can differ from `quantity * price`;
this fee field does not carry an independent settlement cash value.

### Native atomic gross executions

The pinned Native contribution includes exact mixed-scale order arithmetic.
`Order::checked_filled_qty` is shared by report reconciliation and engine preflight. Order application validates scale promotion and arithmetic before
changing state, retaining original gross fill and fee facts. Quantity, filled,
leaves, voided and overfill state align through fills, updates and replay. No new
persistent fields or fee properties are added; the Quantity operators retain their
existing explicit-scale semantics.

The official Python SDK at `6da1708e1f3a9341f5cfd8d0b91bb66c8f43bef1`
uses an 18-decimal integer scale. Its LIMIT amount helper truncates requested
quantity to five significant digits and price to three; outbound helper rules do
not establish a precision limit for actual partial settlements. The
[wallet protocol](https://dev.predict.fun/subscription-topics-1915507m0) reports
`executedSizeWei` separately from `fee.amountWei` and the fee asset.

`ingestion_contract::native_order_atomic_gross_preserves_original_fill_facts`
checks mixed-scale ingestion.
Wallet/REST permutations cover gross `17.000000000000000099` with a fee of
`0.340000000000000001`, exact net inventory, basis, duplicate suppression and one
position publication. JSON/event replay and Arrow position recovery retain atomic
facts. Old projected/quote-valued fee history still cannot be silently reinterpreted.
Retained historical projected allocations remain unchanged on replay. New orders
use exact gross quantities for zero, SHARES and COLLATERAL fees. A proven legacy
projected prefix keeps its existing cumulative carry; an unknown legacy prefix
requires recovery rather than silently changing previously applied executions.
Currency-fee conversion remains independent of gross quantity precision.

The [dependency handoff](predict-validation.md)
identifies the Native baseline and additional API changes required by current
execution. Historical evidence retains its original source revision.

### Rust strategy sells and closing

Rust Strategy submits the normal Native MARKET SELL, associated with its owned
`position_id`, through the Predict execution client. The existing `close_position`
path is exercised by the share-fee entry/exit integration tests. Predict currently
requires `reduce_only=Some(false)`, base quantity, and a `market_price` parameter.
Its quantity-based MARKET preparation preserves the requested amount or rejects
it if the SDK helper would truncate it to five significant digits; a notional below
1 USDT is also rejected. These outbound rules are independent of exact fill ingress.

Native close APIs floor to the instrument increment and retain dust. Exact 18-decimal
fill accounting does not promise that every net balance is immediately sellable or
that a close request leaves zero. Use actual net strategy inventory, account for
outstanding exits, and wait for authoritative fills; do not sell the entire wallet
balance across owners or mark residual inventory flat locally.

## Recovery and validation

Recovery requires original fills or sufficient compatible persisted position state.
Old prototype snapshots used Decimal fee facts, cumulative quantity projection and
pooled-cost expense. Rebuild those positions from original evidence under the new
contract before continuing; do not assume an old snapshot is an equivalent opening
state. Immutable historical captures retain their original meaning.

Offline tests cover REST and wallet fee mapping, subscriber-visible net positions,
gross completion, different-price fills, partial exits and later acquisitions,
exact net close, 18-decimal fees and holdings, duplicate reports, JSON/Arrow replay,
sub-display-unit expenses and unsupported gross projection. They use loopback
HTTP/WS/RPC and recorded economics, without funded venue orders.

Run the commands in the [test guide](../../crates/nautilus-predict/tests/README.md).
Historical prototype captures retain their source revision and cost convention;
they do not validate the current implementation.

### Terminal reconciliation

Polymarket separates actual executions from working order quantity and uses
Native terminal-report normalization. Predict reuses that pattern, preserving
FILLED and original signed terms. Other venues' dust thresholds, IOC conversions
and instrument-precision parsers do not establish Predict settlement semantics.

Only complete raw quantitative evidence authorizes terminal working-quantity
normalization via Native OrderUpdated. An unknown-unit one-wei remainder alone is
insufficient. Actual fills, share fees and net inventory remain exact; missing
history never becomes a fictional fill or a different venue status. See the
[recorded incident and replay](predict-validation.md)
and terminal_recovery integration tests for pending-command, base/quote-sizing,
ordering, replay and incomplete-history controls.

## Framework compatibility

The [dependency handoff](predict-validation.md)
identifies the supported fork revision. Quantity commissions use existing Native
owners and add the following contracts:

| Invariant | Native integration |
| --- | --- |
| Gross execution and inventory fee remain independent | Optional quantity_commission on FillReport and OrderFilled; absent by default |
| Gross progress is exact across raw scales | Checked rescaling and prevalidated order arithmetic |
| Net inventory, cost and fee expense agree | Prepared position accounting, cost allocation and PnL remainders |
| Replay applies facts once | Original fee facts carried through reports, events and persistence |
| Reported venue cash remains authoritative | Calculated balances disabled; no duplicate cash deduction for a share fee |

Rust struct literals must supply the optional fee field, and persisted SQL schemas
must include its column. Rebuild incompatible prototype positions from original
fills before enabling this contract. PostgreSQL runtime and migration behavior
require independent validation; serialization tests alone do not establish them.

Cancel/modify recovery retention is a separate fork extension. It follows Native's
unresolved-submission retention pattern, is opt-in through ExecutionClient, and
publishes diagnostics through MsgBus. Its semantics are independent of fee mapping.
