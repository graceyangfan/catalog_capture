# Inventory-unit commissions

## Status and review entry points

This is the current downstream accounting contract and research summary. It
supersedes the staged prototypes and local patch exports. It is not an upstream
approval or a claim that all account/product combinations are supported.

- [Own-fork RFC](https://github.com/graceyangfan/nautilus_trader/issues/1).
- [Pinned baseline](https://github.com/graceyangfan/nautilus_trader/commit/d44218eed670bf436877a0d35b7db61a4de5f6ea).
- [Fixed comparison with its upstream baseline](https://github.com/graceyangfan/nautilus_trader/compare/779b4b7e00575d38f75e0e7769000f81655abf61...d44218eed670bf436877a0d35b7db61a4de5f6ea).

The earlier own-fork draft PR was closed without merging. Use the pinned source
and comparison, not the frozen PR diff. The workspace bootstrap and CI pin the
same baseline directly; no local patch application is required.

## Problem and ownership

A retained Predict BUY executes 32 shares at 0.04 USDT, spends 1.28 USDT, and
charges 0.64 shares. The order is fully filled at gross 32, while inventory grows
by net 31.36. Reporting 31.36 as execution quantity would leave the order locally
underfilled. A quote-valued fee alone cannot represent the inventory deduction.

Native CurrencyPair already handles base-currency commission inside fill
application. BinaryOption has no base Currency for its outcome inventory, so the
opt-in fork explicitly carries an instrument-quantity fee. It does not manufacture
a Currency, synthetic SELL, adjusted fill price or adapter-maintained position.

```text
Adapter -> gross execution + actual reported fee
Native Order -> gross cumulative fills and status
Native Position -> inventory deduction and ordinary lifecycle events
Portfolio -> existing PnL/valuation semantics
Account -> reported balances within the supported path
Strategy -> decisions using updated Native state
```

The optional quantity commission is denominated in the fill's InstrumentId units.
Existing Money commission retains its actual currency and behavior. Conflicting
inputs must not double-charge inventory. Reported fee facts, not FeeModel estimates,
are authoritative.

## Minimal Native extension

FillReport and OrderFilled persist the fee fact. Position retains optional Decimal
totals for cumulative quantity fees and removed inventory cost. The existing
internal commission adjustment carries both the inventory deduction and cost PnL;
there is no adapter ledger. Existing replay state carries the complete Position; there are no new
Order aggregation methods, PositionSnapshot summary fields or Arrow snapshot columns.

Raw cumulative fees are projected at the Native boundary, avoiding independent
rounding of each tiny partial fee. The restricted path retains a canonical net
quantity so later zero-fee or quote-fee fills do not restore previously deducted
inventory. This is a bounded precision policy, not a global Decimal PnL rewrite.

Real fill application validates supported inventory/account conditions before
advancing gross order state. Existing duplicate classification precedes this
validation. Native order-only historical projection skips economic inventory
validation because it intentionally does not apply Position or account changes.

## Account and PnL boundaries

The prototype is restricted to reported cash and supported long, linear unit
inventory. That guard describes validated implementation coverage, not the
financial definition of an inventory fee. Calculated balances, margin, borrowing,
short/flip, void/restatement and legacy FFI parity require separate work.
PyO3 constructors, fill dictionary round trips and Position fee/cost getters are
covered by the pinned Native extension. Removing the guard alone could update Position net quantity while a
calculated account still books gross assets.

The explicit quantity-fee path includes the cost of removed inventory in quote
realized PnL. It uses the existing internal `PositionAdjusted::Commission` with
both quantity and PnL changes before ordinary position events are published.
For deducted inventory `f` and moving average `A` after the gross fill, the cost
write-off is `-f * A`. Execution prices and `avg_px_open` retain Native semantics.
Future exit fees and slippage remain strategy estimates.

BUY 1 at 100 with an inventory fee of 0.01 leaves 0.99 and realized PnL of -1.
At a mark of 101, total PnL is -0.01 before any future exit fee. SELL 0.99 at 110
closes with 8.90 realized PnL. Legacy CurrencyPair base commissions retain their
existing quote PnL convention outside the explicit quantity-fee path: the same
example represented as a legacy base Money fee still reports 9.90. Whether to
unify that older convention is a separate upstream API decision; cash-flow
regressions do not establish maintainer agreement on that policy.

The recorded Predict round trip spends 1.28, receives 31.36 net shares, and sells
them for 1.19857920 after collateral fees. Corrected Native realized PnL matches
cash profit of -0.08142080. The 0.0256 inventory cost is included exactly once;
SELL collateral commission follows the existing Money path. Original captures
remain immutable and may show the earlier quantity-only PnL convention.

The calibration strategy still bases its profit floor and stop decision on tracked
entry cash cost, realized proceeds and estimated executable exit proceeds after
fees. Whole-account balance changes are not a per-strategy profit calculation.

### Cumulative cost and validation

Native Position retains one additional optional Decimal cost total. Only the
change between cumulative Money projections is booked; no separate projected
balance is persisted. This retains expenses below one Money quantum without an
adapter ledger. Cost inherits the existing floating-point moving-average precision.

Ordinary Redis/SQL fill persistence rebuilds quantity and cost totals without forcing
extra snapshots. Existing complete Position replay state and explicit snapshots also
retain both totals. Duplicate fills are filtered before applying either total;
purge and episode reset rebuild them. Earlier quantity-only prototype snapshots
without cost must be rebuilt from original fills before continuation.

`tests/execution/quantity_fee_cost.rs` verifies actual Native accounting, without
injecting test-side adjustments. Recorded JSON is replayed through Rust Strategy
and LiveNode; synthetic tests cover mixed-price partial executions, BUY/SELL cycles,
sub-Money expenses and duplicate replay. Native Position PnL retains its existing
Money rounding. The pinned revision includes this intermediate cost-accounting prototype.

## Precision

Keep chain atomic precision, Native Quantity precision, order size increments and
Money precision distinct. The adapter retains 18-decimal integer/Decimal facts
for cumulative projection and deduplication. Native `defi` storage support alone
does not make float-based Position/Portfolio operations exact at 18 decimals.

At Predict prices bounded by 1 and unit multiplier, one 16-decimal inventory
quantum has value at most 1e-16 collateral units, below the pinned USDT Money
quantum of 1e-8. This bound does not generalize to higher-priced or multiplied
products. A sellable order size is determined at submission; accounting must not
round away an actual residual merely to meet a venue step size.

## Exchange and implementation references

Research checked on 2026-10-17. Actual fee currencies and account behavior must be
read from each venue; shared accounting principles do not imply identical APIs.

| Reference | Relevance |
| --- | --- |
| [Binance Spot commission FAQ](https://github.com/binance/binance-spot-api-docs/blob/master/faqs/commission_faq.md) | Commission may be deducted from the received asset or paid in BNB; gross execution and fees remain separate facts |
| [Bybit execution stream](https://bybit-exchange.github.io/docs/v5/websocket/private/execution) | Execution quantity, fee, fee currency and liquidity are separate; unified account naming does not alone determine a Native account type |
| [OKX API](https://www.okx.com/docs-v5/en/) | Fill size and fee currency/sign require explicit interpretation rather than copying another adapter's convention |
| [Predict wallet events](https://dev.predict.fun/subscription-topics-1915507m0) | Executed size and SHARES/COLLATERAL fee facts are independently reported |
| [Polymarket V2 upgrade](https://help.polymarket.com/en/articles/14762452-polymarket-exchange-upgrade-april-28-2026) | Fees changed from shares to USDC; historical share fees are not current V2 behavior |
| [Hummingbot fee types](https://github.com/hummingbot/hummingbot/blob/9af100d6822da7d2d0291a906c730ef172284ee2/hummingbot/core/data_type/trade_fee.py) | Separates fees deducted from returns from additional-cost fees |
| [Freqtrade accounting](https://github.com/freqtrade/freqtrade/blob/3ebac56eba525c991462c0714c65a6e2f4e36896/freqtrade/persistence/trade_model.py) | Useful comparison for gross fills versus fee-adjusted amounts, not a replacement for Native ownership |

Inspected pre-V2 Polymarket adapter paths preserved gross fills and independently
queried positions; no complete instrument-unit accounting solution was identified
there. That is a scoped source finding, not proof that all historical versions
used the same method.

## Validation and remaining review

The retained [core review](../../crates/nautilus-predict/tests/fixtures/private_api/quantity_commission_core_review_validation_20261017.json)
and [controlled probes](../../crates/nautilus-predict/tests/fixtures/private_api/quantity_commission_core_necessity_probes_20261017.json)
record cumulative sub-precision fees, mixed-price partial fills, BUY/SELL mixtures,
legacy base-fee parity, duplicate replay, restart/snapshot continuation, historical
order-only projection and atomic oversell rejection. Strategy/LiveNode replay
observes Native callbacks and net inventory; it is not new funded execution.

Before any upstream-ready proposal, resolve supported account capabilities,
public bindings and correction semantics with maintainers. Keep fee valuation
policy separate from inventory deduction and preserve legacy Money behavior.
