# Inventory-unit commissions

## Status and review entry points

This is the current downstream accounting contract and research summary. It
supersedes the staged prototypes and local patch exports. It is not an upstream
approval or a claim that all account/product combinations are supported.

- [Own-fork RFC](https://github.com/graceyangfan/nautilus_trader/issues/1).
- [Pinned implementation](https://github.com/graceyangfan/nautilus_trader/commit/587e1065efde08db19016a8eab921c1605d5d2a4).
- [Fixed comparison with its upstream baseline](https://github.com/graceyangfan/nautilus_trader/compare/7b6e2c27b86bd1688fde49bcf6e81d7a89560b05...587e1065efde08db19016a8eab921c1605d5d2a4).

The earlier own-fork draft PR was closed without merging. Use the pinned source
and comparison, not the frozen PR diff. The workspace bootstrap and CI pin the
same implementation directly; no local patch application is required.

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

FillReport and OrderFilled persist the fee fact. Position retains one optional
Decimal cumulative quantity-fee total. Internal inventory adjustment follows the
existing base-commission convention with no additional `pnl_change` or fee-cost
ledger. Existing replay state carries the complete Position; there are no new
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
short/flip, void/restatement and public Python/PyO3/FFI parity require separate
work. Removing the guard alone could update Position net quantity while a
calculated account still books gross assets.

The quote realized convention matches existing base commissions. For example,
BUY 1 at 100 with 0.01 inventory fee, followed by SELL 0.99 at 110 without an exit
fee, closes inventory and reports 9.90 quote realized PnL. Cash difference is
8.90; the original-asset fee remains separate. Do not relabel quote realized PnL
as an all-fee cash return. Predict SELL collateral fees use the existing Money
commission path. [Real execution evidence](predict-live-evidence.md) shows the
same distinction.

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
