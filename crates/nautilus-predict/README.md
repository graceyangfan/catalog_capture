# Predict adapter

Public market data and opt-in private execution for NautilusTrader. The controlled
mainnet baseline covers Rust Strategy + LiveNode entry, take-profit, nonempty
cold-start adoption, cancellation, MARKET SELL, Filled/Closed and account settlement.
The final recorded run had no open orders or positions. See the
[current architecture and API contract](../../docs/predict-execution-adapter-design.md)
and [live evidence](../../docs/reference/predict-live-evidence.md).

## Features and dependency

- `live` (default): Public data client, discovery and WebSocket transports.
- `high-precision` (default): Native quantity/model/persistence precision features.
- `execution`: Private REST/wallet WS, signing, reports and bounded native commands.
  SubmitOrder and REST removal remain explicit opt-ins; a signer is not inferred.
- `native-quantity-commission`: The inventory-fee mapping actually used by the
  recorded mainnet fill/restart/exit. Requires the project Nautilus quantity-fee fork.
  Historical captures used `587e1065efde08db19016a8eab921c1605d5d2a4`.

The current monetary conversion also requires Native `Money::from_decimal_exact`
from upstream develop `96ec148a8249eda918c9ee3219c7030755cabcf0` or newer.
CI, bootstrap and local verification use fork `797d98618ddbb26dd2bf843df52dd494daad8c7b`,
which includes that upstream revision. Balances and collateral fees use upstream
exact Money conversion with the canonical USDT currency.

The default execution reconciliation mapping is not the same live-tested fee path.
Native quote position PnL and cash round-trip PnL have different inventory-fee
cost treatment. Read account snapshots from Native Cache; every reported balance
refresh is not guaranteed to publish an account topic callback in this fork.

## Native interaction

| Boundary | Ownership |
| --- | --- |
| Public custom snapshots / optional quotes | Adapter parses/routes; DataEngine owns Cache and consumer delivery |
| Submit / cancel / batch cancel | Strategy sends Native commands; adapter signs and maps supported venue APIs |
| Modify / batch create / reduce-only | Unsupported capabilities reject explicitly |
| Confirmed fills | Stable IDs, gross quantity and actual fee asset; Native applies orders/positions |
| Account / reports / startup | Reported cash and standard reports; Native mass status, claims and reconciliation |
| TP / SL | Strategy ordinary maker TP; cancel barrier then taker SELL |
| Physical reconnect | Native network components; adapter handles ACK, heartbeat echo and wallet JWT |

Public recording, Up/Down discovery, expiry rollover and old subscription release
keep their existing contract. Strategies can use `PredictOrderbookSnapshot`
directly for signals; explicit Native quote subscriptions support framework
valuation and risk without republishing custom data from a strategy.

Rolling selectors use canonical feeds such as `BTCUSDT`; legacy `BTC/USDT`
and case aliases enter the same discovery and routing path. Recognized quote
suffixes `USDT`, `USDC`, and `USD` enable deterministic category discovery;
other quotes retain bounded search. A feed selector is independent of Binance's
`BTCUSDT.BINANCE` instrument and Predict's discovered market/outcome instruments.

REST cancellation is orderbook removal, not on-chain signature invalidation. The
SDK on-chain cancellation operation is not automatically sent by this adapter.

Crypto Up/Down definitions carry the title-derived trading window, anchored to
market creation metadata for historical loads, and a 1 USDT minimum notional.
Their price bounds remain 0..1; executable prices must be positive. An ended
trading window does not prove that holdings have settled or can be deleted.

The provider owns static definitions; data-client stop/reset releases that store.
Rolling definitions belong to the subscription registry. Execution worker snapshots
follow Native retention: report/command boundaries and new instrument events release
purged terminal orders, their fully applied atomic fill evidence, and instrument
snapshots no longer held by Native, retained orders or open positions. Unapplied
settlements pin their context and require reconciliation. Configure Native closed
order/position/account-event purging; no second TTL timer or parallel ledger is used.
The 10,000-entry atomic evidence safety cap still applies to retained orders.
The private trading template retains Native instrument metadata while any associated
order remains in Native's closed-order buffer, so a late confirmed fill still has its
instrument. Books and subscriptions retire normally; no-order capture stays immediate.

Cancellation keeps the decimal-ID endpoint's explicit per-order result partition;
the currently documented hash-removal endpoint does not specify an equivalent
receipt contract. HTTP success alone is not converted into an order cancellation.
Numeric routing IDs from validated create receipts, wallet events, account snapshots
and Native reports are reused. Hashes remain the Native venue IDs; conflicting numeric
IDs never replace an existing binding. Missing IDs alone require a GET. CancelAll
covers owned open/inflight orders, defers pre-acceptance cancellation, and reuses the
100-ID batch endpoint. Ready cancellations do not wait for missing-ID preflights;
slow noop follow-ups do not block all subsequent chunks. Only an explicit new command
can retry an ambiguous write.

REST removal remains available during a WS gap in the active client generation.
Definitive removed/rejected results reach Native normally; noop is not proof of
cancellation. Unknown/context-error wallet events gate submissions while the consumer
continues. Recovery reuses confirmed REST fills for already-canceled orders, then waits
for Native application. The existing gap verification timer defaults to one second;
healthy sessions do not perform recovery queries on its ticks.

Independent market/open-order reads occur outside the admission lock. Current local
claims, fresh chain resources, signing and registration remain serialized. Chain ID
and token decimals are verified once per chain-client lifetime. After connection
bootstrap, BUY and SELL admission each use five RPC calls, including one final
canonical-block check. BUY reads only its exchange allowance; SELL reads ERC-1155
inventory/approval/nonce. The full collateral diagnostic still queries all allowances.

Native exact Money retains representable collateral/fees below USDT's display scale.
Sub-native wei residuals use the existing atomic-prefix allocation; historical applied
allocations stay immutable. Wallet and REST share raw evidence indexed by order, with
separate pending-publication IDs. Read-only collection never counts as a queued fill.
Native application clears pending IDs, and Native order retirement releases routing
and raw evidence. Native price-PnL rounding remains owned by the framework.

## Source layout

`http/`, `websocket/`, `signing/`, `blockchain/` hold venue protocol boundaries.
`data/` owns public subscriptions/discovery. `execution/` holds the Native execution
client, lifecycle, commands and reports. Raw precision facts are private adapter
evidence, not FillReport.info or a parallel account/position ledger.

Strategies, LiveNode calibration, fixture generators and recorders live under
`tests/` or the application's assembly layer. Private unit tests can remain
`#[cfg(test)]`; no test strategy or recorder runs in production `src`.

## Offline checks

Use the [test guide](tests/README.md) for feature-specific Rust commands, Python
checks, fixture policy and coverage limits. Ordinary tests do not connect to the
venue or trade. Live entry points require explicit selection and authorization.
Set `NO_PROXY=127.0.0.1,localhost` if ambient proxies interfere with local fixtures.

## Evidence and live operation

Real captures, failures and Native feedback have priority. The
[evidence manifest](tests/fixtures/evidence_manifest.json) protects immutable bytes;
synthetic cases and derived replays are separately labeled.

- [Current work plan](../../docs/predict-execution-work-plan.md)
- [Private API tests and configuration](../../docs/how_to/predict-private-api-tests.md)
- [Rust strategy/live capture operations](../../docs/how_to/predict-rust-strategy-calibration.md)

Do not reuse a completed capture directory or overwrite real JSON to make a test
pass. Retain REST, wallet frames, chain/matches and Native callbacks together.
Do not commit credentials, JWTs or private signatures. New writes remain an
explicit operator decision; cleanup/refactoring does not authorize another trade.
