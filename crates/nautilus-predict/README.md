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
  recorded mainnet fill/restart/exit. Requires the project Nautilus fork pinned in
  CI/bootstrap at `587e1065efde08db19016a8eab921c1605d5d2a4`.

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
