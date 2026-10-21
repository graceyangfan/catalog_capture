# Predict adapter

Public market data and opt-in private execution for NautilusTrader. Start with the
[current architecture and API contract](../../docs/predict-execution-adapter-design.md).
It defines command support, MARKET strategy input, settlement callbacks, accounts
and retention. The [validation record](../../docs/reference/predict-execution-validation.md)
separates current mock validation from historical mainnet evidence and describes
the pinned Native dependency.

## Features and dependency

| Feature | Surface |
| --- | --- |
| live (default) | Public data, discovery and WebSocket transports |
| high-precision (default) | Native model/quantity/persistence precision |
| execution | Private REST/wallet WS, signing and Native commands/reports; writes remain explicit opt-ins |
| native-quantity-commission | Inventory-fee mapping for gross order progress, net BUY shares and SELL quote fees |

The workspace pins the repaired project Nautilus fork, including the Native APIs
and account/purge behavior required by execution. It is not an unmodified upstream
feature. See the [dependency contract](../../docs/reference/predict-execution-validation.md#two-repository-handoff).
Historical mainnet captures used their recorded revisions and do not establish new
MARKET BUY or latency behavior. Legacy execution reconciliation is a separate fee
mapping; use native-quantity-commission for the documented maker/taker contract.

## Native interaction

Strategies send Native commands; Native risk, Engine, Cache and Portfolio own
admission, order/position state and callbacks. The adapter handles venue protocol,
local signing, exact raw evidence and authenticated facts. Normal submission has
no balance/OPEN/metadata/orderbook preflight. Known-ID cancellation is one remove
POST. Modify is unsupported; use explicit cancel then a new order.

Both MARKET sides require params["market_price"] as a strategy-supplied decimal
reference. Native MarketOrder has no price field. The adapter applies configured
slippage once; there is no execution-book fallback.

Early settlement Submitted/Confirmed/Failed uses account/strategy-scoped
PredictSettlementUpdate CustomData. Only confirmed fills update Native economic
state. See the [strategy guide](../../docs/how_to/predict-rust-strategy-calibration.md#quantity-fees-and-settlement-callbacks)
for early hedging, fee accounting and cancel/fill races.

## Public data and retention

Strategies can use PredictOrderbookSnapshot for signals and explicit Native quotes
for valuation/risk. Rolling feed aliases normalize to canonical selectors such as
BTCUSDT. Predict market/outcome identities remain separate from Binance instruments.
Crypto Up/Down windows use market/title metadata, price bounds 0..1 and a 1 USDT
minimum notional; an ended trading window does not prove settled holdings.

Discovery/subscription rollover is shared with capture. Instrument metadata remains
while Native retained orders/open positions or unresolved facts require it. Books
and subscriptions retire separately. Execution contexts, routing, raw evidence and
settlement notices follow Native owner retention, without the former 10,000-fact
caps. Configure Native purge; bounded adapter maintenance does not promise constant
memory for active/unresolved owners or bound one huge history's disposal pause.

## Development and evidence

Protocol modules live in http/, websocket/, signing/ and blockchain/. Public
subscriptions live in data/; execution/ owns Native integration. Strategies,
mock venues and recorders belong under tests or the application assembly layer.
No test strategy or recorder runs in production src.

- [Tests, fixtures and explicit benchmark exporters](tests/README.md)
- [Current development status](../../docs/predict-execution-work-plan.md)
- [Module review and remaining costs](../../docs/predict-execution-structure-review.md)
- [Private API calibration](../../docs/how_to/predict-private-api-tests.md)
- [Historical live evidence](../../docs/reference/predict-live-evidence.md)

Ordinary tests use fixtures/loopback services without credentials or venue writes.
Run only named ignored probes with explicit scope; never all ignored tests.
Recorded primary captures are immutable and hash-indexed. Operational logs and
archives stay in ignored data/predict-validation/, not versioned test inputs.
