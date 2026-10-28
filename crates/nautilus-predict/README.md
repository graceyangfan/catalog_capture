# Predict adapter

Public market data and opt-in private execution for NautilusTrader. Start with the
[architecture and API contract](../../docs/predict-execution-adapter-design.md).
It defines command support, MARKET strategy input, settlement callbacks, accounts
and retention. The [validation record](../../docs/reference/predict-execution-validation.md)
separates current mock validation from historical mainnet evidence and describes
the pinned Native dependency.

## Features and dependency

| Feature | Surface |
| --- | --- |
| live (default) | Public data, discovery and WebSocket transports |
| high-precision (default) | Native model/quantity/persistence precision |
| execution | Private REST/wallet WS, signing, Native commands/reports and exact inventory-fee mapping; writes remain explicit opt-ins |

The workspace pins the project Nautilus fork containing the live reported-cash
quantity-fee implementation linked from RFC #5418. It is not an unmodified upstream
feature. See the [dependency contract](../../docs/reference/predict-execution-validation.md#two-repository-handoff).
Historical mainnet captures apply to their recorded revisions and combinations;
the 2026-10-25 positive MARKET BUY capture is identified separately below. They
do not establish general latency behavior. Execution always uses the RFC #5418 quantity-fee
contract; the previous monetary-only SHARES fee mode has been removed.

Rust enables submission by injecting `submission_signer: Some(signer)`; session
renewal uses the independent auth_signer. Product TOML uses submission_enabled
(default false). The host forwards Native risk and recovery policy without extra
admission rules.

## Native interaction

Strategies send Native commands; Native risk, Engine, Cache and Portfolio own
admission, order/position state and callbacks. The adapter handles venue protocol,
local signing, exact raw evidence and authenticated facts. Normal submission has
no balance/OPEN/metadata/orderbook preflight. Known-ID cancellation is one remove
POST. Modify is unsupported; use explicit cancel then a new order.

Maker LIMIT supports typed `self_trade_prevention` in SubmitOrder params while
post-only and GTC/GTD remain Native fields. Subscribe to
`predict_limit_order_receipt_data_type(account,strategy)` without a client selector
for transport metadata, including the optional removal lock deadline. See the
[maker contract and zero-fill live evidence](../../docs/reference/predict-maker-order-validation.md).

Native `Market + TimeInForce::Fok` maps to Predict MARKET with
`isFillOrKill=true`; the FOK flag never goes in `params`. Existing MARKET GTC
behavior is retained with the field omitted. LIMIT FOK and IOC remain rejected.
Live evidence includes zero-match termination and one actual Native MARKET BUY
FOK full fill: 3.5 shares at .40 within a .4017 signed cap, with .07 shares fee.
This confirms that combination, not general all-or-none enforcement across every
liquidity shape. Its post-only TP rejection reached the strategy normally; the
subsequent SELL used an explicitly identified SDK fallback. See the
[recording and offline replay](tests/README.md#layers-and-focused-replays) and
[positive validation record](../../docs/reference/predict-immediate-order-validation.md#recorded-venue-observations).

Both MARKET sides require params["market_price"] as a strategy-supplied decimal
reference before slippage. Native MarketOrder has no price field. Optional
params["market_order_slippage_bps"] is a JSON integer in 0..9999, matching the
Native Hyperliquid parameter name. Omission means zero extra slippage, matching
the official Predict SDK. No client-level slippage setting is applied.
The adapter applies the resolved value once to signed amounts and HTTP metadata;
there is no execution-book fallback. Invalid values and LIMIT usage are denied.
HTTP slippageBps is not accepted as a Native param. This remains MARKET, not a
Native LIMIT or a verified per-match execution-price guarantee.
The current Native amount model is explicitly non minimum-out. Passing
isMinAmountOut or is_min_amount_out in params is denied before submission;
the flag changes signed BUY economics and cannot be silently ignored or
enabled by forwarding an HTTP field alone.

Native MARKET BUY also accepts `quote_quantity=true`: quantity is the maximum
USDT principal budget, not shares. Supply market_price and a resolved slippage of
zero (typically params["market_order_slippage_bps"]=0). The first version follows
the official SDK's value calculation at that single reference level, floors shares
to five significant digits, and publishes a Native OrderUpdated to base shares
before Submitted. OrderInitialized retains the original quote budget for recovery.
This is local budget-to-share preparation, not a new venue quote-amount field;
it can leave unspent dust. Quote SELL/LIMIT and positive-slippage quote BUY are
explicitly denied. See the [quote BUY contract](../../docs/predict-execution-adapter-design.md#market-buy-quote-sizing).

Early settlement Submitted/Confirmed/Failed uses account/strategy-scoped
PredictSettlementUpdate CustomData. Only confirmed fills update Native economic
state. See the [strategy guide](../../docs/how_to/predict-rust-strategy-calibration.md#quantity-fees-and-settlement-callbacks)
for early hedging, fee accounting and cancel/fill races.

## Public data and retention

PredictOrderbookSnapshot preserves the complete venue YES book for recording and
signals. Standard subscribe_book_depth provides an on-demand YES/NO L2 view over
the same venue subscription. Managed books can use Native quote projection for
risk/valuation; subscribe_quotes remains an explicit BBO API. See the
[strategy subscription guide](../../docs/how_to/predict-rust-strategy-calibration.md#market-data-subscriptions).
Predict selectors use canonical feed symbols such as BTCUSDT; their market/outcome
identities remain separate from Binance instruments.
Crypto Up/Down windows use market/title metadata, price bounds 0..1 and a 1 USDT
minimum notional; an ended trading window does not prove settled holdings.

Discovery/subscription rollover is shared with capture. Instrument metadata remains
while Native retained orders/open positions or unresolved facts require it. Books
and subscriptions retire separately. Execution contexts, routing, raw evidence and
settlement notices follow Native owner retention, without the former 10,000-fact
caps. Configure Native purge; bounded adapter maintenance does not promise constant
memory for active/unresolved owners or bound one huge history's disposal pause.

One market emits one custom-data identity. Competing explicit/rolling identities
are rejected by the session while the existing stream and native quote/depth
owners continue. See the [conflict contract](../../docs/predict-orderbook-capture-design.md#custom-subscription-identity-conflicts)
for asynchronous admission, unsubscribe and retry behavior.

## Development and evidence

The crate follows the same client boundaries as Native's Bybit and Polymarket
adapters:

| Module | Responsibility |
| --- | --- |
| `common/`, `http/`, `websocket/`, `signing/`, `blockchain/` | Venue identity, wire formats and transport primitives |
| `providers.rs`, `universe.rs` | Instrument conversion and rolling-market selection |
| `data/`, `data_types.rs` | Native data client, subscriptions and published custom data |
| `execution/` | Native commands, wallet events, account reports and reconciliation |
| `config.rs`, `factories.rs` | Native client configuration and construction |

Strategies, mock venues and recorders belong under tests or the application
assembly layer. No test strategy or recorder runs in production src.

- [Tests, fixtures and explicit benchmark exporters](tests/README.md)
- [Execution internals and limits](../../docs/reference/predict-execution-internals.md)
- [Private API calibration](../../docs/how_to/predict-private-api-tests.md)
- [Historical live evidence](../../docs/reference/predict-live-evidence.md)

Ordinary tests use fixtures/loopback services without credentials or venue writes.
Run only named ignored probes with explicit scope; never all ignored tests.
Recorded primary captures are immutable and hash-indexed. Operational logs and
archives stay in ignored data/predict-validation/, not versioned test inputs.
