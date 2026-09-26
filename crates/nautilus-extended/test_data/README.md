# Extended Test Data

## Sources

The following fixtures were captured verbatim from Extended Mainnet on
2026-09-26 using the public HTTP API and RPC v2 WebSocket endpoint:

- `http_markets_btc.json`
- `ws_rpc_responses_btc.json`
- `ws_orderbook_snapshot_btc.json`
- `ws_orderbook_delta_btc.json`
- `ws_trades_replay_btc.json`
- `ws_mark_price_btc.json`
- `ws_index_price_btc.json`
- `ws_funding_rate_btc.json`

Endpoints:

- `https://api.starknet.extended.exchange/api/v1/info/markets?market=BTC-USD`
- `wss://api.starknet.extended.exchange/stream.extended.exchange/v2/rpc`

The order-book, trade, mark-price and index-price fixtures came from one
multi-topic connection and preserve the venue's connection-global `seq`
values. The funding fixture was captured later from its own mainnet RPC v2
connection and therefore has an independent sequence beginning at zero. All
payload fixtures are live venue data, not documentation examples.

## References

- API documentation: <https://api.docs.extended.exchange/>
- Official Python SDK: <https://github.com/x10xchange/python_sdk/tree/starknet>
