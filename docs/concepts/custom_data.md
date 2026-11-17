# Custom data

Built-in families (quotes, trades, bars, book deltas, greeks, …) are preferred when the
adapter already models the data that way.

Use **custom data** only when the adapter emits a type that is research-critical and
not a built-in family (for example DVOL, book summary, open interest).

## Config channels (strict)

| Channel | TOML | On disk |
|---------|------|---------|
| Stream / subscribe | `[[capture.custom_data]]` | `data/custom/{TypeName}/` |
| Poll / request | `[[capture.custom_data_requests]]` | same sink path |

Wrong channel → validation failure. Subscribe and request share the same catalog prefix
so loaders do not care how the type was obtained.

## Rules

- Prefer types adapters already emit; do not invent research schemas by default.
- Request jobs expose counters on `/metrics` when metrics are enabled.
- See examples: `capture.deribit-dvol.toml`, `capture.deribit-btc-book-summary.toml`,
  `capture.hyperliquid-open-interest.toml`.

## Binance current open interest

`BinanceFuturesOpenInterest` is request-only. The recorder reuses the pinned
Native Binance DataClient, Decimal model, Arrow registration and catalog decoder;
it does not make a second HTTP request or introduce another OI schema.

See `examples/capture.binance-futures-open-interest.toml` for BTC/ETH polling
every 5 seconds per symbol. This is an explicit recorder setting, not a Binance
refresh guarantee; the generic request default remains unchanged.
Set `metadata.instrument_id` to a canonical Binance Futures instrument. The
resolver supplies the same catalog identifier, records its instrument definition,
and selects the matching configured Futures client. An explicit `client_id` must
match that product client; missing or ambiguous matches are rejected.

Rows are stored under
`data/custom/BinanceFuturesOpenInterest/{instrument_id}/`. Native `ts_event`
identifies the exchange snapshot time and `ts_init` is assigned after the REST
response. The recorder preserves both and the exact Decimal OI quantity, without
converting it to USD notional. Segment files become queryable after seal or
orderly shutdown.

Poll responses accept Native single `CustomData` and batch `Vec<CustomData>`
payloads. Native request IDs correlate completion; late responses retain their own
Native timestamps and cannot finish a successor poll. Unknown payloads are a
recording error, not a successful zero-row response. Historical OI is not enabled
by this profile.

The offline `current_oi` tests exercise the real Native Binance HTTP request
against a local synthetic exchange response, then the actor response lifecycle,
segment sealing and Native catalog readback. They verify both symbols, exact
quantities and timestamps, and late-response isolation; they do not establish
mainnet availability.
