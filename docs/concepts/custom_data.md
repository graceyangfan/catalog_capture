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
