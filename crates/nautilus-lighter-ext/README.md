# nautilus-lighter-ext

Independent, unofficial Lighter binary-option market data for NautilusTrader.
Directly depends on the unmodified `nautilus-lighter` crate and re-exports it as
`upstream`; no copied adapter or Python SDK runtime dependency.

Publishes `BinaryOption`, `TradeTick`, full L2 `OrderBookDeltas`, exact-Decimal
`LighterOpenInterest` CustomData, and 16-decimal-place oracle `IndexPriceUpdate`.
Fixed mainnet markets and automatic BTC/ETH 15m recording are supported through
`catalog-capture-cli` with `venue-lighter-ext`.

See the [operator guide](../../docs/how_to/lighter-binary-capture.md),
[automatic recording profile](../../examples/capture.lighter-updown-15m.toml), and
[mainnet validation](../../docs/reference/lighter-binary-validation.json).

```sh
cargo test -p nautilus-lighter-ext --locked
cargo build -p catalog-capture-cli --no-default-features --features venue-lighter-ext
```

Offline tests exercise protocol fixtures through the real LiveNode, capture
actor and parquet catalog. Network smoke/readback tests are opt-in; see the
guide for commands and temporary-file retention. No authenticated requests or
orders are sent.
