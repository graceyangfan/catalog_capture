# Predict market-data capture

Use `examples/capture.predict-native.toml` for BTC 5m, or
`examples/capture.predict-crypto-updown.toml` for BTC/ETH/BNB 5m and 15m plus
Binance Spot SBE L2 and trades. Run from the repository root:

```bash
cargo run -p catalog-capture-cli --no-default-features \
  --features venue-predict,venue-binance -- \
  validate --config examples/capture.predict-crypto-updown.toml

cargo run -p catalog-capture-cli --no-default-features \
  --features venue-predict,venue-binance -- \
  run --config examples/capture.predict-crypto-updown.toml
```

Keep credentials in ignored `env/` files; see [credentials](credentials.md).
The combined profile requires both Predict and Binance SBE credentials.

## Output and rotation

Each `PredictCryptoUpDown` selector discovers the current market without a fixed ID.
It publishes both outcome `BinaryOption` definitions and records the YES view:

```toml
[[capture.custom_data]]
type_name = "PredictCryptoUpDown"
[capture.custom_data.metadata]
price_feed_symbol = "BTCUSDT"
title_asset = "Bitcoin"
interval_secs = "300"
record_snapshot = "false"
record_depth = "true"
record_trades = "true"
```

`custom_data` configures the discovery subscription. With `record_snapshot=false`,
snapshots remain internal selection signals and no custom snapshot writer is created.
Native files use the ordinary Nautilus catalog schemas:

```text
data/order_book_depths/<market>-1.PREDICT/*.parquet
data/trades/<market>-1.PREDICT/*.parquet
data/binary_option/<outcome instrument>/*.parquet
```

The exact instrument metadata layout follows the Native catalog. Set
`record_snapshot="true"` when the additional venue fields are needed. Its default
is true for existing profiles; depth and trades default to false. At least one
family must be enabled. These options do not change market selection identity.
For fixed instruments, use `[[capture.book_depth]]` with `instrument_id`,
`book_type="L2_MBP"` and optional positive `depth`, plus ordinary `[[capture.trades]]`.

A successor's first valid snapshot confirms rollover. The recorder admits its
native streams, releases its old interests and seals only the retired instrument's
partitions. Each selector rolls independently. Explicit strategy owners may retain
the old market. Hourly seals in the combined profile also bound native segments;
shutdown flushes and seals remaining parts.

## Streams and strategy subscriptions

`predictOrderbook/{marketId}` supplies complete YES books. Snapshot, native depth
and quote consumers share one physical subscription. Depth uses the existing
adapter conversion, preserves full depth, sorts best first and removes zero sizes.
NO depth complements prices and swaps sides. The latest processed snapshot can
initialize a newly admitted depth stream. Admission is not historical replay;
a consumer joining a shared Native stream receives subsequent publications.

`predictTrades/{marketId}` is an independent subscription. Rust strategies use
standard Native methods and callbacks:

```rust,ignore
self.subscribe_book_depth(yes_id, BookType::L2_MBP, None,
    Some(ClientId::from("PREDICT")), false, None);
self.subscribe_trades(yes_id, Some(ClientId::from("PREDICT")), None);
// Implement on_book_depth(&OrderBookDepth) and on_trade(&TradeTick).
```

Use `managed=true` when Native should maintain the cached book. Strategy-owned
subscriptions survive recorder rollover until the strategy releases them.
Reconnect reconciles independent book and trade interests. Last-owner removal
clears retained book snapshots. Depth limits on one instrument must agree.
See the [strategy guide](predict-rust-strategy-calibration.md#market-data-subscriptions)
for managed-book quotes and risk pricing.

## Trade conversion

The taker determines aggressor direction relative to the subscribed outcome.
Each maker allocation produces one tick at its execution price and gross size;
the taker total is not recorded again or substituted for individual maker prices.

| Taker | YES price | YES aggressor |
| --- | --- | --- |
| YES Bid | p | Buy |
| YES Ask | p | Sell |
| NO Bid | 1-p | Sell |
| NO Ask | 1-p | Buy |

Matched `COMPLEMENTARY`, `SPLIT` and `MERGE` events are executions. A standalone
wallet split or merge is not a public trade. Validate market identity, outcomes,
sides, prices, positive sizes and maker allocation totals before publication.

Decode price and size strings as 18-decimal integers. Complement prices before
conversion, then truncate to the catalog-compatible fixed precision (16 with the
default high-precision feature). Positive sizes that truncate to zero are rejected.
Native `defi` quantities can retain 18 decimals in memory, but that does not make
the standard market-data catalog support precision above its fixed scale.
Execution fees and inventory keep their separate exact-atomic contract.

`ts_event` is the venue `tsMs` in nanoseconds; `ts_init` is local ingestion time.
Intervals between venue timestamps measure source cadence, not network latency.
The feed supplies no venue match ID: generated TradeIds are local UUIDs, distinct
identical-looking executions are retained, and reconnect has no lossless replay
or exactly-once guarantee.

## Custom subscription identity conflicts

One market has one custom snapshot output identity in a session. Conflicting
explicit and rolling identities are rejected asynchronously without replacing
existing owners. Release the original custom identity before retrying another.
Native quote, depth and trade owners remain independent. Equivalent rolling
selector subscriptions retain distinct owners; removing one does not stop another.

## Verification

The loopback `quote_rollover` integration test uses a real Rust Strategy, LiveNode,
recording actor and catalog. It covers both WS transports, snapshot-enabled and
native-only recording, two populated markets and independent old-market owners.
Depth prices and quantities are compared with source frames; native files are
read back after shutdown. The `capture_rollover` test preserves legacy
snapshot-only capture coverage. See [validation](../reference/predict-validation.md).
