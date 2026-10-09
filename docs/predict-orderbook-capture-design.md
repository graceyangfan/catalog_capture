# Predict orderbook capture

Catalog Capture records the native Predict.fun orderbook as a complete, typed
snapshot. It does not manufacture deltas, a second NO book, trades, candles, or
an oracle stream that Predict does not publish.

The implementation is intentionally narrow:

- `BinaryOption` definitions for the YES and NO outcome tokens;
- `PredictOrderbookSnapshot` for every accepted native **YES** book;
- optional Binance Spot SBE L2 and `TradeTick` in ordinary Nautilus market-data
  families, recorded independently as an alpha source.

## Native market model

One Predict binary market has two instruments and one upstream book:

```text
{market_id}-1.PREDICT  native YES outcome
{market_id}-2.PREDICT  native NO outcome
predictOrderbook/{market_id}  native YES orderbook topic
```

The provider publishes both immutable `BinaryOption` definitions before the
client admits snapshots for that market. The snapshot is always associated with
the YES `InstrumentId`; the catalog partition remains market-scoped:

```text
data/binary_option/{market_id}-1.PREDICT/…parquet
data/binary_option/{market_id}-2.PREDICT/…parquet
data/custom/PredictOrderbookSnapshot/{market_id}/…parquet
```

The NO view is a reader-side complement of the recorded YES book, quantized at
the market price precision:

```text
no_bid(price, size) = yes_ask(1 - price, size)
no_ask(price, size) = yes_bid(1 - price, size)
```

It is not separately subscribed or persisted, so recording does not duplicate
venue liquidity.

## Snapshot contract

`PredictOrderbookSnapshot` preserves the source observation without a floating
point conversion:

| Field | Meaning |
|---|---|
| `instrument_id` | Canonical YES `InstrumentId` from resolved market metadata |
| `market_id` | Native decimal market ID and custom-data partition identity |
| `bids`, `asks` | Complete native YES levels at declared price/size precision |
| `order_count` | Venue-wide displayed order count; never allocated to a level |
| `ts_event` | `updateTimestampMs`, converted directly to `UnixNanos` |
| `ts_init` | Local receive timestamp |

The canonical `DataType` is:

```text
type_name:  PredictOrderbookSnapshot
identifier: {market_id}
topic:      predictOrderbook/{market_id}
```

`market_id`, topic, and parsed frame identity must agree. A malformed or
mismatched frame is discarded before it reaches the catalog.

## Rolling crypto Up/Down products

Use `PredictCryptoUpDown` when the operator knows a product, not a transient
market ID. The full selector identity is:

```text
price_feed_symbol  exact `variantData.priceFeedSymbol` match
title_asset        exact title prefix, for example `Bitcoin`
interval_secs      title-derived interval, for example `300`
```

The official schema does not publish a universal coin or interval enum. The
operator must configure only a product currently advertised by Predict. The
client discovers the current/next deterministic category first and uses bounded
search only as a fallback. A selector with no live matching market is a
configuration/data-availability error; it must not be treated as an empty book.

For simultaneous products, the complete selector key—not interval alone—owns
the subscription, writer, and rollover state. BTC 5m and ETH 5m therefore have
independent writers even though their intervals are equal.

At a verified window boundary the client follows a safe handover:

```text
discover successor
  → publish successor YES + NO definitions
  → subscribe successor native topic
  → accept successor's first valid snapshot
  → seal retiring selector segment
  → release retiring topic/context and purge unreferenced cache definitions
```

The old stream remains valid until the successor has produced a snapshot.
Consequently a late valid old update belongs to the old parquet segment, while
the successor begins a fresh segment. Cache purge never deletes historical
parquet; it removes only retired, unreferenced Nautilus cache entries. A static
capture or another active selector still referencing that market prevents the
purge.

Normal shutdown seals all active selector writers, including a partially filled
interval, so there are no live `.part` files after a graceful stop.

## Recorder configuration

The Predict-only starter discovers BTC 5m automatically:

```toml
[[venues]]
id = "predict_main"
kind = "predict"

[[capture.custom_data]]
type_name = "PredictCryptoUpDown"
[capture.custom_data.metadata]
price_feed_symbol = "BTC/USDT"
title_asset = "Bitcoin"
interval_secs = "300"
```

Use [capture.predict-orderbook.toml](../examples/capture.predict-orderbook.toml)
for that standalone profile. Use
[capture.predict-binance-spot-sbe-btc-updown.toml](../examples/capture.predict-binance-spot-sbe-btc-updown.toml)
for BTC 5m and 15m snapshots plus hourly Binance Spot SBE L2/trades.

The Predict API key is not capture TOML. Copy
`examples/predictfun.credentials.toml.example` to ignored
`env/predictfun.toml`, put only `api_key` in it, and set mode `0600`.
`PREDICT_CREDENTIALS_FILE` is the explicit alternative for container
deployments. Binance SBE uses the analogous ignored
`env/binance-spot-sbe.toml`, containing only `api_key` and the local Ed25519
`private_key`. See [credential setup](how_to/credentials.md).

## Binance Spot SBE alpha source

Binance Spot SBE is optional and independent from Predict resolution. It stores
canonical Nautilus `OrderBookDeltas` and `TradeTick`, not a raw SBE custom type.
The Nautilus adapter performs the REST snapshot and sequence-gated SBE handoff.
Its hourly managed-book segment boundary makes each sealed hour independently
replayable.

Predict timestamps are milliseconds and SBE timestamps are microseconds. A
backtest must use an explicit as-of join and staleness bound; it must not invent
sub-millisecond ordering for Predict data.

## Reading and backtesting

Register the custom type before querying the Rust `ParquetDataCatalog`, then
query by **market ID**:

```rust
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_predict::register_predict_custom_data;
use std::path::Path;

register_predict_custom_data();
let mut catalog = ParquetDataCatalog::new(Path::new("./data/predict"), None, None, None, None);
let rows = catalog.query_custom_data_dynamic(
    "PredictOrderbookSnapshot",
    Some(&vec!["2859346".to_string()]),
    None, None, None, None, true,
)?;
```

Read the matching outcome definitions by `{market_id}-1.PREDICT` and
`{market_id}-2.PREDICT`. Each custom row itself carries the YES instrument ID.

The stored snapshot supports spread, mid, microprice, L1/L5/full-depth
imbalance, shape, displayed-liquidity, and snapshot-to-snapshot replenishment
research. `order_count` supports aggregate fragmentation analysis, but does not
describe individual levels.

## Non-goals

- Trading, balances, private wallet events, split/merge execution, and market
  making.
- Derived Predict `OrderBookDeltas`, duplicate NO depth, synthetic candles, or
  a raw JSON side channel.
- Pretending a missing snapshot or unavailable product is an empty book.
- Using a community SDK or undocumented oracle topic as a resolution source.

Sources: [Predict subscription topics](https://dev.predict.fun/subscription-topics-1915507m0),
[Predict orderbook semantics](https://dev.predict.fun/understanding-the-orderbook-685654m0),
and [Binance Spot SBE market-data streams](https://github.com/binance/binance-spot-api-docs/blob/master/sbe-market-data-streams.md).
