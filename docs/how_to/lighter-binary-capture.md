# Record Lighter binary options

`nautilus-lighter-ext` is an independent, unofficial extension of the
NautilusTrader Lighter adapter. It directly depends on and re-exports
`nautilus-lighter` as `upstream`; it does not copy the upstream adapter.

The current integration records public mainnet Up/Down markets. It provides
fixed-market recording and automatic discovery of BTC/ETH 15-minute markets.
It has no execution client, authenticated account requests, or candle output.

## Build and run

Follow the [dependency setup](../getting_started/installation.md) first. The
Nautilus revision is pinned in the repository's CI workflow.

```sh
cargo build -p catalog-capture-cli --no-default-features --features venue-lighter-ext
./target/debug/catalog-capture-cli run --config examples/capture.lighter-updown-15m.toml
```

The [automatic recording profile](../../examples/capture.lighter-updown-15m.toml)
records BTC and ETH without instrument IDs. It runs until Ctrl+C or SIGTERM and
uses segment mode. Select series with `tag` only:

```toml
[[capture.lighter_updown]]
tag = "btc-updown-15m"

[[capture.lighter_updown]]
tag = "eth-updown-15m"
```

Each entry automatically records all five data families below. No
`capture.instruments`, `capture.trades`, `capture.book_deltas`,
`capture.index_prices` or OI `capture.custom_data` entries are needed.
Remove either selector to record just BTC or ETH.

Discovery defaults to `refresh_secs = 10` and `prefetch_secs = 30`. Set these
fields inside a selector only when changing that cadence. The recorder derives
market IDs and expiry from venue responses, subscribes successors, seals old
partitions and releases expired runtime state. Currently only 15m series are
supported; changing a tag to `btc-updown-5m` is not supported.

Use `kind = "lighter_binary"`, `id = "LIGHTER"`, `environment = "mainnet"`.
The original `lighter` client and `lighter_binary` cannot share one node because
they share venue routing. Other dynamic option/HIP-4 universes cannot currently
be combined with the rolling selector. At most two distinct series and eight
current/prefetched markets are accepted; excess subscriptions fail explicitly.

## Recorded data

| Family | Representation | Source and meaning |
| --- | --- | --- |
| Instrument | `BinaryOption`, ID `<market_id>-BINARY.LIGHTER` | One Up-priced contract per market; full venue symbol in `raw_symbol` |
| Trades | `TradeTick` | Public `trade` channel; stable ID deduplication; settlement payouts excluded |
| Book | Full L2 `OrderBookDeltas` | `order_book` snapshot and sequenced updates; no finite-depth selector |
| Open interest | `LighterOpenInterest` CustomData | `bo_market_stats.open_interest`, exact Decimal source value, including zero |
| Oracle | `IndexPriceUpdate` | Underlying USD oracle from the 1s `up_down_candle` close, fixed at 16 decimal places |

OI has no matching native Nautilus numeric data family. Its canonical `DataType`
is `LighterOpenInterest`, with the decimal market ID as identifier and no
metadata. Missing OI is not converted to zero.

Oracle precision is independent of the binary contract's trading precision.
Conversion uses Decimal rounding to nearest with ties to even, without `f64`.
The instrument's `info.oracle` records symbol, duration, provider, feed, unit,
resolution and precision. The oracle is an underlying USD reference price;
the contract book/trades are USDC prices per share.

`info.start_price` is the exact oracle close at `event.start_at`, rounded to 16
places. It is a decimal string. Fixed mode fails when this observation is missing
or has the wrong timestamp or source. Rolling mode first publishes a provisional
instrument without `start_price`, then writes a new definition when the exact
boundary observation becomes available. Readers should choose the latest
`ts_init` definition. Publication can take a discovery refresh after the source
makes the boundary value available.

`activation_ns` is the venue trading start; `info.observation_start_ms` is the
separate observation start. Up/Down outcomes share a single public Up-priced
book; the adapter does not invent separate YES/NO token streams.

## Rotation and file boundaries

Discovery uses standard `RequestInstruments`/`InstrumentsResponse`. Venue HTTP
runs in the data client's TaskGroup with cancellation and an overall timeout;
capture owns recording selection and the expiry timer. Trade/book wire models,
parsers and rate limits are reused from upstream. Transport uses Nautilus
WebSocket epochs, SocketControl and heartbeat handling. A nonce gap, queue
overflow or invalid stream requires reconnect and a new book snapshot.

Each market has independent trade, book, OI and oracle partitions. Preloading
opens the successor's files before its observation window; there is no single
combined file created exactly every 15 minutes. At `expiration_ns`, capture:

1. Removes its subscriptions and stops accepting old-market data.
2. Drains and seals the market's partitions without closing successor files.
3. Checks other owners after Native processes queued unsubscribe commands.
4. Purges unused instrument/cache state; the adapter releases expired context,
   nonce, trade deduplication and oracle deduplication state.

Static capture owners and other subscribers are respected. Retirement does not
wait for settlement, zero OI or an empty book. Historical parquet files remain.
Late HTTP responses cannot restore expired markets or reopen retired files.
The shared oracle channel stays subscribed while a successor owns it.

A source point can arrive after the instrument expires. It then belongs to the
remaining oracle subscribers, including a preloaded successor. One-second
resolution does not guarantee one point every second: missing source seconds
are not filled or synthesized. A complete underlying oracle timeline may
therefore span adjacent market partitions.

Instrument definitions are flushed before market-data subscriptions. A
prefetched market normally has two definition files: provisional metadata and
the start-price update. Order-book segments contain a snapshot and can be
validated with the existing Nautilus catalog replay path. Graceful shutdown
seals `.parquet.part` files.

## Tests and validation

Offline tests require no keys or network access:

```sh
cargo test -p nautilus-lighter-ext --locked
cargo test -p catalog-capture-runtime-adapter --lib --locked
```

Fixtures cover five-family catalog round trips, Decimal precision, missing/zero
OI, duplicate trades, settlement filtering, shared oracle owners, nonce gaps,
reconnect/epoch fencing, reset, exact start boundaries, pagination limits and
expiry cleanup despite non-zero OI. Accelerated rotation tests use the real
LiveNode, capture actor and parquet catalog, including a stale HTTP response
crossing expiry and a successor that continues writing while old files seal.

An opt-in mainnet smoke test selects a current ETH market, records for 20
seconds, then reads the catalog and replays the book. It removes its temporary
catalog on success. Set `LIGHTER_TEST_CATALOG` to a new directory to retain it:

```sh
cargo test -p nautilus-lighter-ext --test live_capture mainnet_fixed_market_catalog_readback -- --ignored --nocapture
```

For a same-process two-boundary run, use the rolling CLI profile, stop gracefully
after the second rotation, and read back the three generations of BTC/ETH:

```sh
LIGHTER_TEST_CATALOG=/absolute/path/to/catalog \
LIGHTER_TEST_GENERATIONS=3 \
cargo test -p nautilus-lighter-ext --test live_capture rolling_catalog_readback -- --ignored --nocapture
```

The readback test checks every family's presence, start prices, canonical OI
identity, trade deduplication, 16-place oracle values, retired cache audit,
book replay and the absence of unfinished files. Reports go to stdout.

Readback verifies every selected market and data family after graceful shutdown.
A short smoke or two-boundary run does not establish multi-hour stability or
forced-disconnect recovery. Keep raw operational recordings outside versioned docs.

## Fixed-market diagnostics

Explicit instrument selectors remain supported for targeted diagnostics. The
[static-selector test fixture](../../crates/catalog-capture-cli/test_data/lighter_binary_fixed.toml)
contains historical IDs solely for configuration regression tests; it is not an
operator recording profile. A diagnostic run requires replacing every ID with a
currently observing market from `/api/v1/events` whose exact start-price
observation is available. Fixed-market mode does not automatically replace IDs.
Use the automatic profile above for continuous recording.

## Protocol references

- [Official binary market data documentation](https://apidocs.lighter.xyz/docs/binary-options-websocket)
- [Official positions and settlement documentation](https://apidocs.lighter.xyz/docs/binary-options-positions-and-settlement)
- [Official Python SDK EventApi](https://github.com/elliottech/lighter-python/blob/56b3537cd311117a5a665d74db4c2cce7da155de/docs/EventApi.md)
- [Official Python SDK UpdownApi](https://github.com/elliottech/lighter-python/blob/56b3537cd311117a5a665d74db4c2cce7da155de/docs/UpdownApi.md)

The SDK is a protocol reference, not a runtime dependency. As upstream gains
binary support, individual extension endpoints/parsers can be replaced while
preserving Nautilus data types, identifiers and capture contracts.
