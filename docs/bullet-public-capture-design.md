# Bullet public capture contract

The read-only `nautilus-bullet` adapter uses Native HTTP/WebSocket transport,
`DataClient`, `TaskGroup`, models and Parquet codecs. It is enabled by
`venue-bullet` or `all-venues`. The official SDK supplies protocol definitions,
not a second transport or connection manager.

## Supported data

| Family | Source and mapping |
| --- | --- |
| Instruments | `/fapi/v1/exchangeInfo`; `CryptoPerpetual`, native raw symbol and currencies |
| L2 | Explicit `depth=20`, `L2_MBP`, `SYMBOL@depth20`; source snapshots and absolute-quantity increments |
| Trades | `SYMBOL@aggTrade`; Native `TradeTick`, aggregate ID `a`, price `p`, quantity `q` |
| Mark/index | Shared `SYMBOL@markPrice`; independently owned Native price subscriptions |
| Settled funding | `/fapi/v1/fundingRate`; Native hourly gross `FundingRateUpdate` |
| Funding estimates | Request-style `BulletFundingEstimate`, sampled through Native recorder jobs |

Execution, private streams, full-depth books, BBO, liquidations and open interest
are outside this adapter's supported scope. The SDK exposes `aggTrade` without a
separate public raw-trade topic; the adapter does not aggregate trades locally.
The [capture guide](how_to/bullet-capture.md) contains configuration and checks.

## Protocol authority

- [Bullet Rust SDK 0.0.48, pinned source](https://github.com/bulletxyz/bullet-rust-sdk/tree/79e25490d6de2acf9a25d02a791f1f7745950ef5): `rust/src/ws/topics.rs`, `models.rs` and `rust/openapi.json`.
- [Supplementary wire interface](https://github.com/bulletxyz/bullet-ws-interface/blob/6c8c6339344adccb5f327ae9df5f0856b37538db/src/server.rs): independently reviewed wire types and upstream trade fixture; not asserted to be the SDK dependency's resolved revision.
- Official [WS reference](https://tradingapi.bullet.xyz/docs/ws/),
  [REST reference](https://tradingapi.bullet.xyz/docs/rest) and
  [connectivity reference](https://tradingapi.bullet.xyz/docs/connectivity.html).
- [Versioned fixture provenance](../crates/nautilus-bullet/test_data/README.md).

The live funding API includes required multiplier/spread fields absent from the
pinned SDK schema. Missing fields fail parsing rather than receiving defaults.

## Identity, precision and time

`BTC-USD` maps to `BTC-USD-PERP.BULLET`. Definitions load on each DataClient
connection and remain immutable within that task generation. Exact-ID providers
reject missing, foreign or unsupported IDs. Supported `CryptoPerp` states are
`TRADING`, `POST_ONLY`, `CANCEL_ONLY` and `HALT`; dynamic listing refresh is not provided.

Increments come from `PRICE_FILTER.tickSize` and `LOT_SIZE.stepSize`. Price
precision is the greater of the declared price precision and normalized tick
precision; size precision comes from the normalized lot step. BTC's fixture has
price precision 8, tick 1 and lot step 0.0001, despite `quantityPrecision=0`.
Base, quote and margin currencies retain their API identities. Positive filter
limits are retained; zero disables a limit. Invalid or contradictory filters fail.

Prices and sizes remain decimal strings until checked conversion; numeric IDs
remain integers until Native identity construction. Book/trade values exceeding
instrument precision fail. Mark/index values use Native half-even rounding at
instrument price precision. Standard price catalogs contain those canonical
rounded values; optional mark execution identity remains on the wire model.

| Observation | Native `ts_event` |
| --- | --- |
| Book | Chain-state `T` in microseconds, multiplied by 1,000 |
| Trade | Trade `T` in microseconds, multiplied by 1,000 |
| Mark/index | Publication `E` in microseconds, multiplied by 1,000 |
| Settled funding | Historical `fundingTime` in microseconds, multiplied by 1,000 |
| Funding estimate | Premium-index `time` in milliseconds, multiplied by 1,000,000 |

`ts_init` records local receipt/admission time separately. Conversions use checked
integer arithmetic. Source clocks may lead the host clock; timestamps are not clamped.

## L2 sequence and recovery

Native `OrderBookDelta.sequence` preserves source `u`. The adapter validates
`U == u`; an incremental message requires a current snapshot, `pu == previous_u`
and `u > pu`. Zero quantity removes a level; other quantities are absolute.
All snapshot rows carry `F_SNAPSHOT`, and only the final row carries `F_LAST`.
An empty snapshot clears the book. Empty increments emit no Native batch but
still advance the wire cursor.

The source defines the same change-driven cadence for depth 5/10/20/100 and
ignores speed suffixes. A top-20 subscription receives every update, including
empty messages for deeper-only changes. There is no local sampling or coalescing.
Levels leaving the top 20 arrive with zero quantity; entering levels carry full quantity.

One session task owns desired subscriptions, pending operations and publication.
ACK ID, operation and success result must match before admission. Mark and index
have independent logical references but share one physical subscription. The final
release retires that stream. A renewed owner waits for an outstanding unsubscribe
ACK before subscribing again. Errors are decoded before success/data messages.

The session polls one epoch-bound subscription write alongside transport loss,
frames, health checks and cancellation. Old epochs are rejected before parsing.
Native transport owns reconnection; the adapter replays desired subscriptions and
waits for a fresh snapshot. A gap, malformed supported frame, overflow or timeout
fails the current epoch. Source book time older than the last published state is rejected.

Transport loss invalidates each ready book once with a Native CLEAR:
`sequence=0`, `F_LAST`, **no `F_SNAPSHOT`**. Its event time is the last published
book-state time; its receive time is the local loss-detection time. The same
invalidation precedes an in-connection snapshot with a lower `u`, or a higher `u`
whose `pu` does not join the current cursor. A repeated snapshot at the same `u`
does not imply a gap. Session teardown invalidates ready books; DataClient
reconnection drains the old task generation before replacement.

| Resource | Bound |
| --- | --- |
| Raw ingress / pre-ACK staging | 256 items and 8 MiB each, with global staging limits |
| WS message | 2 MiB |
| Command queue | 256 commands |
| Subscription ACK / first snapshot | 10 seconds |
| Heartbeat | Application PING every 20 seconds; Native peer timeout 65 seconds |
| Reconnect delay | 1–30 seconds, exponential backoff with jitter |

Trade deduplication retains 10,000 aggregate IDs per instrument across transport
reconnects, committing IDs after successful publication. `m=true` means sell
aggressor; `false` means buy. Final trade unsubscribe retires the dedupe state.
Deprecated account/fee placeholders are ignored. Arbitrarily old replay and
missed trades are not backfilled.

## Parquet and Native backtesting

The recorder writes the adapter's invalidation CLEAR into ordinary Native
`OrderBookDelta` Parquet **before** sealing the old continuity window. It retains
the control's timestamps and flags, and records the source gap separately.

The stream is source A, persisted unflagged CLEAR, source B snapshot, then B
increments. Ordinary Native catalog queries can feed one unmodified
BacktestEngine without a continuity loader or replay-time reset injection.
Only the local control uses sequence zero; source `u` is unchanged. The control
is distinct from an exchange snapshot when validating source conformance.

Continuous periodic snapshots remain in the same window. Healthy hourly rotation
writes a managed-book initializer with the last applied `u`, without incrementing
it. An invalid book cannot produce cached initializers or resume from unchecked
increments. Normal files start with a snapshot; a control-only file can contain a
lone unflagged CLEAR. Native readback checks initializers against the complete
preceding source book, including prices and quantities.

Source gaps keep the production quality verdict incomplete. A recovered book
and a successful backtest do not recreate missing observations or certify fills
across an outage. Existing catalogs without persisted invalidations are not
retroactively repaired. Window readers remain available for independent experiments.

## Funding contract

The history endpoint reports eight-hour gross rates while settlement is hourly.
Standard Native funding records use:

- `rate = source fundingRate / 8`, exactly representable and reversible;
- `interval = Some(60)`, `next_funding_ns = None`;
- the historical settlement time, never a forecast or local startup time.

Instrument `info.funding_rate_contract` identifies this as `settled_gross`, with
recorded basis 60 minutes, source basis 480 minutes, source `/fapi/v1/fundingRate`
and `funding_spread_applied=false`.

Only `fundingIntervalHours=1` is supported. History must have correct symbols,
positive hourly-aligned microsecond timestamps and strictly increasing times.
Zero/negative rates are supported. WS `markPriceUpdate.r`, prospective next-funding
time and estimate rollover do not manufacture confirmed settlements.

`BulletFundingEstimate` keeps `estimated_rate_8h`, `reported_last_rate_8h`,
`interval_minutes`, `next_funding_ns`, eight-hour cap/floor, already-applied
multiplier and funding spread. These six decimal fields use Native serde-backed
UTF-8 storage to preserve exact values and scale rather than generic Arrow scale
16. The multiplier is not applied twice; spread is metadata, not a local adjustment
to the standard gross rate or proof of an account's net payment.

The canonical custom DataType is `BulletFundingEstimate`, identifier equal to the
full InstrumentId, and metadata containing exactly `instrument_id`. Requests
support current snapshots only. Five-second sampling does not capture every
intermediate estimate change. Premium-index next-funding time is milliseconds,
converted separately from history's microseconds.

A TaskGroup-owned worker tracks funding owners and acquisition generations,
independently of the WS epoch. Cold acquisition emits the latest historical row
once (`limit=1`). Later polls use `startTime=last_published_us+1&limit=1000`.
Only successful publication advances the cursor; failures retain it for retry.
Results from retired owners are discarded. A process restart starts from the
latest historical baseline; the cursor is not durable. Estimate requests retain
Native request identity and are cancelled with their task generation.
