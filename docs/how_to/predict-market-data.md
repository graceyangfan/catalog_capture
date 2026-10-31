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
NO depth complements prices and swaps sides. The latest admissible source snapshot
from the active connection can initialize a newly admitted depth stream. Older
source timestamps cannot rewind it; equal timestamps and empty books remain valid.
Reconnect invalidates initialization readiness until an admissible fresh snapshot.
Admission is not historical replay;
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
clears retained book source state. Depth limits on one instrument must agree.
See [realtime adapter contracts](../developer_guide/realtime-adapters.md) for
ownership, partial acceptance and send scheduling.
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

A completed six-selector native-only capture crossing a shared 5m/15m boundary can
be checked offline with the Native catalog reader:

```bash
PREDICT_PROBE_CATALOG=/absolute/path/to/catalog \
  cargo test -p nautilus-predict --test catalog_readback \
  native_six_selectors_roll_and_read_back -- --ignored --nocapture
```

This checks both windows for each selector, source timestamp ordering, rollover
ordering, instrument definitions and native TradeTick readback. Quiet markets need
not produce trades. It requires no network access or credentials.

## 4 CPU / 8 GiB recorder

The combined profile retains all six Predict selectors and BTC/ETH/BNB Spot SBE L2
and trades. Its queue and buffer limits apply per family, not to total process RSS.
Depth snapshots use a separate 512-row profile; incremental L2 keeps its larger
row groups. No source sampling or depth truncation is used to reduce resources.

Use a release binary built with `venue-predict,venue-binance`. Prefer deploying a
prebuilt package to the recorder; building the Rust dependency graph has a separate
memory requirement. The recorder exposes metrics on `127.0.0.1:9898`.

For a 4-core host, start with the existing Native runtime controls rather than
introducing a second thread-pool configuration:

```bash
CAPTURE_FEATURES=venue-predict,venue-binance CARGO_BUILD_JOBS=2 \
  make build-release-capture
NAUTILUS_WORKER_THREADS=2 TOKIO_WORKER_THREADS=2 \
  ./scripts/run-capture-service.sh \
  --config examples/capture.predict-crypto-updown.toml --prebuilt
```

These are starting values, not CPU or RSS limits. The four enabled persistence
families have their own worker threads. Keep `dropped_items_total=0`, observe
`queued_bytes` and RSS, and check sustained queue growth before changing concurrency.
The combined profile declares ingress budgets per family:

| Family | Message slots | Estimated bytes |
| --- | ---: | ---: |
| Binance L2 deltas | 32,768 | 64 MiB |
| Predict Depth | 8,192 | 32 MiB |
| All TradeTick | 32,768 | 32 MiB |
| Definitions | 1,024 | 4 MiB |

These are provisional budgets for the first server recording. Edit
`[output.queues.<family>]` to tune `capacity` and `max_bytes`, then restart
gracefully. Effective limits appear as `queue_capacity` and `queue_max_bytes` in
per-family metrics. These overrides affect ingress only, not partition buffers,
flush intervals or row-group sizes. Configurations without overrides retain
their previous fallback limits. See [queue configuration](../developer_guide/realtime-adapters.md#configuring-family-ingress-budgets)
for precedence and field names.

The reference profiles flush partition buffers every two seconds and periodically
fsync already emitted bytes every ten seconds. These are independent worker
deadlines. Less frequent fsync reduces recurring disk stalls without sampling or
changing the flush interval; it cannot guarantee that one long stall will fit the
queue. A completed seal syncs its footer. An abrupt exit before the footer can
still lose the active part, so ten-second fsync is not a ten-second loss bound.

Use `peak_queued_items` and `peak_queued_bytes` to see bursts between five-second
scrapes. Aggregated high-water marks sum family maxima, which may occur at
different times. Preserve `fail_fast`; a full queue rejects the new item and
marks the run as having a gap. It preserves older admitted observations and
continues recording subsequent data as a new replay window.

Rollover and hourly seal commands are queued with data. The writer closes the old
part before processing the new part's checkpoint or later rows, without blocking
the Native receive thread. Short disk stalls can be absorbed within queue capacity;
sustained overload and feeds without replay cannot guarantee gap-free capture.

Check `metadata/capture_quality.jsonl` before selecting backtest intervals. Each
run has a unique ID with an initial `in_progress` record and a terminal `completed`
or `failed` record, plus observations when fault counters change. Ordinary queue
and file faults do not stop the node: later data continues recording, while the
quality history retains the gaps. A missing terminal record is unfinished/unknown.
After an operator-requested stop, known gaps or incomplete shutdown produce a
failing CLI result even with HTTP metrics disabled. `/health` stays available for
ordinary quality gaps and reports 503 for an unavailable worker, which also requests
Native shutdown. The existing systemd helper uses `Restart=on-failure` and a 30-second
restart delay for that fatal case. `completed` certifies only observed local admission
and persistence, not uninterrupted venue coverage through startup or reconnect.

Failed Arrow appenders are quarantined as `.parquet.failed` and later batches open
fresh parts. They remain outside normal catalog queries. Instrument definitions
are retried from Native cache. Binance L2 resumes after a known local gap with a
full managed-book checkpoint in a new segment. The checkpoint restores later book
state; it cannot recreate the missing updates. Keep gap observations and failed
files available when choosing backtest windows.

For `x..A + gap + C..y`, run two separate backtests with fresh engines, strategies,
accounts and books. Use `catalog_capture_core::read_book_replay_windows` for L2
and `read_replay_windows::<TradeTick>` / `::<OrderBookDepth>` for other inputs.
Check the L2 window's `has_book_baseline()` and align all required feeds with their
own initialization/warmup. A failed capture run does not discard its good windows.
Hourly files within one healthy window remain one continuous experiment; marked
initialization prefixes are skipped except when needed at replay start after
retention. A raw Native catalog query ignores these boundaries and prefixes.
See [replay policy](../developer_guide/realtime-adapters.md#faithful-replay-across-file-boundaries).

Daily disk use of 3–4 GB fits an 80 GB SSD with ample operating space. Retention
should remove only closed, old market-data Parquet files. Preserve instrument
partitions and metadata: Binance definitions may have been written only at startup. Keep current `.part` files and their
parent directories until seal/shutdown; unlinking them while the writer is active
can lose the current segment. The smoke cleanup helper is not a retention job.

See [capture resource ownership](../developer_guide/realtime-adapters.md#capture-resource-ownership)
for queue, Arrow and Native cache boundaries and remaining recovery limitations.
