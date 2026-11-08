# Replay a captured catalog in Rust

Capture writes standard Native Parquet directly. No staging or format conversion
is needed. For catalogs predating the v0.65 persistence refactor, migrate into a
separate empty destination before using a current reader.

## Read independent replay windows

Use the capture window reader for instrument-keyed market data. Ordinary Native
catalog queries ignore continuity metadata and local initialization prefixes.
They are useful for inspection, but raw concatenation can bridge known gaps.

```rust,ignore
use catalog_capture_core::{read_book_replay_windows, read_replay_windows};
use nautilus_model::{data::{OrderBookDepth, TradeTick}, identifiers::InstrumentId};
use std::path::Path;

let root = Path::new("data/predict-crypto-updown");
let id = InstrumentId::from("BTCUSDT.BINANCE");
let books = read_book_replay_windows(root, id)?;
let trades = read_replay_windows::<TradeTick>(root, id)?;
// For Predict, use its recorded YES instrument ID:
// let depths = read_replay_windows::<OrderBookDepth>(root, predict_id)?;

for window in books {
    anyhow::ensure!(window.has_book_baseline(), "missing L2 initialization");
    // Create a fresh Native BacktestEngine, strategy, account and book.
    // Initialize and warm up all required feeds within this continuous window.
    // Pass window.data as Native Data::BookDeltas, with aligned trades/depth.
    // Run, collect results and dispose before starting another window.
}
```

`x..A + gap + C..y` is two independent experiments. Do not carry positions,
indicators or outstanding orders across the gap. `common_replay_ranges` intersects
observed ranges across required feeds; it does not prove venue availability or
supply initialization/warmup automatically.

Physical hourly seals within one healthy window remain continuous. Intermediate
managed-book initialization prefixes are skipped. After retention removes the
original baseline, a retained prefix can initialize a new experiment when its
source anchor is valid. Flat L2 rows are grouped by `F_LAST` before reaching the
Native matching engine. Source timestamps and fields are not rewritten.

Reads currently materialize an instrument's windows in memory. Legacy files
without continuity metadata remain Native-readable but are rejected by this
window reader. Inspect quality history and venue coverage before selecting data.

## Bitfinex Spot L3

Build the recorder with `--no-default-features --features venue-bitfinex` and run
`examples/capture.bitfinex-spot-l3.toml`. The example records BTC/USD and ETH/USD
R0 depth 250. The profile records instrument definitions and book deltas. Each
market shares the standard `order_book_deltas/<instrument_id>` catalog layout
with other venues and retains its original order IDs, frame sequence and venue timestamps.

Both book replay readers and `validate-order-book` use `L3_MBO` for this venue.
Configure the backtest venue with `BookType::L3_MBO`. Validation reports the number
of live orders separately from the number of price levels. A source gap ends the
current window and a fresh R0 snapshot starts the next one.

The adapter publishes batches after the next matching Bitfinex checksum.
`ts_init` is the local time when the checksum makes those frames available to
Nautilus. Each instrument retains its original frame order. The checksum covers the best 25 orders per side.
At shutdown the recorded window ends at the latest verified frame. R0 represents
the visible depth, and a removal does not carry an execution or cancellation reason.

## Recovery with earlier received diffs

Binance can publish a REST snapshot followed by buffered diffs whose `ts_init`
precedes that snapshot. Segment recording preserves all source rows in that
publication order. `read_replay_windows::<OrderBookDelta>` is the raw reader;
it does not promise that timestamp sorting is a valid replay.

For a research interval starting after known recovery and required warmup, use:

```rust,ignore
let windows = catalog_capture_core::read_book_replay_windows_in_range(
    root, id, research_start, research_end,
)?;
```

The preceding publication prefix is applied to Native `OrderBook`; one derived
initialization snapshot is emitted at the requested start, followed by unchanged
source frames. A prefix containing timestamps newer than the requested start, or
a remaining non-monotonic suffix, is rejected. Retained checkpoint capture time is
also respected. The loader does not infer recovery completion, restore indicator
history, or establish cross-feed availability. Align trades/depth and strategy
warmup separately. Ordinary healthy-window reads remain unchanged.

The older `validate-order-book` command reads via Native timestamp-sorted catalog
queries. Its framing/final-book checks do not validate this raw recovery order or
certify a window-aware backtest. Use the raw/window APIs and the integration
checks below for that contract. Production recovery recording uses segment mode;
chunked smoke writing retains Native catalog timestamp preconditions.

## Validation

```bash
./scripts/cargo.sh test -p catalog-capture-core --lib replay::tests
./scripts/cargo.sh test -p catalog-capture-runtime-adapter --test replay_windows
```

The integration tests compare callbacks and decisions of the same Rust strategy
through LiveNode and BacktestEngine for L2/trades and full-depth/trades, including
ordinary seals. Gap windows start separate engines. These are injected-data tests,
not proof of real execution fills or global ordering across independent feeds.

See [replay policy](../developer_guide/realtime-adapters.md#faithful-replay-across-file-boundaries)
and [segment lifecycle](../concepts/segment_lifecycle.md#order-book-initialization-and-replay-windows).

An opt-in check can read a completed public BTC/ETH/BNB Futures depth=50 capture
(`@depth@0ms`) with an operator-selected start after recovery and warmup:

```bash
FUTURES_RECOVERY_CATALOG=/absolute/path/to/catalog \
FUTURES_RECOVERY_START_NS=<start-nanoseconds> \
  ./scripts/cargo.sh test --workspace --test replay_windows \
  recorded_futures_recovery_matches_native_backtest_after_explicit_start -- --ignored
```

This check requires an observed recovery time decrease, runs the actual Native
BacktestEngine, and compares its delivered-frame final book with publication-order
raw reconstruction. It sends no orders. It is not a live strategy journal or a
multi-feed latency/availability benchmark.
