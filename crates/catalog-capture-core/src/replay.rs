// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Replay windows retain Native models and Parquet codecs. A gap starts a new
//! experiment; physical file rotation alone does not reset a strategy.

use std::{collections::HashMap, fs::File, path::Path};

use anyhow::{Context, Result, bail};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{HasTsInit, OrderBookDelta, OrderBookDeltas},
    enums::RecordFlag,
    identifiers::InstrumentId,
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, catalog::types::HasCatalogDataType,
};
use nautilus_serialization::arrow::DecodeTypedFromRecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub const WINDOW_ID_KEY: &str = "capture_window_id";
pub const INITIALIZATION_ROWS_KEY: &str = "capture_initialization_rows";
pub const SEGMENT_INDEX_KEY: &str = "capture_segment_index";
pub const CONTRACT_KEY: &str = "capture_contract";
pub const CONTRACT_VERSION: &str = "native_frames_v1";

#[derive(Debug, Clone)]
pub struct ReplayWindow<T> {
    pub id: String,
    pub start: UnixNanos,
    pub end: UnixNanos,
    pub data: Vec<T>,
    /// Local bootstrap rows, kept separate from observed source events.
    pub initialization: Vec<T>,
}

/// Read only sealed local files, in physical row order, using Native discovery
/// and decoders. SQL timestamp sorting must not reorder same-timestamp frames.
/// Legacy files without window metadata cannot be certified by this reader.
pub fn read_replay_windows<T>(root: &Path, instrument: InstrumentId) -> Result<Vec<ReplayWindow<T>>>
where
    T: DecodeTypedFromRecordBatch + HasCatalogDataType + HasTsInit,
{
    let catalog = ParquetDataCatalog::new(root, None, None, None, None);
    let files = catalog.query_files(
        &T::catalog_data_type().into(),
        Some(vec![instrument.to_string()]),
        None,
        None,
    )?;
    let mut segments = Vec::new();
    for path in files {
        let path = Path::new(&path);
        let path = if path.is_absolute() {
            path.to_owned()
        } else {
            root.join(path)
        };
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
        let metadata = builder.schema().metadata().clone();
        if metadata.get(CONTRACT_KEY).map(String::as_str) != Some(CONTRACT_VERSION) {
            bail!(
                "uncertified replay file {}: missing supported continuity contract",
                path.display()
            );
        }
        let id = metadata
            .get(WINDOW_ID_KEY)
            .context("missing capture_window_id")?
            .clone();
        let ordinal: u64 = metadata
            .get(SEGMENT_INDEX_KEY)
            .context("missing segment ordinal")?
            .parse()?;
        segments.push((id, ordinal, path));
    }
    segments.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
    let mut windows: Vec<ReplayWindow<T>> = Vec::new();
    let mut indices: HashMap<String, (usize, u64)> = HashMap::new();
    for (id, ordinal, path) in segments {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
        let metadata = builder.schema().metadata().clone();
        let initialization_rows: usize = metadata
            .get(INITIALIZATION_ROWS_KEY)
            .map_or(Ok(0), |n| n.parse())?;
        let (index, fresh) = match indices.get(&id) {
            Some(&(index, last)) if last.checked_add(1) == Some(ordinal) => (index, false),
            Some(&(_, last)) if ordinal <= last => {
                bail!("duplicate replay segment ordinal in window {id}")
            }
            _ => {
                windows.push(ReplayWindow {
                    id: format!("{id}:{ordinal}"),
                    start: UnixNanos::default(),
                    end: UnixNanos::default(),
                    data: Vec::new(),
                    initialization: Vec::new(),
                });
                (windows.len() - 1, true)
            }
        };
        indices.insert(id, (index, ordinal));
        let window = &mut windows[index];
        let mut row_index = 0;
        for batch in builder.build()? {
            for item in T::decode_typed_batch(&metadata, batch?)? {
                if row_index < initialization_rows {
                    if fresh {
                        window.initialization.push(item);
                    }
                } else {
                    let ts = item.ts_init();
                    if window.data.is_empty() {
                        window.start = ts;
                    } else if ts < window.end {
                        bail!("arrival clock rollback inside replay window {}", window.id);
                    }
                    window.end = ts;
                    window.data.push(item);
                }
                row_index += 1;
            }
        }
        if row_index < initialization_rows {
            bail!("incomplete initialization prefix");
        }
    }
    windows.retain(|window| !window.data.is_empty());
    windows.sort_by_key(|window| window.start);
    Ok(windows)
}

/// Restore venue message boundaries before feeding BacktestEngine. Each returned
/// window needs a separate engine/strategy instance; never concatenate windows.
pub fn read_book_replay_windows(
    root: &Path,
    instrument: InstrumentId,
) -> Result<Vec<ReplayWindow<OrderBookDeltas>>> {
    read_replay_windows::<OrderBookDelta>(root, instrument)?
        .into_iter()
        .map(|window| {
            let mut data = group_book_deltas(instrument, window.data)?;
            let mut initialization = group_book_deltas(instrument, window.initialization)?;
            let source_baseline = data.first().is_some_and(|frame| {
                frame
                    .deltas
                    .iter()
                    .any(|row| RecordFlag::F_SNAPSHOT.matches(row.flags))
            });
            if !source_baseline && initialization.len() == 1 {
                let mut baseline = initialization.remove(0);
                let anchor = data.iter().rposition(|frame| {
                    frame.sequence == baseline.sequence && frame.ts_event == baseline.ts_event
                });
                let covered = anchor.map(|index| index + 1).or_else(|| {
                    (baseline.sequence > 0
                        && data
                            .first()
                            .is_some_and(|frame| frame.sequence > baseline.sequence))
                    .then_some(0)
                });
                if let Some(covered) = covered {
                    let ts = anchor
                        .map(|index| data[index].ts_init)
                        .unwrap_or(baseline.ts_init.min(data[0].ts_init));
                    // Only a synthetic initialization timestamp is adjusted. Every
                    // observed source field remains exactly as recorded.
                    for row in &mut baseline.deltas {
                        row.ts_init = ts;
                    }
                    baseline = OrderBookDeltas::new(instrument, baseline.deltas);
                    data.drain(..covered);
                    data.insert(0, baseline);
                }
            }
            let start = data.first().map_or(window.start, |frame| frame.ts_init);
            Ok(ReplayWindow {
                id: window.id,
                start,
                end: window.end,
                data,
                initialization: Vec::new(),
            })
        })
        .collect()
}

/// Group without sorting, renumbering, timestamp edits or inferred sequence gaps.
pub fn group_book_deltas(
    instrument: InstrumentId,
    rows: Vec<OrderBookDelta>,
) -> Result<Vec<OrderBookDeltas>> {
    let mut frames = Vec::new();
    let mut pending: Vec<OrderBookDelta> = Vec::new();
    for row in rows {
        if row.instrument_id != instrument {
            bail!("mixed instrument in delta frame for {instrument}");
        }
        if let Some(first) = pending.first()
            && (first.ts_init != row.ts_init || first.ts_event != row.ts_event)
        {
            bail!("delta frame changed timestamp before F_LAST for {instrument}");
        }
        let last = RecordFlag::F_LAST.matches(row.flags);
        pending.push(row);
        if last {
            frames.push(OrderBookDeltas::new(
                instrument,
                std::mem::take(&mut pending),
            ));
        }
    }
    if !pending.is_empty() {
        bail!("incomplete delta frame missing F_LAST for {instrument}");
    }
    Ok(frames)
}

impl ReplayWindow<OrderBookDeltas> {
    /// A partial initial window is inspectable but cannot initialize a fresh L2 book.
    #[must_use]
    pub fn has_book_baseline(&self) -> bool {
        self.data.first().is_some_and(|frame| {
            frame
                .deltas
                .iter()
                .any(|row| RecordFlag::F_SNAPSHOT.matches(row.flags))
        })
    }
}

/// Conservative intersection of observed row ranges for all strategy inputs.
/// Baseline history and strategy warmup must still be supplied for each interval.
pub fn common_replay_ranges(feeds: &[Vec<(UnixNanos, UnixNanos)>]) -> Vec<(UnixNanos, UnixNanos)> {
    let Some(first) = feeds.first() else {
        return Vec::new();
    };
    let mut ranges = first.clone();
    for feed in &feeds[1..] {
        ranges = ranges
            .into_iter()
            .flat_map(|(a, b)| {
                feed.iter().filter_map(move |&(c, d)| {
                    let start = a.max(c);
                    let end = b.min(d);
                    (start <= end).then_some((start, end))
                })
            })
            .collect();
    }
    ranges.sort_unstable();
    ranges.dedup();
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BackgroundCaptureRuntime, CaptureConfig, CaptureItem, OverflowPolicy, PartitionKey,
        runtime::FlushResult,
        sink::{BookDeltaCatalogSink, CaptureSink, CatalogSink},
    };
    use nautilus_model::{
        data::{BookOrder, TradeTick},
        enums::{AggressorSide, BookAction, BookType, OrderSide},
        identifiers::TradeId,
        orderbook::OrderBook,
        types::{Price, Quantity},
    };
    use std::{fs, sync::mpsc, time::Duration};

    struct Paused<S> {
        inner: S,
        entered: Option<mpsc::Sender<()>>,
        release: mpsc::Receiver<()>,
    }
    impl<T, S: CaptureSink<T>> CaptureSink<T> for Paused<S> {
        fn write_batch(&mut self, key: &str, rows: Vec<T>) -> Result<Vec<std::path::PathBuf>> {
            if let Some(entered) = self.entered.take() {
                entered.send(()).unwrap();
                self.release.recv().unwrap();
            }
            self.inner.write_batch(key, rows)
        }
        fn write_initialization(&mut self, key: &str, data: T) -> Result<FlushResult> {
            self.inner.write_initialization(key, data)
        }
        fn seal_partition(&mut self, key: &str) -> Result<FlushResult> {
            self.inner.seal_partition(key)
        }
        fn seal_all(&mut self) -> Result<FlushResult> {
            self.inner.seal_all()
        }
        fn seal_all_for_shutdown(&mut self) -> Result<FlushResult> {
            self.inner.seal_all_for_shutdown()
        }
        fn break_continuity(&mut self, key: Option<&str>) -> Result<FlushResult> {
            self.inner.break_continuity(key)
        }
        fn is_segment_mode(&self) -> bool {
            true
        }
    }

    fn root() -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("replay-window-{}", nautilus_core::UUID4::new()));
        fs::create_dir_all(&path).unwrap();
        path
    }
    fn snapshot(id: InstrumentId, ts: u64, size: &str) -> OrderBookDeltas {
        let mut book = OrderBook::new(id, BookType::L2_MBP);
        book.add(
            BookOrder::new(OrderSide::Buy, Price::from("100"), Quantity::from(size), 0),
            0,
            1,
            ts.into(),
        );
        book.add(
            BookOrder::new(OrderSide::Sell, Price::from("101"), Quantity::from("7"), 0),
            0,
            1,
            ts.into(),
        );
        book.to_deltas(ts.into(), ts.into())
    }
    fn update(id: InstrumentId, ts: u64, size: &str) -> OrderBookDeltas {
        OrderBookDeltas::new(
            id,
            vec![OrderBookDelta::new(
                id,
                BookAction::Update,
                BookOrder::new(OrderSide::Buy, Price::from("100"), Quantity::from(size), 0),
                RecordFlag::F_LAST as u8,
                ts,
                ts.into(),
                ts.into(),
            )],
        )
    }
    fn item(data: OrderBookDeltas, bytes: usize) -> CaptureItem<OrderBookDeltas> {
        CaptureItem {
            partition_key: PartitionKey::catalog_data::<OrderBookDelta>(data.instrument_id),
            event_ts_ns: data.ts_event.as_u64(),
            init_ts_ns: Some(data.ts_init.as_u64()),
            row_count: data.deltas.len(),
            estimated_bytes: bytes,
            payload: data,
        }
    }

    #[test]
    fn normal_seal_keeps_one_window_and_preserves_equal_timestamp_frames() {
        let root = root();
        let id = InstrumentId::from("BTCUSDT.BINANCE");
        let key = PartitionKey::catalog_data::<OrderBookDelta>(id).stable_key();
        let config = CaptureConfig {
            catalog_uri: format!("file://{}", root.display()),
            ..Default::default()
        };
        let mut sink = BookDeltaCatalogSink::from_config(&config).unwrap();
        let source = vec![
            snapshot(id, 100, "5"),
            update(id, 110, "6"),
            update(id, 110, "7"),
            update(id, 120, "8"),
        ];
        sink.write_batch(&key, source[..2].to_vec()).unwrap();
        sink.seal_all().unwrap();
        sink.write_batch(&key, source[2..].to_vec()).unwrap();
        sink.seal_all_for_shutdown().unwrap();
        let windows = read_book_replay_windows(&root, id).unwrap();
        assert_eq!(windows.len(), 1);
        assert!(windows[0].has_book_baseline());
        assert_eq!(windows[0].data, source);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn hourly_initialization_is_skipped_in_full_replay_and_used_after_retention() {
        let root = root();
        let id = InstrumentId::from("BTCUSDT.BINANCE");
        let key = PartitionKey::catalog_data::<OrderBookDelta>(id).stable_key();
        let config = CaptureConfig {
            catalog_uri: format!("file://{}", root.display()),
            ..Default::default()
        };
        let initial = snapshot(id, 100, "5");
        let a = update(id, 110, "6");
        let c = update(id, 120, "9");
        let mut book = OrderBook::new(id, BookType::L2_MBP);
        book.apply_deltas(&initial).unwrap();
        book.apply_deltas(&a).unwrap();
        book.apply_deltas(&c).unwrap();
        // Managed cache can be ahead of the first queued post-seal source frame.
        let checkpoint = book.to_deltas(120.into(), 125.into());
        let y = update(id, 130, "10");
        let mut sink = BookDeltaCatalogSink::from_config(&config).unwrap();
        sink.write_batch(&key, vec![initial.clone(), a.clone()])
            .unwrap();
        sink.seal_all().unwrap();
        sink.write_initialization(&key, checkpoint).unwrap();
        sink.write_batch(&key, vec![c.clone(), y.clone()]).unwrap();
        sink.seal_all_for_shutdown().unwrap();
        let all = read_book_replay_windows(&root, id).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].data, vec![initial, a, c, y.clone()]);
        let catalog = ParquetDataCatalog::new(&root, None, None, None, None);
        let files = catalog
            .query_files(
                &nautilus_model::data::NautilusDataType::OrderBookDelta.into(),
                Some(vec![id.to_string()]),
                None,
                None,
            )
            .unwrap();
        let path = Path::new(&files[0]);
        fs::remove_file(if path.is_absolute() {
            path.to_owned()
        } else {
            root.join(path)
        })
        .unwrap();
        let retained = read_book_replay_windows(&root, id).unwrap();
        assert_eq!(retained.len(), 1);
        assert!(retained[0].has_book_baseline());
        assert_eq!(retained[0].data.len(), 2);
        assert_eq!(retained[0].data[0].ts_init.as_u64(), 120);
        assert_eq!(retained[0].data[1], y);
        let mut replay = OrderBook::new(id, BookType::L2_MBP);
        for frame in &retained[0].data {
            replay.apply_deltas(frame).unwrap();
        }
        assert_eq!(
            replay
                .bids_as_map(None)
                .values()
                .next()
                .unwrap()
                .to_string(),
            "10"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn missing_middle_file_splits_a_window_and_cannot_silently_bridge_history() {
        let root = root();
        let id = InstrumentId::from("BTCUSDT.BINANCE");
        let key = PartitionKey::catalog_data::<OrderBookDelta>(id).stable_key();
        let config = CaptureConfig {
            catalog_uri: format!("file://{}", root.display()),
            ..Default::default()
        };
        let mut sink = BookDeltaCatalogSink::from_config(&config).unwrap();
        sink.write_batch(&key, vec![snapshot(id, 100, "5")])
            .unwrap();
        sink.seal_all().unwrap();
        sink.write_batch(&key, vec![update(id, 120, "8")]).unwrap();
        sink.seal_all().unwrap();
        sink.write_initialization(&key, snapshot(id, 120, "8"))
            .unwrap();
        sink.write_batch(&key, vec![update(id, 140, "9")]).unwrap();
        sink.seal_all_for_shutdown().unwrap();
        let catalog = ParquetDataCatalog::new(&root, None, None, None, None);
        let files = catalog
            .query_files(
                &nautilus_model::data::NautilusDataType::OrderBookDelta.into(),
                Some(vec![id.to_string()]),
                None,
                None,
            )
            .unwrap();
        for file in files {
            let path = Path::new(&file);
            let path = if path.is_absolute() {
                path.to_owned()
            } else {
                root.join(path)
            };
            let reader =
                ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
            if reader
                .schema()
                .metadata()
                .get(SEGMENT_INDEX_KEY)
                .map(String::as_str)
                == Some("1")
            {
                drop(reader);
                fs::remove_file(path).unwrap();
            }
        }
        let windows = read_book_replay_windows(&root, id).unwrap();
        assert_eq!(windows.len(), 2);
        assert!(windows.iter().all(ReplayWindow::has_book_baseline));
        assert_eq!(
            (windows[0].start.as_u64(), windows[0].end.as_u64()),
            (100, 100)
        );
        assert_eq!(windows[1].end.as_u64(), 140);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn gap_produces_two_independent_books_and_equal_file_ranges_do_not_overwrite() {
        let root = root();
        let id = InstrumentId::from("BTCUSDT.BINANCE");
        let key = PartitionKey::catalog_data::<OrderBookDelta>(id).stable_key();
        let config = CaptureConfig {
            catalog_uri: format!("file://{}", root.display()),
            ..Default::default()
        };
        let mut sink = BookDeltaCatalogSink::from_config(&config).unwrap();
        sink.write_batch(&key, vec![snapshot(id, 100, "5")])
            .unwrap();
        sink.break_continuity(Some(&key)).unwrap();
        sink.write_batch(&key, vec![snapshot(id, 100, "9")])
            .unwrap();
        sink.seal_all_for_shutdown().unwrap();
        let windows = read_book_replay_windows(&root, id).unwrap();
        assert_eq!(windows.len(), 2);
        assert_ne!(windows[0].id, windows[1].id);
        let mut sizes = Vec::new();
        for window in windows {
            assert!(window.has_book_baseline());
            let mut book = OrderBook::new(id, BookType::L2_MBP);
            for frame in window.data {
                book.apply_deltas(&frame).unwrap();
            }
            sizes.push(book.bids_as_map(None).values().next().unwrap().to_string());
        }
        sizes.sort();
        assert_eq!(sizes, vec!["5", "9"]);
        let mut catalog = ParquetDataCatalog::new(&root, None, None, None, None);
        assert_eq!(
            catalog
                .order_book_deltas(Some(vec![id.to_string()]), None, None)
                .unwrap()
                .len(),
            6
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejected_message_cuts_the_fifo_even_when_the_data_queue_is_full() {
        let root = root();
        let id = InstrumentId::from("BTCUSDT.BINANCE");
        let mut config = CaptureConfig {
            catalog_uri: format!("file://{}", root.display()),
            queue_capacity: 1,
            queue_max_bytes: Some(64),
            overflow_policy: OverflowPolicy::FailFast,
            ..Default::default()
        };
        config.lifecycle.segment.row_group_rows = 1;
        let (entered, waiting) = mpsc::channel();
        let (release, proceed) = mpsc::channel();
        let mut runtime = BackgroundCaptureRuntime::new(
            config.clone(),
            Paused {
                inner: BookDeltaCatalogSink::from_config(&config).unwrap(),
                entered: Some(entered),
                release: proceed,
            },
        )
        .unwrap();
        let x = snapshot(id, 100, "5");
        let a = update(id, 110, "6");
        let c = snapshot(id, 130, "9");
        let y = update(id, 140, "10");
        runtime.submit(item(x.clone(), 64)).unwrap();
        waiting.recv_timeout(Duration::from_secs(2)).unwrap();
        let before = runtime.submit(item(a.clone(), 64));
        let rejected = runtime.submit(item(update(id, 120, "8"), 64));
        release.send(()).unwrap();
        before.unwrap();
        assert!(rejected.is_err());
        runtime.flush_all().unwrap();
        runtime
            .submit_after_partition_seal(item(c.clone(), 64))
            .unwrap();
        runtime.flush_all().unwrap();
        runtime.submit(item(y.clone(), 64)).unwrap();
        assert!(runtime.shutdown().is_err());
        let windows = read_book_replay_windows(&root, id).unwrap();
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].data, vec![x, a]);
        assert_eq!(windows[1].data, vec![c, y]);
        assert!(windows.iter().all(ReplayWindow::has_book_baseline));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn repeated_rejections_with_admitted_trades_between_them_create_separate_windows() {
        let root = root();
        let id = InstrumentId::from("BTCUSDT.BINANCE");
        let config = CaptureConfig {
            catalog_uri: format!("file://{}", root.display()),
            queue_capacity: 4,
            queue_max_bytes: Some(256),
            flush_rows: 1,
            overflow_policy: OverflowPolicy::FailFast,
            ..Default::default()
        };
        let (entered, waiting) = mpsc::channel();
        let (release, proceed) = mpsc::channel();
        let mut runtime = BackgroundCaptureRuntime::new(
            config.clone(),
            Paused {
                inner: CatalogSink::<TradeTick>::from_config(&config).unwrap(),
                entered: Some(entered),
                release: proceed,
            },
        )
        .unwrap();
        let trade = |ts: u64, bytes: usize| CaptureItem {
            partition_key: PartitionKey::catalog_data::<TradeTick>(id),
            event_ts_ns: ts,
            init_ts_ns: Some(ts),
            row_count: 1,
            estimated_bytes: bytes,
            payload: TradeTick::new(
                id,
                Price::from("100"),
                Quantity::from("1"),
                AggressorSide::Buy,
                TradeId::from(ts.to_string().as_str()),
                ts.into(),
                ts.into(),
            ),
        };
        runtime.submit(trade(100, 64)).unwrap();
        waiting.recv_timeout(Duration::from_secs(2)).unwrap();
        let a = runtime.submit(trade(110, 64));
        let gap_b = runtime.submit(trade(120, 257));
        let c = runtime.submit(trade(130, 64));
        let gap_d = runtime.submit(trade(140, 257));
        let y = runtime.submit(trade(150, 64));
        // Repeated losses without an intervening admission coalesce safely.
        let gap_z1 = runtime.submit(trade(160, 257));
        let gap_z2 = runtime.submit(trade(170, 257));
        release.send(()).unwrap();
        a.unwrap();
        c.unwrap();
        y.unwrap();
        assert!(gap_b.is_err() && gap_d.is_err() && gap_z1.is_err() && gap_z2.is_err());
        assert!(runtime.shutdown().is_err());
        let windows = read_replay_windows::<TradeTick>(&root, id).unwrap();
        let times: Vec<Vec<u64>> = windows
            .iter()
            .map(|window| window.data.iter().map(|row| row.ts_init.as_u64()).collect())
            .collect();
        assert_eq!(times, vec![vec![100, 110], vec![130], vec![150]]);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn incomplete_or_mixed_timestamp_book_frames_are_rejected_before_backtest() {
        let id = InstrumentId::from("BTCUSDT.BINANCE");
        let mut rows = snapshot(id, 100, "5").deltas;
        rows.last_mut().unwrap().flags &= !(RecordFlag::F_LAST as u8);
        assert!(
            group_book_deltas(id, rows.clone())
                .unwrap_err()
                .to_string()
                .contains("F_LAST")
        );
        rows.last_mut().unwrap().ts_init = 101.into();
        assert!(
            group_book_deltas(id, rows)
                .unwrap_err()
                .to_string()
                .contains("timestamp")
        );
    }

    #[test]
    fn trade_windows_split_without_a_book_snapshot_and_intersections_do_not_bridge_a_gap() {
        let root = root();
        let id = InstrumentId::from("BTCUSDT.BINANCE");
        let config = CaptureConfig {
            catalog_uri: format!("file://{}", root.display()),
            queue_capacity: 1,
            queue_max_bytes: Some(64),
            flush_rows: 1,
            overflow_policy: OverflowPolicy::FailFast,
            ..Default::default()
        };
        let (entered, waiting) = mpsc::channel();
        let (release, proceed) = mpsc::channel();
        let mut runtime = BackgroundCaptureRuntime::new(
            config.clone(),
            Paused {
                inner: CatalogSink::<TradeTick>::from_config(&config).unwrap(),
                entered: Some(entered),
                release: proceed,
            },
        )
        .unwrap();
        let trade = |ts: u64| {
            TradeTick::new(
                id,
                Price::from("100"),
                Quantity::from("1"),
                AggressorSide::Buy,
                TradeId::from(ts.to_string().as_str()),
                ts.into(),
                ts.into(),
            )
        };
        let item = |ts| CaptureItem {
            partition_key: PartitionKey::catalog_data::<TradeTick>(id),
            event_ts_ns: ts,
            init_ts_ns: Some(ts),
            row_count: 1,
            estimated_bytes: 64,
            payload: trade(ts),
        };
        runtime.submit(item(100)).unwrap();
        waiting.recv_timeout(Duration::from_secs(2)).unwrap();
        let a = runtime.submit(item(110));
        let rejected = runtime.submit(item(120));
        release.send(()).unwrap();
        a.unwrap();
        assert!(rejected.is_err());
        runtime.flush_all().unwrap();
        runtime.submit(item(130)).unwrap();
        runtime.flush_all().unwrap();
        runtime.submit(item(140)).unwrap();
        assert!(runtime.shutdown().is_err());
        let windows = read_replay_windows::<TradeTick>(&root, id).unwrap();
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].data, vec![trade(100), trade(110)]);
        assert_eq!(windows[1].data, vec![trade(130), trade(140)]);
        assert_eq!(
            common_replay_ranges(&[
                windows.iter().map(|w| (w.start, w.end)).collect(),
                vec![(90.into(), 150.into())]
            ]),
            vec![(100.into(), 110.into()), (130.into(), 140.into())]
        );
        fs::remove_dir_all(root).unwrap();
    }
}
