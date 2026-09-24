//! Rust-side validation for replaying canonical order-book delta catalogs.

use std::path::Path;

use anyhow::{bail, Context, Result};
use nautilus_model::{
    data::OrderBookDeltas,
    enums::{BookType, RecordFlag},
    identifiers::InstrumentId,
    orderbook::OrderBook,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OrderBookReplayReport {
    pub instrument_id: String,
    pub row_count: usize,
    pub segment_count: usize,
    pub snapshot_segment_count: usize,
    pub snapshot_batch_count: usize,
    pub first_ts_init_ns: u64,
    pub last_ts_init_ns: u64,
    pub final_bid_level_count: usize,
    pub final_ask_level_count: usize,
}

/// Reads canonical order-book deltas, checks snapshot framing and applies the full stream to a
/// Nautilus managed book. Segment mode is self-contained when every sealed file contains a
/// snapshot reset; timestamp-ordered readback may place delayed rows before that reset. Chunked
/// historical catalogs can opt out of that boundary check.
pub fn validate_order_book_replay(
    catalog_root: &Path,
    instrument_id: InstrumentId,
    require_segment_snapshots: bool,
) -> Result<OrderBookReplayReport> {
    let instrument = instrument_id.to_string();
    let mut catalog = ParquetDataCatalog::new(catalog_root, None, None, None, None);
    let instrument_ids = Some(vec![instrument.clone()]);
    let rows = catalog
        .order_book_deltas(instrument_ids.clone(), None, None)
        .with_context(|| format!("failed to read order_book_deltas for {instrument}"))?;
    if rows.is_empty() {
        bail!("no order_book_deltas rows found for {instrument}");
    }

    let mut snapshot_batch_count = 0;
    let mut open_snapshot = false;
    for pair in rows.windows(2) {
        if pair[0].ts_init > pair[1].ts_init {
            bail!("order_book_deltas timestamps are out of order for {instrument}");
        }
    }
    for delta in &rows {
        let is_snapshot = RecordFlag::F_SNAPSHOT.matches(delta.flags);
        let is_last = RecordFlag::F_LAST.matches(delta.flags);
        if is_snapshot && !open_snapshot {
            snapshot_batch_count += 1;
            open_snapshot = true;
        }
        if is_last && open_snapshot {
            open_snapshot = false;
        }
    }
    if open_snapshot {
        bail!("final snapshot batch for {instrument} is missing F_LAST");
    }
    if snapshot_batch_count == 0 {
        bail!("no F_SNAPSHOT batch found for {instrument}");
    }

    let directory = format!("data/order_book_deltas/{instrument}");
    let intervals = catalog
        .get_directory_intervals(&directory)
        .with_context(|| format!("failed to list order-book segments for {instrument}"))?;
    let mut snapshot_segment_count = 0;
    for (start, end) in &intervals {
        let segment_rows = catalog
            .order_book_deltas(
                instrument_ids.clone(),
                Some((*start).into()),
                Some((*end).into()),
            )
            .with_context(|| format!("failed to read segment {start}_{end} for {instrument}"))?;
        segment_rows
            .first()
            .with_context(|| format!("empty order-book segment {start}_{end} for {instrument}"))?;
        let has_snapshot = segment_rows
            .iter()
            .any(|row| RecordFlag::F_SNAPSHOT.matches(row.flags));
        if has_snapshot {
            snapshot_segment_count += 1;
        } else if require_segment_snapshots {
            bail!("order-book segment {start}_{end} for {instrument} contains no F_SNAPSHOT reset");
        }
    }

    let mut book = OrderBook::new(instrument_id, BookType::L2_MBP);
    book.apply_deltas(&OrderBookDeltas::new(instrument_id, rows.clone()))
        .with_context(|| format!("failed to apply order_book_deltas for {instrument}"))?;

    Ok(OrderBookReplayReport {
        instrument_id: instrument,
        row_count: rows.len(),
        segment_count: intervals.len(),
        snapshot_segment_count,
        snapshot_batch_count,
        first_ts_init_ns: rows
            .first()
            .expect("rows checked non-empty")
            .ts_init
            .as_u64(),
        last_ts_init_ns: rows
            .last()
            .expect("rows checked non-empty")
            .ts_init
            .as_u64(),
        final_bid_level_count: book.bids(None).count(),
        final_ask_level_count: book.asks(None).count(),
    })
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::{stubs::stub_deltas, BookOrder, OrderBookDelta, OrderBookDeltas},
        enums::{BookAction, BookType, OrderSide, RecordFlag},
        identifiers::InstrumentId,
        orderbook::OrderBook,
        types::{Price, Quantity},
    };

    use super::validate_order_book_replay;
    use crate::{
        config::CaptureConfig,
        item::PartitionKey,
        lifecycle::{LifecycleConfig, LifecycleMode},
        sink::{CaptureSink, NautilusCatalogSink},
    };

    fn temp_catalog() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "order-book-replay-validation-{}",
            nautilus_core::UUID4::new()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("temp catalog");
        dir
    }

    #[test]
    fn validates_snapshot_framing_and_rebuilds_book() {
        let root = temp_catalog();
        let config = CaptureConfig {
            catalog_uri: format!("file://{}", root.display()),
            lifecycle: LifecycleConfig {
                mode: LifecycleMode::Chunked,
                ..LifecycleConfig::default()
            },
            ..CaptureConfig::default()
        };
        let mut grouped = stub_deltas();
        grouped.deltas.last_mut().expect("snapshot rows").flags |= RecordFlag::F_LAST as u8;
        let expected_rows = grouped.deltas.len();
        let instrument_id: InstrumentId = grouped.instrument_id;
        let partition = PartitionKey::catalog_data::<OrderBookDelta>(instrument_id).stable_key();
        let mut sink = NautilusCatalogSink::from_config(&config).expect("sink");
        sink.write_batch(&partition, grouped.deltas)
            .expect("write snapshot");

        let report = validate_order_book_replay(&root, instrument_id, true).expect("valid replay");
        assert_eq!(report.row_count, expected_rows);
        assert_eq!(report.segment_count, 1);
        assert_eq!(report.snapshot_segment_count, 1);
        assert_eq!(report.snapshot_batch_count, 1);
        assert!(report.final_bid_level_count > 0);
        assert!(report.final_ask_level_count > 0);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn inserting_managed_snapshot_preserves_final_book_state() {
        let instrument_id = InstrumentId::from("BTC-PERP.TEST");
        let initial = OrderBookDeltas::new(
            instrument_id,
            vec![
                OrderBookDelta::new(
                    instrument_id,
                    BookAction::Add,
                    BookOrder::new(OrderSide::Buy, Price::from("100"), Quantity::from("2"), 1),
                    0,
                    1,
                    UnixNanos::from(1),
                    UnixNanos::from(1),
                ),
                OrderBookDelta::new(
                    instrument_id,
                    BookAction::Add,
                    BookOrder::new(OrderSide::Sell, Price::from("101"), Quantity::from("3"), 2),
                    0,
                    2,
                    UnixNanos::from(2),
                    UnixNanos::from(2),
                ),
            ],
        );
        let continuation = OrderBookDeltas::new(
            instrument_id,
            vec![OrderBookDelta::new(
                instrument_id,
                BookAction::Add,
                BookOrder::new(OrderSide::Buy, Price::from("99"), Quantity::from("1"), 3),
                RecordFlag::F_LAST as u8,
                3,
                UnixNanos::from(3),
                UnixNanos::from(3),
            )],
        );

        let mut without_snapshot = OrderBook::new(instrument_id, BookType::L2_MBP);
        without_snapshot
            .apply_deltas(&initial)
            .expect("initial book");
        let snapshot = without_snapshot.to_deltas(2.into(), 2.into());
        without_snapshot
            .apply_deltas(&continuation)
            .expect("continuation");

        let mut with_snapshot = OrderBook::new(instrument_id, BookType::L2_MBP);
        with_snapshot.apply_deltas(&initial).expect("initial book");
        with_snapshot
            .apply_deltas(&snapshot)
            .expect("managed snapshot");
        with_snapshot
            .apply_deltas(&continuation)
            .expect("continuation");

        assert_eq!(
            without_snapshot.bids_as_map(None),
            with_snapshot.bids_as_map(None)
        );
        assert_eq!(
            without_snapshot.asks_as_map(None),
            with_snapshot.asks_as_map(None)
        );
        assert_eq!(without_snapshot.sequence, with_snapshot.sequence);
        assert_eq!(without_snapshot.ts_last, with_snapshot.ts_last);
    }
}
