// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Opt-in offline validation of all families from a completed combined BTC capture.

use std::{collections::HashSet, io::Write, path::Path};

use anyhow::Context;
use nautilus_bullet::funding::{
    BulletFundingEstimate, ESTIMATE_TYPE, estimate_data_type, register_bullet_custom_data,
};
use nautilus_model::{
    data::{Data, FundingRateUpdate, IndexPriceUpdate, MarkPriceUpdate, OrderBookDelta, TradeTick},
    enums::{BookAction, BookType, RecordFlag},
    identifiers::InstrumentId,
    instruments::Instrument,
    orderbook::{OrderBook, analysis::book_check_integrity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_serialization::arrow::DecodeTypedFromRecordBatch;
use parquet::{
    arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    file::reader::{FileReader, SerializedFileReader},
};
use serde_json::{Value, json};

#[test]
#[ignore = "requires BULLET_SOAK_CATALOG; optional BULLET_SOAK_SETTLEMENT_NS and recovery epoch evidence"]
fn bullet_btc_combined_catalog_readback() -> anyhow::Result<()> {
    let path = std::env::var("BULLET_SOAK_CATALOG")?;
    let root = Path::new(&path);
    let boundary = std::env::var("BULLET_SOAK_SETTLEMENT_NS")
        .ok()
        .map(|s| s.parse::<u64>())
        .transpose()?;
    let report = readback(root, boundary)?;
    std::fs::write(
        root.join("metadata/soak_native_readback.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn readback(root: &Path, boundary: Option<u64>) -> anyhow::Result<Value> {
    let id = InstrumentId::from("BTC-USD-PERP.BULLET");
    register_bullet_custom_data();
    let mut catalog = ParquetDataCatalog::new(root, None, None, None, None);
    let instruments = catalog.instruments(None, None, None)?;
    anyhow::ensure!(
        instruments.len() == 1 && instruments[0].id() == id,
        "wrong instrument set"
    );
    let instrument = &instruments[0];
    anyhow::ensure!(
        instrument.raw_symbol().as_str() == "BTC-USD"
            && instrument.price_precision() == 8
            && instrument.size_precision() == 4,
        "BTC metadata mismatch"
    );
    let info = instrument.info().context("missing recorded metadata")?;
    let contract = info
        .get("funding_rate_contract")
        .context("missing funding basis")?;
    anyhow::ensure!(
        contract["kind"] == "settled_gross"
            && contract["rate_basis_minutes"] == 60
            && contract["source_rate_basis_minutes"] == 480
            && contract["funding_spread_applied"] == false,
        "incorrect funding contract"
    );

    let physical_segments = physical_book_segments(root, id)?;
    let deltas = catalog.query::<OrderBookDelta>(
        Some(vec![id.to_string()]),
        None,
        None,
        None,
        None,
        true,
    )?;
    let trades =
        catalog.query::<TradeTick>(Some(vec![id.to_string()]), None, None, None, None, true)?;
    let marks = catalog.query::<MarkPriceUpdate>(
        Some(vec![id.to_string()]),
        None,
        None,
        None,
        None,
        true,
    )?;
    let indices = catalog.query::<IndexPriceUpdate>(
        Some(vec![id.to_string()]),
        None,
        None,
        None,
        None,
        true,
    )?;
    let settled = catalog.query::<FundingRateUpdate>(
        Some(vec![id.to_string()]),
        None,
        None,
        None,
        None,
        true,
    )?;
    anyhow::ensure!(
        !deltas.is_empty()
            && !trades.is_empty()
            && !marks.is_empty()
            && !indices.is_empty()
            && !settled.is_empty(),
        "empty required family"
    );

    let windows = catalog_capture_core::read_book_replay_windows(root, id)?;
    anyhow::ensure!(!windows.is_empty(), "no Native replay windows");
    let mut batches = 0;
    let mut invalidations = 0;
    let mut invalidation_points = Vec::new();
    let mut snapshot_points = Vec::new();
    let mut previous_sequence = 0;
    let mut final_book = OrderBook::new(id, BookType::L2_MBP);
    for window in &windows {
        anyhow::ensure!(
            window.has_book_baseline(),
            "unready replay window {}",
            window.id
        );
        let mut book = OrderBook::new(id, BookType::L2_MBP);
        previous_sequence = 0;
        for frame in &window.data {
            let first = frame.deltas.first().context("empty source frame")?;
            anyhow::ensure!(
                first.action == BookAction::Clear || frame.sequence > previous_sequence,
                "source sequence did not advance"
            );
            for row in &frame.deltas {
                anyhow::ensure!(
                    row.instrument_id == id && row.ts_event.as_u64().is_multiple_of(1000),
                    "book identity/time mismatch"
                );
                if row.action != BookAction::Clear {
                    anyhow::ensure!(
                        row.order.price.precision == 8 && row.order.size.precision == 4,
                        "book precision mismatch"
                    );
                }
            }
            book.apply_deltas(frame)?;
            book_check_integrity(&book)?;
            anyhow::ensure!(
                book.sequence == frame.sequence,
                "Native cache sequence differs from source u"
            );
            anyhow::ensure!(
                book.bids(None).count() <= 20 && book.asks(None).count() <= 20,
                "top-20 depth exceeded"
            );
            if first.action == BookAction::Clear && RecordFlag::F_SNAPSHOT.matches(first.flags) {
                snapshot_points.push(json!({"ts_init_ns":frame.ts_init.as_u64(),"ts_event_ns":frame.ts_event.as_u64(),"sequence":frame.sequence}));
            } else if first.action == BookAction::Clear {
                anyhow::ensure!(
                    frame.deltas.len() == 1
                        && first.sequence == 0
                        && first.flags == RecordFlag::F_LAST as u8,
                    "invalid recorded recovery control"
                );
                invalidations += 1;
                invalidation_points.push(json!({"ts_init_ns":frame.ts_init.as_u64(),"ts_event_ns":frame.ts_event.as_u64(),"sequence":frame.sequence,"flags":frame.flags}));
            }
            previous_sequence = frame.sequence;
            batches += 1;
        }
        final_book = book;
    }
    let checkpoints_checked = validate_book_checkpoints(root, id, &physical_segments)?;
    let book_report = json!({"row_count":deltas.len(),"segment_count":physical_segments.len(),"source_replay_windows":windows.len(),"source_frames":batches-invalidations,"recorded_invalidation_frames":invalidations,"invalidation_points":invalidation_points,"checkpoints_checked":checkpoints_checked,"first_ts_init_ns":deltas.first().unwrap().ts_init.as_u64(),"last_ts_init_ns":deltas.last().unwrap().ts_init.as_u64(),"final_bid_level_count":final_book.bids(None).count(),"final_ask_level_count":final_book.asks(None).count()});
    let mut trade_ids = HashSet::new();
    for row in &trades {
        anyhow::ensure!(
            row.instrument_id == id
                && row.ts_event.as_u64().is_multiple_of(1000)
                && row.price.precision == 8
                && row.size.precision == 4,
            "trade identity/time/precision mismatch"
        );
        anyhow::ensure!(
            trade_ids.insert(row.trade_id.to_string()),
            "duplicate canonical trade ID"
        );
    }
    anyhow::ensure!(
        marks.len() == indices.len(),
        "shared price owners have different row counts"
    );
    for (mark, index) in marks.iter().zip(&indices) {
        anyhow::ensure!(
            mark.instrument_id == id
                && index.instrument_id == id
                && mark.ts_event == index.ts_event
                && mark.ts_init == index.ts_init
                && mark.ts_event.as_u64().is_multiple_of(1000)
                && mark.value.precision == 8
                && index.value.precision == 8,
            "mark/index identity/time/precision mismatch"
        );
    }
    let mut settlements = HashSet::new();
    for row in &settled {
        anyhow::ensure!(
            row.instrument_id == id && row.interval == Some(60) && row.next_funding_ns.is_none(),
            "settlement contract mismatch"
        );
        anyhow::ensure!(
            settlements.insert(row.ts_event.as_u64()),
            "duplicate settlement event"
        );
    }
    let custom = catalog.query_custom_data_dynamic(
        ESTIMATE_TYPE,
        Some(&[id.to_string()]),
        None,
        None,
        None,
        None,
        true,
    )?;
    let mut estimates = Vec::new();
    for row in &custom {
        let Data::Custom(row) = row else {
            anyhow::bail!("unexpected estimate family")
        };
        anyhow::ensure!(
            row.data_type == estimate_data_type(id),
            "estimate envelope mismatch"
        );
        let estimate = row
            .data
            .as_any()
            .downcast_ref::<BulletFundingEstimate>()
            .context("wrong custom decoder")?;
        anyhow::ensure!(
            estimate.instrument_id == id && estimate.interval_minutes == 60,
            "estimate payload mismatch"
        );
        estimates.push(estimate);
    }
    anyhow::ensure!(!estimates.is_empty(), "empty estimates");
    if let Some(boundary) = boundary {
        anyhow::ensure!(
            settlements.contains(&boundary),
            "expected settlement missing"
        );
        anyhow::ensure!(
            estimates
                .iter()
                .any(|r| r.next_funding_ns.as_u64() == boundary)
                && estimates
                    .iter()
                    .any(|r| r.next_funding_ns.as_u64() > boundary),
            "estimate rollover missing"
        );
        for times in [
            deltas
                .iter()
                .map(|r| r.ts_init.as_u64())
                .collect::<Vec<_>>(),
            trades.iter().map(|r| r.ts_init.as_u64()).collect(),
            marks.iter().map(|r| r.ts_init.as_u64()).collect(),
            indices.iter().map(|r| r.ts_init.as_u64()).collect(),
        ] {
            anyhow::ensure!(
                times.iter().any(|t| *t < boundary) && times.iter().any(|t| *t > boundary),
                "market family did not span settlement window"
            );
        }
    }

    let recovery_path = root.join("metadata/soak_recovery_epochs.json");
    let epochs: Vec<Value> = if recovery_path.exists() {
        serde_json::from_slice(&std::fs::read(recovery_path)?)?
    } else {
        Vec::new()
    };
    let requests_path = root.join("metadata/soak_reconnect_requests.jsonl");
    if requests_path.exists() {
        let count = std::fs::read_to_string(requests_path)?.lines().count();
        anyhow::ensure!(
            count > 0 && epochs.len() == count,
            "reconnect requests require complete confirmed epoch evidence"
        );
    }
    let mut recoveries = Vec::new();
    for epoch in epochs {
        let reset = epoch["reset_ns"]
            .as_u64()
            .context("missing epoch reset time")?;
        let first = deltas
            .iter()
            .find(|row| row.ts_init.as_u64() >= reset)
            .context("no book after reconnect")?;
        anyhow::ensure!(
            first.action == BookAction::Clear && RecordFlag::F_SNAPSHOT.matches(first.flags),
            "first new-epoch book row is not a CLEAR snapshot"
        );
        anyhow::ensure!(
            deltas.iter().any(|row| row.ts_init.as_u64() < reset),
            "no pre-reconnect book control"
        );
        let mark = marks
            .iter()
            .find(|r| r.ts_init.as_u64() >= reset)
            .context("mark did not recover")?;
        let trade = trades
            .iter()
            .find(|r| r.ts_init.as_u64() >= reset)
            .context("trades did not recover")?;
        anyhow::ensure!(
            marks.iter().any(|r| r.ts_init.as_u64() < reset)
                && trades.iter().any(|r| r.ts_init.as_u64() < reset),
            "missing pre-reconnect price/trade control"
        );
        recoveries.push(json!({"epoch":epoch,"first_book_ns":first.ts_init.as_u64(),"first_book_sequence":first.sequence,"first_book_action":"CLEAR","first_mark_ns":mark.ts_init.as_u64(),"first_trade_ns":trade.ts_init.as_u64()}));
    }

    if let Some(boundary) = boundary {
        let spanning = recoveries
            .iter()
            .filter_map(|r| {
                let loss = r["epoch"]["invalidation_ns"].as_u64()?;
                let resumed = r["first_book_ns"].as_u64()?;
                (loss <= boundary && boundary < resumed).then_some((loss, resumed))
            })
            .collect::<Vec<_>>();
        if spanning.is_empty() {
            anyhow::ensure!(
                physical_segments
                    .iter()
                    .any(
                        |row| row["initialization_rows"].as_u64().is_some_and(|n| n > 0)
                            && row["first_ts_init_ns"]
                                .as_u64()
                                .is_some_and(|t| t >= boundary && t < boundary + 5_000_000_000)
                    ),
                "missing healthy hourly checkpoint"
            );
        } else {
            for (loss, resumed) in spanning {
                anyhow::ensure!(
                    !physical_segments
                        .iter()
                        .any(
                            |row| row["initialization_rows"].as_u64().is_some_and(|n| n > 0)
                                && row["first_ts_init_ns"]
                                    .as_u64()
                                    .is_some_and(|t| t >= loss && t < resumed)
                        ),
                    "checkpoint was emitted during source gap"
                );
            }
        }
    }
    let quality: Value = serde_json::from_str(
        std::fs::read_to_string(root.join("metadata/capture_quality.jsonl"))?
            .lines()
            .last()
            .context("missing final quality")?,
    )?;
    anyhow::ensure!(
        quality["status"] == "completed" || super::only_source_gap_failure(&quality),
        "recording did not complete"
    );
    for key in [
        "dropped_items",
        "write_errors",
        "failed_workers",
        "queued_items",
        "in_flight_items",
    ] {
        anyhow::ensure!(
            quality["metrics"]["aggregated"][key].as_u64() == Some(0),
            "nonzero final quality counter {key}"
        );
    }
    let mut export = std::io::BufWriter::new(std::fs::File::create(
        root.join("metadata/soak_native_market_rows.jsonl"),
    )?);
    for row in &trades {
        serde_json::to_writer(&mut export, row)?;
        writeln!(export)?;
    }
    for row in &marks {
        serde_json::to_writer(&mut export, row)?;
        writeln!(export)?;
    }
    for row in &indices {
        serde_json::to_writer(&mut export, row)?;
        writeln!(export)?;
    }
    for row in &estimates {
        serde_json::to_writer(&mut export, row)?;
        writeln!(export)?;
    }
    export.flush()?;
    Ok(
        json!({"instrument_id":id,"recorded_funding_contract":contract,"book":book_report,"physical_book_segments":physical_segments,"book_batches_checked_for_integrity":batches,"last_venue_book_sequence":previous_sequence,"snapshot_points":snapshot_points,"trade_count":trades.len(),"unique_trade_count":trade_ids.len(),"mark_count":marks.len(),"index_count":indices.len(),"settled":settled,"estimate_count":estimates.len(),"first_estimate":estimates.first(),"last_estimate":estimates.last(),"expected_settlement_ns":boundary,"recoveries":recoveries,"quality":quality}),
    )
}

fn physical_book_segments(root: &Path, id: InstrumentId) -> anyhow::Result<Vec<Value>> {
    let mut segments = Vec::new();
    for entry in std::fs::read_dir(root.join("data/order_book_deltas").join(id.to_string()))? {
        let path = entry?.path();
        if path
            .extension()
            .is_none_or(|extension| extension != "parquet")
        {
            continue;
        }
        let reader = SerializedFileReader::new(std::fs::File::open(&path)?)?;
        let first = reader
            .get_row_iter(None)?
            .next()
            .transpose()?
            .context("empty book file")?;
        let value = |name: &str| -> anyhow::Result<u64> {
            first
                .get_column_iter()
                .find(|(key, _)| key.as_str() == name)
                .with_context(|| format!("missing book column {name}"))?
                .1
                .to_string()
                .parse()
                .context("invalid unsigned book field")
        };
        anyhow::ensure!(
            matches!(first
                .get_column_iter()
                .find(|(name, _)| name.as_str() == "action")
                .context("missing action")?
                .1, parquet::record::Field::Str(action) if action == "CLEAR")
                && (RecordFlag::F_SNAPSHOT.matches(value("flags")? as u8)
                    || (reader.metadata().file_metadata().num_rows() == 1
                        && value("flags")? == u64::from(RecordFlag::F_LAST as u8)
                        && value("sequence")? == 0)),
            "physical book file must start with a snapshot or contain only an invalidation: {}",
            path.display()
        );
        let arrow_metadata =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                std::fs::File::open(&path)?,
            )?
            .schema()
            .metadata()
            .clone();
        let initialization_rows = arrow_metadata
            .get("capture_initialization_rows")
            .map(|value| value.parse::<u64>())
            .transpose()?
            .unwrap_or(0);
        segments.push(json!({"file":path.file_name().context("missing filename")?.to_string_lossy(),"rows":reader.metadata().file_metadata().num_rows(),"first_action":"CLEAR","first_flags":value("flags")?,"first_sequence":value("sequence")?,"first_ts_init_ns":value("ts_init")?,"first_ts_event_ns":value("ts_event")?,"initialization_rows":initialization_rows,"window_id":arrow_metadata.get(catalog_capture_core::replay::WINDOW_ID_KEY).context("missing continuity window")?,"segment_index":arrow_metadata.get(catalog_capture_core::replay::SEGMENT_INDEX_KEY).context("missing segment index")?.parse::<u64>()?}));
    }
    segments.sort_by_key(|row| row["first_ts_init_ns"].as_u64());
    anyhow::ensure!(!segments.is_empty(), "no physical book segment control");
    Ok(segments)
}

/// Certify every initializer against preceding source rows in its own continuity window.
/// Read physical files and Native codecs directly: a replay-promoted initializer cannot certify itself.
fn validate_book_checkpoints(
    root: &Path,
    id: InstrumentId,
    segments: &[Value],
) -> anyhow::Result<usize> {
    let mut ordered = segments.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|row| {
        (
            row["window_id"].as_str().unwrap_or_default(),
            row["segment_index"].as_u64().unwrap_or_default(),
        )
    });
    let mut previous: Option<(&str, u64)> = None;
    let mut book = OrderBook::new(id, BookType::L2_MBP);
    let mut checked = 0;
    for segment in ordered {
        let window = segment["window_id"].as_str().context("missing window ID")?;
        let ordinal = segment["segment_index"]
            .as_u64()
            .context("missing segment index")?;
        if previous.is_none_or(|(old, last)| old != window || last.checked_add(1) != Some(ordinal))
        {
            book.reset();
        }
        previous = Some((window, ordinal));
        let file = segment["file"].as_str().context("missing segment file")?;
        let path = root
            .join("data/order_book_deltas")
            .join(id.to_string())
            .join(file);
        let builder = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path)?)?;
        let metadata = builder.schema().metadata().clone();
        let mut rows = Vec::new();
        for batch in builder.build()? {
            rows.extend(OrderBookDelta::decode_typed_batch(&metadata, batch?)?);
        }
        let count = usize::try_from(
            segment["initialization_rows"]
                .as_u64()
                .context("missing initialization count")?,
        )?;
        anyhow::ensure!(count <= rows.len(), "incomplete initialization prefix");
        let source = rows.split_off(count);
        if count > 0 {
            anyhow::ensure!(
                book.update_count > 0,
                "checkpoint has no preceding source baseline"
            );
            let mut initialization = catalog_capture_core::group_book_deltas(id, rows)?;
            anyhow::ensure!(
                initialization.len() == 1,
                "initializer must contain one full snapshot"
            );
            let checkpoint = initialization.remove(0);
            anyhow::ensure!(
                checkpoint.sequence == book.sequence && checkpoint.ts_event == book.ts_last,
                "checkpoint sequence/time differs from preceding source state"
            );
            let expected = book.to_deltas(book.ts_last, checkpoint.ts_init);
            // Native BookOrder equality compares order IDs only, so compare level values too.
            anyhow::ensure!(
                checkpoint.deltas.len() == expected.deltas.len()
                    && checkpoint
                        .deltas
                        .iter()
                        .zip(&expected.deltas)
                        .all(|(actual, expected)| {
                            actual == expected
                                && actual.order.side == expected.order.side
                                && actual.order.price == expected.order.price
                                && actual.order.size == expected.order.size
                        }),
                "checkpoint book contents differ from preceding source state"
            );
            checked += 1;
        }
        for frame in catalog_capture_core::group_book_deltas(id, source)? {
            anyhow::ensure!(
                book.update_count > 0
                    || (frame.deltas[0].action == BookAction::Clear
                        && RecordFlag::F_SNAPSHOT.matches(frame.flags)),
                "source window has no snapshot baseline"
            );
            book.apply_deltas(&frame)?;
            book_check_integrity(&book)?;
        }
    }
    Ok(checked)
}

#[test]
fn native_checkpoint_validation_rejects_wrong_contents_with_valid_identity() -> anyhow::Result<()> {
    use catalog_capture_core::{BookDeltaCatalogSink, CaptureConfig, CaptureSink, PartitionKey};
    use nautilus_model::{
        data::{BookOrder, OrderBookDeltas},
        enums::OrderSide,
        types::{Price, Quantity},
    };
    for corruption in ["none", "price", "quantity", "sequence", "time", "empty"] {
        let root =
            std::env::temp_dir().join(format!("bullet-checkpoint-{}", nautilus_core::UUID4::new()));
        std::fs::create_dir_all(&root)?;
        let id = InstrumentId::from("BTC-USD-PERP.BULLET");
        let mut sink = BookDeltaCatalogSink::from_config(&CaptureConfig {
            catalog_uri: format!("file://{}", root.display()),
            ..Default::default()
        })?
        .with_continuous_snapshots_for_venue(id.venue);
        let key = PartitionKey::catalog_data::<OrderBookDelta>(id).stable_key();
        let mut book = OrderBook::new(id, BookType::L2_MBP);
        book.add(
            BookOrder::new(OrderSide::Buy, Price::from("100"), Quantity::from("1"), 0),
            0,
            1,
            100.into(),
        );
        book.add(
            BookOrder::new(OrderSide::Sell, Price::from("101"), Quantity::from("2"), 0),
            0,
            1,
            100.into(),
        );
        sink.write_batch(&key, vec![book.to_deltas(100.into(), 100.into())])?;
        // A continuous periodic snapshot does not create a new research experiment.
        let mut periodic = book.to_deltas(105.into(), 105.into());
        for row in &mut periodic.deltas {
            row.sequence = 2;
        }
        let periodic = OrderBookDeltas::new(id, periodic.deltas);
        book.apply_deltas(&periodic)?;
        sink.write_batch(&key, vec![periodic])?;
        sink.rotate_partition(&key)?;
        let mut checkpoint = book.to_deltas(book.ts_last, 108.into());
        match corruption {
            "price" => checkpoint.deltas[1].order.price = Price::from("99"),
            "quantity" => checkpoint.deltas[1].order.size = Quantity::from("3"),
            "sequence" => {
                for row in &mut checkpoint.deltas {
                    row.sequence = 7;
                }
            }
            "time" => {
                for row in &mut checkpoint.deltas {
                    row.ts_event = 106.into();
                }
            }
            "empty" => {
                checkpoint.deltas.truncate(1);
                checkpoint.deltas[0].flags |= RecordFlag::F_LAST as u8;
            }
            "none" => {}
            _ => unreachable!(),
        }
        sink.write_initialization(&key, OrderBookDeltas::new(id, checkpoint.deltas))?;
        let update = OrderBookDelta::new(
            id,
            BookAction::Update,
            BookOrder::new(OrderSide::Buy, Price::from("100"), Quantity::from("4"), 0),
            RecordFlag::F_LAST as u8,
            3,
            110.into(),
            110.into(),
        );
        sink.write_batch(&key, vec![OrderBookDeltas::new(id, vec![update])])?;
        sink.seal_all()?;
        let segments = physical_book_segments(&root, id)?;
        assert_eq!(
            segments.len(),
            2,
            "periodic snapshot must not rotate the segment"
        );
        let result = validate_book_checkpoints(&root, id, &segments);
        if corruption == "none" {
            assert_eq!(result?, 1);
            let windows = catalog_capture_core::read_book_replay_windows(&root, id)?;
            assert_eq!(windows.len(), 1, "hourly rotation must preserve continuity");
            assert_eq!(
                windows[0].data.len(),
                3,
                "initializer must not become another source frame"
            );
        } else {
            let error = result
                .expect_err("corrupt checkpoint was accepted")
                .to_string();
            let expected = if matches!(corruption, "sequence" | "time") {
                "sequence/time differs"
            } else {
                "contents differ"
            };
            assert!(error.contains(expected), "{corruption}: {error}");
        }
        drop(sink);
        std::fs::remove_dir_all(root)?;
    }
    Ok(())
}

#[test]
fn physical_segment_decoder_reads_native_enum_labels_and_nanoseconds() -> anyhow::Result<()> {
    use catalog_capture_core::{BookDeltaCatalogSink, CaptureConfig, CaptureSink, PartitionKey};
    let root = std::env::temp_dir().join(format!(
        "bullet-physical-segment-{}",
        nautilus_core::UUID4::new()
    ));
    std::fs::create_dir_all(&root)?;
    let id = InstrumentId::from("BTC-USD-PERP.BULLET");
    let mut sink = BookDeltaCatalogSink::from_config(&CaptureConfig {
        catalog_uri: format!("file://{}", root.display()),
        ..Default::default()
    })?;
    let mut clear = OrderBookDelta::clear(id, 23, 1.into(), 2.into());
    clear.flags |= RecordFlag::F_SNAPSHOT as u8 | RecordFlag::F_LAST as u8;
    let key = PartitionKey::catalog_data::<OrderBookDelta>(id).stable_key();
    CaptureSink::write_initialization(
        &mut sink,
        &key,
        nautilus_model::data::OrderBookDeltas::new(id, vec![clear]),
    )?;
    CaptureSink::seal_all(&mut sink)?;
    let segments = physical_book_segments(&root, id)?;
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0]["first_action"], "CLEAR");
    assert_eq!(segments[0]["initialization_rows"], 1);
    assert_eq!(segments[0]["first_sequence"], 23);
    assert_eq!(segments[0]["first_ts_init_ns"], 2);
    drop(sink);
    std::fs::remove_dir_all(root)?;
    Ok(())
}
