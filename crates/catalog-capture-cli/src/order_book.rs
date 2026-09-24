use anyhow::{bail, Result};
use catalog_capture_core::{
    catalog_root_from_uri, validate_order_book_replay, OrderBookReplayReport,
};
use clap::ValueEnum;
use nautilus_model::identifiers::InstrumentId;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OrderBookValidationFormat {
    Json,
    Text,
}

pub fn validate_order_books(
    catalog_uri: &str,
    instrument_ids: Vec<String>,
    format: OrderBookValidationFormat,
    allow_missing_segment_snapshots: bool,
) -> Result<()> {
    if instrument_ids.is_empty() {
        bail!("at least one --instrument-id is required");
    }
    let catalog_root = catalog_root_from_uri(catalog_uri)?;
    let require_segment_snapshots = !allow_missing_segment_snapshots;
    let reports = instrument_ids
        .into_iter()
        .map(|value| {
            let instrument_id = InstrumentId::from(value.as_str());
            validate_order_book_replay(&catalog_root, instrument_id, require_segment_snapshots)
        })
        .collect::<Result<Vec<_>>>()?;

    match format {
        OrderBookValidationFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&reports)?);
        }
        OrderBookValidationFormat::Text => {
            println!("Catalog: {}", catalog_root.display());
            for report in reports {
                print_order_book_report(&report);
            }
        }
    }
    Ok(())
}

fn print_order_book_report(report: &OrderBookReplayReport) {
    println!(
        "instrument={} rows={} segments={} snapshot_segments={} snapshot_batches={} ts_init={}..{} final_levels={} bids/{} asks",
        report.instrument_id,
        report.row_count,
        report.segment_count,
        report.snapshot_segment_count,
        report.snapshot_batch_count,
        report.first_ts_init_ns,
        report.last_ts_init_ns,
        report.final_bid_level_count,
        report.final_ask_level_count,
    );
}
