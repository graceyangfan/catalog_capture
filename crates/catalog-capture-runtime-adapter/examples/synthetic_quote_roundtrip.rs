use std::{fs, path::Path, str::FromStr};

use anyhow::Result;
use catalog_capture_core::{
    config::CaptureConfig,
    plan::{CapturePlan, QuoteCaptureSpec},
};
use catalog_capture_runtime_adapter::{CatalogCaptureActor, CatalogCaptureActorConfig};
use nautilus_common::actor::DataActor;
use nautilus_core::{UnixNanos, UUID4};
use nautilus_model::{
    data::QuoteTick,
    identifiers::{ActorId, InstrumentId},
    types::{Price, Quantity},
};
use nautilus_persistence::backend::catalog::ParquetDataCatalog;

fn create_quote_ticks(instrument_id: InstrumentId, base_ts: u64, count: usize) -> Vec<QuoteTick> {
    (0..count)
        .map(|index| {
            let ts = base_ts + index as u64 * 1_000;
            QuoteTick::new(
                instrument_id,
                Price::from("1.0001"),
                Price::from("1.0002"),
                Quantity::from("100"),
                Quantity::from("100"),
                UnixNanos::from(ts),
                UnixNanos::from(ts),
            )
        })
        .collect()
}

fn count_parquet_files(root: &Path) -> Result<usize> {
    let mut total = 0usize;

    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            total += count_parquet_files(&path)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
            total += 1;
        }
    }

    Ok(total)
}

fn main() -> Result<()> {
    let instrument_id = InstrumentId::from_str("ETHUSDT-PERP.BINANCE")?;
    let catalog_dir = std::env::temp_dir().join(format!(
        "nautilus-catalog-capture-roundtrip-{}",
        UUID4::new().as_str()
    ));
    fs::create_dir_all(&catalog_dir)?;

    let capture = CaptureConfig {
        catalog_uri: format!("file://{}", catalog_dir.display()),
        flush_rows: 3,
        flush_interval_ms: 1_000,
        max_buffer_bytes: 1024 * 1024,
        ..CaptureConfig::default()
    };

    let plan = CapturePlan {
        quotes: vec![QuoteCaptureSpec { instrument_id }],
        ..CapturePlan::default()
    };

    let config = CatalogCaptureActorConfig {
        actor_id: Some(ActorId::from("CATALOG_CAPTURE-ROUNDTRIP")),
        capture,
        plan,
        online_option_metrics: None,
        dynamic_option_universe: None,
    };

    let mut actor = CatalogCaptureActor::new(config)?;
    let written = create_quote_ticks(instrument_id, 1_000_000, 5);

    for quote in &written {
        DataActor::on_quote(&mut actor, quote)?;
    }

    let flush_results = actor.flush_all()?;
    let files_returned_by_flush_all: usize =
        flush_results.iter().map(|result| result.files.len()).sum();
    let rows_flushed: usize = flush_results.iter().map(|result| result.rows).sum();

    let mut catalog = ParquetDataCatalog::new(catalog_dir.as_path(), None, None, None, None);
    let loaded = catalog.quote_ticks(Some(vec![instrument_id.to_string()]), None, None)?;
    let parquet_files_on_disk = count_parquet_files(&catalog_dir)?;

    assert_eq!(
        loaded.len(),
        written.len(),
        "loaded tick count should match"
    );
    assert_eq!(loaded, written, "loaded ticks should equal written ticks");
    assert!(
        parquet_files_on_disk >= 2,
        "expected at least two chunk files with flush_rows=3"
    );
    assert_eq!(
        rows_flushed, 2,
        "flush_all should flush the remaining tail batch after automatic flushing"
    );

    println!("Synthetic quote round-trip succeeded");
    println!("Catalog dir: {}", catalog_dir.display());
    println!("Files returned by final flush_all: {files_returned_by_flush_all}");
    println!("Parquet files on disk: {parquet_files_on_disk}");
    println!("Rows flushed by final flush_all: {rows_flushed}");
    println!("Loaded ticks: {}", loaded.len());
    println!(
        "Time range: {} -> {}",
        loaded.first().unwrap().ts_init.as_u64(),
        loaded.last().unwrap().ts_init.as_u64()
    );

    fs::remove_dir_all(&catalog_dir)?;
    Ok(())
}
