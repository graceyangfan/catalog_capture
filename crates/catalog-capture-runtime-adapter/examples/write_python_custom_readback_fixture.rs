use std::{env, fs, path::PathBuf, str::FromStr, sync::Arc};

use anyhow::{Context, Result};
use catalog_capture_core::{
    config::CaptureConfig,
    plan::{CapturePlan, CustomDataCaptureSpec, InstrumentCaptureSpec},
};
use catalog_capture_runtime_adapter::{CatalogCaptureActor, CatalogCaptureActorConfig};
use nautilus_common::actor::DataActor;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{CustomData, DataType},
    identifiers::{ActorId, InstrumentId},
    instruments::{InstrumentAny, stubs::crypto_perpetual_ethusdt},
};
use nautilus_persistence::test_data::RustTestCustomData;
use nautilus_serialization::ensure_custom_data_registered;

fn resolve_catalog_dir() -> Result<PathBuf> {
    if let Some(arg1) = env::args().nth(1) {
        return Ok(PathBuf::from(arg1));
    }

    if let Ok(dir) = env::var("CATALOG_DIR") {
        return Ok(PathBuf::from(dir));
    }

    anyhow::bail!("Provide catalog directory as first arg or CATALOG_DIR env var");
}

fn main() -> Result<()> {
    ensure_custom_data_registered::<RustTestCustomData>();

    let catalog_dir = resolve_catalog_dir()?;
    fs::create_dir_all(&catalog_dir)
        .with_context(|| format!("failed to create catalog dir {}", catalog_dir.display()))?;

    let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
    let instrument_id = InstrumentId::from_str("ETHUSDT-PERP.BINANCE")?;

    let capture = CaptureConfig {
        catalog_uri: format!("file://{}", catalog_dir.display()),
        flush_rows: 2,
        flush_interval_ms: 1_000,
        max_buffer_bytes: 1024 * 1024,
        ..CaptureConfig::default()
    };

    let custom_type = DataType::new(
        "RustTestCustomData",
        None,
        Some(instrument_id.to_string()),
    );
    let plan = CapturePlan {
        instruments: vec![InstrumentCaptureSpec { instrument_id }],
        custom_data: vec![CustomDataCaptureSpec {
            data_type: custom_type.clone(),
        }],
        ..CapturePlan::default()
    };

    let config = CatalogCaptureActorConfig {
        actor_id: Some(ActorId::from("CATALOG_CAPTURE-CUSTOM_FIXTURE")),
        capture,
        plan,
        online_option_metrics: None,
        dynamic_option_universe: None,
    };

    let mut actor = CatalogCaptureActor::new(config)?;
    DataActor::on_instrument(&mut actor, &instrument)?;

    let original = [
        RustTestCustomData {
            instrument_id,
            value: 1.23,
            flag: true,
            ts_event: UnixNanos::from(1_000_000),
            ts_init: UnixNanos::from(1_000_000),
        },
        RustTestCustomData {
            instrument_id,
            value: 4.56,
            flag: false,
            ts_event: UnixNanos::from(1_001_000),
            ts_init: UnixNanos::from(1_001_000),
        },
    ];

    for item in original.iter().cloned() {
        let custom = CustomData::new(Arc::new(item), custom_type.clone());
        DataActor::on_data(&mut actor, &custom)?;
    }

    let _ = actor.flush_all()?;

    println!("Python custom readback fixture written");
    println!("Catalog dir: {}", catalog_dir.display());
    println!("Instrument id: {}", instrument_id);
    println!("Custom type: RustTestCustomData");

    Ok(())
}
