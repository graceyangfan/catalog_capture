use std::{
    env, fs,
    path::PathBuf,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use catalog_capture_core::{
    config::CaptureConfig,
    plan::{CapturePlan, InstrumentCaptureSpec, QuoteCaptureSpec},
};
use catalog_capture_runtime_adapter::{CatalogCaptureActor, CatalogCaptureActorConfig};
use nautilus_binance::{
    common::enums::{BinanceEnvironment, BinanceProductType},
    config::BinanceDataClientConfig,
    factories::BinanceDataClientFactory,
};
use nautilus_common::enums::Environment;
use nautilus_live::node::LiveNode;
use nautilus_model::{
    identifiers::{ActorId, InstrumentId, TraderId},
    stubs::TestDefault,
};

fn parse_binance_env(value: &str) -> Result<BinanceEnvironment> {
    match value.to_ascii_lowercase().as_str() {
        "live" => Ok(BinanceEnvironment::Live),
        "testnet" => Ok(BinanceEnvironment::Testnet),
        other => anyhow::bail!("unsupported BINANCE_ENV={other}, expected live|testnet"),
    }
}

fn default_catalog_dir() -> Result<PathBuf> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before UNIX_EPOCH")?
        .as_secs();
    Ok(PathBuf::from(format!(
        "/tmp/nautilus-binance-futures-capture-{ts}"
    )))
}

fn count_parquet_files(root: &PathBuf) -> Result<usize> {
    if !root.exists() {
        return Ok(0);
    }

    let mut count = 0usize;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            count += count_parquet_files(&path)?;
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
            count += 1;
        }
    }

    Ok(count)
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let instrument_id = InstrumentId::from_str(
        &env::var("CAPTURE_INSTRUMENT_ID").unwrap_or_else(|_| "ETHUSDT-PERP.BINANCE".to_string()),
    )?;
    let capture_seconds = env::var("CAPTURE_SECONDS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(30);
    let catalog_dir = env::var("CATALOG_DIR")
        .map(PathBuf::from)
        .unwrap_or(default_catalog_dir()?);
    let binance_env = parse_binance_env(
        &env::var("BINANCE_ENV").unwrap_or_else(|_| "testnet".to_string()),
    )?;

    fs::create_dir_all(&catalog_dir)
        .with_context(|| format!("failed to create catalog dir {}", catalog_dir.display()))?;

    let capture = CaptureConfig {
        catalog_uri: format!("file://{}", catalog_dir.display()),
        flush_rows: 1_000,
        flush_interval_ms: 1_000,
        max_buffer_bytes: 8 * 1024 * 1024,
        ..CaptureConfig::default()
    };

    let plan = CapturePlan {
        instruments: vec![InstrumentCaptureSpec { instrument_id }],
        quotes: vec![QuoteCaptureSpec { instrument_id }],
        ..CapturePlan::default()
    };

    let capture_actor = CatalogCaptureActor::new(CatalogCaptureActorConfig {
        actor_id: Some(ActorId::from("CATALOG_CAPTURE-BINANCE_QUOTES")),
        capture: capture.clone(),
        plan,
        online_option_metrics: None,
        dynamic_option_universe: None,
    })?;

    let trader_id = TraderId::test_default();
    let mut node = LiveNode::builder(trader_id, Environment::Live)?
        .with_name("BINANCE-FUTURES-CAPTURE-001")
        .with_delay_post_stop_secs(2)
        .add_data_client(
            None,
            Box::new(BinanceDataClientFactory::new()),
            Box::new(BinanceDataClientConfig {
                product_type: BinanceProductType::UsdM,
                environment: binance_env,
                api_key: None,
                api_secret: None,
                ..Default::default()
            }),
        )?
        .build()?;

    node.add_actor(capture_actor)?;

    let stop_handle = node.handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(capture_seconds)).await;
        stop_handle.stop();
    });

    println!("Starting Binance Futures quote capture");
    println!("Catalog dir: {}", catalog_dir.display());
    println!("Instrument: {instrument_id}");
    println!("Duration: {capture_seconds}s");
    println!("Environment: {binance_env:?}");

    node.run().await?;

    {
        let cache = node.kernel().cache();
        let _instrument = cache
            .borrow()
            .instrument(&instrument_id)
            .cloned()
            .with_context(|| format!("instrument {instrument_id} was not found in cache after run"))?;
    }

    let parquet_files = count_parquet_files(&catalog_dir)?;

    println!("Capture completed");
    println!("Catalog dir: {}", catalog_dir.display());
    println!("Parquet files: {parquet_files}");
    println!(
        "Verify with: /Users/yfclark/nautilus_trader/.venv/bin/python /Users/yfclark/nautilus_catalog_capture/tests/python_catalog_probe.py {} {} 1",
        catalog_dir.display(),
        instrument_id
    );

    Ok(())
}
