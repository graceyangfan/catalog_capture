use std::{fs, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use catalog_capture_runtime_adapter::{CatalogCaptureActor, CatalogCaptureActorConfig};
use nautilus_binance::{config::BinanceDataClientConfig, factories::BinanceDataClientFactory};
use nautilus_deribit::{config::DeribitDataClientConfig, factories::DeribitDataClientFactory};
use nautilus_common::enums::Environment;
use nautilus_live::node::LiveNode;
use nautilus_model::{
    identifiers::{ActorId, TraderId},
    stubs::TestDefault,
};

use crate::config::{EffectiveConfig, VenueRuntimeConfig};

pub async fn run_capture(config: EffectiveConfig) -> Result<()> {
    let catalog_dir = resolve_catalog_dir(&config.capture.catalog_uri)?;
    fs::create_dir_all(&catalog_dir)
        .with_context(|| format!("failed to create catalog dir {}", catalog_dir.display()))?;

    let capture_actor = CatalogCaptureActor::new(CatalogCaptureActorConfig {
        actor_id: Some(ActorId::from("CATALOG_CAPTURE-CLI")),
        capture: config.capture.clone(),
        plan: config.plan.clone(),
    })?;

    let trader_id = TraderId::test_default();
    let mut builder = LiveNode::builder(trader_id, Environment::Live)?
        .with_name(config.runtime.node_name.as_str())
        .with_delay_post_stop_secs(config.runtime.delay_post_stop_secs);

    for venue in &config.venues {
        match venue {
            VenueRuntimeConfig::BinanceFutures {
                id,
                environment,
                product_type,
            } => {
                println!(
                    "Configuring venue {} ({product_type:?}, {environment:?})",
                    id
                );
                builder = builder.add_data_client(
                    None,
                    Box::new(BinanceDataClientFactory::new()),
                    Box::new(BinanceDataClientConfig {
                        product_type: *product_type,
                        environment: *environment,
                        api_key: None,
                        api_secret: None,
                        ..Default::default()
                    }),
                )?;
            }
            VenueRuntimeConfig::Deribit {
                id,
                environment,
                product_types,
            } => {
                println!(
                    "Configuring venue {} (product_types={product_types:?}, {environment:?})",
                    id
                );
                builder = builder.add_data_client(
                    None,
                    Box::new(DeribitDataClientFactory::new()),
                    Box::new(DeribitDataClientConfig {
                        environment: *environment,
                        product_types: product_types.clone(),
                        api_key: None,
                        api_secret: None,
                        ..Default::default()
                    }),
                )?;
            }
        }
    }

    let mut node = builder.build()?;
    node.add_actor(capture_actor)?;

    println!("Starting catalog capture");
    println!("Catalog dir: {}", catalog_dir.display());
    println!("Capture duration: {}s", config.runtime.capture_seconds);
    println!("Venues: {}", config.venues.len());

    let stop_handle = node.handle();
    let capture_seconds = config.runtime.capture_seconds;
    tokio::spawn(async move {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(capture_seconds)) => {
                stop_handle.stop();
            }
            _ = tokio::signal::ctrl_c() => {
                stop_handle.stop();
            }
        }
    });

    node.run().await?;

    println!("Capture completed");
    println!("Catalog dir: {}", catalog_dir.display());
    Ok(())
}

pub fn validate_runtime(config: &EffectiveConfig) -> Result<()> {
    if config.runtime.capture_seconds == 0 {
        bail!("runtime.capture_seconds must be > 0");
    }
    if config.runtime.shutdown_timeout_secs == 0 {
        bail!("runtime.shutdown_timeout_secs must be > 0");
    }
    if config.venues.is_empty() {
        bail!("at least one venue is required");
    }
    let _ = resolve_catalog_dir(&config.capture.catalog_uri)?;
    Ok(())
}

fn resolve_catalog_dir(catalog_uri: &str) -> Result<PathBuf> {
    let path = catalog_uri.strip_prefix("file://").unwrap_or(catalog_uri);
    if path.is_empty() {
        bail!("output.catalog_uri cannot be empty");
    }
    Ok(PathBuf::from(path))
}
