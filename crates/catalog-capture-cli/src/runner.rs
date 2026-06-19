use std::{fs, path::PathBuf, time::Duration};

use anyhow::{bail, Context, Result};
use catalog_capture_core::{
    expand_option_universe, merge_capture_plans, resolve_option_universe, CapturePlan,
    OptionUniverseFamily, OptionUniverseSpec, ResolvedOptionUniverse,
};
use catalog_capture_runtime_adapter::{CatalogCaptureActor, CatalogCaptureActorConfig};
use nautilus_binance::{config::BinanceDataClientConfig, factories::BinanceDataClientFactory};
use nautilus_bybit::{config::BybitDataClientConfig, factories::BybitDataClientFactory};
use nautilus_common::enums::Environment;
use nautilus_deribit::{
    common::enums::DeribitCurrency, config::DeribitDataClientConfig,
    factories::DeribitDataClientFactory, http::client::DeribitHttpClient,
    http::models::DeribitProductType,
};
use nautilus_hyperliquid::data_types::register_hyperliquid_custom_data;
use nautilus_hyperliquid::{
    config::HyperliquidDataClientConfig, factories::HyperliquidDataClientFactory,
};
use nautilus_live::node::LiveNode;
use nautilus_model::{
    data::DataType,
    identifiers::{ActorId, TraderId},
    stubs::TestDefault,
    types::Price,
};
use nautilus_okx::{config::OKXDataClientConfig, factories::OKXDataClientFactory};
use std::str::FromStr;

use crate::config::{EffectiveConfig, VenueRuntimeConfig};

pub async fn run_capture(config: EffectiveConfig) -> Result<()> {
    let catalog_dir = resolve_catalog_dir(&config.capture.catalog_uri)?;
    fs::create_dir_all(&catalog_dir)
        .with_context(|| format!("failed to create catalog dir {}", catalog_dir.display()))?;

    let plan = materialize_capture_plan(&config).await?;
    if plan.is_empty() {
        bail!("capture plan is empty after option universe expansion");
    }

    register_known_custom_data_types(&plan.custom_data);

    let capture_actor = CatalogCaptureActor::new(CatalogCaptureActorConfig {
        actor_id: Some(ActorId::from("CATALOG_CAPTURE-CLI")),
        capture: config.capture.clone(),
        plan: plan.clone(),
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
            VenueRuntimeConfig::Bybit {
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
                    Box::new(BybitDataClientFactory::new()),
                    Box::new(BybitDataClientConfig {
                        environment: *environment,
                        product_types: product_types.clone(),
                        api_key: None,
                        api_secret: None,
                        ..Default::default()
                    }),
                )?;
            }
            VenueRuntimeConfig::Hyperliquid { id, environment } => {
                println!("Configuring venue {} ({environment:?})", id);
                builder = builder.add_data_client(
                    None,
                    Box::new(HyperliquidDataClientFactory::new()),
                    Box::new(HyperliquidDataClientConfig {
                        environment: *environment,
                        private_key: None,
                        ..Default::default()
                    }),
                )?;
            }
            VenueRuntimeConfig::Okx {
                id,
                environment,
                instrument_types,
                instrument_families,
            } => {
                println!(
                    "Configuring venue {} (instrument_types={instrument_types:?}, families={instrument_families:?}, {environment:?})",
                    id
                );
                builder = builder.add_data_client(
                    None,
                    Box::new(OKXDataClientFactory::new()),
                    Box::new(OKXDataClientConfig {
                        environment: *environment,
                        instrument_types: instrument_types.clone(),
                        instrument_families: instrument_families.clone(),
                        api_key: None,
                        api_secret: None,
                        api_passphrase: None,
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
    validate_option_universes(&config.option_universes, &config.venues)?;
    validate_known_custom_data_types(&config.plan.custom_data, &config.venues)?;
    let _ = resolve_catalog_dir(&config.capture.catalog_uri)?;
    Ok(())
}

async fn materialize_capture_plan(config: &EffectiveConfig) -> Result<CapturePlan> {
    let mut plan = config.plan.clone();

    for spec in &config.option_universes {
        let venue = config
            .venues
            .iter()
            .find(|venue| venue.id() == spec.venue_id)
            .with_context(|| {
                format!(
                    "capture.option_universe references unknown venue_id `{}`",
                    spec.venue_id
                )
            })?;

        let resolved = match venue {
            VenueRuntimeConfig::Deribit { environment, .. } => {
                resolve_deribit_option_universe(spec, *environment).await?
            }
            _ => bail!(
                "capture.option_universe currently only supports Deribit venues; got venue_id `{}`",
                spec.venue_id
            ),
        };

        println!(
            "Resolved option universe {} {} expiry={} atm={} strikes={:?} instruments={:?}",
            spec.venue_id,
            spec.underlying,
            resolved.selected_expiry_ns.as_u64(),
            resolved.atm_reference,
            resolved.selected_strikes,
            resolved.all_instrument_ids
        );

        let expanded = expand_option_universe(spec, &resolved);
        plan = merge_capture_plans(&plan, &expanded);
    }

    Ok(plan)
}

async fn resolve_deribit_option_universe(
    spec: &OptionUniverseSpec,
    environment: nautilus_deribit::common::enums::DeribitEnvironment,
) -> Result<ResolvedOptionUniverse> {
    let currency = DeribitCurrency::from_str(&spec.underlying).with_context(|| {
        format!(
            "unsupported Deribit option underlying `{}`",
            spec.underlying
        )
    })?;
    let client = DeribitHttpClient::new(None, environment, 30, 3, 500, 5_000, None)
        .context("failed to create Deribit HTTP client for option universe resolution")?;

    let option_instruments = client
        .request_instruments(currency, Some(DeribitProductType::Option))
        .await
        .with_context(|| {
            format!(
                "failed to request Deribit option instruments for underlying {}",
                spec.underlying
            )
        })?;

    let atm_reference = request_deribit_atm_reference(&client, spec).await?;
    let perp_instrument_id = spec
        .include_perp
        .then(|| derive_deribit_perpetual_id(&spec.underlying))
        .transpose()?;

    resolve_option_universe(
        spec,
        &option_instruments,
        nautilus_core::time::get_atomic_clock_realtime().get_time_ns(),
        atm_reference,
        perp_instrument_id,
    )
    .map_err(anyhow::Error::from)
}

async fn request_deribit_atm_reference(
    client: &DeribitHttpClient,
    spec: &OptionUniverseSpec,
) -> Result<Price> {
    let summaries = client
        .request_book_summaries(&spec.underlying)
        .await
        .with_context(|| {
            format!(
                "failed to request Deribit option book summaries for underlying {}",
                spec.underlying
            )
        })?;

    let Some(summary) = summaries
        .into_iter()
        .find(|summary| summary.underlying_price.is_some())
    else {
        bail!(
            "no Deribit option book summary returned an underlying_price for {}",
            spec.underlying
        );
    };

    Ok(Price::from(
        summary
            .underlying_price
            .expect("summary filtered to underlying_price")
            .to_string()
            .as_str(),
    ))
}

fn derive_deribit_perpetual_id(
    underlying: &str,
) -> Result<nautilus_model::identifiers::InstrumentId> {
    Ok(nautilus_model::identifiers::InstrumentId::from_str(
        format!("{underlying}-PERPETUAL.DERIBIT").as_str(),
    )?)
}

fn validate_option_universes(
    specs: &[OptionUniverseSpec],
    venues: &[VenueRuntimeConfig],
) -> Result<()> {
    for spec in specs {
        validate_option_universe(spec, venues)?;
    }
    Ok(())
}

fn validate_option_universe(
    spec: &OptionUniverseSpec,
    venues: &[VenueRuntimeConfig],
) -> Result<()> {
    let venue = venues
        .iter()
        .find(|venue| venue.id() == spec.venue_id)
        .with_context(|| {
            format!(
                "capture.option_universe references unknown venue_id `{}`",
                spec.venue_id
            )
        })?;

    match venue {
        VenueRuntimeConfig::Deribit { product_types, .. } => {
            if !product_types.contains(&DeribitProductType::Option) {
                bail!(
                    "capture.option_universe venue_id `{}` requires deribit product_types to include \"option\"",
                    spec.venue_id
                );
            }
            if spec.include_perp && !product_types.contains(&DeribitProductType::Future) {
                bail!(
                    "capture.option_universe venue_id `{}` with include_perp = true requires deribit product_types to include \"future\"",
                    spec.venue_id
                );
            }
        }
        _ => bail!(
            "capture.option_universe currently only supports [[venues]] entries with kind = \"deribit\""
        ),
    }

    let needs_perp = spec.families.iter().any(|family| {
        matches!(
            family,
            OptionUniverseFamily::IndexPrices | OptionUniverseFamily::FundingRates
        )
    });
    if needs_perp && !spec.include_perp {
        bail!(
            "capture.option_universe families index_prices/funding_rates require include_perp = true"
        );
    }

    Ok(())
}

fn register_known_custom_data_types(custom_data: &[catalog_capture_core::CustomDataCaptureSpec]) {
    for spec in custom_data {
        match spec.data_type.type_name() {
            "DeribitVolatilityIndex" => {
                nautilus_deribit::data_types::register_deribit_custom_data()
            }
            "HyperliquidOpenInterest" => register_hyperliquid_custom_data(),
            _ => {}
        }
    }
}

fn validate_known_custom_data_types(
    custom_data: &[catalog_capture_core::CustomDataCaptureSpec],
    venues: &[VenueRuntimeConfig],
) -> Result<()> {
    for spec in custom_data {
        validate_known_custom_data_type(&spec.data_type, venues)?;
    }
    Ok(())
}

fn validate_known_custom_data_type(
    data_type: &DataType,
    venues: &[VenueRuntimeConfig],
) -> Result<()> {
    match data_type.type_name() {
        "BinanceFuturesLiquidation" => {
            bail!(
                "custom_data BinanceFuturesLiquidation is not yet supported for direct parquet \
                 capture in this workspace because the upstream type lacks Arrow batch encoding"
            );
        }
        "DeribitVolatilityIndex" => {
            require_venue(
                venues,
                VenueRequirement::Deribit,
                "custom_data DeribitVolatilityIndex requires at least one [[venues]] entry with kind = \"deribit\"",
            )?;
            let Some(index_name) = data_type
                .metadata()
                .as_ref()
                .and_then(|metadata| metadata.get("index_name"))
                .and_then(|value| value.as_str())
            else {
                bail!(
                    "custom_data DeribitVolatilityIndex requires metadata.index_name \
                     (for example `btc_usd`)"
                );
            };
            if index_name.trim().is_empty() {
                bail!("custom_data DeribitVolatilityIndex metadata.index_name must be non-empty");
            }
        }
        "HyperliquidOpenInterest" => {
            require_venue(
                venues,
                VenueRequirement::Hyperliquid,
                "custom_data HyperliquidOpenInterest requires at least one [[venues]] entry with kind = \"hyperliquid\"",
            )?;
            let Some(instrument_id) = data_type
                .metadata()
                .as_ref()
                .and_then(|metadata| metadata.get("instrument_id"))
                .and_then(|value| value.as_str())
            else {
                bail!(
                    "custom_data HyperliquidOpenInterest requires metadata.instrument_id \
                     (for example `ETH-USD-PERP.HYPERLIQUID`)"
                );
            };
            if instrument_id.trim().is_empty() {
                bail!(
                    "custom_data HyperliquidOpenInterest metadata.instrument_id must be non-empty"
                );
            }
            if let Some(identifier) = data_type.identifier() {
                if identifier != instrument_id {
                    bail!(
                        "custom_data HyperliquidOpenInterest identifier `{identifier}` must match \
                         metadata.instrument_id `{instrument_id}`"
                    );
                }
            }
        }
        other => {
            bail!(
                "unknown custom_data type_name `{other}`; supported values in this workspace: \
                 DeribitVolatilityIndex, HyperliquidOpenInterest, BinanceFuturesLiquidation"
            );
        }
    }

    Ok(())
}

#[derive(Clone, Copy)]
enum VenueRequirement {
    Deribit,
    Hyperliquid,
}

fn require_venue(
    venues: &[VenueRuntimeConfig],
    requirement: VenueRequirement,
    error: &str,
) -> Result<()> {
    let matches_requirement = venues.iter().any(|venue| match (requirement, venue) {
        (VenueRequirement::Deribit, VenueRuntimeConfig::Deribit { .. }) => true,
        (VenueRequirement::Hyperliquid, VenueRuntimeConfig::Hyperliquid { .. }) => true,
        _ => false,
    });
    if matches_requirement {
        return Ok(());
    }
    bail!("{error}")
}

fn resolve_catalog_dir(catalog_uri: &str) -> Result<PathBuf> {
    let path = catalog_uri.strip_prefix("file://").unwrap_or(catalog_uri);
    if path.is_empty() {
        bail!("output.catalog_uri cannot be empty");
    }
    Ok(PathBuf::from(path))
}
