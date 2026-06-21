use std::{fs, path::PathBuf, time::Duration};

use anyhow::{bail, Context, Result};
use catalog_capture_core::{
    append_option_universe_resolution_records, catalog_root_from_uri, derive_perp_instrument_id,
    expand_option_universe, merge_capture_plans, CapturePlan, OptionUniverseVenueKind,
    ResolvedOptionUniverse,
};
use catalog_capture_runtime_adapter::{
    plan_has_index_prices, plan_has_mark_prices, plan_has_quotes, CatalogCaptureActor,
    CatalogCaptureActorConfig, DynamicOptionUniverseConfig, DynamicOptionUniverseEntryConfig,
    OnlineOptionMetricsConfig, OnlineOptionMetricsUniverseConfig,
};
use nautilus_binance::{config::BinanceDataClientConfig, factories::BinanceDataClientFactory};
use nautilus_bybit::{config::BybitDataClientConfig, factories::BybitDataClientFactory};
use nautilus_common::enums::Environment;
use nautilus_deribit::{config::DeribitDataClientConfig, factories::DeribitDataClientFactory};
use nautilus_hyperliquid::data_types::register_hyperliquid_custom_data;
use nautilus_hyperliquid::{
    config::HyperliquidDataClientConfig, factories::HyperliquidDataClientFactory,
};
use nautilus_live::node::LiveNode;
use nautilus_model::{
    data::DataType,
    identifiers::{ActorId, TraderId},
    stubs::TestDefault,
};
use nautilus_okx::{config::OKXDataClientConfig, factories::OKXDataClientFactory};

use crate::config::{EffectiveConfig, VenueRuntimeConfig};
use crate::option_universe::{
    materialize_capture_plan_with_reports, run_option_universe_post_run_report,
    startup_resolution_record_from_report, validate_option_universes, OptionUniverseResolutionReport,
    PostRunReportOptions,
};

pub async fn run_capture(
    config: EffectiveConfig,
    post_run: PostRunReportOptions,
) -> Result<()> {
    let materialized = materialize_capture_plan_with_reports(&config).await?;
    run_capture_with_plan_and_reports(config, materialized.plan, &materialized.reports, post_run)
        .await
}

pub async fn run_capture_with_plan_and_reports(
    config: EffectiveConfig,
    plan: CapturePlan,
    reports: &[OptionUniverseResolutionReport],
    post_run: PostRunReportOptions,
) -> Result<()> {
    let catalog_dir = catalog_root_from_uri(&config.capture.catalog_uri)?;
    fs::create_dir_all(&catalog_dir)
        .with_context(|| format!("failed to create catalog dir {}", catalog_dir.display()))?;

    if !reports.is_empty() {
        let records = reports
            .iter()
            .map(startup_resolution_record_from_report)
            .collect::<Vec<_>>();
        append_option_universe_resolution_records(&catalog_dir, &records)
            .with_context(|| "failed to persist startup option universe resolution metadata")?;
    }

    if plan.is_empty() {
        bail!("capture plan is empty after option universe expansion");
    }

    register_known_custom_data_types(&plan.custom_data);

    let capture_actor = CatalogCaptureActor::new(CatalogCaptureActorConfig {
        actor_id: Some(ActorId::from("CATALOG_CAPTURE-CLI")),
        capture: config.capture.clone(),
        plan: plan.clone(),
        online_option_metrics: build_online_option_metrics_config(&config, &plan, reports)?,
        dynamic_option_universe: build_dynamic_option_universe_config(&config, &plan, reports)?,
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
    if config.runtime.capture_seconds == 0 {
        println!("Capture duration: until shutdown signal (capture_seconds=0)");
    } else {
        println!("Capture duration: {}s", config.runtime.capture_seconds);
    }
    println!("Venues: {}", config.venues.len());

    let stop_handle = node.handle();
    let capture_seconds = config.runtime.capture_seconds;
    tokio::spawn(async move {
        wait_for_capture_shutdown(capture_seconds).await;
        stop_handle.stop();
    });

    node.run().await?;

    println!("Capture completed");
    println!("Catalog dir: {}", catalog_dir.display());
    run_option_universe_post_run_report(&catalog_dir, &config, &post_run)?;
    Ok(())
}

async fn wait_for_capture_shutdown(capture_seconds: u64) {
    if capture_seconds == 0 {
        wait_for_shutdown_signal().await;
        return;
    }

    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(capture_seconds)) => {}
        _ = wait_for_shutdown_signal() => {}
    }
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let ctrl_c = tokio::signal::ctrl_c();
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to register Ctrl+C handler");
    }
}

pub fn validate_runtime(config: &EffectiveConfig) -> Result<()> {
    if config.runtime.shutdown_timeout_secs == 0 {
        bail!("runtime.shutdown_timeout_secs must be > 0");
    }
    if config.runtime.online_option_metrics.enabled
        && config.runtime.online_option_metrics.snapshot_interval_secs == 0
    {
        bail!("runtime.online_option_metrics.snapshot_interval_secs must be > 0");
    }
    if config.runtime.option_universe_refresh.enabled
        && config.runtime.option_universe_refresh.interval_secs == 0
    {
        bail!("runtime.option_universe_refresh.interval_secs must be > 0");
    }
    if config.venues.is_empty() {
        bail!("at least one venue is required");
    }
    validate_option_universes(&config.option_universes, &config.venues)?;
    validate_known_custom_data_types(&config.plan.custom_data, &config.venues)?;
    let _ = resolve_catalog_dir(&config.capture.catalog_uri)?;
    Ok(())
}

fn build_online_option_metrics_config(
    config: &EffectiveConfig,
    plan: &CapturePlan,
    reports: &[OptionUniverseResolutionReport],
) -> Result<Option<OnlineOptionMetricsConfig>> {
    if !config.runtime.online_option_metrics.enabled {
        return Ok(None);
    }
    if reports.is_empty() {
        bail!(
            "runtime.online_option_metrics.enabled requires at least one capture.option_universe entry"
        );
    }

    let planned_quote_ids = plan
        .quotes
        .iter()
        .map(|spec| spec.instrument_id)
        .collect::<std::collections::BTreeSet<_>>();
    let planned_greeks_ids = plan
        .option_greeks
        .iter()
        .map(|spec| spec.instrument_id)
        .collect::<std::collections::BTreeSet<_>>();

    let mut universes = Vec::with_capacity(reports.len());
    for report in reports {
        let Some(perp_instrument_id) = report.perp_instrument_id.as_deref() else {
            bail!(
                "runtime.online_option_metrics.enabled requires option universe venue_id `{}` to resolve a hedge perp (set include_perp = true and capture quotes)",
                report.venue_id
            );
        };
        let perp_instrument_id = perp_instrument_id.parse()?;
        if !planned_quote_ids.contains(&perp_instrument_id) {
            bail!(
                "runtime.online_option_metrics.enabled requires perp quotes for `{}`",
                perp_instrument_id
            );
        }

        let mut option_instrument_ids = Vec::with_capacity(report.option_instrument_ids.len());
        for option_instrument_id in &report.option_instrument_ids {
            let instrument_id = option_instrument_id.parse()?;
            if !planned_quote_ids.contains(&instrument_id) {
                bail!(
                    "runtime.online_option_metrics.enabled requires option quotes for `{}`",
                    instrument_id
                );
            }
            if !planned_greeks_ids.contains(&instrument_id) {
                bail!(
                    "runtime.online_option_metrics.enabled requires option_greeks for `{}`",
                    instrument_id
                );
            }
            option_instrument_ids.push(instrument_id);
        }

        universes.push(OnlineOptionMetricsUniverseConfig {
            venue_id: report.venue_id.clone(),
            underlying: report.underlying.clone(),
            expiry_iso8601: report.selected_expiry_iso8601.clone(),
            perp_instrument_id,
            option_instrument_ids,
        });
    }

    Ok(Some(OnlineOptionMetricsConfig {
        snapshot_interval_secs: config.runtime.online_option_metrics.snapshot_interval_secs,
        universes,
    }))
}

fn build_dynamic_option_universe_config(
    config: &EffectiveConfig,
    plan: &CapturePlan,
    reports: &[OptionUniverseResolutionReport],
) -> Result<Option<DynamicOptionUniverseConfig>> {
    if !config.runtime.option_universe_refresh.enabled {
        return Ok(None);
    }
    if reports.is_empty() {
        bail!("runtime.option_universe_refresh.enabled requires capture.option_universe entries");
    }

    let mut initial_dynamic_plan = CapturePlan::default();
    let mut universes = Vec::with_capacity(reports.len());

    for (spec, report) in config.option_universes.iter().zip(reports.iter()) {
        let resolved = resolved_option_universe_from_report(report)?;
        let venue = report_venue(report)?;
        let venue_config = config
            .venues
            .iter()
            .find(|entry| entry.id() == spec.venue_id)
            .with_context(|| {
                format!(
                    "capture.option_universe references unknown venue_id `{}`",
                    spec.venue_id
                )
            })?;
        let venue_kind = option_universe_venue_kind(venue_config).with_context(|| {
            format!(
                "runtime.option_universe_refresh is not supported for venue_id `{}`",
                spec.venue_id
            )
        })?;
        let reference_perp =
            derive_perp_instrument_id(spec, venue_kind).map_err(anyhow::Error::from)?;
        if !plan_has_quotes(plan, reference_perp)
            && !plan_has_mark_prices(plan, reference_perp)
            && !plan_has_index_prices(plan, reference_perp)
        {
            bail!(
                "runtime.option_universe_refresh requires perp quote/mark/index capture for `{}`",
                reference_perp
            );
        }

        let initial_plan = expand_option_universe(spec, &resolved);
        initial_dynamic_plan = merge_capture_plans(&initial_dynamic_plan, &initial_plan);
        universes.push(DynamicOptionUniverseEntryConfig {
            venue,
            venue_kind,
            spec: spec.clone(),
            initial_plan,
            initial_resolved: resolved,
        });
    }

    Ok(Some(DynamicOptionUniverseConfig {
        refresh_interval_secs: config.runtime.option_universe_refresh.interval_secs,
        strike_change_confirmations: config
            .runtime
            .option_universe_refresh
            .strike_change_confirmations,
        static_plan: config.plan.clone(),
        initial_dynamic_plan,
        universes,
    }))
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

fn resolved_option_universe_from_report(
    report: &OptionUniverseResolutionReport,
) -> Result<ResolvedOptionUniverse> {
    let selected_strikes = report
        .selected_strikes
        .iter()
        .map(|value| value.parse().map_err(anyhow::Error::msg))
        .collect::<Result<Vec<_>>>()?;
    let option_instrument_ids = report
        .option_instrument_ids
        .iter()
        .map(|value| value.parse())
        .collect::<Result<Vec<_>, _>>()?;
    let all_instrument_ids = report
        .all_instrument_ids
        .iter()
        .map(|value| value.parse())
        .collect::<Result<Vec<_>, _>>()?;

    Ok(ResolvedOptionUniverse {
        resolved_at_ns: report.resolved_at_ns.into(),
        selected_expiry_ns: report.selected_expiry_ns.into(),
        atm_reference: report.atm_reference.parse().map_err(anyhow::Error::msg)?,
        atm_reference_source: Some(report.atm_reference_source.clone()),
        selected_strikes,
        perp_instrument_id: report
            .perp_instrument_id
            .as_deref()
            .map(str::parse)
            .transpose()?,
        option_instrument_ids,
        all_instrument_ids,
    })
}

fn report_venue(
    report: &OptionUniverseResolutionReport,
) -> Result<nautilus_model::identifiers::Venue> {
    let sample = report
        .all_instrument_ids
        .first()
        .or(report.perp_instrument_id.as_ref())
        .with_context(|| {
            format!(
                "option universe report for venue_id `{}` did not contain any instrument ids",
                report.venue_id
            )
        })?;
    let instrument_id: nautilus_model::identifiers::InstrumentId = sample.parse()?;
    Ok(instrument_id.venue)
}

fn option_universe_venue_kind(venue: &VenueRuntimeConfig) -> Option<OptionUniverseVenueKind> {
    match venue {
        VenueRuntimeConfig::Deribit { .. } => Some(OptionUniverseVenueKind::Deribit),
        VenueRuntimeConfig::Bybit { .. } => Some(OptionUniverseVenueKind::Bybit),
        VenueRuntimeConfig::Okx { .. } => Some(OptionUniverseVenueKind::Okx),
        _ => None,
    }
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
