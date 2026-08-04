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

use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::{bail, Context, Result};
#[cfg(feature = "venue-hyperliquid")]
use catalog_capture_core::expand_hip4_universe;
use catalog_capture_core::{
    append_hip4_universe_resolution_records, append_option_universe_resolution_records,
    catalog_root_from_uri, derive_perp_instrument_id, estimate_peak_buffered_bytes,
    expand_option_universe, format_budget_warning, format_buffer_estimate, merge_capture_plans,
    new_capture_run_record, write_capture_run_record, CaptureMetricsSnapshot, CapturePlan,
    CaptureRunInput, CaptureRunVenueRecord, LayoutCompatibility, OptionUniverseVenueKind,
    ResolvedHip4Universe, ResolvedOptionUniverse,
};
use catalog_capture_runtime_adapter::{
    plan_has_index_prices, plan_has_mark_prices, plan_has_quotes, CatalogCaptureActor,
    CatalogCaptureActorConfig, DynamicHip4UniverseConfig, DynamicHip4UniverseEntryConfig,
    DynamicOptionUniverseConfig, DynamicOptionUniverseEntryConfig, OnlineOptionMetricsConfig,
    OnlineOptionMetricsUniverseConfig,
};
#[cfg(feature = "venue-binance")]
use nautilus_binance::{config::BinanceDataClientConfig, factories::BinanceDataClientFactory};
#[cfg(feature = "venue-bybit")]
use nautilus_bybit::{config::BybitDataClientConfig, factories::BybitDataClientFactory};
use nautilus_common::{cache::CacheConfig, enums::Environment};
#[cfg(feature = "venue-deribit")]
use nautilus_deribit::{config::DeribitDataClientConfig, factories::DeribitDataClientFactory};
#[cfg(feature = "venue-hyperliquid")]
use nautilus_hyperliquid::common::enums::HyperliquidEnvironment;
#[cfg(feature = "venue-hyperliquid")]
use nautilus_hyperliquid::{
    config::HyperliquidDataClientConfig, factories::HyperliquidDataClientFactory,
};
use nautilus_live::node::LiveNode;
use nautilus_model::identifiers::{ActorId, TraderId};
#[cfg(feature = "venue-okx")]
use nautilus_okx::{config::OKXDataClientConfig, factories::OKXDataClientFactory};

use crate::config::{EffectiveConfig, VenueRuntimeConfig};
use crate::credentials::{
    api_key_secret_present, binance_credentials, bybit_credentials, deribit_credentials,
    hyperliquid_private_key, okx_credentials,
};
use crate::custom_data::{
    register_request_types, register_subscribe_types, validate_request_data_type,
    validate_subscribe_data_type,
};
use crate::hip4::{
    materialize_hip4_capture_plan, startup_resolution_record_from_report as hip4_startup_record,
    validate_hip4_universes, Hip4UniverseResolutionReport,
};
use crate::metrics_server::spawn_metrics_server;
use crate::option_universe::{
    materialize_capture_plan_with_reports, run_option_universe_post_run_report,
    startup_resolution_record_from_report, validate_option_universes,
    OptionUniverseResolutionReport, PostRunReportOptions,
};

pub async fn run_capture(config: EffectiveConfig, post_run: PostRunReportOptions) -> Result<()> {
    let materialized = materialize_full_capture_plan(&config).await?;
    run_capture_with_plan_and_reports(
        config,
        materialized.plan,
        &materialized.option_universe_reports,
        &materialized.hip4_reports,
        &materialized.hip4_resolved,
        post_run,
    )
    .await
}

#[derive(Debug, Clone)]
pub struct MaterializedCapturePlan {
    pub plan: CapturePlan,
    pub option_universe_reports: Vec<OptionUniverseResolutionReport>,
    pub hip4_reports: Vec<Hip4UniverseResolutionReport>,
    pub hip4_resolved: Vec<ResolvedHip4Universe>,
}

pub async fn materialize_full_capture_plan(
    config: &EffectiveConfig,
) -> Result<MaterializedCapturePlan> {
    let option_materialized = materialize_capture_plan_with_reports(config).await?;
    let hip4_materialized = materialize_hip4_capture_plan(config).await?;
    Ok(MaterializedCapturePlan {
        plan: merge_capture_plans(&option_materialized.plan, &hip4_materialized.plan),
        option_universe_reports: option_materialized.reports,
        hip4_reports: hip4_materialized.reports,
        hip4_resolved: hip4_materialized.resolved,
    })
}

pub async fn run_capture_with_plan_and_reports(
    config: EffectiveConfig,
    plan: CapturePlan,
    reports: &[OptionUniverseResolutionReport],
    hip4_reports: &[Hip4UniverseResolutionReport],
    hip4_resolved: &[ResolvedHip4Universe],
    post_run: PostRunReportOptions,
) -> Result<()> {
    let catalog_dir = catalog_root_from_uri(&config.capture.catalog_uri)?;
    fs::create_dir_all(&catalog_dir)
        .with_context(|| format!("failed to create catalog dir {}", catalog_dir.display()))?;

    persist_startup_resolution_metadata(
        &catalog_dir,
        &config,
        reports,
        hip4_reports,
        hip4_resolved,
    )?;
    persist_capture_run_metadata(&catalog_dir, &config, &plan)?;

    if plan.is_empty() {
        bail!("capture plan is empty after universe expansion");
    }

    log_capture_buffer_estimate(&config, &plan);

    register_subscribe_types(&plan.custom_data);
    register_request_types(&plan.custom_data_requests);

    let (metrics_snapshot, metrics_refresh_interval_secs) = build_metrics_runtime_state(&config);
    let capture_actor = CatalogCaptureActor::new(build_capture_actor_config(
        &config,
        &plan,
        reports,
        hip4_reports,
        hip4_resolved,
        metrics_snapshot.clone(),
        metrics_refresh_interval_secs,
    )?)?;

    let trader_id = TraderId::new_checked(config.runtime.node_name.as_str())
        .unwrap_or_else(|_| TraderId::new("CAPTURE-001"));
    // Capture writes parquet via the actor; do not retain market data in cache.
    // Bounded tick/bar deques match lightweight live strategy practice.
    let cache_config = CacheConfig {
        tick_capacity: 2_000,
        bar_capacity: 64,
        save_market_data: false,
        ..Default::default()
    };
    let mut builder = LiveNode::builder(trader_id, Environment::Live)?
        .with_name(config.runtime.node_name.as_str())
        .with_cache_config(cache_config)
        .with_timeout_connection(60)
        .with_delay_post_stop_secs(config.runtime.delay_post_stop_secs);

    for venue in &config.venues {
        match venue {
            #[cfg(feature = "venue-binance")]
            VenueRuntimeConfig::BinanceFutures {
                id,
                environment,
                product_type,
            } => {
                let creds = binance_credentials(id);
                let load_ids = instrument_id_strings_for_venue(&plan, "BINANCE");
                log::info!(
                    "Configuring venue {} ({product_type:?}, {environment:?}, credentials={}, load_ids={})",
                    id,
                    if api_key_secret_present(&creds) {
                        "from_env"
                    } else {
                        "public"
                    },
                    if load_ids.is_empty() {
                        "all".to_string()
                    } else {
                        format!("{}", load_ids.len())
                    }
                );
                let instrument_provider = if load_ids.is_empty() {
                    nautilus_binance::config::BinanceInstrumentProviderConfig::default()
                } else {
                    nautilus_binance::config::BinanceInstrumentProviderConfig {
                        load_all: false,
                        load_ids: Some(load_ids),
                        ..Default::default()
                    }
                };
                builder = builder.add_data_client(
                    None,
                    Box::new(BinanceDataClientFactory::new()),
                    Box::new(BinanceDataClientConfig {
                        product_type: *product_type,
                        environment: *environment,
                        api_key: creds.api_key,
                        api_secret: creds.api_secret,
                        instrument_provider,
                        ..Default::default()
                    }),
                )?;
            }
            #[cfg(feature = "venue-deribit")]
            VenueRuntimeConfig::Deribit {
                id,
                environment,
                product_types,
            } => {
                let creds = deribit_credentials(id);
                log::info!(
                    "Configuring venue {} (product_types={product_types:?}, {environment:?}, credentials={})",
                    id,
                    if api_key_secret_present(&creds) {
                        "from_env"
                    } else {
                        "public"
                    }
                );
                builder = builder.add_data_client(
                    None,
                    Box::new(DeribitDataClientFactory::new()),
                    Box::new(DeribitDataClientConfig {
                        environment: *environment,
                        product_types: product_types.clone(),
                        api_key: creds.api_key,
                        api_secret: creds.api_secret,
                        ..Default::default()
                    }),
                )?;
            }
            #[cfg(feature = "venue-bybit")]
            VenueRuntimeConfig::Bybit {
                id,
                environment,
                product_types,
            } => {
                let creds = bybit_credentials(id);
                log::info!(
                    "Configuring venue {} (product_types={product_types:?}, {environment:?}, credentials={})",
                    id,
                    if api_key_secret_present(&creds) {
                        "from_env"
                    } else {
                        "public"
                    }
                );
                builder = builder.add_data_client(
                    None,
                    Box::new(BybitDataClientFactory::new()),
                    Box::new(BybitDataClientConfig {
                        environment: *environment,
                        product_types: product_types.clone(),
                        api_key: creds.api_key,
                        api_secret: creds.api_secret,
                        ..Default::default()
                    }),
                )?;
            }
            #[cfg(feature = "venue-hyperliquid")]
            VenueRuntimeConfig::Hyperliquid { id, environment } => {
                let private_key = hyperliquid_private_key(id);
                log::info!(
                    "Configuring venue {} ({environment:?}, credentials={})",
                    id,
                    if private_key.is_some() {
                        "from_env"
                    } else {
                        "public"
                    }
                );
                builder = builder.add_data_client(
                    None,
                    Box::new(HyperliquidDataClientFactory::new()),
                    Box::new(HyperliquidDataClientConfig {
                        environment: *environment,
                        private_key,
                        // Prefer frequent instrument refresh when rolling outcome markets.
                        update_instruments_interval_mins: 1,
                        stale_stream_receive_timeout_secs: 90,
                        stream_health_check_interval_secs: 15,
                        ..Default::default()
                    }),
                )?;
            }
            #[cfg(feature = "venue-okx")]
            VenueRuntimeConfig::Okx {
                id,
                environment,
                instrument_types,
                instrument_families,
            } => {
                let creds = okx_credentials(id);
                log::info!(
                    "Configuring venue {} (instrument_types={instrument_types:?}, families={instrument_families:?}, {environment:?}, credentials={})",
                    id,
                    if creds.api_key.is_some() || creds.api_secret.is_some() || creds.api_passphrase.is_some() {
                        "from_env"
                    } else {
                        "public"
                    }
                );
                builder = builder.add_data_client(
                    None,
                    Box::new(OKXDataClientFactory::new()),
                    Box::new(OKXDataClientConfig {
                        environment: *environment,
                        instrument_types: instrument_types.clone(),
                        instrument_families: instrument_families.clone(),
                        api_key: creds.api_key,
                        api_secret: creds.api_secret,
                        api_passphrase: creds.api_passphrase,
                        ..Default::default()
                    }),
                )?;
            }
        }
    }

    let mut node = builder.build()?;
    node.add_actor(capture_actor)?;

    log::info!("Starting catalog capture");
    log::info!("Catalog dir: {}", catalog_dir.display());
    if config.runtime.capture_seconds == 0 {
        log::info!("Capture duration: until shutdown signal (capture_seconds=0)");
    } else {
        log::info!("Capture duration: {}s", config.runtime.capture_seconds);
    }
    log::info!("Venues: {}", config.venues.len());

    let metrics_server = if let Some(snapshot) = metrics_snapshot {
        let server = spawn_metrics_server(&config.runtime.metrics, snapshot)?;
        log::info!(
            "Metrics export: http://{}:{}/metrics (json: /metrics.json, health: /health)",
            config.runtime.metrics.bind_addr,
            config.runtime.metrics.port
        );
        Some(server)
    } else {
        None
    };

    let stop_handle = node.handle();
    let capture_seconds = config.runtime.capture_seconds;
    tokio::spawn(async move {
        wait_for_capture_shutdown(capture_seconds).await;
        stop_handle.stop();
    });

    node.run().await?;

    if let Some((handle, shutdown_tx)) = metrics_server {
        let _ = shutdown_tx.send(());
        handle.abort();
    }

    log::info!("Capture completed");
    log::info!("Catalog dir: {}", catalog_dir.display());
    run_option_universe_post_run_report(&catalog_dir, &config, &post_run)?;
    Ok(())
}

fn persist_startup_resolution_metadata(
    catalog_dir: &Path,
    config: &EffectiveConfig,
    option_reports: &[OptionUniverseResolutionReport],
    hip4_reports: &[Hip4UniverseResolutionReport],
    hip4_resolved: &[ResolvedHip4Universe],
) -> Result<()> {
    if !option_reports.is_empty() {
        let records = option_reports
            .iter()
            .map(startup_resolution_record_from_report)
            .collect::<Vec<_>>();
        append_option_universe_resolution_records(catalog_dir, &records)
            .with_context(|| "failed to persist startup option universe resolution metadata")?;
    }

    if !hip4_reports.is_empty() {
        let records = hip4_reports
            .iter()
            .zip(config.hip4_universes.iter())
            .zip(hip4_resolved.iter())
            .map(|((report, spec), resolved)| hip4_startup_record(report, spec, resolved))
            .collect::<Vec<_>>();
        append_hip4_universe_resolution_records(catalog_dir, &records)
            .with_context(|| "failed to persist startup HIP-4 universe resolution metadata")?;
    }

    Ok(())
}

fn persist_capture_run_metadata(
    catalog_dir: &Path,
    config: &EffectiveConfig,
    plan: &CapturePlan,
) -> Result<()> {
    let venues = config
        .venues
        .iter()
        .map(|venue| CaptureRunVenueRecord {
            id: venue.id().to_string(),
            kind: venue_kind_label(venue).to_string(),
        })
        .collect();
    let nautilus_trader_ref = std::env::var("NAUTILUS_TRADER_REF")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let record = new_capture_run_record(CaptureRunInput {
        node_name: config.runtime.node_name.clone(),
        catalog_uri: config.capture.catalog_uri.clone(),
        layout_compatibility: layout_compatibility_label(&config.capture.layout_compatibility)
            .to_string(),
        capture_seconds: config.runtime.capture_seconds,
        venues,
        plan,
        option_universe_count: config.option_universes.len(),
        hip4_universe_count: config.hip4_universes.len(),
        nautilus_trader_ref,
        cli_venue_features: compiled_venue_features(),
    });
    write_capture_run_record(catalog_dir, &record)
        .with_context(|| "failed to persist metadata/capture_run.json")?;
    log::info!(
        "Wrote capture run metadata: {}",
        catalog_dir.join("metadata/capture_run.json").display()
    );
    Ok(())
}

fn layout_compatibility_label(value: &LayoutCompatibility) -> &'static str {
    match value {
        LayoutCompatibility::RustCanonicalOnly => "rust_canonical_only",
    }
}

fn venue_kind_label(venue: &VenueRuntimeConfig) -> &'static str {
    match venue {
        #[cfg(feature = "venue-binance")]
        VenueRuntimeConfig::BinanceFutures { .. } => "binance_futures",
        #[cfg(feature = "venue-deribit")]
        VenueRuntimeConfig::Deribit { .. } => "deribit",
        #[cfg(feature = "venue-bybit")]
        VenueRuntimeConfig::Bybit { .. } => "bybit",
        #[cfg(feature = "venue-hyperliquid")]
        VenueRuntimeConfig::Hyperliquid { .. } => "hyperliquid",
        #[cfg(feature = "venue-okx")]
        VenueRuntimeConfig::Okx { .. } => "okx",
    }
}

fn compiled_venue_features() -> Vec<String> {
    [
        #[cfg(feature = "venue-binance")]
        "venue-binance",
        #[cfg(feature = "venue-bybit")]
        "venue-bybit",
        #[cfg(feature = "venue-deribit")]
        "venue-deribit",
        #[cfg(feature = "venue-okx")]
        "venue-okx",
        #[cfg(feature = "venue-hyperliquid")]
        "venue-hyperliquid",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn build_metrics_runtime_state(
    config: &EffectiveConfig,
) -> (Option<Arc<RwLock<CaptureMetricsSnapshot>>>, Option<u64>) {
    if !config.runtime.metrics.enabled {
        return (None, None);
    }

    (
        Some(Arc::new(RwLock::new(CaptureMetricsSnapshot::default()))),
        Some(config.runtime.metrics.refresh_interval_secs),
    )
}

fn build_capture_actor_config(
    config: &EffectiveConfig,
    plan: &CapturePlan,
    option_reports: &[OptionUniverseResolutionReport],
    hip4_reports: &[Hip4UniverseResolutionReport],
    hip4_resolved: &[ResolvedHip4Universe],
    metrics_snapshot: Option<Arc<RwLock<CaptureMetricsSnapshot>>>,
    metrics_refresh_interval_secs: Option<u64>,
) -> Result<CatalogCaptureActorConfig> {
    Ok(CatalogCaptureActorConfig {
        actor_id: Some(ActorId::from("CATALOG_CAPTURE-CLI")),
        capture: config.capture.clone(),
        plan: plan.clone(),
        online_option_metrics: build_online_option_metrics_config(config, plan, option_reports)?,
        dynamic_option_universe: build_dynamic_option_universe_config(
            config,
            plan,
            option_reports,
        )?,
        dynamic_hip4_universe: build_dynamic_hip4_universe_config(
            config,
            plan,
            hip4_reports,
            hip4_resolved,
        )?,
        metrics_snapshot,
        metrics_refresh_interval_secs,
    })
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
    validate_runtime_switches(config)?;
    validate_runtime_dependencies(config)?;
    let _ = resolve_catalog_dir(&config.capture.catalog_uri)?;
    Ok(())
}

fn validate_runtime_switches(config: &EffectiveConfig) -> Result<()> {
    ensure_positive_u64(
        config.runtime.shutdown_timeout_secs,
        "runtime.shutdown_timeout_secs must be > 0",
    )?;
    if config.runtime.online_option_metrics.enabled {
        ensure_positive_u64(
            config.runtime.online_option_metrics.snapshot_interval_secs,
            "runtime.online_option_metrics.snapshot_interval_secs must be > 0",
        )?;
    }
    if config.runtime.option_universe_refresh.enabled {
        ensure_positive_u64(
            config.runtime.option_universe_refresh.interval_secs,
            "runtime.option_universe_refresh.interval_secs must be > 0",
        )?;
    }
    if config.runtime.hip4_universe_refresh.enabled {
        ensure_positive_u64(
            config.runtime.hip4_universe_refresh.idle_poll_secs,
            "runtime.hip4_universe_refresh.idle_poll_secs must be > 0",
        )?;
        ensure_positive_u64(
            config.runtime.hip4_universe_refresh.active_poll_secs,
            "runtime.hip4_universe_refresh.active_poll_secs must be > 0",
        )?;
        ensure_positive_u64(
            config.runtime.hip4_universe_refresh.http_timeout_secs,
            "runtime.hip4_universe_refresh.http_timeout_secs must be > 0",
        )?;
    }
    if config.runtime.metrics.enabled {
        ensure_positive_u16(
            config.runtime.metrics.port,
            "runtime.metrics.port must be > 0 when runtime.metrics.enabled = true",
        )?;
        ensure_positive_u64(
            config.runtime.metrics.refresh_interval_secs,
            "runtime.metrics.refresh_interval_secs must be > 0 when runtime.metrics.enabled = true",
        )?;
    }
    Ok(())
}

fn validate_runtime_dependencies(config: &EffectiveConfig) -> Result<()> {
    if config.venues.is_empty() {
        bail!("at least one venue is required");
    }
    validate_option_universes(&config.option_universes, &config.venues)?;
    validate_hip4_universes(&config.hip4_universes, &config.venues)?;
    if config.runtime.hip4_universe_refresh.enabled && config.hip4_universes.is_empty() {
        bail!("runtime.hip4_universe_refresh.enabled requires capture.hip4_universe entries");
    }
    for spec in &config.plan.custom_data {
        validate_subscribe_data_type(&spec.data_type, &config.venues)?;
    }
    for spec in &config.plan.custom_data_requests {
        validate_request_data_type(&spec.data_type, &config.venues)?;
    }
    Ok(())
}

fn ensure_positive_u64(value: u64, error: &str) -> Result<()> {
    if value == 0 {
        bail!("{error}");
    }
    Ok(())
}

fn ensure_positive_u16(value: u16, error: &str) -> Result<()> {
    if value == 0 {
        bail!("{error}");
    }
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
        let entry = build_dynamic_option_universe_entry(config, plan, spec, report)?;
        initial_dynamic_plan = merge_capture_plans(&initial_dynamic_plan, &entry.initial_plan);
        universes.push(entry);
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

fn build_dynamic_hip4_universe_config(
    config: &EffectiveConfig,
    plan: &CapturePlan,
    reports: &[Hip4UniverseResolutionReport],
    resolved_entries: &[ResolvedHip4Universe],
) -> Result<Option<DynamicHip4UniverseConfig>> {
    if !config.runtime.hip4_universe_refresh.enabled {
        return Ok(None);
    }
    if reports.is_empty() {
        bail!("runtime.hip4_universe_refresh.enabled requires capture.hip4_universe entries");
    }

    let refresh = &config.runtime.hip4_universe_refresh;
    let mut initial_dynamic_plan = CapturePlan::default();
    let mut universes = Vec::with_capacity(reports.len());

    for ((spec, report), resolved) in config
        .hip4_universes
        .iter()
        .zip(reports.iter())
        .zip(resolved_entries.iter())
    {
        let entry = build_dynamic_hip4_universe_entry(config, plan, spec, report, resolved)?;
        initial_dynamic_plan = merge_capture_plans(&initial_dynamic_plan, &entry.initial_plan);
        universes.push(entry);
    }

    Ok(Some(DynamicHip4UniverseConfig {
        idle_poll_secs: refresh.idle_poll_secs,
        active_poll_secs: refresh.active_poll_secs,
        pre_expiry_window_secs: refresh.pre_expiry_window_secs,
        http_timeout_secs: refresh.http_timeout_secs,
        purge_removed_instruments: refresh.purge_removed_instruments,
        static_plan: config.plan.clone(),
        initial_dynamic_plan,
        universes,
    }))
}

fn build_dynamic_option_universe_entry(
    config: &EffectiveConfig,
    plan: &CapturePlan,
    spec: &catalog_capture_core::OptionUniverseSpec,
    report: &OptionUniverseResolutionReport,
) -> Result<DynamicOptionUniverseEntryConfig> {
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
    ensure_dynamic_option_universe_runtime_inputs(plan, reference_perp)?;

    let initial_plan = expand_option_universe(spec, &resolved);
    Ok(DynamicOptionUniverseEntryConfig {
        venue,
        venue_kind,
        spec: spec.clone(),
        initial_plan,
        initial_resolved: resolved,
    })
}

fn ensure_dynamic_option_universe_runtime_inputs(
    plan: &CapturePlan,
    reference_perp: nautilus_model::identifiers::InstrumentId,
) -> Result<()> {
    if !plan_has_quotes(plan, reference_perp)
        && !plan_has_mark_prices(plan, reference_perp)
        && !plan_has_index_prices(plan, reference_perp)
    {
        bail!(
            "runtime.option_universe_refresh requires perp quote/mark/index capture for `{}`",
            reference_perp
        );
    }
    Ok(())
}

fn build_dynamic_hip4_universe_entry(
    config: &EffectiveConfig,
    plan: &CapturePlan,
    spec: &catalog_capture_core::Hip4UniverseSpec,
    report: &Hip4UniverseResolutionReport,
    resolved: &ResolvedHip4Universe,
) -> Result<DynamicHip4UniverseEntryConfig> {
    #[cfg(not(feature = "venue-hyperliquid"))]
    {
        let _ = (config, plan, spec, report, resolved);
        bail!(
            "capture.hip4_universe requires cargo feature `venue-hyperliquid` \
             (rebuild with `--features venue-hyperliquid` or `--features all-venues`)"
        );
    }
    #[cfg(feature = "venue-hyperliquid")]
    {
        let environment = hip4_report_environment(config, report)?;
        ensure_dynamic_hip4_universe_runtime_inputs(plan, spec, report)?;

        let initial_plan = expand_hip4_universe(spec, resolved);
        Ok(DynamicHip4UniverseEntryConfig {
            environment,
            spec: spec.clone(),
            initial_plan,
            initial_resolved: resolved.clone(),
        })
    }
}

#[cfg(feature = "venue-hyperliquid")]
fn ensure_dynamic_hip4_universe_runtime_inputs(
    plan: &CapturePlan,
    spec: &catalog_capture_core::Hip4UniverseSpec,
    report: &Hip4UniverseResolutionReport,
) -> Result<()> {
    if !spec.include_perp_mark {
        return Ok(());
    }

    let perp_instrument_id = report
        .perp_instrument_id
        .as_deref()
        .with_context(|| {
            format!(
                "runtime.hip4_universe_refresh requires resolved perp for venue_id `{}`",
                spec.venue_id
            )
        })?
        .parse()?;
    if !plan_has_mark_prices(plan, perp_instrument_id) {
        bail!(
            "runtime.hip4_universe_refresh requires perp mark_prices capture for `{perp_instrument_id}`"
        );
    }

    Ok(())
}

#[cfg(feature = "venue-hyperliquid")]
fn hip4_report_environment(
    config: &EffectiveConfig,
    report: &Hip4UniverseResolutionReport,
) -> Result<HyperliquidEnvironment> {
    let venue = config
        .venues
        .iter()
        .find(|entry| entry.id() == report.venue_id)
        .with_context(|| {
            format!(
                "capture.hip4_universe references unknown venue_id `{}`",
                report.venue_id
            )
        })?;
    match venue {
        VenueRuntimeConfig::Hyperliquid { environment, .. } => Ok(*environment),
        #[allow(unreachable_patterns)]
        _ => bail!(
            "runtime.hip4_universe_refresh requires hyperliquid venue for `{}`",
            report.venue_id
        ),
    }
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
        #[cfg(feature = "venue-deribit")]
        VenueRuntimeConfig::Deribit { .. } => Some(OptionUniverseVenueKind::Deribit),
        #[cfg(feature = "venue-bybit")]
        VenueRuntimeConfig::Bybit { .. } => Some(OptionUniverseVenueKind::Bybit),
        #[cfg(feature = "venue-okx")]
        VenueRuntimeConfig::Okx { .. } => Some(OptionUniverseVenueKind::Okx),
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

/// Instrument IDs in the capture plan that belong to `venue` (e.g. `"BINANCE"`).
fn instrument_id_strings_for_venue(plan: &CapturePlan, venue: &str) -> Vec<String> {
    let mut ids = std::collections::BTreeSet::new();
    for instrument_id in plan.planned_instrument_ids() {
        if instrument_id.venue.as_str() == venue {
            ids.insert(instrument_id.to_string());
        }
    }
    // book/trade/quote/mark families also pin instruments
    for spec in &plan.book_deltas {
        if spec.instrument_id.venue.as_str() == venue {
            ids.insert(spec.instrument_id.to_string());
        }
    }
    for spec in &plan.trades {
        if spec.instrument_id.venue.as_str() == venue {
            ids.insert(spec.instrument_id.to_string());
        }
    }
    for spec in &plan.quotes {
        if spec.instrument_id.venue.as_str() == venue {
            ids.insert(spec.instrument_id.to_string());
        }
    }
    for spec in &plan.mark_prices {
        if spec.instrument_id.venue.as_str() == venue {
            ids.insert(spec.instrument_id.to_string());
        }
    }
    ids.into_iter().collect()
}

fn log_capture_buffer_estimate(config: &EffectiveConfig, plan: &CapturePlan) {
    let estimate = estimate_peak_buffered_bytes(plan, &config.capture);
    log::info!("{}", format_buffer_estimate(&estimate));
    if let Some(budget_bytes) = config.runtime.resource_budget_bytes {
        if estimate.total_peak_buffered_bytes > budget_bytes {
            log::warn!("{}", format_budget_warning(&estimate, budget_bytes));
        }
    }
}

fn resolve_catalog_dir(catalog_uri: &str) -> Result<PathBuf> {
    let path = catalog_uri.strip_prefix("file://").unwrap_or(catalog_uri);
    if path.is_empty() {
        bail!("output.catalog_uri cannot be empty");
    }
    Ok(PathBuf::from(path))
}
