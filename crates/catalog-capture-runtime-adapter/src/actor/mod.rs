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

mod lifecycle;
mod lighter;
mod predict;
mod submit;

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use anyhow::Result;
use catalog_capture_core::{
    background::BackgroundCaptureRuntime,
    catalog_root_from_uri,
    config::CaptureConfig,
    flush_profile::CaptureFlushFamily,
    item::PartitionKey,
    metrics::CaptureMetrics,
    metrics_export::{
        CaptureMetricsSnapshot, CustomDataRequestJobMetrics, FamilyCaptureMetrics,
        process_rss_bytes, unix_time_ms,
    },
    plan::CapturePlan,
    sink::{
        BookDeltaCatalogSink, CatalogSink, ChunkedCatalogSink, CustomDataCatalogSink,
        chunked_catalog_sink_from_config, custom_data_catalog_sink_from_config,
    },
};
use nautilus_common::{
    actor::{DataActorConfig, DataActorCore, DataActorNative},
    nautilus_actor,
};
#[cfg(test)]
use nautilus_model::orderbook::OrderBook;
use nautilus_model::{
    data::{
        Bar, CustomData, DataType, FundingRateUpdate, IndexPriceUpdate, InstrumentStatus,
        MarkPriceUpdate, OptionGreeks, OrderBookDelta, OrderBookDeltas, OrderBookDepth, QuoteTick,
        TradeTick, close::InstrumentClose,
    },
    identifiers::{ActorId, ClientId, InstrumentId},
    instruments::{Instrument, InstrumentAny},
};
use nautilus_predict::common::{
    CryptoUpDownSelectorKey, PREDICT_SELECTOR_INTERVAL_SECS, PREDICT_SELECTOR_PRICE_FEED_SYMBOL,
    PREDICT_SELECTOR_TITLE_ASSET,
};

use crate::{
    actor_plan::{
        count_enabled_background_workers, effective_capture_plan, supplemental_capture_plan,
    },
    actor_runtime::{collect_family_metrics, maybe_family_runtime},
    custom_data_requests::CustomDataRequestJob,
    dynamic_hip4_universe::{DynamicHip4UniverseConfig, DynamicHip4UniverseManager},
    dynamic_option_universe::{DynamicOptionUniverseConfig, DynamicOptionUniverseManager},
    online_option_metrics::{OnlineOptionMetricsConfig, OnlineOptionMetricsObserver},
};
const OPTION_UNIVERSE_REFRESH_TIMER: &str = "OPTION_UNIVERSE_REFRESH";
const HIP4_UNIVERSE_REFRESH_TIMER: &str = "HIP4_UNIVERSE_REFRESH";
const SEGMENT_SEAL_TIMER: &str = "SEGMENT_SEAL";
const METRICS_EXPORT_TIMER: &str = "METRICS_EXPORT";
/// One-shot timer name for post-roll deferred market-data (only while pending non-empty).
const PENDING_MARKET_DATA_TIMER: &str = "PENDING_MARKET_DATA";
/// First re-request interval after a roll when instrument is not yet cached (seconds).
const PENDING_MD_BACKOFF_START_SECS: u64 = 1;
/// Cap on re-request poll interval only — total wait is unbounded until cache-ready or roll-clear.
const PENDING_MD_BACKOFF_MAX_SECS: u64 = 60;
#[derive(Debug, Clone)]
pub struct CatalogCaptureActorConfig {
    pub actor_id: Option<ActorId>,
    pub capture: CaptureConfig,
    pub plan: CapturePlan,
    pub online_option_metrics: Option<OnlineOptionMetricsConfig>,
    pub dynamic_option_universe: Option<DynamicOptionUniverseConfig>,
    pub dynamic_hip4_universe: Option<DynamicHip4UniverseConfig>,
    pub lighter_updown: Vec<crate::LighterUpDownCaptureConfig>,
    /// Explicit routes needed when Binance products share venue `BINANCE`.
    ///
    /// The config resolver owns this map: the actor must never infer product
    /// type from a symbol spelling at subscription time.
    pub binance_client_routes: BTreeMap<InstrumentId, ClientId>,
    /// Futures custom-data types have no `InstrumentId`, but still need the
    /// configured client when Spot and Futures share the `BINANCE` venue.
    pub binance_futures_client_id: Option<ClientId>,
    pub metrics_snapshot: Option<Arc<RwLock<CaptureMetricsSnapshot>>>,
    pub metrics_refresh_interval_secs: Option<u64>,
}

impl CatalogCaptureActorConfig {
    #[must_use]
    pub fn new(capture: CaptureConfig, plan: CapturePlan) -> Self {
        Self {
            actor_id: None,
            capture,
            plan,
            online_option_metrics: None,
            dynamic_option_universe: None,
            dynamic_hip4_universe: None,
            lighter_updown: Vec::new(),
            binance_client_routes: BTreeMap::new(),
            binance_futures_client_id: None,
            metrics_snapshot: None,
            metrics_refresh_interval_secs: None,
        }
    }
}

pub struct CatalogCaptureActor {
    core: DataActorCore,
    lighter: crate::lighter_updown::LighterCaptureState,
    capture: CaptureConfig,
    /// Startup materialized plan (static TOML + one-shot universe expansion).
    initial_materialized_plan: CapturePlan,
    /// Capture specs owned by startup materialization but not by an active refresh manager.
    supplemental_plan: CapturePlan,
    plan: CapturePlan,
    instrument_runtime: Option<BackgroundCaptureRuntime<InstrumentAny, ChunkedCatalogSink>>,
    pending_instrument_definitions: BTreeSet<InstrumentId>,
    instrument_failure_cursor: u64,
    /// Subscribe + request custom payloads share one writer. Segment mode uses
    /// `*.parquet.part` + wall-clock seal (same as market data); chunked mode keeps
    /// immediate catalog parquet files.
    custom_data_runtime: Option<BackgroundCaptureRuntime<CustomData, CustomDataCatalogSink>>,
    /// Predict rolling selectors own independent segment writers, keyed by full product identity.
    predict_custom_data_runtimes: BTreeMap<
        CryptoUpDownSelectorKey,
        BackgroundCaptureRuntime<CustomData, CustomDataCatalogSink>,
    >,
    /// Last accepted market for each Predict rolling selector. A successor's first accepted
    /// snapshot is the authoritative rotation boundary for its writer.
    active_predict_market_ids: BTreeMap<CryptoUpDownSelectorKey, u64>,
    retiring_predict_market_ids: BTreeSet<u64>,
    mark_price_runtime:
        Option<BackgroundCaptureRuntime<MarkPriceUpdate, CatalogSink<MarkPriceUpdate>>>,
    index_price_runtime:
        Option<BackgroundCaptureRuntime<IndexPriceUpdate, CatalogSink<IndexPriceUpdate>>>,
    funding_rate_runtime:
        Option<BackgroundCaptureRuntime<FundingRateUpdate, CatalogSink<FundingRateUpdate>>>,
    instrument_status_runtime:
        Option<BackgroundCaptureRuntime<InstrumentStatus, CatalogSink<InstrumentStatus>>>,
    instrument_close_runtime:
        Option<BackgroundCaptureRuntime<InstrumentClose, CatalogSink<InstrumentClose>>>,
    option_greeks_runtime:
        Option<BackgroundCaptureRuntime<OptionGreeks, CatalogSink<OptionGreeks>>>,
    forward_price_targets: BTreeSet<InstrumentId>,
    quote_runtime: Option<BackgroundCaptureRuntime<QuoteTick, CatalogSink<QuoteTick>>>,
    trade_runtime: Option<BackgroundCaptureRuntime<TradeTick, CatalogSink<TradeTick>>>,
    bar_runtime: Option<BackgroundCaptureRuntime<Bar, CatalogSink<Bar>>>,
    book_depth_runtime:
        Option<BackgroundCaptureRuntime<OrderBookDepth, CatalogSink<OrderBookDepth>>>,
    predict_native_subscriptions:
        BTreeMap<CryptoUpDownSelectorKey, predict::PredictNativeSubscription>,
    book_delta_runtime: Option<BackgroundCaptureRuntime<OrderBookDeltas, BookDeltaCatalogSink>>,
    book_delta_recovery: BTreeSet<InstrumentId>,
    book_delta_failure_cursor: u64,
    binance_client_routes: BTreeMap<InstrumentId, ClientId>,
    binance_futures_client_id: Option<ClientId>,
    online_option_metrics: Option<OnlineOptionMetricsObserver>,
    dynamic_option_universe: Option<DynamicOptionUniverseManager>,
    dynamic_hip4_universe: Option<DynamicHip4UniverseManager>,
    /// Request-style custom data jobs only (`request_data`). Never mixed with subscribe.
    custom_data_request_jobs: Vec<CustomDataRequestJob>,
    /// Post-roll only: market-data subs waiting for instrument to enter cache.
    /// Empty most of the day — no timer / re-request work while empty.
    pending_market_data: BTreeSet<InstrumentId>,
    /// Instrument IDs that already received market-data subscribe commands.
    market_data_live: BTreeSet<InstrumentId>,
    /// Adaptive re-request backoff (seconds) while `pending_market_data` is non-empty.
    pending_market_data_backoff_secs: u64,
    metrics_snapshot: Option<Arc<RwLock<CaptureMetricsSnapshot>>>,
    metrics_refresh_interval_secs: Option<u64>,
    catalog_root: PathBuf,
    shutdown_completed: bool,
    failure: Option<String>,
    worker_shutdown_requested: bool,
}

impl CatalogCaptureActor {
    pub fn new(config: CatalogCaptureActorConfig) -> Result<Self> {
        let actor_config = DataActorConfig {
            actor_id: Some(
                config
                    .actor_id
                    .unwrap_or_else(|| ActorId::from("CATALOG_CAPTURE-001")),
            ),
            ..Default::default()
        };

        for selector in &config.lighter_updown {
            selector.validate()?;
        }
        anyhow::ensure!(
            config
                .lighter_updown
                .iter()
                .map(|c| &c.tag)
                .collect::<BTreeSet<_>>()
                .len()
                == config.lighter_updown.len(),
            "duplicate Lighter series"
        );
        anyhow::ensure!(
            config.lighter_updown.len() <= 2,
            "at most two Lighter series supported"
        );
        anyhow::ensure!(
            config.lighter_updown.is_empty() || config.capture.lifecycle.is_segment_mode(),
            "rolling Lighter capture requires segment mode"
        );
        let rolling_lighter = !config.lighter_updown.is_empty();
        let flags = config.plan.family_runtime_flags();
        let predict_selectors = predict_selector_keys(&config.plan)?;
        // A rolling Predict selector discovers both BinaryOption definitions at runtime.
        // Its plan therefore has no static InstrumentId, but definitions are still required
        // for replay before the first orderbook snapshot is written.
        let captures_dynamic_predict_instruments = !predict_selectors.is_empty();
        let worker_count = config.plan.enabled_background_worker_count()
            + usize::from(captures_dynamic_predict_instruments && !flags.instruments)
            + if rolling_lighter {
                usize::from(!flags.instruments && !captures_dynamic_predict_instruments)
                    + usize::from(!flags.trades)
                    + usize::from(!flags.book_deltas)
                    + usize::from(!flags.index_prices)
                    + usize::from(!flags.custom_data && !flags.custom_data_requests)
            } else {
                0
            };
        log::info!("Capture background workers: {worker_count} enabled for plan");

        // Instruments: always chunked. Custom: segment when mode=segment (shared for
        // subscribe + request). Market families: CatalogSink + per-family flush profile.
        let capture = config.capture.clone();

        let instrument_runtime = maybe_family_runtime(
            flags.instruments || captures_dynamic_predict_instruments || rolling_lighter,
            CaptureFlushFamily::Instruments,
            &capture,
            chunked_catalog_sink_from_config,
        )?;
        let custom_data_runtime = maybe_family_runtime(
            needs_generic_custom_data_writer(&config.plan) || rolling_lighter,
            CaptureFlushFamily::CustomData,
            &capture,
            custom_data_catalog_sink_from_config,
        )?;
        let snapshot_selectors = config
            .plan
            .custom_data
            .iter()
            .filter(|spec| {
                spec.data_type.type_name() == "PredictCryptoUpDown" && spec.records_payload()
            })
            .map(|spec| predict_selector_key(&spec.data_type))
            .collect::<Result<BTreeSet<_>>>()?;
        let predict_custom_data_runtimes = snapshot_selectors
            .into_iter()
            .map(|selector_key| {
                let runtime = maybe_family_runtime(
                    true,
                    CaptureFlushFamily::CustomData,
                    &capture,
                    custom_data_catalog_sink_from_config,
                )?
                .expect("enabled Predict custom writer must be created");
                Ok((selector_key, runtime))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let custom_data_request_jobs = config
            .plan
            .custom_data_requests
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, spec)| CustomDataRequestJob::new(index, spec))
            .collect();

        let mark_price_runtime = maybe_family_runtime(
            flags.mark_prices,
            CaptureFlushFamily::MarkPrices,
            &capture,
            CatalogSink::<MarkPriceUpdate>::from_config,
        )?;
        let index_price_runtime = maybe_family_runtime(
            flags.index_prices || rolling_lighter,
            CaptureFlushFamily::IndexPrices,
            &capture,
            CatalogSink::<IndexPriceUpdate>::from_config,
        )?;
        let funding_rate_runtime = maybe_family_runtime(
            flags.funding_rates,
            CaptureFlushFamily::FundingRates,
            &capture,
            CatalogSink::<FundingRateUpdate>::from_config,
        )?;
        let instrument_status_runtime = maybe_family_runtime(
            flags.instrument_statuses,
            CaptureFlushFamily::InstrumentStatus,
            &capture,
            CatalogSink::<InstrumentStatus>::from_config,
        )?;
        let instrument_close_runtime = maybe_family_runtime(
            flags.instrument_closes,
            CaptureFlushFamily::InstrumentClose,
            &capture,
            CatalogSink::<InstrumentClose>::from_config,
        )?;
        let option_greeks_runtime = maybe_family_runtime(
            flags.option_greeks,
            CaptureFlushFamily::OptionGreeks,
            &capture,
            CatalogSink::<OptionGreeks>::from_config,
        )?;
        let quote_runtime = maybe_family_runtime(
            flags.quotes,
            CaptureFlushFamily::Quotes,
            &capture,
            CatalogSink::<QuoteTick>::from_config,
        )?;
        let trade_runtime = maybe_family_runtime(
            flags.trades || rolling_lighter,
            CaptureFlushFamily::Trades,
            &capture,
            CatalogSink::<TradeTick>::from_config,
        )?;
        let bar_runtime = maybe_family_runtime(
            flags.bars,
            CaptureFlushFamily::Bars,
            &capture,
            CatalogSink::<Bar>::from_config,
        )?;
        let book_depth_runtime = maybe_family_runtime(
            flags.book_depth,
            CaptureFlushFamily::BookDepth,
            &capture,
            CatalogSink::<OrderBookDepth>::from_config,
        )?;
        let book_delta_runtime = maybe_family_runtime(
            flags.book_deltas || rolling_lighter,
            CaptureFlushFamily::BookDeltas,
            &capture,
            BookDeltaCatalogSink::from_config,
        )?;
        let catalog_root = catalog_root_from_uri(&config.capture.catalog_uri)?;
        let initial_materialized_plan = config.plan.clone();
        let supplemental_plan = supplemental_capture_plan(
            &initial_materialized_plan,
            &config.dynamic_option_universe,
            &config.dynamic_hip4_universe,
        );
        let forward_price_targets = initial_materialized_plan
            .forward_prices
            .iter()
            .map(|spec| spec.instrument_id)
            .collect();

        Ok(Self {
            core: DataActorCore::new(actor_config),
            lighter: crate::lighter_updown::LighterCaptureState {
                configs: config.lighter_updown,
                ..Default::default()
            },
            capture: config.capture.clone(),
            initial_materialized_plan,
            supplemental_plan,
            plan: config.plan,
            instrument_runtime,
            pending_instrument_definitions: BTreeSet::new(),
            instrument_failure_cursor: 0,
            custom_data_runtime,
            predict_custom_data_runtimes,
            active_predict_market_ids: BTreeMap::new(),
            retiring_predict_market_ids: BTreeSet::new(),
            mark_price_runtime,
            index_price_runtime,
            funding_rate_runtime,
            instrument_status_runtime,
            instrument_close_runtime,
            option_greeks_runtime,
            forward_price_targets,
            quote_runtime,
            trade_runtime,
            bar_runtime,
            book_delta_runtime,
            book_delta_recovery: BTreeSet::new(),
            book_delta_failure_cursor: 0,
            book_depth_runtime,
            predict_native_subscriptions: BTreeMap::new(),
            binance_client_routes: config.binance_client_routes,
            binance_futures_client_id: config.binance_futures_client_id,
            online_option_metrics: config
                .online_option_metrics
                .map(OnlineOptionMetricsObserver::new),
            dynamic_option_universe: config
                .dynamic_option_universe
                .map(DynamicOptionUniverseManager::new),
            dynamic_hip4_universe: config
                .dynamic_hip4_universe
                .map(DynamicHip4UniverseManager::new),
            custom_data_request_jobs,
            pending_market_data: BTreeSet::new(),
            market_data_live: BTreeSet::new(),
            pending_market_data_backoff_secs: PENDING_MD_BACKOFF_START_SECS,
            metrics_snapshot: config.metrics_snapshot,
            metrics_refresh_interval_secs: config.metrics_refresh_interval_secs,
            catalog_root,
            shutdown_completed: false,
            failure: None,
            worker_shutdown_requested: false,
        })
    }

    #[must_use]
    pub fn enabled_background_worker_count(&self) -> usize {
        if self.shutdown_completed {
            return 0;
        }
        count_enabled_background_workers([
            self.instrument_runtime.is_some(),
            self.custom_data_runtime.is_some(),
            self.mark_price_runtime.is_some(),
            self.index_price_runtime.is_some(),
            self.funding_rate_runtime.is_some(),
            self.instrument_status_runtime.is_some(),
            self.instrument_close_runtime.is_some(),
            self.option_greeks_runtime.is_some(),
            self.quote_runtime.is_some(),
            self.trade_runtime.is_some(),
            self.bar_runtime.is_some(),
            self.book_delta_runtime.is_some(),
        ]) + usize::from(self.book_depth_runtime.is_some())
            + self.predict_custom_data_runtimes.len()
    }

    fn capture_metrics(&self) -> CaptureMetrics {
        self.metrics_snapshot_data().aggregated
    }

    #[must_use]
    pub fn metrics_snapshot_data(&self) -> CaptureMetricsSnapshot {
        let mut aggregated = CaptureMetrics::default();
        let mut families = Vec::new();
        collect_family_metrics(
            "instruments",
            &self.instrument_runtime,
            &mut families,
            &mut aggregated,
        );
        collect_family_metrics(
            "custom_data",
            &self.custom_data_runtime,
            &mut families,
            &mut aggregated,
        );
        for (selector_key, runtime) in &self.predict_custom_data_runtimes {
            let metrics = runtime.metrics();
            aggregated.merge(&metrics);
            families.push(FamilyCaptureMetrics {
                family: format!("predict_custom_data_{selector_key}"),
                failures: runtime.failure_snapshot(),
                metrics,
            });
        }
        collect_family_metrics(
            "mark_prices",
            &self.mark_price_runtime,
            &mut families,
            &mut aggregated,
        );
        collect_family_metrics(
            "index_prices",
            &self.index_price_runtime,
            &mut families,
            &mut aggregated,
        );
        collect_family_metrics(
            "funding_rates",
            &self.funding_rate_runtime,
            &mut families,
            &mut aggregated,
        );
        collect_family_metrics(
            "instrument_statuses",
            &self.instrument_status_runtime,
            &mut families,
            &mut aggregated,
        );
        collect_family_metrics(
            "instrument_closes",
            &self.instrument_close_runtime,
            &mut families,
            &mut aggregated,
        );
        collect_family_metrics(
            "option_greeks",
            &self.option_greeks_runtime,
            &mut families,
            &mut aggregated,
        );
        collect_family_metrics(
            "quotes",
            &self.quote_runtime,
            &mut families,
            &mut aggregated,
        );
        collect_family_metrics(
            "trades",
            &self.trade_runtime,
            &mut families,
            &mut aggregated,
        );
        collect_family_metrics("bars", &self.bar_runtime, &mut families, &mut aggregated);
        collect_family_metrics(
            "book_deltas",
            &self.book_delta_runtime,
            &mut families,
            &mut aggregated,
        );

        collect_family_metrics(
            "book_depth",
            &self.book_depth_runtime,
            &mut families,
            &mut aggregated,
        );

        let custom_data_requests = self
            .custom_data_request_jobs
            .iter()
            .map(|job| CustomDataRequestJobMetrics {
                index: job.index,
                type_name: job.spec.data_type.type_name().to_string(),
                identifier: job.spec.data_type.identifier().map(str::to_string),
                in_flight: job.in_flight,
                polls: job.polls,
                rows: job.rows,
                skipped_inflight: job.skipped_inflight,
                timeouts: job.timeouts,
            })
            .collect();

        CaptureMetricsSnapshot {
            captured_at_unix_ms: unix_time_ms(),
            enabled_background_workers: self.enabled_background_worker_count(),
            process_rss_bytes: process_rss_bytes(),
            aggregated,
            families,
            custom_data_requests,
            failure: self.failure.clone(),
        }
    }

    pub fn publish_metrics_snapshot(&self) {
        let Some(snapshot_store) = &self.metrics_snapshot else {
            return;
        };
        let snapshot = self.metrics_snapshot_data();
        if let Ok(mut store) = snapshot_store.write() {
            *store = snapshot;
        }
    }

    fn log_capture_metrics_summary(&self) {
        let metrics = self.capture_metrics();
        if metrics.accepted_items == 0 && metrics.completed_files == 0 {
            return;
        }
        log::info!("Capture metrics: {}", metrics.summary_line());
    }

    fn effective_capture_plan(&self) -> CapturePlan {
        let base = effective_capture_plan(
            &self.initial_materialized_plan,
            &self.supplemental_plan,
            self.dynamic_option_universe.as_ref(),
            self.dynamic_hip4_universe.as_ref(),
        );
        catalog_capture_core::merge_capture_plans(&base, &self.lighter.plan())
    }

    fn sync_plan_state(&mut self) {
        let plan = self.effective_capture_plan();
        self.sync_forward_price_targets(&plan);
        self.plan = plan;
    }

    fn sync_forward_price_targets(&mut self, plan: &CapturePlan) {
        self.forward_price_targets = plan
            .forward_prices
            .iter()
            .map(|spec| spec.instrument_id)
            .collect();
    }
}

fn predict_selector_keys(plan: &CapturePlan) -> Result<BTreeSet<CryptoUpDownSelectorKey>> {
    plan.custom_data
        .iter()
        .filter(|spec| spec.data_type.type_name() == "PredictCryptoUpDown")
        .map(|spec| predict_selector_key(&spec.data_type))
        .collect()
}

fn predict_selector_key(data_type: &DataType) -> Result<CryptoUpDownSelectorKey> {
    let metadata = data_type
        .metadata()
        .ok_or_else(|| anyhow::anyhow!("PredictCryptoUpDown is missing metadata"))?;
    let string = |key: &str| -> Result<&str> {
        metadata
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow::anyhow!("PredictCryptoUpDown is missing {key}"))
    };
    let interval_secs = string("interval_secs")?
        .parse::<u64>()
        .map_err(|_| anyhow::anyhow!("PredictCryptoUpDown interval_secs must be an integer"))?;
    CryptoUpDownSelectorKey::new(
        string("price_feed_symbol")?,
        string("title_asset")?,
        interval_secs,
    )
}

/// Returns the complete rolling-selector provenance carried by an emitted Predict snapshot.
///
/// Static snapshots deliberately return `None`; partial dynamic provenance is rejected instead
/// of being silently routed through the generic custom-data writer.
pub(super) fn predict_output_selector_key(
    data_type: &DataType,
) -> Result<Option<CryptoUpDownSelectorKey>> {
    if data_type.type_name() != "PredictOrderbookSnapshot" {
        return Ok(None);
    }
    let Some(metadata) = data_type.metadata() else {
        return Ok(None);
    };
    let has_selector_provenance = [
        PREDICT_SELECTOR_PRICE_FEED_SYMBOL,
        PREDICT_SELECTOR_TITLE_ASSET,
        PREDICT_SELECTOR_INTERVAL_SECS,
    ]
    .into_iter()
    .any(|key| metadata.contains_key(key));
    if !has_selector_provenance {
        return Ok(None);
    }
    let string = |key: &str| -> Result<&str> {
        metadata
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow::anyhow!("Predict snapshot is missing {key}"))
    };
    let interval_secs = string(PREDICT_SELECTOR_INTERVAL_SECS)?
        .parse::<u64>()
        .map_err(|_| anyhow::anyhow!("invalid Predict snapshot interval seconds"))?;
    Ok(Some(CryptoUpDownSelectorKey::new(
        string(PREDICT_SELECTOR_PRICE_FEED_SYMBOL)?,
        string(PREDICT_SELECTOR_TITLE_ASSET)?,
        interval_secs,
    )?))
}

/// Dynamic Predict selectors own selector-scoped writers. Every other custom
/// subscription or request continues through the ordinary shared writer.
fn needs_generic_custom_data_writer(plan: &CapturePlan) -> bool {
    !plan.custom_data_requests.is_empty()
        || plan
            .custom_data
            .iter()
            .any(|spec| spec.data_type.type_name() != "PredictCryptoUpDown")
}

nautilus_actor!(CatalogCaptureActor);

impl Debug for CatalogCaptureActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogCaptureActor")
            .field("plan", &self.plan)
            .field(
                "enabled_background_workers",
                &self.enabled_background_worker_count(),
            )
            .field(
                "instrument_queue_depth",
                &self
                    .instrument_runtime
                    .as_ref()
                    .map(BackgroundCaptureRuntime::queue_depth),
            )
            .field(
                "custom_data_queue_depth",
                &self
                    .custom_data_runtime
                    .as_ref()
                    .map(BackgroundCaptureRuntime::queue_depth),
            )
            .field(
                "mark_price_queue_depth",
                &self
                    .mark_price_runtime
                    .as_ref()
                    .map(BackgroundCaptureRuntime::queue_depth),
            )
            .field(
                "index_price_queue_depth",
                &self
                    .index_price_runtime
                    .as_ref()
                    .map(BackgroundCaptureRuntime::queue_depth),
            )
            .field(
                "funding_rate_queue_depth",
                &self
                    .funding_rate_runtime
                    .as_ref()
                    .map(BackgroundCaptureRuntime::queue_depth),
            )
            .field(
                "instrument_status_queue_depth",
                &self
                    .instrument_status_runtime
                    .as_ref()
                    .map(BackgroundCaptureRuntime::queue_depth),
            )
            .field(
                "instrument_close_queue_depth",
                &self
                    .instrument_close_runtime
                    .as_ref()
                    .map(BackgroundCaptureRuntime::queue_depth),
            )
            .field(
                "option_greeks_queue_depth",
                &self
                    .option_greeks_runtime
                    .as_ref()
                    .map(BackgroundCaptureRuntime::queue_depth),
            )
            .field(
                "quote_queue_depth",
                &self
                    .quote_runtime
                    .as_ref()
                    .map(BackgroundCaptureRuntime::queue_depth),
            )
            .field(
                "trade_queue_depth",
                &self
                    .trade_runtime
                    .as_ref()
                    .map(BackgroundCaptureRuntime::queue_depth),
            )
            .field(
                "bar_queue_depth",
                &self
                    .bar_runtime
                    .as_ref()
                    .map(BackgroundCaptureRuntime::queue_depth),
            )
            .field(
                "book_delta_queue_depth",
                &self
                    .book_delta_runtime
                    .as_ref()
                    .map(BackgroundCaptureRuntime::queue_depth),
            )
            .field(
                "online_option_metrics",
                &self.online_option_metrics.is_some(),
            )
            .finish()
    }
}

pub trait RuntimeCaptureAdapter {
    fn build_capture_actor(&self) -> Result<CatalogCaptureActor>;
}

impl Drop for CatalogCaptureActor {
    fn drop(&mut self) {
        if !self.shutdown_completed {
            let _ = self.shutdown_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, fs, rc::Rc};

    use catalog_capture_core::{
        CaptureConfig,
        plan::{CapturePlan, CustomDataCaptureSpec, QuoteCaptureSpec},
    };
    use nautilus_common::{
        actor::{Component, DataActor},
        cache::Cache,
        clock::VirtualClock,
    };
    use nautilus_core::{Params, UnixNanos};
    use nautilus_model::{
        data::{DataType, QuoteTick},
        enums::AssetClass,
        identifiers::{ClientId, InstrumentId, Symbol, TraderId},
        instruments::{BinaryOption, InstrumentAny, stubs::audusd_sim},
        stubs::TestDefault,
        types::{Currency, Price, Quantity},
    };
    use ustr::Ustr;

    use super::*;
    use crate::DynamicHip4UniverseChange;

    fn test_quote(instrument_id: InstrumentId) -> QuoteTick {
        QuoteTick::new(
            instrument_id,
            Price::from("1.0"),
            Price::from("1.1"),
            Quantity::from("1"),
            Quantity::from("1"),
            UnixNanos::default(),
            UnixNanos::default(),
        )
    }

    #[test]
    fn admission_gaps_preserve_liveness_and_final_quality_state() {
        use catalog_capture_core::plan::{BookDepthCaptureSpec, TradeCaptureSpec};
        use nautilus_common::{
            messages::system::ShutdownSystem,
            msgbus::{self, MessagingSwitchboard, typed_handler::ShareableMessageHandler},
        };
        use nautilus_model::{
            enums::{AggressorSide, BookType},
            identifiers::TradeId,
        };

        let root =
            std::env::temp_dir().join(format!("capture-admission-{}", nautilus_core::UUID4::new()));
        fs::create_dir_all(&root).unwrap();
        let id = InstrumentId::from("42-1.PREDICT");
        let store = Arc::new(RwLock::new(CaptureMetricsSnapshot::default()));
        let mut config = CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", root.display()),
                max_buffer_bytes: 4096,
                max_total_buffer_bytes: 4096,
                overflow_policy: catalog_capture_core::OverflowPolicy::FailFast,
                ..Default::default()
            },
            CapturePlan {
                trades: vec![TradeCaptureSpec { instrument_id: id }],
                book_depth: vec![BookDepthCaptureSpec {
                    instrument_id: id,
                    book_type: BookType::L2_MBP,
                    depth: None,
                }],
                ..Default::default()
            },
        );
        config.metrics_snapshot = Some(Arc::clone(&store));
        let mut actor = CatalogCaptureActor::new(config).unwrap();
        actor
            .register(
                TraderId::test_default(),
                Rc::new(RefCell::new(VirtualClock::new())),
                Rc::new(RefCell::new(Cache::new(None, None))),
            )
            .unwrap();
        let commands = Rc::new(RefCell::new(Vec::new()));
        let received = Rc::clone(&commands);
        let handler = ShareableMessageHandler::from_typed(move |command: &ShutdownSystem| {
            received.borrow_mut().push(command.clone())
        });
        let topic = MessagingSwitchboard::shutdown_system_topic();
        msgbus::subscribe_any(topic.into(), handler.clone(), None);
        let trade = TradeTick::new(
            id,
            Price::from("0.50"),
            Quantity::from("1"),
            AggressorSide::Buy,
            TradeId::from("test-trade"),
            1.into(),
            2.into(),
        );
        let levels = (1..=99)
            .rev()
            .map(|value| {
                nautilus_model::data::BookOrder::new(
                    nautilus_model::enums::OrderSide::Buy,
                    Price::from(format!("0.{value:03}").as_str()),
                    Quantity::from("1"),
                    0,
                )
            })
            .collect::<Vec<_>>();
        let large = OrderBookDepth::new(id, levels, [], vec![0; 99], [], 0, 0, 1.into(), 2.into());
        assert!(actor.on_book_depth(&large).is_err());
        let first_failure = actor.failure.clone();
        assert!(first_failure.is_some());
        assert!(actor.on_trade(&trade).is_ok());
        let depth = OrderBookDepth::new(id, [], [], [], [], 0, 0, 1.into(), 2.into());
        assert!(actor.on_book_depth(&depth).is_ok());
        assert_eq!(commands.borrow().len(), 0);
        assert_eq!(actor.failure, first_failure);
        assert!(DataActor::on_stop(&mut actor).is_err());
        let final_state = store.read().unwrap();
        assert!(final_state.has_recording_failure());
        assert_eq!(final_state.failure, first_failure);
        assert_eq!(final_state.aggregated.dropped_items, 1);
        assert_eq!(final_state.aggregated.accepted_items, 2);
        assert_eq!(final_state.aggregated.flushed_rows, 2);
        assert_eq!(final_state.enabled_background_workers, 0);
        msgbus::unsubscribe_any(topic.into(), &handler);
        drop(final_state);
        drop(actor);
        fs::remove_dir_all(root).unwrap();
    }

    fn test_predict_outcome(market_id: u64, outcome_index: u8) -> InstrumentAny {
        let instrument_id =
            nautilus_predict::common::predict_outcome_instrument_id(market_id, outcome_index)
                .expect("valid test Predict id");
        let outcome = if outcome_index == 1 { "YES" } else { "NO" };
        InstrumentAny::BinaryOption(
            BinaryOption::builder()
                .instrument_id(instrument_id)
                .raw_symbol(Symbol::new(format!("{market_id}-{outcome}")))
                .asset_class(AssetClass::Alternative)
                .currency(Currency::get_or_create_crypto("USDT"))
                .activation_ns(UnixNanos::default())
                .expiration_ns(UnixNanos::from(i64::MAX as u64))
                .price_precision(2)
                .size_precision(3)
                .price_increment(Price::from("0.01"))
                .size_increment(Quantity::from("0.001"))
                .min_price(Price::zero(2))
                .max_price(Price::from("1.00"))
                .outcome(Ustr::from(outcome))
                .description(Ustr::from("test Predict market"))
                .ts_event(UnixNanos::default())
                .ts_init(UnixNanos::default())
                .build()
                .expect("valid test BinaryOption"),
        )
    }

    #[test]
    fn admission_fault_does_not_replace_a_healthy_instruments_delta_with_a_checkpoint() {
        use catalog_capture_core::plan::BookDeltasCaptureSpec;
        use nautilus_model::{
            data::{OrderBookDeltas, stubs::stub_deltas},
            enums::{BookAction, BookType, RecordFlag},
        };
        use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
        let root = std::env::temp_dir().join(format!(
            "capture-scoped-fault-{}",
            nautilus_core::UUID4::new()
        ));
        fs::create_dir_all(&root).unwrap();
        let source = stub_deltas();
        let failed_id = source.instrument_id;
        let healthy_id = InstrumentId::from("ETHUSDT.BINANCE");
        let initial = OrderBookDeltas::new(
            healthy_id,
            source
                .deltas
                .iter()
                .map(|row| {
                    let mut row = *row;
                    row.instrument_id = healthy_id;
                    row.ts_event = 100.into();
                    row.ts_init = 100.into();
                    row
                })
                .collect(),
        );
        let mut book = OrderBook::new(healthy_id, BookType::L2_MBP);
        book.apply_deltas(&initial).unwrap();
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        cache.borrow_mut().add_order_book(book).unwrap();
        let mut actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", root.display()),
                max_buffer_bytes: 4096,
                max_total_buffer_bytes: 4096,
                overflow_policy: catalog_capture_core::OverflowPolicy::FailFast,
                ..Default::default()
            },
            CapturePlan {
                book_deltas: [failed_id, healthy_id]
                    .into_iter()
                    .map(|instrument_id| BookDeltasCaptureSpec {
                        instrument_id,
                        book_type: BookType::L2_MBP,
                        depth: None,
                    })
                    .collect(),
                ..Default::default()
            },
        ))
        .unwrap();
        actor
            .register(
                TraderId::test_default(),
                Rc::new(RefCell::new(VirtualClock::new())),
                Rc::clone(&cache),
            )
            .unwrap();
        actor.on_book_deltas(&initial).unwrap();
        let oversized = OrderBookDeltas::new(failed_id, vec![source.deltas[1]; 1024]);
        assert!(actor.on_book_deltas(&oversized).is_err());
        let mut row = initial.deltas[1];
        row.action = BookAction::Update;
        row.flags = RecordFlag::F_LAST as u8;
        row.sequence += 1;
        row.ts_event = 200.into();
        row.ts_init = 200.into();
        row.order.size = Quantity::from("333");
        let continuation = OrderBookDeltas::new(healthy_id, vec![row]);
        cache
            .borrow_mut()
            .order_book_mut(&healthy_id)
            .unwrap()
            .apply_deltas(&continuation)
            .unwrap();
        actor.on_book_deltas(&continuation).unwrap();
        assert_eq!(actor.book_delta_recovery, BTreeSet::from([failed_id]));
        assert!(actor.shutdown_all().is_err());
        let mut catalog = ParquetDataCatalog::new(&root, None, None, None, None);
        let rows = catalog
            .order_book_deltas(Some(vec![healthy_id.to_string()]), None, None)
            .unwrap();
        assert_eq!(
            rows.iter()
                .filter(|row| row.action == BookAction::Clear)
                .count(),
            1,
            "an unrelated admission fault must not inject another snapshot"
        );
        assert_eq!(rows.len(), initial.deltas.len() + 1);
        assert_eq!(rows.last().unwrap(), &row);
        drop(actor);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn incremental_book_gap_recovers_from_native_cache_then_records_later_deltas() {
        use catalog_capture_core::plan::BookDeltasCaptureSpec;
        use nautilus_model::{
            data::{OrderBookDeltas, stubs::stub_deltas},
            enums::{BookAction, BookType, RecordFlag},
        };
        use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
        let root = std::env::temp_dir().join(format!(
            "capture-l2-recovery-{}",
            nautilus_core::UUID4::new()
        ));
        fs::create_dir_all(&root).unwrap();
        let initial = stub_deltas();
        let id = initial.instrument_id;
        let mut book = OrderBook::new(id, BookType::L2_MBP);
        book.apply_deltas(&initial).unwrap();
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        cache.borrow_mut().add_order_book(book).unwrap();
        let mut actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", root.display()),
                max_buffer_bytes: 4096,
                max_total_buffer_bytes: 4096,
                overflow_policy: catalog_capture_core::OverflowPolicy::FailFast,
                ..Default::default()
            },
            CapturePlan {
                book_deltas: vec![BookDeltasCaptureSpec {
                    instrument_id: id,
                    book_type: BookType::L2_MBP,
                    depth: None,
                }],
                ..Default::default()
            },
        ))
        .unwrap();
        actor
            .register(
                TraderId::test_default(),
                Rc::new(RefCell::new(VirtualClock::new())),
                Rc::clone(&cache),
            )
            .unwrap();
        let mut update = initial.deltas[1];
        update.action = BookAction::Update;
        update.flags = 0;
        let mut rows = vec![update; 1024];
        rows.last_mut().unwrap().flags = RecordFlag::F_LAST as u8;
        let large = OrderBookDeltas::new(id, rows);
        cache
            .borrow_mut()
            .order_book_mut(&id)
            .unwrap()
            .apply_deltas(&large)
            .unwrap();
        assert!(actor.on_book_deltas(&large).is_err());
        for (ts, size) in [(3_u64, "333"), (4, "444")] {
            update.flags = RecordFlag::F_LAST as u8;
            update.ts_event = ts.into();
            update.ts_init = ts.into();
            update.order.size = Quantity::from(size);
            let next = OrderBookDeltas::new(id, vec![update]);
            cache
                .borrow_mut()
                .order_book_mut(&id)
                .unwrap()
                .apply_deltas(&next)
                .unwrap();
            actor.on_book_deltas(&next).unwrap();
        }
        assert!(actor.book_delta_recovery.is_empty());
        assert!(actor.shutdown_all().is_err());
        let mut catalog = ParquetDataCatalog::new(&root, None, None, None, None);
        let rows = catalog
            .order_book_deltas(Some(vec![id.to_string()]), None, None)
            .unwrap();
        assert!(rows.iter().any(|row| row.action == BookAction::Clear));
        assert_eq!(rows.last().unwrap().action, BookAction::Update);
        let mut replay = OrderBook::new(id, BookType::L2_MBP);
        replay
            .apply_deltas(&OrderBookDeltas::new(id, rows))
            .unwrap();
        let expected = cache
            .borrow()
            .order_book(&id)
            .unwrap()
            .to_deltas(0.into(), 0.into());
        let actual = replay.to_deltas(0.into(), 0.into());
        assert_eq!(
            actual
                .deltas
                .iter()
                .map(|row| (row.action, row.order))
                .collect::<Vec<_>>(),
            expected
                .deltas
                .iter()
                .map(|row| (row.action, row.order))
                .collect::<Vec<_>>()
        );
        drop(catalog);
        drop(actor);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_seal_still_rearms_the_next_scheduled_seal() {
        let root = std::env::temp_dir().join(format!(
            "capture-seal-rearm-{}",
            nautilus_core::UUID4::new()
        ));
        fs::create_dir_all(&root).unwrap();
        let mut capture = CaptureConfig {
            catalog_uri: format!("file://{}", root.display()),
            ..Default::default()
        };
        capture.lifecycle.seal.enabled = true;
        let mut actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(
            capture,
            CapturePlan {
                trades: vec![catalog_capture_core::plan::TradeCaptureSpec {
                    instrument_id: "42-1.PREDICT".into(),
                }],
                ..Default::default()
            },
        ))
        .unwrap();
        actor
            .register(
                TraderId::test_default(),
                Rc::new(RefCell::new(VirtualClock::new())),
                Rc::new(RefCell::new(Cache::new(None, None))),
            )
            .unwrap();
        actor.trade_runtime.as_mut().unwrap().shutdown().unwrap();
        let event = nautilus_common::timer::TimeEvent::new(
            SEGMENT_SEAL_TIMER.into(),
            nautilus_core::UUID4::new(),
            0.into(),
            0.into(),
        );
        assert!(DataActor::on_time_event(&mut actor, &event).is_err());
        assert!(
            actor
                .clock()
                .timer_names()
                .iter()
                .any(|name| name == SEGMENT_SEAL_TIMER)
        );
        drop(actor);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn instrument_definition_is_retried_after_a_transient_catalog_write_failure() {
        let root =
            std::env::temp_dir().join(format!("capture-def-retry-{}", nautilus_core::UUID4::new()));
        fs::create_dir_all(root.join("data")).unwrap();
        let obstruction = root.join("data/binary_option");
        fs::write(&obstruction, b"temporary obstruction").unwrap();
        let instrument = test_predict_outcome(42, 1);
        let id = instrument.id();
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();
        let mut actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", root.display()),
                ..Default::default()
            },
            CapturePlan {
                instruments: vec![catalog_capture_core::plan::InstrumentCaptureSpec {
                    instrument_id: id,
                }],
                ..Default::default()
            },
        ))
        .unwrap();
        actor
            .register(
                TraderId::test_default(),
                Rc::new(RefCell::new(VirtualClock::new())),
                cache,
            )
            .unwrap();
        actor.submit_instrument(instrument).unwrap();
        assert!(
            actor
                .instrument_runtime
                .as_ref()
                .unwrap()
                .flush_all()
                .is_err()
        );
        fs::remove_file(obstruction).unwrap();
        actor.retry_instrument_definitions().unwrap();
        actor
            .instrument_runtime
            .as_ref()
            .unwrap()
            .flush_all()
            .unwrap();
        assert!(actor.pending_instrument_definitions.is_empty());
        assert!(actor.shutdown_all().is_err());
        let catalog = nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog::new(
            &root, None, None, None, None,
        );
        let definitions = catalog.instruments(None, None, None).unwrap();
        assert_eq!(definitions.len(), 1);
        assert_eq!(definitions[0].id(), id);
        drop(catalog);
        drop(actor);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn actor_starts_only_plan_enabled_background_workers() {
        let catalog_dir = std::env::temp_dir().join("catalog-capture-actor-lazy-runtime-test");
        fs::create_dir_all(&catalog_dir).expect("temp catalog dir should exist");
        let plan = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: InstrumentId::from("BTCUSDT-PERP.BINANCE"),
            }],
            ..CapturePlan::default()
        };
        let mut actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", catalog_dir.display()),
                ..CaptureConfig::default()
            },
            plan,
        ))
        .expect("actor should construct");

        assert_eq!(actor.enabled_background_worker_count(), 2);
        assert!(actor.instrument_runtime.is_some());
        assert!(actor.quote_runtime.is_some());
        assert!(actor.trade_runtime.is_none());
        assert!(actor.book_delta_runtime.is_none());
        let _ = actor.shutdown_all().expect("shutdown should succeed");
    }

    #[test]
    fn dynamic_predict_selector_uses_only_its_interval_writer() {
        let catalog_dir = std::env::temp_dir().join(format!(
            "catalog-capture-actor-predict-runtime-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&catalog_dir);
        fs::create_dir_all(&catalog_dir).expect("temp catalog dir");
        let metadata = Params::new(
            [
                ("price_feed_symbol".to_string(), "BTC/USDT".into()),
                ("title_asset".to_string(), "Bitcoin".into()),
                ("interval_secs".to_string(), "300".into()),
            ]
            .into(),
        );
        let plan = CapturePlan {
            custom_data: vec![CustomDataCaptureSpec {
                data_type: DataType::new("PredictCryptoUpDown", Some(metadata), None),
            }],
            ..CapturePlan::default()
        };
        let mut actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", catalog_dir.display()),
                ..CaptureConfig::default()
            },
            plan,
        ))
        .expect("actor");

        let selector_key = CryptoUpDownSelectorKey::new("BTC/USDT", "Bitcoin", 300).unwrap();
        assert!(actor.custom_data_runtime.is_none());
        assert!(actor.instrument_runtime.is_some());
        assert!(
            actor
                .predict_custom_data_runtimes
                .contains_key(&selector_key)
        );
        assert_eq!(actor.enabled_background_worker_count(), 2);
        let _ = actor.shutdown_all().expect("shutdown");
        let _ = fs::remove_dir_all(&catalog_dir);
    }

    #[test]
    fn dynamic_predict_same_interval_selectors_get_independent_writers() {
        let catalog_dir = std::env::temp_dir().join(format!(
            "catalog-capture-actor-predict-same-interval-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&catalog_dir);
        fs::create_dir_all(&catalog_dir).expect("temp catalog dir");
        let selector = |price_feed_symbol: &str, title_asset: &str| {
            let metadata = Params::new(
                [
                    ("price_feed_symbol".to_string(), price_feed_symbol.into()),
                    ("title_asset".to_string(), title_asset.into()),
                    ("interval_secs".to_string(), "300".into()),
                ]
                .into(),
            );
            CustomDataCaptureSpec {
                data_type: DataType::new("PredictCryptoUpDown", Some(metadata), None),
            }
        };
        let plan = CapturePlan {
            custom_data: vec![
                selector("BTC/USDT", "Bitcoin"),
                selector("ETH/USDT", "Ethereum"),
            ],
            ..CapturePlan::default()
        };
        let mut actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", catalog_dir.display()),
                ..CaptureConfig::default()
            },
            plan,
        ))
        .expect("actor");

        assert_eq!(actor.predict_custom_data_runtimes.len(), 2);
        assert!(
            actor
                .predict_custom_data_runtimes
                .contains_key(&CryptoUpDownSelectorKey::new("BTCUSDT", "bitcoin", 300).unwrap())
        );
        assert!(
            actor
                .predict_custom_data_runtimes
                .contains_key(&CryptoUpDownSelectorKey::new("ETHUSDT", "ethereum", 300).unwrap())
        );
        assert!(actor.instrument_runtime.is_some());
        assert_eq!(actor.enabled_background_worker_count(), 3);
        let metrics = actor.metrics_snapshot_data();
        assert!(
            metrics
                .families
                .iter()
                .any(|family| family.family == "predict_custom_data_BTCUSDT|bitcoin|300s")
        );
        assert!(
            metrics
                .families
                .iter()
                .any(|family| family.family == "predict_custom_data_ETHUSDT|ethereum|300s")
        );
        let _ = actor.shutdown_all().expect("shutdown");
        let _ = fs::remove_dir_all(&catalog_dir);
    }

    #[test]
    fn binance_futures_custom_data_uses_the_configured_client() {
        let catalog_dir = std::env::temp_dir().join(format!(
            "catalog-capture-actor-binance-custom-client-test-{}",
            std::process::id()
        ));
        fs::create_dir_all(&catalog_dir).expect("temp catalog dir");
        let mut config = CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", catalog_dir.display()),
                ..CaptureConfig::default()
            },
            CapturePlan::default(),
        );
        config.binance_futures_client_id = Some(ClientId::from("binance_usdm"));
        let mut actor = CatalogCaptureActor::new(config).expect("actor");

        let data_type = DataType::new("BinanceFuturesTicker", None, None);
        assert_eq!(
            actor.custom_data_client_id(&data_type),
            Some(ClientId::from("binance_usdm"))
        );
        let _ = actor.shutdown_all().expect("shutdown");
        let _ = fs::remove_dir_all(&catalog_dir);
    }

    #[test]
    fn actor_purges_removed_hip4_instruments_from_cache_when_enabled() {
        let catalog_dir = std::env::temp_dir().join("catalog-capture-actor-hip4-purge-test");
        fs::create_dir_all(&catalog_dir).expect("temp catalog dir should exist");
        let mut actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", catalog_dir.display()),
                ..CaptureConfig::default()
            },
            CapturePlan::default(),
        ))
        .expect("actor should construct");
        actor.dynamic_hip4_universe =
            Some(DynamicHip4UniverseManager::new(DynamicHip4UniverseConfig {
                idle_poll_secs: 1800,
                active_poll_secs: 10,
                pre_expiry_window_secs: 900,
                http_timeout_secs: 10,
                purge_removed_instruments: true,
                static_plan: CapturePlan::default(),
                initial_dynamic_plan: CapturePlan::default(),
                universes: vec![],
            }));

        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        actor
            .register(TraderId::test_default(), clock, cache.clone())
            .expect("actor should register");

        let instrument = audusd_sim();
        let instrument_id = instrument.id;
        {
            let mut cache = cache.borrow_mut();
            cache
                .add_instrument(InstrumentAny::CurrencyPair(instrument))
                .expect("instrument should be cached");
            cache
                .add_quote(test_quote(instrument_id))
                .expect("quote should be cached");
        }

        // Simulate post-roll actor bookkeeping still holding the expired id.
        actor.pending_market_data.insert(instrument_id);
        actor.market_data_live.insert(instrument_id);

        assert!(cache.borrow().instrument(&instrument_id).is_some());
        assert!(cache.borrow().quote(&instrument_id).is_some());

        actor.purge_removed_hip4_instruments(&[DynamicHip4UniverseChange {
            venue_id: "hyperliquid_main".to_string(),
            underlying: "BTC".to_string(),
            period: "1d".to_string(),
            market_class: "priceBinary".to_string(),
            question_id: 55,
            expiration_iso8601: "2026-06-21T06:00:00Z".to_string(),
            perp_instrument_id: Some(InstrumentId::from("BTC-USD-PERP.HYPERLIQUID")),
            outcome_instrument_ids: vec![instrument_id],
            previous_count: 3,
            next_count: 3,
            added_instrument_ids: vec![],
            removed_instrument_ids: vec![instrument_id],
        }]);

        // Nautilus Cache::purge_instrument clears definition + per-instrument market data.
        assert!(cache.borrow().instrument(&instrument_id).is_none());
        assert!(cache.borrow().quote(&instrument_id).is_none());
        assert!(!actor.pending_market_data.contains(&instrument_id));
        assert!(!actor.market_data_live.contains(&instrument_id));

        let _ = actor.shutdown_all().expect("shutdown should succeed");
    }

    #[test]
    fn hip4_retirement_seals_only_old_partition_and_rejects_late_quotes() {
        use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
        let dir = std::env::temp_dir().join(format!("hip4-retire-{}", nautilus_core::UUID4::new()));
        fs::create_dir_all(&dir).unwrap();
        let old = InstrumentId::from("1001-YES-OUTCOME.HYPERLIQUID");
        let new = InstrumentId::from("1009-YES-OUTCOME.HYPERLIQUID");
        let binance = InstrumentId::from("BTCUSDT-PERP.BINANCE");
        let plan = CapturePlan {
            quotes: [old, new, binance]
                .into_iter()
                .map(|instrument_id| QuoteCaptureSpec { instrument_id })
                .collect(),
            ..CapturePlan::default()
        };
        let mut actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", dir.display()),
                ..CaptureConfig::default()
            },
            plan,
        ))
        .unwrap();
        actor.dynamic_hip4_universe =
            Some(DynamicHip4UniverseManager::new(DynamicHip4UniverseConfig {
                idle_poll_secs: 1800,
                active_poll_secs: 10,
                pre_expiry_window_secs: 900,
                http_timeout_secs: 10,
                purge_removed_instruments: true,
                static_plan: CapturePlan::default(),
                initial_dynamic_plan: CapturePlan::default(),
                universes: vec![],
            }));
        actor.market_data_live.extend([old, new, binance]);
        actor.submit_quote(test_quote(old)).unwrap();
        actor.submit_quote(test_quote(binance)).unwrap();
        actor.flush_all().unwrap();
        let remove = CapturePlan {
            quotes: vec![QuoteCaptureSpec { instrument_id: old }],
            ..CapturePlan::default()
        };
        actor.seal_removed_hip4_partitions(&remove).unwrap();
        // Explicit writer barrier: retirement now enqueues its seal without blocking Native.
        actor.quote_runtime.as_ref().unwrap().flush_all().unwrap();
        let old_dir = dir.join("data/quotes").join(old.to_string());
        assert_eq!(fs::read_dir(&old_dir).unwrap().count(), 1);
        assert!(
            fs::read_dir(&old_dir)
                .unwrap()
                .all(|entry| entry.unwrap().path().extension().unwrap() == "parquet")
        );
        let binance_dir = dir.join("data/quotes").join(binance.to_string());
        assert_eq!(fs::read_dir(&binance_dir).unwrap().count(), 1);
        assert!(
            fs::read_dir(&binance_dir).unwrap().all(|entry| entry
                .unwrap()
                .path()
                .extension()
                .unwrap()
                == "part")
        );
        // A different channel may still own this instrument. A removed quote
        // subscription must reject late data even while the instrument stays live.
        actor.plan.quotes.retain(|spec| spec.instrument_id != old);
        actor.submit_quote(test_quote(old)).unwrap();
        actor.market_data_live.remove(&old);
        actor.submit_quote(test_quote(old)).unwrap();
        actor.submit_quote(test_quote(new)).unwrap();
        actor.shutdown_all().unwrap();
        let mut catalog = ParquetDataCatalog::new(&dir, None, None, None, None);
        for id in [old, new, binance] {
            let rows = catalog
                .quote_ticks(Some(vec![id.to_string()]), None, None)
                .unwrap();
            assert_eq!(rows.len(), 1, "exactly one accepted quote for {id}");
        }
        assert_eq!(
            fs::read_dir(&old_dir).unwrap().count(),
            1,
            "late quote must not reopen old file"
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn predict_roll_purges_unreferenced_retired_outcomes_but_keeps_static_capture() {
        let catalog_dir = std::env::temp_dir().join(format!(
            "catalog-capture-actor-predict-purge-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&catalog_dir);
        fs::create_dir_all(&catalog_dir).expect("temp catalog dir");
        let retired_market_id = 3_063_835;
        let retired_yes =
            nautilus_predict::common::predict_outcome_instrument_id(retired_market_id, 1)
                .expect("valid test id");
        let retired_no =
            nautilus_predict::common::predict_outcome_instrument_id(retired_market_id, 2)
                .expect("valid test id");
        // Explicit user capture always wins over automatic rolling-market cleanup.
        let plan = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: retired_yes,
            }],
            ..CapturePlan::default()
        };
        let mut actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", catalog_dir.display()),
                ..CaptureConfig::default()
            },
            plan,
        ))
        .expect("actor");
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        actor
            .register(TraderId::test_default(), clock, cache.clone())
            .expect("register");
        {
            let mut cache = cache.borrow_mut();
            cache
                .add_instrument(test_predict_outcome(retired_market_id, 1))
                .expect("cache retired yes");
            cache
                .add_instrument(test_predict_outcome(retired_market_id, 2))
                .expect("cache retired no");
        }

        actor.active_predict_market_ids.insert(
            CryptoUpDownSelectorKey::new("BTCUSDT", "Bitcoin", 300).unwrap(),
            3_063_863,
        );
        actor
            .purge_retired_predict_market_from_cache(retired_market_id)
            .expect("purge retired market");

        assert!(
            cache.borrow().instrument(&retired_yes).is_some(),
            "explicit static capture must retain its definition"
        );
        assert!(
            cache.borrow().instrument(&retired_no).is_none(),
            "unreferenced retired outcome must not accumulate in cache"
        );

        let _ = actor.shutdown_all().expect("shutdown");
        let _ = fs::remove_dir_all(&catalog_dir);
    }

    #[test]
    fn predict_roll_keeps_a_market_still_owned_by_another_selector() {
        let catalog_dir = std::env::temp_dir().join(format!(
            "catalog-capture-actor-predict-shared-market-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&catalog_dir);
        fs::create_dir_all(&catalog_dir).expect("temp catalog dir");
        let market_id = 3_063_835;
        let yes_id = nautilus_predict::common::predict_outcome_instrument_id(market_id, 1)
            .expect("valid test id");
        let no_id = nautilus_predict::common::predict_outcome_instrument_id(market_id, 2)
            .expect("valid test id");
        let mut actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", catalog_dir.display()),
                ..CaptureConfig::default()
            },
            CapturePlan::default(),
        ))
        .expect("actor");
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        actor
            .register(TraderId::test_default(), clock, cache.clone())
            .expect("register");
        {
            let mut cache = cache.borrow_mut();
            cache
                .add_instrument(test_predict_outcome(market_id, 1))
                .expect("cache yes");
            cache
                .add_instrument(test_predict_outcome(market_id, 2))
                .expect("cache no");
        }
        actor.active_predict_market_ids.insert(
            CryptoUpDownSelectorKey::new("ETHUSDT", "Ethereum", 300).unwrap(),
            market_id,
        );

        actor
            .purge_retired_predict_market_from_cache(market_id)
            .expect("shared market retained");

        assert!(cache.borrow().instrument(&yes_id).is_some());
        assert!(cache.borrow().instrument(&no_id).is_some());

        let _ = actor.shutdown_all().expect("shutdown");
        let _ = fs::remove_dir_all(&catalog_dir);
    }

    #[test]
    fn hip4_roll_defers_market_data_until_instrument_ready_then_subscribes() {
        use catalog_capture_core::plan::TradeCaptureSpec;

        let catalog_dir = std::env::temp_dir().join(format!(
            "catalog-capture-actor-hip4-defer-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&catalog_dir);
        fs::create_dir_all(&catalog_dir).expect("temp catalog dir");

        let old_id = InstrumentId::from("1001-NO-OUTCOME.HYPERLIQUID");
        let new_id = InstrumentId::from("1009-NO-OUTCOME.HYPERLIQUID");
        let initial_plan = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: old_id,
            }],
            trades: vec![TradeCaptureSpec {
                instrument_id: old_id,
            }],
            ..CapturePlan::default()
        };
        let mut actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", catalog_dir.display()),
                ..CaptureConfig::default()
            },
            initial_plan.clone(),
        ))
        .expect("actor");

        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        actor
            .register(TraderId::test_default(), clock, cache.clone())
            .expect("register");

        // Pretend old outcome was live.
        actor.market_data_live.insert(old_id);
        actor.plan = initial_plan;

        let add = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: new_id,
            }],
            trades: vec![TradeCaptureSpec {
                instrument_id: new_id,
            }],
            ..CapturePlan::default()
        };
        let remove = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: old_id,
            }],
            trades: vec![TradeCaptureSpec {
                instrument_id: old_id,
            }],
            ..CapturePlan::default()
        };

        // New instrument not in cache yet → must defer (no race subscribe).
        actor
            .apply_subscription_delta(&add, &remove)
            .expect("delta");
        assert!(
            actor.pending_market_data.contains(&new_id),
            "new instrument should be pending until cache-ready"
        );
        assert!(
            !actor.market_data_live.contains(&new_id),
            "must not mark market-data live before instrument is cached"
        );
        assert!(
            !actor.market_data_live.contains(&old_id),
            "removed instrument must leave market_data_live"
        );

        // Instrument arrives (simulates request_instrument / adapter response).
        let mut new_inst = audusd_sim();
        new_inst.id = new_id;
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CurrencyPair(new_inst.clone()))
            .expect("cache add");
        // Plan already synced by apply_subscription_delta to effective plan (may be empty
        // without dynamic managers). Point plan at the post-roll add set for on_instrument.
        actor.plan = add;
        actor
            .on_instrument(&InstrumentAny::CurrencyPair(new_inst))
            .expect("on_instrument");

        assert!(
            !actor.pending_market_data.contains(&new_id),
            "pending cleared once instrument is ready"
        );
        assert!(
            actor.market_data_live.contains(&new_id),
            "market-data marked live after ready-then-subscribe"
        );

        let _ = actor.shutdown_all().expect("shutdown");
        let _ = fs::remove_dir_all(&catalog_dir);
    }

    #[test]
    fn purge_skips_perp_and_clears_expired_outcome_only() {
        let catalog_dir = std::env::temp_dir().join(format!(
            "catalog-capture-actor-hip4-purge-perp-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&catalog_dir);
        fs::create_dir_all(&catalog_dir).expect("temp");

        let mut actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", catalog_dir.display()),
                ..CaptureConfig::default()
            },
            CapturePlan::default(),
        ))
        .expect("actor");
        actor.dynamic_hip4_universe =
            Some(DynamicHip4UniverseManager::new(DynamicHip4UniverseConfig {
                idle_poll_secs: 1800,
                active_poll_secs: 10,
                pre_expiry_window_secs: 900,
                http_timeout_secs: 10,
                purge_removed_instruments: true,
                static_plan: CapturePlan::default(),
                initial_dynamic_plan: CapturePlan::default(),
                universes: vec![],
            }));

        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        actor
            .register(TraderId::test_default(), clock, cache.clone())
            .expect("register");

        let mut outcome = audusd_sim();
        let outcome_id = InstrumentId::from("1001-NO-OUTCOME.HYPERLIQUID");
        outcome.id = outcome_id;
        let mut perp = audusd_sim();
        let perp_id = InstrumentId::from("BTC-USD-PERP.HYPERLIQUID");
        perp.id = perp_id;
        {
            let mut c = cache.borrow_mut();
            c.add_instrument(InstrumentAny::CurrencyPair(outcome))
                .unwrap();
            c.add_instrument(InstrumentAny::CurrencyPair(perp)).unwrap();
            c.add_quote(test_quote(outcome_id)).unwrap();
            c.add_quote(test_quote(perp_id)).unwrap();
        }
        actor.market_data_live.insert(outcome_id);
        actor.market_data_live.insert(perp_id);

        actor.purge_removed_hip4_instruments(&[DynamicHip4UniverseChange {
            venue_id: "hyperliquid_main".to_string(),
            underlying: "BTC".to_string(),
            period: "1d".to_string(),
            market_class: "priceBinary".to_string(),
            question_id: 1009,
            expiration_iso8601: "2026-08-05T06:00:00Z".to_string(),
            perp_instrument_id: Some(perp_id),
            outcome_instrument_ids: vec![InstrumentId::from("1009-NO-OUTCOME.HYPERLIQUID")],
            previous_count: 3,
            next_count: 3,
            added_instrument_ids: vec![InstrumentId::from("1009-NO-OUTCOME.HYPERLIQUID")],
            // Even if remove list wrongly includes perp, purge must keep it.
            removed_instrument_ids: vec![outcome_id, perp_id],
        }]);

        assert!(
            cache.borrow().instrument(&outcome_id).is_none(),
            "expired outcome purged"
        );
        assert!(
            cache.borrow().quote(&outcome_id).is_none(),
            "outcome quotes purged with instrument"
        );
        assert!(
            cache.borrow().instrument(&perp_id).is_some(),
            "perp reference retained"
        );
        assert!(
            cache.borrow().quote(&perp_id).is_some(),
            "perp market data retained"
        );
        assert!(!actor.market_data_live.contains(&outcome_id));
        assert!(
            actor.market_data_live.contains(&perp_id),
            "perp live flag not cleared by purge of outcomes"
        );

        let _ = actor.shutdown_all().expect("shutdown");
        let _ = fs::remove_dir_all(&catalog_dir);
    }

    #[test]
    fn option_universe_purge_clears_expired_options_keeps_perp_and_still_active() {
        use catalog_capture_core::plan::QuoteCaptureSpec;

        let catalog_dir = std::env::temp_dir().join(format!(
            "catalog-capture-actor-option-purge-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&catalog_dir);
        fs::create_dir_all(&catalog_dir).expect("temp");

        let expired = InstrumentId::from("BTC-26JUN26-65000-C.DERIBIT");
        let still_active = InstrumentId::from("BTC-26JUN26-66000-C.DERIBIT");
        let perp = InstrumentId::from("BTC-PERPETUAL.DERIBIT");

        let mut actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", catalog_dir.display()),
                ..CaptureConfig::default()
            },
            CapturePlan {
                // Static plan still references still_active → must not purge it
                // even if a change lists it under removed.
                quotes: vec![QuoteCaptureSpec {
                    instrument_id: still_active,
                }],
                ..CapturePlan::default()
            },
        ))
        .expect("actor");
        actor.dynamic_option_universe = Some(DynamicOptionUniverseManager::new(
            DynamicOptionUniverseConfig {
                refresh_interval_secs: 60,
                strike_change_confirmations: 0,
                purge_removed_instruments: true,
                static_plan: CapturePlan {
                    quotes: vec![QuoteCaptureSpec {
                        instrument_id: still_active,
                    }],
                    ..CapturePlan::default()
                },
                initial_dynamic_plan: CapturePlan::default(),
                universes: vec![],
            },
        ));

        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        actor
            .register(TraderId::test_default(), clock, cache.clone())
            .expect("register");

        // Seed cache like a multi-day option capture.
        for id in [expired, still_active, perp] {
            let mut inst = audusd_sim();
            inst.id = id;
            cache
                .borrow_mut()
                .add_instrument(InstrumentAny::CurrencyPair(inst))
                .unwrap();
            cache.borrow_mut().add_quote(test_quote(id)).unwrap();
            actor.market_data_live.insert(id);
        }
        actor.pending_market_data.insert(expired);
        // Post-roll plan already applied (still_active remains).
        actor.sync_plan_state();

        actor.purge_removed_option_instruments(&[crate::DynamicOptionUniverseChange {
            venue_id: "deribit_main".to_string(),
            underlying: "BTC".to_string(),
            selected_expiry_iso8601: "2026-06-26T08:00:00.000000000Z".to_string(),
            perp_instrument_id: Some(perp),
            option_instrument_ids: vec![still_active],
            previous_count: 3,
            next_count: 2,
            added_instrument_ids: vec![],
            // Include still_active + perp in remove list to prove guards work.
            removed_instrument_ids: vec![expired, still_active, perp],
        }]);

        assert!(
            cache.borrow().instrument(&expired).is_none(),
            "expired option definition purged from Cache"
        );
        assert!(
            cache.borrow().quote(&expired).is_none(),
            "expired option quotes purged with instrument"
        );
        assert!(!actor.market_data_live.contains(&expired));
        assert!(!actor.pending_market_data.contains(&expired));

        assert!(
            cache.borrow().instrument(&perp).is_some(),
            "perp reference must not be purged"
        );
        assert!(cache.borrow().quote(&perp).is_some());
        assert!(
            cache.borrow().instrument(&still_active).is_some(),
            "still-active plan instrument must not be purged"
        );
        assert!(cache.borrow().quote(&still_active).is_some());

        let _ = actor.shutdown_all().expect("shutdown");
        let _ = fs::remove_dir_all(&catalog_dir);
    }

    #[test]
    fn lighter_retirement_preserves_another_owner_and_purges_unowned_cache() {
        use nautilus_common::msgbus::{self, switchboard, typed_handler::ShareableMessageHandler};
        let root = std::env::temp_dir().join(format!(
            "lighter-other-owner-{}",
            nautilus_core::UUID4::new()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let mut config = CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", root.display()),
                ..Default::default()
            },
            CapturePlan::default(),
        );
        config
            .lighter_updown
            .push(crate::LighterUpDownCaptureConfig {
                tag: "eth-updown-15m".into(),
                client_id: "LIGHTER".into(),
                refresh_secs: 10,
                prefetch_secs: 30,
            });
        let mut actor = CatalogCaptureActor::new(config).unwrap();
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        actor
            .register(TraderId::test_default(), clock, cache.clone())
            .unwrap();
        let mut retired = Vec::new();
        for market in [98001, 98002] {
            let InstrumentAny::BinaryOption(mut binary) = test_predict_outcome(market, 1) else {
                unreachable!()
            };
            binary.id = InstrumentId::from(format!("{market}-BINARY.LIGHTER").as_str());
            binary.expiration_ns = UnixNanos::from(1);
            let info = Params::new([("market_id".into(), serde_json::json!(market))].into());
            binary.info = Some(info);
            let instrument = InstrumentAny::BinaryOption(binary);
            cache
                .borrow_mut()
                .add_instrument(instrument.clone())
                .unwrap();
            retired.push(instrument.clone());
            actor.lighter.retiring.insert(instrument.id(), instrument);
        }
        let protected = retired[0].id();
        let unowned = retired[1].id();
        let topic = switchboard::get_instrument_topic(protected);
        let handler = ShareableMessageHandler::from_any(|_| {});
        msgbus::subscribe_any(topic.as_str().into(), handler.clone(), None);
        actor.lighter.next_refresh_ns = u64::MAX;
        actor.refresh_lighter().unwrap();
        assert!(
            cache.borrow().instrument(&protected).is_some(),
            "other instrument subscriber owns its cache"
        );
        assert!(
            cache.borrow().instrument(&unowned).is_none(),
            "unowned retired instrument must be purged"
        );
        msgbus::unsubscribe_any(topic.as_str().into(), &handler);
        actor.shutdown_all().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
