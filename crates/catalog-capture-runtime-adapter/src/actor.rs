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
    collections::BTreeSet,
    fmt::Debug,
    mem::size_of,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use anyhow::{bail, Result};

use catalog_capture_core::{
    append_forward_price_records, append_hip4_universe_resolution_records,
    append_option_universe_resolution_records,
    background::BackgroundCaptureRuntime,
    capture_plan_difference, catalog_root_from_uri,
    config::CaptureConfig,
    forward_price_from_option_greeks, forward_price_record_from_model,
    item::{CaptureItem, PartitionKey},
    merge_capture_plans,
    metrics::CaptureMetrics,
    metrics_export::{process_rss_bytes, unix_time_ms, CaptureMetricsSnapshot, FamilyCaptureMetrics},
    next_seal_boundary_ns,
    plan::CapturePlan,
    runtime::FlushResult,
    sink::{chunked_catalog_sink_from_config, CaptureSink, CatalogSink, ChunkedCatalogSink},
};
use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    nautilus_actor,
    timer::TimeEvent,
};
use nautilus_model::{
    data::{
        close::InstrumentClose, Bar, CustomData, DataType, FundingRateUpdate, IndexPriceUpdate,
        InstrumentStatus, MarkPriceUpdate, OptionGreeks, OrderBookDelta, OrderBookDeltas,
        QuoteTick, TradeTick,
    },
    identifiers::{ActorId, ClientId, InstrumentId},
    instruments::{Instrument, InstrumentAny},
};

use crate::dynamic_hip4_universe::{
    DynamicHip4UniverseConfig, DynamicHip4UniverseManager,
};
use crate::dynamic_option_universe::{
    DynamicOptionUniverseConfig, DynamicOptionUniverseManager,
};
use crate::online_option_metrics::{OnlineOptionMetricsConfig, OnlineOptionMetricsObserver};
use nautilus_core::UnixNanos;

const OPTION_UNIVERSE_REFRESH_TIMER: &str = "OPTION_UNIVERSE_REFRESH";
const HIP4_UNIVERSE_REFRESH_TIMER: &str = "HIP4_UNIVERSE_REFRESH";
const SEGMENT_SEAL_TIMER: &str = "SEGMENT_SEAL";
const METRICS_EXPORT_TIMER: &str = "METRICS_EXPORT";

#[derive(Debug, Clone)]
pub struct CatalogCaptureActorConfig {
    pub actor_id: Option<ActorId>,
    pub capture: CaptureConfig,
    pub plan: CapturePlan,
    pub online_option_metrics: Option<OnlineOptionMetricsConfig>,
    pub dynamic_option_universe: Option<DynamicOptionUniverseConfig>,
    pub dynamic_hip4_universe: Option<DynamicHip4UniverseConfig>,
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
            metrics_snapshot: None,
            metrics_refresh_interval_secs: None,
        }
    }
}

pub struct CatalogCaptureActor {
    core: DataActorCore,
    capture: CaptureConfig,
    /// Startup materialized plan (static TOML + one-shot universe expansion).
    initial_materialized_plan: CapturePlan,
    /// Capture specs owned by startup materialization but not by an active refresh manager.
    supplemental_plan: CapturePlan,
    plan: CapturePlan,
    instrument_runtime: Option<BackgroundCaptureRuntime<InstrumentAny, ChunkedCatalogSink>>,
    custom_data_runtime: Option<BackgroundCaptureRuntime<CustomData, ChunkedCatalogSink>>,
    mark_price_runtime: Option<BackgroundCaptureRuntime<MarkPriceUpdate, CatalogSink<MarkPriceUpdate>>>,
    index_price_runtime: Option<BackgroundCaptureRuntime<IndexPriceUpdate, CatalogSink<IndexPriceUpdate>>>,
    funding_rate_runtime:
        Option<BackgroundCaptureRuntime<FundingRateUpdate, CatalogSink<FundingRateUpdate>>>,
    instrument_status_runtime:
        Option<BackgroundCaptureRuntime<InstrumentStatus, CatalogSink<InstrumentStatus>>>,
    instrument_close_runtime:
        Option<BackgroundCaptureRuntime<InstrumentClose, CatalogSink<InstrumentClose>>>,
    option_greeks_runtime: Option<BackgroundCaptureRuntime<OptionGreeks, CatalogSink<OptionGreeks>>>,
    forward_price_targets: BTreeSet<InstrumentId>,
    quote_runtime: Option<BackgroundCaptureRuntime<QuoteTick, CatalogSink<QuoteTick>>>,
    trade_runtime: Option<BackgroundCaptureRuntime<TradeTick, CatalogSink<TradeTick>>>,
    bar_runtime: Option<BackgroundCaptureRuntime<Bar, CatalogSink<Bar>>>,
    book_delta_runtime: Option<BackgroundCaptureRuntime<OrderBookDelta, CatalogSink<OrderBookDelta>>>,
    online_option_metrics: Option<OnlineOptionMetricsObserver>,
    dynamic_option_universe: Option<DynamicOptionUniverseManager>,
    dynamic_hip4_universe: Option<DynamicHip4UniverseManager>,
    metrics_snapshot: Option<Arc<RwLock<CaptureMetricsSnapshot>>>,
    metrics_refresh_interval_secs: Option<u64>,
    catalog_root: PathBuf,
    shutdown_completed: bool,
}

fn optional_submit<T, S>(
    runtime: &Option<BackgroundCaptureRuntime<T, S>>,
    item: CaptureItem<T>,
) -> Result<()>
where
    T: Send + 'static,
    S: CaptureSink<T> + Send + 'static,
{
    let Some(runtime) = runtime.as_ref() else {
        bail!("capture callback received data for a family without an enabled background runtime");
    };
    runtime.submit(item).map(|_| ())
}

fn optional_flush_all<T, S>(
    runtime: &Option<BackgroundCaptureRuntime<T, S>>,
) -> Result<FlushResult>
where
    T: Send + 'static,
    S: CaptureSink<T> + Send + 'static,
{
    match runtime.as_ref() {
        Some(runtime) => runtime.flush_all(),
        None => Ok(FlushResult::default()),
    }
}

fn optional_seal_all<T, S>(runtime: &Option<BackgroundCaptureRuntime<T, S>>) -> Result<FlushResult>
where
    T: Send + 'static,
    S: CaptureSink<T> + Send + 'static,
{
    match runtime.as_ref() {
        Some(runtime) => runtime.seal_all(),
        None => Ok(FlushResult::default()),
    }
}

fn optional_shutdown<T, S>(
    runtime: &mut Option<BackgroundCaptureRuntime<T, S>>,
) -> Result<FlushResult>
where
    T: Send + 'static,
    S: CaptureSink<T> + Send + 'static,
{
    match runtime.take() {
        Some(mut runtime) => runtime.shutdown(),
        None => Ok(FlushResult::default()),
    }
}

fn collect_family_metrics<T, S>(
    family: &str,
    runtime: &Option<BackgroundCaptureRuntime<T, S>>,
    families: &mut Vec<FamilyCaptureMetrics>,
    aggregated: &mut CaptureMetrics,
) where
    T: Send + 'static,
    S: CaptureSink<T> + Send + 'static,
{
    let Some(runtime) = runtime.as_ref() else {
        return;
    };
    let metrics = runtime.metrics();
    aggregated.merge(&metrics);
    families.push(FamilyCaptureMetrics {
        family: family.to_string(),
        metrics,
    });
}

fn custom_data_client_id(data_type: &DataType) -> Option<ClientId> {
    match data_type.type_name() {
        "DeribitVolatilityIndex" => Some(ClientId::from("DERIBIT")),
        "HyperliquidOpenInterest" => Some(ClientId::from("HYPERLIQUID")),
        _ => None,
    }
}

fn manager_active_plan_from_config(
    static_plan: &CapturePlan,
    initial_dynamic_plan: &CapturePlan,
) -> CapturePlan {
    merge_capture_plans(static_plan, initial_dynamic_plan)
}

fn supplemental_capture_plan(
    initial_materialized_plan: &CapturePlan,
    dynamic_option_universe: &Option<DynamicOptionUniverseConfig>,
    dynamic_hip4_universe: &Option<DynamicHip4UniverseConfig>,
) -> CapturePlan {
    let option_active = dynamic_option_universe.as_ref().map(|config| {
        manager_active_plan_from_config(&config.static_plan, &config.initial_dynamic_plan)
    });
    let hip4_active = dynamic_hip4_universe.as_ref().map(|config| {
        manager_active_plan_from_config(&config.static_plan, &config.initial_dynamic_plan)
    });

    match (&option_active, &hip4_active) {
        (Some(_option), Some(_)) => CapturePlan::default(),
        (Some(option), None) => capture_plan_difference(initial_materialized_plan, option),
        (None, Some(hip4)) => capture_plan_difference(initial_materialized_plan, hip4),
        (None, None) => CapturePlan::default(),
    }
}

fn count_spawned_background_workers(actor: &CatalogCaptureActor) -> usize {
    [
        actor.instrument_runtime.is_some(),
        actor.custom_data_runtime.is_some(),
        actor.mark_price_runtime.is_some(),
        actor.index_price_runtime.is_some(),
        actor.funding_rate_runtime.is_some(),
        actor.instrument_status_runtime.is_some(),
        actor.instrument_close_runtime.is_some(),
        actor.option_greeks_runtime.is_some(),
        actor.quote_runtime.is_some(),
        actor.trade_runtime.is_some(),
        actor.bar_runtime.is_some(),
        actor.book_delta_runtime.is_some(),
    ]
    .into_iter()
    .filter(|enabled| *enabled)
    .count()
}

fn effective_capture_plan(
    initial_materialized_plan: &CapturePlan,
    supplemental_plan: &CapturePlan,
    dynamic_option_universe: Option<&DynamicOptionUniverseManager>,
    dynamic_hip4_universe: Option<&DynamicHip4UniverseManager>,
) -> CapturePlan {
    match (dynamic_option_universe, dynamic_hip4_universe) {
        (None, None) => initial_materialized_plan.clone(),
        (Some(option), Some(hip4)) => merge_capture_plans(
            &option.active_capture_plan(),
            &hip4.active_capture_plan(),
        ),
        (Some(option), None) => {
            merge_capture_plans(&option.active_capture_plan(), supplemental_plan)
        }
        (None, Some(hip4)) => merge_capture_plans(&hip4.active_capture_plan(), supplemental_plan),
    }
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

        let flags = config.plan.family_runtime_flags();
        let worker_count = config.plan.enabled_background_worker_count();
        println!("Capture background workers: {worker_count} enabled for plan");

        // Instruments and custom data stay chunked: catalog paths are heterogeneous and do not
        // use the segment `.part` lifecycle.
        let capture = config.capture.clone();
        let instrument_runtime = if flags.instruments {
            Some(BackgroundCaptureRuntime::new(
                capture.clone(),
                chunked_catalog_sink_from_config(&capture)?,
            )?)
        } else {
            None
        };
        let custom_data_runtime = if flags.custom_data {
            Some(BackgroundCaptureRuntime::new(
                capture.clone(),
                chunked_catalog_sink_from_config(&capture)?,
            )?)
        } else {
            None
        };
        let mark_price_runtime = if flags.mark_prices {
            Some(BackgroundCaptureRuntime::new(
                capture.clone(),
                CatalogSink::<MarkPriceUpdate>::from_config(&capture)?,
            )?)
        } else {
            None
        };
        let index_price_runtime = if flags.index_prices {
            Some(BackgroundCaptureRuntime::new(
                capture.clone(),
                CatalogSink::<IndexPriceUpdate>::from_config(&capture)?,
            )?)
        } else {
            None
        };
        let funding_rate_runtime = if flags.funding_rates {
            Some(BackgroundCaptureRuntime::new(
                capture.clone(),
                CatalogSink::<FundingRateUpdate>::from_config(&capture)?,
            )?)
        } else {
            None
        };
        let instrument_status_runtime = if flags.instrument_statuses {
            Some(BackgroundCaptureRuntime::new(
                capture.clone(),
                CatalogSink::<InstrumentStatus>::from_config(&capture)?,
            )?)
        } else {
            None
        };
        let instrument_close_runtime = if flags.instrument_closes {
            Some(BackgroundCaptureRuntime::new(
                capture.clone(),
                CatalogSink::<InstrumentClose>::from_config(&capture)?,
            )?)
        } else {
            None
        };
        let option_greeks_runtime = if flags.option_greeks {
            Some(BackgroundCaptureRuntime::new(
                capture.clone(),
                CatalogSink::<OptionGreeks>::from_config(&capture)?,
            )?)
        } else {
            None
        };
        let quote_runtime = if flags.quotes {
            Some(BackgroundCaptureRuntime::new(
                capture.clone(),
                CatalogSink::<QuoteTick>::from_config(&capture)?,
            )?)
        } else {
            None
        };
        let trade_runtime = if flags.trades {
            Some(BackgroundCaptureRuntime::new(
                capture.clone(),
                CatalogSink::<TradeTick>::from_config(&capture)?,
            )?)
        } else {
            None
        };
        let bar_runtime = if flags.bars {
            Some(BackgroundCaptureRuntime::new(
                capture.clone(),
                CatalogSink::<Bar>::from_config(&capture)?,
            )?)
        } else {
            None
        };
        let book_delta_runtime = if flags.book_deltas {
            Some(BackgroundCaptureRuntime::new(
                capture.clone(),
                CatalogSink::<OrderBookDelta>::from_config(&capture)?,
            )?)
        } else {
            None
        };
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
            capture: config.capture.clone(),
            initial_materialized_plan,
            supplemental_plan,
            plan: config.plan,
            instrument_runtime,
            custom_data_runtime,
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
            online_option_metrics: config
                .online_option_metrics
                .map(OnlineOptionMetricsObserver::new),
            dynamic_option_universe: config
                .dynamic_option_universe
                .map(DynamicOptionUniverseManager::new),
            dynamic_hip4_universe: config
                .dynamic_hip4_universe
                .map(DynamicHip4UniverseManager::new),
            metrics_snapshot: config.metrics_snapshot,
            metrics_refresh_interval_secs: config.metrics_refresh_interval_secs,
            catalog_root,
            shutdown_completed: false,
        })
    }

    #[must_use]
    pub fn enabled_background_worker_count(&self) -> usize {
        count_spawned_background_workers(self)
    }

    fn submit_instrument(&mut self, instrument: InstrumentAny) -> Result<()> {
        let ts_init = Instrument::ts_init(&instrument).as_u64();
        optional_submit(
            &self.instrument_runtime,
            CaptureItem {
                partition_key: PartitionKey::market_data("instruments", Instrument::id(&instrument)),
                event_ts_ns: ts_init,
                init_ts_ns: Some(ts_init),
                estimated_bytes: size_of::<InstrumentAny>(),
                payload: instrument,
            },
        )
    }

    fn submit_quote(&mut self, quote: QuoteTick) -> Result<()> {
        optional_submit(&self.quote_runtime, CaptureItem {
            partition_key: PartitionKey::catalog_data::<QuoteTick>(quote.instrument_id),
            event_ts_ns: quote.ts_event.as_u64(),
            init_ts_ns: Some(quote.ts_init.as_u64()),
            estimated_bytes: size_of::<QuoteTick>(),
            payload: quote,
        })
    }

    fn submit_custom_data(&mut self, data: CustomData) -> Result<()> {
        let data_type = data.data_type.clone();
        let ts_init = data.data.ts_init().as_u64();
        let event_ts = data.data.ts_event().as_u64();
        optional_submit(&self.custom_data_runtime, CaptureItem {
            partition_key: PartitionKey::custom_data(
                data_type.type_name(),
                data_type.identifier().map(str::to_string),
                data_type.topic(),
            ),
            event_ts_ns: event_ts,
            init_ts_ns: Some(ts_init),
            estimated_bytes: size_of::<CustomData>(),
            payload: data,
        })
    }

    fn submit_mark_price(&mut self, data: MarkPriceUpdate) -> Result<()> {
        optional_submit(&self.mark_price_runtime, CaptureItem {
            partition_key: PartitionKey::catalog_data::<MarkPriceUpdate>(data.instrument_id),
            event_ts_ns: data.ts_event.as_u64(),
            init_ts_ns: Some(data.ts_init.as_u64()),
            estimated_bytes: size_of::<MarkPriceUpdate>(),
            payload: data,
        })
    }

    fn submit_index_price(&mut self, data: IndexPriceUpdate) -> Result<()> {
        optional_submit(&self.index_price_runtime, CaptureItem {
            partition_key: PartitionKey::catalog_data::<IndexPriceUpdate>(data.instrument_id),
            event_ts_ns: data.ts_event.as_u64(),
            init_ts_ns: Some(data.ts_init.as_u64()),
            estimated_bytes: size_of::<IndexPriceUpdate>(),
            payload: data,
        })
    }

    fn submit_funding_rate(&mut self, data: FundingRateUpdate) -> Result<()> {
        optional_submit(&self.funding_rate_runtime, CaptureItem {
            partition_key: PartitionKey::catalog_data::<FundingRateUpdate>(data.instrument_id),
            event_ts_ns: data.ts_event.as_u64(),
            init_ts_ns: Some(data.ts_init.as_u64()),
            estimated_bytes: size_of::<FundingRateUpdate>(),
            payload: data,
        })
    }

    fn submit_instrument_status(&mut self, data: InstrumentStatus) -> Result<()> {
        optional_submit(&self.instrument_status_runtime, CaptureItem {
            partition_key: PartitionKey::catalog_data::<InstrumentStatus>(data.instrument_id),
            event_ts_ns: data.ts_event.as_u64(),
            init_ts_ns: Some(data.ts_init.as_u64()),
            estimated_bytes: size_of::<InstrumentStatus>(),
            payload: data,
        })
    }

    fn submit_instrument_close(&mut self, data: InstrumentClose) -> Result<()> {
        optional_submit(&self.instrument_close_runtime, CaptureItem {
            partition_key: PartitionKey::catalog_data::<InstrumentClose>(data.instrument_id),
            event_ts_ns: data.ts_event.as_u64(),
            init_ts_ns: Some(data.ts_init.as_u64()),
            estimated_bytes: size_of::<InstrumentClose>(),
            payload: data,
        })
    }

    fn submit_option_greeks(&mut self, data: OptionGreeks) -> Result<()> {
        optional_submit(&self.option_greeks_runtime, CaptureItem {
            partition_key: PartitionKey::catalog_data::<OptionGreeks>(data.instrument_id),
            event_ts_ns: data.ts_event.as_u64(),
            init_ts_ns: Some(data.ts_init.as_u64()),
            estimated_bytes: size_of::<OptionGreeks>(),
            payload: data,
        })
    }

    fn persist_forward_price(
        &mut self,
        forward_price: nautilus_model::data::ForwardPrice,
    ) -> Result<()> {
        let record =
            forward_price_record_from_model(&forward_price, "option_greeks_underlying_price");
        append_forward_price_records(&self.catalog_root, std::slice::from_ref(&record))?;
        Ok(())
    }

    fn submit_trade(&mut self, trade: TradeTick) -> Result<()> {
        optional_submit(&self.trade_runtime, CaptureItem {
            partition_key: PartitionKey::catalog_data::<TradeTick>(trade.instrument_id),
            event_ts_ns: trade.ts_event.as_u64(),
            init_ts_ns: Some(trade.ts_init.as_u64()),
            estimated_bytes: size_of::<TradeTick>(),
            payload: trade,
        })
    }

    fn submit_bar(&mut self, bar: Bar) -> Result<()> {
        optional_submit(&self.bar_runtime, CaptureItem {
            partition_key: PartitionKey::catalog_data::<Bar>(bar.bar_type),
            event_ts_ns: bar.ts_event.as_u64(),
            init_ts_ns: Some(bar.ts_init.as_u64()),
            estimated_bytes: size_of::<Bar>(),
            payload: bar,
        })
    }

    /// Bootstrap instrument metadata before market-data subscriptions.
    ///
    /// Adapters load definitions into cache during `connect` (HTTP bulk on Binance,
    /// Bybit, Deribit, OKX), so snapshot cache first. When cache is cold (e.g.
    /// Derive lazy_load), fall back to `request_instrument` and then subscribe for
    /// adapters that support instrument update streams.
    /// Instrument status is subscribed only when declared in `capture.instrument_statuses`.
    fn bootstrap_instruments(&mut self) -> Result<()> {
        for instrument_id in self.plan.planned_instrument_ids() {
            self.bootstrap_instrument(instrument_id)?;
        }

        Ok(())
    }

    fn bootstrap_instrument(&mut self, instrument_id: InstrumentId) -> Result<()> {
        let instrument = { self.cache().instrument(&instrument_id).cloned() };
        if let Some(instrument) = instrument {
            self.on_instrument(&instrument)
        } else {
            self.request_instrument(instrument_id, None, None, None, None)?;
            self.subscribe_instrument(instrument_id, None, None);
            Ok(())
        }
    }

    fn submit_book_deltas(&mut self, deltas: &OrderBookDeltas) -> Result<()> {
        for delta in &deltas.deltas {
            optional_submit(
                &self.book_delta_runtime,
                CaptureItem {
                    partition_key: PartitionKey::catalog_data::<OrderBookDelta>(delta.instrument_id),
                    event_ts_ns: delta.ts_event.as_u64(),
                    init_ts_ns: Some(delta.ts_init.as_u64()),
                    estimated_bytes: size_of::<OrderBookDelta>(),
                    payload: *delta,
                },
            )?;
        }

        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<Vec<FlushResult>> {
        Ok(vec![
            optional_flush_all(&self.instrument_runtime)?,
            optional_flush_all(&self.custom_data_runtime)?,
            optional_flush_all(&self.mark_price_runtime)?,
            optional_flush_all(&self.index_price_runtime)?,
            optional_flush_all(&self.funding_rate_runtime)?,
            optional_flush_all(&self.instrument_status_runtime)?,
            optional_flush_all(&self.instrument_close_runtime)?,
            optional_flush_all(&self.option_greeks_runtime)?,
            optional_flush_all(&self.quote_runtime)?,
            optional_flush_all(&self.trade_runtime)?,
            optional_flush_all(&self.bar_runtime)?,
            optional_flush_all(&self.book_delta_runtime)?,
        ])
    }

    pub fn shutdown_all(&mut self) -> Result<Vec<FlushResult>> {
        if self.shutdown_completed {
            return Ok(Vec::new());
        }

        let results = vec![
            optional_shutdown(&mut self.instrument_runtime)?,
            optional_shutdown(&mut self.custom_data_runtime)?,
            optional_shutdown(&mut self.mark_price_runtime)?,
            optional_shutdown(&mut self.index_price_runtime)?,
            optional_shutdown(&mut self.funding_rate_runtime)?,
            optional_shutdown(&mut self.instrument_status_runtime)?,
            optional_shutdown(&mut self.instrument_close_runtime)?,
            optional_shutdown(&mut self.option_greeks_runtime)?,
            optional_shutdown(&mut self.quote_runtime)?,
            optional_shutdown(&mut self.trade_runtime)?,
            optional_shutdown(&mut self.bar_runtime)?,
            optional_shutdown(&mut self.book_delta_runtime)?,
        ];
        self.shutdown_completed = true;
        Ok(results)
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
        collect_family_metrics("quotes", &self.quote_runtime, &mut families, &mut aggregated);
        collect_family_metrics("trades", &self.trade_runtime, &mut families, &mut aggregated);
        collect_family_metrics("bars", &self.bar_runtime, &mut families, &mut aggregated);
        collect_family_metrics(
            "book_deltas",
            &self.book_delta_runtime,
            &mut families,
            &mut aggregated,
        );

        CaptureMetricsSnapshot {
            captured_at_unix_ms: unix_time_ms(),
            enabled_background_workers: count_spawned_background_workers(self),
            process_rss_bytes: process_rss_bytes(),
            aggregated,
            families,
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
        println!("Capture metrics: {}", metrics.summary_line());
    }

    fn subscribe_plan(&mut self, plan: &CapturePlan) {
        for spec in &plan.custom_data {
            self.subscribe_data(
                spec.data_type.clone(),
                custom_data_client_id(&spec.data_type),
                None,
            );
        }

        for spec in &plan.mark_prices {
            self.subscribe_mark_prices(spec.instrument_id, None, None);
        }

        for spec in &plan.index_prices {
            self.subscribe_index_prices(spec.instrument_id, None, None);
        }

        for spec in &plan.funding_rates {
            self.subscribe_funding_rates(spec.instrument_id, None, None);
        }

        for spec in &plan.instrument_statuses {
            self.subscribe_instrument_status(spec.instrument_id, None, None);
        }

        for spec in &plan.instrument_closes {
            self.subscribe_instrument_close(spec.instrument_id, None, None);
        }

        for spec in &plan.option_greeks {
            self.subscribe_option_greeks(spec.instrument_id, None, None);
        }

        for spec in &plan.quotes {
            self.subscribe_quotes(spec.instrument_id, None, None);
        }

        for spec in &plan.trades {
            self.subscribe_trades(spec.instrument_id, None, None);
        }

        for spec in &plan.bars {
            self.subscribe_bars(spec.bar_type, None, None);
        }

        for spec in &plan.book_deltas {
            self.subscribe_book_deltas(spec.instrument_id, spec.book_type, None, None, false, None);
        }
    }

    fn unsubscribe_plan(&mut self, plan: &CapturePlan) {
        for spec in &plan.custom_data {
            self.unsubscribe_data(
                spec.data_type.clone(),
                custom_data_client_id(&spec.data_type),
                None,
            );
        }

        for spec in &plan.mark_prices {
            self.unsubscribe_mark_prices(spec.instrument_id, None, None);
        }

        for spec in &plan.index_prices {
            self.unsubscribe_index_prices(spec.instrument_id, None, None);
        }

        for spec in &plan.funding_rates {
            self.unsubscribe_funding_rates(spec.instrument_id, None, None);
        }

        for spec in &plan.instrument_statuses {
            self.unsubscribe_instrument_status(spec.instrument_id, None, None);
        }

        for spec in &plan.instrument_closes {
            self.unsubscribe_instrument_close(spec.instrument_id, None, None);
        }

        for spec in &plan.option_greeks {
            self.unsubscribe_option_greeks(spec.instrument_id, None, None);
        }

        for spec in &plan.quotes {
            self.unsubscribe_quotes(spec.instrument_id, None, None);
        }

        for spec in &plan.trades {
            self.unsubscribe_trades(spec.instrument_id, None, None);
        }

        for spec in &plan.bars {
            self.unsubscribe_bars(spec.bar_type, None, None);
        }

        for spec in &plan.book_deltas {
            self.unsubscribe_book_deltas(spec.instrument_id, None, None);
        }
    }

    fn apply_dynamic_option_universe_refresh(&mut self) -> Result<()> {
        if self.dynamic_option_universe.is_none() {
            return Ok(());
        }

        let now = self.clock().timestamp_ns();
        let cache_rc = self.cache_rc();
        let delta = {
            let cache = cache_rc.borrow();
            self.dynamic_option_universe
                .as_mut()
                .expect("checked above")
                .refresh_from_cache(&cache, now)?
        };
        for change in &delta.changes {
            println!(
                "Option universe refresh venue_id={} underlying={} instruments={} -> {} add=[{}] remove=[{}]",
                change.venue_id,
                change.underlying,
                change.previous_count,
                change.next_count,
                change
                    .added_instrument_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
                change
                    .removed_instrument_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        if !delta.is_empty() {
            for change in &delta.changes {
                if let Some(observer) = &mut self.online_option_metrics {
                    observer.apply_universe_change(change);
                }
            }
            if !delta.resolution_records.is_empty() {
                append_option_universe_resolution_records(
                    &self.catalog_root,
                    &delta.resolution_records,
                )?;
            }
            for instrument_id in delta.add.planned_instrument_ids() {
                self.bootstrap_instrument(instrument_id)?;
            }
            self.subscribe_plan(&delta.add);
            self.unsubscribe_plan(&delta.remove);
            self.sync_plan_state();
        }
        Ok(())
    }

    fn apply_dynamic_hip4_universe_refresh(&mut self) -> Result<()> {
        if self.dynamic_hip4_universe.is_none() {
            return Ok(());
        }

        let now = self.clock().timestamp_ns();
        let delta = self
            .dynamic_hip4_universe
            .as_mut()
            .expect("checked above")
            .refresh(now.as_u64())?;
        for change in &delta.changes {
            println!(
                "HIP-4 universe refresh venue_id={} underlying={} question_id={} instruments={} -> {} add=[{}] remove=[{}]",
                change.venue_id,
                change.underlying,
                change.question_id,
                change.previous_count,
                change.next_count,
                change
                    .added_instrument_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
                change
                    .removed_instrument_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        if !delta.is_empty() {
            if !delta.resolution_records.is_empty() {
                append_hip4_universe_resolution_records(
                    &self.catalog_root,
                    &delta.resolution_records,
                )?;
            }
            for instrument_id in delta.add.planned_instrument_ids() {
                self.bootstrap_instrument(instrument_id)?;
            }
            self.subscribe_plan(&delta.add);
            self.unsubscribe_plan(&delta.remove);
            self.sync_plan_state();
        }
        self.schedule_hip4_universe_refresh()?;
        Ok(())
    }

    fn schedule_hip4_universe_refresh(&mut self) -> Result<()> {
        let Some(manager) = &self.dynamic_hip4_universe else {
            return Ok(());
        };

        let now = self.clock().timestamp_ns();
        let delay_secs = manager.next_rotation_check_delay_secs(now.as_u64());
        let alert_time = now + UnixNanos::from(delay_secs.saturating_mul(1_000_000_000));
        self.clock().cancel_timer(HIP4_UNIVERSE_REFRESH_TIMER);
        self.clock()
            .set_time_alert_ns(HIP4_UNIVERSE_REFRESH_TIMER, alert_time, None, None)?;
        Ok(())
    }

    fn schedule_metrics_export(&mut self) -> Result<()> {
        let Some(interval_secs) = self.metrics_refresh_interval_secs else {
            return Ok(());
        };
        if self.metrics_snapshot.is_none() {
            return Ok(());
        }

        let interval_ns = interval_secs.saturating_mul(1_000_000_000);
        self.clock().cancel_timer(METRICS_EXPORT_TIMER);
        self.clock().set_timer_ns(
            METRICS_EXPORT_TIMER,
            interval_ns,
            None,
            None,
            None,
            None,
            None,
        )?;
        Ok(())
    }

    fn schedule_segment_seal(&mut self) -> Result<()> {
        if !self.capture.lifecycle.is_segment_mode() || !self.capture.lifecycle.seal.enabled {
            return Ok(());
        }

        let seal = self
            .capture
            .lifecycle
            .resolved_seal()?
            .expect("enabled seal schedule should resolve");
        let now = self.clock().timestamp_ns();
        let next = next_seal_boundary_ns(now.as_u64(), &seal);
        self.clock().cancel_timer(SEGMENT_SEAL_TIMER);
        self.clock()
            .set_time_alert_ns(SEGMENT_SEAL_TIMER, UnixNanos::from(next), None, None)?;
        Ok(())
    }

    fn seal_segment_runtimes(&mut self) -> Result<()> {
        if !self.capture.lifecycle.is_segment_mode() {
            return Ok(());
        }

        optional_flush_all(&self.instrument_runtime)?;
        optional_flush_all(&self.custom_data_runtime)?;
        optional_seal_all(&self.mark_price_runtime)?;
        optional_seal_all(&self.index_price_runtime)?;
        optional_seal_all(&self.funding_rate_runtime)?;
        optional_seal_all(&self.instrument_status_runtime)?;
        optional_seal_all(&self.instrument_close_runtime)?;
        optional_seal_all(&self.option_greeks_runtime)?;
        optional_seal_all(&self.quote_runtime)?;
        optional_seal_all(&self.trade_runtime)?;
        optional_seal_all(&self.bar_runtime)?;
        optional_seal_all(&self.book_delta_runtime)?;
        Ok(())
    }

    fn effective_capture_plan(&self) -> CapturePlan {
        effective_capture_plan(
            &self.initial_materialized_plan,
            &self.supplemental_plan,
            self.dynamic_option_universe.as_ref(),
            self.dynamic_hip4_universe.as_ref(),
        )
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
                &self.quote_runtime.as_ref().map(BackgroundCaptureRuntime::queue_depth),
            )
            .field(
                "trade_queue_depth",
                &self.trade_runtime.as_ref().map(BackgroundCaptureRuntime::queue_depth),
            )
            .field(
                "bar_queue_depth",
                &self.bar_runtime.as_ref().map(BackgroundCaptureRuntime::queue_depth),
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

impl DataActor for CatalogCaptureActor {
    fn on_start(&mut self) -> Result<()> {
        self.bootstrap_instruments()?;
        let plan = self.plan.clone();
        self.sync_forward_price_targets(&plan);
        self.subscribe_plan(&plan);

        if let Some(manager) = &self.dynamic_option_universe {
            self.clock().set_timer_ns(
                OPTION_UNIVERSE_REFRESH_TIMER,
                manager.refresh_interval_secs() * 1_000_000_000,
                None,
                None,
                None,
                None,
                None,
            )?;
        }
        self.schedule_hip4_universe_refresh()?;
        self.schedule_segment_seal()?;
        self.publish_metrics_snapshot();
        self.schedule_metrics_export()?;

        Ok(())
    }

    fn on_stop(&mut self) -> Result<()> {
        self.clock().cancel_timer(OPTION_UNIVERSE_REFRESH_TIMER);
        self.clock().cancel_timer(HIP4_UNIVERSE_REFRESH_TIMER);
        self.clock().cancel_timer(SEGMENT_SEAL_TIMER);
        self.clock().cancel_timer(METRICS_EXPORT_TIMER);
        let _ = self.shutdown_all()?;
        self.publish_metrics_snapshot();
        self.log_capture_metrics_summary();
        Ok(())
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> Result<()> {
        if event.name == OPTION_UNIVERSE_REFRESH_TIMER {
            self.apply_dynamic_option_universe_refresh()?;
        }
        if event.name == HIP4_UNIVERSE_REFRESH_TIMER {
            self.apply_dynamic_hip4_universe_refresh()?;
        }
        if event.name == SEGMENT_SEAL_TIMER {
            self.seal_segment_runtimes()?;
            self.schedule_segment_seal()?;
        }
        if event.name == METRICS_EXPORT_TIMER {
            self.publish_metrics_snapshot();
            self.schedule_metrics_export()?;
        }
        Ok(())
    }

    fn on_instrument(&mut self, instrument: &InstrumentAny) -> Result<()> {
        self.submit_instrument(instrument.clone())
    }

    fn on_data(&mut self, data: &CustomData) -> Result<()> {
        self.submit_custom_data(data.clone())
    }

    fn on_mark_price(&mut self, mark_price: &MarkPriceUpdate) -> Result<()> {
        self.submit_mark_price(*mark_price)
    }

    fn on_index_price(&mut self, index_price: &IndexPriceUpdate) -> Result<()> {
        self.submit_index_price(*index_price)
    }

    fn on_funding_rate(&mut self, funding_rate: &FundingRateUpdate) -> Result<()> {
        self.submit_funding_rate(*funding_rate)
    }

    fn on_instrument_status(&mut self, data: &InstrumentStatus) -> Result<()> {
        self.submit_instrument_status(*data)
    }

    fn on_instrument_close(&mut self, close: &InstrumentClose) -> Result<()> {
        self.submit_instrument_close(*close)
    }

    fn on_option_greeks(&mut self, greeks: &OptionGreeks) -> Result<()> {
        if let Some(observer) = &mut self.online_option_metrics {
            for line in observer.on_option_greeks(greeks) {
                println!("{line}");
            }
        }
        self.submit_option_greeks(*greeks)?;
        if self.forward_price_targets.contains(&greeks.instrument_id) {
            if let Some(forward_price) = forward_price_from_option_greeks(greeks) {
                self.persist_forward_price(forward_price)?;
            }
        }
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> Result<()> {
        if let Some(observer) = &mut self.online_option_metrics {
            for line in observer.on_quote(quote) {
                println!("{line}");
            }
        }
        self.submit_quote(*quote)?;
        Ok(())
    }

    fn on_trade(&mut self, trade: &TradeTick) -> Result<()> {
        self.submit_trade(*trade)?;
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> Result<()> {
        self.submit_bar(*bar)?;
        Ok(())
    }

    fn on_book_deltas(&mut self, deltas: &OrderBookDeltas) -> Result<()> {
        self.submit_book_deltas(deltas)
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
    use std::{fs, str::FromStr};

    use nautilus_model::identifiers::InstrumentId;

    use catalog_capture_core::{
        plan::{CapturePlan, QuoteCaptureSpec},
        CaptureConfig,
    };

    use super::*;

    #[test]
    fn effective_capture_plan_merges_option_and_hip4_manager_views() {
        let static_plan = CapturePlan::default();
        let option_dynamic = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: InstrumentId::from_str("BTC-OPT.DERIBIT").unwrap(),
            }],
            ..CapturePlan::default()
        };
        let hip4_dynamic = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: InstrumentId::from_str("BTC-HIP4.HYPERLIQUID").unwrap(),
            }],
            ..CapturePlan::default()
        };
        let initial = merge_capture_plans(
            &merge_capture_plans(&static_plan, &option_dynamic),
            &hip4_dynamic,
        );

        let option_manager = DynamicOptionUniverseManager::new(DynamicOptionUniverseConfig {
            refresh_interval_secs: 60,
            strike_change_confirmations: 0,
            static_plan: static_plan.clone(),
            initial_dynamic_plan: option_dynamic,
            universes: vec![],
        });
        let hip4_manager = DynamicHip4UniverseManager::new(DynamicHip4UniverseConfig {
            idle_poll_secs: 1800,
            active_poll_secs: 10,
            pre_expiry_window_secs: 900,
            http_timeout_secs: 10,
            static_plan: static_plan.clone(),
            initial_dynamic_plan: hip4_dynamic,
            universes: vec![],
        });

        let merged = effective_capture_plan(
            &initial,
            &CapturePlan::default(),
            Some(&option_manager),
            Some(&hip4_manager),
        );

        assert_eq!(merged.quotes.len(), 2);
        assert!(merged.quotes.iter().any(|spec| {
            spec.instrument_id
                == InstrumentId::from_str("BTC-OPT.DERIBIT").unwrap()
        }));
        assert!(merged.quotes.iter().any(|spec| {
            spec.instrument_id
                == InstrumentId::from_str("BTC-HIP4.HYPERLIQUID").unwrap()
        }));
    }

    #[test]
    fn supplemental_plan_preserves_hip4_when_only_option_refresh_enabled() {
        let static_plan = CapturePlan::default();
        let option_dynamic = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: InstrumentId::from_str("BTC-OPT.DERIBIT").unwrap(),
            }],
            ..CapturePlan::default()
        };
        let hip4_only = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: InstrumentId::from_str("BTC-HIP4.HYPERLIQUID").unwrap(),
            }],
            ..CapturePlan::default()
        };
        let initial = merge_capture_plans(
            &merge_capture_plans(&static_plan, &option_dynamic),
            &hip4_only,
        );
        let option_config = DynamicOptionUniverseConfig {
            refresh_interval_secs: 60,
            strike_change_confirmations: 0,
            static_plan: static_plan.clone(),
            initial_dynamic_plan: option_dynamic,
            universes: vec![],
        };

        let supplemental = supplemental_capture_plan(&initial, &Some(option_config), &None);

        assert_eq!(supplemental.quotes.len(), 1);
        assert_eq!(
            supplemental.quotes[0].instrument_id,
            InstrumentId::from_str("BTC-HIP4.HYPERLIQUID").unwrap()
        );
    }

    #[test]
    fn actor_starts_only_plan_enabled_background_workers() {
        let catalog_dir = std::env::temp_dir().join("catalog-capture-actor-lazy-runtime-test");
        fs::create_dir_all(&catalog_dir).expect("temp catalog dir should exist");
        let plan = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: InstrumentId::from_str("BTCUSDT-PERP.BINANCE").unwrap(),
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
}
