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
mod submit;

use std::{
    collections::BTreeSet,
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
        process_rss_bytes, unix_time_ms, CaptureMetricsSnapshot, CustomDataRequestJobMetrics,
    },
    plan::CapturePlan,
    sink::{
        chunked_catalog_sink_from_config, custom_data_catalog_sink_from_config, CatalogSink,
        ChunkedCatalogSink, CustomDataCatalogSink,
    },
};
use crate::actor_runtime::maybe_family_runtime;
use nautilus_common::{
    actor::{DataActorConfig, DataActorCore, DataActorNative},
    nautilus_actor,
};
use nautilus_model::{
    data::{
        close::InstrumentClose, Bar, CustomData, FundingRateUpdate, IndexPriceUpdate,
        InstrumentStatus, MarkPriceUpdate, OptionGreeks, OrderBookDelta, OrderBookDeltas,
        QuoteTick, TradeTick,
    },
    identifiers::{ActorId, ClientId, InstrumentId},
    instruments::{Instrument, InstrumentAny},
};

use crate::actor_plan::{
    count_enabled_background_workers, effective_capture_plan, supplemental_capture_plan,
};
use crate::actor_runtime::collect_family_metrics;
use crate::custom_data_requests::CustomDataRequestJob;
use crate::dynamic_hip4_universe::{DynamicHip4UniverseConfig, DynamicHip4UniverseManager};
use crate::dynamic_option_universe::{DynamicOptionUniverseConfig, DynamicOptionUniverseManager};
use crate::online_option_metrics::{OnlineOptionMetricsConfig, OnlineOptionMetricsObserver};
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
    /// Subscribe + request custom payloads share one writer. Segment mode uses
    /// `*.parquet.part` + wall-clock seal (same as market data); chunked mode keeps
    /// immediate catalog parquet files.
    custom_data_runtime: Option<BackgroundCaptureRuntime<CustomData, CustomDataCatalogSink>>,
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
    book_delta_runtime:
        Option<BackgroundCaptureRuntime<OrderBookDelta, CatalogSink<OrderBookDelta>>>,
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
        log::info!("Capture background workers: {worker_count} enabled for plan");

        // Instruments: always chunked. Custom: segment when mode=segment (shared for
        // subscribe + request). Market families: CatalogSink + per-family flush profile.
        let capture = config.capture.clone();

        let instrument_runtime = maybe_family_runtime(
            flags.instruments,
            CaptureFlushFamily::Instruments,
            &capture,
            chunked_catalog_sink_from_config,
        )?;
        let custom_data_runtime = maybe_family_runtime(
            flags.needs_custom_data_writer(),
            CaptureFlushFamily::CustomData,
            &capture,
            custom_data_catalog_sink_from_config,
        )?;
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
            flags.index_prices,
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
            flags.trades,
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
        let book_delta_runtime = maybe_family_runtime(
            flags.book_deltas,
            CaptureFlushFamily::BookDeltas,
            &capture,
            CatalogSink::<OrderBookDelta>::from_config,
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
            custom_data_request_jobs,
            pending_market_data: BTreeSet::new(),
            market_data_live: BTreeSet::new(),
            pending_market_data_backoff_secs: PENDING_MD_BACKOFF_START_SECS,
            metrics_snapshot: config.metrics_snapshot,
            metrics_refresh_interval_secs: config.metrics_refresh_interval_secs,
            catalog_root,
            shutdown_completed: false,
        })
    }

    #[must_use]
    pub fn enabled_background_worker_count(&self) -> usize {
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
        ])
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
        plan::{CapturePlan, QuoteCaptureSpec},
        CaptureConfig,
    };
    use nautilus_common::{
        actor::{Component, DataActor},
        cache::Cache,
        clock::TestClock,
    };
    use nautilus_model::{
        data::QuoteTick,
        identifiers::{InstrumentId, TraderId},
        instruments::{stubs::audusd_sim, InstrumentAny},
        stubs::TestDefault,
    };

    use super::*;
    use crate::DynamicHip4UniverseChange;

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

        let clock = Rc::new(RefCell::new(TestClock::new()));
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
                .add_quote(QuoteTick {
                    instrument_id,
                    ..QuoteTick::default()
                })
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

        let clock = Rc::new(RefCell::new(TestClock::new()));
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

        let clock = Rc::new(RefCell::new(TestClock::new()));
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
            c.add_instrument(InstrumentAny::CurrencyPair(perp))
                .unwrap();
            c.add_quote(QuoteTick {
                instrument_id: outcome_id,
                ..QuoteTick::default()
            })
            .unwrap();
            c.add_quote(QuoteTick {
                instrument_id: perp_id,
                ..QuoteTick::default()
            })
            .unwrap();
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
        actor.dynamic_option_universe =
            Some(DynamicOptionUniverseManager::new(DynamicOptionUniverseConfig {
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
            }));

        let clock = Rc::new(RefCell::new(TestClock::new()));
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
            cache
                .borrow_mut()
                .add_quote(QuoteTick {
                    instrument_id: id,
                    ..QuoteTick::default()
                })
                .unwrap();
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
}
