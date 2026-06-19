use std::{fmt::Debug, mem::size_of};

use anyhow::Result;
use std::path::PathBuf;

use catalog_capture_core::{
    append_option_universe_resolution_records, background::BackgroundCaptureRuntime,
    catalog_root_from_uri, config::CaptureConfig, item::{CaptureItem, PartitionKey},
    plan::CapturePlan, runtime::FlushResult, sink::NautilusCatalogSink,
};
use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    nautilus_actor,
    timer::TimeEvent,
};
use nautilus_model::{
    data::{
        close::InstrumentClose, Bar, CustomData, FundingRateUpdate, IndexPriceUpdate,
        InstrumentStatus, MarkPriceUpdate, OptionGreeks, OrderBookDelta, OrderBookDeltas,
        QuoteTick, TradeTick,
    },
    identifiers::{ActorId, InstrumentId},
    instruments::{Instrument, InstrumentAny},
};

use crate::dynamic_option_universe::{DynamicOptionUniverseConfig, DynamicOptionUniverseManager};
use crate::online_option_metrics::{OnlineOptionMetricsConfig, OnlineOptionMetricsObserver};

const OPTION_UNIVERSE_REFRESH_TIMER: &str = "OPTION_UNIVERSE_REFRESH";

#[derive(Debug, Clone)]
pub struct CatalogCaptureActorConfig {
    pub actor_id: Option<ActorId>,
    pub capture: CaptureConfig,
    pub plan: CapturePlan,
    pub online_option_metrics: Option<OnlineOptionMetricsConfig>,
    pub dynamic_option_universe: Option<DynamicOptionUniverseConfig>,
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
        }
    }
}

pub struct CatalogCaptureActor {
    core: DataActorCore,
    plan: CapturePlan,
    instrument_runtime: BackgroundCaptureRuntime<InstrumentAny, NautilusCatalogSink>,
    custom_data_runtime: BackgroundCaptureRuntime<CustomData, NautilusCatalogSink>,
    mark_price_runtime: BackgroundCaptureRuntime<MarkPriceUpdate, NautilusCatalogSink>,
    index_price_runtime: BackgroundCaptureRuntime<IndexPriceUpdate, NautilusCatalogSink>,
    funding_rate_runtime: BackgroundCaptureRuntime<FundingRateUpdate, NautilusCatalogSink>,
    instrument_status_runtime: BackgroundCaptureRuntime<InstrumentStatus, NautilusCatalogSink>,
    instrument_close_runtime: BackgroundCaptureRuntime<InstrumentClose, NautilusCatalogSink>,
    option_greeks_runtime: BackgroundCaptureRuntime<OptionGreeks, NautilusCatalogSink>,
    quote_runtime: BackgroundCaptureRuntime<QuoteTick, NautilusCatalogSink>,
    trade_runtime: BackgroundCaptureRuntime<TradeTick, NautilusCatalogSink>,
    bar_runtime: BackgroundCaptureRuntime<Bar, NautilusCatalogSink>,
    book_delta_runtime: BackgroundCaptureRuntime<OrderBookDelta, NautilusCatalogSink>,
    online_option_metrics: Option<OnlineOptionMetricsObserver>,
    dynamic_option_universe: Option<DynamicOptionUniverseManager>,
    catalog_root: PathBuf,
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

        let instrument_sink = NautilusCatalogSink::from_config(&config.capture)?;
        let custom_data_sink = NautilusCatalogSink::from_config(&config.capture)?;
        let mark_price_sink = NautilusCatalogSink::from_config(&config.capture)?;
        let index_price_sink = NautilusCatalogSink::from_config(&config.capture)?;
        let funding_rate_sink = NautilusCatalogSink::from_config(&config.capture)?;
        let instrument_status_sink = NautilusCatalogSink::from_config(&config.capture)?;
        let instrument_close_sink = NautilusCatalogSink::from_config(&config.capture)?;
        let option_greeks_sink = NautilusCatalogSink::from_config(&config.capture)?;
        let quote_sink = NautilusCatalogSink::from_config(&config.capture)?;
        let trade_sink = NautilusCatalogSink::from_config(&config.capture)?;
        let bar_sink = NautilusCatalogSink::from_config(&config.capture)?;
        let book_delta_sink = NautilusCatalogSink::from_config(&config.capture)?;
        let catalog_root = catalog_root_from_uri(&config.capture.catalog_uri)?;

        Ok(Self {
            core: DataActorCore::new(actor_config),
            plan: config.plan,
            instrument_runtime: BackgroundCaptureRuntime::new(
                config.capture.clone(),
                instrument_sink,
            ),
            custom_data_runtime: BackgroundCaptureRuntime::new(
                config.capture.clone(),
                custom_data_sink,
            ),
            mark_price_runtime: BackgroundCaptureRuntime::new(
                config.capture.clone(),
                mark_price_sink,
            ),
            index_price_runtime: BackgroundCaptureRuntime::new(
                config.capture.clone(),
                index_price_sink,
            ),
            funding_rate_runtime: BackgroundCaptureRuntime::new(
                config.capture.clone(),
                funding_rate_sink,
            ),
            instrument_status_runtime: BackgroundCaptureRuntime::new(
                config.capture.clone(),
                instrument_status_sink,
            ),
            instrument_close_runtime: BackgroundCaptureRuntime::new(
                config.capture.clone(),
                instrument_close_sink,
            ),
            option_greeks_runtime: BackgroundCaptureRuntime::new(
                config.capture.clone(),
                option_greeks_sink,
            ),
            quote_runtime: BackgroundCaptureRuntime::new(config.capture.clone(), quote_sink),
            trade_runtime: BackgroundCaptureRuntime::new(config.capture.clone(), trade_sink),
            bar_runtime: BackgroundCaptureRuntime::new(config.capture.clone(), bar_sink),
            book_delta_runtime: BackgroundCaptureRuntime::new(config.capture, book_delta_sink),
            online_option_metrics: config
                .online_option_metrics
                .map(OnlineOptionMetricsObserver::new),
            dynamic_option_universe: config
                .dynamic_option_universe
                .map(DynamicOptionUniverseManager::new),
            catalog_root,
        })
    }

    fn submit_instrument(&mut self, instrument: InstrumentAny) -> Result<FlushResult> {
        let ts_init = Instrument::ts_init(&instrument).as_u64();
        self.instrument_runtime.submit(CaptureItem {
            partition_key: PartitionKey::market_data("instruments", Instrument::id(&instrument)),
            event_ts_ns: ts_init,
            init_ts_ns: Some(ts_init),
            estimated_bytes: size_of::<InstrumentAny>(),
            payload: instrument,
        })?;
        Ok(FlushResult::default())
    }

    fn submit_quote(&mut self, quote: QuoteTick) -> Result<FlushResult> {
        self.quote_runtime.submit(CaptureItem {
            partition_key: PartitionKey::market_data("quotes", quote.instrument_id),
            event_ts_ns: quote.ts_event.as_u64(),
            init_ts_ns: Some(quote.ts_init.as_u64()),
            estimated_bytes: size_of::<QuoteTick>(),
            payload: quote,
        })?;
        Ok(FlushResult::default())
    }

    fn submit_custom_data(&mut self, data: CustomData) -> Result<FlushResult> {
        let data_type = data.data_type.clone();
        let ts_init = data.data.ts_init().as_u64();
        let event_ts = data.data.ts_event().as_u64();
        self.custom_data_runtime.submit(CaptureItem {
            partition_key: PartitionKey::custom_data(
                data_type.type_name(),
                data_type.identifier().map(str::to_string),
                data_type.topic(),
            ),
            event_ts_ns: event_ts,
            init_ts_ns: Some(ts_init),
            estimated_bytes: size_of::<CustomData>(),
            payload: data,
        })?;
        Ok(FlushResult::default())
    }

    fn submit_mark_price(&mut self, data: MarkPriceUpdate) -> Result<FlushResult> {
        self.mark_price_runtime.submit(CaptureItem {
            partition_key: PartitionKey::market_data("mark_prices", data.instrument_id),
            event_ts_ns: data.ts_event.as_u64(),
            init_ts_ns: Some(data.ts_init.as_u64()),
            estimated_bytes: size_of::<MarkPriceUpdate>(),
            payload: data,
        })?;
        Ok(FlushResult::default())
    }

    fn submit_index_price(&mut self, data: IndexPriceUpdate) -> Result<FlushResult> {
        self.index_price_runtime.submit(CaptureItem {
            partition_key: PartitionKey::market_data("index_prices", data.instrument_id),
            event_ts_ns: data.ts_event.as_u64(),
            init_ts_ns: Some(data.ts_init.as_u64()),
            estimated_bytes: size_of::<IndexPriceUpdate>(),
            payload: data,
        })?;
        Ok(FlushResult::default())
    }

    fn submit_funding_rate(&mut self, data: FundingRateUpdate) -> Result<FlushResult> {
        self.funding_rate_runtime.submit(CaptureItem {
            partition_key: PartitionKey::market_data("funding_rate_update", data.instrument_id),
            event_ts_ns: data.ts_event.as_u64(),
            init_ts_ns: Some(data.ts_init.as_u64()),
            estimated_bytes: size_of::<FundingRateUpdate>(),
            payload: data,
        })?;
        Ok(FlushResult::default())
    }

    fn submit_instrument_status(&mut self, data: InstrumentStatus) -> Result<FlushResult> {
        self.instrument_status_runtime.submit(CaptureItem {
            partition_key: PartitionKey::market_data("instrument_status", data.instrument_id),
            event_ts_ns: data.ts_event.as_u64(),
            init_ts_ns: Some(data.ts_init.as_u64()),
            estimated_bytes: size_of::<InstrumentStatus>(),
            payload: data,
        })?;
        Ok(FlushResult::default())
    }

    fn submit_instrument_close(&mut self, data: InstrumentClose) -> Result<FlushResult> {
        self.instrument_close_runtime.submit(CaptureItem {
            partition_key: PartitionKey::market_data("instrument_closes", data.instrument_id),
            event_ts_ns: data.ts_event.as_u64(),
            init_ts_ns: Some(data.ts_init.as_u64()),
            estimated_bytes: size_of::<InstrumentClose>(),
            payload: data,
        })?;
        Ok(FlushResult::default())
    }

    fn submit_option_greeks(&mut self, data: OptionGreeks) -> Result<FlushResult> {
        self.option_greeks_runtime.submit(CaptureItem {
            partition_key: PartitionKey::market_data("option_greeks", data.instrument_id),
            event_ts_ns: data.ts_event.as_u64(),
            init_ts_ns: Some(data.ts_init.as_u64()),
            estimated_bytes: size_of::<OptionGreeks>(),
            payload: data,
        })?;
        Ok(FlushResult::default())
    }

    fn submit_trade(&mut self, trade: TradeTick) -> Result<FlushResult> {
        self.trade_runtime.submit(CaptureItem {
            partition_key: PartitionKey::market_data("trades", trade.instrument_id),
            event_ts_ns: trade.ts_event.as_u64(),
            init_ts_ns: Some(trade.ts_init.as_u64()),
            estimated_bytes: size_of::<TradeTick>(),
            payload: trade,
        })?;
        Ok(FlushResult::default())
    }

    fn submit_bar(&mut self, bar: Bar) -> Result<FlushResult> {
        self.bar_runtime.submit(CaptureItem {
            partition_key: PartitionKey::market_data("bars", bar.bar_type),
            event_ts_ns: bar.ts_event.as_u64(),
            init_ts_ns: Some(bar.ts_init.as_u64()),
            estimated_bytes: size_of::<Bar>(),
            payload: bar,
        })?;
        Ok(FlushResult::default())
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
            self.book_delta_runtime.submit(CaptureItem {
                partition_key: PartitionKey::market_data("book_deltas", delta.instrument_id),
                event_ts_ns: delta.ts_event.as_u64(),
                init_ts_ns: Some(delta.ts_init.as_u64()),
                estimated_bytes: size_of::<OrderBookDelta>(),
                payload: *delta,
            })?;
        }

        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<Vec<FlushResult>> {
        Ok(vec![
            self.instrument_runtime.flush_all()?,
            self.custom_data_runtime.flush_all()?,
            self.mark_price_runtime.flush_all()?,
            self.index_price_runtime.flush_all()?,
            self.funding_rate_runtime.flush_all()?,
            self.instrument_status_runtime.flush_all()?,
            self.instrument_close_runtime.flush_all()?,
            self.option_greeks_runtime.flush_all()?,
            self.quote_runtime.flush_all()?,
            self.trade_runtime.flush_all()?,
            self.bar_runtime.flush_all()?,
            self.book_delta_runtime.flush_all()?,
        ])
    }

    pub fn shutdown_all(&mut self) -> Result<Vec<FlushResult>> {
        Ok(vec![
            self.instrument_runtime.shutdown()?,
            self.custom_data_runtime.shutdown()?,
            self.mark_price_runtime.shutdown()?,
            self.index_price_runtime.shutdown()?,
            self.funding_rate_runtime.shutdown()?,
            self.instrument_status_runtime.shutdown()?,
            self.instrument_close_runtime.shutdown()?,
            self.option_greeks_runtime.shutdown()?,
            self.quote_runtime.shutdown()?,
            self.trade_runtime.shutdown()?,
            self.bar_runtime.shutdown()?,
            self.book_delta_runtime.shutdown()?,
        ])
    }

    fn subscribe_plan(&mut self, plan: &CapturePlan) {
        for spec in &plan.custom_data {
            self.subscribe_data(spec.data_type.clone(), None, None);
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
            self.unsubscribe_data(spec.data_type.clone(), None, None);
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
        let active_plan = self
            .dynamic_option_universe
            .as_ref()
            .expect("checked above")
            .active_capture_plan();
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
            self.plan = active_plan;
        }
        Ok(())
    }
}

nautilus_actor!(CatalogCaptureActor);

impl Debug for CatalogCaptureActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogCaptureActor")
            .field("plan", &self.plan)
            .field(
                "instrument_queue_depth",
                &self.instrument_runtime.queue_depth(),
            )
            .field(
                "custom_data_queue_depth",
                &self.custom_data_runtime.queue_depth(),
            )
            .field(
                "mark_price_queue_depth",
                &self.mark_price_runtime.queue_depth(),
            )
            .field(
                "index_price_queue_depth",
                &self.index_price_runtime.queue_depth(),
            )
            .field(
                "funding_rate_queue_depth",
                &self.funding_rate_runtime.queue_depth(),
            )
            .field(
                "instrument_status_queue_depth",
                &self.instrument_status_runtime.queue_depth(),
            )
            .field(
                "instrument_close_queue_depth",
                &self.instrument_close_runtime.queue_depth(),
            )
            .field(
                "option_greeks_queue_depth",
                &self.option_greeks_runtime.queue_depth(),
            )
            .field("quote_queue_depth", &self.quote_runtime.queue_depth())
            .field("trade_queue_depth", &self.trade_runtime.queue_depth())
            .field("bar_queue_depth", &self.bar_runtime.queue_depth())
            .field(
                "book_delta_queue_depth",
                &self.book_delta_runtime.queue_depth(),
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

        Ok(())
    }

    fn on_stop(&mut self) -> Result<()> {
        self.clock().cancel_timer(OPTION_UNIVERSE_REFRESH_TIMER);
        let _ = self.shutdown_all()?;
        Ok(())
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> Result<()> {
        if event.name == OPTION_UNIVERSE_REFRESH_TIMER {
            self.apply_dynamic_option_universe_refresh()?;
        }
        Ok(())
    }

    fn on_instrument(&mut self, instrument: &InstrumentAny) -> Result<()> {
        let _ = self.submit_instrument(instrument.clone())?;
        Ok(())
    }

    fn on_data(&mut self, data: &CustomData) -> Result<()> {
        let _ = self.submit_custom_data(data.clone())?;
        Ok(())
    }

    fn on_mark_price(&mut self, mark_price: &MarkPriceUpdate) -> Result<()> {
        let _ = self.submit_mark_price(*mark_price)?;
        Ok(())
    }

    fn on_index_price(&mut self, index_price: &IndexPriceUpdate) -> Result<()> {
        let _ = self.submit_index_price(*index_price)?;
        Ok(())
    }

    fn on_funding_rate(&mut self, funding_rate: &FundingRateUpdate) -> Result<()> {
        let _ = self.submit_funding_rate(*funding_rate)?;
        Ok(())
    }

    fn on_instrument_status(&mut self, data: &InstrumentStatus) -> Result<()> {
        let _ = self.submit_instrument_status(*data)?;
        Ok(())
    }

    fn on_instrument_close(&mut self, close: &InstrumentClose) -> Result<()> {
        let _ = self.submit_instrument_close(*close)?;
        Ok(())
    }

    fn on_option_greeks(&mut self, greeks: &OptionGreeks) -> Result<()> {
        if let Some(observer) = &mut self.online_option_metrics {
            for line in observer.on_option_greeks(greeks) {
                println!("{line}");
            }
        }
        let _ = self.submit_option_greeks(greeks.clone())?;
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> Result<()> {
        if let Some(observer) = &mut self.online_option_metrics {
            for line in observer.on_quote(quote) {
                println!("{line}");
            }
        }
        let _ = self.submit_quote(*quote)?;
        Ok(())
    }

    fn on_trade(&mut self, trade: &TradeTick) -> Result<()> {
        let _ = self.submit_trade(*trade)?;
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> Result<()> {
        let _ = self.submit_bar(*bar)?;
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
        let _ = self.shutdown_all();
    }
}
