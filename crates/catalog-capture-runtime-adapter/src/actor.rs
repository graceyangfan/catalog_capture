use std::{fmt::Debug, mem::size_of};

use anyhow::Result;
use catalog_capture_core::{
    background::BackgroundCaptureRuntime,
    config::CaptureConfig,
    item::{CaptureItem, PartitionKey},
    plan::CapturePlan,
    runtime::FlushResult,
    sink::NautilusCatalogSink,
};
use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    nautilus_actor,
};
use nautilus_model::{
    data::{
        Bar, CustomData, FundingRateUpdate, IndexPriceUpdate, InstrumentStatus, MarkPriceUpdate,
        OptionGreeks, OrderBookDelta, OrderBookDeltas, QuoteTick, TradeTick,
        close::InstrumentClose,
    },
    identifiers::ActorId,
    instruments::{Instrument, InstrumentAny},
};

#[derive(Debug, Clone)]
pub struct CatalogCaptureActorConfig {
    pub actor_id: Option<ActorId>,
    pub capture: CaptureConfig,
    pub plan: CapturePlan,
}

impl CatalogCaptureActorConfig {
    #[must_use]
    pub fn new(capture: CaptureConfig, plan: CapturePlan) -> Self {
        Self {
            actor_id: None,
            capture,
            plan,
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

        Ok(Self {
            core: DataActorCore::new(actor_config),
            plan: config.plan,
            instrument_runtime: BackgroundCaptureRuntime::new(config.capture.clone(), instrument_sink),
            custom_data_runtime: BackgroundCaptureRuntime::new(config.capture.clone(), custom_data_sink),
            mark_price_runtime: BackgroundCaptureRuntime::new(config.capture.clone(), mark_price_sink),
            index_price_runtime: BackgroundCaptureRuntime::new(config.capture.clone(), index_price_sink),
            funding_rate_runtime: BackgroundCaptureRuntime::new(config.capture.clone(), funding_rate_sink),
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
}

nautilus_actor!(CatalogCaptureActor);

impl Debug for CatalogCaptureActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogCaptureActor")
            .field("plan", &self.plan)
            .field("instrument_queue_depth", &self.instrument_runtime.queue_depth())
            .field("custom_data_queue_depth", &self.custom_data_runtime.queue_depth())
            .field("mark_price_queue_depth", &self.mark_price_runtime.queue_depth())
            .field("index_price_queue_depth", &self.index_price_runtime.queue_depth())
            .field("funding_rate_queue_depth", &self.funding_rate_runtime.queue_depth())
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
            .finish()
    }
}

impl DataActor for CatalogCaptureActor {
    fn on_start(&mut self) -> Result<()> {
        let instrument_specs = self.plan.instruments.clone();
        for spec in instrument_specs {
            self.subscribe_instrument(spec.instrument_id, None, None);
        }

        let custom_data_specs = self.plan.custom_data.clone();
        for spec in custom_data_specs {
            self.subscribe_data(spec.data_type, None, None);
        }

        let mark_price_specs = self.plan.mark_prices.clone();
        for spec in mark_price_specs {
            self.subscribe_mark_prices(spec.instrument_id, None, None);
        }

        let index_price_specs = self.plan.index_prices.clone();
        for spec in index_price_specs {
            self.subscribe_index_prices(spec.instrument_id, None, None);
        }

        let funding_rate_specs = self.plan.funding_rates.clone();
        for spec in funding_rate_specs {
            self.subscribe_funding_rates(spec.instrument_id, None, None);
        }

        let instrument_status_specs = self.plan.instrument_statuses.clone();
        for spec in instrument_status_specs {
            self.subscribe_instrument_status(spec.instrument_id, None, None);
        }

        let instrument_close_specs = self.plan.instrument_closes.clone();
        for spec in instrument_close_specs {
            self.subscribe_instrument_close(spec.instrument_id, None, None);
        }

        let option_greeks_specs = self.plan.option_greeks.clone();
        for spec in option_greeks_specs {
            self.subscribe_option_greeks(spec.instrument_id, None, None);
        }

        let quote_specs = self.plan.quotes.clone();
        for spec in quote_specs {
            self.subscribe_quotes(spec.instrument_id, None, None);
        }

        let trade_specs = self.plan.trades.clone();
        for spec in trade_specs {
            self.subscribe_trades(spec.instrument_id, None, None);
        }

        let bar_specs = self.plan.bars.clone();
        for spec in bar_specs {
            self.subscribe_bars(spec.bar_type, None, None);
        }

        let delta_specs = self.plan.book_deltas.clone();
        for spec in delta_specs {
            self.subscribe_book_deltas(
                spec.instrument_id,
                spec.book_type,
                None,
                None,
                false,
                None,
            );
        }

        Ok(())
    }

    fn on_stop(&mut self) -> Result<()> {
        let _ = self.shutdown_all()?;
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
        let _ = self.submit_option_greeks(greeks.clone())?;
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> Result<()> {
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
