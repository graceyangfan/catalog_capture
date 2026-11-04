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

//! Predict-style handover: discover current ID, subscribe, confirm both books, retire old ID.
use super::*;
use crate::{actor_runtime::optional_flush_all, hip4_rolling::Hip4RollingState};
use catalog_capture_core::{capture_plan_difference, hip4::rolling::select_rolling_market};
use nautilus_common::{
    actor::DataActor,
    msgbus::{self, switchboard},
};

pub(super) const TIMER: &str = "HIP4_ROLLING";

impl CatalogCaptureActor {
    pub(super) fn request_hip4_instruments(&mut self) -> Result<()> {
        let now = self.clock().timestamp_ns().as_u64();
        if self
            .hip4_rolling
            .instrument_request
            .is_some_and(|(_, deadline)| now < deadline)
        {
            return Ok(());
        }
        // Native refreshes the whole registry even for one instrument request. One tagged
        // request covers YES/NO and both assets; the watchdog only retries a lost response.
        let params = nautilus_core::Params::new(
            [("hip4_rolling_capture".to_string(), serde_json::json!(true))]
                .into_iter()
                .collect(),
        );
        let request = self.request_instruments(
            Some(nautilus_model::identifiers::Venue::from("HYPERLIQUID")),
            None,
            None,
            None,
            Some(params),
        )?;
        self.hip4_rolling.instrument_request = Some((request, now.saturating_add(90_000_000_000)));
        Ok(())
    }

    pub(super) fn start_hip4_rolling(&mut self) -> Result<()> {
        if self.hip4_rolling.config.is_none() {
            return Ok(());
        }
        self.clock().set_timer_ns(
            TIMER,
            nautilus_core::DurationNanos::new(1_000_000_000),
            None,
            None,
            None,
            None,
            None,
        )?;
        self.refresh_hip4_rolling()
    }
    pub(super) fn refresh_hip4_rolling(&mut self) -> Result<()> {
        let Some(config) = self.hip4_rolling.config.clone() else {
            return Ok(());
        };
        let now = self.clock().timestamp_ns().as_u64();
        // Unsubscribe commands from the preceding actor turn have now been applied.
        for id in std::mem::take(&mut self.hip4_rolling.retiring) {
            if !self.plan.planned_instrument_ids().contains(&id)
                && msgbus::subscriber_count_deltas(switchboard::get_book_deltas_topic(id)) == 0
                && msgbus::exact_subscriber_count_trades(switchboard::get_trades_topic(id)) == 0
                && msgbus::subscriptions_count_any(switchboard::get_instrument_topic(id).as_str())?
                    == 0
            {
                self.purge_instrument_from_cache(id);
            }
        }
        let result = self.hip4_rolling.discovery.as_ref().and_then(|pending| {
            match pending.receiver.try_recv() {
                Ok(result) => Some(result),
                Err(std::sync::mpsc::TryRecvError::Empty) => None,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    Some(Err("discovery worker disconnected".into()))
                }
            }
        });
        if let Some(result) = result {
            self.hip4_rolling.discovery = None;
            self.hip4_rolling.next_discovery_ns =
                now.saturating_add(config.retry_secs * 1_000_000_000);
            match result {
                Ok(payload) => self.apply_hip4_rolling_payload(&payload)?,
                Err(error) => log::warn!("HIP-4 discovery retry: {error}"),
            }
        }
        self.reconcile_hip4_rolling()?;
        let due = config.underlyings.iter().any(|asset| {
            let Some(series) = self.hip4_rolling.series.get(asset) else {
                return true;
            };
            series.pending.is_none()
                && now >= series.retry_after
                && series
                    .current
                    .and_then(|id| self.hip4_rolling.markets.get(&id))
                    .is_none_or(|m| m.market.end_ns <= now)
        });
        if due
            && self.hip4_rolling.discovery.is_none()
            && now >= self.hip4_rolling.next_discovery_ns
        {
            self.hip4_rolling.discovery = Some(crate::hip4_rolling::discover()?);
        }
        Ok(())
    }
    pub(super) fn apply_hip4_rolling_payload(&mut self, payload: &serde_json::Value) -> Result<()> {
        let config = self.hip4_rolling.config.clone().expect("HIP-4 configured");
        let now = self.clock().timestamp_ns();
        for asset in &config.underlyings {
            let market = match select_rolling_market(payload, asset, now.as_u64()) {
                Ok(Some(market)) => market,
                Ok(None) => continue,
                Err(error) => {
                    log::warn!("HIP-4 {asset} selection retry: {error:#}");
                    continue;
                }
            };
            let series = self.hip4_rolling.series.entry(asset.clone()).or_default();
            if series.current == Some(market.outcome)
                || series.pending.is_some()
                || now.as_u64() < series.retry_after
            {
                continue;
            }
            let id = market.outcome;
            let recording = Hip4RollingState::recording(market, now)?;
            let previous = self.plan.clone();
            self.hip4_rolling.markets.insert(id, recording);
            let series = self
                .hip4_rolling
                .series
                .get_mut(asset)
                .expect("series inserted");
            series.pending = Some(id);
            self.sync_plan_state();
            let add = capture_plan_difference(&self.plan, &previous);
            log::info!(
                "HIP-4 {asset}: subscribing current outcome={id}, awaiting YES/NO snapshots"
            );
            for id in add.planned_instrument_ids() {
                self.bootstrap_instrument(id)?;
            }
            self.subscribe_plan(&add)?;
        }
        Ok(())
    }
    pub(super) fn reconcile_hip4_rolling(&mut self) -> Result<()> {
        let Some(config) = self.hip4_rolling.config.clone() else {
            return Ok(());
        };
        let now = self.clock().timestamp_ns().as_u64();
        let mut retire = BTreeSet::new();
        for asset in &config.underlyings {
            let mut series = self
                .hip4_rolling
                .series
                .get(asset)
                .cloned()
                .unwrap_or_default();
            if let Some(id) = series.pending {
                let market = self
                    .hip4_rolling
                    .markets
                    .get(&id)
                    .expect("owned pending market");
                let ready = market.ids().all(|id| {
                    self.hip4_rolling.ready.contains(&id)
                        && !self.pending_instrument_definitions.contains(&id)
                });
                if ready && market.market.end_ns > now {
                    // Observe the new baseline in the writer before closing the predecessor.
                    optional_flush_all(&self.instrument_runtime)?;
                    optional_flush_all(&self.book_delta_runtime)?;
                    if let Some(old) = series.current.replace(id) {
                        retire.insert(old);
                    }
                    series.pending = None;
                    log::info!("HIP-4 {asset}: active outcome={id}, YES/NO books recorded");
                } else if market.market.end_ns <= now {
                    series.pending = None;
                    series.retry_after = now.saturating_add(config.retry_secs * 1_000_000_000);
                    retire.insert(id);
                    log::warn!(
                        "HIP-4 {asset}: pending market expired outcome={id}; discovering current window"
                    );
                }
            }
            self.hip4_rolling.series.insert(asset.clone(), series);
        }
        self.retire_hip4_rolling(&retire)
    }
    fn retire_hip4_rolling(&mut self, markets: &BTreeSet<u32>) -> Result<()> {
        if markets.is_empty() {
            return Ok(());
        }
        let previous = self.plan.clone();
        for id in markets {
            if let Some(market) = self.hip4_rolling.markets.remove(id) {
                for id in market.ids() {
                    self.hip4_rolling.ready.remove(&id);
                    self.hip4_rolling.definitions_recorded.remove(&id);
                }
                log::info!("HIP-4 retired outcome={id}");
            }
        }
        self.sync_plan_state();
        let remove = capture_plan_difference(&previous, &self.plan);
        let mut live_remove = remove.clone();
        live_remove
            .trades
            .retain(|s| self.market_data_live.contains(&s.instrument_id));
        live_remove
            .book_deltas
            .retain(|s| self.market_data_live.contains(&s.instrument_id));
        self.unsubscribe_plan(&live_remove);
        if let Some(runtime) = &self.book_delta_runtime {
            for spec in &remove.book_deltas {
                runtime.enqueue_seal_partition(
                    &PartitionKey::catalog_data::<OrderBookDelta>(spec.instrument_id).stable_key(),
                )?;
            }
        }
        if let Some(runtime) = &self.trade_runtime {
            for spec in &remove.trades {
                runtime.enqueue_seal_partition(
                    &PartitionKey::catalog_data::<TradeTick>(spec.instrument_id).stable_key(),
                )?;
            }
        }
        for id in remove.planned_instrument_ids() {
            if !self.plan.planned_instrument_ids().contains(&id) {
                self.forget_instrument_runtime_state(id);
                self.hip4_rolling.retiring.insert(id);
            }
        }
        Ok(())
    }
    pub(super) fn observe_hip4_rolling(&mut self, deltas: &OrderBookDeltas) -> Result<()> {
        if self.hip4_rolling.definition(deltas.instrument_id).is_some()
            && nautilus_model::enums::RecordFlag::F_SNAPSHOT.matches(deltas.flags)
            && deltas
                .deltas
                .first()
                .is_some_and(|d| d.action == nautilus_model::enums::BookAction::Clear)
            && deltas
                .deltas
                .last()
                .is_some_and(|d| nautilus_model::enums::RecordFlag::F_LAST.matches(d.flags))
        {
            self.hip4_rolling.ready.insert(deltas.instrument_id);
            self.reconcile_hip4_rolling()?;
        }
        Ok(())
    }
    pub(super) fn stop_hip4_rolling(&mut self) -> Result<()> {
        self.clock().cancel_timer(TIMER);
        self.hip4_rolling.discovery = None;
        self.hip4_rolling.instrument_request = None;
        let ids = self.hip4_rolling.markets.keys().copied().collect();
        self.hip4_rolling.series.clear();
        self.retire_hip4_rolling(&ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog_capture_core::{
        CaptureConfig,
        hip4::{parse_expiry_to_ns, rolling::Hip4RollingConfig},
    };
    use nautilus_common::{
        actor::Component,
        cache::Cache,
        clock::VirtualClock,
        messages::data::{DataCommand, SubscribeCommand, UnsubscribeCommand},
        msgbus::{MessagingSwitchboard, typed_handler::TypedIntoHandler},
    };
    use nautilus_hyperliquid::websocket::{
        messages::WsBookData, parse::parse_ws_order_book_deltas,
    };
    use nautilus_model::{
        identifiers::{TradeId, TraderId},
        stubs::TestDefault,
    };
    use serde_json::{Value, json};
    use std::{cell::RefCell, rc::Rc};
    fn payload(id: u32, end: &str) -> Value {
        json!({"outcomes":[{"outcome":id,"venue":"out","name":"template:binaryPrice",
            "quoteToken":"USDC","deployerFeeScale":"1.0",
            "description":format!("perp:BTC|priceDescription:BTC-USDC perp mark|seconds:60|threshold:78000|time:{end}"),
            "sideSpecs":[{"name":"template:Yes"},{"name":"template:No"}]}]})
    }
    fn snapshot(
        actor: &mut CatalogCaptureActor,
        cache: &Rc<RefCell<Cache>>,
        id: InstrumentId,
        ts: u64,
    ) {
        let instrument = cache.borrow().instrument(&id).unwrap().clone();
        let book: WsBookData = serde_json::from_value(json!({"coin":instrument.raw_symbol().as_str(),
            "time":ts/1_000_000,"levels":[[{"px":"0.4","sz":"100","n":1}],[{"px":"0.6","sz":"80","n":1}]]})).unwrap();
        let deltas = parse_ws_order_book_deltas(&book, &instrument, ts.into()).unwrap();
        let mut book =
            nautilus_model::orderbook::OrderBook::new(id, nautilus_model::enums::BookType::L2_MBP);
        book.apply_deltas(&deltas).unwrap();
        cache.borrow_mut().add_order_book(book).unwrap();
        actor.on_book_deltas(&deltas).unwrap();
    }
    fn trade(actor: &mut CatalogCaptureActor, id: InstrumentId, ts: u64) {
        actor
            .on_trade(&TradeTick::new(
                id,
                "0.5".parse().unwrap(),
                "1".parse().unwrap(),
                nautilus_model::enums::AggressorSide::Buy,
                TradeId::new(format!("{ts}")),
                ts.into(),
                ts.into(),
            ))
            .unwrap();
    }
    fn config(root: &std::path::Path) -> CatalogCaptureActorConfig {
        std::fs::create_dir_all(root).unwrap();
        let mut config = CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", root.display()),
                ..Default::default()
            },
            CapturePlan::default(),
        );
        config.hip4_rolling = Some(Hip4RollingConfig {
            venue_id: "hyperliquid_main".into(),
            underlyings: vec!["BTC".into()],
            retry_secs: 2,
        });
        config
    }
    fn cache_definitions(
        actor: &mut CatalogCaptureActor,
        cache: &Rc<RefCell<Cache>>,
        market: u32,
    ) -> Vec<InstrumentId> {
        let definitions = actor.hip4_rolling.markets[&market].definitions.clone();
        let ids = definitions.iter().map(Instrument::id).collect();
        for definition in definitions {
            cache
                .borrow_mut()
                .add_instrument(definition.clone())
                .unwrap();
            actor.on_instrument(&definition).unwrap();
            actor.on_instrument(&definition).unwrap();
        }
        ids
    }
    fn md_counts(
        commands: &[DataCommand],
    ) -> BTreeMap<(&'static str, &'static str, InstrumentId), usize> {
        let mut counts = BTreeMap::new();
        for command in commands {
            let entry = match command {
                DataCommand::Subscribe(SubscribeCommand::Trades(c)) => {
                    Some(("subscribe", "trades", &c.instrument_ids))
                }
                DataCommand::Subscribe(SubscribeCommand::BookDeltas(c)) => {
                    Some(("subscribe", "books", &c.instrument_ids))
                }
                DataCommand::Unsubscribe(UnsubscribeCommand::Trades(c)) => {
                    Some(("unsubscribe", "trades", &c.instrument_ids))
                }
                DataCommand::Unsubscribe(UnsubscribeCommand::BookDeltas(c)) => {
                    Some(("unsubscribe", "books", &c.instrument_ids))
                }
                _ => None,
            };
            if let Some((action, family, ids)) = entry {
                for id in ids {
                    *counts.entry((action, family, *id)).or_insert(0) += 1;
                }
            }
        }
        counts
    }
    #[test]
    fn current_window_handover_waits_for_both_snapshots_across_hourly_seal() {
        let root = std::env::temp_dir().join(format!(
            "hip4-rolling-handover-{}",
            nautilus_core::UUID4::new()
        ));
        let hour = parse_expiry_to_ns("20261104-0800").unwrap();
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock
            .borrow_mut()
            .advance_time((hour - 1_000_000_000).into(), true);
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        let commands = Rc::new(RefCell::new(Vec::new()));
        let received = commands.clone();
        msgbus::register_data_command_endpoint(
            MessagingSwitchboard::data_engine_queue_execute(),
            TypedIntoHandler::from(move |c: DataCommand| received.borrow_mut().push(c)),
        );
        let mut actor = CatalogCaptureActor::new(config(&root)).unwrap();
        actor
            .register(TraderId::test_default(), clock.clone(), cache.clone())
            .unwrap();
        let first = payload(101, "20261104-0800");
        let next = payload(102, "20261104-0815");
        actor.apply_hip4_rolling_payload(&first).unwrap();
        let old_ids = cache_definitions(&mut actor, &cache, 101);
        for id in &old_ids {
            snapshot(&mut actor, &cache, *id, hour - 500_000_000);
            snapshot(&mut actor, &cache, *id, hour - 450_000_000);
            trade(&mut actor, *id, hour - 400_000_000);
        }
        assert_eq!(actor.hip4_rolling.series["BTC"].current, Some(101));
        actor.apply_hip4_rolling_payload(&first).unwrap();
        actor.apply_hip4_rolling_payload(&next).unwrap();
        assert_eq!(
            actor.hip4_rolling.markets.len(),
            1,
            "no advance subscription to future windows"
        );
        clock.borrow_mut().advance_time(hour.into(), true);
        actor
            .on_time_event(&nautilus_common::timer::TimeEvent::new(
                super::super::SEGMENT_SEAL_TIMER.into(),
                nautilus_core::UUID4::new(),
                hour.into(),
                hour.into(),
            ))
            .unwrap();
        actor
            .apply_hip4_rolling_payload(&json!({"outcomes":[]}))
            .unwrap();
        actor.reconcile_hip4_rolling().unwrap();
        assert_eq!(
            actor.hip4_rolling.series["BTC"].current,
            Some(101),
            "deployment delay retains old stream briefly"
        );
        // Provider creation can lag expiry beyond the activation timeout. As with Predict,
        // that timeout must not release the predecessor before the new two-sided baseline.
        clock
            .borrow_mut()
            .advance_time((hour + 45_000_000_000).into(), true);
        actor.reconcile_hip4_rolling().unwrap();
        assert_eq!(actor.hip4_rolling.series["BTC"].current, Some(101));
        actor.apply_hip4_rolling_payload(&next).unwrap();
        assert_eq!(actor.hip4_rolling.markets.len(), 2);
        let new_ids = cache_definitions(&mut actor, &cache, 102);
        snapshot(&mut actor, &cache, new_ids[0], hour + 45_100_000_000);
        assert_eq!(
            actor.hip4_rolling.series["BTC"].current,
            Some(101),
            "one side is insufficient"
        );
        actor.apply_hip4_rolling_payload(&next).unwrap();
        snapshot(&mut actor, &cache, new_ids[1], hour + 45_200_000_000);
        assert_eq!(actor.hip4_rolling.series["BTC"].current, Some(102));
        assert!(!actor.hip4_rolling.markets.contains_key(&101));
        for id in &new_ids {
            trade(&mut actor, *id, hour + 45_300_000_000);
        }
        for id in &old_ids {
            trade(&mut actor, *id, hour + 45_400_000_000);
            assert!(!actor.accepts_market_data(*id, || true));
        }
        actor.stop_hip4_rolling().unwrap();
        actor.shutdown_all().unwrap();
        let counts = md_counts(&commands.borrow());
        assert_eq!(counts.len(), 16);
        assert!(
            counts.values().all(|n| *n == 1),
            "exactly one subscription and unsubscription per side/family"
        );
        let mut catalog = nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog::new(
            &root, None, None, None, None,
        );
        let instruments = catalog.instruments(None, None, None).unwrap();
        assert_eq!(
            instruments.len(),
            4,
            "duplicate Native definitions do not duplicate recording"
        );
        assert!(
            instruments
                .iter()
                .all(|i| i.expiration_ns().unwrap().as_u64() > 0
                    && i.info().unwrap().contains_key("hip4_raw"))
        );
        assert_eq!(
            catalog.trade_ticks(None, None, None).unwrap().len(),
            4,
            "late old-side trades are rejected"
        );
        for (id, segments) in old_ids
            .into_iter()
            .map(|id| (id, 2))
            .chain(new_ids.into_iter().map(|id| (id, 1)))
        {
            let replay = catalog_capture_core::validate_order_book_replay(&root, id, true).unwrap();
            assert_eq!(replay.final_bid_level_count, 1);
            assert_eq!(replay.final_ask_level_count, 1);
            assert_eq!(replay.segment_count, segments);
            assert_eq!(replay.snapshot_segment_count, segments);
        }
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn cold_definition_requests_are_coalesced_and_pending_survives_until_cache_ready() {
        use nautilus_common::messages::data::{InstrumentsResponse, RequestCommand};
        let root = std::env::temp_dir().join(format!(
            "hip4-rolling-retry-{}",
            nautilus_core::UUID4::new()
        ));
        let now = parse_expiry_to_ns("20261104-0801").unwrap();
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock.borrow_mut().advance_time(now.into(), true);
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        let commands = Rc::new(RefCell::new(Vec::new()));
        let received = commands.clone();
        msgbus::register_data_command_endpoint(
            MessagingSwitchboard::data_engine_queue_execute(),
            TypedIntoHandler::from(move |c: DataCommand| received.borrow_mut().push(c)),
        );
        let mut actor = CatalogCaptureActor::new(config(&root)).unwrap();
        actor
            .register(TraderId::test_default(), clock.clone(), cache.clone())
            .unwrap();
        actor
            .apply_hip4_rolling_payload(&payload(103, "20261104-0815"))
            .unwrap();
        let request = actor.hip4_rolling.instrument_request.unwrap().0;
        let retry = |actor: &mut CatalogCaptureActor, ts: u64| {
            actor
                .on_time_event(&nautilus_common::timer::TimeEvent::new(
                    super::super::PENDING_MARKET_DATA_TIMER.into(),
                    nautilus_core::UUID4::new(),
                    ts.into(),
                    ts.into(),
                ))
                .unwrap()
        };
        retry(&mut actor, now);
        clock
            .borrow_mut()
            .advance_time((now + 45_000_000_000).into(), true);
        actor.reconcile_hip4_rolling().unwrap();
        retry(&mut actor, now + 45_000_000_000);
        assert_eq!(actor.hip4_rolling.series["BTC"].pending, Some(103));
        let requests = || {
            commands
                .borrow()
                .iter()
                .filter(|c| matches!(c, DataCommand::Request(RequestCommand::Instruments(_))))
                .count()
        };
        assert_eq!(
            requests(),
            1,
            "both sides and retry share one Native universe request"
        );
        assert!(md_counts(&commands.borrow()).is_empty());
        assert!(
            !commands
                .borrow()
                .iter()
                .any(|c| matches!(c, DataCommand::Subscribe(SubscribeCommand::Instrument(_)))),
            "cold requests do not repeatedly subscribe missing definitions"
        );
        clock
            .borrow_mut()
            .advance_time((now + 91_000_000_000).into(), true);
        retry(&mut actor, now + 91_000_000_000);
        assert_eq!(
            requests(),
            2,
            "a lost-response watchdog permits a later retry"
        );
        let second = actor.hip4_rolling.instrument_request.unwrap().0;
        assert_ne!(second, request);
        let tags = || {
            Some(nautilus_core::Params::new(
                [("hip4_rolling_capture".to_string(), json!(true))]
                    .into_iter()
                    .collect(),
            ))
        };
        actor.handle_instruments_response(&InstrumentsResponse::new(
            request,
            ClientId::from("HYPERLIQUID"),
            nautilus_model::identifiers::Venue::from("HYPERLIQUID"),
            Vec::new(),
            None,
            None,
            (now + 91_000_000_000).into(),
            tags(),
        ));
        assert_eq!(
            actor.hip4_rolling.instrument_request.unwrap().0,
            second,
            "late responses cannot release a newer request's guard"
        );
        let definitions = actor.hip4_rolling.markets[&103].definitions.clone();
        for definition in &definitions {
            let mut native = definition.clone();
            if let InstrumentAny::BinaryOption(option) = &mut native {
                option.expiration_ns = 0.into();
            }
            cache.borrow_mut().add_instrument(native).unwrap();
        }
        // Periodic adapter refresh can populate the cache before the correlated response arrives.
        // The backup path must still record enriched definitions before subscribing market data.
        retry(&mut actor, now + 91_000_000_000);
        assert_eq!(md_counts(&commands.borrow()).len(), 4);
        for definition in &definitions {
            let instrument = cache.borrow().instrument(&definition.id()).unwrap().clone();
            assert_eq!(instrument.expiration_ns(), definition.expiration_ns());
            assert!(
                actor
                    .hip4_rolling
                    .definitions_recorded
                    .contains(&definition.id())
            );
        }
        let mut response_rows = definitions.clone();
        response_rows.push(InstrumentAny::CurrencyPair(
            nautilus_model::instruments::stubs::audusd_sim(),
        ));
        actor.handle_instruments_response(&InstrumentsResponse::new(
            second,
            ClientId::from("HYPERLIQUID"),
            nautilus_model::identifiers::Venue::from("HYPERLIQUID"),
            response_rows,
            None,
            None,
            (now + 91_000_000_000).into(),
            tags(),
        ));
        assert!(actor.hip4_rolling.instrument_request.is_none());
        for definition in &definitions {
            snapshot(&mut actor, &cache, definition.id(), now + 92_000_000_000);
        }
        assert_eq!(actor.hip4_rolling.series["BTC"].current, Some(103));
        assert_eq!(
            md_counts(&commands.borrow()).len(),
            4,
            "late metadata cannot resubscribe active streams"
        );
        actor.stop_hip4_rolling().unwrap();
        actor.shutdown_all().unwrap();
        let catalog = nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog::new(
            &root, None, None, None, None,
        );
        assert_eq!(
            catalog.instruments(None, None, None).unwrap().len(),
            2,
            "a full universe response records only configured outcomes"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
