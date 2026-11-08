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
use anyhow::Context;
use catalog_capture_core::{capture_plan_difference, hip4::rolling::select_rolling_market};
use nautilus_common::{
    actor::DataActor,
    msgbus::{self, switchboard},
};

use super::*;
use crate::{
    actor_runtime::writer_failure_generation,
    hip4_rolling::{Handover, Hip4RollingState},
};

pub(super) const TIMER: &str = "HIP4_ROLLING";

impl CatalogCaptureActor {
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
        let result = self
            .hip4_rolling
            .discovery
            .as_ref()
            .and_then(|pending| pending.try_complete());
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
            self.hip4_rolling.discovery = Some(crate::hip4_rolling::discover(
                nautilus_hyperliquid::common::enums::HyperliquidEnvironment::Mainnet,
                10,
            )?);
        }
        Ok(())
    }
    pub(super) fn apply_hip4_rolling_payload(&mut self, payload: &serde_json::Value) -> Result<()> {
        let config = self.hip4_rolling.config.clone().expect("HIP-4 configured");
        let now = self.clock().timestamp_ns();
        let mut needs_refresh = false;
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
            let definition_generation = self
                .instrument_runtime
                .as_ref()
                .context("HIP-4 instrument writer unavailable")?
                .failure_snapshot()
                .generation;
            let recording = Hip4RollingState::recording(market, now, definition_generation)?;
            needs_refresh |= recording
                .ids()
                .any(|id| self.cache().instrument(&id).is_none());
            self.hip4_rolling.markets.insert(id, recording);
            let series = self
                .hip4_rolling
                .series
                .get_mut(asset)
                .expect("series inserted");
            series.pending = Some(id);
            self.sync_plan_state();
            log::info!(
                "HIP-4 {asset}: discovered current outcome={id}, waiting for Native YES/NO definitions"
            );
        }
        // One full-universe request covers both assets. Native periodic refresh
        // is the fallback if this request fails or misses a late definition.
        if needs_refresh && self.hip4_rolling.instrument_request.is_none() {
            self.hip4_rolling.instrument_request = Some(self.request_instruments(
                Some(nautilus_model::identifiers::Venue::from("HYPERLIQUID")),
                None,
                None,
                None,
                None,
            )?);
        }
        Ok(())
    }
    pub(super) fn reconcile_hip4_rolling(&mut self) -> Result<()> {
        let Some(config) = self.hip4_rolling.config.clone() else {
            return Ok(());
        };
        let now = self.clock().timestamp_ns().as_u64();
        let mut retire = BTreeSet::new();
        let mut gaps = Vec::new();
        let mut failure = None;
        for asset in &config.underlyings {
            let mut series = self
                .hip4_rolling
                .series
                .get(asset)
                .cloned()
                .unwrap_or_default();
            if let Some(id) = series.pending {
                if self.hip4_rolling.markets[&id].market.end_ns <= now {
                    series.pending = None;
                    series.retry_after = now.saturating_add(config.retry_secs * 1_000_000_000);
                    retire.insert(id);
                    log::warn!(
                        "HIP-4 {asset}: pending market expired outcome={id}; discovering current window"
                    );
                } else {
                    match self.poll_hip4_handover(id) {
                        Ok(true) => {
                            if let Some(old) = series.current.replace(id) {
                                retire.insert(old);
                            }
                            series.pending = None;
                            log::info!("HIP-4 {asset}: active outcome={id}, YES/NO books recorded");
                        }
                        Ok(false) => {}
                        Err(error) => {
                            failure.get_or_insert_with(|| {
                                error.context(format!(
                                    "HIP-4 {asset} outcome={id} handover will retry"
                                ))
                            });
                        }
                    }
                }
            }
            if let Some(id) = series.current {
                let end_ns = self.hip4_rolling.markets[&id].market.end_ns;
                let deadline = end_ns.saturating_add(config.handover_grace_secs * 1_000_000_000);
                if now >= deadline {
                    series.current = None;
                    retire.insert(id);
                    log::warn!(
                        "HIP-4 {asset}: handover grace elapsed outcome={id} successor={:?}",
                        series.pending,
                    );
                    gaps.push(serde_json::json!({
                        "underlying": asset,
                        "expired_outcome": id,
                        "pending_outcome": series.pending,
                        "expired_ns": end_ns,
                        "observed_ns": now,
                        "reason": "handover_grace_elapsed",
                    }));
                }
            }
            self.hip4_rolling.series.insert(asset.clone(), series);
        }
        let retired = self.retire_hip4_rolling(&retire);
        let recorded = gaps.into_iter().fold(Ok(()), |result, gap| {
            result.and(self.submit_audit("metadata/hip4_rolling_gaps.jsonl", gap))
        });
        failure.map_or(Ok(()), Err).and(retired).and(recorded)
    }

    fn poll_hip4_handover(&mut self, outcome: u32) -> Result<bool> {
        let market = &self.hip4_rolling.markets[&outcome];
        if matches!(market.handover, Handover::LoadingDefinitions) {
            // Native refreshes its instrument registry before publishing into the cache.
            let definitions: Vec<_> = market
                .ids()
                .filter(|id| !self.hip4_rolling.definitions_recorded.contains(id))
                .filter_map(|id| self.cache().instrument(&id))
                .collect();
            for instrument in definitions {
                let id = instrument.id();
                self.submit_instrument(instrument)?;
                log::debug!("HIP-4 definition ready outcome={outcome} instrument={id}");
            }
        }
        let market = self
            .hip4_rolling
            .markets
            .get_mut(&outcome)
            .expect("pending market");
        match &market.handover {
            Handover::LoadingDefinitions => {
                if market.ids().all(|id| {
                    self.hip4_rolling.definitions_recorded.contains(&id)
                        && !self.pending_instrument_definitions.contains(&id)
                }) {
                    let partitions = market
                        .ids()
                        .map(|id| PartitionKey::market_data("instruments", id).stable_key())
                        .collect();
                    let writer = self
                        .instrument_runtime
                        .as_ref()
                        .context("HIP-4 instrument writer unavailable")?;
                    match writer.enqueue_partition_flush(partitions, market.failure_cursor) {
                        Ok(flush) => market.handover = Handover::WritingDefinitions(flush),
                        Err(error) => log::debug!(
                            "HIP-4 outcome={outcome} will retry definition flush admission: {error:#}"
                        ),
                    }
                }
            }
            Handover::WritingDefinitions(flush) => {
                let complete = match flush.try_complete() {
                    Ok(complete) => complete,
                    Err(error) => {
                        market.failure_cursor = writer_failure_generation(&self.instrument_runtime);
                        market.handover = Handover::LoadingDefinitions;
                        let retry = self.retry_instrument_definitions();
                        let _ = self.record_capture_result(retry);
                        return Err(error);
                    }
                };
                if complete {
                    market.failure_cursor = self
                        .book_delta_runtime
                        .as_ref()
                        .context("HIP-4 book writer unavailable")?
                        .failure_snapshot()
                        .generation;
                    market.handover = Handover::Subscribed;
                    let ids: Vec<_> = market.ids().collect();
                    for id in ids {
                        self.subscribe_market_data_for_instrument(id);
                    }
                }
            }
            Handover::Subscribed => {
                if market.ids().all(|id| self.hip4_rolling.ready.contains(&id)) {
                    let partitions = market
                        .ids()
                        .map(|id| PartitionKey::catalog_data::<OrderBookDelta>(id).stable_key())
                        .collect();
                    let writer = self
                        .book_delta_runtime
                        .as_ref()
                        .context("HIP-4 book writer unavailable")?;
                    match writer.enqueue_partition_flush(partitions, market.failure_cursor) {
                        Ok(flush) => market.handover = Handover::WritingBooks(flush),
                        Err(error) => log::debug!(
                            "HIP-4 outcome={outcome} will retry book flush admission: {error:#}"
                        ),
                    }
                }
            }
            Handover::WritingBooks(flush) => match flush.try_complete() {
                Ok(true) => {
                    market.handover = Handover::Subscribed;
                    return Ok(true);
                }
                Ok(false) => {}
                Err(error) => {
                    let writer = self
                        .book_delta_runtime
                        .as_ref()
                        .context("HIP-4 book writer unavailable")?;
                    let affected = writer.failures_since(&mut market.failure_cursor);
                    for id in market.ids() {
                        if affected.as_ref().is_none_or(|scope| {
                            scope.contains(
                                &PartitionKey::catalog_data::<OrderBookDelta>(id).stable_key(),
                            )
                        }) {
                            self.hip4_rolling.ready.remove(&id);
                        }
                    }
                    market.handover = Handover::Subscribed;
                    return Err(error);
                }
            },
        }
        Ok(false)
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
        for id in remove.planned_instrument_ids() {
            if !self.plan.planned_instrument_ids().contains(&id) {
                self.forget_instrument_runtime_state(id);
                self.hip4_rolling.retiring.insert(id);
            }
        }
        let mut seals = Vec::new();
        if let Some(runtime) = &self.book_delta_runtime {
            for spec in &remove.book_deltas {
                seals.push(runtime.enqueue_seal_partition(
                    &PartitionKey::catalog_data::<OrderBookDelta>(spec.instrument_id).stable_key(),
                ));
            }
        }
        if let Some(runtime) = &self.trade_runtime {
            for spec in &remove.trades {
                seals.push(runtime.enqueue_seal_partition(
                    &PartitionKey::catalog_data::<TradeTick>(spec.instrument_id).stable_key(),
                ));
            }
        }
        seals.into_iter().collect::<Result<()>>()
    }
    pub(super) fn observe_hip4_rolling(&mut self, deltas: &OrderBookDeltas) -> Result<()> {
        if deltas.instrument_id.venue.as_str() != "HYPERLIQUID"
            || !nautilus_model::enums::RecordFlag::F_SNAPSHOT.matches(deltas.flags)
            || !self.market_data_live.contains(&deltas.instrument_id)
        {
            return Ok(());
        }
        let now = self.clock().timestamp_ns().as_u64();
        if self.hip4_rolling.series.values().any(|series| {
            series.pending.is_some_and(|id| {
                let pending = &self.hip4_rolling.markets[&id];
                pending.market.end_ns > now
                    && matches!(pending.handover, Handover::Subscribed)
                    && pending.ids().any(|id| id == deltas.instrument_id)
            })
        }) && deltas
            .deltas
            .first()
            .is_some_and(|d| d.action == nautilus_model::enums::BookAction::Clear)
            && deltas
                .deltas
                .last()
                .is_some_and(|d| nautilus_model::enums::RecordFlag::F_LAST.matches(d.flags))
            && self.hip4_rolling.ready.insert(deltas.instrument_id)
        {
            log::info!(
                "HIP-4 first snapshot instrument={} local_delivery_age_ms={}",
                deltas.instrument_id,
                now.saturating_sub(deltas.ts_init.as_u64()) / 1_000_000,
            );
            self.reconcile_hip4_rolling()?;
        }
        Ok(())
    }
    pub(super) fn stop_hip4_rolling(&mut self) -> Result<()> {
        self.clock().cancel_timer(TIMER);
        self.hip4_rolling.discovery = None;
        let ids = self.hip4_rolling.markets.keys().copied().collect();
        self.hip4_rolling.series.clear();
        self.retire_hip4_rolling(&ids)
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

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

    use super::*;
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
            handover_grace_secs: 60,
        });
        config
    }
    fn cache_definitions(
        actor: &mut CatalogCaptureActor,
        cache: &Rc<RefCell<Cache>>,
        market: u32,
    ) -> Vec<InstrumentId> {
        let definitions = actor.hip4_rolling.markets[&market].definitions.clone();
        let ids: Vec<_> = definitions.iter().map(Instrument::id).collect();
        for definition in definitions {
            cache
                .borrow_mut()
                .add_instrument(definition.clone())
                .unwrap();
            actor.on_instrument(&definition).unwrap();
            actor.on_instrument(&definition).unwrap();
        }
        reconcile_until(actor, |actor| {
            ids.iter().all(|id| actor.market_data_live.contains(id))
        });
        ids
    }

    fn reconcile_until(
        actor: &mut CatalogCaptureActor,
        reached: impl Fn(&CatalogCaptureActor) -> bool,
    ) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while !reached(actor) {
            actor.reconcile_hip4_rolling().unwrap();
            assert!(
                std::time::Instant::now() < deadline,
                "writer reply timed out"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    fn await_current(actor: &mut CatalogCaptureActor, outcome: u32) {
        reconcile_until(actor, |actor| {
            actor.hip4_rolling.series["BTC"].current == Some(outcome)
        });
    }

    #[test]
    fn handover_retries_control_admission_and_refreshes_only_the_lost_side() {
        let root =
            std::env::temp_dir().join(format!("hip4-write-retry-{}", nautilus_core::UUID4::new()));
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
            .register(TraderId::test_default(), clock, cache.clone())
            .unwrap();
        actor
            .apply_hip4_rolling_payload(&payload(101, "20261104-0815"))
            .unwrap();
        let ids = cache_definitions(&mut actor, &cache, 101);
        snapshot(&mut actor, &cache, ids[0], now + 1);
        // A loss after the first snapshot invalidates its partition's confirmation.
        actor
            .book_delta_runtime
            .as_ref()
            .unwrap()
            .mark_source_gap(&PartitionKey::catalog_data::<OrderBookDelta>(ids[0]))
            .unwrap();
        snapshot(&mut actor, &cache, ids[1], now + 2);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while actor.reconcile_hip4_rolling().is_ok() {
            assert!(
                std::time::Instant::now() < deadline,
                "failed flush was not returned"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(!actor.hip4_rolling.ready.contains(&ids[0]));
        assert!(actor.hip4_rolling.ready.contains(&ids[1]));
        assert_eq!(actor.hip4_rolling.series["BTC"].current, None);
        // Interrupt the control path only, keeping both data subscriptions.
        let recovered = cache
            .borrow()
            .order_book(&ids[0])
            .unwrap()
            .to_deltas((now + 3).into(), (now + 3).into());
        actor.submit_book_deltas(&recovered).unwrap();
        let mut unavailable =
            CatalogCaptureActor::new(config(&root.join("unavailable-writer"))).unwrap();
        unavailable
            .book_delta_runtime
            .as_mut()
            .unwrap()
            .shutdown()
            .unwrap();
        let writer = std::mem::replace(
            &mut actor.book_delta_runtime,
            unavailable.book_delta_runtime.take(),
        );
        actor.observe_hip4_rolling(&recovered).unwrap();
        assert!(actor.hip4_rolling.ready.contains(&ids[0]));
        assert!(matches!(
            actor.hip4_rolling.markets[&101].handover,
            Handover::Subscribed
        ));
        actor.book_delta_runtime = writer;
        await_current(&mut actor, 101);
        actor.stop_hip4_rolling().unwrap();
        actor.shutdown_all().unwrap();
        unavailable.shutdown_all().unwrap();
        let counts = md_counts(&commands.borrow());
        assert_eq!(counts.len(), 8);
        assert!(counts.values().all(|count| *count == 1));
        std::fs::remove_dir_all(root).unwrap();
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
        let reference = InstrumentAny::CryptoPerpetual(
            nautilus_model::instruments::stubs::crypto_perpetual_ethusdt(),
        );
        let reference_id = reference.id();
        cache.borrow_mut().add_instrument(reference).unwrap();
        let mut config = config(&root);
        config
            .plan
            .book_deltas
            .push(catalog_capture_core::BookDeltasCaptureSpec {
                instrument_id: reference_id,
                book_type: nautilus_model::enums::BookType::L2_MBP,
                depth: None,
            });
        let reference_plan = config.plan.clone();
        let mut actor = CatalogCaptureActor::new(config).unwrap();
        actor
            .register(TraderId::test_default(), clock.clone(), cache.clone())
            .unwrap();
        actor.subscribe_plan(&reference_plan).unwrap();
        snapshot(&mut actor, &cache, reference_id, hour - 300_000_000);
        let first = payload(101, "20261104-0800");
        let next = payload(102, "20261104-0815");
        actor.apply_hip4_rolling_payload(&first).unwrap();
        let old_ids = cache_definitions(&mut actor, &cache, 101);
        for id in &old_ids {
            snapshot(&mut actor, &cache, *id, hour - 500_000_000);
            snapshot(&mut actor, &cache, *id, hour - 450_000_000);
            trade(&mut actor, *id, hour - 400_000_000);
        }
        await_current(&mut actor, 101);
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
        // The reference feed stays connected and sends an increment after hourly rotation.
        let ts = (hour + 1_000_000).into();
        let update = OrderBookDelta::new(
            reference_id,
            nautilus_model::enums::BookAction::Update,
            nautilus_model::data::BookOrder::new(
                nautilus_model::enums::OrderSide::Buy,
                "0.4".parse().unwrap(),
                "101".parse().unwrap(),
                0,
            ),
            nautilus_model::enums::RecordFlag::F_LAST as u8,
            2,
            ts,
            ts,
        );
        actor
            .on_book_deltas(&OrderBookDeltas::new(reference_id, vec![update]))
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
        // Provider creation can lag expiry. A successor within the grace period
        // still gets the normal two-sided handover.
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
        assert_eq!(actor.hip4_rolling.series["BTC"].current, Some(101));
        assert!(matches!(
            actor.hip4_rolling.markets[&102].handover,
            Handover::WritingBooks(_)
        ));
        snapshot(&mut actor, &cache, old_ids[0], hour + 45_210_000_000);
        await_current(&mut actor, 102);
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
        let mut counts = md_counts(&commands.borrow());
        assert_eq!(
            counts.remove(&("subscribe", "books", reference_id)),
            Some(1)
        );
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
        for id in old_ids.into_iter().chain(new_ids) {
            let replay = catalog_capture_core::validate_order_book_replay(&root, id, true).unwrap();
            assert_eq!(replay.final_bid_level_count, 1);
            assert_eq!(replay.final_ask_level_count, 1);
            assert_eq!(replay.segment_count, 1);
            assert_eq!(replay.snapshot_segment_count, 1);
        }
        let reference =
            catalog_capture_core::validate_order_book_replay(&root, reference_id, true).unwrap();
        assert_eq!(reference.segment_count, 2);
        assert_eq!(reference.snapshot_segment_count, 2);
        let windows = catalog_capture_core::read_book_replay_windows(&root, reference_id).unwrap();
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].data.len(), 2);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn expired_predecessor_exits_without_a_complete_successor_and_late_data_is_ignored() {
        let root = std::env::temp_dir().join(format!(
            "hip4-rolling-grace-{}",
            nautilus_core::UUID4::new()
        ));
        let end = parse_expiry_to_ns("20261104-0800").unwrap();
        let grace_end = end + 60_000_000_000;
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock
            .borrow_mut()
            .advance_time((end - 1_000_000_000).into(), true);
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
            .apply_hip4_rolling_payload(&payload(101, "20261104-0800"))
            .unwrap();
        let old = cache_definitions(&mut actor, &cache, 101);
        for id in &old {
            snapshot(&mut actor, &cache, *id, end - 500_000_000);
        }
        await_current(&mut actor, 101);

        clock.borrow_mut().advance_time(end.into(), true);
        actor
            .apply_hip4_rolling_payload(&payload(102, "20261104-0815"))
            .unwrap();
        let next = cache_definitions(&mut actor, &cache, 102);
        snapshot(&mut actor, &cache, next[0], end + 1);
        clock
            .borrow_mut()
            .advance_time((grace_end - 1).into(), true);
        actor.reconcile_hip4_rolling().unwrap();
        assert_eq!(actor.hip4_rolling.series["BTC"].current, Some(101));

        clock.borrow_mut().advance_time(grace_end.into(), true);
        actor.reconcile_hip4_rolling().unwrap();
        actor.reconcile_hip4_rolling().unwrap();
        assert_eq!(actor.hip4_rolling.series["BTC"].current, None);
        assert_eq!(actor.hip4_rolling.series["BTC"].pending, Some(102));
        assert_eq!(actor.hip4_rolling.markets.len(), 1);
        crate::actor_runtime::optional_flush_all(&actor.audit_runtime).unwrap();
        let gap = std::fs::read_to_string(root.join("metadata/hip4_rolling_gaps.jsonl")).unwrap();
        let [record] = gap.lines().collect::<Vec<_>>()[..] else {
            panic!("expected exactly one gap record");
        };
        let record: serde_json::Value = serde_json::from_str(record).unwrap();
        assert_eq!(record["expired_outcome"], 101);
        assert_eq!(record["pending_outcome"], 102);
        assert_eq!(record["expired_ns"], end);
        assert_eq!(record["observed_ns"], grace_end);

        snapshot(&mut actor, &cache, old[0], grace_end + 1);
        trade(&mut actor, old[0], grace_end + 2);
        assert!(!actor.hip4_rolling.ready.contains(&old[0]));
        assert_eq!(actor.hip4_rolling.series["BTC"].current, None);
        snapshot(&mut actor, &cache, next[1], grace_end + 3);
        await_current(&mut actor, 102);

        actor.stop_hip4_rolling().unwrap();
        actor.shutdown_all().unwrap();
        let counts = md_counts(&commands.borrow());
        assert_eq!(counts.len(), 16);
        assert!(counts.values().all(|count| *count == 1));
        for id in old.into_iter().chain(next) {
            let replay = catalog_capture_core::validate_order_book_replay(&root, id, true).unwrap();
            assert_eq!(replay.segment_count, 1);
            assert_eq!(replay.snapshot_segment_count, 1);
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rolling_assets_share_one_refresh_and_recover_after_native_failure() {
        assert_rolling_refresh(false);
    }

    #[test]
    fn rolling_refresh_reply_preserves_enriched_definitions() {
        assert_rolling_refresh(true);
    }

    fn assert_rolling_refresh(success: bool) {
        use nautilus_common::{
            actor::registry::{deregister_actor, get_actor_unchecked, register_actor},
            messages::data::{
                DataResponse, DataResponseEnvelope, InstrumentsResponse, RequestCommand,
            },
        };
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
        let mut config = config(&root);
        config
            .hip4_rolling
            .as_mut()
            .unwrap()
            .underlyings
            .push("ETH".into());
        let mut actor = CatalogCaptureActor::new(config).unwrap();
        actor
            .register(TraderId::test_default(), clock.clone(), cache.clone())
            .unwrap();
        let actor_id = actor.core.actor_id().inner();
        register_actor(actor);
        let actor = || get_actor_unchecked::<CatalogCaptureActor>(&actor_id);
        let mut markets = payload(103, "20261104-0815");
        let mut eth = payload(104, "20261104-0815")["outcomes"][0].clone();
        eth["description"] = json!(eth["description"].as_str().unwrap().replace("BTC", "ETH"));
        markets["outcomes"].as_array_mut().unwrap().push(eth);
        actor().apply_hip4_rolling_payload(&markets).unwrap();
        let request_id = actor().hip4_rolling.instrument_request.unwrap();
        assert_eq!(actor().core.pending_requests(), vec![request_id]);
        assert!(matches!(&commands.borrow()[..],
            [DataCommand::Request(RequestCommand::Instruments(request))] if request.request_id == request_id
        ));

        // A late terminal reply keeps the original request in flight without a watchdog.
        clock
            .borrow_mut()
            .advance_time((now + 91_000_000_000).into(), true);
        actor().apply_hip4_rolling_payload(&markets).unwrap();
        actor().reconcile_hip4_rolling().unwrap();
        assert_eq!(commands.borrow().len(), 1);
        if success {
            let definition = actor().hip4_rolling.markets[&103].definitions[0].clone();
            let mut native = definition.clone();
            if let InstrumentAny::BinaryOption(option) = &mut native {
                option.expiration_ns = 0.into();
            }
            cache.borrow_mut().add_instrument(native.clone()).unwrap();
            actor().reconcile_hip4_rolling().unwrap();
            // DataEngine caches the response payload before invoking the actor.
            cache.borrow_mut().add_instrument(native.clone()).unwrap();
            msgbus::send_response(
                &request_id,
                &DataResponse::Instruments(InstrumentsResponse::new(
                    request_id,
                    ClientId::from("HYPERLIQUID"),
                    nautilus_model::identifiers::Venue::from("HYPERLIQUID"),
                    vec![native],
                    None,
                    None,
                    (now + 91_000_000_000).into(),
                    None,
                )),
            );
            let cached = cache.borrow().instrument(&definition.id()).unwrap().clone();
            assert_eq!(cached.expiration_ns(), definition.expiration_ns());
        } else {
            msgbus::send_response_envelope(&DataResponseEnvelope::terminal(
                request_id,
                Some("instrument request timed out".into()),
            ));
        }
        assert!(actor().core.pending_requests().is_empty());
        assert!(actor().hip4_rolling.instrument_request.is_none());
        assert!(
            msgbus::get_message_bus()
                .borrow()
                .get_response_handler(&request_id)
                .is_none()
        );

        // Repeated discovery waits for the adapter's periodic refresh.
        actor().apply_hip4_rolling_payload(&markets).unwrap();
        assert_eq!(commands.borrow().len(), 1);
        let mut actor = actor();
        let definitions: Vec<_> = [103, 104]
            .iter()
            .flat_map(|id| actor.hip4_rolling.markets[id].definitions.clone())
            .collect();
        for (index, definition) in definitions.iter().enumerate() {
            if actor
                .hip4_rolling
                .definitions_recorded
                .contains(&definition.id())
            {
                continue;
            }
            let mut native = definition.clone();
            if let InstrumentAny::BinaryOption(option) = &mut native {
                option.expiration_ns = 0.into();
            }
            cache.borrow_mut().add_instrument(native).unwrap();
            actor.reconcile_hip4_rolling().unwrap();
            assert!(
                actor
                    .hip4_rolling
                    .definitions_recorded
                    .contains(&definition.id())
            );
            if index == 0 {
                assert!(md_counts(&commands.borrow()).is_empty());
            }
        }

        // Writer acknowledgements are consumed on a later rolling tick.
        assert!(md_counts(&commands.borrow()).is_empty());
        reconcile_until(&mut actor, |actor| {
            definitions
                .iter()
                .all(|definition| actor.market_data_live.contains(&definition.id()))
        });
        assert!(actor.core.pending_requests().is_empty());
        assert_eq!(
            commands
                .borrow()
                .iter()
                .filter(|c| matches!(c, DataCommand::Request(_)))
                .count(),
            1
        );
        for definition in &definitions {
            let cached = cache.borrow().instrument(&definition.id()).unwrap().clone();
            assert_eq!(cached.expiration_ns(), definition.expiration_ns());
        }
        let recovered_at = now + 92_000_000_000;
        for definition in &definitions {
            snapshot(&mut actor, &cache, definition.id(), recovered_at);
        }
        await_current(&mut actor, 103);
        reconcile_until(&mut actor, |actor| {
            actor.hip4_rolling.series["ETH"].current == Some(104)
        });
        let counts = md_counts(&commands.borrow());
        assert_eq!(counts.len(), 8);
        assert!(counts.values().all(|count| *count == 1));
        actor.stop_hip4_rolling().unwrap();
        actor.shutdown_all().unwrap();
        drop(actor);
        deregister_actor(&actor_id);
        let catalog = nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog::new(
            &root, None, None, None, None,
        );
        assert_eq!(
            catalog.instruments(None, None, None).unwrap().len(),
            4,
            "only the four configured outcomes are recorded"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
