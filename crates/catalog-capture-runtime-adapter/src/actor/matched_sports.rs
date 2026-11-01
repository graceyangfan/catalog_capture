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

//! Complete-refresh reconciliation; independent events never wait for a predecessor to close.
use super::*;
use crate::actor_runtime::optional_flush_all;
use crate::matched_sports::Preparation;
use catalog_capture_core::{
    capture_plan_difference,
    paired_markets::{MatchedMarket, MatchedUniverse},
};
use nautilus_common::{
    actor::DataActor,
    msgbus::{self, switchboard},
};
use serde_json::json;
use std::io::Write;
pub(super) const TIMER: &str = "matched_sports_capture_refresh";

impl CatalogCaptureActor {
    pub(super) fn start_matched_sports(&mut self) -> Result<()> {
        if let Some(inbox) = &self.matched_sports_inbox {
            self.matched_sports.generation = inbox.begin_generation();
            self.matched_sports_audit(
                "started",
                json!({"generation":self.matched_sports.generation,"config":self.matched_sports_config}),
            )?;
            self.clock().set_timer_ns(
                TIMER,
                nautilus_core::DurationNanos::new(1_000_000_000),
                None,
                None,
                None,
                None,
                None,
            )?;
        }
        Ok(())
    }
    pub(super) fn refresh_matched_sports(&mut self) -> Result<()> {
        let now = self.clock().timestamp_ns().as_u64();
        let update = self
            .matched_sports_inbox
            .as_ref()
            .and_then(|i| i.take(self.matched_sports.generation));
        if let Some(update) = update {
            match update {
                Ok(universe) => self.apply_matched_sports_universe(universe)?,
                Err(reason) => self
                    .matched_sports_audit("discovery_failed_retained", json!({"reason":reason}))?,
            }
        }
        self.reconcile_matched_sports()?;
        let retired: Vec<_> = self
            .matched_sports
            .retiring
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in retired {
            self.matched_sports.retiring.remove(&id);
            let owned = self.plan.planned_instrument_ids().contains(&id)
                || msgbus::subscriber_count_depth(switchboard::get_book_depth_topic(id)) > 0
                || msgbus::subscriber_count_deltas(switchboard::get_book_deltas_topic(id)) > 0
                || msgbus::exact_subscriber_count_trades(switchboard::get_trades_topic(id)) > 0
                || msgbus::subscriptions_count_any(switchboard::get_instrument_topic(id).as_str())?
                    > 0;
            if !owned {
                self.purge_instrument_from_cache(id);
                self.matched_sports.instrument_versions.remove(&id);
            }
        }
        self.refresh_matched_sports_intervals()
    }
    pub(super) fn apply_matched_sports_universe(
        &mut self,
        universe: MatchedUniverse,
    ) -> Result<()> {
        let candidates: BTreeMap<_, _> = universe
            .markets
            .iter()
            .map(|m| (m.predict_market_id, m))
            .collect();
        anyhow::ensure!(
            candidates.len() == universe.markets.len(),
            "duplicate MatchedSports market identity"
        );
        let conditions: BTreeSet<_> = universe
            .markets
            .iter()
            .map(|m| &m.polymarket_condition_id)
            .collect();
        anyhow::ensure!(
            conditions.len() == universe.markets.len(),
            "duplicate Polymarket condition mapping"
        );
        if let Some(ids) = &universe.admission_ids {
            let unique: BTreeSet<_> = ids.iter().collect();
            anyhow::ensure!(
                unique.len() == ids.len() && ids.iter().all(|id| candidates.contains_key(id)),
                "invalid or duplicate MatchedSports admission identity"
            );
        }
        self.matched_sports.universe = Some(universe.clone());
        self.reconcile_matched_sports()?;
        self.matched_sports_audit("discovery", json!({"observed_ns":universe.observed_ns,
            "active_markets":self.matched_sports.active.len(),"owned_markets":self.matched_sports.markets.len(),
            "preparing":self.matched_sports.preparing.as_ref().map(|p|p.market_id),
            "rejected":universe.rejected,"metadata":universe.metadata}))
    }

    fn matched_sports_ready(&self, market: &MatchedMarket) -> bool {
        market.instrument_ids().all(|id| {
            self.matched_sports.instrument_versions.contains_key(&id)
                && !self.pending_instrument_definitions.contains(&id)
        }) && market.recording_instrument_ids().all(|id| {
            self.market_data_live.contains(&id)
                && self.matched_sports.observed.contains_key(&id)
                && self.matched_sports.usable_books.contains(&id)
        })
    }

    fn reconcile_matched_sports(&mut self) -> Result<()> {
        let Some(universe) = self.matched_sports.universe.clone() else {
            return Ok(());
        };
        let Some(config) = self.matched_sports_config.clone() else {
            return Ok(());
        };
        let now = self.clock().timestamp_ns().as_u64();
        let candidates: BTreeMap<_, _> = universe
            .markets
            .iter()
            .map(|m| (m.predict_market_id, m.clone()))
            .collect();
        let admissions = universe.admission_ids.clone().unwrap_or_else(|| {
            universe
                .markets
                .iter()
                .map(|m| m.predict_market_id)
                .collect()
        });
        self.matched_sports
            .retry_after
            .retain(|id, deadline| candidates.contains_key(id) && *deadline > now);
        let mut events = Vec::new();
        let mut preparing = self.matched_sports.preparing.clone().filter(|p| {
            config.preload_next
                && candidates.get(&p.market_id) == self.matched_sports.markets.get(&p.market_id)
        });
        if let Some(p) = &mut preparing {
            let ready = self.matched_sports_ready(&self.matched_sports.markets[&p.market_id]);
            if ready != p.ready {
                if ready {
                    events.push(("prepared", json!({"predict_market_id":p.market_id})));
                } else {
                    p.started_ns = now;
                }
                p.ready = ready;
            }
            if !p.ready
                && now.saturating_sub(p.started_ns)
                    >= config.preparation_timeout_secs * 1_000_000_000
            {
                let deadline = now.saturating_add(config.preparation_retry_secs * 1_000_000_000);
                self.matched_sports
                    .retry_after
                    .insert(p.market_id, deadline);
                events.push((
                    "preparation_timeout",
                    json!({"predict_market_id":p.market_id,"retry_after_ns":deadline}),
                ));
                preparing = None;
            }
        }
        let mut active = self.matched_sports.active.clone();
        // Retain an ended incumbent while a known successor can still be prepared.
        // Unsafe same-ID changes cannot share a preparation identity with the old pair.
        active.retain(|id| {
            let unchanged = candidates.get(id) == self.matched_sports.markets.get(id);
            unchanged
                || (config.preload_next
                    && !candidates.contains_key(id)
                    && (preparing.is_some()
                        || admissions
                            .iter()
                            .any(|candidate| !self.matched_sports.active.contains(candidate))))
        });
        if let Some(p) = &preparing {
            let ended = active
                .iter()
                .find(|id| candidates.get(id) != self.matched_sports.markets.get(id))
                .copied();
            if p.ready && (active.len() < config.max_markets || ended.is_some()) {
                // The initial book and definitions must have reached their sinks before old subscriptions exit.
                optional_flush_all(&self.instrument_runtime)?;
                optional_flush_all(&self.book_depth_runtime)?;
                optional_flush_all(&self.book_delta_runtime)?;
                optional_flush_all(&self.trade_runtime)?;
                if let Some(id) = ended {
                    active.remove(&id);
                }
                active.insert(p.market_id);
                events.push((
                    "promoted",
                    json!({"predict_market_id":p.market_id,"replaced_market_id":ended}),
                ));
                preparing = None;
            }
        }
        for id in &admissions {
            if active.len() >= config.max_markets {
                break;
            }
            if !self.matched_sports.retry_after.contains_key(id)
                && preparing.as_ref().is_none_or(|p| p.market_id != *id)
            {
                active.insert(*id);
            }
        }
        if config.preload_next
            && preparing.is_none()
            && !active.is_empty()
            && let Some(id) = admissions.iter().find(|id| {
                !active.contains(id) && !self.matched_sports.retry_after.contains_key(id)
            })
        {
            preparing = Some(Preparation {
                market_id: *id,
                started_ns: now,
                ready: false,
            });
        }
        let mut next = BTreeMap::new();
        for id in &active {
            if let Some(market) = candidates
                .get(id)
                .or_else(|| self.matched_sports.markets.get(id))
            {
                next.insert(*id, market.clone());
            }
        }
        if let Some(p) = &preparing {
            next.insert(p.market_id, candidates[&p.market_id].clone());
        }
        anyhow::ensure!(
            next.len() <= config.max_markets + usize::from(config.preload_next),
            "preparation resource budget exceeded"
        );
        let unique: BTreeSet<_> = next.values().map(|m| &m.polymarket_condition_id).collect();
        anyhow::ensure!(
            unique.len() == next.len(),
            "overlapping Polymarket preparation identity"
        );
        self.matched_sports.active = active;
        self.matched_sports.preparing = preparing;
        self.apply_matched_sports_owned(next, &universe)?;
        for (kind, detail) in events {
            self.matched_sports_audit(kind, detail)?;
        }
        Ok(())
    }

    fn apply_matched_sports_owned(
        &mut self,
        next: BTreeMap<u64, MatchedMarket>,
        universe: &MatchedUniverse,
    ) -> Result<()> {
        if self.matched_sports.markets == next {
            return Ok(());
        }
        let old_plan = self.plan.clone();
        let previous = std::mem::replace(&mut self.matched_sports.markets, next);
        for (id, market) in &previous {
            if self.matched_sports.markets.get(id) != Some(market) {
                self.matched_sports.intervals.remove(id);
                for instrument in market.instrument_ids() {
                    self.matched_sports.observed.remove(&instrument);
                    self.matched_sports.usable_books.remove(&instrument);
                }
                let reason = universe
                    .rejected
                    .iter()
                    .find(|r| r.predict_market_id == *id)
                    .map_or_else(
                        || {
                            if self.matched_sports.retry_after.contains_key(id) {
                                "preparation_timeout"
                            } else if universe.markets.iter().any(|m| m.predict_market_id == *id) {
                                "market_identity_or_rules_changed"
                            } else {
                                "absent_from_complete_validated_universe"
                            }
                        },
                        |r| r.reason.as_str(),
                    );
                self.matched_sports_audit(
                    "retired",
                    json!({"market":market,"observed_ns":universe.observed_ns,"reason":reason}),
                )?;
            }
        }
        self.sync_plan_state();
        let removed = capture_plan_difference(&old_plan, &self.plan);
        let added = capture_plan_difference(&self.plan, &old_plan);
        let mut subscribed = removed.clone();
        subscribed
            .trades
            .retain(|s| self.market_data_live.contains(&s.instrument_id));
        subscribed
            .book_depth
            .retain(|s| self.market_data_live.contains(&s.instrument_id));
        subscribed
            .book_deltas
            .retain(|s| self.market_data_live.contains(&s.instrument_id));
        self.unsubscribe_plan(&subscribed);
        for spec in &removed.trades {
            if let Some(runtime) = &self.trade_runtime {
                runtime.enqueue_seal_partition(
                    &PartitionKey::catalog_data::<TradeTick>(spec.instrument_id).stable_key(),
                )?;
            }
        }
        for spec in &removed.book_depth {
            if let Some(runtime) = &self.book_depth_runtime {
                runtime.enqueue_seal_partition(
                    &PartitionKey::catalog_data::<OrderBookDepth>(spec.instrument_id).stable_key(),
                )?;
            }
        }
        for spec in &removed.book_deltas {
            if let Some(runtime) = &self.book_delta_runtime {
                runtime.enqueue_seal_partition(
                    &PartitionKey::catalog_data::<OrderBookDelta>(spec.instrument_id).stable_key(),
                )?;
            }
        }
        let now = self.clock().timestamp_ns().as_u64();
        for id in removed.planned_instrument_ids() {
            if !self.plan.planned_instrument_ids().contains(&id) {
                // Matched metadata uses requests, not an instrument subscription.
                self.forget_instrument_runtime_state(id);
                // Allow bounded in-flight metadata requests to finish before purging the cache.
                self.matched_sports
                    .retiring
                    .insert(id, now.saturating_add(30_000_000_000));
            }
        }
        // A stale same-venue callback must not be accepted after the plan changes.
        for id in added.planned_instrument_ids() {
            self.matched_sports.retiring.remove(&id);
            self.bootstrap_instrument(id)?;
        }
        // An instrument can already be live for a static trade selector. Its new book
        // family still needs a command even though the per-instrument gate is already set.
        for spec in &added.trades {
            if self.market_data_live.contains(&spec.instrument_id) {
                self.subscribe_trades(
                    spec.instrument_id,
                    self.market_data_client_id(spec.instrument_id),
                    None,
                );
            }
        }
        for spec in &added.book_depth {
            if self.market_data_live.contains(&spec.instrument_id) {
                self.subscribe_book_depth(
                    spec.instrument_id,
                    spec.book_type,
                    spec.depth.and_then(std::num::NonZeroUsize::new),
                    self.market_data_client_id(spec.instrument_id),
                    false,
                    None,
                );
            }
        }
        for spec in &added.book_deltas {
            if self.market_data_live.contains(&spec.instrument_id) {
                self.subscribe_book_deltas(
                    spec.instrument_id,
                    spec.book_type,
                    spec.depth.and_then(std::num::NonZeroUsize::new),
                    self.market_data_client_id(spec.instrument_id),
                    true,
                    None,
                );
            }
        }
        self.subscribe_plan(&added)?;
        for (id, market) in &self.matched_sports.markets {
            if previous.get(id) != Some(market) {
                self.matched_sports_audit(
                    if self
                        .matched_sports
                        .preparing
                        .as_ref()
                        .is_some_and(|p| p.market_id == *id)
                    {
                        "preparing"
                    } else {
                        "admitted"
                    },
                    json!({"market":market,
                    "predict_recorded_outcomes":[1],"polymarket_recorded_outcomes":"both",
                    "observed_ns":universe.observed_ns}),
                )?;
            }
        }
        Ok(())
    }
    pub(super) fn observe_matched_sports(
        &mut self,
        id: InstrumentId,
        snapshot: bool,
        usable: bool,
    ) -> Result<()> {
        if self.matched_sports_inbox.is_none() {
            return Ok(());
        }
        let configured = if id.venue.as_str() == "PREDICT" {
            self.plan.book_depth.iter().any(|s| s.instrument_id == id)
        } else {
            self.plan.book_deltas.iter().any(|s| s.instrument_id == id)
        };
        if !self.accepts_market_data(id, || configured) {
            return Ok(());
        }
        let now = self.clock().timestamp_ns().as_u64();
        self.matched_sports.observe(id, now, snapshot);
        if usable && self.matched_sports.observed.contains_key(&id) {
            self.matched_sports.usable_books.insert(id);
        } else {
            self.matched_sports.usable_books.remove(&id);
        }
        let readiness_changed = self.matched_sports.preparing.as_ref().is_some_and(|p| {
            let market = &self.matched_sports.markets[&p.market_id];
            market.recording_instrument_ids().any(|i| i == id)
                && self.matched_sports_ready(market) != p.ready
        });
        if readiness_changed {
            self.reconcile_matched_sports()?;
        }
        self.refresh_matched_sports_intervals()
    }
    fn refresh_matched_sports_intervals(&mut self) -> Result<()> {
        let Some(config) = &self.matched_sports_config else {
            return Ok(());
        };
        let now = self.clock().timestamp_ns().as_u64();
        let timeout = config.observation_timeout_secs * 1_000_000_000;
        for market in self.matched_sports.markets.values() {
            let ready = market.recording_instrument_ids().all(|id| {
                self.matched_sports
                    .observed
                    .get(&id)
                    .is_some_and(|seen| now.saturating_sub(*seen) <= timeout)
            });
            let previous = self
                .matched_sports
                .intervals
                .insert(market.predict_market_id, ready);
            if previous != Some(ready) {
                // Quiet feeds can time out too: this is observed comparability, not connection health.
                self.matched_sports_audit(if ready { "comparable_open" } else { "comparable_closed" },
                    json!({"predict_market_id":market.predict_market_id,"local_ns":now,
                        "reason":if ready {"both_books_observed"} else {"awaiting_snapshot_or_observation_timeout"}}))?;
            }
        }
        Ok(())
    }
    pub(super) fn stop_matched_sports(&mut self) -> Result<()> {
        self.clock().cancel_timer(TIMER);
        if let Some(inbox) = &self.matched_sports_inbox {
            inbox.stop();
            self.matched_sports_audit(
                "stopped",
                json!({"open_intervals":self.matched_sports.intervals,
                "local_ns":self.clock().timestamp_ns().as_u64()}),
            )?;
        }
        let old_plan = self.plan.clone();
        self.matched_sports = Default::default();
        self.sync_plan_state();
        let mut removed = capture_plan_difference(&old_plan, &self.plan);
        removed
            .trades
            .retain(|s| self.market_data_live.contains(&s.instrument_id));
        removed
            .book_depth
            .retain(|s| self.market_data_live.contains(&s.instrument_id));
        removed
            .book_deltas
            .retain(|s| self.market_data_live.contains(&s.instrument_id));
        self.unsubscribe_plan(&removed);
        for id in capture_plan_difference(&old_plan, &self.plan).planned_instrument_ids() {
            if !self.plan.planned_instrument_ids().contains(&id) {
                self.forget_instrument_runtime_state(id);
            }
        }
        optional_flush_all(&self.instrument_runtime)?;
        Ok(())
    }
    fn matched_sports_audit(&self, kind: &str, detail: serde_json::Value) -> Result<()> {
        std::fs::create_dir_all(&self.catalog_root)?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.catalog_root.join("matched_sports_capture.jsonl"))?;
        serde_json::to_writer(
            &mut file,
            &json!({"kind":kind,"detail":detail,
            "generation":self.matched_sports.generation,"local_ns":self.clock().timestamp_ns().as_u64()}),
        )?;
        file.write_all(b"\n")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matched_sports::MatchedSportsDiscoveryInbox;
    use catalog_capture_core::{
        CaptureConfig,
        paired_markets::{MatchedMarket, MatchedOutcome, MatchedSportsCaptureConfig},
    };
    use nautilus_common::{actor::Component, cache::Cache, clock::VirtualClock};
    use nautilus_model::{identifiers::TraderId, stubs::TestDefault};
    use std::{cell::RefCell, rc::Rc};
    fn pair(id: u64) -> MatchedMarket {
        let condition = format!("0x{id:064x}");
        MatchedMarket {
            event_slug: format!("lol-{id}"),
            predict_market_id: id,
            polymarket_condition_id: condition.clone(),
            game_number: None,
            rules: "same rules".into(),
            outcomes: [1, 2].map(|index| MatchedOutcome {
                predict_index_set: index,
                predict_label: format!("team-{index}"),
                polymarket_label: format!("team-{index}"),
                predict_instrument: format!("{id}-{index}.PREDICT").parse().unwrap(),
                polymarket_instrument: format!("{condition}-{index}.POLYMARKET").parse().unwrap(),
            }),
        }
    }
    fn definitions(market: &MatchedMarket) -> Vec<nautilus_model::instruments::InstrumentAny> {
        use nautilus_model::instruments::InstrumentAny;
        market
            .outcomes
            .iter()
            .flat_map(|outcome| {
                [outcome.predict_instrument, outcome.polymarket_instrument].map(|id| {
                    let InstrumentAny::BinaryOption(mut instrument) =
                        super::super::tests::test_predict_outcome(
                            market.predict_market_id,
                            outcome.predict_index_set,
                        )
                    else {
                        unreachable!()
                    };
                    instrument.id = id;
                    InstrumentAny::BinaryOption(instrument)
                })
            })
            .collect()
    }
    fn md_counts(
        commands: &[nautilus_common::messages::data::DataCommand],
    ) -> BTreeMap<(&'static str, &'static str, InstrumentId), usize> {
        use nautilus_common::messages::data::{DataCommand, SubscribeCommand, UnsubscribeCommand};

        let mut counts = BTreeMap::new();
        for command in commands {
            let entry = match command {
                DataCommand::Subscribe(SubscribeCommand::Trades(c)) => {
                    Some(("subscribe", "trades", &c.instrument_ids))
                }
                DataCommand::Subscribe(SubscribeCommand::BookDepth(c)) => {
                    Some(("subscribe", "depth", &c.instrument_ids))
                }
                DataCommand::Subscribe(SubscribeCommand::BookDeltas(c)) => {
                    Some(("subscribe", "deltas", &c.instrument_ids))
                }
                DataCommand::Unsubscribe(UnsubscribeCommand::Trades(c)) => {
                    Some(("unsubscribe", "trades", &c.instrument_ids))
                }
                DataCommand::Unsubscribe(UnsubscribeCommand::BookDepth(c)) => {
                    Some(("unsubscribe", "depth", &c.instrument_ids))
                }
                DataCommand::Unsubscribe(UnsubscribeCommand::BookDeltas(c)) => {
                    Some(("unsubscribe", "deltas", &c.instrument_ids))
                }
                _ => None,
            };
            if let Some((action, family, ids)) = entry {
                for id in ids {
                    *counts.entry((action, family, *id)).or_insert(0usize) += 1;
                }
            }
        }
        counts
    }
    #[test]
    fn discovery_reconciles_parallel_events_retains_on_failure_and_preserves_static_owner() {
        let root =
            std::env::temp_dir().join(format!("matched-rotation-{}", nautilus_core::UUID4::new()));
        std::fs::create_dir_all(&root).unwrap();
        let first = pair(42);
        let next = pair(43);
        let static_id = first.outcomes[0].predict_instrument;
        let mut config = CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", root.display()),
                ..Default::default()
            },
            CapturePlan {
                trades: vec![catalog_capture_core::TradeCaptureSpec {
                    instrument_id: static_id,
                }],
                ..Default::default()
            },
        );
        let inbox = Arc::new(MatchedSportsDiscoveryInbox::default());
        config.matched_sports_config = Some(MatchedSportsCaptureConfig {
            max_markets: 2,
            ..Default::default()
        });
        config.matched_sports_inbox = Some(inbox.clone());
        let mut actor = CatalogCaptureActor::new(config).unwrap();
        actor
            .register(
                TraderId::test_default(),
                Rc::new(RefCell::new(VirtualClock::new())),
                Rc::new(RefCell::new(Cache::new(None, None))),
            )
            .unwrap();
        actor.start_matched_sports().unwrap();
        actor
            .apply_matched_sports_universe(MatchedUniverse {
                markets: vec![first.clone(), next.clone()],
                ..Default::default()
            })
            .unwrap();
        assert_eq!(actor.matched_sports.markets.len(), 2);
        assert_eq!(actor.plan.book_deltas.len(), 4);
        let instrument_topic =
            switchboard::get_instrument_topic(first.outcomes[0].predict_instrument);
        assert_eq!(
            msgbus::subscriptions_count_any(instrument_topic.as_str()).unwrap(),
            0
        );
        let retry = nautilus_common::timer::TimeEvent::new(
            super::super::PENDING_MARKET_DATA_TIMER.into(),
            nautilus_core::UUID4::new(),
            1.into(),
            1.into(),
        );
        actor.on_time_event(&retry).unwrap();
        assert_eq!(
            msgbus::subscriptions_count_any(instrument_topic.as_str()).unwrap(),
            0,
            "cold matched metadata retries must wait for the correlated response, not subscribe an unknown instrument"
        );
        let definition = super::super::tests::test_predict_outcome(42, 1);
        actor.on_instrument(&definition).unwrap();
        actor.on_instrument(&definition).unwrap();
        actor
            .on_instrument(&super::super::tests::test_predict_outcome(42, 2))
            .unwrap();
        // Feed the same callbacks used by Native after instrument-ready subscription.
        // Both sides must reach readable Parquet, not merely an in-memory plan.
        for id in first.instrument_ids() {
            actor.market_data_live.insert(id);
        }
        for o in &first.outcomes {
            actor
                .on_book_depth(&OrderBookDepth::new(
                    o.predict_instrument,
                    [],
                    [],
                    [],
                    [],
                    nautilus_model::enums::RecordFlag::F_SNAPSHOT as u8,
                    0,
                    100.into(),
                    101.into(),
                ))
                .unwrap();
            let book = nautilus_model::orderbook::OrderBook::new(
                o.polymarket_instrument,
                nautilus_model::enums::BookType::L2_MBP,
            );
            actor
                .on_book_deltas(&book.to_deltas(100.into(), 101.into()))
                .unwrap();
            for id in [o.predict_instrument, o.polymarket_instrument] {
                actor
                    .on_trade(&TradeTick::new(
                        id,
                        "0.50".parse().unwrap(),
                        "1".parse().unwrap(),
                        nautilus_model::enums::AggressorSide::Buy,
                        nautilus_model::identifiers::TradeId::new("paired-test"),
                        100.into(),
                        101.into(),
                    ))
                    .unwrap();
            }
        }
        assert_eq!(actor.matched_sports.intervals.get(&42), Some(&true));
        assert!(
            !actor
                .matched_sports
                .observed
                .contains_key(&first.outcomes[1].predict_instrument)
        );
        let generation = actor.matched_sports.generation;
        inbox.publish(generation, Err("429 incomplete catalogue".into()));
        actor.refresh_matched_sports().unwrap();
        assert_eq!(actor.matched_sports.markets.len(), 2);
        // A delta before an initial snapshot cannot create a comparable sample.
        let id = next.outcomes[0].polymarket_instrument;
        actor.matched_sports.observe(id, 1, false);
        assert!(!actor.matched_sports.observed.contains_key(&id));
        actor
            .apply_matched_sports_universe(MatchedUniverse {
                markets: vec![next],
                ..Default::default()
            })
            .unwrap();
        assert_eq!(actor.matched_sports.markets.len(), 1);
        assert!(
            actor
                .plan
                .trades
                .iter()
                .any(|s| s.instrument_id == static_id)
        );
        assert!(!actor.matched_sports.retiring.contains_key(&static_id));
        let removed = first.outcomes[1].predict_instrument;
        assert!(actor.matched_sports.retiring.contains_key(&removed));
        assert!(!actor.accepts_market_data(removed, || true));
        actor.stop_matched_sports().unwrap();
        assert_eq!(inbox.active_generation(), None);
        actor.shutdown_all().unwrap();
        let mut catalog = nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog::new(
            &root, None, None, None, None,
        );
        let depths = catalog.order_book_depths(None, None, None).unwrap();
        assert_eq!(depths.len(), 1);
        assert_eq!(
            depths[0].instrument_id,
            first.outcomes[0].predict_instrument
        );
        let deltas = catalog.order_book_deltas(None, None, None).unwrap();
        assert_eq!(deltas.len(), 2);
        let trades = catalog.trade_ticks(None, None, None).unwrap();
        assert_eq!(trades.len(), 3);
        assert!(
            !trades
                .iter()
                .any(|t| t.instrument_id == first.outcomes[1].predict_instrument)
        );
        let instruments = catalog.instruments(None, None, None).unwrap();
        assert!(
            instruments
                .iter()
                .any(|i| i.id() == first.outcomes[0].predict_instrument)
        );
        assert!(
            instruments
                .iter()
                .any(|i| i.id() == first.outcomes[1].predict_instrument)
        );
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn ended_market_retires_when_only_unqualified_successors_remain() {
        let root = std::env::temp_dir().join(format!(
            "matched-ended-quiet-{}",
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
        config.matched_sports_config = Some(MatchedSportsCaptureConfig::default());
        config.matched_sports_inbox = Some(Arc::new(MatchedSportsDiscoveryInbox::default()));
        let first = pair(501);
        let quiet = pair(502);
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        for definition in definitions(&first) {
            cache.borrow_mut().add_instrument(definition).unwrap();
        }
        let mut actor = CatalogCaptureActor::new(config).unwrap();
        actor
            .register(
                TraderId::test_default(),
                Rc::new(RefCell::new(VirtualClock::new())),
                cache.clone(),
            )
            .unwrap();
        actor.start_matched_sports().unwrap();
        actor
            .apply_matched_sports_universe(MatchedUniverse {
                markets: vec![first.clone(), quiet.clone()],
                admission_ids: Some(vec![501]),
                ..Default::default()
            })
            .unwrap();
        emit_recording_batch(&mut actor, &cache, &first, 1_000_000_000, true);
        assert_eq!(actor.matched_sports.active, BTreeSet::from([501]));
        assert!(actor.matched_sports.preparing.is_none());
        actor
            .apply_matched_sports_universe(MatchedUniverse {
                markets: vec![quiet],
                admission_ids: Some(vec![]),
                ..Default::default()
            })
            .unwrap();
        // Stop also clears ownership; retain the transition result before cleanup.
        let retired = actor.matched_sports.markets.is_empty()
            && actor.matched_sports.active.is_empty()
            && first
                .recording_instrument_ids()
                .all(|id| !actor.market_data_live.contains(&id));
        actor.stop_matched_sports().unwrap();
        actor.shutdown_all().unwrap();
        let mut catalog = nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog::new(
            &root, None, None, None, None,
        );
        assert_eq!(catalog.trade_ticks(None, None, None).unwrap().len(), 3);
        let audit = std::fs::read_to_string(root.join("matched_sports_capture.jsonl")).unwrap();
        std::fs::remove_dir_all(&root).unwrap();
        assert!(
            retired,
            "quiet catalogue entries cannot keep an ended market subscribed"
        );
        assert!(
            audit
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .any(|row| row["kind"] == "retired"
                    && row["detail"]["market"]["predict_market_id"] == 501)
        );
    }
    #[test]
    fn admission_rank_changes_and_quiet_activity_do_not_interrupt_incumbent() {
        let root =
            std::env::temp_dir().join(format!("matched-sticky-{}", nautilus_core::UUID4::new()));
        std::fs::create_dir_all(&root).unwrap();
        let mut config = CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", root.display()),
                ..Default::default()
            },
            CapturePlan::default(),
        );
        let inbox = Arc::new(MatchedSportsDiscoveryInbox::default());
        config.matched_sports_config = Some(MatchedSportsCaptureConfig {
            preload_next: false,
            ..Default::default()
        });
        config.matched_sports_inbox = Some(inbox.clone());
        let mut actor = CatalogCaptureActor::new(config).unwrap();
        actor
            .register(
                TraderId::test_default(),
                Rc::new(RefCell::new(VirtualClock::new())),
                Rc::new(RefCell::new(Cache::new(None, None))),
            )
            .unwrap();
        actor.start_matched_sports().unwrap();
        let first = pair(80);
        let next = pair(81);
        // A validated but unseasoned candidate must not consume the slot.
        actor
            .apply_matched_sports_universe(MatchedUniverse {
                markets: vec![first.clone(), next.clone()],
                admission_ids: Some(vec![]),
                ..Default::default()
            })
            .unwrap();
        assert!(actor.matched_sports.markets.is_empty());
        actor
            .apply_matched_sports_universe(MatchedUniverse {
                markets: vec![first.clone(), next.clone()],
                admission_ids: Some(vec![80]),
                ..Default::default()
            })
            .unwrap();
        for id in first.recording_instrument_ids() {
            actor.matched_sports.observe(id, 1, true);
        }
        let observations = actor.matched_sports.observed.clone();
        actor
            .apply_matched_sports_universe(MatchedUniverse {
                // Next is now better ranked; the first no longer qualifies for new admission.
                markets: vec![next.clone(), first.clone()],
                admission_ids: Some(vec![81]),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            actor
                .matched_sports
                .markets
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![80]
        );
        assert_eq!(actor.matched_sports.observed, observations);
        assert!(actor.matched_sports.retiring.is_empty());
        assert!(
            !actor
                .plan
                .planned_instrument_ids()
                .contains(&next.outcomes[0].polymarket_instrument)
        );
        inbox.publish(
            actor.matched_sports.generation,
            Err("statistics unavailable".into()),
        );
        actor.refresh_matched_sports().unwrap();
        assert_eq!(actor.matched_sports.markets[&80], first);
        // The complete scan now explicitly excludes the finished incumbent and fills its slot.
        actor
            .apply_matched_sports_universe(MatchedUniverse {
                markets: vec![next.clone()],
                admission_ids: Some(vec![81]),
                rejected: vec![catalog_capture_core::paired_markets::MatchRejection {
                    predict_market_id: 80,
                    reason: "closed_or_event_finished".into(),
                }],
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            actor
                .matched_sports
                .markets
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![81]
        );
        assert_eq!(actor.matched_sports.markets[&81], next);
        assert!(
            first
                .instrument_ids()
                .all(|id| actor.matched_sports.retiring.contains_key(&id))
        );
        assert!(
            actor
                .plan
                .planned_instrument_ids()
                .contains(&next.outcomes[0].polymarket_instrument)
        );
        let audit = std::fs::read_to_string(root.join("matched_sports_capture.jsonl")).unwrap();
        assert!(
            audit
                .lines()
                .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
                .any(|v| v["kind"] == "retired"
                    && v["detail"]["reason"] == "closed_or_event_finished")
        );
        actor.stop_matched_sports().unwrap();
        actor.shutdown_all().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn prepared_handover_across_hourly_seal_keeps_both_tapes_and_never_resubscribes_successor() {
        use nautilus_common::{
            messages::data::DataCommand,
            msgbus::{MessagingSwitchboard, typed_handler::TypedIntoHandler},
        };
        const HOUR: u64 = 3_600_000_000_000;
        let root = std::env::temp_dir().join(format!(
            "matched-handover-hourly-{}",
            nautilus_core::UUID4::new()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let mut capture = CaptureConfig {
            catalog_uri: format!("file://{}", root.display()),
            ..Default::default()
        };
        capture.lifecycle.seal = catalog_capture_core::lifecycle::SealConfigFile {
            enabled: true,
            schedule: "00:00".into(),
            timezone: "UTC".into(),
            interval_secs: 3600,
        };
        let inbox = Arc::new(MatchedSportsDiscoveryInbox::default());
        let mut config = CatalogCaptureActorConfig::new(capture, CapturePlan::default());
        config.matched_sports_config = Some(MatchedSportsCaptureConfig::default());
        config.matched_sports_inbox = Some(inbox.clone());
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock
            .borrow_mut()
            .advance_time((HOUR - 2_000_000_000).into(), true);
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        let first = pair(201);
        let next = pair(202);
        for definition in definitions(&first) {
            cache.borrow_mut().add_instrument(definition).unwrap();
        }
        let commands = Rc::new(RefCell::new(Vec::<DataCommand>::new()));
        let received = commands.clone();
        let endpoint = MessagingSwitchboard::data_engine_queue_execute();
        msgbus::register_data_command_endpoint(
            endpoint,
            TypedIntoHandler::from(move |command: DataCommand| received.borrow_mut().push(command)),
        );
        let mut actor = CatalogCaptureActor::new(config).unwrap();
        actor
            .register(TraderId::test_default(), clock.clone(), cache.clone())
            .unwrap();
        DataActor::on_start(&mut actor).unwrap();
        let generation = actor.matched_sports.generation;
        let universe = MatchedUniverse {
            markets: vec![first.clone(), next.clone()],
            admission_ids: Some(vec![201, 202]),
            ..Default::default()
        };
        inbox.publish(generation, Ok(universe.clone()));
        actor.refresh_matched_sports().unwrap();
        assert_eq!(md_counts(&commands.borrow()).values().sum::<usize>(), 6);
        emit_recording_batch(&mut actor, &cache, &first, HOUR - 1_500_000_000, true);
        assert_eq!(
            actor.matched_sports.preparing.as_ref().unwrap().market_id,
            202
        );
        for definition in definitions(&next) {
            cache
                .borrow_mut()
                .add_instrument(definition.clone())
                .unwrap();
            actor.on_instrument(&definition).unwrap();
            actor.on_instrument(&definition).unwrap();
        }
        clock
            .borrow_mut()
            .advance_time((HOUR - 1_000_000_000).into(), true);
        emit_recording_batch(&mut actor, &cache, &next, HOUR - 1_000_000_000, true);
        assert!(actor.matched_sports.preparing.as_ref().unwrap().ready);
        assert_eq!(actor.matched_sports.active, BTreeSet::from([201]));
        for _ in 0..2 {
            inbox.publish(generation, Ok(universe.clone()));
            actor.refresh_matched_sports().unwrap();
        }
        let before_seal = commands.borrow().len();
        let events = clock.borrow_mut().advance_time(HOUR.into(), true);
        actor
            .on_time_event(
                events
                    .iter()
                    .find(|e| e.name == super::super::SEGMENT_SEAL_TIMER)
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            commands.borrow().len(),
            before_seal,
            "hourly seal must not resubscribe"
        );
        clock
            .borrow_mut()
            .advance_time((HOUR + 1_000_000_000).into(), true);
        emit_recording_batch(&mut actor, &cache, &first, HOUR + 1_000_000_000, false);
        emit_recording_batch(&mut actor, &cache, &next, HOUR + 1_000_000_000, false);
        let before_promotion = commands.borrow().len();
        // Confirmed terminal discovery uses the production mailbox/timer consumer.
        inbox.publish(
            generation,
            Ok(MatchedUniverse {
                markets: vec![next.clone()],
                admission_ids: Some(vec![202]),
                rejected: vec![catalog_capture_core::paired_markets::MatchRejection {
                    predict_market_id: 201,
                    reason: "closed_or_event_finished".into(),
                }],
                ..Default::default()
            }),
        );
        actor.refresh_matched_sports().unwrap();
        assert_eq!(
            actor
                .matched_sports
                .markets
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![202]
        );
        assert!(
            first
                .recording_instrument_ids()
                .all(|id| !actor.market_data_live.contains(&id))
        );
        assert!(
            next.recording_instrument_ids()
                .all(|id| actor.market_data_live.contains(&id))
        );
        assert_eq!(
            commands.borrow().len() - before_promotion,
            6,
            "only old market-data unsubscribe commands are sent at promotion"
        );
        assert!(actor.matched_sports.preparing.is_none());
        assert_eq!(actor.matched_sports.active, BTreeSet::from([202]));
        emit_recording_batch(&mut actor, &cache, &first, HOUR + 2_000_000_000, false);
        clock
            .borrow_mut()
            .advance_time((HOUR + 3_000_000_000).into(), true);
        emit_sized_recording_batch(&mut actor, &cache, &next, HOUR + 3_000_000_000, false, "3");
        DataActor::on_stop(&mut actor).unwrap();
        let counts = md_counts(&commands.borrow());
        assert_eq!(counts.len(), 24);
        assert!(
            counts.values().all(|count| *count == 1),
            "each market/family subscribes and unsubscribes exactly once"
        );
        for market in [&first, &next] {
            for id in market.recording_instrument_ids() {
                assert_eq!(counts.get(&("subscribe", "trades", id)), Some(&1));
                assert_eq!(counts.get(&("unsubscribe", "trades", id)), Some(&1));
            }
        }
        let mut catalog = nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog::new(
            &root, None, None, None, None,
        );
        let trades = catalog.trade_ticks(None, None, None).unwrap();
        assert_eq!(trades.len(), 15);
        assert!(
            trades
                .iter()
                .all(|t| t.ts_init.as_u64() != HOUR + 2_500_000_000),
            "cold successor data is outside ready-stream coverage"
        );
        let mut identities = BTreeSet::new();
        for trade in &trades {
            assert!(identities.insert((trade.instrument_id, trade.trade_id)));
            assert_ne!(
                trade.ts_init.as_u64(),
                HOUR + 2_000_000_000,
                "retired late trade was recorded"
            );
        }
        let depths = catalog.order_book_depths(None, None, None).unwrap();
        assert_eq!(depths.len(), 5);
        for market in [&first, &next] {
            let expected = 2;
            for outcome in &market.outcomes {
                assert_eq!(
                    sealed_files(&root, "order_book_deltas", outcome.polymarket_instrument),
                    expected
                );
                assert_eq!(
                    sealed_files(&root, "trades", outcome.polymarket_instrument),
                    expected
                );
                let report = catalog_capture_core::validate_order_book_replay(
                    &root,
                    outcome.polymarket_instrument,
                    true,
                )
                .unwrap();
                assert_eq!(report.segment_count, expected);
                assert_eq!(report.snapshot_segment_count, expected);
                assert_eq!(report.final_bid_level_count, 1);
                assert_eq!(report.final_ask_level_count, 1);
            }
        }
        let deltas = catalog.order_book_deltas(None, None, None).unwrap();
        assert!(
            deltas
                .iter()
                .all(|d| d.ts_init.as_u64() != HOUR + 2_000_000_000)
        );
        assert_eq!(
            deltas.len(),
            36,
            "raw snapshots/checkpoints and increments must have exact cardinality"
        );
        for outcome in &next.outcomes {
            let last = deltas
                .iter()
                .rfind(|d| d.instrument_id == outcome.polymarket_instrument)
                .unwrap();
            assert_eq!(last.order.size, nautilus_model::types::Quantity::from("3"));
            assert_eq!(last.ts_init.as_u64(), HOUR + 3_000_000_000);
        }
        // The retained handler must not capture this test's state after teardown.
        msgbus::register_data_command_endpoint(
            endpoint,
            TypedIntoHandler::from(|_: DataCommand| {}),
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn preparation_timeout_retries_one_candidate_and_keeps_old_until_usable_books() {
        use nautilus_common::{
            messages::data::DataCommand,
            msgbus::{MessagingSwitchboard, typed_handler::TypedIntoHandler},
        };
        let root = std::env::temp_dir().join(format!(
            "matched-preparation-retry-{}",
            nautilus_core::UUID4::new()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let inbox = Arc::new(MatchedSportsDiscoveryInbox::default());
        let mut config = CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", root.display()),
                ..Default::default()
            },
            CapturePlan::default(),
        );
        config.matched_sports_config = Some(MatchedSportsCaptureConfig {
            preparation_timeout_secs: 5,
            preparation_retry_secs: 5,
            ..Default::default()
        });
        config.matched_sports_inbox = Some(inbox.clone());
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        let first = pair(301);
        let next = pair(302);
        let third = pair(303);
        for definition in definitions(&first) {
            cache.borrow_mut().add_instrument(definition).unwrap();
        }
        let commands = Rc::new(RefCell::new(Vec::new()));
        let received = commands.clone();
        let endpoint = MessagingSwitchboard::data_engine_queue_execute();
        msgbus::register_data_command_endpoint(
            endpoint,
            TypedIntoHandler::from(move |c: DataCommand| received.borrow_mut().push(c)),
        );
        let mut actor = CatalogCaptureActor::new(config).unwrap();
        actor
            .register(TraderId::test_default(), clock.clone(), cache.clone())
            .unwrap();
        DataActor::on_start(&mut actor).unwrap();
        let generation = actor.matched_sports.generation;
        inbox.publish(
            generation,
            Ok(MatchedUniverse {
                markets: vec![first.clone(), next.clone(), third.clone()],
                admission_ids: Some(vec![301, 302, 303]),
                ..Default::default()
            }),
        );
        actor.refresh_matched_sports().unwrap();
        emit_recording_batch(&mut actor, &cache, &first, 1_000_000_000, true);
        assert_eq!(actor.matched_sports.markets.len(), 2);
        assert_eq!(
            actor.matched_sports.preparing.as_ref().unwrap().market_id,
            302
        );
        clock.borrow_mut().advance_time(5_000_000_000.into(), true);
        actor.refresh_matched_sports().unwrap();
        assert_eq!(
            actor.matched_sports.preparing.as_ref().unwrap().market_id,
            303
        );
        assert_eq!(actor.matched_sports.markets.len(), 2);
        assert!(!actor.matched_sports.markets.contains_key(&302));
        assert_eq!(
            md_counts(&commands.borrow()).values().sum::<usize>(),
            6,
            "cold metadata timeout must not emit phantom unsubscribe commands"
        );
        // Late metadata is cacheable by Native but cannot write orphan definitions or subscribe.
        for definition in definitions(&next) {
            cache
                .borrow_mut()
                .add_instrument(definition.clone())
                .unwrap();
            actor.on_instrument(&definition).unwrap();
        }
        assert!(
            next.instrument_ids()
                .all(|id| !actor.matched_sports.instrument_versions.contains_key(&id))
        );
        assert_eq!(md_counts(&commands.borrow()).values().sum::<usize>(), 6);
        clock.borrow_mut().advance_time(6_000_000_000.into(), true);
        inbox.publish(
            generation,
            Ok(MatchedUniverse {
                markets: vec![first.clone(), next.clone()],
                admission_ids: Some(vec![301, 302]),
                ..Default::default()
            }),
        );
        actor.refresh_matched_sports().unwrap();
        assert!(
            actor.matched_sports.preparing.is_none(),
            "retry cooldown cannot be bypassed by discovery"
        );
        inbox.publish(generation, Err("temporary discovery outage".into()));
        actor.refresh_matched_sports().unwrap();
        assert_eq!(actor.matched_sports.active, BTreeSet::from([301]));
        clock.borrow_mut().advance_time(10_000_000_000.into(), true);
        actor.refresh_matched_sports().unwrap();
        assert_eq!(
            actor.matched_sports.preparing.as_ref().unwrap().market_id,
            302
        );
        assert!(!actor.matched_sports.preparing.as_ref().unwrap().ready);
        assert!(
            next.recording_instrument_ids()
                .all(|id| actor.market_data_live.contains(&id))
        );
        assert_eq!(md_counts(&commands.borrow()).values().sum::<usize>(), 12);
        inbox.publish(
            generation,
            Ok(MatchedUniverse {
                markets: vec![next.clone()],
                admission_ids: Some(vec![302]),
                rejected: vec![catalog_capture_core::paired_markets::MatchRejection {
                    predict_market_id: 301,
                    reason: "closed_or_event_finished".into(),
                }],
                ..Default::default()
            }),
        );
        actor.refresh_matched_sports().unwrap();
        assert!(
            actor.matched_sports.markets.contains_key(&301),
            "old market exits only after successor readiness"
        );
        let id = next.outcomes[0].polymarket_instrument;
        let empty =
            nautilus_model::orderbook::OrderBook::new(id, nautilus_model::enums::BookType::L2_MBP);
        cache.borrow_mut().add_order_book(empty.clone()).unwrap();
        actor
            .on_book_deltas(&empty.to_deltas(10_000_000_000.into(), 10_000_000_000.into()))
            .unwrap();
        assert_eq!(
            actor.matched_sports.active,
            BTreeSet::from([301]),
            "an empty initial book cannot authorize handover"
        );
        clock.borrow_mut().advance_time(10_500_000_000.into(), true);
        emit_recording_batch(&mut actor, &cache, &next, 10_500_000_000, true);
        assert_eq!(actor.matched_sports.active, BTreeSet::from([302]));
        assert_eq!(actor.matched_sports.markets.len(), 1);
        assert!(actor.matched_sports.preparing.is_none());
        emit_recording_batch(&mut actor, &cache, &first, 11_000_000_000, false);
        DataActor::on_stop(&mut actor).unwrap();
        let counts = md_counts(&commands.borrow());
        assert_eq!(counts.len(), 24);
        assert!(
            counts.values().all(|v| *v == 1),
            "failure/retry and promotion must not duplicate market-data subscriptions"
        );
        let mut catalog = nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog::new(
            &root, None, None, None, None,
        );
        assert_eq!(catalog.trade_ticks(None, None, None).unwrap().len(), 6);
        assert_eq!(
            catalog.order_book_depths(None, None, None).unwrap().len(),
            2
        );
        for id in next.instrument_ids() {
            assert_eq!(
                catalog
                    .instruments(Some(&[id.to_string()]), None, None)
                    .unwrap()
                    .len(),
                1,
                "late metadata and retry must not duplicate definitions"
            );
        }
        assert!(actor.matched_sports.markets.is_empty());
        assert!(actor.matched_sports.retry_after.is_empty());
        let audit = std::fs::read_to_string(root.join("matched_sports_capture.jsonl")).unwrap();
        assert!(
            audit
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .any(|v| v["kind"] == "retired" && v["detail"]["reason"] == "preparation_timeout")
        );
        msgbus::register_data_command_endpoint(
            endpoint,
            TypedIntoHandler::from(|_: DataCommand| {}),
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn stopping_with_cold_preparation_cancels_both_roles_and_stale_generation() {
        use nautilus_common::{
            messages::data::DataCommand,
            msgbus::{MessagingSwitchboard, typed_handler::TypedIntoHandler},
        };
        let root = std::env::temp_dir().join(format!(
            "matched-preparation-stop-{}",
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
        let inbox = Arc::new(MatchedSportsDiscoveryInbox::default());
        config.matched_sports_config = Some(MatchedSportsCaptureConfig::default());
        config.matched_sports_inbox = Some(inbox.clone());
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        let first = pair(401);
        let next = pair(402);
        for definition in definitions(&first) {
            cache.borrow_mut().add_instrument(definition).unwrap();
        }
        let commands = Rc::new(RefCell::new(Vec::new()));
        let received = commands.clone();
        let endpoint = MessagingSwitchboard::data_engine_queue_execute();
        msgbus::register_data_command_endpoint(
            endpoint,
            TypedIntoHandler::from(move |c: DataCommand| received.borrow_mut().push(c)),
        );
        let mut actor = CatalogCaptureActor::new(config).unwrap();
        actor
            .register(
                TraderId::test_default(),
                Rc::new(RefCell::new(VirtualClock::new())),
                cache.clone(),
            )
            .unwrap();
        actor.start_matched_sports().unwrap();
        let old = actor.matched_sports.generation;
        let universe = MatchedUniverse {
            markets: vec![first.clone(), next],
            admission_ids: Some(vec![401, 402]),
            ..Default::default()
        };
        inbox.publish(old, Ok(universe.clone()));
        actor.refresh_matched_sports().unwrap();
        emit_recording_batch(&mut actor, &cache, &first, 1_000_000_000, true);
        assert!(actor.matched_sports.preparing.is_some());
        actor.stop_matched_sports().unwrap();
        assert!(actor.matched_sports.markets.is_empty());
        assert!(actor.matched_sports.preparing.is_none());
        assert!(actor.matched_sports.universe.is_none());
        assert!(actor.matched_sports.retry_after.is_empty());
        assert!(actor.pending_market_data.is_empty());
        assert!(actor.market_data_live.is_empty());
        let count = commands.borrow().len();
        actor.stop_matched_sports().unwrap();
        assert_eq!(commands.borrow().len(), count);
        let counts = md_counts(&commands.borrow());
        assert_eq!(counts.len(), 12);
        assert!(counts.values().all(|v| *v == 1));
        actor.start_matched_sports().unwrap();
        assert_ne!(actor.matched_sports.generation, old);
        inbox.publish(old, Ok(universe));
        actor.refresh_matched_sports().unwrap();
        assert!(actor.matched_sports.markets.is_empty());
        assert_eq!(commands.borrow().len(), count);
        actor.stop_matched_sports().unwrap();
        actor.shutdown_all().unwrap();
        msgbus::register_data_command_endpoint(
            endpoint,
            TypedIntoHandler::from(|_: DataCommand| {}),
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    fn sealed_files(root: &std::path::Path, family: &str, id: InstrumentId) -> usize {
        std::fs::read_dir(root.join("data").join(family).join(id.to_string()))
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name().to_string_lossy().ends_with(".parquet")
                    && !e.file_name().to_string_lossy().contains(".part")
            })
            .count()
    }

    fn emit_recording_batch(
        actor: &mut CatalogCaptureActor,
        cache: &Rc<RefCell<Cache>>,
        market: &MatchedMarket,
        ts: u64,
        snapshot: bool,
    ) {
        emit_sized_recording_batch(
            actor,
            cache,
            market,
            ts,
            snapshot,
            if snapshot { "1" } else { "2" },
        );
    }
    fn emit_sized_recording_batch(
        actor: &mut CatalogCaptureActor,
        cache: &Rc<RefCell<Cache>>,
        market: &MatchedMarket,
        ts: u64,
        snapshot: bool,
        size: &str,
    ) {
        use nautilus_model::{
            data::BookOrder,
            enums::{BookAction, BookType, OrderSide, RecordFlag},
            orderbook::OrderBook,
            types::{Price, Quantity},
        };
        for outcome in &market.outcomes {
            let id = outcome.polymarket_instrument;
            let flags = RecordFlag::F_LAST as u8
                | if snapshot {
                    RecordFlag::F_SNAPSHOT as u8
                } else {
                    0
                };
            let mut rows = Vec::new();
            if snapshot {
                rows.push(OrderBookDelta::clear(id, 1, ts.into(), ts.into()));
            }
            rows.push(OrderBookDelta::new(
                id,
                if snapshot {
                    BookAction::Add
                } else {
                    BookAction::Update
                },
                BookOrder::new(OrderSide::Buy, Price::from("0.50"), Quantity::from(size), 0),
                flags & !(RecordFlag::F_LAST as u8),
                if snapshot { 1 } else { 2 },
                ts.into(),
                ts.into(),
            ));
            rows.push(OrderBookDelta::new(
                id,
                if snapshot {
                    BookAction::Add
                } else {
                    BookAction::Update
                },
                BookOrder::new(
                    OrderSide::Sell,
                    Price::from("0.60"),
                    Quantity::from(size),
                    0,
                ),
                flags,
                if snapshot { 1 } else { 2 },
                ts.into(),
                ts.into(),
            ));
            let deltas = OrderBookDeltas::new(id, rows);
            if snapshot {
                let mut book = OrderBook::new(id, BookType::L2_MBP);
                book.apply_deltas(&deltas).unwrap();
                cache.borrow_mut().add_order_book(book).unwrap();
            } else {
                cache
                    .borrow_mut()
                    .order_book_mut(&id)
                    .unwrap()
                    .apply_deltas(&deltas)
                    .unwrap();
            }
            actor.on_book_deltas(&deltas).unwrap();
        }
        for id in market.recording_instrument_ids() {
            actor
                .on_trade(&TradeTick::new(
                    id,
                    "0.50".parse().unwrap(),
                    "1".parse().unwrap(),
                    nautilus_model::enums::AggressorSide::Buy,
                    nautilus_model::identifiers::TradeId::new(format!("trade-{ts}")),
                    ts.into(),
                    ts.into(),
                ))
                .unwrap();
        }
        let id = market.outcomes[0].predict_instrument;
        actor
            .on_book_depth(&OrderBookDepth::new(
                id,
                vec![BookOrder::new(
                    OrderSide::Buy,
                    Price::from("0.50"),
                    Quantity::from("1"),
                    0,
                )],
                vec![BookOrder::new(
                    OrderSide::Sell,
                    Price::from("0.60"),
                    Quantity::from("1"),
                    0,
                )],
                vec![1],
                vec![1],
                RecordFlag::F_SNAPSHOT as u8,
                0,
                ts.into(),
                ts.into(),
            ))
            .unwrap();
    }

    #[test]
    fn hourly_seal_reopens_with_book_baseline_and_stop_commits_the_partial_hour() {
        const HOUR: u64 = 3_600_000_000_000;
        let root =
            std::env::temp_dir().join(format!("matched-hourly-{}", nautilus_core::UUID4::new()));
        std::fs::create_dir_all(&root).unwrap();
        let mut capture = CaptureConfig {
            catalog_uri: format!("file://{}", root.display()),
            ..Default::default()
        };
        capture.lifecycle.seal = catalog_capture_core::lifecycle::SealConfigFile {
            enabled: true,
            schedule: "00:00".into(),
            timezone: "UTC".into(),
            interval_secs: 3600,
        };
        let mut config = CatalogCaptureActorConfig::new(capture, CapturePlan::default());
        config.matched_sports_config = Some(MatchedSportsCaptureConfig::default());
        config.matched_sports_inbox = Some(Arc::new(MatchedSportsDiscoveryInbox::default()));
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock
            .borrow_mut()
            .advance_time((HOUR - 1_000_000_000).into(), true);
        let cache = Rc::new(RefCell::new(Cache::new(None, None)));
        let mut actor = CatalogCaptureActor::new(config).unwrap();
        actor
            .register(TraderId::test_default(), clock.clone(), cache.clone())
            .unwrap();
        DataActor::on_start(&mut actor).unwrap();
        let market = pair(99);
        actor
            .apply_matched_sports_universe(MatchedUniverse {
                markets: vec![market.clone()],
                ..Default::default()
            })
            .unwrap();
        for id in market.instrument_ids() {
            actor.market_data_live.insert(id);
        }
        emit_recording_batch(&mut actor, &cache, &market, HOUR - 500_000_000, true);
        optional_flush_all(&actor.book_delta_runtime).unwrap();
        optional_flush_all(&actor.trade_runtime).unwrap();
        optional_flush_all(&actor.book_depth_runtime).unwrap();
        for o in &market.outcomes {
            assert_eq!(
                sealed_files(&root, "order_book_deltas", o.polymarket_instrument),
                0
            );
        }
        // Exercise the actual actor timer dispatch, one second after startup (no real wait).
        let events = clock.borrow_mut().advance_time(HOUR.into(), true);
        let seal = events
            .iter()
            .find(|e| e.name == super::super::SEGMENT_SEAL_TIMER)
            .unwrap();
        actor.on_time_event(seal).unwrap();
        optional_flush_all(&actor.book_delta_runtime).unwrap();
        for o in &market.outcomes {
            assert_eq!(
                sealed_files(&root, "order_book_deltas", o.polymarket_instrument),
                1
            );
        }
        clock
            .borrow_mut()
            .advance_time((HOUR + 1_000_000_000).into(), true);
        emit_recording_batch(&mut actor, &cache, &market, HOUR + 1_000_000_000, false);
        DataActor::on_stop(&mut actor).unwrap();
        // The second file contains just a second of recording, not a full hour.
        for o in &market.outcomes {
            assert_eq!(
                sealed_files(&root, "order_book_deltas", o.polymarket_instrument),
                2
            );
            assert_eq!(sealed_files(&root, "trades", o.polymarket_instrument), 2);
            let report = catalog_capture_core::validate_order_book_replay(
                &root,
                o.polymarket_instrument,
                true,
            )
            .unwrap();
            assert_eq!(report.segment_count, 2);
            assert_eq!(report.snapshot_segment_count, 2);
            assert_eq!(report.final_bid_level_count, 1);
        }
        assert_eq!(
            sealed_files(
                &root,
                "order_book_depths",
                market.outcomes[0].predict_instrument
            ),
            2
        );
        assert_eq!(
            sealed_files(&root, "trades", market.outcomes[0].predict_instrument),
            2
        );
        assert_eq!(
            sealed_files(
                &root,
                "order_book_depths",
                market.outcomes[1].predict_instrument
            ),
            0
        );
        assert_eq!(
            sealed_files(&root, "trades", market.outcomes[1].predict_instrument),
            0
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
