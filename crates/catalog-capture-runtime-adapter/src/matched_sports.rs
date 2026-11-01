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

//! Bounded discovery mailbox and actor-owned recording subscriptions.
use catalog_capture_core::{
    CapturePlan, InstrumentCaptureSpec, TradeCaptureSpec,
    paired_markets::{MatchedMarket, MatchedUniverse},
    plan::{BookDeltasCaptureSpec, BookDepthCaptureSpec},
};
use nautilus_model::{enums::BookType, identifiers::InstrumentId};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

#[derive(Debug, Default)]
pub struct MatchedSportsDiscoveryInbox {
    generation: AtomicU64,
    active: AtomicBool,
    update: Mutex<Option<(u64, Result<MatchedUniverse, String>)>>,
}
impl MatchedSportsDiscoveryInbox {
    pub fn begin_generation(&self) -> u64 {
        let next = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        *self.update.lock().expect("MatchedSports inbox poisoned") = None;
        self.active.store(true, Ordering::Release);
        next
    }
    pub fn stop(&self) {
        self.active.store(false, Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel);
        *self.update.lock().expect("MatchedSports inbox poisoned") = None;
    }
    pub fn active_generation(&self) -> Option<u64> {
        self.active
            .load(Ordering::Acquire)
            .then(|| self.generation.load(Ordering::Acquire))
    }
    pub fn publish(&self, generation: u64, result: Result<MatchedUniverse, String>) {
        let mut slot = self.update.lock().expect("MatchedSports inbox poisoned");
        if self.active_generation() == Some(generation) {
            *slot = Some((generation, result));
        }
    }
    pub(crate) fn take(&self, generation: u64) -> Option<Result<MatchedUniverse, String>> {
        let mut slot = self.update.lock().expect("MatchedSports inbox poisoned");
        if slot.as_ref().is_some_and(|(g, _)| *g == generation) {
            slot.take().map(|(_, value)| value)
        } else {
            None
        }
    }
}
#[derive(Debug, Default)]
pub(crate) struct MatchedSportsState {
    pub generation: u64,
    /// All owned subscriptions: active slots plus at most one preparation.
    pub markets: BTreeMap<u64, MatchedMarket>,
    pub active: BTreeSet<u64>,
    pub preparing: Option<Preparation>,
    pub universe: Option<MatchedUniverse>,
    pub retry_after: BTreeMap<u64, u64>,
    pub usable_books: BTreeSet<InstrumentId>,
    pub observed: BTreeMap<InstrumentId, u64>,
    pub instrument_versions: BTreeMap<InstrumentId, u64>,
    pub intervals: BTreeMap<u64, bool>,
    pub retiring: BTreeMap<InstrumentId, u64>,
}
#[derive(Debug, Clone)]
pub(crate) struct Preparation {
    pub market_id: u64,
    pub started_ns: u64,
    pub ready: bool,
}
impl MatchedSportsState {
    pub fn plan(&self) -> CapturePlan {
        let mut plan = CapturePlan::default();
        for market in self.markets.values() {
            for o in &market.outcomes {
                for id in [o.predict_instrument, o.polymarket_instrument] {
                    plan.instruments
                        .push(InstrumentCaptureSpec { instrument_id: id });
                }
                plan.trades.push(TradeCaptureSpec {
                    instrument_id: o.polymarket_instrument,
                });
                if o.predict_index_set == 1 {
                    plan.trades.push(TradeCaptureSpec {
                        instrument_id: o.predict_instrument,
                    });
                    plan.book_depth.push(BookDepthCaptureSpec {
                        instrument_id: o.predict_instrument,
                        book_type: BookType::L2_MBP,
                        depth: None,
                    });
                }
                plan.book_deltas.push(BookDeltasCaptureSpec {
                    instrument_id: o.polymarket_instrument,
                    book_type: BookType::L2_MBP,
                    depth: None,
                });
            }
        }
        plan
    }
    pub fn observe(&mut self, id: InstrumentId, now: u64, snapshot: bool) {
        if self
            .markets
            .values()
            .any(|m| m.recording_instrument_ids().any(|i| i == id))
            && (snapshot || self.observed.contains_key(&id))
        {
            self.observed.insert(id, now);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mailbox_rejects_late_generation_and_keeps_latest_complete_refresh() {
        let inbox = MatchedSportsDiscoveryInbox::default();
        let old = inbox.begin_generation();
        let current = inbox.begin_generation();
        inbox.publish(
            current,
            Ok(MatchedUniverse {
                observed_ns: 7,
                ..Default::default()
            }),
        );
        inbox.publish(old, Err("late".into()));
        assert!(
            inbox.take(old).is_none(),
            "a retired consumer must not consume a new generation's update"
        );
        assert_eq!(inbox.take(current).unwrap().unwrap().observed_ns, 7);
        assert!(inbox.take(current).is_none());
    }
    #[test]
    fn stopped_mailbox_rejects_inflight_results_and_can_restart() {
        let inbox = MatchedSportsDiscoveryInbox::default();
        assert_eq!(inbox.active_generation(), None);
        let old = inbox.begin_generation();
        inbox.stop();
        inbox.publish(old, Ok(MatchedUniverse::default()));
        assert_eq!(inbox.active_generation(), None);
        let new = inbox.begin_generation();
        assert!(new > old);
        assert!(inbox.take(new).is_none());
        inbox.publish(new, Ok(MatchedUniverse::default()));
        assert!(inbox.take(new).unwrap().is_ok());
    }
}
