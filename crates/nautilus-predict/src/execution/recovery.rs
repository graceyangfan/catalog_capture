// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/LICENSE-3.0.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Bounded recovery intents for rejected settlements, never order or position state.
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use nautilus_model::identifiers::{ClientOrderId, TradeId};

pub(super) type SharedRecovery = Arc<Mutex<SettlementRecovery>>;
const MAX_ATTEMPTS: u32 = 5;
const MAX_ACTIVE_QUERIES: usize = 4;

#[derive(Debug, Default)]
pub(super) struct SettlementRecovery {
    entries: HashMap<ClientOrderId, Request>,
    active: HashSet<ClientOrderId>,
}
#[derive(Debug, Default)]
struct Request {
    trades: HashSet<TradeId>,
    attempts: u32,
    scheduled: bool,
    next_attempt: u64,
    blocked: bool,
}
impl SettlementRecovery {
    pub fn request(&mut self, owner: ClientOrderId, trade: TradeId) {
        self.entries.entry(owner).or_default().trades.insert(trade);
    }
    pub fn contains(&self, owner: ClientOrderId) -> bool {
        self.entries.contains_key(&owner)
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn resolved(&mut self, owner: ClientOrderId, trade: TradeId) {
        if let Some(request) = self.entries.get_mut(&owner) {
            request.trades.remove(&trade);
            if request.trades.is_empty() {
                self.entries.remove(&owner);
            }
        }
    }
    pub fn reconnect(&mut self) {
        self.active.clear();
        for request in self.entries.values_mut() {
            request.scheduled = false;
            request.blocked = false;
            request.attempts = 0;
            request.next_attempt = 0;
        }
    }
    pub fn due(&mut self, now: u64) -> Vec<ClientOrderId> {
        let mut due = Vec::new();
        let scheduled = self
            .entries
            .values()
            .filter(|request| request.scheduled)
            .count();
        let available =
            MAX_ACTIVE_QUERIES.saturating_sub(self.active.len().saturating_add(scheduled));
        for (owner, request) in &mut self.entries {
            if due.len() == available {
                break;
            }
            if request.blocked
                || request.scheduled
                || self.active.contains(owner)
                || now < request.next_attempt
            {
                continue;
            }
            if request.attempts >= MAX_ATTEMPTS {
                request.blocked = true;
                log::error!(
                    "Predict settlement recovery exhausted for {owner}; unresolved execution retained, explicit query or reconnect required"
                );
                continue;
            }
            request.scheduled = true;
            due.push(*owner);
        }
        due
    }
    pub fn block(&mut self, owner: ClientOrderId) {
        if let Some(request) = self.entries.get_mut(&owner) {
            request.blocked = true;
            request.scheduled = false;
        }
    }
}

// Serialize queries for an unresolved settlement, including cancellation.
// Ordinary Native/strategy queries retain their independent feedback contract.
// A queued report does not resolve an intent: only the Native fill observer does.
pub(super) struct QueryGuard {
    recovery: SharedRecovery,
    owner: ClientOrderId,
    clock: &'static nautilus_core::time::AtomicTime,
}
impl QueryGuard {
    pub fn acquire(
        recovery: SharedRecovery,
        owner: ClientOrderId,
        clock: &'static nautilus_core::time::AtomicTime,
    ) -> Option<Self> {
        {
            let mut state = recovery.lock().expect("settlement recovery poisoned");
            if !state.active.insert(owner) {
                return None;
            }
            if let Some(request) = state.entries.get_mut(&owner) {
                request.scheduled = false;
                request.attempts = request.attempts.saturating_add(1);
            }
        }
        Some(Self {
            recovery,
            owner,
            clock,
        })
    }
}
impl Drop for QueryGuard {
    fn drop(&mut self) {
        let mut state = self.recovery.lock().expect("settlement recovery poisoned");
        state.active.remove(&self.owner);
        if let Some(request) = state.entries.get_mut(&self.owner) {
            let seconds = (1u64 << request.attempts.min(5)).min(30);
            request.next_attempt = self
                .clock
                .get_time_ns()
                .as_u64()
                .saturating_add(seconds * 1_000_000_000);
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Predict settlement history is not indexed yet")]
pub(super) struct HistoryPending;

pub(super) fn retryable(error: &anyhow::Error) -> bool {
    use crate::http::error::{Error, PredictTransportErrorKind};
    if error.is::<HistoryPending>() || error.is::<tokio::time::error::Elapsed>() {
        return true;
    }
    matches!(
        error.downcast_ref::<Error>(),
        Some(
            Error::Timeout { .. }
                | Error::RateLimit { .. }
                | Error::Transport {
                    kind: PredictTransportErrorKind::Network
                        | PredictTransportErrorKind::HttpClient,
                    ..
                }
                | Error::Http {
                    status: 404 | 408 | 429 | 500..=599,
                    ..
                }
                | Error::Api {
                    status: 404 | 408 | 429 | 500..=599,
                    ..
                }
        )
    )
}

#[cfg(test)]
#[path = "../../tests/unit/recovery.rs"]
mod tests;
