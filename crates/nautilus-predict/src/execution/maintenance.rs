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

//! Account refresh, one-shot Native query dispatch and bounded mirror cleanup.

use std::{
    collections::HashSet,
    rc::Rc,
    sync::{Arc, atomic::Ordering},
};

use nautilus_common::{
    messages::execution::{QueryAccount, QueryOrder, TradingCommand},
    msgbus::{self, switchboard::MessagingSwitchboard},
    timer::TimeEventCallback,
};
use nautilus_core::{DurationNanos, UUID4};
use nautilus_model::orders::Order;

use crate::execution::{PredictExecutionClient, account};
use crate::execution::{context, fills, metadata, orders};

#[cfg(test)]
thread_local! {
    static INSTRUMENT_OWNER_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

// Core-thread view shared by request boundaries and the Native timer.
// It owns no trading state and is released when that timer generation is stopped.
struct MirrorCleanup {
    core: nautilus_live::ExecutionClientCore,
    account_orders: context::OrderSnapshots<nautilus_model::identifiers::ClientOrderId>,
    orders: context::OrderSnapshots<nautilus_model::identifiers::VenueOrderId>,
    account_sync: Arc<account::AccountSync>,
    fill_evidence: fills::SharedFillEvidence,
    instruments: Arc<
        nautilus_core::AtomicMap<
            nautilus_model::identifiers::InstrumentId,
            metadata::ExecutionInstrument,
        >,
    >,
    rest_order_ids: orders::RestOrderIds,
    account_refresh_required: Arc<std::sync::atomic::AtomicBool>,
    cursor: std::rc::Rc<std::cell::Cell<(usize, usize)>>,
}

// Cache and account borrows end before Native dispatch, which may be reentrant.
fn dispatch_order_queries(
    core: &nautilus_live::ExecutionClientCore,
    pending: &orders::PendingSubmissions,
    clock: &'static nautilus_core::time::AtomicTime,
) {
    let requests = std::mem::take(
        &mut pending
            .lock()
            .expect("order requests poisoned")
            .order_status_requests,
    );
    let commands = {
        let cache = core.cache();
        requests
            .into_iter()
            .filter_map(|id| {
                let order = cache.order(&id)?;
                (!order.is_closed() && order.account_id() == Some(core.account_id)).then(|| {
                    QueryOrder::new(
                        core.trader_id,
                        Some(core.client_id),
                        order.strategy_id(),
                        order.instrument_id(),
                        id,
                        order.venue_order_id(),
                        UUID4::new(),
                        clock.get_time_ns(),
                        None,
                        None,
                    )
                })
            })
            .collect::<Vec<_>>()
    };
    for command in commands {
        msgbus::send_trading_command(
            MessagingSwitchboard::exec_engine_queue_execute(),
            TradingCommand::QueryOrder(command),
        );
    }
}

impl PredictExecutionClient {
    /// Release terminal worker snapshots only after Native has purged their owners.
    /// Uses the installed maintenance timer, or request/instrument boundaries in manual mode.
    /// Atomic evidence retires only with its fully applied terminal Native owner.
    fn mirror_cleanup(&self) -> MirrorCleanup {
        MirrorCleanup {
            core: self.core.clone(),
            account_orders: self.account_orders.clone(),
            orders: self.orders.clone(),
            account_sync: self.account_sync.clone(),
            fill_evidence: self.fill_evidence.clone(),
            instruments: self.metadata.instruments.clone(),
            rest_order_ids: self.rest_order_ids.clone(),
            account_refresh_required: self.account_refresh_required.clone(),
            cursor: self.cleanup_cursor.clone(),
        }
    }

    fn prune_mirrors(&self) {
        self.mirror_cleanup().prune();
    }

    pub(super) fn prune_mirrors_on_request(&self) {
        let timer = format!("predict-execution-maintenance-{}", self.core.client_id);
        let scheduled = self
            .maintenance_clock
            .as_ref()
            .is_some_and(|clock| clock.borrow().timer_exists(&timer.as_str().into()));
        // Timer-driven maintenance owns full sweeps. Explicit requests clean up
        // in manual mode, including clients without an installed Native clock.
        if !scheduled {
            self.prune_mirrors();
        }
    }

    pub(super) fn start_maintenance_timer(&mut self) -> anyhow::Result<()> {
        let Some(secs) = self.config.reconnect_reconciliation_interval_secs else {
            return Ok(());
        };
        if self.config.account_state_mode != crate::config::PredictAccountStateMode::Reported {
            return Ok(());
        }
        let auth = self
            .wallet_auth
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Predict maintenance lacks wallet authentication"))?;
        let epoch = self
            .wallet_epoch
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Predict maintenance lacks wallet epoch"))?;
        let refreshed = self.refreshed_epoch.clone();
        let required = self.account_refresh_required.clone();
        let checking = self.account_query_active.clone();
        let pending = self.pending_submissions.clone();
        let core = self.core.clone();
        let healthy = self.healthy.clone();
        let cancellation = self.tasks.cancellation_token();
        let clock = self.clock;
        let mirrors = self.mirror_cleanup();
        let cleanup_interval = self
            .config
            .account_refresh_interval_secs
            .saturating_mul(1_000_000_000);
        let next_cleanup = std::cell::Cell::new(0u64);
        let timer = self.maintenance_clock.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Predict maintenance requires the factory-provided Nautilus clock")
        })?;
        timer.borrow_mut().set_timer_ns(
            &format!("predict-execution-maintenance-{}", self.core.client_id),
            DurationNanos::from_secs(secs),
            None,
            None,
            Some(TimeEventCallback::RustLocal(Rc::new(move |event| {
                if cancellation.is_cancelled() || !healthy.load(Ordering::Acquire) {
                    return;
                }
                // Retire mirrors after Native purges, independent of incoming commands.
                // Reuse this lifecycle timer; no extra REST query or timer is created.
                if event.ts_event.as_u64() >= next_cleanup.get() {
                    let more = mirrors.prune();
                    // Continue the sweep on the next timer tick, then return
                    // to the normal cleanup cadence. Never drain it in one callback.
                    next_cleanup.set(if more {
                        event.ts_event.as_u64()
                    } else {
                        event.ts_event.as_u64().saturating_add(cleanup_interval)
                    });
                }
                if !auth.is_authenticated()
                    || epoch.load(Ordering::Acquire) != refreshed.load(Ordering::Acquire)
                {
                    required.store(true, Ordering::Release);
                }
                if auth.is_authenticated() {
                    dispatch_order_queries(&core, &pending, clock);
                }
                if !required.load(Ordering::Acquire)
                    || !auth.is_authenticated()
                    || checking.load(Ordering::Acquire)
                {
                    return;
                }
                // LiveNode owns order/position recovery. Refresh account resources
                // independently, including while an order or cancellation is unresolved.
                let endpoint = MessagingSwitchboard::exec_engine_queue_execute();
                msgbus::send_trading_command(
                    endpoint,
                    TradingCommand::QueryAccount(QueryAccount::new(
                        core.trader_id,
                        Some(core.client_id),
                        core.account_id,
                        UUID4::new(),
                        clock.get_time_ns(),
                        None,
                        None,
                    )),
                );
            }))),
            None,
            Some(false),
        )?;
        Ok(())
    }

    pub(super) fn stop_maintenance_timer(&self) {
        if let Some(clock) = &self.maintenance_clock {
            let mut clock = clock.borrow_mut();
            let name = format!("predict-execution-maintenance-{}", self.core.client_id);
            if clock.timer_exists(&name.as_str().into()) {
                clock.cancel_timer(&name);
            }
        }
    }
}

// Bound Native-thread work with cursors into the owner indexes.
// Swap-removal may defer an owner to the
// next pass; every stable retained owner is revisited after wrapping.
const CLEANUP_BATCH: usize = 128;
impl MirrorCleanup {
    fn prune(&self) -> bool {
        let cache = self.core.cache();
        let by_client = self
            .account_orders
            .lock()
            .expect("order snapshots poisoned");
        let by_hash = self.orders.lock().expect("order snapshots poisoned");
        let (mut client_cursor, mut hash_cursor) = self.cursor.get();
        let mut views = Vec::with_capacity(CLEANUP_BATCH * 2);
        // Each index contributes at most one batch, including hash-only owners.
        if client_cursor >= by_client.len() {
            client_cursor = 0;
        }
        let client_end = (client_cursor + CLEANUP_BATCH).min(by_client.len());
        for index in client_cursor..client_end {
            let (_, order) = by_client.get_index(index).expect("bounded client cursor");
            views.push((order.venue_order_id(), Arc::clone(order)));
        }
        client_cursor = client_end;
        if hash_cursor >= by_hash.len() {
            hash_cursor = 0;
        }
        let hash_end = (hash_cursor + CLEANUP_BATCH).min(by_hash.len());
        for index in hash_cursor..hash_end {
            let (hash, order) = by_hash.get_index(index).expect("bounded hash cursor");
            if !by_client.contains_key(&order.client_order_id()) {
                views.push((Some(*hash), Arc::clone(order)));
            }
        }
        hash_cursor = hash_end;
        self.cursor.set((client_cursor, hash_cursor));
        drop(by_hash);
        drop(by_client);
        // Validate history without pinning account/client/hash indexes. Reuse
        // the existing prefix slot so deferred cold owners are not rescanned.
        let mut candidates = Vec::new();
        let mut retirement_work = 0usize;
        let mut deferred = false;
        for (hash, order) in views {
            if !order.is_closed() || cache.order(&order.client_order_id()).is_some() {
                continue;
            }
            let work = {
                let mut evidence = self.fill_evidence.lock().expect("fill evidence poisoned");
                if let Some(hash) = hash
                    && evidence.has_unapplied(hash, &order)
                {
                    self.account_refresh_required.store(true, Ordering::Release);
                    continue;
                }
                order
                    .fill_count()
                    .saturating_add(hash.map_or(0, |hash| evidence.retirement_work(hash)))
            };
            if !candidates.is_empty() && retirement_work.saturating_add(work) > CLEANUP_BATCH {
                deferred = true;
                continue;
            }
            retirement_work = retirement_work.saturating_add(work);
            candidates.push((hash, order));
        }
        let mut sync = self
            .account_sync
            .state
            .lock()
            .expect("account sync poisoned");
        let mut by_client = self
            .account_orders
            .lock()
            .expect("order snapshots poisoned");
        let mut by_hash = self.orders.lock().expect("order snapshots poisoned");
        let mut evidence = self.fill_evidence.lock().expect("fill evidence poisoned");
        let mut retired_hashes = HashSet::new();
        let mut retired = 0;
        let mut released = Vec::new();
        let before_fill = sync.latest_fill;
        let mut changed_resources = false;
        for (hash, order) in candidates {
            let current = by_client
                .get(&order.client_order_id())
                .or_else(|| hash.and_then(|hash| by_hash.get(&hash)));
            if !current.is_some_and(|current| Arc::ptr_eq(current, &order)) {
                deferred = true;
                released.push(order);
                continue;
            }
            // Wallet evidence can arrive between validation and index mutation.
            if let Some(hash) = hash
                && evidence.has_unapplied(hash, &order)
            {
                self.account_refresh_required.store(true, Ordering::Release);
                continue;
            }
            if let Some(hash) = hash {
                retired_hashes.insert(hash);
                // Remove by the actual hash-index key, including pre-ACK hash-only views.
                by_hash.swap_remove(&hash);
            }
            let client = order.client_order_id();
            by_client.swap_remove(&client);
            changed_resources |= sync.active_orders.remove(&client).is_some();
            sync.latest_fill = sync
                .latest_fill
                .max(account::latest_venue_fill(order.as_ref()));
            retired += 1;
            released.push(order);
        }
        if !retired_hashes.is_empty() {
            sync.retire_orders(&retired_hashes);
            evidence.retire_orders(&retired_hashes);
            let mut routes = self.rest_order_ids.lock().expect("REST order IDs poisoned");
            for hash in &retired_hashes {
                routes.remove(hash);
            }
        }
        // Only missing Native definitions need the cold ownership checks. Do not
        // pin every expired definition merely because another market is trading.
        let mut stale = self
            .instruments
            .load()
            .keys()
            .filter(|id| cache.instrument(id).is_none())
            .copied()
            .collect::<ahash::AHashSet<_>>();
        if !stale.is_empty() {
            for order in by_client.values().chain(by_hash.values()) {
                stale.remove(&order.instrument_id());
                #[cfg(test)]
                INSTRUMENT_OWNER_READS.with(|n| n.set(n.get() + 1));
            }
            if !stale.is_empty() {
                for position in cache.positions_open(
                    Some(&self.core.venue),
                    None,
                    None,
                    Some(&self.core.account_id),
                    None,
                ) {
                    stale.remove(&position.instrument_id);
                }
            }
        }
        if !stale.is_empty() {
            self.instruments
                .rcu(|map| map.retain(|id, _| !stale.contains(id)));
        }
        let more = deferred
            || client_cursor < by_client.len()
            || hash_cursor < by_hash.len()
            || retired > 0 && (!by_client.is_empty() || !by_hash.is_empty());
        // Retiring already-closed mirrors does not change Native trading state.
        // Keep fences if a restored owner advances real freshness/reservation inputs.
        changed_resources |= sync.latest_fill != before_fill;
        if changed_resources {
            sync.resource_revision = sync.resource_revision.wrapping_add(1);
        }
        drop(evidence);
        drop(by_hash);
        drop(by_client);
        drop(sync);
        // Last history references can run large destructors. Never hold the WS
        // and account indexes during that release. A single owner is still O(N).
        drop(released);
        if changed_resources {
            self.account_sync.notify.notify_one();
        }
        more
    }
}

#[cfg(test)]
#[path = "../../tests/unit/lookup.rs"]
mod tests;
