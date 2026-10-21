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

//! Session setup, Native cache snapshots and teardown. Trading state stays in Native.
use std::{
    collections::HashSet,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use alloy::primitives::B256;
use nautilus_common::{
    clients::ExecutionClient,
    msgbus::{self, TypedHandler},
};
use nautilus_live::task::TaskGroupGuard;
use nautilus_model::{
    accounts::{Account, AccountAny},
    events::{AccountState, OrderEventAny},
    orders::Order,
};

use super::{PredictExecutionClient, account, runtime};
use crate::{config::PredictAccountStateMode, websocket::PredictWalletWebSocketClient};

#[cfg(test)]
thread_local! {
    static INSTRUMENT_OWNER_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

// A core-thread view used by lifecycle callbacks and the existing Native timer.
// It owns no trading state and is released when that timer generation is stopped.
pub(super) struct OrderLookups {
    core: nautilus_live::ExecutionClientCore,
    account_orders: super::types::OrderSnapshots<nautilus_model::identifiers::ClientOrderId>,
    orders: super::types::OrderSnapshots<nautilus_model::identifiers::VenueOrderId>,
    account_sync: Arc<account::AccountSync>,
    fill_evidence: super::precision::SharedFillEvidence,
    instruments: Arc<
        nautilus_core::AtomicMap<
            nautilus_model::identifiers::InstrumentId,
            super::types::ExecutionInstrument,
        >,
    >,
    rest_order_ids: super::routing::RestOrderIds,
    reconciliation_required: Arc<std::sync::atomic::AtomicBool>,
    cursor: std::rc::Rc<std::cell::Cell<(usize, usize)>>,
}

impl PredictExecutionClient {
    pub(super) fn prepare_lookup(&mut self) -> anyhow::Result<()> {
        self.cleanup_cursor.set((0, 0));
        let cache = self.core.cache();
        let mut definitions = ahash::AHashMap::new();
        for instrument in cache.instruments(&self.core.venue, None) {
            super::types::upsert_instrument(&mut definitions, instrument.clone());
        }
        self.instruments.store(definitions);
        let account_orders: indexmap::IndexMap<_, _, ahash::RandomState> = cache
            .orders_refs(
                Some(&self.core.venue),
                None,
                None,
                Some(&self.core.account_id),
                None,
            )
            .into_iter()
            .map(|order| {
                (
                    order.client_order_id(),
                    Arc::new(super::context::WorkerOrder::capture(&order)),
                )
            })
            .collect();
        let orders = account_orders
            .values()
            .filter_map(|order| {
                order
                    .venue_order_id()
                    .or_else(|| {
                        self.pending_submissions
                            .lock()
                            .expect("submission registry poisoned")
                            .entries
                            .get(&order.client_order_id())
                            .and_then(|entry| entry.venue_id)
                    })
                    .filter(|id| id.as_str().parse::<B256>().is_ok())
                    .map(|id| (id, Arc::clone(order)))
            })
            .collect();
        let active_orders = account_orders
            .values()
            .filter(|order| !order.is_closed())
            .map(|order| {
                let hash = order
                    .venue_order_id()
                    .or_else(|| {
                        self.pending_submissions
                            .lock()
                            .expect("submission registry poisoned")
                            .entries
                            .get(&order.client_order_id())
                            .and_then(|entry| entry.venue_id)
                    })
                    .filter(|id| id.as_str().parse::<B256>().is_ok());
                (order.client_order_id(), hash)
            })
            .collect();
        *self
            .account_orders
            .lock()
            .expect("order snapshots poisoned") = account_orders;
        *self.orders.lock().expect("order snapshots poisoned") = orders;
        if self.config.account_state_mode == PredictAccountStateMode::Reported
            && let Some(account) = cache.account(&self.core.account_id)
        {
            anyhow::ensure!(
                matches!(&*account, AccountAny::Cash(_))
                    && !account.calculated_account_state()
                    && account.base_currency().is_none()
                    && account
                        .balances()
                        .keys()
                        .all(|currency| *currency == nautilus_model::types::Currency::USDT()),
                "Predict balance reporting requires a reported cash account, not a calculated account"
            );
        }
        anyhow::ensure!(
            self.config.account_state_mode == PredictAccountStateMode::Reported
                || self
                    .orders
                    .lock()
                    .expect("order snapshots poisoned")
                    .is_empty()
                || matches!(
                    cache.account_owned(&self.core.account_id),
                    Some(AccountAny::Cash(_))
                ),
            "Predict cached-order reporting requires an initialized Nautilus cash account"
        );
        drop(cache);
        {
            let mut sync = self
                .account_sync
                .state
                .lock()
                .expect("account sync poisoned");
            sync.active_orders = active_orders;
            sync.latest_fill = sync.latest_fill.max(
                self.account_orders
                    .lock()
                    .expect("order snapshots poisoned")
                    .values()
                    .map(account::latest_venue_fill)
                    .max()
                    .unwrap_or_default(),
            );
        }
        if self.order_observer.is_none() {
            let core = self.core.clone();
            let orders = Arc::clone(&self.orders);
            let account_orders = Arc::clone(&self.account_orders);
            let account_sync = Arc::clone(&self.account_sync);
            let fill_evidence = Arc::clone(&self.fill_evidence);
            let pending_cancels = Arc::clone(&self.pending_cancels);
            let deferred_cancels = Arc::clone(&self.deferred_cancels);
            let pending_submissions = Arc::clone(&self.pending_submissions);
            let local_reporter = (self.config.account_state_mode
                == PredictAccountStateMode::Reported)
                .then(|| self.account_reporter().ok())
                .flatten();
            let handler = TypedHandler::from(move |event: &OrderEventAny| {
                if event.instrument_id().venue != core.venue {
                    return;
                }
                let (resources_changed, deferred, previous, order, registered, collateral) = {
                    let cache = core.cache();
                    if let Some(order) = cache.order(&event.client_order_id())
                        && order.account_id() == Some(core.account_id)
                    {
                        let mut sync = account_sync.state.lock().expect("account sync poisoned");
                        sync.revision = sync.revision.wrapping_add(1);
                        let (registered, collateral) = pending_submissions
                            .lock()
                            .expect("submission registry poisoned")
                            .entries
                            .get(&order.client_order_id())
                            .map(|entry| (entry.venue_id, entry.market_buy_collateral))
                            .unwrap_or_default();
                        let previous_native = account_orders
                            .lock()
                            .expect("order snapshots poisoned")
                            .get(&order.client_order_id())
                            .cloned();
                        // Signing installs the hash view before POST; it may have an
                        // emitted Accepted not yet applied to Native's client view.
                        let previous = order
                            .venue_order_id()
                            .or(registered)
                            .and_then(|hash| {
                                orders
                                    .lock()
                                    .expect("order snapshots poisoned")
                                    .get(&hash)
                                    .filter(|view| {
                                        view.client_order_id() == order.client_order_id()
                                    })
                                    .cloned()
                            })
                            .or_else(|| previous_native.clone());
                        let mut resources_changed = matches!(event, OrderEventAny::Filled(_))
                            || account::order_resources_changed(
                                previous_native.as_deref(),
                                &*order,
                            );
                        if let OrderEventAny::Filled(fill) = event
                            && !account::is_synthetic_order(&*order)
                        {
                            sync.latest_fill = sync.latest_fill.max(fill.ts_event);
                            sync.request_cash();
                            fill_evidence
                                .lock()
                                .expect("fill evidence poisoned")
                                .applied(fill.trade_id);
                        }
                        let order = Arc::new(super::context::WorkerOrder::observe(
                            &order,
                            event,
                            previous.as_deref(),
                        ));
                        if order.is_closed() {
                            pending_cancels
                                .lock()
                                .expect("pending cancels poisoned")
                                .remove(&order.client_order_id());
                        }
                        account_orders
                            .lock()
                            .expect("order snapshots poisoned")
                            .insert(order.client_order_id(), Arc::clone(&order));
                        if matches!(event, OrderEventAny::Filled(_))
                            && order.status() == nautilus_model::enums::OrderStatus::Filled
                            && !account::is_synthetic_order(&order)
                            && let Some(id) = order.venue_order_id().or(registered)
                        {
                            // A complete confirmed fill retires a stale OPEN reservation without a GET.
                            sync.confirmed_terminal.insert(id);
                        }
                        if order.is_closed() {
                            pending_submissions
                                .lock()
                                .expect("submission registry poisoned")
                                .entries
                                .remove(&order.client_order_id());
                        }
                        if matches!(
                            event,
                            OrderEventAny::Canceled(_) | OrderEventAny::Expired(_)
                        ) && !order
                            .venue_order_id()
                            .or(registered)
                            .is_some_and(|id| sync.confirmed_terminal.contains(&id))
                        {
                            // Native retry exhaustion is not venue cancellation evidence.
                            sync.invalidate_cash();
                            resources_changed = true;
                        }
                        if order.is_closed() {
                            sync.active_orders.remove(&order.client_order_id());
                        } else {
                            sync.active_orders.insert(
                                order.client_order_id(),
                                order
                                    .venue_order_id()
                                    .or(registered)
                                    .filter(|id| id.as_str().parse::<B256>().is_ok()),
                            );
                        }
                        if let Some(id) = order.venue_order_id().or(registered)
                            && id.as_str().parse::<B256>().is_ok()
                        {
                            orders
                                .lock()
                                .expect("order snapshots poisoned")
                                .insert(id, Arc::clone(&order));
                        }
                        let deferred = if order.is_closed() {
                            deferred_cancels
                                .lock()
                                .expect("deferred cancels poisoned")
                                .remove(&order.client_order_id());
                            None
                        } else if order.ts_accepted().is_some() && order.venue_order_id().is_some()
                        {
                            deferred_cancels
                                .lock()
                                .expect("deferred cancels poisoned")
                                .remove(&order.client_order_id())
                        } else {
                            None
                        };
                        if resources_changed {
                            sync.resource_revision = sync.resource_revision.wrapping_add(1);
                        }
                        drop(sync);
                        (
                            resources_changed,
                            deferred,
                            previous_native,
                            order,
                            registered,
                            collateral,
                        )
                    } else {
                        return;
                    }
                };
                if resources_changed {
                    // Local lock projections must follow Native order application.
                    // An async Accepted projection could otherwise arrive after a
                    // cancel callback and relock the replacement's available cash.
                    if !matches!(event, OrderEventAny::Filled(_))
                        && let Some(reporter) = &local_reporter
                        && let Err(error) = reporter.project_order_on_core(
                            &core,
                            previous.as_deref(),
                            &order,
                            registered,
                            collateral,
                        )
                    {
                        log::debug!("Predict callback-local account projection deferred: {error}");
                    }
                    account_sync.notify.notify_one();
                }
                if let Some(command) = deferred {
                    // Reuse normal command admission, REST identity proof and callbacks.
                    // Queue after native state application; never re-enter the engine here.
                    msgbus::send_trading_command(
                        nautilus_common::msgbus::MessagingSwitchboard::exec_engine_queue_execute(),
                        nautilus_common::messages::execution::TradingCommand::CancelOrder(command),
                    );
                }
            });
            msgbus::subscribe_order_events("events.order.*".into(), handler.clone(), Some(10));
            self.order_observer = Some(handler);
        }
        if self.account_observer.is_none()
            && self.config.account_state_mode == PredictAccountStateMode::Reported
            && let Ok(reporter) = self.account_reporter()
        {
            let core = self.core.clone();
            let account_id = core.account_id;
            let handler = TypedHandler::from(move |event: &AccountState| {
                if let Err(error) = reporter.on_account_state(&core, event) {
                    log::debug!("Predict applied-account projection deferred: {error}");
                }
            });
            msgbus::subscribe_account_state(
                format!("events.account.{account_id}").into(),
                handler.clone(),
                None,
            );
            self.account_observer = Some(handler);
        }
        self.metadata_notify.notify_one();
        Ok(())
    }

    /// Release terminal worker snapshots only after Native has purged their owners.
    /// Uses the installed recovery timer, or request/instrument boundaries in manual mode.
    /// Atomic evidence retires only with its fully applied terminal Native owner.
    pub(super) fn order_lookups(&self) -> OrderLookups {
        OrderLookups {
            core: self.core.clone(),
            account_orders: self.account_orders.clone(),
            orders: self.orders.clone(),
            account_sync: self.account_sync.clone(),
            fill_evidence: self.fill_evidence.clone(),
            instruments: self.instruments.clone(),
            rest_order_ids: self.rest_order_ids.clone(),
            reconciliation_required: self.reconciliation_required.clone(),
            cursor: self.cleanup_cursor.clone(),
        }
    }

    pub(super) fn prune_order_lookups(&self) {
        self.order_lookups().prune();
    }

    pub(super) fn prune_order_lookups_on_request(&self) {
        let timer = format!("predict-execution-recovery-{}", self.core.client_id);
        let scheduled = self
            .recovery_clock
            .as_ref()
            .is_some_and(|clock| clock.borrow().timer_exists(&timer.as_str().into()));
        // The existing timer owns maintenance. Explicit requests only clean up
        // in manual mode, including clients without an installed Native clock.
        if !scheduled {
            self.prune_order_lookups();
        }
    }

    pub(super) fn clear_observer(&mut self) {
        self.deferred_cancels
            .lock()
            .expect("deferred cancels poisoned")
            .clear();
        if let Some(handler) = self.order_observer.take() {
            msgbus::unsubscribe_order_events("events.order.*".into(), &handler);
        }
        if let Some(handler) = self.account_observer.take() {
            msgbus::unsubscribe_account_state(
                format!("events.account.{}", self.core.account_id).into(),
                &handler,
            );
        }
    }

    pub(super) fn account_reporter(&self) -> anyhow::Result<account::AccountReporter> {
        Ok(account::AccountReporter {
            allow_stream_gap: false,
            config: self.config.clone(),
            chain: self.chain.clone().ok_or_else(|| {
                anyhow::anyhow!("Predict account balance source is not configured")
            })?,
            instruments: Arc::clone(&self.instruments),
            orders: Arc::clone(&self.account_orders),
            pending: Arc::clone(&self.pending_submissions),
            rest_ids: Arc::clone(&self.rest_order_ids),
            sync: Arc::clone(&self.account_sync),
            emitter: self.emitter.clone(),
            clock: self.clock,
            authentication: self.wallet_auth.clone().ok_or_else(|| {
                anyhow::anyhow!("Predict wallet account source has no authentication tracker")
            })?,
            connection_epoch: self.wallet_epoch.clone().ok_or_else(|| {
                anyhow::anyhow!("Predict wallet account source has no connection identity")
            })?,
        })
    }

    pub(super) async fn await_account_registered(&self) -> anyhow::Result<()> {
        let expected = self
            .account_sync
            .state
            .lock()
            .expect("account sync poisoned")
            .last_event
            .ok_or_else(|| anyhow::anyhow!("Predict account initialization has no fresh report"))?;
        tokio::time::timeout(
            Duration::from_secs(self.config.report_timeout_secs),
            async {
                loop {
                    let applied = self
                        .core
                        .cache()
                        .account(&self.core.account_id)
                        .is_some_and(|account| {
                            matches!(&*account, AccountAny::Cash(_))
                                && account
                                    .last_event()
                                    .is_some_and(|event| event.event_id == expected)
                        });
                    if applied {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            },
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "Predict account report was not registered by the Nautilus portfolio before timeout"
            )
        })
    }
    pub(super) async fn connect_impl(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        anyhow::ensure!(
            self.config.account_state_mode == PredictAccountStateMode::Disabled
                || self.chain.is_some(),
            "Predict reported cash account requires an explicit balance RPC source"
        );
        anyhow::ensure!(
            self.tasks.is_empty(),
            "Predict execution tasks must be drained before reconnect"
        );
        anyhow::ensure!(
            self.emitter.is_initialized(),
            "Nautilus execution event sender is not installed"
        );
        if !self.tasks.is_open() {
            self.tasks.start_generation()?;
        }
        let healthy = Arc::clone(&self.healthy);
        let setup_guard = TaskGroupGuard::new(&[&self.tasks], move || {
            healthy.store(false, Ordering::Release)
        });
        self.config.session.request_account().await?;
        let mut ws = PredictWalletWebSocketClient::connect_observed(
            self.config.websocket.clone(),
            &self.config.session,
            self.tasks.cancellation_token(),
            self.config.latency.is_some(),
        )
        .await?;
        ws.wait_until_subscribed().await?;
        self.wallet_auth = Some(ws.authentication_tracker());
        self.wallet_epoch = Some(ws.connection_epoch_handle());
        self.verified_epoch.store(0, Ordering::Release);
        // Preserve ambiguous identities/budgets, but never reuse another transport's live-fill marker.
        for entry in self
            .pending_submissions
            .lock()
            .expect("submission registry poisoned")
            .entries
            .values_mut()
        {
            entry.wallet_epoch = None;
        }
        // A new transport restarts its epoch at zero. Nonces belong to this
        // connection setup, not to an equal counter from a previous connection.
        self.account_sync
            .state
            .lock()
            .expect("account sync poisoned")
            .clear_connection();
        self.prepare_lookup()?;
        self.start_recovery_timer()?;
        if self.config.account_state_mode == PredictAccountStateMode::Reported {
            let reporter = self.account_reporter()?;
            anyhow::ensure!(
                reporter.refresh().await?,
                "Predict native account context changed during initialization"
            );
            self.await_account_registered().await?;
            self.account_task = Some(self.tasks.spawn_named(
                "predict-account-reports",
                account::run(reporter, self.tasks.cancellation_token()),
            )?);
        }
        if self.config.submission.is_some() {
            let epoch = self.wallet_epoch.as_ref().expect("wallet connected");
            super::metadata::prepare(&self.config, &self.instruments, self.clock, epoch).await;
            self.tasks.spawn_named(
                "predict-market-metadata",
                super::metadata::run(
                    self.config.clone(),
                    Arc::clone(&self.instruments),
                    self.clock,
                    Arc::clone(epoch),
                    self.wallet_auth.clone().expect("wallet connected"),
                    Arc::clone(&self.metadata_notify),
                    self.tasks.cancellation_token(),
                ),
            )?;
        }
        anyhow::ensure!(
            ws.authentication_tracker().is_authenticated()
                && ws.connection_epoch_handle().load(Ordering::Acquire) == 0,
            "Predict wallet transport lost its original acknowledged connection during initialization"
        );
        self.start_authentication_refresh()?;
        self.healthy.store(true, Ordering::Release);
        let task = self.tasks.spawn_named(
            "predict-wallet-execution",
            runtime::run_wallet(
                ws,
                self.tasks.cancellation_token(),
                Arc::clone(&self.healthy),
                runtime::WalletReportContext {
                    instruments: Arc::clone(&self.instruments),
                    orders: Arc::clone(&self.orders),
                    rest_ids: Arc::clone(&self.rest_order_ids),
                    sync: Arc::clone(&self.account_sync),
                    pending: Arc::clone(&self.pending_submissions),
                    fill_evidence: Arc::clone(&self.fill_evidence),
                    reconciliation_required: Arc::clone(&self.reconciliation_required),
                    allow_share_fees: self.config.account_state_mode
                        == PredictAccountStateMode::Reported,
                    emitter: self.emitter.clone(),
                    account: self.config.account_id,
                    wallet: self.config.session.account_address(),
                    clock: self.clock,
                    latency: self.config.latency.clone(),
                    settlement_sender: self.settlement_sender.clone(),
                },
            ),
        );
        match task {
            Ok(task) => {
                self.stream_task = Some(task);
                self.core.set_connected();
                setup_guard.disarm();
                Ok(())
            }
            Err(error) => {
                self.healthy.store(false, Ordering::Release);
                Err(error.into())
            }
        }
    }
    pub(super) async fn disconnect_impl(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.tasks
            .finish_shutdown(Duration::from_secs(2), Duration::from_secs(2))
            .await?;
        self.stream_task = None;
        self.account_task = None;
        self.wallet_auth = None;
        self.wallet_epoch = None;
        Ok(())
    }
}

// Bound owner work on the Native thread. Cursors refer to the existing indexes,
// not another retained owner queue. Swap-removal may defer a moved owner to the
// next pass; every stable retained owner is revisited after wrapping.
pub(super) const CLEANUP_BATCH: usize = 128;
impl OrderLookups {
    pub(super) fn prune(&self) -> bool {
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
                    self.reconciliation_required.store(true, Ordering::Release);
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
                self.reconciliation_required.store(true, Ordering::Release);
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
            sync.latest_fill = sync.latest_fill.max(account::latest_venue_fill(&order));
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
            sync.revision = sync.revision.wrapping_add(1);
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
