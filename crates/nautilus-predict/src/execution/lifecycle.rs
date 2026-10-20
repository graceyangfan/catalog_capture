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
use super::{PredictExecutionClient, account, runtime};
use crate::{config::PredictAccountStateMode, websocket::PredictWalletWebSocketClient};
use alloy::primitives::B256;
use nautilus_common::{
    clients::ExecutionClient,
    msgbus::{self, TypedHandler},
};
use nautilus_live::task::TaskGroupGuard;
use nautilus_model::{
    accounts::{Account, AccountAny},
    events::OrderEventAny,
    instruments::Instrument,
    orders::Order,
};
use std::{
    collections::HashSet,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

impl PredictExecutionClient {
    pub(super) fn prepare_lookup(&mut self) -> anyhow::Result<()> {
        let cache = self.core.cache();
        self.instruments.store(
            cache
                .instruments(&self.core.venue, None)
                .into_iter()
                .map(|i| (i.id(), i.clone()))
                .collect(),
        );
        self.account_orders.store(
            cache
                .orders_refs(
                    Some(&self.core.venue),
                    None,
                    None,
                    Some(&self.core.account_id),
                    None,
                )
                .into_iter()
                .map(|order| (order.client_order_id(), Arc::new((*order).clone())))
                .collect(),
        );
        self.orders.store(
            self.account_orders
                .load()
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
                                .copied()
                                .flatten()
                        })
                        .filter(|id| id.as_str().parse::<B256>().is_ok())
                        .map(|id| (id, Arc::clone(order)))
                })
                .collect(),
        );
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
                || self.orders.load().is_empty()
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
            sync.latest_fill = sync.latest_fill.max(
                self.account_orders
                    .load()
                    .values()
                    .map(|order| account::latest_venue_fill(order))
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
            let handler = TypedHandler::from(move |event: &OrderEventAny| {
                if event.instrument_id().venue != core.venue {
                    return;
                }
                let cache = core.cache();
                if let Some(order) = cache.order_owned(&event.client_order_id())
                    && order.account_id() == Some(core.account_id)
                {
                    let mut sync = account_sync.state.lock().expect("account sync poisoned");
                    sync.revision = sync.revision.wrapping_add(1);
                    if let OrderEventAny::Filled(fill) = event
                        && !account::is_synthetic_order(&order)
                    {
                        sync.latest_fill = sync.latest_fill.max(fill.ts_event);
                        fill_evidence
                            .lock()
                            .expect("fill evidence poisoned")
                            .applied(fill.trade_id);
                    }
                    let order = Arc::new(order);
                    if order.is_closed() {
                        pending_cancels
                            .lock()
                            .expect("pending cancels poisoned")
                            .remove(&order.client_order_id());
                    }
                    account_orders.insert(order.client_order_id(), Arc::clone(&order));
                    let registered = pending_submissions
                        .lock()
                        .expect("submission registry poisoned")
                        .entries
                        .get(&order.client_order_id())
                        .copied()
                        .flatten();
                    if order.is_closed() {
                        pending_submissions
                            .lock()
                            .expect("submission registry poisoned")
                            .entries
                            .remove(&order.client_order_id());
                    }
                    if let Some(id) = order.venue_order_id().or(registered)
                        && id.as_str().parse::<B256>().is_ok()
                    {
                        orders.insert(id, Arc::clone(&order));
                    }
                    let deferred = if order.is_closed() {
                        deferred_cancels
                            .lock()
                            .expect("deferred cancels poisoned")
                            .remove(&order.client_order_id());
                        None
                    } else if order.ts_accepted().is_some() && order.venue_order_id().is_some() {
                        deferred_cancels
                            .lock()
                            .expect("deferred cancels poisoned")
                            .remove(&order.client_order_id())
                    } else {
                        None
                    };
                    drop(sync);
                    drop(cache);
                    account_sync.notify.notify_one();
                    if let Some(command) = deferred {
                        // Reuse normal command admission, REST identity proof and callbacks.
                        // Queue after native state application; never re-enter the engine here.
                        msgbus::send_trading_command(
                            nautilus_common::msgbus::MessagingSwitchboard::exec_engine_queue_execute(),
                            nautilus_common::messages::execution::TradingCommand::CancelOrder(command),
                        );
                    }
                }
            });
            msgbus::subscribe_order_events("events.order.*".into(), handler.clone(), Some(10));
            self.order_observer = Some(handler);
        }
        Ok(())
    }

    /// Release terminal worker snapshots only after Native has purged their owners.
    /// Runs at core-thread report and instrument boundaries, without a separate timer.
    /// Atomic evidence retires only with its fully applied terminal Native owner.
    pub(super) fn prune_order_lookups(&self) {
        let cache = self.core.cache();
        let account_orders = self.account_orders.load();
        let orders = self.orders.load();
        let mut sync = self
            .account_sync
            .state
            .lock()
            .expect("account sync poisoned");
        let mut evidence = self.fill_evidence.lock().expect("fill evidence poisoned");
        let candidates = account_orders
            .values()
            .chain(orders.values())
            .filter(|order| order.is_closed() && cache.order(&order.client_order_id()).is_none())
            .collect::<Vec<_>>();
        let applied = candidates
            .iter()
            .flat_map(|order| order.trade_ids().into_iter().copied())
            .collect();
        let unapplied = if candidates.is_empty() {
            HashSet::new()
        } else {
            evidence.unapplied_orders(&applied)
        };
        let mut retired = HashSet::new();
        let mut retired_hashes = HashSet::new();
        for order in candidates {
            if order
                .venue_order_id()
                .is_some_and(|id| unapplied.contains(&id))
            {
                // Native cannot have applied these queued facts to this snapshot yet.
                // Keep their prefix and stop admission rather than discarding settlement.
                self.reconciliation_required.store(true, Ordering::Release);
                continue;
            }
            retired.insert(order.client_order_id());
            if let Some(hash) = order.venue_order_id() {
                retired_hashes.insert(hash);
            }
            sync.latest_fill = sync.latest_fill.max(account::latest_venue_fill(order));
        }
        let retained_instruments = account_orders
            .values()
            .chain(orders.values())
            .filter(|order| !retired.contains(&order.client_order_id()))
            .map(|order| order.instrument_id())
            .chain(
                cache
                    .positions_open(
                        Some(&self.core.venue),
                        None,
                        None,
                        Some(&self.core.account_id),
                        None,
                    )
                    .into_iter()
                    .map(|position| position.instrument_id),
            )
            .collect::<HashSet<_>>();
        let stale_instruments = self
            .instruments
            .load()
            .keys()
            .filter(|id| cache.instrument(id).is_none() && !retained_instruments.contains(id))
            .copied()
            .collect::<HashSet<_>>();
        if retired.is_empty() && stale_instruments.is_empty() {
            return;
        }
        if !retired_hashes.is_empty() {
            evidence.retire_orders(&retired_hashes);
        }
        self.instruments
            .rcu(|map| map.retain(|id, _| !stale_instruments.contains(id)));
        self.account_orders
            .rcu(|map| map.retain(|id, _| !retired.contains(id)));
        self.orders
            .rcu(|map| map.retain(|_, order| !retired.contains(&order.client_order_id())));
        self.rest_order_ids
            .rcu(|map| map.retain(|hash, _| !retired_hashes.contains(hash)));
        // Invalidate account collection/readiness started with the previous snapshot.
        sync.revision = sync.revision.wrapping_add(1);
        drop(evidence);
        drop(sync);
        self.account_sync.notify.notify_one();
    }

    pub(super) fn clear_observer(&mut self) {
        self.deferred_cancels
            .lock()
            .expect("deferred cancels poisoned")
            .clear();
        if let Some(handler) = self.order_observer.take() {
            msgbus::unsubscribe_order_events("events.order.*".into(), &handler);
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
        let mut ws = PredictWalletWebSocketClient::connect(
            self.config.websocket.clone(),
            &self.config.session,
            self.tasks.cancellation_token(),
        )
        .await?;
        ws.wait_until_subscribed().await?;
        self.wallet_auth = Some(ws.authentication_tracker());
        self.wallet_epoch = Some(ws.connection_epoch_handle());
        self.verified_epoch.store(0, Ordering::Release);
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

#[cfg(test)]
#[path = "../../tests/unit/lookup.rs"]
mod tests;
