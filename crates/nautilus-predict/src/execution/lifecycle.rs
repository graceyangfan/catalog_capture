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

use crate::execution::{context, metadata, wallet};
use crate::{
    config::PredictAccountStateMode,
    execution::{PredictExecutionClient, account},
    websocket::PredictWalletWebSocketClient,
};

impl PredictExecutionClient {
    pub(super) fn start_impl(&mut self) -> anyhow::Result<()> {
        let sender = nautilus_common::live::runner::try_get_exec_event_sender()
            .ok_or_else(|| anyhow::anyhow!("Nautilus execution event sender is not installed"))?;
        self.emitter.set_sender(sender);
        self.data_sender = nautilus_common::live::runner::try_get_data_event_sender();
        self.core.set_started();
        Ok(())
    }
    pub(super) fn stop_impl(&mut self) -> anyhow::Result<()> {
        if self.core.is_connected()
            && !self
                .account_orders
                .lock()
                .expect("order snapshots poisoned")
                .is_empty()
        {
            self.account_refresh_required.store(true, Ordering::Release);
        }
        self.healthy.store(false, Ordering::Release);
        self.core.set_disconnected();
        self.core.set_stopped();
        self.tasks.begin_shutdown();
        self.stop_maintenance_timer();
        {
            let mut sync = self
                .account_sync
                .state
                .lock()
                .expect("account sync poisoned");
            sync.resource_revision = sync.resource_revision.wrapping_add(1);
        }
        self.clear_observer();
        Ok(())
    }
    pub(super) fn reset_impl(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.pending_submissions
                .lock()
                .expect("submission registry poisoned")
                .entries
                .is_empty(),
            "Predict submitted orders must be reconciled before reset"
        );
        anyhow::ensure!(
            self.pending_cancels
                .lock()
                .expect("pending cancels poisoned")
                .is_empty(),
            "Predict cancellations must be reconciled before reset"
        );
        anyhow::ensure!(
            self.tasks.is_empty(),
            "Predict tasks must be drained before reset"
        );
        self.stop()?;
        self.metadata.instruments.store(Default::default());
        self.metadata.reset_nonces();
        self.cleanup_cursor.set((0, 0));
        self.orders
            .lock()
            .expect("order snapshots poisoned")
            .clear();
        self.rest_order_ids
            .lock()
            .expect("REST order IDs poisoned")
            .clear();
        self.fill_evidence = Default::default();
        self.account_refresh_required
            .store(false, Ordering::Release);
        self.account_orders
            .lock()
            .expect("order snapshots poisoned")
            .clear();
        {
            let mut sync = self
                .account_sync
                .state
                .lock()
                .expect("account sync poisoned");
            sync.active_orders.clear();
            sync.clear_connection();
        }
        Ok(())
    }

    pub(super) fn prepare_lookup(&mut self) -> anyhow::Result<()> {
        self.cleanup_cursor.set((0, 0));
        let cache = self.core.cache();
        let mut definitions = ahash::AHashMap::new();
        for instrument in cache.instruments(&self.core.venue, None) {
            metadata::upsert_instrument(&mut definitions, instrument.clone());
        }
        self.metadata.instruments.store(definitions);
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
                    Arc::new(context::WorkerOrder::capture(&order)),
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
                            && !context::OrderView::is_synthetic(&*order)
                        {
                            sync.latest_fill = sync.latest_fill.max(fill.ts_event);
                            sync.request_cash();
                            fill_evidence
                                .lock()
                                .expect("fill evidence poisoned")
                                .applied(fill.trade_id);
                        }
                        let order = Arc::new(context::WorkerOrder::observe(
                            &order,
                            event,
                            previous.as_deref(),
                        ));
                        if order.is_closed() {
                            pending_submissions
                                .lock()
                                .expect("order requests poisoned")
                                .order_status_requests
                                .remove(&order.client_order_id());
                            pending_cancels
                                .lock()
                                .expect("pending cancels poisoned")
                                .remove(&order.client_order_id());
                        }
                        account_orders
                            .lock()
                            .expect("order snapshots poisoned")
                            .insert(order.client_order_id(), Arc::clone(&order));
                        if (matches!(event, OrderEventAny::Filled(_))
                            || matches!(event, OrderEventAny::Updated(update) if update.reconciliation))
                            && order.status() == nautilus_model::enums::OrderStatus::Filled
                            && !context::OrderView::is_synthetic(&order)
                            && let Some(id) = order.venue_order_id().or(registered)
                        {
                            // Confirmed fills or a terminal reconciliation retire a stale OPEN reservation.
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
        self.metadata.notify.notify_one();
        Ok(())
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
            instruments: Arc::clone(&self.metadata.instruments),
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
            Duration::from_secs(self.config.request_timeout_secs),
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
        self.refreshed_epoch.store(0, Ordering::Release);
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
        // Explicit connection setup starts fresh account/signing observations.
        // Automatic WS reconnect keeps this client generation and its valid nonces.
        self.account_sync
            .state
            .lock()
            .expect("account sync poisoned")
            .clear_connection();
        self.metadata.reset_nonces();
        self.prepare_lookup()?;
        self.start_maintenance_timer()?;
        // Signing inputs are optional readiness, not execution connectivity.
        // Prepare signing inputs independently of account setup and wallet delivery.
        if self.config.submission_signer.is_some() {
            let epoch = self.wallet_epoch.as_ref().expect("wallet connected");
            self.tasks.spawn_named(
                "predict-market-metadata",
                Arc::clone(&self.metadata).run(
                    self.config.clone(),
                    self.chain.clone(),
                    self.clock,
                    Arc::clone(epoch),
                    self.wallet_auth.clone().expect("wallet connected"),
                    self.tasks.cancellation_token(),
                ),
            )?;
        }
        if self.config.account_state_mode == PredictAccountStateMode::Reported {
            let reporter = self.account_reporter()?;
            let reported = reporter.refresh().await?;
            anyhow::ensure!(
                reported,
                "Predict native account context changed during initialization"
            );
            self.await_account_registered().await?;
            self.tasks.spawn_named(
                "predict-account-reports",
                account::run(
                    reporter,
                    self.tasks.cancellation_token(),
                    Arc::clone(&self.account_query_active),
                    Arc::clone(&self.refreshed_epoch),
                    Arc::clone(&self.account_refresh_required),
                ),
            )?;
        }
        if self.config.submission_signer.is_some() {
            let epoch = self.wallet_epoch.as_ref().expect("wallet connected");
            metadata::prepare(&self.config, &self.metadata.instruments, self.clock, epoch).await;
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
            wallet::run_wallet(
                ws,
                self.tasks.cancellation_token(),
                Arc::clone(&self.healthy),
                wallet::WalletReportContext {
                    instruments: Arc::clone(&self.metadata.instruments),
                    orders: Arc::clone(&self.orders),
                    rest_ids: Arc::clone(&self.rest_order_ids),
                    sync: Arc::clone(&self.account_sync),
                    pending: Arc::clone(&self.pending_submissions),
                    fill_evidence: Arc::clone(&self.fill_evidence),
                    metadata_notify: Arc::clone(&self.metadata.notify),
                    account_refresh_required: Arc::clone(&self.account_refresh_required),
                    allow_share_fees: self.config.account_state_mode
                        == PredictAccountStateMode::Reported,
                    emitter: self.emitter.clone(),
                    account: self.config.account_id,
                    wallet: self.config.session.account_address(),
                    clock: self.clock,
                    latency: self.config.latency.clone(),
                    data_sender: self.data_sender.clone(),
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
        self.wallet_auth = None;
        self.wallet_epoch = None;
        Ok(())
    }
}

fn renewal_delay(expiry: u64, now: u64) -> Duration {
    // Like Deribit, renew after 80% of the remaining token lifetime.
    Duration::from_secs(expiry.saturating_sub(now).saturating_mul(4) / 5)
}

impl PredictExecutionClient {
    fn start_authentication_refresh(&self) -> anyhow::Result<()> {
        let Some(signer) = self.config.auth_signer.clone() else {
            return Ok(());
        };
        let session = self.config.session.clone();
        if session.token().expires_at()?.is_none() {
            log::warn!(
                "Predict external token has no expiry; automatic authentication renewal unavailable"
            );
            return Ok(());
        }
        let cancellation = self.tasks.cancellation_token();
        let clock = self.clock;
        self.tasks.spawn_named("predict-authentication", async move {
            loop {
                let now = clock.get_time_ns().as_u64() / 1_000_000_000;
                let expiry = match session.token().expires_at() {
                    Ok(Some(value)) => value,
                    _ => break,
                };
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => break,
                    _ = tokio::time::sleep(renewal_delay(expiry, now)) => {},
                }
                // Keep the old acknowledged wallet active while HTTP renewal is pending.
                // Publishing a replacement token makes the WS handler require a fresh ACK.
                let mut retry = 1;
                loop {
                    let result = tokio::select! {
                        biased;
                        () = cancellation.cancelled() => return,
                        result = session.renew(&signer) => result,
                    };
                    match result {
                        Ok(()) => {
                            // The wallet token watcher requests Native connection replacement and ACKs.
                            break;
                        }
                        Err(_) => {
                            log::warn!("Predict authentication renewal failed; no trading operation retried");
                        }
                    }
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => return,
                        _ = tokio::time::sleep(Duration::from_secs(retry)) => {},
                    }
                    retry = (retry * 2).min(30);
                }
            }
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod authentication_tests {
    use super::*;
    #[test]
    fn renewal_is_before_expiration_and_expired_tokens_refresh_immediately() {
        assert_eq!(renewal_delay(86400, 0), Duration::from_secs(69120));
        assert_eq!(renewal_delay(101, 100), Duration::ZERO);
        assert_eq!(renewal_delay(90, 100), Duration::ZERO);
    }
}
