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

//! Native command admission and event dispatch; ExecutionEngine remains the only OMS.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use alloy::primitives::U256;
use nautilus_common::{
    clients::ExecutionClient,
    factories::OrderEventFactory,
    messages::execution::{ModifyOrder, SubmitOrder},
};
use nautilus_live::execution::failure::CommandFailure;
use nautilus_model::{
    accounts::Account,
    identifiers::{ClientOrderId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::Order,
    types::Currency,
};
use rust_decimal::Decimal;

use super::{PredictExecutionClient, order_builder::terms, submitter::SubmitWorker};
use crate::config::PredictAccountStateMode;

#[derive(Debug, Default)]
pub(super) struct SubmissionRegistry {
    pub entries: HashMap<ClientOrderId, Option<VenueOrderId>>,
    /// Worst-case collateral of every write attempted by this client lifetime, including ambiguous writes.
    pub attempted_usdt: Decimal,
}
pub(super) type PendingSubmissions = Arc<Mutex<SubmissionRegistry>>;

pub(super) struct SubmissionGuard {
    pub registry: PendingSubmissions,
    pub id: ClientOrderId,
    pub retain: bool,
    pub transmitted: bool,
}
impl Drop for SubmissionGuard {
    fn drop(&mut self) {
        if !self.retain {
            self.registry
                .lock()
                .expect("submission registry poisoned")
                .entries
                .remove(&self.id);
        }
    }
}

impl PredictExecutionClient {
    /// Predict's public API and official SDK expose no atomic amend operation.
    /// No modification request is sent to the venue. Publish a native rejection so callers
    /// can resolve PendingUpdate before explicitly cancelling and replacing an order.
    pub(super) fn modify_order_command(&self, cmd: &ModifyOrder) -> anyhow::Result<()> {
        match self.try_modify_order_command(cmd) {
            Ok(()) => Ok(()),
            Err(error) => {
                let cache = self.core.cache();
                let Some(order) = cache.order_owned(&cmd.client_order_id) else {
                    return Err(error);
                };
                if cmd.trader_id != self.core.trader_id
                    || order.trader_id() != self.core.trader_id
                    || cmd.strategy_id != order.strategy_id()
                    || cmd.instrument_id != order.instrument_id()
                    || cmd.client_id.is_some_and(|id| id != self.core.client_id)
                    || cmd
                        .venue_order_id
                        .is_some_and(|id| Some(id) != order.venue_order_id())
                    || order.account_id() != Some(self.core.account_id)
                    || cache
                        .client_id(&cmd.client_order_id)
                        .is_some_and(|id| *id != self.core.client_id)
                    || order.is_closed()
                {
                    return Err(error);
                }
                drop(cache);
                let now = self.clock.get_time_ns();
                let factory = OrderEventFactory::new(
                    self.core.trader_id,
                    self.core.account_id,
                    self.core.account_type,
                    self.core.base_currency,
                );
                self.emitter
                    .try_send_order_event(factory.generate_order_modify_rejected(
                        &order,
                        order.venue_order_id(),
                        &error.to_string(),
                        now,
                        now,
                    ))?;
                Ok(())
            }
        }
    }

    fn try_modify_order_command(&self, cmd: &ModifyOrder) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.is_connected(),
            "Predict execution session is not connected"
        );
        let order = self
            .core
            .cache()
            .order_owned(&cmd.client_order_id)
            .ok_or_else(|| anyhow::anyhow!("Predict modify requires a cached native order"))?;
        anyhow::ensure!(
            cmd.trader_id == self.core.trader_id
                && order.trader_id() == self.core.trader_id
                && cmd.strategy_id == order.strategy_id()
                && cmd.instrument_id == order.instrument_id()
                && cmd.client_id.is_none_or(|id| id == self.core.client_id)
                && cmd
                    .venue_order_id
                    .is_none_or(|id| Some(id) == order.venue_order_id())
                && order.account_id() == Some(self.core.account_id)
                && self
                    .core
                    .cache()
                    .client_id(&cmd.client_order_id)
                    .is_none_or(|id| *id == self.core.client_id),
            "Predict modify identity mismatch"
        );
        let factory = OrderEventFactory::new(
            self.core.trader_id,
            self.core.account_id,
            self.core.account_type,
            self.core.base_currency,
        );
        let now = self.clock.get_time_ns();
        let event = factory.generate_order_modify_rejected(
            &order,
            order.venue_order_id(),
            "Predict atomic order modification is unsupported; cancel then replace explicitly",
            now,
            now,
        );
        self.emitter.try_send_order_event(event)?;
        Ok(())
    }

    pub(super) fn submit_order_command(&self, cmd: &SubmitOrder) -> anyhow::Result<()> {
        self.prune_order_lookups();
        anyhow::ensure!(
            !self
                .reconciliation_required
                .load(std::sync::atomic::Ordering::Acquire),
            "Predict stream gap requires reconciliation before new submissions"
        );
        let policy = self
            .config
            .submission
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Predict order submission is not enabled"))?
            .clone();
        anyhow::ensure!(
            self.config.account_state_mode == PredictAccountStateMode::Reported
                && self.is_connected(),
            "Predict reported execution session is not connected"
        );
        let cache = self.core.cache();
        let order = cache
            .order_owned(&cmd.client_order_id)
            .ok_or_else(|| anyhow::anyhow!("Predict submit requires a cached native order"))?;
        anyhow::ensure!(
            cmd.trader_id == self.core.trader_id
                && order.trader_id() == self.core.trader_id
                && cmd.strategy_id == order.strategy_id()
                && cmd.instrument_id == order.instrument_id()
                && cmd.client_id.is_none_or(|id| id == self.core.client_id)
                && order
                    .account_id()
                    .is_none_or(|id| id == self.core.account_id),
            "Predict submit identity mismatch"
        );
        let Some(InstrumentAny::BinaryOption(instrument)) =
            cache.instrument(&order.instrument_id())
        else {
            anyhow::bail!("Predict native binary instrument is missing");
        };
        anyhow::ensure!(
            order.status() == nautilus_model::enums::OrderStatus::Initialized,
            "Predict cannot resubmit an active or terminal native order"
        );
        let instrument = instrument.clone();
        let validated = terms(
            &order,
            &instrument,
            &policy,
            self.clock.get_time_ns().as_u64(),
        );
        let (cost, price, shares, expiry) = match validated {
            Ok(terms) => terms,
            Err(reason) => {
                let factory = OrderEventFactory::new(
                    self.core.trader_id,
                    self.core.account_id,
                    self.core.account_type,
                    self.core.base_currency,
                );
                self.emitter
                    .try_send_order_event(factory.generate_order_denied(
                        &order,
                        &reason.to_string(),
                        self.clock.get_time_ns(),
                    ))?;
                return Ok(());
            }
        };
        let account = cache
            .account_owned(&self.core.account_id)
            .ok_or_else(|| anyhow::anyhow!("Predict cash account is not registered"))?;
        let free = account
            .balance_free(Some(Currency::USDT()))
            .ok_or_else(|| anyhow::anyhow!("Predict free collateral is unavailable"))?
            .as_decimal();
        anyhow::ensure!(
            order.order_side() == nautilus_model::enums::OrderSide::Sell || cost <= free,
            "Predict native account has insufficient free collateral"
        );
        drop(cache);
        let mut registry = self
            .pending_submissions
            .lock()
            .expect("submission registry poisoned");
        anyhow::ensure!(
            !registry.entries.contains_key(&cmd.client_order_id),
            "Predict submission for this client order is already active or unresolved"
        );
        anyhow::ensure!(
            order.order_side() == nautilus_model::enums::OrderSide::Sell
                || registry
                    .attempted_usdt
                    .checked_add(cost)
                    .is_some_and(|v| v <= policy.max_total_usdt),
            "Predict client lifetime collateral cap is exhausted"
        );
        registry.entries.insert(cmd.client_order_id, None);
        drop(registry);
        let mut guard = SubmissionGuard {
            registry: Arc::clone(&self.pending_submissions),
            id: cmd.client_order_id,
            retain: false,
            transmitted: false,
        };
        let factory = OrderEventFactory::new(
            self.core.trader_id,
            self.core.account_id,
            self.core.account_type,
            self.core.base_currency,
        );
        self.emitter.try_send_order_event(
            factory.generate_order_submitted(&order, self.clock.get_time_ns()),
        )?;
        guard.retain = true; // A queued native Submitted event must retain ownership through shutdown.
        self.instruments.insert(
            instrument.id(),
            InstrumentAny::BinaryOption(instrument.clone()),
        );
        let worker = SubmitWorker {
            signer: Arc::clone(
                self.order_signer
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Predict order signer is not configured"))?,
            ),
            session: self.config.session.clone(),
            chain: self
                .chain
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Predict admission RPC is missing"))?,
            policy,
            order: Arc::new(order),
            instrument,
            price,
            shares,
            expiry,
            cost,
            free,
            // Match the official SDK's default salt range while retaining the complete
            // native command UUID for ownership and duplicate-command protection.
            salt: U256::from(u128::from_be_bytes(cmd.command_id.as_bytes()) % 2_147_483_648),
            pending: Arc::clone(&self.pending_submissions),
            admission: Arc::clone(&self.submission_admission),
            orders: Arc::clone(&self.orders),
            rest_ids: Arc::clone(&self.rest_order_ids),
            reconciliation_required: Arc::clone(&self.reconciliation_required),
            account_sync: Arc::clone(&self.account_sync),
            instruments: Arc::clone(&self.instruments),
            healthy: Arc::clone(&self.healthy),
            authentication: self
                .wallet_auth
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Predict wallet auth is missing"))?,
            admitted_epoch: self
                .verified_epoch
                .load(std::sync::atomic::Ordering::Acquire),
            epoch: self
                .wallet_epoch
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Predict wallet epoch is missing"))?,
            emitter: self.emitter.clone(),
            factory,
            clock: self.clock,
            config: self.config.clone(),
        };
        let cancellation = self.tasks.cancellation_token();
        let deadline = Duration::from_secs(self.config.report_timeout_secs);
        self.tasks.spawn_named("predict-submit-order", async move {
            let mut guard = guard;
            let result = tokio::select! { biased;
                _ = cancellation.cancelled() => return,
                result = tokio::time::timeout(deadline, worker.run(&mut guard)) => result,
            };
            if !worker.available() {
                return;
            }
            match result {
                Ok(Ok(())) => {}
                Ok(Err(
                    CommandFailure::NotSent(reason) | CommandFailure::VenueRejected(reason),
                )) => {
                    // Native state published before an HTTP reply takes precedence.
                    let terminal = worker.orders.load().values().any(|o| {
                        o.client_order_id() == worker.order.client_order_id()
                            && (o.is_closed() || o.ts_accepted().is_some())
                    });
                    if !terminal {
                        let event = worker.factory.generate_order_rejected(
                            &worker.order,
                            &reason,
                            worker.clock.get_time_ns(),
                            worker.clock.get_time_ns(),
                            false,
                        );
                        let _ = worker.emitter.try_send_order_event(event);
                    }
                }
                Ok(Err(CommandFailure::Ambiguous(reason))) if guard.transmitted => {
                    // `reason` comes only from sanitized typed private HTTP errors. Preserve
                    // status/decode classification without logging response bodies or signatures.
                    log::warn!(
                        "Predict submission outcome is unresolved; client_order_id={} reason={reason}; do not resubmit",
                        worker.order.client_order_id(),
                    );
                }
                Err(_) if guard.transmitted => {
                    log::warn!(
                        "Predict submission outcome is unresolved; client_order_id={} reason=command deadline elapsed after transmission; do not resubmit",
                        worker.order.client_order_id(),
                    );
                }
                _ => {
                    let event = worker.factory.generate_order_rejected(
                        &worker.order,
                        "Predict preflight did not complete; order was not transmitted",
                        worker.clock.get_time_ns(),
                        worker.clock.get_time_ns(),
                        false,
                    );
                    let _ = worker.emitter.try_send_order_event(event);
                }
            }
        })?;
        Ok(())
    }
}
