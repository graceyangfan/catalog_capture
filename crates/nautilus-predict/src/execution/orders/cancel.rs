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

//! Verified REST removal for native single/batch commands; ExecutionEngine owns transitions.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use alloy::primitives::{B256, U256};
use futures_util::StreamExt;
use nautilus_common::{
    factories::OrderEventFactory,
    messages::execution::{BatchCancelOrders, CancelAllOrders, CancelOrder},
};
use nautilus_core::time::AtomicTime;
use nautilus_live::{ExecutionEventEmitter, execution::failure::CommandFailure};
use nautilus_model::{
    identifiers::{ClientOrderId, VenueOrderId},
    instruments::{BinaryOption, InstrumentAny},
    orders::Order,
};
use tokio_util::sync::CancellationToken;

use crate::execution::{account, context, orders};
use crate::{
    config::PredictAccountStateMode,
    execution::{
        PredictExecutionClient, orders::classify_http_command_failure,
        parse::validate_rest_order_terms,
    },
    http::{
        PredictAuthenticatedHttpClient, account_models::PredictOrderStatus,
        command_models::PredictRemoveOrdersReceipt,
    },
};

// Tracks outstanding commands only, never order status, balances or position state. Ambiguous
// writes retain ownership until a terminal native event resolves the order.
pub(in crate::execution) type PendingCancels = Arc<Mutex<HashSet<ClientOrderId>>>;
// Only unsent command intent. Native Cache remains authoritative for order state.
pub(in crate::execution) type DeferredCancels = Arc<Mutex<HashMap<ClientOrderId, CancelOrder>>>;

struct CancelGuard {
    pending: PendingCancels,
    id: ClientOrderId,
    retain: bool,
}
impl Drop for CancelGuard {
    fn drop(&mut self) {
        if !self.retain {
            self.pending
                .lock()
                .expect("pending cancels poisoned")
                .remove(&self.id);
        }
    }
}

impl PredictExecutionClient {
    /// Uses the official multi-ID removal endpoint after independent per-order preflight.
    /// Each removed/noop result is resolved separately; batch success is not atomic.
    pub(in crate::execution) fn batch_cancel_orders_command(
        &self,
        cmd: &BatchCancelOrders,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            cmd.trader_id == self.core.trader_id
                && cmd.client_id.is_none_or(|id| id == self.core.client_id),
            "Predict batch cancel identity differs from execution client"
        );
        // Validate the envelope before any child can write. Duplicate IDs are coalesced.
        anyhow::ensure!(
            cmd.cancels
                .iter()
                .all(|cancel| cancel.trader_id == cmd.trader_id
                    && cancel.client_id.is_none_or(|id| id == self.core.client_id)
                    && cancel.strategy_id == cmd.strategy_id
                    && cancel.instrument_id == cmd.instrument_id),
            "Predict batch cancel child differs from command scope"
        );
        self.dispatch_cancels(&cmd.cancels)
    }

    fn dispatch_cancels(&self, cancels: &[CancelOrder]) -> anyhow::Result<()> {
        let mut seen = HashSet::new();
        let mut errors = Vec::new();
        let mut entries = Vec::new();
        for cancel in cancels {
            if !seen.insert(cancel.client_order_id) {
                continue;
            }
            match self.prepare_cancel_order_command(cancel) {
                Ok(Some(entry)) => entries.push(entry),
                Ok(None) => {} // Deferred or definitively rejected through the standard path.
                Err(error) => errors.push(format!("{}: {error}", cancel.client_order_id)),
            }
        }
        if !entries.is_empty() {
            let cancellation = self.tasks.cancellation_token();
            self.tasks
                .spawn_named("predict-batch-cancel-orders", async move {
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => {},
                        () = execute_batch(entries) => {},
                    }
                })?;
        }
        anyhow::ensure!(
            errors.is_empty(),
            "Predict batch cancel dispatch failures: {}",
            errors.join("; ")
        );
        Ok(())
    }

    pub(in crate::execution) fn cancel_all_orders_command(
        &self,
        cmd: &CancelAllOrders,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            cmd.trader_id == self.core.trader_id
                && cmd.client_id.is_none_or(|id| id == self.core.client_id),
            "Predict cancel-all identity differs from execution client"
        );
        // Strategy expands strategy_only into explicit commands; CancelAll spans strategies.
        let cancels = {
            let cache = self.core.cache();
            let mut seen = HashSet::new();
            cache
                .orders_open(
                    Some(&self.core.venue),
                    Some(&cmd.instrument_id),
                    None,
                    None,
                    cmd.order_side,
                )
                .into_iter()
                .chain(cache.orders_inflight(
                    Some(&self.core.venue),
                    Some(&cmd.instrument_id),
                    None,
                    None,
                    cmd.order_side,
                ))
                .filter(|order| {
                    order.trader_id() == self.core.trader_id
                        && order
                            .account_id()
                            .is_none_or(|id| id == self.core.account_id)
                        && cache
                            .client_id(&order.client_order_id())
                            .is_none_or(|id| *id == self.core.client_id)
                })
                .filter(|order| seen.insert(order.client_order_id()))
                .map(|order| {
                    let mut cancel = CancelOrder::new(
                        cmd.trader_id,
                        Some(self.core.client_id),
                        order.strategy_id(),
                        order.instrument_id(),
                        order.client_order_id(),
                        order.venue_order_id(),
                        cmd.command_id,
                        cmd.ts_init,
                        cmd.params.clone(),
                        cmd.correlation_id,
                    );
                    cancel.causation_id = cmd.causation_id;
                    cancel
                })
                .collect::<Vec<_>>()
        };
        self.dispatch_cancels(&cancels)
    }

    pub(in crate::execution) fn cancel_order_command(
        &self,
        cmd: &CancelOrder,
    ) -> anyhow::Result<()> {
        let Some((worker, guard)) = self.prepare_cancel_order_command(cmd)? else {
            return Ok(());
        };
        self.tasks.spawn_named(
            "predict-cancel-order",
            worker.run(guard, self.tasks.cancellation_token()),
        )?;
        Ok(())
    }

    fn prepare_cancel_order_command(
        &self,
        cmd: &CancelOrder,
    ) -> anyhow::Result<Option<(CancelWorker, CancelGuard)>> {
        match self.try_prepare_cancel_order(cmd) {
            Ok(entry) => Ok(entry),
            Err(error) => {
                let cache = self.core.cache();
                let Some(order) = cache.order_owned(&cmd.client_order_id) else {
                    return Err(error);
                };
                // Never publish an event into another command owner's order. An unresolved
                // earlier cancellation also cannot be rejected by a duplicate command.
                if cmd.trader_id != self.core.trader_id
                    || cmd.trader_id != order.trader_id()
                    || cmd.client_id.is_some_and(|id| id != self.core.client_id)
                    || cmd.strategy_id != order.strategy_id()
                    || cmd.instrument_id != order.instrument_id()
                    || order
                        .account_id()
                        .is_some_and(|id| id != self.core.account_id)
                    || cache
                        .client_id(&cmd.client_order_id)
                        .is_some_and(|id| *id != self.core.client_id)
                    || cmd
                        .venue_order_id
                        .is_some_and(|id| Some(id) != order.venue_order_id())
                    || order.is_closed()
                    || self
                        .pending_cancels
                        .lock()
                        .expect("pending cancels poisoned")
                        .contains(&cmd.client_order_id)
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
                    .try_send_order_event(factory.generate_order_cancel_rejected(
                        &order,
                        order.venue_order_id(),
                        &error.to_string(),
                        now,
                        now,
                    ))?;
                Ok(None)
            }
        }
    }

    fn try_prepare_cancel_order(
        &self,
        cmd: &CancelOrder,
    ) -> anyhow::Result<Option<(CancelWorker, CancelGuard)>> {
        anyhow::ensure!(
            self.config.orderbook_removal_enabled,
            "Predict REST orderbook removal is not enabled"
        );
        anyhow::ensure!(
            self.config.account_state_mode == PredictAccountStateMode::Reported
                && self.core.is_connected()
                && !self.tasks.cancellation_token().is_cancelled(),
            "Predict reported execution session is not connected"
        );
        let cache = self.core.cache();
        let order = cache
            .order(&cmd.client_order_id)
            .ok_or_else(|| anyhow::anyhow!("Predict cancel requires a cached native order"))?;
        anyhow::ensure!(
            cmd.trader_id == self.core.trader_id
                && cmd.trader_id == order.trader_id()
                && cmd.client_id.is_none_or(|id| id == self.core.client_id)
                && cmd.strategy_id == order.strategy_id()
                && cmd.instrument_id == order.instrument_id()
                && order
                    .account_id()
                    .is_none_or(|id| id == self.core.account_id)
                && cache
                    .client_id(&cmd.client_order_id)
                    .is_none_or(|id| *id == self.core.client_id)
                && cmd
                    .venue_order_id
                    .is_none_or(|id| Some(id) == order.venue_order_id()),
            "Predict cancel identity differs from the cached order"
        );
        anyhow::ensure!(
            !order.is_closed(),
            "Predict cancel requires an active order"
        );
        if order.ts_accepted().is_none() {
            anyhow::ensure!(
                self.pending_submissions
                    .lock()
                    .expect("submission registry poisoned")
                    .entries
                    .contains_key(&cmd.client_order_id),
                "Predict deferred cancel requires a locally owned pending submission"
            );
            // Like Polymarket, wait for venue identity without another query/retry loop.
            // The Native event observer releases this command or drops it at terminal.
            self.deferred_cancels
                .lock()
                .expect("deferred cancels poisoned")
                .entry(cmd.client_order_id)
                .or_insert_with(|| cmd.clone());
            return Ok(None);
        }
        let venue_id = order
            .venue_order_id()
            .ok_or_else(|| anyhow::anyhow!("Predict cancel requires an accepted exchange hash"))?;
        let hash = venue_id
            .as_str()
            .parse::<B256>()
            .map_err(|_| anyhow::anyhow!("Predict cancel venue ID is not an exchange hash"))?;
        let Some(InstrumentAny::BinaryOption(instrument)) =
            cache.instrument(&order.instrument_id())
        else {
            anyhow::bail!("Predict cancel instrument is missing from native cache");
        };
        let instrument = instrument.clone();
        drop(order);
        drop(cache);
        anyhow::ensure!(
            self.pending_cancels
                .lock()
                .expect("pending cancels poisoned")
                .insert(cmd.client_order_id),
            "Predict cancellation is already in flight or unresolved"
        );
        let guard = CancelGuard {
            pending: Arc::clone(&self.pending_cancels),
            id: cmd.client_order_id,
            retain: false,
        };
        let worker = CancelWorker {
            session: self.config.session.clone(),
            orders: Arc::clone(&self.orders),
            client_order_id: cmd.client_order_id,
            instrument,
            venue_id,
            hash,
            rest_ids: Arc::clone(&self.rest_order_ids),
            account_sync: Arc::clone(&self.account_sync),
            healthy: Arc::clone(&self.healthy),
            factory: OrderEventFactory::new(
                self.core.trader_id,
                self.core.account_id,
                self.core.account_type,
                self.core.base_currency,
            ),
            emitter: self.emitter.clone(),
            clock: self.clock,
            timeout: Duration::from_secs(self.config.request_timeout_secs),
            cancellation: self.tasks.cancellation_token(),
            latency: self.config.latency.clone(),
        };
        Ok(Some((worker, guard)))
    }
}

struct CancelWorker {
    session: PredictAuthenticatedHttpClient,
    orders: context::OrderSnapshots<VenueOrderId>,
    client_order_id: ClientOrderId,
    instrument: BinaryOption,
    venue_id: VenueOrderId,
    hash: B256,
    rest_ids: orders::RestOrderIds,
    account_sync: Arc<account::AccountSync>,
    healthy: Arc<AtomicBool>,
    factory: OrderEventFactory,
    emitter: ExecutionEventEmitter,
    clock: &'static AtomicTime,
    timeout: Duration,
    cancellation: CancellationToken,
    latency: Option<crate::latency::PredictLatency>,
}

impl CancelWorker {
    async fn run(self, mut guard: CancelGuard, cancellation: CancellationToken) {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {}
            result = tokio::time::timeout(self.timeout, self.execute(&mut guard)) => {
                self.resolve(&mut guard, result);
            }
        }
    }

    fn session_active(&self) -> bool {
        !self.cancellation.is_cancelled()
    }
    fn reject(&self, reason: &str) {
        // A proven not-sent/definitively rejected command can resolve PendingCancel
        // during a wallet gap. Only shutdown of its owning generation suppresses it.
        if !self.cancellation.is_cancelled()
            && let Some(order) = self.current_order()
            && !order.is_closed()
        {
            let now = self.clock.get_time_ns();
            let event = order.cancel_rejected(&self.factory, Some(self.venue_id), reason, now, now);
            if self.emitter.try_send_order_event(event).is_err() {
                self.healthy.store(false, Ordering::Release);
            }
        }
    }
    fn current_order(&self) -> Option<Arc<context::WorkerOrder>> {
        self.orders
            .lock()
            .expect("order snapshots poisoned")
            .get(&self.venue_id)
            .cloned()
            .filter(|order| order.client_order_id() == self.client_order_id)
    }
    fn resolve(
        &self,
        guard: &mut CancelGuard,
        result: Result<Result<(), CommandFailure>, tokio::time::error::Elapsed>,
    ) {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(CommandFailure::NotSent(reason) | CommandFailure::VenueRejected(reason))) => {
                guard.retain = false;
                self.reject(&reason);
            }
            Ok(Err(CommandFailure::Ambiguous(reason))) => {
                log::warn!(
                    "Predict cancel outcome unresolved for {}: {reason}",
                    guard.id
                );
            }
            Err(_) => self.resolve_timeout(guard),
        }
    }

    fn resolve_timeout(&self, guard: &CancelGuard) {
        if !guard.retain {
            self.reject("Predict cancel preflight timed out");
        }
        log::warn!(
            "Predict cancel timed out for {} (transmitted: {})",
            guard.id,
            guard.retain
        );
    }

    async fn preflight(&self) -> Result<Option<U256>, CommandFailure> {
        if !self.session_active() {
            return Err(CommandFailure::not_sent(
                "Predict cancellation generation stopped",
            ));
        }
        let order = self
            .current_order()
            .ok_or_else(|| CommandFailure::not_sent("Predict cancel lost native context"))?;
        if order.is_closed() {
            return Ok(None);
        }
        if let Some(id) = self
            .rest_ids
            .lock()
            .expect("REST order IDs poisoned")
            .get(&self.venue_id)
            .copied()
        {
            return Ok(Some(id));
        }
        let row = self
            .session
            .request_order(self.hash)
            .await
            .map_err(|e| CommandFailure::NotSent(e.to_string()))?;
        let order = self
            .current_order()
            .ok_or_else(|| CommandFailure::not_sent("Predict cancel lost native context"))?;
        if order.is_closed() {
            return Ok(None);
        }
        validate_rest_order_terms(
            &row,
            order.as_ref(),
            &self.instrument,
            self.factory.account_id(),
            self.session.account_address(),
        )
        .map_err(|e| CommandFailure::NotSent(e.to_string()))?;
        if !self.session_active() {
            return Err(CommandFailure::not_sent(
                "Predict cancellation generation stopped",
            ));
        }
        if matches!(
            row.status,
            PredictOrderStatus::Cancelled | PredictOrderStatus::Expired
        ) {
            let now = self.clock.get_time_ns();
            let event = if row.status == PredictOrderStatus::Cancelled {
                order.canceled(&self.factory, Some(self.venue_id), now, now)
            } else {
                order.expired(&self.factory, Some(self.venue_id), now, now)
            };
            self.emitter
                .try_send_order_event(event)
                .map_err(|e| CommandFailure::NotSent(e.to_string()))?;
            self.account_sync
                .confirm_terminal(self.venue_id, Some(self.client_order_id));
            return Ok(None);
        }
        if row.status != PredictOrderStatus::Open {
            return Err(CommandFailure::not_sent(
                "Predict cancel preflight order is not OPEN; reconciliation required",
            ));
        }
        if !self.session_active() {
            return Err(CommandFailure::not_sent(
                "Predict cancellation generation stopped",
            ));
        }
        // REST IDs are a routing detail fetched by hash. They never replace VenueOrderId in OMS.
        let id = orders::parse_id(&row.id).map_err(|_| {
            CommandFailure::not_sent("Predict cancel preflight ID is not a decimal uint256")
        })?;
        orders::register(&self.rest_ids, self.venue_id, id)
            .map_err(|e| CommandFailure::NotSent(e.to_string()))?;
        Ok(Some(id))
    }

    async fn execute(&self, guard: &mut CancelGuard) -> Result<(), CommandFailure> {
        let Some(id) = self.preflight().await? else {
            return Ok(());
        };
        guard.retain = true;
        let timing = self.latency.as_ref().map(|probe| {
            probe.count(crate::latency::LatencyCounter::RemoveCalls);
            probe.timer(crate::latency::LatencyMetric::CancelRequest)
        });
        let receipt = self.session.request_remove_orders(&[id]).await;
        if let Some(timing) = timing {
            timing.finish();
        }
        let receipt = receipt.map_err(|e| classify_http_command_failure(&e))?;
        self.complete(guard, &receipt, id)
    }

    fn complete(
        &self,
        guard: &mut CancelGuard,
        receipt: &PredictRemoveOrdersReceipt,
        id: U256,
    ) -> Result<(), CommandFailure> {
        let order = self.current_order().ok_or_else(|| {
            CommandFailure::ambiguous("Predict cancel lost native context after transmission")
        })?;
        if self.session_active() && receipt.removed.contains(&id.to_string()) {
            // A late definitive receipt also resolves an earlier Native-only
            // terminal state, without publishing a duplicate order event.
            self.account_sync
                .confirm_terminal(self.venue_id, Some(self.client_order_id));
        }
        if order.is_closed() {
            guard.retain = false;
            return Ok(());
        }
        let id = id.to_string();
        if receipt.rejected.contains(&id) {
            return Err(CommandFailure::VenueRejected(
                "Predict removal rejected during the market removal lock window".into(),
            ));
        }
        // REST receipts are valid during a WS gap. Only the owning task generation
        // and current Native order constrain publication of a definitive result.
        if !self.session_active() {
            return Err(CommandFailure::ambiguous(
                "Predict cancellation generation stopped after transmission",
            ));
        }
        if receipt.removed.contains(&id) {
            let now = self.clock.get_time_ns();
            let event = order.canceled(&self.factory, Some(self.venue_id), now, now);
            self.emitter.try_send_order_event(event).map_err(|_| {
                self.healthy.store(false, Ordering::Release);
                CommandFailure::ambiguous(
                    "Predict removal acknowledged but native event channel is closed",
                )
            })?;
            // Keep ownership until ExecutionEngine publishes its terminal event. Otherwise a
            // duplicate command could be sent while the acknowledgement is still queued.
            Ok(())
        } else {
            // A noop may mean filled, already cancelled or still unresolved.
            // Native owns the subsequent report query and its retry/deadline policy.
            Err(CommandFailure::ambiguous(
                "Predict removal noop requires Native order recovery",
            ))
        }
    }
}

/// Preparation retains individual failure boundaries; only proven, active IDs share a write.
async fn execute_batch(entries: Vec<(CancelWorker, CancelGuard)>) {
    futures_util::stream::iter(entries)
        .map(|(worker, mut guard)| async move {
            match tokio::time::timeout(worker.timeout, worker.preflight()).await {
                Ok(Ok(Some(id))) => Some((worker, guard, id)),
                Ok(Ok(None)) => None,
                result => {
                    worker.resolve(&mut guard, result.map(|r| r.map(|_| ())));
                    None
                }
            }
        })
        .buffer_unordered(100)
        .filter_map(|entry| async { entry })
        .ready_chunks(100)
        .for_each_concurrent(4, |chunk| async move {
            let mut active = Vec::new();
            for (worker, guard, id) in chunk {
                let Some(order) = worker.current_order() else {
                    worker.reject("Predict batch cancel lost native context before transmission");
                    continue;
                };
                if order.is_closed() {
                    continue;
                }
                if !worker.session_active() {
                    worker.reject(
                        "Predict batch cancellation generation stopped before transmission",
                    );
                    continue;
                }
                active.push((worker, guard, id));
            }
            if active.is_empty() {
                return;
            }
            let ids = active.iter().map(|(_, _, id)| *id).collect::<Vec<_>>();
            let session = active[0].0.session.clone();
            let timeout = active[0].0.timeout;
            for (_, guard, _) in &mut active {
                guard.retain = true;
            }
            let probe = active[0].0.latency.clone();
            let timing = probe.as_ref().map(|probe| {
                probe.count(crate::latency::LatencyCounter::RemoveCalls);
                probe.timer(crate::latency::LatencyMetric::CancelRequest)
            });
            let result = tokio::time::timeout(timeout, session.request_remove_orders(&ids)).await;
            if let Some(timing) = timing
                && result.is_ok()
            {
                timing.finish();
            }
            match result {
                Ok(Ok(receipt)) => {
                    for (worker, mut guard, id) in active {
                        let result = worker.complete(&mut guard, &receipt, id);
                        worker.resolve(&mut guard, Ok(result));
                    }
                }
                Ok(Err(error)) => {
                    for (worker, mut guard, _) in active {
                        worker.resolve(&mut guard, Ok(Err(classify_http_command_failure(&error))));
                    }
                }
                Err(_) => {
                    for (worker, guard, _) in active {
                        worker.resolve_timeout(&guard);
                    }
                }
            }
        })
        .await;
}
