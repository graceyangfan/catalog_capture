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

//! Native private reports, reported cash, bounded order commands and verified reconnect recovery.

mod account;
mod authentication;
mod cancellations;
mod external;
pub mod factory;
mod inventory_scope;
mod lifecycle;
mod matches;
mod order_builder;
mod orders;
pub mod parse;
mod precision;
mod readiness;
mod reconciliation;
mod reconnect;
mod recovery;
mod reports;
mod resources;
mod runtime;
mod submitter;
mod types;

pub use reports::reconciliation_market_ids;

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use async_trait::async_trait;
use nautilus_common::{
    clients::{ExecutionClient, ExecutionReportTask},
    live::runner::try_get_exec_event_sender,
    messages::execution::{
        BatchCancelOrders, BatchModifyOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
        GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
        ModifyOrder, QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList,
    },
    msgbus::TypedHandler,
};
use nautilus_core::{
    AtomicMap, Params, UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{
    ExecutionClientCore, ExecutionEventEmitter,
    task::{TaskGroup, TaskRef},
};
use nautilus_model::{
    accounts::AccountAny,
    enums::{AccountType, OmsType},
    events::OrderEventAny,
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, Venue, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::OrderAny,
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance},
};
use nautilus_network::websocket::AuthTracker;
use reports::{order_report_task, position_report_task};
use rust_decimal::Decimal;

use crate::{
    common::PREDICT_VENUE,
    config::{PredictAccountStateMode, PredictExecutionClientConfig},
};

/// Incremental execution adapter with native account/order/position ownership.
/// Connectivity includes verified wallet continuity; submission also requires configured admission.
///
/// Predict's public API and official SDK expose no atomic order amendment operation.
/// `ModifyOrder` emits `OrderModifyRejected`; callers must explicitly cancel the old order
/// and submit a newly signed replacement after resolving its cancellation outcome.
/// No public batch-create endpoint is mapped: submit independent orders with `SubmitOrder`.
#[derive(Debug)]
pub struct PredictExecutionClient {
    core: ExecutionClientCore,
    config: PredictExecutionClientConfig,
    order_signer: Option<Arc<crate::signing::PredictOrderSigner>>,
    order_hasher: Arc<crate::signing::PredictOrderHasher>,
    chain: Option<crate::blockchain::PredictChainClient>,
    clock: &'static AtomicTime,
    emitter: ExecutionEventEmitter,
    tasks: TaskGroup,
    stream_task: Option<TaskRef>,
    account_task: Option<TaskRef>,
    account_query_active: Arc<AtomicBool>,
    order_queries: Arc<std::sync::Mutex<std::collections::HashSet<ClientOrderId>>>,
    account_orders: Arc<AtomicMap<ClientOrderId, Arc<OrderAny>>>,
    account_sync: Arc<account::AccountSync>,
    healthy: Arc<AtomicBool>,
    reconciliation_required: Arc<AtomicBool>,
    wallet_auth: Option<AuthTracker>,
    wallet_epoch: Option<Arc<AtomicU64>>,
    verified_epoch: Arc<AtomicU64>,
    recovery_clock: Option<std::rc::Rc<std::cell::RefCell<dyn nautilus_common::clock::Clock>>>,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    orders: Arc<AtomicMap<VenueOrderId, Arc<OrderAny>>>,
    order_observer: Option<TypedHandler<OrderEventAny>>,
    pending_cancels: cancellations::PendingCancels,
    deferred_cancels: cancellations::DeferredCancels,
    pending_submissions: orders::PendingSubmissions,
    submission_admission: Arc<tokio::sync::Mutex<()>>,
    fill_evidence: precision::SharedFillEvidence,
}

impl PredictExecutionClient {
    /// Creates a live client using Nautilus's realtime clock.
    ///
    /// # Errors
    /// Returns an error for unsupported core identity or invalid private configuration.
    pub fn new(
        core: ExecutionClientCore,
        config: PredictExecutionClientConfig,
    ) -> anyhow::Result<Self> {
        Self::new_with_clock(core, config, get_atomic_clock_realtime())
    }

    /// Creates a client with an explicit Nautilus clock for deterministic integration tests.
    ///
    /// # Errors
    /// Returns an error for unsupported core identity or invalid private configuration.
    pub fn new_with_clock(
        core: ExecutionClientCore,
        config: PredictExecutionClientConfig,
        clock: &'static AtomicTime,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let order_hasher = Arc::new(crate::signing::PredictOrderHasher::new(
            config.session.environment(),
        ));
        let order_signer = config.submission.as_ref().map(|policy| {
            Arc::new(crate::signing::PredictOrderSigner::new(Arc::clone(
                &policy.signer,
            )))
        });
        let chain = config
            .chain
            .clone()
            .map(crate::blockchain::PredictChainClient::new)
            .transpose()?;
        anyhow::ensure!(
            core.venue == *PREDICT_VENUE
                && core.account_id == config.account_id
                && core.oms_type == OmsType::Netting
                && core.account_type == AccountType::Cash
                && core.base_currency.is_none(),
            "Predict execution core requires the configured cash/netting account and venue"
        );
        let mut emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            core.account_type,
            core.base_currency,
        );
        if let Some(sender) = try_get_exec_event_sender() {
            emitter.set_sender(sender);
        }
        Ok(Self {
            core,
            config,
            order_signer,
            order_hasher,
            chain,
            clock,
            emitter,
            tasks: TaskGroup::new(),
            stream_task: None,
            account_task: None,
            account_query_active: Arc::new(AtomicBool::new(false)),
            order_queries: Default::default(),
            account_orders: Arc::new(AtomicMap::new()),
            account_sync: Arc::new(account::AccountSync::default()),
            healthy: Arc::new(AtomicBool::new(false)),
            reconciliation_required: Arc::new(AtomicBool::new(false)),
            wallet_auth: None,
            wallet_epoch: None,
            verified_epoch: Arc::new(AtomicU64::new(0)),
            recovery_clock: None,
            instruments: Arc::new(AtomicMap::new()),
            orders: Arc::new(AtomicMap::new()),
            order_observer: None,
            pending_cancels: Default::default(),
            deferred_cancels: Default::default(),
            pending_submissions: Default::default(),
            submission_admission: Default::default(),
            fill_evidence: Default::default(),
        })
    }

    /// Supplies the core-thread Nautilus clock for automatic recovery in custom live hosts.
    /// The standard execution factory installs this clock automatically.
    #[must_use]
    pub fn with_reconciliation_clock(
        mut self,
        clock: std::rc::Rc<std::cell::RefCell<dyn nautilus_common::clock::Clock>>,
    ) -> Self {
        self.recovery_clock = Some(clock);
        self
    }

    /// Returns the standard emitter, including its shared sender slot for custom live hosts.
    #[must_use]
    pub fn emitter(&self) -> &ExecutionEventEmitter {
        &self.emitter
    }

    /// Reads collateral for the authenticated account/deposit address. Reservations, funding
    /// admission and account publication remain separate; allowance is never a free balance.
    ///
    /// # Errors
    /// Missing RPC configuration, account mismatch or unverifiable chain snapshots return errors.
    pub async fn request_collateral_snapshot(
        &self,
    ) -> anyhow::Result<crate::blockchain::models::PredictCollateralSnapshot> {
        let chain = self
            .chain
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Predict collateral RPC is not configured"))?;
        self.config.session.request_account().await?;
        chain
            .request_collateral_snapshot(self.config.session.account_address(), self.clock)
            .await
    }

    /// Whether both the wallet session and applied-state reconciliation permit new submissions.
    #[must_use]
    pub fn is_submission_ready(&self) -> bool {
        self.config.submission.is_some()
            && self.config.account_state_mode == PredictAccountStateMode::Reported
            && self.is_connected()
            && !self.reconciliation_required.load(Ordering::Acquire)
    }
}

impl Drop for PredictExecutionClient {
    fn drop(&mut self) {
        self.healthy.store(false, Ordering::Release);
        self.tasks.abort();
        self.stop_recovery_timer();
        self.clear_observer();
    }
}

#[async_trait(?Send)]
impl ExecutionClient for PredictExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
            && self.healthy.load(Ordering::Acquire)
            && self
                .wallet_auth
                .as_ref()
                .is_some_and(AuthTracker::is_authenticated)
            && self.wallet_epoch.as_ref().is_some_and(|epoch| {
                epoch.load(Ordering::Acquire) == self.verified_epoch.load(Ordering::Acquire)
            })
            && self.stream_task.as_ref().is_some_and(TaskRef::is_active)
            && (self.config.account_state_mode == PredictAccountStateMode::Disabled
                || self.account_task.as_ref().is_some_and(TaskRef::is_active))
    }
    fn client_id(&self) -> ClientId {
        self.core.client_id
    }
    fn account_id(&self) -> AccountId {
        self.core.account_id
    }
    fn venue(&self) -> Venue {
        self.core.venue
    }
    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }
    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }
    fn retain_unresolved_submissions(&self) -> bool {
        true
    }
    fn provides_bulk_position_coverage(&self, _: InstrumentId) -> bool {
        false
    }
    fn position_reconciliation_tolerance(&self) -> Decimal {
        precision::POSITION_RECONCILIATION_TOLERANCE
    }
    fn start(&mut self) -> anyhow::Result<()> {
        let sender = try_get_exec_event_sender()
            .ok_or_else(|| anyhow::anyhow!("Nautilus execution event sender is not installed"))?;
        self.emitter.set_sender(sender);
        self.core.set_started();
        Ok(())
    }
    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_connected() && !self.account_orders.load().is_empty() {
            self.reconciliation_required.store(true, Ordering::Release);
        }
        self.healthy.store(false, Ordering::Release);
        self.core.set_disconnected();
        self.core.set_stopped();
        self.tasks.begin_shutdown();
        self.stop_recovery_timer();
        {
            let mut sync = self
                .account_sync
                .state
                .lock()
                .expect("account sync poisoned");
            sync.revision = sync.revision.wrapping_add(1);
        }
        self.clear_observer();
        Ok(())
    }
    fn reset(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.pending_submissions
                .lock()
                .expect("submission registry poisoned")
                .entries
                .is_empty(),
            "Predict submitted orders must be reconciled before reset"
        );
        anyhow::ensure!(
            self.tasks.is_empty(),
            "Predict tasks must be drained before reset"
        );
        self.stop()?;
        self.instruments.store(Default::default());
        self.orders.store(Default::default());
        self.fill_evidence = Default::default();
        self.reconciliation_required.store(false, Ordering::Release);
        self.account_orders.store(Default::default());
        Ok(())
    }
    async fn connect(&mut self) -> anyhow::Result<()> {
        self.connect_impl().await
    }
    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.disconnect_impl().await
    }
    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
        info: Option<Params>,
    ) -> anyhow::Result<()> {
        self.emitter
            .try_emit_account_state(balances, margins, reported, ts_event, info)
    }
    fn on_instrument(&mut self, instrument: InstrumentAny) {
        if instrument.id().venue == self.core.venue {
            let mut sync = self
                .account_sync
                .state
                .lock()
                .expect("account sync poisoned");
            sync.revision = sync.revision.wrapping_add(1);
            self.instruments.insert(instrument.id(), instrument);
            drop(sync);
            self.account_sync.notify.notify_one();
        }
    }
    fn generate_position_status_reports_task(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> Option<ExecutionReportTask<Vec<PositionStatusReport>>> {
        self.prune_order_lookups();
        Some(position_report_task(
            self.config.clone(),
            self.core.clone(),
            self.clock,
            cmd,
        ))
    }
    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        self.prune_order_lookups();
        let task = position_report_task(self.config.clone(), self.core.clone(), self.clock, cmd);
        task.collection.await;
        task.result.await
    }
    fn generate_order_status_report_task(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> Option<ExecutionReportTask<Option<OrderStatusReport>>> {
        Some(order_report_task(self, cmd))
    }
    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let task = order_report_task(self, cmd);
        task.collection.await;
        task.result.await
    }
    fn generate_order_status_reports_task(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> Option<ExecutionReportTask<Vec<OrderStatusReport>>> {
        let task = reconciliation::bulk_task(self, cmd, false, None);
        Some(ExecutionReportTask {
            collection: task.collection,
            result: Box::pin(async move { Ok(task.result.await?.0) }),
        })
    }
    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let task = reconciliation::bulk_task(self, cmd, false, None);
        task.collection.await;
        Ok(task.result.await?.0)
    }
    fn generate_fill_reports_task(
        &self,
        cmd: &GenerateFillReports,
    ) -> Option<ExecutionReportTask<Vec<FillReport>>> {
        self.prune_order_lookups();
        Some(matches::fill_report_task(
            self.config.clone(),
            self.core.clone(),
            self.clock,
            Arc::clone(&self.fill_evidence),
            cmd,
        ))
    }
    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        self.prune_order_lookups();
        let task = matches::fill_report_task(
            self.config.clone(),
            self.core.clone(),
            self.clock,
            Arc::clone(&self.fill_evidence),
            &cmd,
        );
        task.collection.await;
        task.result.await
    }
    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        anyhow::ensure!(
            self.core.is_connected(),
            "Predict mass status requires an active client generation"
        );
        // The native lookback scopes unknown fill history, never current orders/holdings.
        // Known orders retain their full settlement prefix for exact recovery.
        let now = self.clock.get_time_ns();
        let lookback_start = lookback_mins
            .map(nautilus_core::DurationNanos::try_from_mins)
            .transpose()?
            .map(|duration| now.saturating_sub(duration));
        let cancellation = self.tasks.cancellation_token();
        let epoch = self
            .wallet_epoch
            .as_ref()
            .map(|e| e.load(Ordering::Acquire));
        let cmd = GenerateOrderStatusReports::new(
            nautilus_core::UUID4::new(),
            now,
            false,
            None,
            None,
            None,
            None,
            None,
        );
        let task = reconciliation::bulk_task(self, &cmd, true, lookback_start);
        task.collection.await;
        let (orders, fills) = task.result.await?;
        let positions_cmd = GeneratePositionStatusReports::new(
            nautilus_core::UUID4::new(),
            now,
            None,
            None,
            None,
            None,
            None,
        );
        let positions = self
            .generate_position_status_reports(&positions_cmd)
            .await?;
        anyhow::ensure!(
            !cancellation.is_cancelled()
                && self.core.is_connected()
                && self
                    .wallet_epoch
                    .as_ref()
                    .map(|e| e.load(Ordering::Acquire))
                    == epoch,
            "Predict mass status generation changed during collection"
        );
        let mut report = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            self.core.venue,
            now,
            None,
        );
        report.set_report_window(lookback_start, true);
        report.add_order_reports(orders);
        report.add_fill_reports(fills);
        report.add_position_reports(positions);
        Ok(Some(report))
    }
    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        self.submit_order_command(&cmd)
    }
    fn submit_order_list(&self, _: SubmitOrderList) -> anyhow::Result<()> {
        anyhow::bail!(
            "Predict public API exposes no batch-create endpoint; submit independent orders explicitly"
        )
    }
    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        self.modify_order_command(&cmd)
    }
    fn batch_modify_orders(&self, _: BatchModifyOrders) -> anyhow::Result<()> {
        anyhow::bail!("Predict public API and SDK expose no atomic order modification operation")
    }
    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.tasks.abort();
        self.stop_recovery_timer();
        Ok(())
    }
    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        self.cancel_order_command(&cmd)
    }
    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        self.cancel_all_orders_command(&cmd)
    }
    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        self.batch_cancel_orders_command(&cmd)
    }
    fn query_account(&self, cmd: QueryAccount) -> anyhow::Result<()> {
        anyhow::ensure!(
            cmd.account_id == self.core.account_id
                && cmd.trader_id == self.core.trader_id
                && cmd.client_id.is_none_or(|id| id == self.core.client_id),
            "Predict account query identity differs from execution client"
        );
        anyhow::ensure!(
            self.config.account_state_mode == PredictAccountStateMode::Reported
                && self.core.is_connected()
                && self.tasks.is_open(),
            "Predict reported account client generation is not active"
        );
        self.prune_order_lookups();
        if self.reconciliation_required.load(Ordering::Acquire) || !self.is_connected() {
            self.reconciliation_required.store(true, Ordering::Release);
            let mut reporter = self.account_reporter()?;
            reporter.allow_stream_gap = true;
            let check = match readiness::ReadinessCheck::new(self) {
                Ok(check) => Some(check),
                Err(error) => {
                    log::warn!("Predict readiness snapshot is not eligible: {error}");
                    None
                }
            };
            let Some(guard) = account::AccountQueryGuard::acquire(&self.account_query_active)
            else {
                return Ok(());
            };
            let cancellation = self.tasks.cancellation_token();
            self.tasks
                .spawn_named("predict-gap-account-query", async move {
                    let _guard = guard;
                    for _ in 0..5 {
                        let result = tokio::select! {biased;
                            _ = cancellation.cancelled() => return,
                            result = reporter.refresh() => result,
                        };
                        match result {
                            Ok(true) => {
                                let result = tokio::select! {biased;
                                    _ = cancellation.cancelled() => return,
                                    result = async {match check {Some(check)=>check.confirm().await,None=>Ok(())}} => result,
                                };
                                if let Err(error) = result {
                                    log::warn!(
                                        "Predict submission readiness remains gated: {error}"
                                    );
                                }
                                return;
                            }
                            Ok(false) => continue,
                            Err(error) => {
                                log::warn!("Predict gap account query remains unresolved: {error}");
                                return;
                            }
                        }
                    }
                })?;
            return Ok(());
        }
        anyhow::ensure!(
            self.is_connected(),
            "Predict reported account session is not connected"
        );
        self.account_sync.notify.notify_one();
        Ok(())
    }
    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        self.query_order_command(&cmd)
    }
}
