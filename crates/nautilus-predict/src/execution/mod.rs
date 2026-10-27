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

//! Live execution client for Predict commands, wallet facts and Native reports.

mod account;
mod context;
mod fills;
mod lifecycle;
mod maintenance;
mod metadata;
pub mod observations;
mod orders;
pub mod parse;
mod reports;
mod wallet;

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use async_trait::async_trait;
use nautilus_common::{
    clients::{ExecutionClient, ExecutionReportTask},
    live::runner::try_get_exec_event_sender,
    messages::execution::{
        BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
        GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
        ModifyOrder, QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList,
    },
    msgbus::TypedHandler,
};
use nautilus_core::{
    Params, UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{
    ExecutionClientCore, ExecutionEventEmitter,
    task::{TaskGroup, TaskRef},
};
use nautilus_model::{
    accounts::AccountAny,
    enums::{AccountType, OmsType},
    events::{AccountState, OrderEventAny},
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, Venue, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance},
};
use nautilus_network::websocket::AuthTracker;
use rust_decimal::Decimal;

use crate::execution::reports::{order_report_task, position_report_task};
use crate::{
    common::PREDICT_VENUE,
    config::{PredictAccountStateMode, PredictExecutionClientConfig},
};

pub use reports::scope::reconciliation_market_ids;

/// Predict execution client with Native order, position and account ownership.
///
/// Connectivity reflects authenticated wallet transport. Signing readiness is checked
/// per command. ModifyOrder emits OrderModifyRejected; strategies own cancellation
/// and replacement. Orders are submitted individually through SubmitOrder.
#[derive(Debug)]
pub struct PredictExecutionClient {
    core: ExecutionClientCore,
    config: PredictExecutionClientConfig,
    order_signer: Option<Arc<crate::signing::PredictOrderSigner>>,
    order_hasher: Arc<crate::signing::PredictOrderHasher>,
    chain: Option<crate::blockchain::PredictChainClient>,
    clock: &'static AtomicTime,
    emitter: ExecutionEventEmitter,
    data_sender:
        Option<nautilus_common::live::sender::EventSender<nautilus_common::messages::DataEvent>>,
    tasks: TaskGroup,
    stream_task: Option<TaskRef>,
    account_query_active: Arc<AtomicBool>,
    account_orders: context::OrderSnapshots<ClientOrderId>,
    account_sync: Arc<account::AccountSync>,
    healthy: Arc<AtomicBool>,
    account_refresh_required: Arc<AtomicBool>,
    wallet_auth: Option<AuthTracker>,
    wallet_epoch: Option<Arc<AtomicU64>>,
    refreshed_epoch: Arc<AtomicU64>,
    maintenance_clock: Option<std::rc::Rc<std::cell::RefCell<dyn nautilus_common::clock::Clock>>>,
    metadata: Arc<metadata::ExecutionMetadata>,
    orders: context::OrderSnapshots<VenueOrderId>,
    order_observer: Option<TypedHandler<OrderEventAny>>,
    account_observer: Option<TypedHandler<AccountState>>,
    cleanup_cursor: std::rc::Rc<std::cell::Cell<(usize, usize)>>,
    pending_cancels: orders::cancel::PendingCancels,
    deferred_cancels: orders::cancel::DeferredCancels,
    pending_submissions: orders::PendingSubmissions,
    fill_evidence: fills::SharedFillEvidence,
    rest_order_ids: orders::RestOrderIds,
}

impl PredictExecutionClient {
    /// Creates a live client using Nautilus's realtime clock.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported core identity or invalid private configuration.
    pub fn new(
        core: ExecutionClientCore,
        config: PredictExecutionClientConfig,
    ) -> anyhow::Result<Self> {
        Self::new_with_clock(core, config, get_atomic_clock_realtime())
    }

    /// Creates a client using the supplied Nautilus clock.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported core identity or invalid private configuration.
    pub fn new_with_clock(
        core: ExecutionClientCore,
        config: PredictExecutionClientConfig,
        clock: &'static AtomicTime,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        nautilus_model::data::ensure_custom_data_json_registered::<
            observations::PredictSettlementUpdate,
        >()?;
        let order_hasher = Arc::new(crate::signing::PredictOrderHasher::new(
            config.session.environment(),
        ));
        let order_signer = config
            .submission_signer
            .as_ref()
            .map(|signer| Arc::new(crate::signing::PredictOrderSigner::new(Arc::clone(signer))));
        let mut chain = config
            .chain
            .clone()
            .map(crate::blockchain::PredictChainClient::new)
            .transpose()?;
        if let Some(chain) = chain.as_mut() {
            chain.latency = config.latency.clone();
        }
        anyhow::ensure!(
            core.venue == *PREDICT_VENUE
                && core.account_id == config.account_id
                && core.oms_type == OmsType::Netting
                && core.account_type == AccountType::Cash
                && core.base_currency.is_none(),
            "Predict execution core requires the configured cash/netting account and venue"
        );
        anyhow::ensure!(
            config
                .latency
                .as_ref()
                .is_none_or(|probe| probe.matches_owner(core.trader_id, core.account_id)),
            "Predict latency probe belongs to another trader/account"
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
            data_sender: nautilus_common::live::runner::try_get_data_event_sender(),
            tasks: TaskGroup::new(),
            stream_task: None,
            account_query_active: Arc::new(AtomicBool::new(false)),
            account_orders: Default::default(),
            account_sync: Arc::new(account::AccountSync::default()),
            healthy: Arc::new(AtomicBool::new(false)),
            account_refresh_required: Arc::new(AtomicBool::new(false)),
            wallet_auth: None,
            wallet_epoch: None,
            refreshed_epoch: Arc::new(AtomicU64::new(0)),
            maintenance_clock: None,
            metadata: Arc::new(metadata::ExecutionMetadata::default()),
            orders: Default::default(),
            order_observer: None,
            account_observer: None,
            cleanup_cursor: Default::default(),
            pending_cancels: Default::default(),
            deferred_cancels: Default::default(),
            pending_submissions: Default::default(),
            fill_evidence: Default::default(),
            rest_order_ids: Default::default(),
        })
    }

    /// Supplies the core-thread Nautilus clock for account refresh and mirror maintenance.
    /// The standard execution factory installs this clock automatically.
    #[must_use]
    pub fn with_reconciliation_clock(
        mut self,
        clock: std::rc::Rc<std::cell::RefCell<dyn nautilus_common::clock::Clock>>,
    ) -> Self {
        self.maintenance_clock = Some(clock);
        self
    }

    /// Returns the standard emitter, including its shared sender slot for custom live hosts.
    #[must_use]
    pub fn emitter(&self) -> &ExecutionEventEmitter {
        &self.emitter
    }

    /// Reads chain collateral and allowances for the authenticated account.
    /// This query does not publish AccountState; allowance is not available cash.
    ///
    /// # Errors
    ///
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

    /// Returns whether signing is enabled on a connected client with reported cash.
    /// Market metadata and contract nonces are checked separately for each command.
    #[must_use]
    pub fn is_submission_ready(&self) -> bool {
        self.config.submission_signer.is_some()
            && self.config.account_state_mode == PredictAccountStateMode::Reported
            && self.is_connected()
    }
}

impl Drop for PredictExecutionClient {
    fn drop(&mut self) {
        self.healthy.store(false, Ordering::Release);
        self.tasks.abort();
        self.stop_maintenance_timer();
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
            && self.stream_task.as_ref().is_some_and(TaskRef::is_active)
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

    fn retain_unresolved_commands(&self) -> bool {
        true
    }
    fn provides_bulk_position_coverage(&self, _: InstrumentId) -> bool {
        false
    }
    fn position_reconciliation_tolerance(&self) -> Decimal {
        fills::POSITION_RECONCILIATION_TOLERANCE
    }
    fn start(&mut self) -> anyhow::Result<()> {
        self.start_impl()
    }
    fn stop(&mut self) -> anyhow::Result<()> {
        self.stop_impl()
    }
    fn reset(&mut self) -> anyhow::Result<()> {
        self.reset_impl()
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
            self.prune_mirrors_on_request();
            let mut sync = self
                .account_sync
                .state
                .lock()
                .expect("account sync poisoned");
            sync.resource_revision = sync.resource_revision.wrapping_add(1);
            self.metadata
                .instruments
                .rcu(|map| metadata::upsert_instrument(map, instrument.clone()));
            drop(sync);
            self.account_sync.notify.notify_one();
            self.metadata.notify.notify_one();
        }
    }
    fn generate_position_status_reports_task(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> Option<ExecutionReportTask<Vec<PositionStatusReport>>> {
        self.prune_mirrors_on_request();
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
        self.prune_mirrors_on_request();
        let task = position_report_task(self.config.clone(), self.core.clone(), self.clock, cmd);
        task.collection.await;
        (task.finish)()
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
        (task.finish)()
    }
    fn generate_order_status_reports_task(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> Option<ExecutionReportTask<Vec<OrderStatusReport>>> {
        let task = reports::bulk::bulk_task(self, cmd, false, None);
        Some(ExecutionReportTask {
            collection: task.collection,
            finish: Box::new(move || Ok((task.finish)()?.0)),
        })
    }
    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let task = reports::bulk::bulk_task(self, cmd, false, None);
        task.collection.await;
        Ok((task.finish)()?.0)
    }
    fn generate_fill_reports_task(
        &self,
        cmd: &GenerateFillReports,
    ) -> Option<ExecutionReportTask<Vec<FillReport>>> {
        self.prune_mirrors_on_request();
        Some(reports::trades::fill_report_task(
            self.config.clone(),
            self.core.clone(),
            self.clock,
            Arc::clone(&self.fill_evidence),
            Arc::clone(&self.account_orders),
            cmd,
        ))
    }
    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        self.prune_mirrors_on_request();
        let task = reports::trades::fill_report_task(
            self.config.clone(),
            self.core.clone(),
            self.clock,
            Arc::clone(&self.fill_evidence),
            Arc::clone(&self.account_orders),
            &cmd,
        );
        task.collection.await;
        (task.finish)()
    }
    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        self.mass_status(lookback_mins).await
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
    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.tasks.abort();
        self.stop_maintenance_timer();
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
        self.query_account_command(cmd)
    }
    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        self.query_order_command(&cmd)
    }
}
