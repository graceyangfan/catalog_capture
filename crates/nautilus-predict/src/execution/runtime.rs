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

//! Wallet feedback for known orders. Native owns transitions and strategy delivery.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use alloy::primitives::Address;
use nautilus_common::{factories::OrderEventFactory, messages::ExecutionReport};
use nautilus_core::{AtomicMap, time::AtomicTime};
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::{
    enums::{AccountType, OrderStatus},
    events::OrderEventAny,
    identifiers::{AccountId, InstrumentId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    types::Quantity,
};
use tokio_util::sync::CancellationToken;

use super::context::OrderView;
use crate::{
    common::instrument_id_from_outcome,
    websocket::{
        PredictWalletWebSocketClient,
        private_messages::{
            PredictWalletEventKind, PredictWalletOutcome, PredictWalletRejectionReason,
            PredictWalletStreamMessage,
        },
    },
};

struct HealthGuard(Arc<AtomicBool>);
impl Drop for HealthGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

pub(super) struct WalletReportContext {
    pub data_sender:
        Option<nautilus_common::live::sender::EventSender<nautilus_common::messages::DataEvent>>,
    pub instruments: Arc<AtomicMap<InstrumentId, super::types::ExecutionInstrument>>,
    pub orders: super::types::OrderSnapshots<VenueOrderId>,
    pub pending: super::orders::PendingSubmissions,
    pub rest_ids: super::routing::RestOrderIds,
    pub sync: Arc<super::account::AccountSync>,
    pub fill_evidence: super::precision::SharedFillEvidence,
    pub account_refresh_required: Arc<AtomicBool>,
    pub allow_share_fees: bool,
    pub emitter: ExecutionEventEmitter,
    pub account: AccountId,
    pub wallet: Address,
    pub clock: &'static AtomicTime,
    pub latency: Option<crate::latency::PredictLatency>,
}

impl WalletReportContext {
    fn process_wallet(
        &self,
        event: &crate::websocket::private_messages::PredictWalletEvent,
        connection_epoch: u64,
        received: Option<std::time::Instant>,
        wallet_epoch: &std::sync::atomic::AtomicU64,
    ) -> anyhow::Result<()> {
        let WalletReportContext {
            instruments,
            pending,
            rest_ids,
            allow_share_fees,
            account,
            wallet,
            clock,
            ..
        } = self;
        let id = VenueOrderId::new(format!("{:#x}", event.order.order_hash));
        let needs_fills = matches!(
            event.kind,
            PredictWalletEventKind::OrderTransactionSuccess { .. }
                | PredictWalletEventKind::OrderNotAccepted { .. }
        );
        let (order, mut evidence) = self.order_snapshot(id)?;
        let outcome = match event.order.details.outcome {
            PredictWalletOutcome::Yes => 1,
            PredictWalletOutcome::No => 2,
        };
        let instrument_id = instrument_id_from_outcome(event.order.details.market_id, outcome)?;
        let definitions = instruments.load();
        let Some(InstrumentAny::BinaryOption(instrument)) = definitions
            .get(&instrument_id)
            .map(|entry| &entry.instrument)
        else {
            anyhow::bail!("Predict wallet instrument is absent from the Nautilus cache context");
        };
        let mut confirmed = order.filled_qty().as_decimal();
        let mut confirmed_precision = instrument
            .size_precision()
            .max(order.filled_qty().precision);
        if needs_fills {
            for fill in evidence.pending_for(&order, id) {
                confirmed_precision = confirmed_precision.max(fill.report.last_qty.precision);
                confirmed = confirmed
                    .checked_add(fill.report.last_qty.as_decimal())
                    .ok_or_else(|| anyhow::anyhow!("Predict confirmed fill aggregate overflow"))?;
            }
        }
        let confirmed =
            Quantity::from_decimal_dp(confirmed, confirmed_precision).map_err(|_| {
                anyhow::anyhow!("Predict confirmed fill aggregate exceeds Nautilus range")
            })?;
        let (registered, live_submission) = pending
            .lock()
            .expect("submission registry poisoned")
            .entries
            .get(&order.client_order_id())
            .map(|entry| {
                (
                    entry.venue_id.filter(|hash| *hash == id),
                    entry.wallet_epoch == Some(connection_epoch),
                )
            })
            .unwrap_or((None, false));
        let Some(report) = super::parse::parse_wallet_report_with_registration(
            event,
            &order,
            instrument,
            *account,
            *wallet,
            clock.get_time_ns(),
            super::parse::WalletReportEvidence {
                venue_id: id,
                instrument_id,
                confirmed_qty: confirmed,
                pending_settlement: evidence.notifications.has_pending(id),
                registered,
                queued_acceptance: order.queued_acceptance(),
                normalize: true,
                allow_share_fees: *allow_share_fees,
            },
        )?
        else {
            drop(evidence);
            self.matching(
                event,
                &order,
                id,
                connection_epoch,
                received,
                live_submission
                    && registered.is_some()
                    && wallet_epoch.load(Ordering::Acquire) == connection_epoch,
            )?;
            return Ok(());
        };
        super::routing::observe(rest_ids, id, &event.order.order_id);
        if let ExecutionReport::Fill(native) = report {
            let mut fill = super::precision::BufferedFill::from_wallet(*native, event)?;
            evidence
                .notifications
                .validate_success(event, fill.report.trade_id, id)?;
            evidence.validate_wallet_timestamp(&fill)?;
            if evidence.prepare_wallet(&mut fill, &order, instrument.size_precision())? {
                drop(evidence);
                self.settlement(event, &order, connection_epoch, received)?;
                return Ok(());
            }
            anyhow::ensure!(
                fill.report
                    .last_qty
                    .as_decimal()
                    .checked_add(confirmed.as_decimal())
                    .is_some_and(|total| total <= order.submitted_quantity().as_decimal()),
                "Predict confirmed fills exceed cached order quantity"
            );
            if live_submission
                && wallet_epoch.load(Ordering::Acquire) == connection_epoch
                && registered.is_some()
                && !order.is_closed()
                && self.send_acceptance(
                    &order,
                    id,
                    fill.report.ts_event,
                    order.queued_acceptance().is_some(),
                )?
            {
                order.record_acceptance(fill.report.ts_event);
            }
            // Native report ingestion owns snapshot coverage and fee preflight for both
            // current wallet fills and restored/gap fills. A direct OrderFilled bypasses
            // its snapshot-covered inventory suppression on this dependency revision.
            self.send_report(
                ExecutionReport::Fill(Box::new(fill.report.clone())),
                received,
            )?;
            evidence.queue_wallet(&fill)?;
            // Native infers acceptance when applying a gap fill report. Retain that
            // queued fact so a late HTTP error cannot reject the same order first.
            order.record_acceptance(fill.report.ts_event);
            drop(evidence);
            self.settlement(event, &order, connection_epoch, received)?;
        } else {
            drop(evidence);
            self.lifecycle(report, &order, received)?;
        }
        Ok(())
    }

    fn matching(
        &self,
        event: &crate::websocket::private_messages::PredictWalletEvent,
        order: &super::context::WorkerOrder,
        id: VenueOrderId,
        epoch: u64,
        received: Option<std::time::Instant>,
        live: bool,
    ) -> anyhow::Result<()> {
        if let PredictWalletEventKind::OrderNotAccepted { reason } = &event.kind {
            let reason = match reason {
                PredictWalletRejectionReason::NoMarketMatch => "noMarketMatch",
                PredictWalletRejectionReason::RejectedDuplicate => "rejectedDuplicate",
                PredictWalletRejectionReason::RejectedPostOnly => "rejectedPostOnly",
                PredictWalletRejectionReason::Unknown => "unknown",
            };
            let update = super::rejection::PredictOrderRejectionUpdate {
                account_id: self.account,
                strategy_id: order.strategy_id(),
                instrument_id: order.instrument_id(),
                client_order_id: order.client_order_id(),
                venue_order_id: id,
                reason: reason.to_string(),
                reported_quantity_filled: event.order.details.quantity_filled.to_string(),
                native_filled_quantity: order.filled_qty().as_decimal().to_string(),
                native_status: order.status(),
                requires_reconciliation: !order.is_closed(),
                connection_epoch: epoch,
                ts_event: event.order.timestamp_ns()?,
                ts_init: self.clock.get_time_ns(),
            };
            if let Some(sender) = &self.data_sender
                && sender
                    .send(nautilus_common::messages::DataEvent::Data(
                        nautilus_model::data::Data::Custom(update.into_custom_data()),
                    ))
                    .is_err()
            {
                log::warn!("Predict order rejection metadata channel closed");
            }
            if !order.is_closed() {
                self.sync
                    .state
                    .lock()
                    .expect("account sync poisoned")
                    .order_status_requests
                    .insert(order.client_order_id());
                log::warn!(
                    "Predict {reason} does not prove the original order terminated; requesting Native order status for {}",
                    order.client_order_id()
                );
            }
            return Ok(());
        }
        self.settlement(event, order, epoch, received)?;
        if matches!(
            event.kind,
            PredictWalletEventKind::OrderTransactionSubmitted { .. }
                | PredictWalletEventKind::OrderTransactionFailed { .. }
        ) {
            super::routing::observe(&self.rest_ids, id, &event.order.order_id);
        }
        if matches!(
            event.kind,
            PredictWalletEventKind::OrderTransactionSubmitted { .. }
        ) && live
            && !order.is_closed()
            && self.send_acceptance(
                order,
                id,
                event.order.timestamp_ns()?,
                order.queued_acceptance().is_some(),
            )?
        {
            order.record_acceptance(event.order.timestamp_ns()?);
        }
        Ok(())
    }

    fn lifecycle(
        &self,
        report: ExecutionReport,
        order: &super::context::WorkerOrder,
        received: Option<std::time::Instant>,
    ) -> anyhow::Result<()> {
        let accepted = if let ExecutionReport::Order(status) = &report {
            (status.ts_accepted.as_u64() > 0).then_some(status.ts_accepted)
        } else {
            None
        };
        self.send_lifecycle(report, order, order.queued_acceptance().is_some(), received)?;
        if let Some(accepted) = accepted
            && order.queued_acceptance().is_none()
        {
            order.record_acceptance(accepted);
        }
        Ok(())
    }

    fn settlement(
        &self,
        event: &crate::websocket::private_messages::PredictWalletEvent,
        order: &super::context::WorkerOrder,
        epoch: u64,
        received: Option<std::time::Instant>,
    ) -> anyhow::Result<()> {
        // No IO, economic projection or Native Filled is performed for Submitted/Failed.
        if !matches!(
            event.kind,
            PredictWalletEventKind::OrderTransactionSubmitted { .. }
                | PredictWalletEventKind::OrderTransactionSuccess { .. }
                | PredictWalletEventKind::OrderTransactionFailed { .. }
        ) {
            return Ok(());
        }
        let update = self
            .fill_evidence
            .lock()
            .expect("fill evidence poisoned")
            .notifications
            .update(event, order, self.account, epoch, self.clock.get_time_ns())?;
        if let (Some(sender), Some(update)) = (&self.data_sender, update) {
            if let Some((probe, received)) = self.latency.as_ref().zip(received) {
                probe.match_feedback(&update, received, std::time::Instant::now());
            }
            if sender
                .send(nautilus_common::messages::DataEvent::Data(
                    nautilus_model::data::Data::Custom(update.into_custom_data()),
                ))
                .is_err()
            {
                log::warn!("Predict settlement metadata channel closed");
            }
        }
        Ok(())
    }

    fn send_acceptance(
        &self,
        order: &super::context::WorkerOrder,
        id: VenueOrderId,
        event_time: nautilus_core::UnixNanos,
        queued: bool,
    ) -> anyhow::Result<bool> {
        if order.ts_accepted().is_some() || queued {
            return Ok(false);
        }
        let factory = OrderEventFactory::new(
            self.emitter.trader_id(),
            self.account,
            AccountType::Cash,
            None,
        );
        self.emitter.try_send_order_event(order.accepted(
            &factory,
            id,
            event_time,
            self.clock.get_time_ns(),
        ))?;
        Ok(true)
    }
    // Lifecycle facts use native events; confirmed fills use report ingestion.
    fn send_lifecycle(
        &self,
        report: ExecutionReport,
        order: &super::context::WorkerOrder,
        acceptance_queued: bool,
        received: Option<std::time::Instant>,
    ) -> anyhow::Result<()> {
        let ExecutionReport::Order(status) = &report else {
            anyhow::bail!("Predict wallet lifecycle requires an order status");
        };
        self.sync.confirm_report(status);
        if order.is_closed() {
            return Ok(());
        }
        // Bind a missing venue hash, or supply acceptance required before Expired.
        // Reuse the existing queued proof; Canceled with a known hash needs no bootstrap.
        if (status.order_status == OrderStatus::Expired
            || status.order_status == OrderStatus::Canceled && order.venue_order_id().is_none())
            && order.ts_accepted().is_none()
            && !acceptance_queued
            && self.send_acceptance(order, status.venue_order_id, status.ts_last, false)?
        {
            order.record_acceptance(status.ts_last);
        }
        let factory = OrderEventFactory::new(
            self.emitter.trader_id(),
            self.account,
            AccountType::Cash,
            None,
        );
        let event = match status.order_status {
            OrderStatus::Accepted => {
                if order.ts_accepted().is_some() || acceptance_queued {
                    return Ok(());
                }
                order.accepted(
                    &factory,
                    status.venue_order_id,
                    status.ts_accepted,
                    status.ts_init,
                )
            }
            // Confirmed quantity is already applied or queued as a fill report.
            // Reconciliation supplies acceptance there; another Accepted after it
            // could move PartiallyFilled back to Accepted in the native FSM.
            OrderStatus::PartiallyFilled | OrderStatus::Filled => return Ok(()),
            OrderStatus::Canceled => {
                let mut event = order.canceled(
                    &factory,
                    Some(status.venue_order_id),
                    status.ts_last,
                    status.ts_init,
                );
                if let OrderEventAny::Canceled(event) = &mut event {
                    event.reason = status.cancel_reason.as_deref().map(Into::into);
                }
                event
            }
            OrderStatus::Expired => order.expired(
                &factory,
                Some(status.venue_order_id),
                status.ts_last,
                status.ts_init,
            ),
            OrderStatus::Rejected => order.rejected(
                &factory,
                status
                    .cancel_reason
                    .as_deref()
                    .unwrap_or("Predict order rejected"),
                status.ts_last,
                status.ts_init,
                status.cancel_reason.as_deref() == Some("Predict post-only order rejected"),
            ),
            other => anyhow::bail!("unsupported Predict wallet status {other:?}"),
        };
        let timing = self
            .latency
            .as_ref()
            .zip(received)
            .and_then(|(probe, received)| {
                probe.prepare_feedback(&report, received, std::time::Instant::now())
            });
        self.emitter.try_send_order_event(event)?;
        if let (Some(probe), Some(timing)) = (&self.latency, timing) {
            probe.feedback(timing);
        }
        Ok(())
    }

    fn send_report(
        &self,
        report: ExecutionReport,
        received: Option<std::time::Instant>,
    ) -> anyhow::Result<()> {
        let timing = self
            .latency
            .as_ref()
            .zip(received)
            .and_then(|(probe, received)| {
                probe.prepare_feedback(&report, received, std::time::Instant::now())
            });
        self.emitter.try_send_execution_report(report)?;
        if let (Some(probe), Some(timing)) = (&self.latency, timing) {
            probe.feedback(timing);
        }
        Ok(())
    }

    pub(super) fn order_snapshot(
        &self,
        id: VenueOrderId,
    ) -> anyhow::Result<(
        Arc<super::context::WorkerOrder>,
        std::sync::MutexGuard<'_, super::precision::FillEvidence>,
    )> {
        // Sample the view and pending facts under the observer's publication lock.
        // Release that global lock before parsing/normalization. Keep the evidence
        // guard until enqueue so Native cannot clear pending against an older view.
        let _state = self.sync.state.lock().expect("account sync poisoned");
        let order = self
            .orders
            .lock()
            .expect("order snapshots poisoned")
            .get(&id)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("Predict wallet order is absent from the Nautilus cache context")
            })?;
        let evidence = self
            .fill_evidence
            .lock()
            .map_err(|_| anyhow::anyhow!("Predict fill evidence poisoned"))?;
        Ok((order, evidence))
    }
}

pub(super) async fn run_wallet(
    mut ws: PredictWalletWebSocketClient,
    cancellation: CancellationToken,
    healthy: Arc<AtomicBool>,
    context: WalletReportContext,
) {
    let _health = HealthGuard(Arc::clone(&healthy));
    let result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => Ok(()),
        result = consume(&mut ws, &context) => result,
    };
    healthy.store(false, Ordering::Release);
    if !cancellation.is_cancelled() {
        context
            .account_refresh_required
            .store(true, Ordering::Release);
    }
    if let Err(error) = result {
        log::warn!("Predict wallet execution stream stopped: {error}");
    }
    if ws.close().await.is_err() {
        log::warn!("Predict wallet transport cleanup failed");
    }
}

async fn consume(
    ws: &mut PredictWalletWebSocketClient,
    context: &WalletReportContext,
) -> anyhow::Result<()> {
    let wallet_epoch = ws.connection_epoch_handle();
    while let Some((message, received)) = ws.next_timed_message().await? {
        match message {
            PredictWalletStreamMessage::Subscribed {
                connection_epoch: 0,
                ..
            } => {}
            PredictWalletStreamMessage::Subscribed { .. } => {
                context
                    .account_refresh_required
                    .store(true, Ordering::Release);
                log::info!("Predict wallet resubscribed; requesting account refresh");
            }
            PredictWalletStreamMessage::Wallet {
                event,
                connection_epoch,
            } => {
                let result =
                    context.process_wallet(&event, connection_epoch, received, &wallet_epoch);
                if let Err(error) = result {
                    // Missing external context and contradictory evidence request a refresh,
                    // but never kill the consumer needed to reconcile subsequent own events.
                    let mut state = context.sync.state.lock().expect("account sync poisoned");
                    state.revision = state.revision.wrapping_add(1);
                    state.resource_revision = state.resource_revision.wrapping_add(1);
                    context
                        .account_refresh_required
                        .store(true, Ordering::Release);
                    log::warn!("Predict wallet event requires reconciliation: {error}");
                }
            }
        }
    }
    anyhow::bail!("Predict wallet execution stream closed")
}
