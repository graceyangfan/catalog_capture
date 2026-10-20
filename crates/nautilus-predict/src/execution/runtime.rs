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

//! Worker-side report publication. Native order/instrument snapshots arrive from core-thread cache observers.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use alloy::primitives::Address;
use nautilus_common::{cache::fifo::FifoCacheMap, messages::ExecutionReport};
use nautilus_core::{AtomicMap, time::AtomicTime};
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::{
    identifiers::{AccountId, InstrumentId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    types::Quantity,
};
use tokio_util::sync::CancellationToken;

use crate::{
    common::instrument_id_from_outcome,
    websocket::{
        PredictWalletWebSocketClient,
        private_messages::{PredictWalletOutcome, PredictWalletStreamMessage},
    },
};

struct HealthGuard(Arc<AtomicBool>);
impl Drop for HealthGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

pub(super) struct WalletReportContext {
    pub instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    pub orders: Arc<AtomicMap<VenueOrderId, Arc<OrderAny>>>,
    pub pending: super::orders::PendingSubmissions,
    pub rest_ids: super::routing::RestOrderIds,
    pub sync: Arc<super::account::AccountSync>,
    pub fill_evidence: super::precision::SharedFillEvidence,
    pub reconciliation_required: Arc<AtomicBool>,
    pub allow_share_fees: bool,
    pub emitter: ExecutionEventEmitter,
    pub account: AccountId,
    pub wallet: Address,
    pub clock: &'static AtomicTime,
}

impl WalletReportContext {
    pub(super) fn order_snapshot(
        &self,
        id: VenueOrderId,
    ) -> anyhow::Result<(Arc<OrderAny>, Vec<super::precision::BufferedFill>)> {
        // Native's observer publishes the order view and clears queued-fill markers
        // under this same lock. Mixing their revisions loses a raw precision prefix.
        let _state = self.sync.state.lock().expect("account sync poisoned");
        let order = self.orders.get_cloned(&id).ok_or_else(|| {
            anyhow::anyhow!("Predict wallet order is absent from the Nautilus cache context")
        })?;
        let queued = self
            .fill_evidence
            .lock()
            .map_err(|_| anyhow::anyhow!("Predict fill evidence poisoned"))?
            .pending_for(&order, id);
        Ok((order, queued))
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
            .reconciliation_required
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
    let WalletReportContext {
        instruments,
        orders: _,
        pending,
        rest_ids,
        sync: _,
        fill_evidence,
        reconciliation_required: _,
        allow_share_fees,
        emitter,
        account,
        wallet,
        clock,
    } = context;
    // Facts queued to the native engine can precede its cache observer, just like confirmed fills.
    let mut acceptances: FifoCacheMap<VenueOrderId, nautilus_core::UnixNanos, 10000> =
        FifoCacheMap::new();
    while let Some(message) = ws.next_message().await? {
        match message {
            PredictWalletStreamMessage::Subscribed {
                connection_epoch: 0,
                ..
            } => {}
            PredictWalletStreamMessage::Subscribed { .. } => {
                context
                    .reconciliation_required
                    .store(true, Ordering::Release);
                log::info!(
                    "Predict wallet resubscribed; submissions remain gated pending REST reconciliation"
                );
            }
            PredictWalletStreamMessage::Wallet { event, .. } => {
                let result = (|| -> anyhow::Result<()> {
                    let id = VenueOrderId::new(format!("{:#x}", event.order.order_hash));
                    let (order, queued) = context.order_snapshot(id)?;
                    let outcome = match event.order.details.outcome {
                        PredictWalletOutcome::Yes => 1,
                        PredictWalletOutcome::No => 2,
                    };
                    let instrument_id =
                        instrument_id_from_outcome(event.order.details.market_id, outcome)?;
                    let Some(InstrumentAny::BinaryOption(instrument)) =
                        instruments.get_cloned(&instrument_id)
                    else {
                        anyhow::bail!(
                            "Predict wallet instrument is absent from the Nautilus cache context"
                        );
                    };
                    let mut confirmed = order.filled_qty().as_decimal();
                    for fill in &queued {
                        confirmed = confirmed
                            .checked_add(fill.report.last_qty.as_decimal())
                            .ok_or_else(|| {
                                anyhow::anyhow!("Predict confirmed fill aggregate overflow")
                            })?;
                    }
                    let confirmed = Quantity::from_decimal_dp(
                        confirmed,
                        instrument.size_precision(),
                    )
                    .map_err(|_| {
                        anyhow::anyhow!("Predict confirmed fill aggregate exceeds Nautilus range")
                    })?;
                    let registered = pending
                        .lock()
                        .expect("submission registry poisoned")
                        .entries
                        .get(&order.client_order_id())
                        .copied()
                        .flatten()
                        .filter(|hash| *hash == id);
                    let historical_ack = matches!(
                        event.kind,
                        crate::websocket::private_messages::PredictWalletEventKind::OrderAccepted
                    ) && order.ts_accepted().is_some()
                        && event.order.timestamp_ns()? <= order.ts_last()
                        && (event.order.details.quantity_filled < confirmed.as_decimal()
                            || order.is_closed()
                                && order.status() != nautilus_model::enums::OrderStatus::Filled
                                && event.order.details.quantity_filled <= confirmed.as_decimal());
                    if !historical_ack && !matches!(event.kind,crate::websocket::private_messages::PredictWalletEventKind::OrderTransactionSuccess{..}
                    | crate::websocket::private_messages::PredictWalletEventKind::OrderTransactionSubmitted{..}
                    | crate::websocket::private_messages::PredictWalletEventKind::OrderMarketMatchPlan{..}
                    | crate::websocket::private_messages::PredictWalletEventKind::OrderTransactionFailed{..}) {
                    let facts=fill_evidence.lock().map_err(|_|anyhow::anyhow!("Predict fill evidence poisoned"))?;
                    if let Some(raw)=facts.confirmed_raw(&order,&queued)? {
                        anyhow::ensure!(crate::common::parse::exact_wei(raw)?==event.order.details.quantity_filled,
                            "Predict lifecycle includes unresolved raw cumulative fills");
                    } else {
                        anyhow::ensure!(event.order.details.quantity_filled==confirmed.as_decimal(),
                            "Predict lifecycle lacks raw settlement prefix evidence");
                    }
                }
                    let Some(report) = super::parse::parse_wallet_report_with_registration(
                        &event,
                        &order,
                        &instrument,
                        *account,
                        *wallet,
                        clock.get_time_ns(),
                        super::parse::WalletReportEvidence {
                            confirmed_qty: confirmed,
                            registered,
                            queued_acceptance: acceptances.get(&id).copied(),
                            normalize: true,
                            allow_share_fees: *allow_share_fees,
                        },
                    )?
                    else {
                        return Ok(());
                    };
                    let rest_id = super::routing::parse_id(&event.order.order_id)?;
                    super::routing::register(rest_ids, id, rest_id)?;
                    if let ExecutionReport::Fill(native) = report {
                        let mut fill =
                            super::precision::BufferedFill::from_wallet(*native, &event)?;
                        let mut evidence = fill_evidence
                            .lock()
                            .map_err(|_| anyhow::anyhow!("Predict fill evidence poisoned"))?;
                        evidence.validate_wallet_timestamp(&fill)?;
                        let current = std::iter::once((id, (*order).clone())).collect();
                        evidence.prepare(
                            std::slice::from_mut(&mut fill),
                            &current,
                            &queued,
                            |_| instrument.size_precision(),
                        )?;
                        if super::parse::validate_applied_fill(&fill.report, &order)? {
                            return Ok(());
                        }
                        if queued
                            .iter()
                            .any(|old| old.report.trade_id == fill.report.trade_id)
                        {
                            return Ok(());
                        }
                        anyhow::ensure!(
                            fill.report
                                .last_qty
                                .as_decimal()
                                .checked_add(confirmed.as_decimal())
                                .is_some_and(|total| total <= order.quantity().as_decimal()),
                            "Predict confirmed fills exceed cached order quantity"
                        );
                        emitter.try_send_execution_report(ExecutionReport::Fill(Box::new(
                            fill.report.clone(),
                        )))?;
                        evidence.queue_wallet(&fill)?;
                    } else {
                        let accepted = if let ExecutionReport::Order(status) = &report {
                            (status.ts_accepted.as_u64() > 0).then_some(status.ts_accepted)
                        } else {
                            None
                        };
                        emitter.try_send_execution_report(report)?;
                        if let Some(accepted) = accepted
                            && acceptances.get(&id).is_none()
                        {
                            acceptances.insert(id, accepted);
                        }
                    }
                    Ok(())
                })();
                if let Err(error) = result {
                    // Missing external context and contradictory evidence gate submissions,
                    // but never kill the consumer needed to reconcile subsequent own events.
                    let mut state = context.sync.state.lock().expect("account sync poisoned");
                    state.revision = state.revision.wrapping_add(1);
                    context
                        .reconciliation_required
                        .store(true, Ordering::Release);
                    log::warn!("Predict wallet event requires reconciliation: {error}");
                }
            }
        }
    }
    anyhow::bail!("Predict wallet execution stream closed")
}
