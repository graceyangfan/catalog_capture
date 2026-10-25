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

//! Early matching facts through Native custom data, separate from final order/position accounting.
use super::context::WorkerOrder;
use crate::websocket::private_messages::{
    PredictWalletEvent, PredictWalletEventKind, PredictWalletFill, PredictWalletTransactionKind,
};
use alloy::primitives::U256;
use nautilus_core::UnixNanos;
use nautilus_model::{
    custom_data,
    data::{CustomData, DataType},
    identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId, TradeId, VenueOrderId},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// A matching notification does not itself apply a Native fill or change cash/positions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PredictSettlementStatus {
    Submitted,
    Confirmed,
    Failed,
}

/// Authenticated, strategy-scoped settlement facts. Exact integer wei is retained as strings.
/// Subscribe with `subscribe_data(predict_settlement_data_type(account,strategy),None,None)`.
/// Success still produces the ordinary Native Filled event with final fees.
#[custom_data]
pub struct PredictSettlementUpdate {
    pub account_id: AccountId,
    pub strategy_id: StrategyId,
    pub instrument_id: InstrumentId,
    pub client_order_id: ClientOrderId,
    pub venue_order_id: VenueOrderId,
    pub trade_id: TradeId,
    pub settlement_id: String,
    #[custom_data_field(serde)]
    pub status: PredictSettlementStatus,
    pub executed_price_wei: String,
    pub executed_size_wei: String,
    pub executed_value_wei: String,
    pub connection_epoch: u64,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

/// Canonical account/strategy identity for Native topic subscriptions; no venue subscription is sent.
#[must_use]
pub fn predict_settlement_data_type(account: AccountId, strategy: StrategyId) -> DataType {
    // Length prefix prevents ambiguous account/strategy joins.
    DataType::new(
        "PredictSettlementUpdate",
        None,
        Some(format!(
            "{}:{}{}",
            account.as_str().len(),
            account,
            strategy
        )),
    )
}
impl PredictSettlementUpdate {
    #[must_use]
    pub fn into_custom_data(self) -> CustomData {
        let data_type = predict_settlement_data_type(self.account_id, self.strategy_id);
        CustomData::new(Arc::new(self), data_type)
    }
}

#[derive(Clone, Debug)]
struct Notice {
    order: VenueOrderId,
    settlement_id: String,
    fill: PredictWalletFill,
    submitted: bool,
    terminal: Option<PredictSettlementStatus>,
    applied: bool,
}
#[derive(Debug, Default, Clone)]
pub(super) struct SettlementNotifications {
    entries: HashMap<TradeId, Notice>,
    by_order: HashMap<VenueOrderId, HashSet<TradeId>>,
}
impl SettlementNotifications {
    pub fn validate_success(
        &self,
        event: &PredictWalletEvent,
        trade_id: TradeId,
        venue_order_id: VenueOrderId,
    ) -> anyhow::Result<()> {
        if let PredictWalletEventKind::OrderTransactionSuccess {
            settlement_id,
            fill,
            ..
        } = &event.kind
            && let Some(previous) = self.entries.get(&trade_id)
        {
            anyhow::ensure!(
                previous.order == venue_order_id
                    && previous.settlement_id == *settlement_id
                    && previous.fill == *fill
                    && previous.terminal != Some(PredictSettlementStatus::Failed),
                "Predict confirmed settlement contradicts prior matching facts"
            );
        }
        Ok(())
    }
    pub fn retire_orders(&mut self, ids: &HashSet<VenueOrderId>) {
        for owner in ids {
            if let Some(trades) = self.by_order.remove(owner) {
                for id in trades {
                    self.entries.remove(&id);
                }
            }
        }
    }
    pub fn retirement_work(&self, owner: VenueOrderId) -> usize {
        self.by_order.get(&owner).map_or(0, HashSet::len)
    }
    pub fn has_pending(&self, owner: VenueOrderId) -> bool {
        self.by_order.get(&owner).is_some_and(|ids| {
            ids.iter().any(|id| {
                self.entries
                    .get(id)
                    .is_some_and(|entry| !entry.applied && entry.terminal.is_none())
            })
        })
    }
    pub fn applied(&mut self, id: TradeId) {
        if let Some(entry) = self.entries.get_mut(&id) {
            entry.applied = true;
        }
    }
    #[cfg(test)]
    pub fn pending_orders(&self) -> impl Iterator<Item = VenueOrderId> + '_ {
        self.entries
            .values()
            .filter(|entry| !entry.applied && entry.terminal.is_none())
            .map(|entry| entry.order)
    }
    pub fn update(
        &mut self,
        event: &PredictWalletEvent,
        order: &WorkerOrder,
        account: AccountId,
        epoch: u64,
        observed: UnixNanos,
    ) -> anyhow::Result<Option<PredictSettlementUpdate>> {
        let (status, settlement_id, fill) = match &event.kind {
            PredictWalletEventKind::OrderTransactionSubmitted {
                kind,
                settlement_id,
                fill,
            } => {
                anyhow::ensure!(
                    matches!(
                        (kind, order.order_side()),
                        (
                            PredictWalletTransactionKind::Purchase,
                            nautilus_model::enums::OrderSide::Buy
                        ) | (
                            PredictWalletTransactionKind::Sale,
                            nautilus_model::enums::OrderSide::Sell
                        )
                    ),
                    "Predict submitted settlement direction differs from order"
                );
                (PredictSettlementStatus::Submitted, settlement_id, fill)
            }
            PredictWalletEventKind::OrderTransactionSuccess {
                settlement_id,
                fill,
                ..
            } => (PredictSettlementStatus::Confirmed, settlement_id, fill),
            PredictWalletEventKind::OrderTransactionFailed {
                settlement_id,
                fill,
            } => (PredictSettlementStatus::Failed, settlement_id, fill),
            _ => return Ok(None),
        };
        let hash = VenueOrderId::new(format!("{:#x}", event.order.order_hash));
        let id = super::parse::settlement_trade_id(settlement_id, event.order.order_hash);
        anyhow::ensure!(
            !settlement_id.is_empty()
                && !fill.executed_size_wei.is_zero()
                && fill.executed_size_wei
                    <= crate::common::parse::decimal_to_wei(order.quantity().as_decimal())?
                && fill.executed_price_wei <= U256::from(1_000_000_000_000_000_000u64)
                && fill
                    .executed_price_wei
                    .checked_mul(fill.executed_size_wei)
                    .is_some_and(|product| product / U256::from(1_000_000_000_000_000_000u64)
                        == fill.executed_value_wei),
            "Predict settlement notification has inconsistent atomic amounts"
        );
        let applied = order.has_applied_fill(id);
        anyhow::ensure!(
            status != PredictSettlementStatus::Failed || !applied,
            "Predict failed settlement contradicts Native applied fill"
        );
        let timestamp = event.order.timestamp_ns()?;
        let entry = self.entries.entry(id).or_insert_with(|| Notice {
            order: hash,
            settlement_id: settlement_id.clone(),
            fill: fill.clone(),
            submitted: false,
            terminal: None,
            applied,
        });
        anyhow::ensure!(
            entry.order == hash && entry.settlement_id == *settlement_id && entry.fill == *fill,
            "Predict settlement notification raw facts changed"
        );
        self.by_order.entry(hash).or_default().insert(id);
        let publish = match status {
            PredictSettlementStatus::Submitted => {
                let publish = !entry.submitted && entry.terminal.is_none() && !entry.applied;
                entry.submitted = true;
                publish
            }
            terminal => {
                anyhow::ensure!(
                    entry.terminal.is_none_or(|previous| previous == terminal),
                    "Predict settlement terminal status changed"
                );
                let publish = entry.terminal.is_none();
                entry.terminal = Some(terminal);
                publish
            }
        };
        if !publish {
            return Ok(None);
        }
        Ok(Some(PredictSettlementUpdate {
            account_id: account,
            strategy_id: order.strategy_id(),
            instrument_id: order.instrument_id(),
            client_order_id: order.client_order_id(),
            venue_order_id: hash,
            trade_id: id,
            settlement_id: settlement_id.clone(),
            status,
            executed_price_wei: fill.executed_price_wei.to_string(),
            executed_size_wei: fill.executed_size_wei.to_string(),
            executed_value_wei: fill.executed_value_wei.to_string(),
            connection_epoch: epoch,
            ts_event: timestamp,
            ts_init: observed,
        }))
    }
}

#[cfg(test)]
#[path = "../../tests/unit/settlement.rs"]
mod tests;
