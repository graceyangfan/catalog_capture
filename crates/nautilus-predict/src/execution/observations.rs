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

//! Account/strategy-scoped venue observations; Native events remain order and position authority.

use std::sync::Arc;

use nautilus_core::UnixNanos;
use nautilus_model::{
    custom_data,
    data::{CustomData, DataType},
    enums::OrderStatus,
    identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId, TradeId, VenueOrderId},
};
use serde::{Deserialize, Serialize};

use crate::http::command_models::PredictSelfTradePrevention;

fn scoped_data_type(name: &'static str, account: AccountId, strategy: StrategyId) -> DataType {
    // The length prefix keeps adjacent account and strategy IDs unambiguous.
    DataType::new(
        name,
        None,
        Some(format!(
            "{}:{}{}",
            account.as_str().len(),
            account,
            strategy
        )),
    )
}

/// Authenticated create receipt. Subscribe by account/strategy, as for settlements.
/// None for removal_locked_until means unknown; it does not establish removability.
/// This event can arrive before or after the Native Accepted/terminal callback.
#[custom_data]
pub struct PredictLimitOrderReceiptUpdate {
    pub account_id: AccountId,
    pub strategy_id: StrategyId,
    pub instrument_id: InstrumentId,
    pub client_order_id: ClientOrderId,
    pub venue_order_id: VenueOrderId,
    pub is_post_only: bool,
    #[custom_data_field(serde)]
    pub self_trade_prevention: Option<PredictSelfTradePrevention>,
    #[custom_data_field(serde)]
    pub removal_locked_until: Option<UnixNanos>,
    pub connection_epoch: u64,
    /// Local receipt observation time; no venue acceptance timestamp is inferred.
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

/// Returns the account/strategy data type for limit-order receipt observations.
#[must_use]
pub fn predict_limit_order_receipt_data_type(account: AccountId, strategy: StrategyId) -> DataType {
    scoped_data_type("PredictLimitOrderReceiptUpdate", account, strategy)
}

impl PredictLimitOrderReceiptUpdate {
    /// Wraps this receipt using its account/strategy data type.
    #[must_use]
    pub fn into_custom_data(self) -> CustomData {
        let data_type = predict_limit_order_receipt_data_type(self.account_id, self.strategy_id);
        CustomData::new(Arc::new(self), data_type)
    }
}

/// Immediate observation, not a Native rejection/cancellation or an economic execution.
/// Known reasons retain their wire spelling; unrecognized reasons are classified as `unknown`.
/// Use ordinary Native queries/reconciliation to determine the original order's status.
#[custom_data]
pub struct PredictOrderNotAcceptedUpdate {
    pub account_id: AccountId,
    pub strategy_id: StrategyId,
    pub instrument_id: InstrumentId,
    pub client_order_id: ClientOrderId,
    pub venue_order_id: VenueOrderId,
    pub reason: String,
    pub reported_quantity_filled: String,
    pub native_filled_quantity: String,
    #[custom_data_field(serde)]
    pub native_status: OrderStatus,
    pub connection_epoch: u64,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

/// Account/strategy-scoped local metadata; no additional venue subscription.
#[must_use]
pub fn predict_order_not_accepted_data_type(account: AccountId, strategy: StrategyId) -> DataType {
    scoped_data_type("PredictOrderNotAcceptedUpdate", account, strategy)
}

impl PredictOrderNotAcceptedUpdate {
    /// Wraps this observation using its account/strategy data type.
    #[must_use]
    pub fn into_custom_data(self) -> CustomData {
        let data_type = predict_order_not_accepted_data_type(self.account_id, self.strategy_id);
        CustomData::new(Arc::new(self), data_type)
    }
}

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
    scoped_data_type("PredictSettlementUpdate", account, strategy)
}

impl PredictSettlementUpdate {
    /// Wraps this settlement using its account/strategy data type.
    #[must_use]
    pub fn into_custom_data(self) -> CustomData {
        let data_type = predict_settlement_data_type(self.account_id, self.strategy_id);
        CustomData::new(Arc::new(self), data_type)
    }
}
