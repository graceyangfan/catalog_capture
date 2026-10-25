// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/lgpl-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Rejection observations whose effect on an existing order requires venue status evidence.

use std::sync::Arc;

use nautilus_core::UnixNanos;
use nautilus_model::{
    custom_data,
    data::{CustomData, DataType},
    enums::OrderStatus,
    identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId, VenueOrderId},
};

/// Immediate observation, not a Native rejection/cancellation or an economic execution.
/// Known reasons retain their wire spelling; unrecognized reasons are classified as `unknown`.
/// Use ordinary Native queries/reconciliation to determine the original order's status.
#[custom_data]
pub struct PredictOrderRejectionUpdate {
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
    pub requires_reconciliation: bool,
    pub connection_epoch: u64,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

/// Account/strategy-scoped local metadata; no additional venue subscription.
#[must_use]
pub fn predict_order_rejection_data_type(account: AccountId, strategy: StrategyId) -> DataType {
    DataType::new(
        "PredictOrderRejectionUpdate",
        None,
        Some(format!(
            "{}:{}{}",
            account.as_str().len(),
            account,
            strategy
        )),
    )
}

impl PredictOrderRejectionUpdate {
    #[must_use]
    pub fn into_custom_data(self) -> CustomData {
        let data_type = predict_order_rejection_data_type(self.account_id, self.strategy_id);
        CustomData::new(Arc::new(self), data_type)
    }
}
