// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! LIMIT transport metadata, never Native acceptance or an order-state registry.
use std::sync::Arc;

use nautilus_core::UnixNanos;
use nautilus_model::{
    custom_data,
    data::{CustomData, DataType},
    identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId, VenueOrderId},
};

use crate::http::command_models::PredictSelfTradePrevention;

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

#[must_use]
pub fn predict_limit_order_receipt_data_type(account: AccountId, strategy: StrategyId) -> DataType {
    DataType::new(
        "PredictLimitOrderReceiptUpdate",
        None,
        Some(format!(
            "{}:{}{}",
            account.as_str().len(),
            account,
            strategy
        )),
    )
}

impl PredictLimitOrderReceiptUpdate {
    #[must_use]
    pub fn into_custom_data(self) -> CustomData {
        let data_type = predict_limit_order_receipt_data_type(self.account_id, self.strategy_id);
        CustomData::new(Arc::new(self), data_type)
    }
}
