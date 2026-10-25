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

//! Predict orderbook custom data and exact wire-to-catalog conversion.

use std::sync::Arc;

use nautilus_core::UnixNanos;
use nautilus_model::{
    custom_data,
    data::CustomData,
    identifiers::InstrumentId,
    types::{Price, Quantity},
};
use nautilus_serialization::arrow_custom_data;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::common::{PredictMarketContext, predict_orderbook_data_type};
pub use crate::websocket::messages::{
    PredictLastOrderSettledWire, PredictOrderbookWire, PredictSettlementsPendingWire,
};

/// One complete native Predict price level.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredictOrderbookLevel {
    pub price: Price,
    pub quantity: Quantity,
}

/// The most recent order whose settlement completed for this market.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredictLastOrderSettled {
    pub id: String,
    pub price: Price,
    pub kind: String,
    pub market_id: u64,
    pub side: String,
    pub outcome: String,
}

/// One venue-provided `[price, value]` level whose settlement is in flight.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredictSettlementPendingLevel {
    pub price: Price,
    pub value: Quantity,
}

/// Pending settlement levels separated by the native YES-book side.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredictSettlementsPending {
    pub asks: Vec<PredictSettlementPendingLevel>,
    pub bids: Vec<PredictSettlementPendingLevel>,
}

/// One accepted complete native YES orderbook snapshot.
///
/// `instrument_id` identifies the tradeable YES outcome; custom-data partitioning remains
/// market-scoped via the `DataType` identifier returned by `PredictMarketContext`.
#[arrow_custom_data]
#[custom_data]
pub struct PredictOrderbookSnapshot {
    pub instrument_id: InstrumentId,
    pub market_id: u64,
    pub order_count: u64,
    #[custom_data_field(serde)]
    pub bids: Vec<PredictOrderbookLevel>,
    #[custom_data_field(serde)]
    pub asks: Vec<PredictOrderbookLevel>,
    #[custom_data_field(serde)]
    pub last_order_settled: Option<PredictLastOrderSettled>,
    #[custom_data_field(serde)]
    pub settlements_pending: Option<PredictSettlementsPending>,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

impl PredictOrderbookSnapshot {
    /// Builds a snapshot only when the wire market identity agrees with the loaded context.
    pub fn from_wire(
        context: &PredictMarketContext,
        wire: PredictOrderbookWire,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            wire.market_id == context.market_id,
            "Predict orderbook marketId {} does not match context {}",
            wire.market_id,
            context.market_id
        );
        let bids = normalize_levels(wire.bids, context)?;
        let asks = normalize_levels(wire.asks, context)?;
        let last_order_settled = wire
            .last_order_settled
            .map(|order| normalize_last_order_settled(order, context))
            .transpose()?;
        let settlements_pending = wire
            .settlements_pending
            .map(|pending| normalize_settlements_pending(pending, context))
            .transpose()?;
        let ts_event = UnixNanos::from_millis(wire.update_timestamp_ms);

        Ok(Self {
            instrument_id: context.yes_instrument_id,
            market_id: context.market_id,
            order_count: wire.order_count,
            bids,
            asks,
            last_order_settled,
            settlements_pending,
            ts_event,
            ts_init,
        })
    }

    /// Wraps this snapshot with its canonical market-scoped `DataType`.
    ///
    /// The type identity derives from the snapshot's validated `market_id`; callers cannot
    /// accidentally write it under another market's catalog partition.
    #[must_use]
    pub fn into_custom_data(self) -> CustomData {
        let market_id = self.market_id;
        CustomData::new(Arc::new(self), predict_orderbook_data_type(market_id))
    }

    /// Wraps the snapshot with a caller-owned canonical market-scoped identity.
    #[must_use]
    pub fn into_custom_data_with_type(
        self,
        data_type: nautilus_model::data::DataType,
    ) -> CustomData {
        debug_assert_eq!(data_type.type_name(), "PredictOrderbookSnapshot");
        debug_assert_eq!(
            data_type.identifier(),
            Some(self.market_id.to_string().as_str())
        );
        CustomData::new(Arc::new(self), data_type)
    }
}

fn normalize_last_order_settled(
    order: PredictLastOrderSettledWire,
    context: &PredictMarketContext,
) -> anyhow::Result<PredictLastOrderSettled> {
    anyhow::ensure!(
        order.market_id == context.market_id,
        "Predict lastOrderSettled marketId {} does not match context {}",
        order.market_id,
        context.market_id
    );
    let price = normalize_price(order.price, context, "lastOrderSettled price")?;
    Ok(PredictLastOrderSettled {
        id: order.id,
        price,
        kind: order.kind,
        market_id: order.market_id,
        side: order.side,
        outcome: order.outcome,
    })
}

fn normalize_settlements_pending(
    pending: PredictSettlementsPendingWire,
    context: &PredictMarketContext,
) -> anyhow::Result<PredictSettlementsPending> {
    Ok(PredictSettlementsPending {
        asks: normalize_pending_levels(pending.asks, context)?,
        bids: normalize_pending_levels(pending.bids, context)?,
    })
}

fn normalize_pending_levels(
    levels: Vec<(Decimal, Decimal)>,
    context: &PredictMarketContext,
) -> anyhow::Result<Vec<PredictSettlementPendingLevel>> {
    levels
        .into_iter()
        .map(|(price, value)| {
            let price = normalize_price(price, context, "settlementsPending price")?;
            anyhow::ensure!(
                value >= Decimal::ZERO,
                "Predict settlementsPending value {value} is negative"
            );
            let value = normalize_level_value(
                value,
                context.size_precision,
                "settlementsPending value",
                true,
            )?;
            Ok(PredictSettlementPendingLevel {
                price,
                value: Quantity::from_decimal_dp(value, context.size_precision)?,
            })
        })
        .collect()
}

fn normalize_price(
    price: Decimal,
    context: &PredictMarketContext,
    field: &str,
) -> anyhow::Result<Price> {
    anyhow::ensure!(
        price >= Decimal::ZERO && price <= Decimal::ONE,
        "Predict {field} {price} is outside [0, 1]"
    );
    let price = normalize_level_value(price, context.price_precision, field, false)?;
    Ok(Price::from_decimal_dp(price, context.price_precision)?)
}

/// Registers the Arrow and JSON decoders required before writing or querying this custom type.
///
/// Registration is idempotent.
pub fn register_predict_custom_data() {
    nautilus_serialization::ensure_custom_data_registered::<PredictOrderbookSnapshot>();
    #[cfg(feature = "execution")]
    let _ = nautilus_model::data::ensure_custom_data_json_registered::<
        crate::PredictSettlementUpdate,
    >();
    #[cfg(feature = "execution")]
    let _ = nautilus_model::data::ensure_custom_data_json_registered::<
        crate::PredictLimitOrderReceiptUpdate,
    >();
    #[cfg(feature = "execution")]
    let _ = nautilus_model::data::ensure_custom_data_json_registered::<
        crate::PredictOrderRejectionUpdate,
    >();
}

fn normalize_levels(
    levels: Vec<(Decimal, Decimal)>,
    context: &PredictMarketContext,
) -> anyhow::Result<Vec<PredictOrderbookLevel>> {
    levels
        .into_iter()
        .map(|(price, quantity)| {
            anyhow::ensure!(
                quantity >= Decimal::ZERO,
                "Predict orderbook quantity {quantity} is negative"
            );
            let price = normalize_price(price, context, "orderbook price")?;
            let quantity =
                normalize_level_value(quantity, context.size_precision, "quantity", true)?;
            Ok(PredictOrderbookLevel {
                price,
                quantity: Quantity::from_decimal_dp(quantity, context.size_precision)?,
            })
        })
        .collect()
}

fn normalize_level_value(
    value: Decimal,
    precision: u8,
    field: &str,
    allow_rounding: bool,
) -> anyhow::Result<Decimal> {
    let normalized = value.round_dp(u32::from(precision));
    anyhow::ensure!(
        allow_rounding || normalized == value,
        "Predict orderbook {field} {value} exceeds declared precision {precision}"
    );
    Ok(normalized)
}
