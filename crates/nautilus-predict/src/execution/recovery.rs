// Copyright (C) 2026 yfclark and contributors. Licensed under LGPL-3.0.
//! Targeted order/trade evidence bundled for native reconciliation, never an OMS.
use alloy::primitives::{Address, U256};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::OrderStatus,
    identifiers::{AccountId, VenueOrderId},
    instruments::{BinaryOption, Instrument},
    orders::OrderAny,
    reports::OrderStatusReport,
    types::Quantity,
};
use rust_decimal::Decimal;

use super::precision::BufferedFill;
use crate::{
    common::{contracts::PredictExchangeType, parse::decimal_to_wei},
    http::account_models::{PredictOrderData, PredictOrderStatus},
    signing::PredictOrderHasher,
};

pub(super) struct OrderRecovery<'a, O = OrderAny> {
    pub order: &'a O,
    pub instrument: &'a BinaryOption,
    pub venue_id: VenueOrderId,
    pub account_id: AccountId,
    pub wallet: Address,
    pub hasher: &'a PredictOrderHasher,
}

impl<O: super::context::OrderView> OrderRecovery<'_, O> {
    /// Native already owns applied trades. Add only unseen confirmed executions;
    /// complete raw history is needed only to normalize the final working quantity.
    pub fn report(
        &self,
        row: &PredictOrderData,
        fills: &[&BufferedFill],
        observed: UnixNanos,
    ) -> anyhow::Result<OrderStatusReport> {
        let order = self.order;
        let hash = self.hasher.order_hash(
            &row.order.fields,
            PredictExchangeType::from_market_flags(row.is_neg_risk, row.is_yield_bearing),
        );
        anyhow::ensure!(
            self.venue_id == VenueOrderId::new(format!("{hash:#x}")),
            "Predict recovery contract fields do not reproduce the transmitted hash"
        );
        super::parse::validate_rest_order_terms_for_hash(
            row,
            order,
            self.instrument,
            self.account_id,
            self.wallet,
            self.venue_id,
        )?;
        anyhow::ensure!(
            order.venue_order_id() == Some(self.venue_id)
                || matches!(
                    order.status(),
                    OrderStatus::Submitted | OrderStatus::PendingCancel
                ) && order.ts_submitted().is_some()
                    && order.ts_accepted().is_none()
                    && order.venue_order_id().is_none(),
            "Predict recovery lost native order ownership"
        );
        let venue_amount = Decimal::from_str_exact(&row.amount_filled)?;
        anyhow::ensure!(
            venue_amount > Decimal::ZERO && !fills.is_empty(),
            "Predict recovery lacks nonzero match evidence"
        );
        let mut raw = U256::ZERO;
        let mut qty = order.filled_qty().as_decimal();
        let mut ids = std::collections::HashSet::with_capacity(fills.len());
        let mut earliest = fills[0].report.ts_event;
        for fill in fills {
            ids.insert(fill.report.trade_id);
            earliest = earliest.min(fill.report.ts_event);
            anyhow::ensure!(
                fill.report.client_order_id == Some(order.client_order_id())
                    && fill.report.venue_order_id == self.venue_id
                    && fill.report.instrument_id == order.instrument_id(),
                "Predict recovery match belongs to a different order"
            );
            raw = raw
                .checked_add(fill.raw.gross_shares)
                .ok_or_else(|| anyhow::anyhow!("Predict recovery raw sum overflow"))?;
            if !order.has_applied_fill(fill.report.trade_id) {
                qty = qty
                    .checked_add(fill.report.last_qty.as_decimal())
                    .ok_or_else(|| anyhow::anyhow!("Predict recovery quantity overflow"))?;
            }
        }
        let mut complete_history = true;
        order.for_each_trade_id(|id| {
            complete_history &= ids.contains(&id);
            Ok(())
        })?;
        let submitted = decimal_to_wei(order.submitted_quantity().as_decimal())?;
        anyhow::ensure!(
            raw <= submitted && qty <= order.submitted_quantity().as_decimal(),
            "Predict recovery history contradicts cumulative native fills"
        );
        let status = match row.status {
            // The order row is sampled before trade history. A wallet terminal
            // arriving during that IO supersedes OPEN, not the real executions
            // collected with it. Preserve closure while delivering those fills.
            PredictOrderStatus::Open
                if matches!(order.status(), OrderStatus::Canceled | OrderStatus::Expired) =>
            {
                order.status()
            }
            PredictOrderStatus::Open => OrderStatus::PartiallyFilled,
            PredictOrderStatus::Filled => OrderStatus::Filled,
            PredictOrderStatus::Cancelled => OrderStatus::Canceled,
            PredictOrderStatus::Expired => OrderStatus::Expired,
            _ => anyhow::bail!("Predict recovery status lacks matching settlement evidence"),
        };
        anyhow::ensure!(
            !order.is_closed()
                || order.status() == status
                || status == OrderStatus::Filled
                    && matches!(order.status(), OrderStatus::Canceled | OrderStatus::Expired),
            "Predict recovery terminal conflicts with confirmed native execution state"
        );
        // Venue milliseconds can round below a local nanosecond submission.
        // Ownership and signed terms above establish association, not clock ordering.
        let accepted = order.ts_accepted().unwrap_or(earliest);
        // Preserve atomic allocations, including SHARES fees. Rounding this sum to
        // instrument precision could manufacture an incremental inferred fill.
        let precision = fills.iter().fold(
            self.instrument
                .size_precision()
                .max(order.filled_qty().precision),
            |precision, fill| precision.max(fill.report.last_qty.precision),
        );
        let filled_quantity = Quantity::from_decimal_dp(qty, precision)?;
        // Follow Polymarket's terminal report normalization: the venue terminal
        // stays FILLED, real fills stay unchanged, and Native closes via its
        // reconciliation OrderUpdated. Original signed quantity stays in Native
        // initialization/conversion history for replay validation.
        // Where this row explicitly uses signed-share wei for `amount`, its
        // cumulative provides an additional fence against lagging match history.
        // Other endpoint amount models are not converted or guessed here.
        let share_cumulative = row.amount.parse::<U256>().ok() == Some(submitted);
        let complete_share_cumulative =
            share_cumulative && row.amount_filled.parse::<U256>().ok() == Some(raw);
        let complete_terminal_quantity =
            complete_history && (raw == submitted || complete_share_cumulative);
        let report_quantity = if status == OrderStatus::Filled && complete_terminal_quantity {
            filled_quantity
        } else {
            order.quantity()
        };
        let mut report = OrderStatusReport::new(
            self.account_id,
            self.instrument.id(),
            Some(order.client_order_id()),
            self.venue_id,
            Some(order.order_side()),
            order.order_type(),
            order.time_in_force(),
            status,
            report_quantity,
            filled_quantity,
            accepted,
            observed,
            observed,
            None,
        )
        .with_post_only(order.is_post_only())
        .with_reduce_only(order.is_reduce_only());
        if let Some(px) = order.price() {
            report = report.with_price(px);
        }
        report.expire_time = order.expire_time();
        Ok(report)
    }
}
