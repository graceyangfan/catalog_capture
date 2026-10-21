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
    /// Full history is required, including previously applied trade IDs. REST amountFilled
    /// has undocumented units; only its zero/nonzero property is used, never a conversion.
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
        let mut qty = Decimal::ZERO;
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
            qty = qty
                .checked_add(fill.report.last_qty.as_decimal())
                .ok_or_else(|| anyhow::anyhow!("Predict recovery quantity overflow"))?;
        }
        order.for_each_trade_id(|id| {
            anyhow::ensure!(
                ids.contains(&id),
                "Predict recovery history omits an applied trade"
            );
            Ok(())
        })?;
        let full = raw == decimal_to_wei(order.quantity().as_decimal())?;
        anyhow::ensure!(
            raw <= decimal_to_wei(order.quantity().as_decimal())?
                && qty >= order.filled_qty().as_decimal(),
            "Predict recovery history contradicts cumulative native fills"
        );
        let status = match row.status {
            PredictOrderStatus::Open if !full => OrderStatus::PartiallyFilled,
            PredictOrderStatus::Filled if full => OrderStatus::Filled,
            PredictOrderStatus::Cancelled => OrderStatus::Canceled,
            PredictOrderStatus::Expired => OrderStatus::Expired,
            _ => anyhow::bail!("Predict recovery status lacks matching settlement evidence"),
        };
        anyhow::ensure!(
            !order.is_closed() || order.status() == status,
            "Predict recovery cannot change a terminal native status"
        );
        // Venue milliseconds can round below a local nanosecond submission.
        // Ownership and signed terms above establish association, not clock ordering.
        anyhow::ensure!(
            earliest <= observed,
            "Predict recovery execution is after observation"
        );
        let accepted = order.ts_accepted().unwrap_or(earliest);
        let mut report = OrderStatusReport::new(
            self.account_id,
            self.instrument.id(),
            Some(order.client_order_id()),
            self.venue_id,
            Some(order.order_side()),
            order.order_type(),
            order.time_in_force(),
            status,
            order.quantity(),
            Quantity::from_decimal_dp(qty, self.instrument.size_precision())?,
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
