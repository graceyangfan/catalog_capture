// -------------------------------------------------------------------------------------------------
// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
// See https://www.gnu.org/licenses/lgpl-3.0.html for license details.
// -------------------------------------------------------------------------------------------------
//! Venue-only reports. Native reconciliation creates orders and assigns external ownership.
use alloy::primitives::U256;
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{OrderSide, OrderStatus, OrderType, TimeInForce},
    identifiers::VenueOrderId,
    instruments::{Instrument, InstrumentAny},
    reports::OrderStatusReport,
    types::{Currency, Quantity},
};
use rust_decimal::Decimal;

use super::{matches::MatchContext, precision::BufferedFill};
use crate::{
    common::{
        contracts::PredictExchangeType,
        enums::PredictOrderSide,
        parse::{exact_wei, floor_wei_quantity},
    },
    http::account_models::{PredictOrderData, PredictOrderStatus, PredictOrderStrategy},
    signing::PredictOrderHasher,
};

pub(super) fn order_report(
    row: &PredictOrderData,
    context: &MatchContext<impl super::context::OrderView>,
    hasher: &PredictOrderHasher,
    fills: &[&BufferedFill],
    observed: UnixNanos,
) -> anyhow::Result<OrderStatusReport> {
    let fields = &row.order.fields;
    let hash = row
        .order
        .hash
        .ok_or_else(|| anyhow::anyhow!("Predict external order lacks hash"))?;
    anyhow::ensure!(
        !hash.is_zero()
            && fields.maker == context.wallet
            && fields.signer == context.wallet
            && fields.taker.is_zero()
            && row.currency == "USDT"
            && fields.fee_rate_bps <= U256::from(10_000)
            && !fields.maker_amount.is_zero()
            && !fields.taker_amount.is_zero(),
        "Predict external order identity or terms are invalid"
    );
    anyhow::ensure!(
        hash == hasher.order_hash(
            fields,
            PredictExchangeType::from_market_flags(row.is_neg_risk, row.is_yield_bearing)
        ),
        "Predict external order fields do not reproduce signed hash"
    );
    // Instrument loading belongs to the provider/DataClient; never invent a binary contract.
    let instrument = context
        .instruments
        .values()
        .find(|i| {
            super::parse::market_id_from_instrument(&i.id()).ok() == Some(row.market_id)
                && i.raw_symbol().as_str().parse::<U256>().ok() == Some(fields.token_id)
        })
        .ok_or_else(|| anyhow::anyhow!("Predict external order instrument is absent"))?;
    anyhow::ensure!(
        matches!(instrument, InstrumentAny::BinaryOption(_))
            && instrument.quote_currency() == Currency::USDT(),
        "Predict external order requires a USDT binary instrument"
    );
    let side = match fields.side {
        PredictOrderSide::Buy => OrderSide::Buy,
        PredictOrderSide::Sell => OrderSide::Sell,
    };
    let (order_type, raw_quantity) = match (row.strategy, side) {
        (PredictOrderStrategy::Limit, OrderSide::Buy) => (OrderType::Limit, fields.taker_amount),
        (PredictOrderStrategy::Limit, OrderSide::Sell) => (OrderType::Limit, fields.maker_amount),
        (PredictOrderStrategy::Market, OrderSide::Sell) => (OrderType::Market, fields.maker_amount),
        _ => anyhow::bail!(
            "Predict external MARKET BUY quantity cannot be inferred from signed minimum output"
        ),
    };
    let id = VenueOrderId::new(format!("{hash:#x}"));
    let quantity = floor_wei_quantity(raw_quantity, instrument.size_precision())?;
    let mut raw_filled = U256::ZERO;
    let mut filled = Decimal::ZERO;
    let mut first = observed;
    for fill in fills {
        anyhow::ensure!(
            fill.report.venue_order_id == id
                && fill.report.instrument_id == instrument.id()
                && fill.report.order_side == side,
            "Predict external match differs from signed order"
        );
        raw_filled = raw_filled
            .checked_add(fill.raw.gross_shares)
            .ok_or_else(|| anyhow::anyhow!("Predict external raw fill overflow"))?;
        filled = filled
            .checked_add(fill.report.last_qty.as_decimal())
            .ok_or_else(|| anyhow::anyhow!("Predict external fill overflow"))?;
        first = first.min(fill.report.ts_event);
    }
    // Supported venue-only kinds require raw cumulative equality, not merely nonzero evidence.
    // Missing partial prefixes must not turn into Native inferred fills during cold recovery.
    let reported_filled = row
        .amount_filled
        .parse::<U256>()
        .map_err(|_| anyhow::anyhow!("Predict external filled amount is not integer wei"))?;
    anyhow::ensure!(
        reported_filled == raw_filled
            && raw_filled <= raw_quantity
            && filled <= quantity.as_decimal(),
        "Predict external cumulative fill lacks consistent matches"
    );
    let full = raw_filled == raw_quantity;
    let status = match row.status {
        PredictOrderStatus::Open if !full && raw_filled.is_zero() => OrderStatus::Accepted,
        PredictOrderStatus::Open if !full => OrderStatus::PartiallyFilled,
        PredictOrderStatus::Filled if full => OrderStatus::Filled,
        PredictOrderStatus::Cancelled => OrderStatus::Canceled,
        PredictOrderStatus::Expired => OrderStatus::Expired,
        _ => anyhow::bail!("Predict external terminal status lacks matching settlement evidence"),
    };
    let expiry: u64 = fields
        .expiration
        .try_into()
        .map_err(|_| anyhow::anyhow!("Predict external expiry exceeds u64"))?;
    let expire_time = if order_type == OrderType::Limit && expiry != 0 {
        Some(UnixNanos::from(
            expiry
                .checked_mul(1_000_000_000)
                .ok_or_else(|| anyhow::anyhow!("Predict external expiry overflow"))?,
        ))
    } else {
        None
    };
    let mut report = OrderStatusReport::new(
        context.account_id,
        instrument.id(),
        None,
        id,
        Some(side),
        order_type,
        if expire_time.is_some() {
            TimeInForce::Gtd
        } else {
            TimeInForce::Gtc
        },
        status,
        quantity,
        Quantity::from_decimal_dp(filled, instrument.size_precision())?,
        first,
        observed,
        observed,
        None,
    );
    report.expire_time = expire_time;
    if order_type == OrderType::Limit {
        let (quote, base) = if side == OrderSide::Buy {
            (fields.maker_amount, fields.taker_amount)
        } else {
            (fields.taker_amount, fields.maker_amount)
        };
        let px = exact_wei(quote)?
            .checked_div(exact_wei(base)?)
            .ok_or_else(|| anyhow::anyhow!("Predict external limit price overflow"))?;
        anyhow::ensure!(px > Decimal::ZERO, "Predict external limit price is zero");
        let price = super::parse::price(px, instrument.price_precision())?;
        anyhow::ensure!(
            fills
                .iter()
                .filter(|f| f.report.venue_order_id == id)
                .all(|f| if side == OrderSide::Buy {
                    f.report.last_px <= price
                } else {
                    f.report.last_px >= price
                }),
            "Predict external fill violates signed limit"
        );
        report = report.with_price(price);
    }
    Ok(report)
}
