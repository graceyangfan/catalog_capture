// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Pure wire-to-Nautilus conversions; the live session will own continuity and admission.

use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{
        BookOrder, IndexPriceUpdate, MarkPriceUpdate, OrderBookDelta, OrderBookDeltas, TradeTick,
    },
    enums::{AggressorSide, BookAction, OrderSide, RecordFlag},
    identifiers::TradeId,
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use rust_decimal::Decimal;

use super::messages::{BulletAggTrade, BulletDepthUpdate, BulletMarkPrice, BulletMessageType};

/// Convert microseconds without a floating-point intermediate or an overflow fallback.
pub fn micros_to_nanos(value: u64) -> anyhow::Result<UnixNanos> {
    value
        .checked_mul(1_000)
        .map(UnixNanos::from)
        .context("Bullet microsecond timestamp exceeds UnixNanos range")
}

fn validate_instrument(symbol: &str, instrument: &InstrumentAny) -> anyhow::Result<()> {
    anyhow::ensure!(
        instrument.id().venue.as_str() == "BULLET",
        "instrument is not on BULLET"
    );
    anyhow::ensure!(
        symbol == instrument.raw_symbol().as_str(),
        "Bullet symbol `{symbol}` does not match instrument {}",
        instrument.id(),
    );
    Ok(())
}

fn exact_decimal(value: &str, precision: u8) -> anyhow::Result<Decimal> {
    let decimal = Decimal::from_str_exact(value).context("invalid Bullet decimal string")?;
    anyhow::ensure!(
        decimal.normalize().scale() <= u32::from(precision),
        "Bullet value `{value}` exceeds instrument precision {precision}",
    );
    Ok(decimal)
}

fn parse_price(value: &str, instrument: &InstrumentAny) -> anyhow::Result<Price> {
    let precision = instrument.price_precision();
    Price::from_decimal_dp(exact_decimal(value, precision)?, precision)
        .context("invalid Bullet price")
}

fn parse_quantity(value: &str, instrument: &InstrumentAny) -> anyhow::Result<Quantity> {
    let precision = instrument.size_precision();
    Quantity::from_decimal_dp(exact_decimal(value, precision)?, precision)
        .context("invalid Bullet quantity")
}

/// Convert a complete frame atomically, preserving the venue update ID as Native sequence.
///
/// The caller must validate the current epoch, subscription and `pu` chain before publication.
/// An empty update emits no batch, but its venue sequence still belongs to the live chain.
pub fn parse_book(
    message: &BulletDepthUpdate,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<OrderBookDeltas>> {
    validate_instrument(&message.symbol, instrument)?;
    anyhow::ensure!(
        message.first_update_id == message.last_update_id,
        "Bullet depth U != u"
    );
    let sequence = message.last_update_id;
    let ts_event = micros_to_nanos(message.transaction_time)?;
    let snapshot = message.message_type == BulletMessageType::Snapshot;
    anyhow::ensure!(
        !snapshot
            || (message.bids.len() <= crate::common::BULLET_BOOK_DEPTH
                && message.asks.len() <= crate::common::BULLET_BOOK_DEPTH),
        "Bullet depth20 snapshot exceeds subscribed depth"
    );
    if !snapshot && message.bids.is_empty() && message.asks.is_empty() {
        return Ok(None);
    }
    let mut deltas =
        Vec::with_capacity(message.bids.len() + message.asks.len() + usize::from(snapshot));
    if snapshot {
        let mut clear = OrderBookDelta::clear(instrument.id(), sequence, ts_event, ts_init);
        clear.flags |= RecordFlag::F_SNAPSHOT as u8;
        deltas.push(clear);
    }
    for (side, levels) in [
        (OrderSide::Buy, &message.bids),
        (OrderSide::Sell, &message.asks),
    ] {
        for level in levels {
            let price = parse_price(&level.0, instrument)?;
            let size = parse_quantity(&level.1, instrument)?;
            anyhow::ensure!(
                !snapshot || !size.is_zero(),
                "zero Bullet snapshot quantity"
            );
            let action = if snapshot {
                BookAction::Add
            } else if size.is_zero() {
                BookAction::Delete
            } else {
                BookAction::Update
            };
            let flags = if snapshot {
                RecordFlag::F_SNAPSHOT as u8
            } else {
                0
            };
            deltas.push(OrderBookDelta::new_checked(
                instrument.id(),
                action,
                BookOrder::new(side, price, size, 0),
                flags,
                sequence,
                ts_event,
                ts_init,
            )?);
        }
    }
    deltas.last_mut().context("empty Bullet book batch")?.flags |= RecordFlag::F_LAST as u8;
    OrderBookDeltas::new_checked(instrument.id(), deltas)
        .map(Some)
        .context("invalid Bullet book batch")
}

/// Preserve aggregate trade identity and infer aggressor side from buyer-maker status.
pub fn parse_trade(
    message: &BulletAggTrade,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<TradeTick> {
    validate_instrument(&message.symbol, instrument)?;
    anyhow::ensure!(
        message.first_trade_id <= message.last_trade_id,
        "Bullet trade f > l"
    );
    TradeTick::new_checked(
        instrument.id(),
        parse_price(&message.price, instrument)?,
        parse_quantity(&message.quantity, instrument)?,
        if message.is_buyer_maker {
            AggressorSide::Sell
        } else {
            AggressorSide::Buy
        },
        TradeId::new_checked(message.agg_trade_id.to_string())?,
        micros_to_nanos(message.trade_time)?,
        ts_init,
    )
    .context("invalid Bullet trade tick")
}

/// A single mark publication carries both canonical price families.
#[derive(Debug)]
pub struct BulletPrices {
    pub mark: MarkPriceUpdate,
    pub index: IndexPriceUpdate,
}

/// Use publication time for both current-price updates; execution identity stays on the wire model.
///
/// The session must only publish each family while it has a logical subscriber. The shared
/// physical topic must survive until the final mark/index consumer unsubscribes.
pub fn parse_prices(
    message: &BulletMarkPrice,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<BulletPrices> {
    validate_instrument(&message.symbol, instrument)?;
    let ts_event = micros_to_nanos(message.event_time)?;
    Ok(BulletPrices {
        mark: MarkPriceUpdate::new(
            instrument.id(),
            Price::from_decimal_dp(
                Decimal::from_str_exact(&message.mark_price)?,
                instrument.price_precision(),
            )?,
            ts_event,
            ts_init,
        ),
        index: IndexPriceUpdate::new(
            instrument.id(),
            Price::from_decimal_dp(
                Decimal::from_str_exact(&message.index_price)?,
                instrument.price_precision(),
            )?,
            ts_event,
            ts_init,
        ),
    })
}
