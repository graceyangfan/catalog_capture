// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use std::collections::HashMap;

use anyhow::{Context, ensure};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BookOrder, OrderBookDelta, OrderBookDeltas},
    enums::{BookAction, OrderSide, RecordFlag},
    identifiers::InstrumentId,
    types::{Price, Quantity},
};
use rust_decimal::Decimal;
use serde_json::value::RawValue;

#[derive(Debug, Clone, Copy)]
struct Order {
    price: Decimal,
    amount: Decimal,
}

impl Order {
    fn canonical(self, id: u64, delete: bool) -> anyhow::Result<BookOrder> {
        let side = if self.amount.is_sign_positive() {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        let size = if delete {
            Decimal::ZERO
        } else {
            self.amount.abs()
        };
        Ok(BookOrder::new(
            side,
            Price::from_decimal_dp(self.price, 8)?,
            Quantity::from_decimal_dp(size, 8)?,
            id,
        ))
    }
}

#[derive(Debug, Default)]
pub(crate) struct RawBook {
    orders: HashMap<u64, Order>,
}

impl RawBook {
    pub(crate) fn snapshot(
        &mut self,
        rows: &[&RawValue],
        instrument: InstrumentId,
        sequence: u64,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> anyhow::Result<OrderBookDeltas> {
        let mut orders = HashMap::with_capacity(rows.len());
        let mut deltas = Vec::with_capacity(rows.len() + 1);
        deltas.push(OrderBookDelta::clear(
            instrument, sequence, ts_event, ts_init,
        ));
        for row in rows {
            let (id, order) = parse_row(row)?;
            ensure!(
                order.price > Decimal::ZERO,
                "delete in Bitfinex R0 snapshot"
            );
            ensure!(
                orders.insert(id, order).is_none(),
                "duplicate R0 snapshot order {id}"
            );
            deltas.push(OrderBookDelta::new(
                instrument,
                BookAction::Add,
                order.canonical(id, false)?,
                RecordFlag::F_SNAPSHOT as u8,
                sequence,
                ts_event,
                ts_init,
            ));
        }
        deltas.last_mut().expect("snapshot contains CLEAR").flags |= RecordFlag::F_LAST as u8;
        self.orders = orders;
        Ok(OrderBookDeltas::new(instrument, deltas))
    }

    pub(crate) fn update(
        &mut self,
        row: &RawValue,
        instrument: InstrumentId,
        sequence: u64,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> anyhow::Result<OrderBookDeltas> {
        let (id, order) = parse_row(row)?;
        let previous = self.orders.get(&id).copied();
        let mut deltas = Vec::with_capacity(2);
        let delta = |action, order, flags| {
            OrderBookDelta::new(
                instrument, action, order, flags, sequence, ts_event, ts_init,
            )
        };
        let last = RecordFlag::F_LAST as u8;
        if order.price.is_zero() {
            let previous = previous.with_context(|| format!("unknown R0 delete for order {id}"))?;
            ensure!(
                order.amount.is_sign_positive() == previous.amount.is_sign_positive(),
                "R0 delete side mismatch for order {id}"
            );
            deltas.push(delta(
                BookAction::Delete,
                previous.canonical(id, true)?,
                last,
            ));
            self.orders.remove(&id);
        } else {
            let canonical = order.canonical(id, false)?;
            let action = match previous {
                Some(old)
                    if old.price == order.price
                        && old.amount.is_sign_positive() == order.amount.is_sign_positive() =>
                {
                    BookAction::Update
                }
                Some(old) => {
                    // Like Kraken L3, a move leaves the old queue before entering the new one.
                    deltas.push(delta(BookAction::Delete, old.canonical(id, true)?, 0));
                    BookAction::Add
                }
                None => BookAction::Add,
            };
            deltas.push(delta(action, canonical, last));
            self.orders.insert(id, order);
        }
        Ok(OrderBookDeltas::new(instrument, deltas))
    }

    pub(crate) fn checksum(&self) -> i32 {
        let (mut bids, mut asks): (Vec<_>, Vec<_>) = self
            .orders
            .iter()
            .partition(|(_, order)| order.amount.is_sign_positive());
        bids.sort_unstable_by(|(a, x), (b, y)| y.price.cmp(&x.price).then_with(|| a.cmp(b)));
        asks.sort_unstable_by(|(a, x), (b, y)| x.price.cmp(&y.price).then_with(|| a.cmp(b)));
        let mut fields = Vec::with_capacity(100);
        for index in 0..25 {
            for side in [&bids, &asks] {
                if let Some((id, order)) = side.get(index) {
                    fields.push(id.to_string());
                    let amount = order.amount.normalize();
                    fields.push(if amount.abs() < Decimal::new(1, 6) {
                        format!("{amount:e}")
                    } else {
                        amount.to_string()
                    });
                }
            }
        }
        crc32fast::hash(fields.join(":").as_bytes()) as i32
    }
}

fn parse_row(row: &RawValue) -> anyhow::Result<(u64, Order)> {
    let fields: Vec<&RawValue> = serde_json::from_str(row.get())?;
    let [id, price, amount, ..] = fields.as_slice() else {
        anyhow::bail!("incomplete R0 order");
    };
    let id: u64 = serde_json::from_str(id.get()).context("invalid R0 order ID")?;
    ensure!(id != 0, "invalid R0 order ID");
    let price = decimal(price.get())?;
    let amount = decimal(amount.get())?;
    ensure!(
        price >= Decimal::ZERO
            && !amount.is_zero()
            && (price != Decimal::ZERO || amount.abs() == Decimal::ONE),
        "invalid R0 price or amount for order {id}",
    );
    Ok((id, Order { price, amount }))
}

fn decimal(token: &str) -> anyhow::Result<Decimal> {
    let value = if token.contains(['e', 'E']) {
        Decimal::from_scientific(token)?
    } else {
        Decimal::from_str_exact(token)?
    };
    ensure!(
        value.normalize().scale() <= 8,
        "R0 value exceeds the supported decimal precision"
    );
    Ok(value)
}
