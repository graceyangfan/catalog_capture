// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Test-only L1 OFI gate and bounded, position-owned exit for the partial-fill probe.
use std::collections::VecDeque;

use nautilus_model::{data::QuoteTick, instruments::Instrument};
use rust_decimal::RoundingStrategy;

use super::*;

#[derive(Debug, Default)]
pub(super) struct OfiWindow {
    previous: Option<(u64, [Decimal; 4])>,
    samples: VecDeque<(u64, Decimal, Decimal)>,
}
fn contribution(old: [Decimal; 4], new: [Decimal; 4]) -> Decimal {
    let [bp, bq, ap, aq] = new;
    let [obp, obq, oap, oaq] = old;
    (if bp >= obp { bq } else { Decimal::ZERO })
        - (if bp <= obp { obq } else { Decimal::ZERO })
        - (if ap <= oap { aq } else { Decimal::ZERO })
        + (if ap >= oap { oaq } else { Decimal::ZERO })
}
impl OfiWindow {
    pub(super) fn observe(&mut self, q: &QuoteTick, now: u64) {
        let frame = [
            q.bid_price.as_decimal(),
            q.bid_size.as_decimal(),
            q.ask_price.as_decimal(),
            q.ask_size.as_decimal(),
        ];
        if let Some((then, old)) = self.previous {
            if frame == old {
                return;
            }
            if now.saturating_sub(then) > 5_000_000_000 {
                self.samples.clear();
            }
            self.samples.push_back((
                now,
                contribution(old, frame),
                (frame[1] + frame[3]) / Decimal::TWO,
            ));
        }
        self.previous = Some((now, frame));
        while self
            .samples
            .front()
            .is_some_and(|x| now.saturating_sub(x.0) > 5_000_000_000)
        {
            self.samples.pop_front();
        }
    }
    pub(super) fn evidence(&self, now: u64) -> Value {
        let rows: Vec<_> = self
            .samples
            .iter()
            .filter(|s| now.saturating_sub(s.0) <= 5_000_000_000)
            .collect();
        let sum: Decimal = rows.iter().map(|s| s.1).sum();
        let depth: Decimal = rows.iter().map(|s| s.2).sum();
        let normalized = if depth > Decimal::ZERO {
            sum * Decimal::from(rows.len()) / depth
        } else {
            Decimal::ZERO
        };
        let span = rows
            .first()
            .zip(rows.last())
            .map_or(0, |(a, b)| b.0.saturating_sub(a.0));
        json!({"definition":"L1 event OFI over five seconds, divided by mean L1 depth; not traded-volume OFI","events":rows.len(),"spanNs":span,"sumShares":sum.to_string(),"normalized":normalized.to_string(),"eligible":rows.len()>=3 && span>=200_000_000 && normalized>=Decimal::new(25,2)})
    }
}

#[derive(Debug, Default)]
pub(super) struct RoundTrip {
    since: Option<u64>,
    exit: Option<ClientOrderId>,
    exit_ns: u64,
    cancel_sent: bool,
    maker_attempted: bool,
    taker_attempts: u32,
    pub(super) done: bool,
}
impl RoundTrip {
    pub(super) fn observe_entry_fill(&mut self, now: u64) {
        // Exposure begins at settlement, not at the eventual order terminal.
        self.since.get_or_insert(now);
    }
}

fn take_profit(cost: Decimal, qty: Decimal, bid: Decimal, tick: Decimal) -> Decimal {
    // Actual entry cash / net shares includes the SHARES fee. Tick rounding
    // leaves positive cash profit (post-only maker fee zero).
    let mut price = ((cost / qty).max(bid + tick) / tick)
        .round_dp_with_strategy(0, RoundingStrategy::ToPositiveInfinity)
        * tick;
    if price * qty <= cost {
        price += tick;
    }
    price
}
fn sale_quantity(qty: Decimal) -> Decimal {
    let mut step = Decimal::new(1, 4);
    while qty / step >= Decimal::from(100_000) {
        step *= Decimal::TEN;
    }
    (qty / step).round_dp_with_strategy(0, RoundingStrategy::ToZero) * step
}
impl MarketProbe {
    pub(super) fn cancel_exit(&mut self) -> anyhow::Result<()> {
        let Some(state) = self.round_trip.as_mut() else {
            return Ok(());
        };
        if let Some(id) = state.exit
            && !state.cancel_sent
            && self.cache().order(&id).is_some_and(|o| !o.is_closed())
        {
            self.round_trip.as_mut().unwrap().cancel_sent = true;
            self.journal.borrow_mut().push(json!({"type":"NativeExitCancelRequested","clientOrderId":id.to_string(),"observedNs":self.clock().timestamp_ns().as_u64()}));
            self.cancel_order(id, Some(ClientId::new("PREDICT")), None)?;
        }
        Ok(())
    }
    pub(super) fn drive_round_trip(&mut self) -> anyhow::Result<()> {
        let Some(state) = self.round_trip.as_ref() else {
            return Ok(());
        };
        if state.done {
            return Ok(());
        }
        let now = self.clock().timestamp_ns().as_u64();
        let entry = self.entry.unwrap();
        let (gross, cost, fee) = {
            let cache = self.cache();
            let Some(order) = cache.order(&entry) else {
                return Ok(());
            };
            if !order.is_closed() {
                return Ok(());
            }
            let mut gross = Decimal::ZERO;
            let mut cost = Decimal::ZERO;
            let mut fee = Decimal::ZERO;
            for e in order.events() {
                if let OrderEventAny::Filled(f) = e {
                    gross += f.last_qty.as_decimal();
                    cost += f.last_qty.as_decimal() * f.last_px.as_decimal();
                    fee += f
                        .quantity_commission
                        .map(|v| v.as_decimal())
                        .unwrap_or_else(|| {
                            f.commission
                                .map_or(Decimal::ZERO, |v| v.as_decimal() / f.last_px.as_decimal())
                        });
                }
            }
            (gross, cost, fee)
        };
        if gross.is_zero() {
            return self.finish_round_trip("entry_closed_without_fill", Decimal::ZERO);
        }
        let since = *self.round_trip.as_mut().unwrap().since.get_or_insert(now);
        let qty: Decimal = self
            .cache()
            .positions_open(
                None,
                Some(&self.instrument),
                self.strategy_id().as_ref(),
                None,
                None,
            )
            .iter()
            .map(|p| p.quantity.as_decimal())
            .sum();
        if qty.is_zero() {
            if self.round_trip.as_ref().unwrap().maker_attempted
                || now.saturating_sub(since) > 10_000_000_000
            {
                return self.finish_round_trip("native_position_flat", qty);
            }
            return Ok(());
        }
        if qty > gross - fee + Decimal::new(1, 7) {
            return Ok(());
        }
        let Some(quote) = self.cache().quote(&self.instrument) else {
            return Ok(());
        };
        if now.saturating_sub(quote.ts_init.as_u64()) > 2_000_000_000 {
            return Ok(());
        }
        let bid = quote.bid_price.as_decimal();
        // The entire purchase cost at risk fits the authorized tiny hold.
        if cost <= Decimal::new(10, 2) {
            return self.finish_round_trip("authorized_small_holding_until_settlement", qty);
        }
        let mut sold_net = Decimal::ZERO;
        {
            let cache = self.cache();
            for order in cache.orders(
                None,
                Some(&self.instrument),
                self.strategy_id().as_ref(),
                None,
                None,
            ) {
                if order.order_side() == OrderSide::Sell {
                    for e in order.events() {
                        if let OrderEventAny::Filled(f) = e {
                            sold_net += f.last_qty.as_decimal() * f.last_px.as_decimal()
                                - f.commission.map_or(Decimal::ZERO, |v| v.as_decimal());
                        }
                    }
                }
            }
        }
        let stop = now.saturating_sub(since) >= 12_000_000_000
            || cost - sold_net - qty * bid * Decimal::new(98, 2) >= self.max_loss;
        if let Some(id) = self.round_trip.as_ref().unwrap().exit {
            let closed = self.cache().order(&id).is_some_and(|o| o.is_closed());
            if !closed {
                if stop
                    || now.saturating_sub(self.round_trip.as_ref().unwrap().exit_ns) > 4_000_000_000
                        && self.round_trip.as_ref().unwrap().taker_attempts > 0
                {
                    self.cancel_exit()?;
                }
                return Ok(());
            }
            self.round_trip.as_mut().unwrap().exit = None;
            // Wait a timer turn after a terminal: Portfolio applies final fills separately.
            return Ok(());
        }
        if self.round_trip.as_ref().unwrap().taker_attempts >= 2 {
            return self
                .finish_round_trip("native_exit_attempt_limit_supervisor_audit_required", qty);
        }
        let quantity = sale_quantity(qty);
        if quantity * bid * Decimal::new(97, 2) < Decimal::ONE {
            if cost - sold_net <= Decimal::new(10, 2) || qty <= Decimal::new(10, 2) {
                return self.finish_round_trip("authorized_small_residual_until_settlement", qty);
            }
            // Never top up. Preserve this explicit incomplete disposition for the supervisor.
            return self.finish_round_trip("exit_below_local_notional_policy", qty);
        }
        if quote.bid_size.as_decimal() < quantity {
            return Ok(());
        }
        let instrument = self.cache().instrument(&self.instrument).unwrap().clone();
        let quantity = Quantity::from_decimal_dp(quantity, instrument.size_precision())?;
        let taker = stop || self.round_trip.as_ref().unwrap().maker_attempted;
        let (order, params, price) = if taker {
            (
                self.order().market(
                    self.instrument,
                    OrderSide::Sell,
                    quantity,
                    Some(TimeInForce::Fok),
                    None,
                    Some(false),
                    None,
                    None,
                    None,
                    None,
                ),
                Some(nautilus_core::Params::new(
                    [
                        ("market_price".into(), json!(bid.to_string())),
                        ("market_order_slippage_bps".into(), json!(300)),
                    ]
                    .into(),
                )),
                bid,
            )
        } else {
            let price = take_profit(
                (cost - sold_net).max(Decimal::ZERO),
                quantity.as_decimal(),
                bid,
                instrument.price_increment().as_decimal(),
            );
            if price >= Decimal::ONE {
                return self.finish_round_trip("take_profit_outside_binary_range", qty);
            }
            (
                self.order().limit(
                    self.instrument,
                    OrderSide::Sell,
                    quantity,
                    Price::from_decimal_dp(price, instrument.price_precision())?,
                    Some(TimeInForce::Gtc),
                    None,
                    Some(true),
                    Some(false),
                    Some(false),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ),
                None,
                price,
            )
        };
        let id = order.client_order_id();
        let state = self.round_trip.as_mut().unwrap();
        state.exit = Some(id);
        state.exit_ns = now;
        state.cancel_sent = false;
        if taker {
            state.taker_attempts += 1;
        } else {
            state.maker_attempted = true;
        }
        let intent = json!({"type":if taker {"NativeTakerExitSubmitted"} else {"NativeMakerTakeProfitSubmitted"},"clientOrderId":id.to_string(),"quantity":quantity.to_string(),"price":price.to_string(),"entryCashCost":cost.to_string(),"entryShareFee":fee.to_string(),"nativeNetQuantity":qty.to_string(),"realizedNetProceeds":sold_net.to_string(),"expectedCashProfitAtLimit":(sold_net+quantity.as_decimal()*price-cost).to_string(),"observedNs":now,"market_order_slippage_bps":if taker {300}else{0}});
        if let Some(output) = &self.output {
            fill_session::checkpoint(&output.join("exit_intent.json"), &intent)?;
        }
        self.journal.borrow_mut().push(intent);
        self.submit_order(order, None, Some(ClientId::new("PREDICT")), params)
    }
    pub(super) fn finish_round_trip(&mut self, reason: &str, qty: Decimal) -> anyhow::Result<()> {
        self.round_trip.as_mut().unwrap().done = true;
        self.journal.borrow_mut().push(json!({"type":"RoundTripComplete","reason":reason,"remainingNativeQuantity":qty.to_string(),"observedNs":self.clock().timestamp_ns().as_u64()}));
        Ok(())
    }
}

#[test]
fn ofi_uses_price_and_size_changes_not_static_depth_ratio() {
    let d = |s| Decimal::from_str_exact(s).unwrap();
    let a = [d(".49"), d("100"), d(".50"), d("100")];
    assert_eq!(contribution(a, a), Decimal::ZERO);
    assert_eq!(
        contribution(a, [d(".49"), d("110"), d(".50"), d("80")]),
        d("30")
    );
    assert_eq!(
        contribution(a, [d(".50"), d("10"), d(".51"), d("20")]),
        d("110")
    );
    assert_eq!(
        contribution(a, [d(".48"), d("110"), d(".49"), d("80")]),
        d("-180")
    );
}
#[test]
fn maker_price_covers_share_fee_and_sales_round_down() {
    let d = |s| Decimal::from_str_exact(s).unwrap();
    let p = take_profit(d("1.4"), d("2.744"), d(".49"), d(".01"));
    assert_eq!(p, d(".52"));
    assert!(p * d("2.744") > d("1.4"));
    assert_eq!(sale_quantity(d("5.713412")), d("5.7134"));
    assert_eq!(sale_quantity(d("12.345678")), d("12.345"));
}

#[test]
fn ofi_window_expires_and_does_not_count_duplicate_quotes() {
    let q = |ask_size: &str, now: u64| {
        QuoteTick::new(
            InstrumentId::from("42-1.PREDICT"),
            Price::from("0.49"),
            Price::from("0.50"),
            Quantity::from("100"),
            Quantity::from(ask_size),
            now.into(),
            now.into(),
        )
    };
    let mut window = OfiWindow::default();
    for (now, size) in [
        (1_000_000_000, "100"),
        (1_100_000_000, "80"),
        (1_300_000_000, "60"),
        (1_400_000_000, "1"),
    ] {
        window.observe(&q(size, now), now);
    }
    assert_eq!(window.evidence(1_500_000_000)["eligible"], true);
    window.observe(&q("1", 1_600_000_000), 1_600_000_000);
    assert_eq!(window.evidence(1_600_000_000)["events"], 3);
    assert_eq!(window.evidence(7_000_000_000)["eligible"], false);
}
