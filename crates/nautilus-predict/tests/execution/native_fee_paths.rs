// Copyright (C) 2026 yfclark and contributors. Licensed under LGPL-3.0.

//! Native model feasibility probes, not an adapter implementation or LiveNode run.
//! Direct model mutation here does not prove a standard live ingestion path exists.
use nautilus_model::{
    enums::{LiquiditySide, PositionAdjustmentType},
    events::{OrderEventAny, OrderFilled, PositionAdjusted, order::spec::OrderFilledSpec},
    identifiers::{PositionId, TradeId},
    instruments::{Instrument, stubs::currency_pair_btcusdt},
    position::Position,
};

use super::*;

pub(super) fn fill(
    instrument: &InstrumentAny,
    sequence: u64,
    side: OrderSide,
    quantity: &str,
    price: &str,
    commission: Money,
) -> OrderFilled {
    OrderFilledSpec::builder()
        .trader_id(trader_id())
        .account_id(account_id())
        .instrument_id(instrument.id())
        .client_order_id(format!("FEE-PROBE-{sequence}").into())
        .trade_id(TradeId::new(format!("FEE-PROBE-{sequence}")))
        .position_id(PositionId::new("P-FEE-PROBE"))
        .order_type(OrderType::Limit)
        .order_side(side)
        .last_qty(Quantity::from(quantity))
        .last_px(Price::from(price))
        .currency(instrument.quote_currency())
        .commission(commission)
        .liquidity_side(LiquiditySide::Taker)
        .ts_event(UnixNanos::from(sequence * 1000))
        .ts_init(UnixNanos::from(sequence * 1000))
        .build()
}

fn spot_base_fee() -> Value {
    let instrument = InstrumentAny::CurrencyPair(currency_pair_btcusdt());
    let base = instrument.base_currency().unwrap();
    let buy = fill(
        &instrument,
        1,
        OrderSide::Buy,
        "1.00",
        "100.00",
        Money::from_decimal(Decimal::new(1, 2), base).unwrap(),
    );
    let mut position = Position::new(&instrument, buy);
    let net = position.quantity.as_decimal();
    let adjustments = position.adjustments.len();
    let sell = fill(
        &instrument,
        2,
        OrderSide::Sell,
        "0.99",
        "110.00",
        Money::zero(Currency::USDT()),
    );
    position.try_apply(&sell).unwrap();
    json!({"gross": "1", "net":net.to_string(), "adjustments":adjustments,
        "nativeRealizedUSDT":position.realized_pnl.unwrap().as_decimal().to_string(),
        "baseCommission":position.commissions()[0].to_string(),
        "flatCashDifferenceUSDT":"8.90",
        "meaning":"Base fee is retained in its own currency; quote realized alone is not all-in cash PnL"})
}

#[test]
fn currency_pair_base_fee_keeps_gross_and_net_but_separate_fee_currency() {
    let result = spot_base_fee();
    assert_eq!(result["net"], "0.990000");
    assert_eq!(result["adjustments"], 1);
    assert_eq!(result["nativeRealizedUSDT"], "9.90000000");
    assert_ne!(
        result["nativeRealizedUSDT"],
        result["flatCashDifferenceUSDT"]
    );
}

#[test]
fn binary_non_quote_commission_does_not_activate_spot_base_fee_path() {
    let instrument = InstrumentAny::BinaryOption(instrument(1));
    assert_eq!(instrument.base_currency(), None);
    // BTC stands for an arbitrary non-settlement commission in this model probe.
    // It is not a proposed Predict fee asset or a change to the instrument type.
    let fee = Money::from_decimal(Decimal::new(64, 2), Currency::BTC()).unwrap();
    let buy = fill(&instrument, 1, OrderSide::Buy, "32", "0.040", fee);
    let position = Position::new(&instrument, buy);
    assert_eq!(position.quantity.as_decimal(), Decimal::from(32));
    assert!(position.adjustments.is_empty());
    assert_eq!(position.commissions(), vec![fee]);
}

fn binary_adjustments(include_disposal_pnl: bool, maker_second: bool) -> Value {
    let instrument = InstrumentAny::BinaryOption(instrument(1));
    let mut native_order = order(1);
    let mut position: Option<Position> = None;
    for (sequence, quantity, price, fee_shares) in [
        (1, "17", "0.009", "0.34"),
        (2, "15", "0.010", if maker_second { "0" } else { "0.30" }),
    ] {
        let price = Decimal::from_str_exact(price).unwrap();
        let shares = Decimal::from_str_exact(fee_shares).unwrap();
        let mut event = fill(
            &instrument,
            sequence,
            OrderSide::Buy,
            quantity,
            &price.to_string(),
            Money::from_decimal(shares * price, Currency::USDT()).unwrap(),
        );
        event.client_order_id = native_order.client_order_id();
        event.strategy_id = native_order.strategy_id();
        event.venue_order_id = native_order.venue_order_id().unwrap();
        if maker_second && sequence == 2 {
            event.liquidity_side = LiquiditySide::Maker;
        }
        native_order
            .apply(OrderEventAny::Filled(event.clone()))
            .unwrap();
        if let Some(position) = &mut position {
            position.try_apply(&event).unwrap();
        } else {
            position = Some(Position::new(&instrument, event));
        }
        let position = position.as_mut().unwrap();
        if !shares.is_zero() {
            // Test-only valuation of a fee-asset disposal at the actual fill price.
            // This does NOT set the venue position average to that price, and does
            // NOT implement a live adapter ledger or publish a fake SELL.
            let avg = Decimal::from_str_exact(&position.avg_px_open.to_string()).unwrap();
            let disposal_pnl = include_disposal_pnl
                .then(|| Money::from_decimal(shares * (price - avg), Currency::USDT()).unwrap());
            position
                .apply_adjustment(PositionAdjusted::new(
                    position.trader_id,
                    position.strategy_id,
                    position.instrument_id,
                    position.id,
                    position.account_id,
                    PositionAdjustmentType::Commission,
                    Some(-shares),
                    disposal_pnl,
                    Some("offline-model-fee-probe".into()),
                    UUID4::new(),
                    UnixNanos::from(sequence * 1000 + 1),
                    UnixNanos::from(sequence * 1000 + 1),
                ))
                .unwrap();
        }
    }
    let mut position = position.unwrap();
    let net = position.quantity.as_decimal();
    let average = position.avg_px_open;
    let close = fill(
        &instrument,
        3,
        OrderSide::Sell,
        &net.to_string(),
        "0.010",
        Money::zero(Currency::USDT()),
    );
    position.try_apply(&close).unwrap();
    let actual_net = Decimal::from(32)
        - Decimal::new(34, 2)
        - if maker_second {
            Decimal::ZERO
        } else {
            Decimal::new(30, 2)
        };
    assert!((net - actual_net).abs() <= Decimal::new(1, 12));
    let cash_pnl = actual_net * Decimal::new(1, 2) - Decimal::new(303, 3);
    let native_pnl = position.realized_pnl.unwrap().as_decimal();
    json!({"source":"direct Native Order/Position model, not live routing",
        "includeFeeDisposalPnL":include_disposal_pnl,"secondFillMaker":maker_second,
        "grossOrderFilled":native_order.filled_qty().as_decimal().to_string(),
        "netPositionBeforeExit":net.to_string(),"positionAverage":average,
        "cashPnL":cash_pnl.to_string(),"nativeRealizedPnL":native_pnl.to_string(),
        "matchesCash":(cash_pnl-native_pnl).abs()<=Decimal::new(1,8),
        "finalQuantity":position.quantity.as_decimal().to_string(),
        "liveAdapterIngressProven":false})
}

#[test]
fn quantity_only_adjustment_does_not_complete_binary_fee_cost_accounting() {
    let result = binary_adjustments(false, false);
    assert_eq!(result["grossOrderFilled"], "32");
    assert_eq!(
        Decimal::from_str_exact(result["cashPnL"].as_str().unwrap()).unwrap(),
        Decimal::new(106, 4)
    );
    assert!(
        (Decimal::from_str_exact(result["nativeRealizedPnL"].as_str().unwrap()).unwrap()
            - Decimal::from_str_exact("0.010442135186355").unwrap())
        .abs()
            < Decimal::new(1, 15)
    );
    assert_eq!(result["matchesCash"], false);
}

#[test]
fn existing_position_adjustment_model_can_express_fee_disposal_but_not_live_ingress() {
    for maker_second in [false, true] {
        let result = binary_adjustments(true, maker_second);
        assert_eq!(result["grossOrderFilled"], "32");
        assert_eq!(result["matchesCash"], true);
        assert_eq!(result["liveAdapterIngressProven"], false);
        assert_eq!(result["finalQuantity"], "0.0000000000000000");
    }
}

#[test]
#[ignore = "Writes synthetic direct-Native-model feasibility evidence; never calls venue"]
fn record_native_fee_path_model_json() {
    use std::{fs::OpenOptions, io::Write};
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/native_fee_path_model_synthetic_20261017.json");
    let value = json!({"source":"synthetic direct Native model probes; not Rust LiveNode or venue execution",
        "nativeModified":false,"newExchangeRequests":0,"newOrders":0,
        "spot":spot_base_fee(),
        "binary":[binary_adjustments(false,false),binary_adjustments(true,false),binary_adjustments(true,true)],
        "liveAdapterIngressProven":false});
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    writeln!(file, "{}", serde_json::to_string_pretty(&value).unwrap()).unwrap();
}

pub(super) fn inventory_fees(events: Vec<&OrderEventAny>) -> Decimal {
    events
        .into_iter()
        .filter_map(|event| match event {
            OrderEventAny::Filled(fill) => fill.quantity_commission,
            _ => None,
        })
        .map(|fee| fee.as_decimal())
        .sum()
}

pub(super) fn project_inventory_fee(quantity: Decimal, precision: u8) -> Decimal {
    quantity.round_dp_with_strategy(
        u32::from(precision),
        rust_decimal::RoundingStrategy::ToPositiveInfinity,
    )
}

pub(super) fn inventory_fee_residual(quantity: Decimal, precision: u8) -> Decimal {
    project_inventory_fee(quantity, precision) - quantity
}

/// Sum original persisted fee facts to verify replay without a duplicate accounting ledger.
pub(super) fn quantity_fees(position: &Position) -> Option<Decimal> {
    position
        .events
        .iter()
        .filter_map(|fill| fill.quantity_commission)
        .map(|fee| fee.as_decimal())
        .reduce(|a, b| a + b)
}

pub(super) fn quantity_fee_value(position: &Position) -> Option<Decimal> {
    position
        .events
        .iter()
        .filter_map(|fill| {
            fill.quantity_commission
                .map(|fee| fee.as_decimal() * fill.last_px.as_decimal())
        })
        .reduce(|a, b| a + b)
}
