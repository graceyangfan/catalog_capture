// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Native inventory-cost accounting through direct fills and recorded LiveNode replay.
use nautilus_model::{events::OrderFilled, instruments::Instrument, position::Position};

use super::*;

/// Applies actual Native accounting; no test-side PnL or inventory adjustment.
fn apply_fill(
    position: &mut Option<Position>,
    instrument: &InstrumentAny,
    fill: &OrderFilled,
) -> bool {
    let count = position.as_ref().map_or(0, |p| p.events.len());
    if let Some(position) = position {
        position.try_apply(fill).unwrap();
    } else {
        *position = Some(Position::new_checked(instrument, fill.clone()).unwrap());
    }
    position.as_ref().unwrap().events.len() != count
}

fn fill(sequence: u64, side: OrderSide, qty: &str, px: &str, fee: &str) -> OrderFilled {
    super::native_fee_paths::fill(
        &instrument(1).into(),
        sequence,
        side,
        qty,
        px,
        Money::from(fee),
    )
}

fn mixed_cost_audit() -> Value {
    let instrument: InstrumentAny = instrument(1).into();
    let mut position = None;
    let mut cash = Decimal::ZERO;
    let mut fills = vec![];
    let mut states = vec![];
    for (sequence, side, qty, px, shares_fee, quote_fee) in [
        (1, OrderSide::Buy, "17", "0.009", Some("0.34"), "0 USDT"),
        (2, OrderSide::Buy, "15", "0.010", Some("0.30"), "0 USDT"),
        (3, OrderSide::Sell, "10", "0.011", None, "0.0022 USDT"),
        (4, OrderSide::Buy, "5", "0.008", None, "0 USDT"),
        (5, OrderSide::Sell, "26.36", "0.012", None, "0.0063264 USDT"),
    ] {
        let mut fill = fill(sequence, side, qty, px, quote_fee);
        fill.quantity_commission = shares_fee.map(Quantity::from);
        if sequence == 4 {
            fill.liquidity_side = nautilus_model::enums::LiquiditySide::Maker;
        }
        if let Some(current) = &position {
            position =
                Some(serde_json::from_str(&serde_json::to_string(current).unwrap()).unwrap());
        }
        assert!(apply_fill(&mut position, &instrument, &fill));
        let notional = fill.last_qty.as_decimal() * fill.last_px.as_decimal();
        cash += if side == OrderSide::Buy {
            -notional
        } else {
            notional
        } - fill.commission.unwrap().as_decimal();
        let current = position.as_ref().unwrap();
        let marked_cash = cash + current.quantity.as_decimal() * fill.last_px.as_decimal();
        let pnl = current.total_pnl(fill.last_px).unwrap().as_decimal();
        assert!((pnl - marked_cash).abs() <= Decimal::new(1, 8));
        states.push(
            json!({"quantity":current.quantity,"average":current.avg_px_open,
            "nativeTotalPnl":pnl.to_string(),"cashPlusMarkedInventory":marked_cash.to_string()}),
        );
        fills.push(fill);
        let before = serde_json::to_string(position.as_ref().unwrap()).unwrap();
        for prior in fills.iter().rev() {
            assert!(!apply_fill(&mut position, &instrument, prior));
        }
        assert_eq!(
            serde_json::to_string(position.as_ref().unwrap()).unwrap(),
            before
        );
    }
    let position = position.unwrap();
    assert!(position.is_closed());
    assert_eq!(cash, Decimal::from_str_exact("0.0747936").unwrap());
    json!({"source":"synthetic Native mixed-price cost accounting; not a venue execution",
        "states":states,"fills":fills,"cashPnl":cash.to_string(),"nativeRealized":position.realized_pnl,
        "restoreAndDuplicateReplayUnchanged":true})
}

#[test]
fn inventory_fee_cost_mixed_prices_partials_and_restore_match_cash() {
    mixed_cost_audit();
}

#[test]
fn inventory_fee_cost_preserves_legacy_base_fee_behavior_and_trade_average() {
    let spot: InstrumentAny = nautilus_model::instruments::stubs::currency_pair_btcusdt().into();
    for base_fee in [true, false] {
        let mut buy = super::native_fee_paths::fill(
            &spot,
            1,
            OrderSide::Buy,
            "1",
            "100",
            Money::zero(Currency::USDT()),
        );
        if base_fee {
            buy.commission = Some(
                Money::from_decimal(Decimal::new(1, 2), spot.base_currency().unwrap()).unwrap(),
            );
        } else {
            buy.quantity_commission = Some(Quantity::from_decimal(Decimal::new(1, 2)).unwrap());
        }
        let mut position = None;
        apply_fill(&mut position, &spot, &buy);
        let current = position.as_ref().unwrap();
        assert_eq!(current.avg_px_open, Decimal::from(100));
        let expected_entry = if base_fee { "0 USDT" } else { "-1 USDT" };
        assert_eq!(current.realized_pnl.unwrap(), Money::from(expected_entry));
        assert_eq!(
            current.total_pnl(Price::from("101")).unwrap(),
            Money::from(if base_fee { "0.99 USDT" } else { "-0.01 USDT" })
        );
        let close = super::native_fee_paths::fill(
            &spot,
            2,
            OrderSide::Sell,
            "0.99",
            "110",
            Money::zero(Currency::USDT()),
        );
        apply_fill(&mut position, &spot, &close);
        assert!(position.as_ref().unwrap().is_closed());
        assert_eq!(
            position.unwrap().realized_pnl.unwrap(),
            Money::from(if base_fee { "9.9 USDT" } else { "8.9 USDT" })
        );
    }
}

async fn recorded_cost_audit() -> Value {
    let replay = super::recorded_startup::run_trade_flow().await;
    let instrument: InstrumentAny = instrument(1).into();
    let mut position = None;
    let mut fills = replay["nativeFills"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| serde_json::from_value::<OrderFilled>(value.clone()).unwrap())
        .collect::<Vec<_>>();
    fills.sort_by_key(|fill| fill.ts_event);
    // Native model identity is rebound to the test instrument; economic facts are unchanged.
    for fill in &mut fills {
        fill.instrument_id = instrument.id();
        apply_fill(&mut position, &instrument, fill);
    }
    let position = position.unwrap();
    assert!(position.is_closed());
    assert_eq!(
        position.realized_pnl.unwrap(),
        Money::from("-0.08142080 USDT")
    );
    assert_eq!(
        super::native_fee_paths::quantity_fees(&position),
        Some(Decimal::new(64, 2))
    );
    json!({
        "source":"previous real JSON ingested through loopback LiveNode with Native inventory-cost accounting",
        "nativeInventoryCostAccounting":true,"mainnetReads":0,"mainnetWrites":0,
        "liveNodeReplay":replay,"costedNativePosition":position,"mixedCostAudit":mixed_cost_audit(),
        "precisionBoundary":subquantum_cost_audit(),
        "limits":["identities and times rebound to loopback", "mixed-price and precision cases are synthetic",
            "Native checked Decimal products and PnL remainders remain bounded by domain precision"]
    })
}

#[tokio::test(flavor = "current_thread")]
async fn inventory_fee_cost_recorded_json_matches_flat_cash() {
    recorded_cost_audit().await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline recorded-economics Native cost capture; no venue access"]
async fn record_inventory_fee_cost_validation_json() {
    let audit = recorded_cost_audit().await;
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../data/predict-validation")
        .join(UUID4::new().to_string());
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("inventory_fee_cost_validation.json");
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .unwrap();
    serde_json::to_writer_pretty(file, &audit).unwrap();
    eprintln!(
        "Saved Native inventory-fee cost validation: {}",
        path.display()
    );
}

fn subquantum_cost_audit() -> Value {
    let instrument: InstrumentAny = instrument(1).into();
    let mut position = None;
    let mut raw_cost_total = Decimal::ZERO;
    let mut cumulative_expense = Money::zero(Currency::USDT());
    for sequence in 1..=4 {
        let mut buy = fill(sequence, OrderSide::Buy, "1", "0.04", "0 USDT");
        buy.quantity_commission = Some(Quantity::from_decimal(Decimal::new(1, 7)).unwrap());
        apply_fill(&mut position, &instrument, &buy);
        // Independent valuation of the original fills; Native owns its PnL remainder.
        let previous = Money::from_decimal_exact(raw_cost_total, Currency::USDT()).unwrap();
        raw_cost_total += buy.quantity_commission.unwrap().as_decimal() * buy.last_px.as_decimal();
        cumulative_expense = cumulative_expense
            + (Money::from_decimal_exact(raw_cost_total, Currency::USDT()).unwrap() - previous);
    }
    let position = position.unwrap();
    let cash_with_inventory =
        position.quantity.as_decimal() * Decimal::new(4, 2) - Decimal::new(16, 2);
    assert_eq!(cash_with_inventory, Decimal::new(-16, 9));
    let expected = Money::from_decimal_exact(cash_with_inventory, Currency::USDT()).unwrap();
    let actual = position.total_pnl(Price::from("0.04")).unwrap();
    assert_eq!(actual, expected);
    assert!(
        (super::native_fee_paths::quantity_fee_value(&position).unwrap() - raw_cost_total).abs()
            < Decimal::new(1, 22)
    );
    assert_eq!(expected.as_decimal(), Decimal::new(-16, 9));
    assert_eq!(cumulative_expense.as_decimal(), Decimal::new(16, 9));
    assert_eq!(raw_cost_total, -cash_with_inventory);
    json!({"source":"synthetic original-fill valuation against Native PnL remainders; not a venue execution",
        "fills":4,"feePerFill":"0.0000001","price":"0.04",
        "rawCostTotal":raw_cost_total.to_string(),"nativeExpense":actual,
        "cumulativeProjectedDebits":cumulative_expense,
        "cumulativeMoneyExpense":expected,"cashPlusMarkedInventory":cash_with_inventory.to_string()})
}

#[test]
fn inventory_fee_cost_cumulative_projection_retains_subquantum_expenses() {
    subquantum_cost_audit();
}
