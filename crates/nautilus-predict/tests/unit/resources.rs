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

//! Signed API budgets and Native remaining notional are distinct resource views.
use super::*;
use nautilus_common::factories::OrderEventFactory;
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{AccountType, AssetClass, LiquiditySide},
    orders::builder::OrderTestBuilder,
    types::{AccountBalance, Quantity},
};
use serde_json::{Value, json};

fn signed(side: PredictOrderSide) -> PredictOrderData {
    let source: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/cold_start_readonly_20261016T133220Z.json"
    ))
    .unwrap();
    let mut row = source["ordersByHash"][0].clone();
    row["status"] = json!("OPEN");
    row["order"]["side"] = json!(if side == PredictOrderSide::Buy { 0 } else { 1 });
    row["order"]["tokenId"] = json!("11");
    row["order"]["makerAmount"] = json!(if side == PredictOrderSide::Buy {
        "1280000000000000000"
    } else {
        "32000000000000000000"
    });
    row["order"]["takerAmount"] = json!(if side == PredictOrderSide::Buy {
        "32000000000000000000"
    } else {
        "1280000000000000000"
    });
    row["order"]["feeRateBps"] = json!("200");
    serde_json::from_value(row).unwrap()
}

#[test]
fn reported_signed_budget_and_admission_headroom_do_not_double_charge_share_fees() {
    let mut row = signed(PredictOrderSide::Buy);
    open_order_hash(&row).unwrap();
    // The signed budget remains valid regardless of cumulative gross fills.
    for filled in ["0", "17000000000000000000", "32000000000000000000"] {
        row.amount_filled = filled.into();
        assert!(open_order_hash(&row).is_ok());
        assert_eq!(
            reported_signed_reservation(&row).unwrap(),
            Money::from("1.28 USDT")
        );
        assert_eq!(
            signed_admission_collateral(&row).unwrap(),
            Decimal::new(13056, 4)
        );
    }
    row.order.fields.maker_amount = U256::from(1);
    row.order.fields.taker_amount = U256::from(1);
    assert!(open_order_hash(&row).is_ok());
    assert_eq!(
        reported_signed_reservation(&row).unwrap(),
        Money::from("0.00000001 USDT")
    );
    assert!(signed_admission_collateral(&row).unwrap() > exact_wei(U256::from(1)).unwrap());
    row.order.fields.fee_rate_bps = U256::from(10001);
    assert!(signed_admission_collateral(&row).is_err());
}

#[test]
fn sell_inventory_is_token_scoped_and_never_locks_collateral() {
    let row = signed(PredictOrderSide::Sell);
    assert!(reported_signed_reservation(&row).unwrap().is_zero());
    assert_eq!(signed_admission_collateral(&row).unwrap(), Decimal::ZERO);
    assert_eq!(
        signed_inventory(&row, U256::from(11)),
        row.order.fields.maker_amount
    );
    assert_eq!(signed_inventory(&row, U256::from(22)), U256::ZERO);
    assert_eq!(
        signed_inventory(&signed(PredictOrderSide::Buy), U256::from(11)),
        U256::ZERO
    );
}

#[test]
fn open_resource_view_rejects_invalid_signed_terms() {
    for field in ["maker", "taker", "currency", "status", "hash"] {
        let mut row = signed(PredictOrderSide::Buy);
        match field {
            "maker" => row.order.fields.maker_amount = U256::ZERO,
            "taker" => row.order.fields.taker_amount = U256::ZERO,
            "currency" => row.currency = "OTHER".into(),
            "status" => row.status = PredictOrderStatus::Cancelled,
            "hash" => row.order.hash = None,
            _ => unreachable!(),
        }
        assert!(open_order_hash(&row).is_err(), "{field}");
    }
}

#[test]
fn native_remaining_notional_uses_gross_fills_and_native_cash_math() {
    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id("42-1.PREDICT".into())
        .side(OrderSide::Buy)
        .quantity(Quantity::from(32))
        .price(Price::from("0.040"))
        .build();
    let factory = OrderEventFactory::new(
        order.trader_id(),
        "PREDICT-resources".into(),
        AccountType::Cash,
        None,
    );
    order
        .apply(factory.generate_order_submitted(&order, UnixNanos::from(1)))
        .unwrap();
    order
        .apply(factory.generate_order_accepted(
            &order,
            "resource-hash".into(),
            UnixNanos::from(2),
            UnixNanos::from(2),
        ))
        .unwrap();
    let mut fill = factory.generate_order_filled(
        &order,
        order.venue_order_id().unwrap(),
        None,
        "partial-resource-fill".into(),
        Quantity::from(17),
        Price::from("0.040"),
        Currency::USDT(),
        Some(Money::zero(Currency::USDT())),
        LiquiditySide::Taker,
        UnixNanos::from(3),
        UnixNanos::from(3),
    );
    if let nautilus_model::events::OrderEventAny::Filled(fill) = &mut fill {
        fill.quantity_commission = Some(Decimal::new(34, 2));
    }
    order.apply(fill).unwrap();
    let binary = nautilus_model::instruments::BinaryOption::builder()
        .instrument_id(order.instrument_id())
        .raw_symbol("11".into())
        .asset_class(AssetClass::Alternative)
        .currency(Currency::USDT())
        .activation_ns(UnixNanos::default())
        .expiration_ns(UnixNanos::from(u64::MAX))
        .price_precision(3)
        .price_increment(Price::from("0.001"))
        .size_precision(3)
        .size_increment(Quantity::from("0.001"))
        .ts_event(UnixNanos::default())
        .ts_init(UnixNanos::default())
        .build()
        .unwrap();
    let cash = CashAccount::new(
        factory.generate_account_state(
            vec![AccountBalance::new(
                Money::from("100 USDT"),
                Money::zero(Currency::USDT()),
                Money::from("100 USDT"),
            )],
            vec![],
            true,
            UnixNanos::default(),
            UnixNanos::default(),
            None,
        ),
        false,
        false,
    );
    assert_eq!(order.leaves_qty(), Quantity::from(15));
    assert_eq!(
        native_admission_collateral(&order).unwrap(),
        Decimal::new(60, 2)
    );
    assert_eq!(
        native_reservation(&cash, &order, &InstrumentAny::BinaryOption(binary)).unwrap(),
        Money::from("0.60 USDT")
    );
    assert_eq!(native_inventory(&order), Decimal::ZERO);
}
