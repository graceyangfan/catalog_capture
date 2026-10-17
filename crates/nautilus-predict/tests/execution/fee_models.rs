// Copyright (C) 2026 yfclark and contributors. Licensed under LGPL-3.0.

use nautilus_execution::models::fee::{FeeModel, FeeModelHandle};
use nautilus_model::{
    enums::LiquiditySide, instruments::Instrument, orders::stubs::TestOrderStubs,
};
use nautilus_predict::fees::PredictFeeModel;

use super::*;

#[test]
fn predict_fee_model_matches_official_curve_through_native_handle() {
    let model = FeeModelHandle::new(PredictFeeModel::new(200).unwrap());
    for index in [1, 2] {
        let instrument = InstrumentAny::BinaryOption(instrument(index));
        for side in [OrderSide::Buy, OrderSide::Sell] {
            let order = OrderTestBuilder::new(OrderType::Limit)
                .instrument_id(instrument.id())
                .side(side)
                .quantity(Quantity::from(100))
                .price(Price::from("0.500"))
                .build();
            for liquidity in [LiquiditySide::Maker, LiquiditySide::Taker] {
                let filled = TestOrderStubs::make_filled_order(&order, &instrument, liquidity);
                for (price, fee) in [
                    ("0.010", "0.02"),
                    ("0.200", "0.4"),
                    ("0.500", "1"),
                    ("0.800", "0.4"),
                    ("0.990", "0.02"),
                ] {
                    let result = model
                        .get_commission(
                            &filled,
                            Quantity::from(100),
                            Price::from(price),
                            &instrument,
                        )
                        .unwrap();
                    assert_eq!(result.currency, Currency::USDT());
                    assert_eq!(
                        result.as_decimal(),
                        if liquidity == LiquiditySide::Maker {
                            Decimal::ZERO
                        } else {
                            Decimal::from_str_exact(fee).unwrap()
                        }
                    );
                    assert_eq!(filled.filled_qty().as_decimal(), Decimal::from(100));
                }
            }
        }
    }
}

#[test]
fn fee_schedule_overrides_and_verified_discount_preserve_instrument_identity() {
    let mut model = PredictFeeModel::new(200)
        .unwrap()
        .with_discount(1000)
        .unwrap();
    model.set_override(instrument_id(2), 400).unwrap();
    assert!(model.set_override(instrument_id(2), 10_001).is_err());
    for (index, expected) in [(1, "0.9"), (2, "1.8")] {
        let value = model
            .calculate_commission(
                &InstrumentAny::BinaryOption(instrument(index)),
                Quantity::from(100),
                Price::from("0.500"),
                LiquiditySide::Taker,
            )
            .unwrap();
        assert_eq!(
            value.as_decimal(),
            Decimal::from_str_exact(expected).unwrap()
        );
    }
    assert!(PredictFeeModel::new(10_001).is_err());
    assert!(
        PredictFeeModel::new(200)
            .unwrap()
            .with_discount(10_001)
            .is_err()
    );
}

#[test]
fn unknown_liquidity_and_invalid_fee_context_are_errors_not_free_trades() {
    let model = PredictFeeModel::new(200).unwrap();
    let instrument = InstrumentAny::BinaryOption(instrument(1));
    assert!(
        model
            .calculate_commission(
                &instrument,
                Quantity::from(100),
                Price::from("0.500"),
                LiquiditySide::NoLiquiditySide
            )
            .is_err()
    );
    for price in ["-0.001", "1.001"] {
        assert!(
            model
                .calculate_commission(
                    &instrument,
                    Quantity::from(100),
                    Price::from(price),
                    LiquiditySide::Taker
                )
                .is_err()
        );
    }
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(instrument.id())
        .quantity(Quantity::from(100))
        .price(Price::from("0.500"))
        .build();
    assert!(
        model
            .get_commission(
                &order,
                Quantity::from(100),
                Price::from("0.500"),
                &instrument
            )
            .is_err()
    );
    let wrong = InstrumentAny::BinaryOption(super::instrument(2));
    assert!(
        model
            .get_commission(&order, Quantity::from(100), Price::from("0.500"), &wrong)
            .is_err()
    );
    for price in ["0.000", "1.000"] {
        assert!(
            model
                .calculate_commission(
                    &instrument,
                    Quantity::from(100),
                    Price::from(price),
                    LiquiditySide::Taker
                )
                .unwrap()
                .is_zero()
        );
    }
}

#[test]
fn captured_share_fee_receipt_keeps_asset_units_and_confirms_net_transfer() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/share_fee_settlement.json"
    ))
    .unwrap();
    let number = |name: &str| fixture[name].as_str().unwrap().parse::<U256>().unwrap();
    let gross = number("gross_shares_wei");
    let fee = number("fee_shares_wei");
    let received = number("received_shares_wei");
    assert!(!fee.is_zero());
    assert_eq!(gross - fee, received);
    assert_eq!(gross * U256::from(200) / U256::from(10_000), fee);
    assert_ne!(number("collateral_paid_wei"), fee);
    assert!(
        fixture["provenance"]["scope"]
            .as_str()
            .unwrap()
            .contains("not evidence of WebSocket")
    );
}
