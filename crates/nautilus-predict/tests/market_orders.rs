// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
#![cfg(feature = "execution")]

use std::sync::Arc;

use alloy::primitives::U256;
use nautilus_predict::{
    PredictEnvironment,
    common::{
        contracts::PredictExchangeType, enums::PredictOrderSide, market::PredictMarketOrderAmounts,
    },
    http::{command_models::PredictCreateOrderRequest, order_models::PredictOrder},
    signing::{PredictAuthSigner, PredictOrderSigner},
};
use rust_decimal::Decimal;
use serde_json::Value;

const KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

fn wei(text: &str) -> U256 {
    let value = Decimal::from_str_exact(text).unwrap();
    U256::from(value.mantissa() as u128) * U256::from(10).pow(U256::from(18 - value.scale()))
}

#[test]
fn market_amounts_signed_hash_signature_and_envelope_match_official_sdk() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/market_requests.json")).unwrap();
    let vectors = fixture["vectors"].as_array().unwrap();
    assert_eq!(vectors.len(), 64);
    for vector in vectors {
        let side = if vector["side"] == 0 {
            PredictOrderSide::Buy
        } else {
            PredictOrderSide::Sell
        };
        let levels = vector["levels"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| (wei(row[0].as_str().unwrap()), wei(row[1].as_str().unwrap())))
            .collect::<Vec<_>>();
        let amounts = PredictMarketOrderAmounts::from_levels(
            side,
            wei("100"),
            &levels,
            vector["body"]["data"]["slippageBps"].as_u64().unwrap() as u32,
        )
        .unwrap();
        amounts.validate().unwrap();
        assert_eq!(amounts.last_price.to_string(), vector["lastPrice"]);
        assert_eq!(
            amounts.price_per_share.to_string(),
            vector["body"]["data"]["pricePerShare"]
        );
        let order: PredictOrder =
            serde_json::from_value(vector["body"]["data"]["order"].clone()).unwrap();
        let signer = PredictOrderSigner::new(Arc::new(
            PredictAuthSigner::new(
                KEY.into(),
                PredictEnvironment::BnbMainnet,
                vector["predictAccount"].as_str(),
            )
            .unwrap(),
        ));
        let exchange = PredictExchangeType::from_market_flags(
            vector["isNegRisk"].as_bool().unwrap(),
            vector["isYieldBearing"].as_bool().unwrap(),
        );
        let signed = signer.sign_order(&order, exchange).unwrap();
        let body =
            serde_json::to_value(PredictCreateOrderRequest::market(&signed, &amounts).unwrap())
                .unwrap();
        // Retain the saved SDK signing oracle unchanged. Its hand-built HTTP
        // envelope predates the REST schema's string slippage and explicit mode.
        let mut expected = vector["body"].clone();
        expected["data"]["slippageBps"] = amounts.slippage_bps.to_string().into();
        expected["data"]["isMinAmountOut"] = false.into();
        assert_eq!(body, expected);
        assert_eq!(body["data"]["order"]["expiration"], 1_800_000_300u64);
        assert!(body["data"].get("isPostOnly").is_none());
        assert!(body["data"].get("timeInForce").is_none());
        assert_eq!(body["data"]["isMinAmountOut"], false);
    }
}

#[test]
fn market_depth_and_quantity_cannot_silently_resize_or_reorder_native_intent() {
    let valid = [(wei("0.01"), wei("100"))];
    let prepare = |levels: &[(U256, U256)], qty, slip| {
        PredictMarketOrderAmounts::from_levels(PredictOrderSide::Buy, qty, levels, slip)
    };
    assert!(prepare(&valid, wei("100"), 0).is_ok());
    for levels in [
        vec![],
        vec![(wei("0.01"), wei("99"))],
        vec![(wei("0.02"), wei("1")), (wei("0.01"), wei("100"))],
        vec![(wei("0.01"), wei("100")), (wei("0.01"), wei("100"))],
        vec![(U256::ZERO, wei("100"))],
        vec![(wei("1.01"), wei("100"))],
        vec![(wei("0.01"), U256::ZERO)],
    ] {
        assert!(prepare(&levels, wei("100"), 0).is_err());
    }
    assert!(prepare(&valid, wei("0.009"), 0).is_err());
    assert!(prepare(&valid, wei("10.0001"), 0).is_err());
    assert!(prepare(&valid, wei("100"), 10_000).is_err());
    let huge = U256::from(10).pow(U256::from(76));
    assert!(prepare(&[(wei("1"), huge)], huge, 0).is_err());
}

#[test]
fn market_average_price_is_not_the_signed_limit_ratio_or_cash_commitment() {
    let amounts = PredictMarketOrderAmounts::from_levels(
        PredictOrderSide::Buy,
        wei("100"),
        &[(wei("0.01"), wei("40")), (wei("0.02"), wei("60"))],
        100,
    )
    .unwrap();
    assert_eq!(amounts.price_per_share, wei("0.016"));
    assert_eq!(amounts.last_price, wei("0.02"));
    assert_eq!(amounts.maker_amount, wei("2.02"));
    assert_eq!(amounts.taker_amount, wei("100"));
    let mut inconsistent = amounts;
    inconsistent.slippage_bps = 0;
    assert!(inconsistent.validate().is_err());
    inconsistent = amounts;
    inconsistent.amount = wei("99");
    assert!(inconsistent.validate().is_err());
    inconsistent = amounts;
    inconsistent.price_per_share = wei("0.021");
    assert!(inconsistent.validate().is_err());
    let capped = PredictMarketOrderAmounts::from_levels(
        PredictOrderSide::Buy,
        wei("100"),
        &[(wei("0.999"), wei("100"))],
        100,
    )
    .unwrap();
    assert_eq!(capped.maker_amount, capped.amount);
    capped.validate().unwrap();
}

#[test]
fn saved_actual_strategy_books_match_sdk_and_incomplete_depth_is_not_resized() {
    let fixture: Value = serde_json::from_str(include_str!(
        "fixtures/private_api/market_live_snapshot_sdk_quotes_20261015.json"
    ))
    .unwrap();
    let vectors = fixture["vectors"].as_array().unwrap();
    assert!(!vectors.is_empty());
    for vector in vectors {
        let side = if vector["side"] == 0 {
            PredictOrderSide::Buy
        } else {
            PredictOrderSide::Sell
        };
        let levels = vector["levelsWei"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| {
                (
                    row[0].as_str().unwrap().parse::<U256>().unwrap(),
                    row[1].as_str().unwrap().parse::<U256>().unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let result = PredictMarketOrderAmounts::from_levels(side, wei("100"), &levels, 0);
        if !vector["nativeFullQuantityAdmissible"].as_bool().unwrap() {
            assert!(result.is_err());
            continue;
        }
        let amounts = result.unwrap();
        assert_eq!(
            amounts.price_per_share.to_string(),
            vector["expected"]["pricePerShare"]
        );
        assert_eq!(
            amounts.last_price.to_string(),
            vector["expected"]["lastPrice"]
        );
        assert_eq!(
            amounts.maker_amount.to_string(),
            vector["expected"]["makerAmount"]
        );
        assert_eq!(
            amounts.taker_amount.to_string(),
            vector["expected"]["takerAmount"]
        );
    }
}

#[test]
fn strategy_price_uses_sdk_single_level_amounts_with_slippage_once() {
    for side in [PredictOrderSide::Buy, PredictOrderSide::Sell] {
        for price in ["0.02", "0.42", "0.99"] {
            for slip in [0, 100, 9999] {
                let expected = PredictMarketOrderAmounts::from_levels(
                    side,
                    wei("100"),
                    &[(wei(price), wei("100"))],
                    slip,
                )
                .unwrap();
                let actual =
                    PredictMarketOrderAmounts::from_price(side, wei("100"), wei(price), slip)
                        .unwrap();
                assert_eq!(actual, expected);
                actual.validate().unwrap();
            }
        }
    }
    assert!(
        PredictMarketOrderAmounts::from_price(PredictOrderSide::Sell, wei("100"), U256::ZERO, 100)
            .is_err()
    );
}

#[test]
fn quote_value_reference_amounts_match_official_unsigned_sdk_vectors() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/market_value_amounts.json")).unwrap();
    let vectors = fixture["vectors"].as_array().unwrap();
    assert_eq!(vectors.len(), 7);
    for vector in vectors {
        let budget = wei(vector["value"].as_str().unwrap());
        let amounts = PredictMarketOrderAmounts::from_value_price(
            budget,
            wei(vector["price"].as_str().unwrap()),
        )
        .unwrap();
        amounts.validate().unwrap();
        assert_eq!(amounts.maker_amount.to_string(), vector["makerAmount"]);
        assert_eq!(amounts.taker_amount.to_string(), vector["takerAmount"]);
        assert_eq!(amounts.amount.to_string(), vector["amount"]);
        assert_eq!(amounts.price_per_share.to_string(), vector["pricePerShare"]);
        assert_eq!(amounts.slippage_bps, 0);
        assert!(amounts.maker_amount <= budget);
    }
    for (value, price) in [
        (wei("0.99"), wei(".01")),
        (wei("1"), U256::ZERO),
        (wei("1"), wei("1.01")),
        (U256::MAX, wei(".01")),
    ] {
        assert!(PredictMarketOrderAmounts::from_value_price(value, price).is_err());
    }
}

#[test]
fn zero_slippage_single_reference_is_not_full_depth_market_preparation() {
    // Shape observed by a read-only market snapshot: one minimum-notional
    // order cannot fit at the displayed best ask. This asserts arithmetic,
    // not a venue fill or a promise about MARKET remainder handling.
    let qty = wei("8.11");
    let native =
        PredictMarketOrderAmounts::from_price(PredictOrderSide::Buy, qty, wei(".148"), 0).unwrap();
    let depth = PredictMarketOrderAmounts::from_levels(
        PredictOrderSide::Buy,
        qty,
        &[(wei(".148"), wei("5")), (wei(".149"), wei("3.11"))],
        0,
    )
    .unwrap();
    assert_eq!(native.maker_amount, wei("1.20028"));
    assert_eq!(depth.maker_amount, wei("1.20839"));
    assert_eq!(depth.price_per_share, wei(".148383477188655980"));
    assert_eq!(depth.last_price, wei(".149"));
    assert!(
        native.maker_amount < wei("1.20339"),
        "reference-only payment cannot cover visible aggregate cost"
    );
    assert_eq!(native.slippage_bps, depth.slippage_bps);
    assert_ne!(native.price_per_share, depth.price_per_share);
}
