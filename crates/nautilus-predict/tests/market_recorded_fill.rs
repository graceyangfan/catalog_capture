// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Recorded Native mainnet BUY and separately identified SDK exit; no network.
#![cfg(feature = "execution")]
use alloy::primitives::U256;
use nautilus_common::{factories::OrderEventFactory, messages::ExecutionReport};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{AccountType, AssetClass, OrderSide, OrderStatus, OrderType, TimeInForce},
    events::{OrderEventAny, OrderFilled, OrderRejected},
    identifiers::{AccountId, Symbol, TraderId, VenueOrderId},
    instruments::{BinaryOption, InstrumentAny},
    orders::{Order, OrderAny, builder::OrderTestBuilder},
    position::Position,
    types::{Currency, Money, Price, Quantity, fixed::FIXED_PRECISION},
};
use nautilus_predict::{
    http::account_models::PredictFeeType,
    websocket::private_messages::{
        PredictWalletEvent, PredictWalletEventKind, PredictWalletRejectionReason,
    },
};

use rust_decimal::Decimal;
use serde_json::Value;

#[test]
fn recorded_valid_market_parameter_matrix_preserves_zero_match_rejections() {
    let fixture: Value = serde_json::from_str(include_str!(
        "fixtures/private_api/market_parameter_matrix_no_match_20261024.json"
    ))
    .unwrap();
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 8);
    let account = AccountId::new("PREDICT-recorded");
    let trader = TraderId::new("RECORDED-001");
    let wallet = "0x1111111111111111111111111111111111111111"
        .parse()
        .unwrap();
    let instrument_id = "2508071-1.PREDICT".parse().unwrap();
    let instrument = BinaryOption::builder()
        .instrument_id(instrument_id)
        .raw_symbol(Symbol::new(
            cases[0]["request"]["tokenId"].as_str().unwrap(),
        ))
        .asset_class(AssetClass::Alternative)
        .currency(Currency::USDT())
        .activation_ns(UnixNanos::default())
        .expiration_ns(UnixNanos::from(i64::MAX as u64))
        .price_precision(2)
        .price_increment(Price::from("0.01"))
        .size_precision(FIXED_PRECISION)
        .size_increment(
            Quantity::from_decimal_dp(Decimal::new(1, u32::from(FIXED_PRECISION)), FIXED_PRECISION)
                .unwrap(),
        )
        .ts_event(UnixNanos::default())
        .ts_init(UnixNanos::default())
        .build()
        .unwrap();
    let factory = OrderEventFactory::new(trader, account, AccountType::Cash, None);
    for case in cases {
        let request = &case["request"];
        let tif = if request["isFillOrKill"] == true {
            TimeInForce::Fok
        } else {
            TimeInForce::Gtc
        };
        let quantity = Quantity::from(case["targetShares"].as_str().unwrap());
        let mut order = OrderTestBuilder::new(OrderType::Market)
            .trader_id(trader)
            .instrument_id(instrument_id)
            .side(OrderSide::Buy)
            .quantity(quantity)
            .time_in_force(tif)
            .build();
        let hash = VenueOrderId::new(request["hash"].as_str().unwrap());
        order
            .apply(factory.generate_order_submitted(&order, 1.into()))
            .unwrap();
        order
            .apply(factory.generate_order_accepted(&order, hash, 2.into(), 2.into()))
            .unwrap();

        // The sanitized projection omits envelope identity. Restore only those
        // fields; preserve captured timestamps, details, reasons and economics.
        // This tests inbound reporting, not Native admission of minimum-out BUY.
        let events = case["walletEvents"].as_array().unwrap();
        assert_eq!(events.len(), 2);
        for (wire, expected) in events
            .iter()
            .zip([OrderStatus::Accepted, OrderStatus::Rejected])
        {
            let mut wire = wire.clone();
            wire["orderHash"] = request["hash"].clone();
            wire["orderId"] = Value::String("1".into());
            wire["walletAddress"] = Value::String(format!("{wallet:#x}"));
            let event: PredictWalletEvent = serde_json::from_value(wire).unwrap();
            let report = nautilus_predict::execution::parse::parse_wallet_report(
                &event,
                &order,
                &instrument,
                account,
                wallet,
                event.order.timestamp_ns().unwrap(),
            )
            .unwrap()
            .unwrap();
            let ExecutionReport::Order(report) = report else {
                panic!("zero-match feedback must not fabricate a fill")
            };
            assert_eq!(report.order_status, expected, "{}", case["name"]);
            assert_eq!(report.venue_order_id, hash);
            assert_eq!(report.time_in_force, tif);
            assert_eq!(report.quantity, quantity);
            assert!(report.filled_qty.is_zero());
            if expected == OrderStatus::Rejected {
                assert_eq!(
                    report.cancel_reason.as_deref(),
                    Some("Predict no market match")
                );
            }
        }
        assert_eq!(case["httpStatus"], 201);
        assert_eq!(case["finalRestStatus"], "CANCELLED");
        assert_eq!(case["finalAmountFilled"], "0");
        assert_eq!(case["openBeforeCleanup"], 0);
        assert_eq!(case["finalRunOpenOrders"], 0);
    }
}

fn recording() -> Value {
    serde_json::from_str(include_str!(
        "fixtures/private_api/native_market_fill_post_only_rejected_20261025.json"
    ))
    .unwrap()
}

#[test]
fn actual_market_buy_preserves_slippage_execution_and_net_inventory() {
    let fixture = recording();
    let wallet = fixture["walletEvents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| serde_json::from_value::<PredictWalletEvent>(v.clone()).unwrap())
        .collect::<Vec<_>>();
    let success = wallet
        .iter()
        .find(|e| {
            matches!(
                e.kind,
                PredictWalletEventKind::OrderTransactionSuccess { .. }
            )
        })
        .unwrap();
    let PredictWalletEventKind::OrderTransactionSuccess {
        fill,
        fee: Some(fee),
        ..
    } = &success.kind
    else {
        panic!("missing actual settlement")
    };
    assert_eq!(success.order.details.market_id, 3732355);
    assert_eq!(success.order.details.outcome_index, 2);
    assert_eq!(success.order.details.price, Decimal::new(39, 2));
    assert_eq!(
        fill.executed_price_wei,
        U256::from(400_000_000_000_000_000u64)
    );
    assert_eq!(
        fill.executed_size_wei,
        U256::from(3_500_000_000_000_000_000u64)
    );
    assert_eq!(
        fill.executed_value_wei,
        U256::from(1_400_000_000_000_000_000u64)
    );
    assert_eq!(fee.fee_type, PredictFeeType::Shares);
    assert_eq!(fee.amount_wei, U256::from(70_000_000_000_000_000u64));
    let native: OrderFilled =
        serde_json::from_value(fixture["nativeEntry"]["fill"].clone()).unwrap();
    assert_eq!(native.order_type, OrderType::Market);
    assert_eq!(native.last_qty, Quantity::from("3.5"));
    assert_eq!(native.last_px, Price::from("0.40"));
    assert_eq!(native.quantity_commission, Some(Quantity::from("0.07")));
    assert_eq!(native.commission, Some(Money::zero(Currency::USDT())));
    let i = BinaryOption::builder()
        .instrument_id(native.instrument_id)
        .raw_symbol(Symbol::new(fixture["tokenId"].as_str().unwrap()))
        .asset_class(AssetClass::Alternative)
        .currency(Currency::USDT())
        .activation_ns(UnixNanos::from(1_792_907_100_000_000_000))
        .expiration_ns(UnixNanos::from(1_792_907_400_000_000_000))
        .price_precision(2)
        .price_increment(Price::from("0.01"))
        .size_precision(FIXED_PRECISION)
        .size_increment(
            Quantity::from_decimal_dp(Decimal::new(1, u32::from(FIXED_PRECISION)), FIXED_PRECISION)
                .unwrap(),
        )
        .ts_event(UnixNanos::default())
        .ts_init(UnixNanos::default())
        .build()
        .unwrap();
    let mut position =
        Position::new_checked(&InstrumentAny::BinaryOption(i), native.clone()).unwrap();
    assert_eq!(position.quantity.as_decimal(), Decimal::new(343, 2));
    let before = serde_json::to_string(&position).unwrap();
    position.try_apply(&native).unwrap();
    assert_eq!(serde_json::to_string(&position).unwrap(), before);
    let cap = Decimal::from_str_exact(fixture["expected"]["signedMaximumPrice"].as_str().unwrap())
        .unwrap();
    assert!(native.last_px.as_decimal() <= cap);
    assert!(native.last_px.as_decimal() > success.order.details.price);
}

#[test]
fn actual_post_only_rejection_and_sdk_exit_keep_their_distinct_provenance() {
    let fixture = recording();
    let rejected: OrderRejected =
        serde_json::from_value(fixture["nativePostOnlyRejection"].clone()).unwrap();
    assert!(rejected.due_post_only);
    assert_eq!(rejected.reason.as_str(), "Predict post-only order rejected");
    let event: PredictWalletEvent = serde_json::from_value(
        fixture["walletEvents"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()
            .clone(),
    )
    .unwrap();
    assert!(matches!(
        event.kind,
        PredictWalletEventKind::OrderNotAccepted {
            reason: PredictWalletRejectionReason::RejectedPostOnly
        }
    ));
    let exit = &fixture["sdkFallbackExit"];
    assert!(exit["source"].as_str().unwrap().contains("SDK fallback"));
    let success = exit["walletEvents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| serde_json::from_value::<PredictWalletEvent>(v.clone()).unwrap())
        .find(|e| {
            matches!(
                e.kind,
                PredictWalletEventKind::OrderTransactionSuccess { .. }
            )
        })
        .unwrap();
    let PredictWalletEventKind::OrderTransactionSuccess {
        fill,
        fee: Some(fee),
        ..
    } = success.kind
    else {
        unreachable!()
    };
    assert_eq!(
        fill.executed_size_wei,
        U256::from(3_430_000_000_000_000_000u64)
    );
    assert_eq!(
        fill.executed_value_wei,
        U256::from(2_572_500_000_000_000_000u64)
    );
    assert_eq!(fee.fee_type, PredictFeeType::Collateral);
    assert_eq!(fee.amount_wei, U256::from(17_150_000_000_000_000u64));
    assert_eq!(exit["remainingChainWei"], "0");
    assert_eq!(exit["openRunOrders"], 0);
}

fn non_fok_recording() -> Value {
    serde_json::from_str(include_str!(
        "fixtures/private_api/native_non_fok_full_plan_two_settlements_20261025.json"
    ))
    .unwrap()
}

#[test]
fn actual_non_fok_full_plan_is_not_an_unmatched_partial_tail() {
    let fixture = non_fok_recording();
    let data = &fixture["request"]["data"];
    assert!(data.get("isFillOrKill").is_none());
    assert_eq!(data["slippageBps"], "0");
    assert_eq!(data["order"]["makerAmount"], "1399200000000000000");
    assert_eq!(data["order"]["takerAmount"], "5830000000000000000");
    let events = fixture["nativeEvents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| serde_json::from_value::<OrderEventAny>(v.clone()).unwrap())
        .collect::<Vec<_>>();
    let partial = OrderAny::from_events(events[..4].to_vec()).unwrap();
    assert_eq!(partial.time_in_force(), TimeInForce::Gtc);
    assert_eq!(partial.status(), OrderStatus::PartiallyFilled);
    assert_eq!(partial.filled_qty().as_decimal(), Decimal::from(5));
    assert_eq!(partial.leaves_qty().as_decimal(), Decimal::new(83, 2));
    let complete = OrderAny::from_events(events.clone()).unwrap();
    assert_eq!(complete.status(), OrderStatus::Filled);
    assert_eq!(complete.filled_qty().as_decimal(), Decimal::new(583, 2));
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, OrderEventAny::Canceled(_)))
    );
    let fills = events
        .iter()
        .filter_map(|e| {
            if let OrderEventAny::Filled(f) = e {
                Some(f.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let first = &fills[0];
    let instrument = InstrumentAny::BinaryOption(
        BinaryOption::builder()
            .instrument_id(first.instrument_id)
            .raw_symbol(Symbol::new(fixture["tokenId"].as_str().unwrap()))
            .asset_class(AssetClass::Alternative)
            .currency(Currency::USDT())
            .activation_ns(UnixNanos::from(fixture["activationNs"].as_u64().unwrap()))
            .expiration_ns(UnixNanos::from(fixture["expirationNs"].as_u64().unwrap()))
            .price_precision(2)
            .price_increment(Price::from("0.01"))
            .size_precision(FIXED_PRECISION)
            .size_increment(
                Quantity::from_decimal_dp(
                    Decimal::new(1, u32::from(FIXED_PRECISION)),
                    FIXED_PRECISION,
                )
                .unwrap(),
            )
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap(),
    );
    let mut position = Position::new_checked(&instrument, first.clone()).unwrap();
    assert_eq!(position.quantity.as_decimal(), Decimal::new(49, 1));
    position.try_apply(&fills[1]).unwrap();
    assert_eq!(position.quantity.as_decimal(), Decimal::new(57134, 4));
    let before = serde_json::to_string(&position).unwrap();
    position.try_apply(first).unwrap();
    assert_eq!(serde_json::to_string(&position).unwrap(), before);
    let plan = fixture["walletEvents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| serde_json::from_value::<PredictWalletEvent>(v.clone()).unwrap())
        .find(|e| matches!(&e.kind, PredictWalletEventKind::OrderMarketMatchPlan { .. }))
        .unwrap();
    let PredictWalletEventKind::OrderMarketMatchPlan {
        amount,
        planned_fill,
        remainder_cancelled,
        settlement_ids,
        ..
    } = &plan.kind
    else {
        unreachable!()
    };
    assert_eq!(*amount, U256::from(5_830_000_000_000_000_000u64));
    assert_eq!(amount, planned_fill);
    assert_eq!(*remainder_cancelled, U256::ZERO);
    assert_eq!(settlement_ids.len(), 2);
    let report = nautilus_predict::execution::parse::parse_wallet_report(
        &plan,
        &partial,
        match &instrument {
            InstrumentAny::BinaryOption(i) => i,
            _ => unreachable!(),
        },
        partial.account_id().unwrap(),
        plan.order.wallet_address,
        UnixNanos::from(1_792_913_000_000_000_000),
    )
    .unwrap();
    assert!(
        report.is_none(),
        "a repeated full plan does not cancel the not-yet-settled 0.83 shares"
    );
}

#[test]
fn actual_complementary_buy_matches_and_cash_audit_preserve_their_provenance() {
    let fixture = non_fok_recording();
    let scale = U256::from(1_000_000_000_000_000_000u64);
    for row in fixture["entryMatches"].as_array().unwrap() {
        assert_eq!(row["taker"]["quoteType"], "Bid");
        assert_eq!(row["taker"]["outcome"]["indexSet"], 1);
        let taker: U256 = row["taker"]["price"].as_str().unwrap().parse().unwrap();
        for maker in row["makers"].as_array().unwrap() {
            assert_eq!(maker["quoteType"], "Bid");
            assert_eq!(maker["outcome"]["indexSet"], 2);
            let price: U256 = maker["price"].as_str().unwrap().parse().unwrap();
            assert_eq!(taker + price, scale);
        }
    }
    let mut minted_yes = U256::ZERO;
    let receipts = fixture["mintEvidence"]["matches"].as_array().unwrap();
    assert_eq!(receipts.len(), 2);
    for receipt in receipts {
        assert_eq!(receipt["receiptStatus"], 1);
        let mints = receipt["transfers"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["isMintFromZero"] == true)
            .collect::<Vec<_>>();
        assert_eq!(
            mints.len(),
            2,
            "both complementary outcome tokens were minted"
        );
        assert_eq!(mints[0]["quantityWei"], mints[1]["quantityWei"]);
        assert_ne!(mints[0]["tokenId"], mints[1]["tokenId"]);
        let yes = mints
            .iter()
            .find(|e| e["tokenId"] == fixture["tokenId"])
            .unwrap();
        minted_yes += yes["quantityWei"]
            .as_str()
            .unwrap()
            .parse::<U256>()
            .unwrap();
    }
    assert_eq!(minted_yes, U256::from(5_830_000_000_000_000_000u64));
    let expected = &fixture["expected"];
    let decimal = |key: &str| Decimal::from_str_exact(expected[key].as_str().unwrap()).unwrap();
    assert_eq!(
        decimal("exitGross") - decimal("exitFee") - decimal("entryCash"),
        decimal("cashDelta")
    );
    assert_eq!(fixture["sdkExit"]["finalChain"]["tokenWei"], "0");
    assert_eq!(expected["partialTailValidated"], false);
}
