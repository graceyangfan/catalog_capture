// -------------------------------------------------------------------------------------------------
// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
// See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Recorded maker executions through report collection and the Native order model.
//! Local Accepted context and public identities are rebound; economic values are retained.

use nautilus_model::enums::LiquiditySide;
use nautilus_predict::{common::contracts::PredictExchangeType, signing::PredictOrderHasher};

use super::*;

const ORIGINAL: &str = "0xf57ca5fc7af9c51726c7dc525ae9092dd36c13990244426d219862b1f0e16b80";

#[tokio::test]
async fn recorded_maker_sell_uses_its_three_legs_and_naturally_becomes_partially_filled() {
    let saved: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/live_order_lifecycle_survey_20261025.json"
    ))
    .unwrap();
    let events = saved["events"].as_array().unwrap();
    let mut row = events
        .iter()
        .find(|event| event["path"] == format!("/v1/orders/{ORIGINAL}"))
        .unwrap()["responseBody"]["data"]
        .clone();
    assert_eq!(row["strategy"], "LIMIT");
    assert_eq!(row["status"], "FILLED");
    assert_eq!(row["amountFilled"], "4174800000000000000");
    row["marketId"] = json!(42);
    row["order"]["maker"] = json!(WALLET);
    row["order"]["signer"] = json!(WALLET);
    row["order"]["tokenId"] = json!("11");
    let fields = serde_json::from_value(row["order"].clone()).unwrap();
    let hash = PredictOrderHasher::new(PredictEnvironment::BnbMainnet).order_hash(
        &fields,
        PredictExchangeType::from_market_flags(
            row["isNegRisk"].as_bool().unwrap(),
            row["isYieldBearing"].as_bool().unwrap(),
        ),
    );
    row["order"]["hash"] = json!(hash);
    let mut executions = Vec::new();
    for event in events.iter().filter(|event| {
        event["path"] == "/v1/orders/matches" && event["query"]["isSignerMaker"][0] == "true"
    }) {
        for original in event["responseBody"]["data"].as_array().unwrap() {
            if !original["makers"]
                .as_array()
                .unwrap()
                .iter()
                .any(|leg| leg["hash"] == ORIGINAL)
            {
                continue;
            }
            let mut execution = original.clone();
            execution["market"]["id"] = json!(42);
            for outcome in execution["market"]["outcomes"].as_array_mut().unwrap() {
                outcome["onChainId"] = json!(if outcome["indexSet"] == 1 { "11" } else { "22" });
            }
            let rebind = |leg: &mut Value| {
                leg["outcome"]["onChainId"] = json!(if leg["outcome"]["indexSet"] == 1 {
                    "11"
                } else {
                    "22"
                });
                if leg["hash"] == ORIGINAL {
                    leg["signer"] = json!(WALLET);
                    leg["hash"] = json!(hash);
                }
            };
            rebind(&mut execution["taker"]);
            for leg in execution["makers"].as_array_mut().unwrap() {
                rebind(leg);
            }
            executions.push(execution);
        }
    }
    assert_eq!(executions.len(), 3);
    // Two real aggregate matches have five shares, larger than this maker's leg.
    assert_eq!(
        executions
            .iter()
            .filter(|v| v["amountFilled"] == "5000000000000000000")
            .count(),
        2
    );
    let server = server(vec![]).await;
    let (client, mut rx, cache) = client(&server);
    for index in [1, 2] {
        cache
            .borrow_mut()
            .add_instrument(instrument(index).into())
            .unwrap();
    }
    let mut first = executions[..2].to_vec();
    first.push(executions[0].clone());
    server.state.match_pages.lock().unwrap().extend([
        (
            200,
            json!({"success":true,"data":first,"cursor":"overlap"}),
            0,
        ),
        (
            200,
            json!({"success":true,"data":executions,"cursor":null}),
            0,
        ),
    ]);
    let mut cmd = GenerateFillReports::new(
        UUID4::new(),
        UnixNanos::default(),
        Some(instrument_id(1)),
        None,
        None,
        None,
        None,
        None,
    );
    let venue_id = VenueOrderId::new(format!("{hash:#x}"));
    cmd.venue_order_id = Some(venue_id);
    let reports = client.generate_fill_reports(cmd).await.unwrap();
    assert_eq!(reports.len(), 3);
    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(trader_id())
        .instrument_id(instrument_id(1))
        .side(OrderSide::Sell)
        .quantity(Quantity::from("4.1748"))
        .price(Price::from("0.500"))
        .build();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    order
        .apply(factory.generate_order_submitted(&order, 1.into()))
        .unwrap();
    order
        .apply(factory.generate_order_accepted(&order, venue_id, 2.into(), 2.into()))
        .unwrap();
    for ((report, quantity), status) in reports.iter().zip(["1.51999", "0.89", "1.76481"]).zip([
        OrderStatus::PartiallyFilled,
        OrderStatus::PartiallyFilled,
        OrderStatus::Filled,
    ]) {
        assert_eq!(report.last_qty, Quantity::from(quantity));
        assert_eq!(report.last_px, Price::from("0.500"));
        assert_eq!(report.order_side, OrderSide::Sell);
        assert_eq!(report.liquidity_side, LiquiditySide::Maker);
        assert!(report.commission.is_zero());
        let event = factory.generate_order_filled(
            &order,
            report.venue_order_id,
            None,
            report.trade_id,
            report.last_qty,
            report.last_px,
            Currency::USDT(),
            Some(report.commission),
            report.liquidity_side,
            report.ts_event,
            report.ts_init,
        );
        order.apply(event).unwrap();
        assert_eq!(order.status(), status);
    }
    assert_eq!(order.filled_qty(), Quantity::from("4.1748"));
    assert!(order.leaves_qty().is_zero());
    let rest: nautilus_predict::http::account_models::PredictOrderData =
        serde_json::from_value(row).unwrap();
    assert_eq!(
        rest.status,
        nautilus_predict::http::account_models::PredictOrderStatus::Filled
    );
    assert_eq!(rest.amount_filled, "4174800000000000000");
    assert!(rx.try_recv().is_err());
    assert!(server.state.creations.lock().unwrap().is_empty());
    assert!(server.state.removals.lock().unwrap().is_empty());
}
