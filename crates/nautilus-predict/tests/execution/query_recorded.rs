// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Real read-only terminal response replay. Local accepted context is explicitly derived.
use nautilus_common::messages::execution::QueryOrder;
use nautilus_predict::{common::contracts::PredictExchangeType, signing::PredictOrderHasher};

use super::*;

async fn replay() -> Value {
    let source: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/query_commands_readonly_20261016T145520Z_41000.json"
    ))
    .unwrap();
    let actual = source["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| {
            e["phase"] == "query-order-1"
                && e["path"].as_str().unwrap().starts_with("/v1/orders/0x")
        })
        .unwrap();
    assert_eq!(actual["status"], 200);
    assert_eq!(actual["officialSdkHashVerifiedBeforeRedaction"], true);
    let mut row = actual["responseBody"]["data"].clone();
    assert_eq!(row["status"], "CANCELLED");
    assert_eq!(row["amountFilled"], "0");
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
    row["order"]["hash"] = json!(format!("{hash:#x}"));
    let qty = Decimal::from_str_exact(row["order"]["makerAmount"].as_str().unwrap()).unwrap()
        / Decimal::from(1_000_000_000_000_000_000u64);
    let collateral = Decimal::from_str_exact(row["order"]["takerAmount"].as_str().unwrap())
        .unwrap()
        / Decimal::from(1_000_000_000_000_000_000u64);
    let server = server(vec![]).await;
    *server.state.position_snapshot.lock().unwrap() = Some(vec![]);
    *server.state.match_snapshot.lock().unwrap() = Some(vec![]);
    *server.state.order_snapshot.lock().unwrap() = Some(json!({"success":true,"data":row}));
    let balance = source["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| {
            e.get("requestBody")
                .is_some_and(|b| b["method"] == "eth_call")
                && e["status"] == 200
        })
        .unwrap()["responseBody"]["result"]
        .as_str()
        .unwrap();
    server.state.rpc_frame.lock().unwrap().0 =
        u128::from_str_radix(balance.trim_start_matches("0x"), 16).unwrap();
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(trader_id())
        .instrument_id(instrument_id(1))
        .side(OrderSide::Sell)
        .quantity(Quantity::from_decimal_dp(qty, 16).unwrap())
        .price(Price::from_decimal_dp(collateral / qty, 3).unwrap())
        .build();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    order
        .apply(factory.generate_order_submitted(&order, UnixNanos::from(10u64)))
        .unwrap();
    order
        .apply(factory.generate_order_accepted(
            &order,
            VenueOrderId::new(format!("{hash:#x}")),
            UnixNanos::from(100u64),
            UnixNanos::from(100u64),
        ))
        .unwrap();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let mut engine = cancellations::engine(&cache);
    let mut feedback = vec![];
    for _ in 0..2 {
        let current = cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .clone();
        client
            .query_order(QueryOrder::new(
                trader_id(),
                Some(ClientId::new("PREDICT")),
                current.strategy_id(),
                current.instrument_id(),
                current.client_order_id(),
                current.venue_order_id(),
                UUID4::new(),
                UnixNanos::from(OBSERVED),
                None,
                None,
            ))
            .unwrap();
        let report = timeout(Duration::from_secs(3), async {
            loop {
                match next(&mut rx).await {
                    ExecutionEvent::Report(report) => break report,
                    ExecutionEvent::Account(state) => msgbus::send_account_state(
                        MessagingSwitchboard::portfolio_update_account(),
                        &state,
                    ),
                    ExecutionEvent::Order(event) => engine.process(&event),
                    _ => panic!("unexpected query event"),
                }
            }
        })
        .await
        .unwrap();
        engine.reconcile_execution_report(&report);
        let cache = cache.borrow();
        let current = cache.order(&order.client_order_id()).unwrap();
        assert_eq!(current.status(), OrderStatus::Canceled);
        assert!(current.filled_qty().is_zero());
        assert!(
            cache
                .positions_open(None, None, None, None, None)
                .is_empty()
        );
        feedback.push(json!({"status":current.status().to_string(),"events":current.events().len(),"filledQty":current.filled_qty().to_string()}));
    }
    assert_eq!(feedback[0], feedback[1]);
    assert_eq!(server.state.order_requests.lock().unwrap().len(), 2);
    assert!(server.state.match_requests.lock().unwrap().is_empty());
    assert!(server.state.creations.lock().unwrap().is_empty());
    assert!(server.state.removals.lock().unwrap().is_empty());
    client.disconnect().await.unwrap();
    json!({"source":"offline Native QueryOrder + ExecutionEngine replay of actual read-only CANCELLED response","primarySource":"query_commands_readonly_20261016T145520Z_41000.json","transforms":["market/token/wallet rebound; SDK-compatible hash recalculated","local Accepted context simulates missing cancellation receipt","RPC balance value from actual capture; block identity synthetic"],"derivedOrder":row,"nativeFeedback":feedback,"orderWrites":0})
}
#[tokio::test]
async fn actual_terminal_query_response_reconciles_once_and_repeats_without_new_events() {
    replay().await;
}
#[tokio::test]
#[ignore = "explicit offline recorded query replay capture, create_new only"]
async fn record_terminal_query_replay() {
    let out = replay().await;
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/query_commands_native_replay_20261016.json");
    serde_json::to_writer_pretty(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap(),
        &out,
    )
    .unwrap();
}
