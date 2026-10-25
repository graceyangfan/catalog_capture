// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Native TIF is authoritative; loopback tests do not establish venue all-or-none execution.
use nautilus_common::messages::execution::QueryOrder;
use nautilus_model::{enums::TimeInForce, events::OrderEventAny, identifiers::ClientOrderId};

use super::*;

fn market(side: OrderSide, outcome: u8, tif: TimeInForce) -> OrderAny {
    let mut factory = nautilus_common::factories::OrderFactory::new(
        trader_id(),
        nautilus_model::identifiers::StrategyId::new("FOK-001"),
        None,
        None,
        Rc::new(RefCell::new(VirtualClock::new())),
        false,
        false,
    );
    factory.market(
        instrument_id(outcome),
        side,
        Quantity::from(100),
        Some(tif),
        None,
        None,
        None,
        None,
        None,
        Some(ClientOrderId::new("MARKET-FOK-001")),
    )
}

fn query(order: &OrderAny) -> QueryOrder {
    QueryOrder::new(
        trader_id(),
        Some(ClientId::new("PREDICT")),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        None,
        UUID4::new(),
        UnixNanos::from(OBSERVED),
        None,
        None,
    )
}

fn response(body: &Value, status: &str) -> Value {
    let mut row = rest_order_data(status, 1, OrderSide::Buy);
    row["strategy"] = json!("MARKET");
    row["order"] = body["data"]["order"].clone();
    json!({"success":true,"data":row})
}

#[tokio::test]
async fn native_market_tif_maps_to_fok_without_changing_price_or_default_behavior() {
    for side in [OrderSide::Buy, OrderSide::Sell] {
        for outcome in [1, 2] {
            for tif in [TimeInForce::Gtc, TimeInForce::Fok] {
                let server = server(vec![]).await;
                let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
                let _portfolio = portfolio(&cache);
                cache
                    .borrow_mut()
                    .add_instrument(InstrumentAny::BinaryOption(instrument(outcome)))
                    .unwrap();
                connect_reported(&mut client, &mut rx).await;
                let own = market(side, outcome, tif);
                cache
                    .borrow_mut()
                    .add_order(own.clone(), None, None, false)
                    .unwrap();
                client
                    .submit_order(market_preparation::priced(&own, json!("0.020"), 100))
                    .unwrap();
                let mut engine = submissions::engine(&cache);
                submissions::apply(&mut engine, &next(&mut rx).await);
                exit_orders::barrier("Native MARKET TIF POST", || {
                    !server.state.creations.lock().unwrap().is_empty()
                })
                .await;
                let posts = server.state.creations.lock().unwrap().clone();
                assert_eq!(posts.len(), 1);
                let body = &posts[0]["data"];
                assert_eq!(body["strategy"], "MARKET");
                assert_eq!(
                    body.get("isFillOrKill"),
                    (tif == TimeInForce::Fok).then_some(&json!(true))
                );
                assert_eq!(body["pricePerShare"], "20000000000000000");
                assert_eq!(body["slippageBps"], "100");
                assert_eq!(body["isMinAmountOut"], false);
                assert!(body.get("isPostOnly").is_none());
                assert_eq!(body["order"]["expiration"], 300);
                let collateral = if side == OrderSide::Buy {
                    "makerAmount"
                } else {
                    "takerAmount"
                };
                assert_eq!(
                    body["order"][collateral],
                    if side == OrderSide::Buy {
                        "2020000000000000000"
                    } else {
                        "1980000000000000000"
                    }
                );
                assert_eq!(
                    cache
                        .borrow()
                        .order(&own.client_order_id())
                        .unwrap()
                        .time_in_force(),
                    tif
                );
                assert_eq!(*server.state.book_requests.lock().unwrap(), 0);
                client.disconnect().await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn fok_missing_price_and_market_ioc_are_denied_before_transmission() {
    for (tif, price) in [(TimeInForce::Fok, None), (TimeInForce::Ioc, Some("0.020"))] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        let own = market(OrderSide::Buy, 1, tif);
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        let command = price.map_or_else(
            || submissions::submit(&own),
            |p| market_preparation::priced(&own, json!(p), 100),
        );
        client.submit_order(command).unwrap();
        assert!(matches!(
            next(&mut rx).await,
            ExecutionEvent::Order(OrderEventAny::Denied(_))
        ));
        assert!(server.state.creations.lock().unwrap().is_empty());
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn zero_fill_fok_terminal_recovers_without_wallet_ack_or_resubmission() {
    for status in ["CANCELLED", "EXPIRED"] {
        let server = server(vec![]).await;
        server
            .state
            .create_replies
            .lock()
            .unwrap()
            .push_back((500, 0, vec![]));
        let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        let own = market(OrderSide::Buy, 1, TimeInForce::Fok);
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        let mut engine = submissions::engine(&cache);
        let mut command = submissions::submit(&own);
        command.params = Some(nautilus_core::Params::new(
            [
                ("market_price".into(), json!("0.020")),
                ("market_order_slippage_bps".into(), json!(250)),
            ]
            .into(),
        ));
        client.submit_order(command).unwrap();
        submissions::apply(&mut engine, &next(&mut rx).await);
        exit_orders::barrier("ambiguous FOK POST", || {
            !server.state.creations.lock().unwrap().is_empty()
        })
        .await;
        let body = server.state.creations.lock().unwrap()[0].clone();
        assert_eq!(body["data"]["slippageBps"], "250");
        assert_eq!(body["data"]["order"]["makerAmount"], "2050000000000000000");
        let row = response(&body, status);
        server.state.orders.lock().unwrap().push_back(row.clone());
        client.query_order(query(&own)).unwrap();
        let expected = if status == "CANCELLED" {
            OrderStatus::Canceled
        } else {
            OrderStatus::Expired
        };
        timeout(Duration::from_secs(3), async {
            while cache
                .borrow()
                .order(&own.client_order_id())
                .unwrap()
                .status()
                != expected
            {
                submissions::apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .unwrap();
        let restored = cache.borrow().order_owned(&own.client_order_id()).unwrap();
        assert_eq!(restored.time_in_force(), TimeInForce::Fok);
        assert!(restored.filled_qty().is_zero());
        assert!(
            cache
                .borrow()
                .positions(None, None, None, None, None)
                .is_empty()
        );
        // A repeated query must preserve terminal state, including after local snapshots refresh.
        server.state.orders.lock().unwrap().push_back(row);
        client.query_order(query(&restored)).unwrap();
        loop {
            let event = next(&mut rx).await;
            let report = matches!(event, ExecutionEvent::Report(_));
            submissions::apply(&mut engine, &event);
            if report {
                break;
            }
        }
        assert_eq!(
            cache
                .borrow()
                .order(&own.client_order_id())
                .unwrap()
                .status(),
            expected
        );
        assert_eq!(server.state.creations.lock().unwrap().len(), 1);
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn wallet_no_match_rejection_agrees_with_cancelled_rest_and_late_ack() {
    for tif in [TimeInForce::Fok, TimeInForce::Gtc] {
        no_match_recovery_round_trip(tif).await;
    }
}

#[tokio::test]
async fn recorded_default_market_matrix_closes_without_a_strategy_cancel() {
    use nautilus_common::msgbus::{TypedHandler, switchboard};
    let saved: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/market_parameter_matrix_no_match_20261024.json"
    ))
    .unwrap();
    let history: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/live_market_history_readonly_20261025.json"
    ))
    .unwrap();
    let cases = saved["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case["request"]["isMinAmountOut"] != true)
        .collect::<Vec<_>>();
    assert_eq!(
        cases.len(),
        6,
        "minimum-out is not a Native submission mode"
    );
    for case in cases {
        let recorded = &case["request"];
        let events = case["walletEvents"].as_array().unwrap();
        let started = events[0]["timestamp"].as_u64().unwrap() * 1_000_000 - 1_000_000_000;
        let server = server(vec![]).await;
        *server.state.market_fee.lock().unwrap() = 200;
        *server.state.market_yield_bearing.lock().unwrap() = true;
        let (mut client, mut rx, cache, clock) =
            submissions::setup_client_with_clock(&server, started);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        let tif = if recorded["isFillOrKill"] == true {
            TimeInForce::Fok
        } else {
            TimeInForce::Gtc
        };
        let own = OrderTestBuilder::new(OrderType::Market)
            .trader_id(trader_id())
            .instrument_id(instrument_id(1))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(case["targetShares"].as_str().unwrap()))
            .time_in_force(tif)
            .build();
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        let callbacks = Rc::new(RefCell::new(Vec::new()));
        let handler = TypedHandler::from({
            let callbacks = callbacks.clone();
            let cache = cache.clone();
            let id = own.client_order_id();
            move |event: &OrderEventAny| {
                if event.client_order_id() != id {
                    return;
                }
                let expected = match event {
                    OrderEventAny::Accepted(_) => OrderStatus::Accepted,
                    OrderEventAny::Rejected(rejected) => {
                        assert_eq!(rejected.reason.as_str(), "Predict no market match");
                        OrderStatus::Rejected
                    }
                    OrderEventAny::Canceled(_) | OrderEventAny::Filled(_) => {
                        panic!("no-match rejection must not fabricate cancel or fill")
                    }
                    _ => return,
                };
                let cache = cache.borrow();
                let order = cache.order(&id).unwrap();
                assert_eq!(order.status(), expected);
                assert!(order.filled_qty().is_zero());
                callbacks.borrow_mut().push(expected);
            }
        });
        let topic = switchboard::get_event_order_topic(own.strategy_id());
        msgbus::subscribe_order_events(topic.into(), handler.clone(), None);
        let reference = Decimal::from_str_exact(recorded["pricePerShare"].as_str().unwrap())
            .unwrap()
            / Decimal::from(1_000_000_000_000_000_000u64);
        client
            .submit_order(market_preparation::priced(
                &own,
                json!(reference.to_string()),
                0,
            ))
            .unwrap();
        let mut engine = submissions::engine(&cache);
        submissions::apply(&mut engine, &next(&mut rx).await);
        exit_orders::barrier("recorded MARKET request", || {
            !server.state.creations.lock().unwrap().is_empty()
        })
        .await;
        let body = server.state.creations.lock().unwrap()[0].clone();
        let data = &body["data"];
        for field in ["strategy", "pricePerShare"] {
            assert_eq!(data[field], recorded[field]);
        }
        for field in ["makerAmount", "takerAmount", "feeRateBps"] {
            assert_eq!(data["order"][field], recorded[field]);
        }
        assert_eq!(data["slippageBps"], "0");
        assert_eq!(data["isMinAmountOut"], false);
        assert_eq!(
            data.get("isFillOrKill"),
            (tif == TimeInForce::Fok).then_some(&json!(true))
        );
        assert!(
            callbacks.borrow().is_empty(),
            "HTTP 201 is not Native acceptance"
        );
        for (index, recorded_event) in events.iter().enumerate() {
            let mut wire = recorded_event.clone();
            // Rebind only envelope/market identity to the loopback order. Keep
            // actual event times, quantities, price, strategy and reason.
            wire["orderHash"] = data["order"]["hash"].clone();
            wire["orderId"] = json!("12345");
            wire["walletAddress"] = json!(WALLET);
            wire["details"]["marketId"] = json!(42);
            clock.set_time(UnixNanos::from(
                wire["timestamp"].as_u64().unwrap() * 1_000_000,
            ));
            server.wallet(wire);
            timeout(Duration::from_secs(3), async {
                while callbacks.borrow().len() <= index {
                    submissions::apply(&mut engine, &next(&mut rx).await);
                }
            })
            .await
            .unwrap();
            assert!(server.state.removals.lock().unwrap().is_empty());
        }
        assert_eq!(
            *callbacks.borrow(),
            vec![OrderStatus::Accepted, OrderStatus::Rejected]
        );
        assert_eq!(case["finalRestStatus"], "CANCELLED");
        assert_eq!(case["finalAmountFilled"], "0");
        assert_eq!(case["openBeforeCleanup"], 0);
        // Combine original wallet events with a later real historical snapshot.
        // Rebind identity/signing fields only; retained status/cumulative are venue
        // facts from those two observations, not a reconstructed REST wrapper.
        let mut row = history["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["responseBody"]["data"]["order"]["hash"] == recorded["hash"])
            .unwrap()["responseBody"]
            .clone();
        assert_eq!(row["data"]["status"], case["finalRestStatus"]);
        assert_eq!(row["data"]["amountFilled"], case["finalAmountFilled"]);
        for field in ["makerAmount", "takerAmount", "feeRateBps"] {
            assert_eq!(row["data"]["order"][field], data["order"][field]);
        }
        row["data"]["order"] = data["order"].clone();
        row["data"]["marketId"] = json!(42);
        row["data"]["id"] = json!("12345");
        server.state.orders.lock().unwrap().push_back(row);
        client.query_order(query(&own)).unwrap();
        loop {
            let event = next(&mut rx).await;
            let report = matches!(event, ExecutionEvent::Report(_));
            if let ExecutionEvent::Report(ExecutionReport::Order(report)) = &event {
                assert_eq!(report.order_status, OrderStatus::Rejected);
            }
            submissions::apply(&mut engine, &event);
            if report {
                break;
            }
        }
        assert_eq!(
            callbacks.borrow().len(),
            2,
            "recovery must not add another terminal callback"
        );
        assert_eq!(server.state.creations.lock().unwrap().len(), 1);
        assert!(server.state.removals.lock().unwrap().is_empty());
        assert_eq!(*server.state.book_requests.lock().unwrap(), 0);
        msgbus::unsubscribe_order_events(topic.into(), &handler);
        client.disconnect().await.unwrap();
    }
}

async fn no_match_recovery_round_trip(tif: TimeInForce) {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let own = market(OrderSide::Buy, 1, tif);
    cache
        .borrow_mut()
        .add_order(own.clone(), None, None, false)
        .unwrap();
    let mut engine = submissions::engine(&cache);
    client
        .submit_order(market_preparation::priced(&own, json!("0.020"), 100))
        .unwrap();
    submissions::apply(&mut engine, &next(&mut rx).await);
    exit_orders::barrier("MARKET POST", || {
        !server.state.creations.lock().unwrap().is_empty()
    })
    .await;
    let body = server.state.creations.lock().unwrap()[0].clone();
    for kind in ["orderAccepted", "orderNotAccepted"] {
        let mut wire = event(kind, 1);
        wire["orderHash"] = body["data"]["order"]["hash"].clone();
        wire["details"]["strategyType"] = json!("MARKET");
        wire["details"]["price"] = json!("0.020");
        wire["reason"] = json!("noMarketMatch");
        server.wallet(wire);
        let expected = if kind == "orderAccepted" {
            OrderStatus::Accepted
        } else {
            OrderStatus::Rejected
        };
        timeout(Duration::from_secs(3), async {
            while cache
                .borrow()
                .order(&own.client_order_id())
                .unwrap()
                .status()
                != expected
            {
                submissions::apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .unwrap();
        // Neither venue acceptance nor noMarketMatch initiates a removal command.
        // A strategy's cancel-on-Accepted callback is a separate caller action.
        assert!(server.state.removals.lock().unwrap().is_empty());
    }
    server
        .state
        .orders
        .lock()
        .unwrap()
        .push_back(response(&body, "CANCELLED"));
    client.query_order(query(&own)).unwrap();
    loop {
        let next_event = next(&mut rx).await;
        let report = matches!(next_event, ExecutionEvent::Report(_));
        if let ExecutionEvent::Report(ExecutionReport::Order(report)) = &next_event {
            assert_eq!(report.order_status, OrderStatus::Rejected);
            assert_eq!(report.time_in_force, tif);
        }
        submissions::apply(&mut engine, &next_event);
        if report {
            break;
        }
    }
    let mut late = event("orderAccepted", 1);
    late["orderHash"] = body["data"]["order"]["hash"].clone();
    late["details"]["strategyType"] = json!("MARKET");
    let closed = cache.borrow().order_owned(&own.client_order_id()).unwrap();
    let late: nautilus_predict::websocket::private_messages::PredictWalletEvent =
        serde_json::from_value(late).unwrap();
    assert!(
        nautilus_predict::execution::parse::parse_wallet_report(
            &late,
            &closed,
            &instrument(1),
            account_id(),
            WALLET.parse().unwrap(),
            UnixNanos::from(OBSERVED)
        )
        .unwrap()
        .is_none()
    );

    assert_eq!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Rejected
    );
    assert!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .filled_qty()
            .is_zero()
    );
    assert_eq!(server.state.creations.lock().unwrap().len(), 1);
    client.disconnect().await.unwrap();
}

#[test]
fn restored_native_market_preserves_rejection_without_masking_contradictory_rest_evidence() {
    use nautilus_predict::{
        execution::parse::parse_rest_order_report, http::account_models::PredictOrderData,
    };
    for tif in [TimeInForce::Fok, TimeInForce::Gtc] {
        let mut native = market(OrderSide::Buy, 1, tif);
        let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
        native
            .apply(factory.generate_order_submitted(&native, UnixNanos::from(1)))
            .unwrap();
        native
            .apply(factory.generate_order_accepted(
                &native,
                VenueOrderId::new(HASH),
                UnixNanos::from(1),
                UnixNanos::from(1),
            ))
            .unwrap();
        native
            .apply(factory.generate_order_rejected(
                &native,
                "Predict no market match",
                UnixNanos::from(2),
                UnixNanos::from(2),
                false,
            ))
            .unwrap();
        // Native persistence owns TIF; REST signed economic terms do not encode FOK.
        let restored: OrderAny =
            serde_json::from_str(&serde_json::to_string(&native).unwrap()).unwrap();
        assert_eq!(restored.time_in_force(), tif);
        let mut row = rest_order_data("CANCELLED", 1, OrderSide::Buy);
        row["strategy"] = json!("MARKET");
        row["order"]["makerAmount"] = json!("2020000000000000000");
        for contradiction in ["none", "filled", "open", "wrong_hash"] {
            let mut current = row.clone();
            match contradiction {
                "filled" => current["amountFilled"] = json!("1"),
                "open" => current["status"] = json!("OPEN"),
                "wrong_hash" => current["order"]["hash"] = json!(format!("0x{}", "44".repeat(32))),
                _ => {}
            }
            let current: PredictOrderData = serde_json::from_value(current).unwrap();
            let report = parse_rest_order_report(
                &current,
                &restored,
                &instrument(1),
                account_id(),
                WALLET.parse().unwrap(),
                UnixNanos::from(OBSERVED),
            );
            if contradiction == "none" {
                let report = report.unwrap();
                assert_eq!(report.order_status, OrderStatus::Rejected);
                assert_eq!(report.time_in_force, tif);
                assert!(report.filled_qty.is_zero());
            } else {
                assert!(report.is_err());
            }
        }
    }
}

#[tokio::test]
async fn fok_wire_parameter_cannot_override_native_time_in_force() {
    for tif in [TimeInForce::Gtc, TimeInForce::Fok] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        let own = market(OrderSide::Buy, 1, tif);
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        let mut cmd = market_preparation::priced(&own, json!("0.020"), 100);
        cmd.params = Some(nautilus_core::Params::new(
            [
                ("market_price".into(), json!("0.020")),
                ("isFillOrKill".into(), json!(tif != TimeInForce::Fok)),
            ]
            .into(),
        ));
        client.submit_order(cmd).unwrap();
        let ExecutionEvent::Order(OrderEventAny::Denied(denied)) = next(&mut rx).await else {
            panic!("wire flag must be denied");
        };
        assert!(denied.reason.as_str().contains("native TimeInForce::Fok"));
        assert!(server.state.creations.lock().unwrap().is_empty());
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn per_order_slippage_changes_signed_bounds_and_http_metadata_together() {
    for side in [OrderSide::Buy, OrderSide::Sell] {
        for outcome in [1, 2] {
            for tif in [TimeInForce::Gtc, TimeInForce::Fok] {
                for supplied_bps in [None, Some(0u32), Some(250)] {
                    let bps = supplied_bps.unwrap_or(0);
                    let server = server(vec![]).await;
                    let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
                    let _portfolio = portfolio(&cache);
                    cache
                        .borrow_mut()
                        .add_instrument(InstrumentAny::BinaryOption(instrument(outcome)))
                        .unwrap();
                    connect_reported(&mut client, &mut rx).await;
                    let own = market(side, outcome, tif);
                    cache
                        .borrow_mut()
                        .add_order(own.clone(), None, None, false)
                        .unwrap();
                    let mut command = submissions::submit(&own);
                    let mut params = [("market_price".into(), json!("0.020"))]
                        .into_iter()
                        .collect::<indexmap::IndexMap<String, Value>>();
                    if let Some(bps) = supplied_bps {
                        params.insert("market_order_slippage_bps".into(), json!(bps));
                    }
                    command.params = Some(nautilus_core::Params::new(params));
                    client.submit_order(command).unwrap();
                    let mut engine = submissions::engine(&cache);
                    submissions::apply(&mut engine, &next(&mut rx).await);
                    exit_orders::barrier("per-order slippage signed POST", || {
                        !server.state.creations.lock().unwrap().is_empty()
                    })
                    .await;
                    let bodies = server.state.creations.lock().unwrap().clone();
                    assert_eq!(bodies.len(), 1);
                    let body = &bodies[0]["data"];
                    assert_eq!(body["strategy"], "MARKET");
                    assert_eq!(body["slippageBps"], bps.to_string());
                    assert_eq!(body["pricePerShare"], "20000000000000000");
                    assert_eq!(body["isMinAmountOut"], false);
                    assert_eq!(
                        body.get("isFillOrKill"),
                        (tif == TimeInForce::Fok).then_some(&json!(true))
                    );
                    // Independent SDK arithmetic: 100 shares * .02, then BUY +bps / SELL -bps.
                    let factor = if side == OrderSide::Buy {
                        10_000 + bps
                    } else {
                        10_000 - bps
                    };
                    let collateral = 2_000_000_000_000_000_000u128 * u128::from(factor) / 10_000;
                    let (payment, shares) = if side == OrderSide::Buy {
                        ("makerAmount", "takerAmount")
                    } else {
                        ("takerAmount", "makerAmount")
                    };
                    assert_eq!(body["order"][payment], collateral.to_string());
                    assert_eq!(body["order"][shares], "100000000000000000000");
                    assert_eq!(
                        cache
                            .borrow()
                            .order(&own.client_order_id())
                            .unwrap()
                            .time_in_force(),
                        tif
                    );
                    client.disconnect().await.unwrap();
                }
            }
        }
    }
}

#[tokio::test]
async fn invalid_or_wrong_order_slippage_is_denied_without_post() {
    let mut cases = [
        json!(-1),
        json!(10000),
        json!(u64::MAX),
        json!(4294967296u64),
        json!(0.5),
        json!("0"),
        json!(true),
        Value::Null,
    ]
    .into_iter()
    .map(|value| (false, "market_order_slippage_bps", value))
    .collect::<Vec<_>>();
    cases.extend([
        (true, "market_order_slippage_bps", json!(0)),
        (false, "slippageBps", json!(0)),
        (false, "market_slippage_bps", json!(0)),
    ]);
    for (limit, key, value) in cases {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        let own = if limit {
            OrderTestBuilder::new(OrderType::Limit)
                .trader_id(trader_id())
                .instrument_id(instrument_id(1))
                .side(OrderSide::Buy)
                .quantity(Quantity::from(100))
                .price(Price::from("0.020"))
                .build()
        } else {
            market(OrderSide::Buy, 1, TimeInForce::Fok)
        };
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        let mut command = if limit {
            submissions::submit(&own)
        } else {
            market_preparation::priced(&own, json!("0.020"), 100)
        };
        let mut values = std::collections::HashMap::new();
        if !limit {
            values.insert("market_price".into(), json!("0.020"));
        }
        values.insert(key.into(), value);
        command.params = Some(nautilus_core::Params::new(values.into_iter().collect()));
        client.submit_order(command).unwrap();
        let ExecutionEvent::Order(OrderEventAny::Denied(event)) = next(&mut rx).await else {
            panic!("invalid slippage must deny")
        };
        assert!(event.reason.as_str().contains("market_order_slippage_bps"));
        assert!(server.state.creations.lock().unwrap().is_empty());
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn unsupported_minimum_out_model_is_denied_before_native_submission() {
    for key in ["isMinAmountOut", "is_min_amount_out"] {
        for value in [json!(true), json!(false), Value::Null] {
            let server = server(vec![]).await;
            let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
            let _portfolio = portfolio(&cache);
            cache
                .borrow_mut()
                .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
                .unwrap();
            connect_reported(&mut client, &mut rx).await;
            let own = market(OrderSide::Buy, 1, TimeInForce::Fok);
            cache
                .borrow_mut()
                .add_order(own.clone(), None, None, false)
                .unwrap();
            let mut command = market_preparation::priced(&own, json!("0.020"), 100);
            command.params = Some(nautilus_core::Params::new(
                [
                    ("market_price".into(), json!("0.020")),
                    ("market_order_slippage_bps".into(), json!(100)),
                    (key.into(), value),
                ]
                .into(),
            ));
            client.submit_order(command).unwrap();
            let event = next(&mut rx).await;
            let ExecutionEvent::Order(OrderEventAny::Denied(denied)) = event else {
                panic!("unsupported amount model must deny, received {event:?}");
            };
            assert!(denied.reason.as_str().contains("minimum-out"));
            assert!(server.state.creations.lock().unwrap().is_empty());
            assert_eq!(*server.state.book_requests.lock().unwrap(), 0);
            client.disconnect().await.unwrap();
        }
    }
}
