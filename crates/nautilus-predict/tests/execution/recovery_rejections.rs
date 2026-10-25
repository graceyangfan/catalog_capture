// Copyright (C) 2026 yfclark and contributors. Licensed under LGPL-3.0.
//! Apply confirmed executions promptly; contradictory batches cannot manufacture facts.
use nautilus_common::{
    messages::execution::QueryOrder,
    msgbus::{TypedHandler, switchboard},
};

use super::*;

#[tokio::test]
async fn gap_account_read_does_not_allow_a_new_initialized_submission() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache, _portfolio, order) = submissions::connected(&server, 1).await;
    server
        .state
        .sockets
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .send(Message::Close(None))
        .unwrap();
    timeout(Duration::from_secs(3), async {
        while client.is_connected() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client
        .query_account(QueryAccount::new(
            trader_id(),
            Some(ClientId::new("PREDICT")),
            account_id(),
            UUID4::new(),
            UnixNanos::from(OBSERVED),
            None,
            None,
        ))
        .unwrap();
    let error = client
        .submit_order(submissions::submit(&order))
        .unwrap_err();
    assert!(error.to_string().contains("not connected"));
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Initialized
    );
    assert!(server.state.creations.lock().unwrap().is_empty());
    // Read-only refresh remains usable while the write barrier is latched.
    timeout(Duration::from_secs(3), async {
        loop {
            if matches!(rx.recv().await.unwrap(), ExecutionEvent::Account(_)) {
                break;
            }
        }
    })
    .await
    .unwrap();
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn confirmed_partial_history_is_applied_while_conflicting_batches_preserve_unknown_intent() {
    run_rejections().await;
}

#[tokio::test]
#[ignore = "explicit capture of synthetic rejection responses, never overwrite evidence"]
async fn record_match_rejection_json() {
    let sessions = run_rejections().await;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/private_api/matches_gap_rejections_synthetic_20261015.json"),
        )
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic loopback invalid batches followed by valid retry; not live venue errors","sessions":sessions})).unwrap();
}

async fn run_rejections() -> Vec<Value> {
    let mut captured = Vec::new();
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "tests/fixtures/private_api/matches_gap_native_feedback_synthetic_20261015.json",
        ))
        .unwrap(),
    )
    .unwrap();
    for case in [
        "incomplete",
        "missing_prefix",
        "conflicting",
        "overfill",
        "foreign",
    ] {
        let server = server(vec![]).await;
        server
            .state
            .create_replies
            .lock()
            .unwrap()
            .push_back((500, 0, vec![]));
        let (mut client, mut rx, cache, _portfolio, order) =
            submissions::connected(&server, 1).await;
        let mut engine = submissions::engine(&cache);
        client.submit_order(submissions::submit(&order)).unwrap();
        submissions::apply(&mut engine, &next(&mut rx).await);
        timeout(Duration::from_secs(3), async {
            while server.state.creations.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let body = server.state.creations.lock().unwrap()[0].clone();
        let mut good = fixture["sessions"][0]["matches"]
            .as_array()
            .unwrap()
            .clone();
        for row in &mut good {
            row["executedAt"] = json!("1970-01-01T00:00:00.002Z");
            for leg in row["makers"].as_array_mut().unwrap() {
                if leg["signer"] == WALLET {
                    leg["hash"] = body["data"]["order"]["hash"].clone();
                }
            }
        }
        let mut bad = good.clone();
        match case {
            "incomplete" | "missing_prefix" => {
                bad.pop();
            }
            "conflicting" => {
                let mut duplicate = bad[0].clone();
                duplicate["makers"][0]["amount"] = json!("32000000000000000000");
                bad.push(duplicate);
            }
            "overfill" => bad[1]["makers"][0]["amount"] = json!("80000000000000000000"),
            "foreign" => {
                bad[1]["makers"][0]["signer"] = json!("0x2222222222222222222222222222222222222222")
            }
            "unstable" | "disconnect_during_check" => {}
            _ => unreachable!(),
        }
        let mut row = rest_order_data("FILLED", 1, OrderSide::Buy);
        row["order"] = body["data"]["order"].clone();
        row["amountFilled"] = json!("100000000000000000000");
        let mut fence = row.clone();
        if matches!(case, "unstable" | "disconnect_during_check") {
            fence["status"] = json!("CANCELLED");
        }
        server.state.orders.lock().unwrap().extend([
            json!({"success":true,"data":row.clone()}),
            json!({"success":true,"data":fence}),
        ]);
        server.state.match_pages.lock().unwrap().push_back((
            200,
            json!({"success":true,"cursor":null,"data":bad.clone()}),
            0,
        ));
        let query = || {
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
        };
        let fill_callbacks = Rc::new(RefCell::new(Vec::new()));
        let handler = TypedHandler::from({
            let callbacks = fill_callbacks.clone();
            let id = order.client_order_id();
            move |event: &nautilus_model::events::OrderEventAny| {
                if let nautilus_model::events::OrderEventAny::Filled(fill) = event
                    && fill.client_order_id == id
                {
                    callbacks.borrow_mut().push((fill.trade_id, fill.last_qty));
                }
            }
        });
        let topic = switchboard::get_event_order_topic(order.strategy_id());
        msgbus::subscribe_order_events(topic.into(), handler.clone(), None);
        client.query_order(query()).unwrap();
        let partial = matches!(case, "incomplete" | "missing_prefix");
        let first = std::iter::once(&good[0]["taker"])
            .chain(good[0]["makers"].as_array().unwrap().iter())
            .find(|leg| leg["signer"] == WALLET)
            .unwrap();
        let expected_partial = Decimal::from_str_exact(first["amount"].as_str().unwrap()).unwrap()
            / Decimal::from(1_000_000_000_000_000_000u64);
        if partial {
            timeout(Duration::from_secs(2), async {
                while cache
                    .borrow()
                    .order(&order.client_order_id())
                    .unwrap()
                    .filled_qty()
                    .is_zero()
                {
                    submissions::apply(&mut engine, &next(&mut rx).await);
                }
            })
            .await
            .unwrap();
            let native = cache.borrow();
            let current = native.order(&order.client_order_id()).unwrap();
            assert_eq!(current.filled_qty().as_decimal(), expected_partial);
            assert_eq!(current.quantity().as_decimal(), Decimal::from(100));
            assert_eq!(current.status(), OrderStatus::PartiallyFilled);
            assert_eq!(
                native.positions_open(None, Some(&order.instrument_id()), None, None, None)[0]
                    .quantity
                    .as_decimal(),
                expected_partial
            );
            assert_eq!(
                fill_callbacks.borrow().len(),
                1,
                "confirmed trade is delivered promptly"
            );
        } else {
            assert!(
                timeout(Duration::from_millis(150), async {
                    loop {
                        let event = next(&mut rx).await;
                        assert!(
                            !matches!(event, ExecutionEvent::Report(_) | ExecutionEvent::Order(_)),
                            "invalid batch published native facts: {case}"
                        );
                        submissions::apply(&mut engine, &event);
                    }
                })
                .await
                .is_err()
            );
            assert_eq!(
                cache
                    .borrow()
                    .order(&order.client_order_id())
                    .unwrap()
                    .status(),
                OrderStatus::Submitted,
                "{case}"
            );
            assert!(
                cache
                    .borrow()
                    .positions(None, None, None, None, None)
                    .is_empty()
            );
            assert_eq!(
                cache
                    .borrow()
                    .account_owned(&account_id())
                    .unwrap()
                    .balance_locked(Some(Currency::USDT()))
                    .unwrap()
                    .as_decimal(),
                Decimal::ZERO // Submitted is held by Native Cash risk, not reported twice.
            );
        }
        let after_first = {
            let native = cache.borrow();
            let current = native.order(&order.client_order_id()).unwrap();
            (
                current.status().to_string(),
                current.filled_qty().to_string(),
                native.positions(None, None, None, None, None).len(),
                native
                    .account_owned(&account_id())
                    .unwrap()
                    .balance_locked(Some(Currency::USDT()))
                    .unwrap()
                    .to_string(),
            )
        };
        // Later full history must preserve and deduplicate already delivered real trades.
        if matches!(case, "incomplete" | "missing_prefix") {
            client.disconnect().await.unwrap();
            connect_reported(&mut client, &mut rx).await;
        }
        server.state.orders.lock().unwrap().extend([
            json!({"success":true,"data":row.clone()}),
            json!({"success":true,"data":row}),
        ]);
        server.state.match_pages.lock().unwrap().push_back((
            200,
            json!({"success":true,"cursor":null,"data":good}),
            0,
        ));
        client.query_order(query()).unwrap();
        timeout(Duration::from_secs(3), async {
            while cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status()
                != OrderStatus::Filled
            {
                submissions::apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .unwrap();
        assert_eq!(
            cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .filled_qty()
                .as_decimal(),
            Decimal::from(100)
        );
        assert_eq!(
            fill_callbacks.borrow().len(),
            2,
            "each real trade is delivered once: {case}"
        );
        msgbus::unsubscribe_order_events(topic.into(), &handler);
        assert_eq!(server.state.creations.lock().unwrap().len(), 1);
        client.disconnect().await.unwrap();
        *server.state.rpc_frame.lock().unwrap() = (938_000_000_000_000_000_000, 3, 3);
        let mut holding = holding(1);
        holding["amount"] = json!("100000000000000000000");
        *server.state.position_snapshot.lock().unwrap() = Some(vec![holding.clone()]);
        *server.state.match_snapshot.lock().unwrap() = Some(good.clone());
        let mut terminal = rest_order_data("FILLED", 1, OrderSide::Buy);
        terminal["order"] = body["data"]["order"].clone();
        terminal["amountFilled"] = json!("100000000000000000000");
        *server.state.order_snapshot.lock().unwrap() =
            Some(json!({"success":true,"data":terminal.clone()}));
        server.state.orders.lock().unwrap().clear();
        connect_reported(&mut client, &mut rx).await;
        let check_gate = Arc::new(tokio::sync::Notify::new());
        match case {
            "incomplete" => {
                let mut wrong = holding.clone();
                wrong["amount"] = json!("99000000000000000000");
                *server.state.position_snapshot.lock().unwrap() = Some(vec![wrong]);
            }
            "missing_prefix" => {
                // Every remaining trade is already applied, but total coverage is
                // incomplete. Native terminal status must not hide a truncated page.
                let mut truncated = good.clone();
                truncated.pop();
                *server.state.match_snapshot.lock().unwrap() = Some(truncated);
            }
            "conflicting" => {
                let mut wrong = good.clone();
                wrong[0]["makers"][0]["hash"] = json!(format!("0x{}", "66".repeat(32)));
                *server.state.match_snapshot.lock().unwrap() = Some(wrong);
            }
            "overfill" => server
                .state
                .open_orders
                .lock()
                .unwrap()
                .push(terminal.clone()),
            "unstable" => server.state.rpc_frame.lock().unwrap().0 = 937_000_000_000_000_000_000,
            "foreign" => {
                let mut wrong = good.clone();
                wrong[0]["settlementId"] = json!("not-applied-trade");
                *server.state.match_snapshot.lock().unwrap() = Some(wrong);
            }
            "disconnect_during_check" => {
                *server.state.match_gate.lock().unwrap() = Some(check_gate.clone())
            }
            _ => unreachable!(),
        }
        let cash_query = || {
            QueryAccount::new(
                trader_id(),
                Some(ClientId::new("PREDICT")),
                account_id(),
                UUID4::new(),
                UnixNanos::from(OBSERVED),
                None,
                None,
            )
        };
        client.query_account(cash_query()).unwrap();
        let _ = timeout(Duration::from_millis(150), async {
            loop {
                submissions::apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await;
        assert!(
            client.is_submission_ready(),
            "history errors cannot disable an authenticated submission transport: {case}"
        );
        assert_eq!(server.state.creations.lock().unwrap().len(), 1);
        if case == "disconnect_during_check" {
            client.disconnect().await.unwrap();
            check_gate.notify_one();
            *server.state.match_gate.lock().unwrap() = None;
            assert!(!client.is_submission_ready());
            connect_reported(&mut client, &mut rx).await;
        }
        *server.state.rpc_frame.lock().unwrap() = (938_000_000_000_000_000_000, 4, 4);
        server.state.open_orders.lock().unwrap().clear();
        *server.state.position_snapshot.lock().unwrap() = Some(vec![holding]);
        *server.state.match_snapshot.lock().unwrap() = Some(good.clone());
        for _ in 0..5 {
            client.query_account(cash_query()).unwrap();
            let _ = timeout(Duration::from_millis(150), async {
                while !client.is_submission_ready() {
                    submissions::apply(&mut engine, &next(&mut rx).await);
                }
            })
            .await;
            if client.is_submission_ready() {
                break;
            }
        }
        assert!(
            client.is_submission_ready(),
            "aligned sources did not release gate: {case}"
        );
        assert_eq!(server.state.creations.lock().unwrap().len(), 1);
        captured.push(json!({"case":case,"request":body,"invalidMatches":bad,"validMatches":good,"orderResponses":server.state.order_response_log.lock().unwrap().clone(),"afterInvalidStatus":after_first.0,"afterInvalidFilled":after_first.1,"afterInvalidPositions":after_first.2,"afterInvalidCashLocked":after_first.3,"afterValidStatus":"FILLED","afterValidFilled":"100","posts":1,"explicitReconnect":true,"historyDoesNotGateSubmission":true,"gateReadyAfterRepair":client.is_submission_ready()}));
        client.disconnect().await.unwrap();
    }
    captured
}

#[tokio::test]
#[ignore = "explicit offline readiness failure capture, never overwrite evidence"]
async fn record_readiness_json() {
    let sessions = run_rejections().await;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/private_api/reconciliation_checks_synthetic_20261016.json"),
        )
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic offline readiness mismatches and successful repair; no mainnet writes","sessions":sessions})).unwrap();
}
