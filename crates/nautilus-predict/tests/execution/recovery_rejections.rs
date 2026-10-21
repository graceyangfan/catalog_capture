// Copyright (C) 2026 yfclark and contributors. Licensed under LGPL-3.0.
//! Rejected recovery batches leave no acceptance, fill or reservation side effects.
use nautilus_common::messages::execution::QueryOrder;
use nautilus_model::events::OrderEventAny;

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
    assert!(error.to_string().contains("stream gap"));
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
async fn incomplete_conflicting_overfilled_and_unstable_history_preserve_unknown_intent() {
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
        "unstable",
        "foreign",
        "disconnect_during_check",
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
        client.query_order(query()).unwrap();
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
        // The rejected batch must not poison atomic fill evidence for a later valid read.
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
            !client.is_submission_ready(),
            "mismatched source released the gate: {case}"
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
        captured.push(json!({"case":case,"request":body,"invalidMatches":bad,"validMatches":good,"orderResponses":server.state.order_response_log.lock().unwrap().clone(),"afterInvalidStatus":"SUBMITTED","afterInvalidFilled":"0","afterInvalidPositions":0,"afterInvalidCashLocked":"62","afterValidStatus":"FILLED","afterValidFilled":"100","posts":1,"explicitReconnect":true,"gateBlockedOnMismatch":true,"gateReadyAfterRepair":client.is_submission_ready()}));
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

#[tokio::test]
async fn cleanup_backlog_does_not_gate_recovery_or_invalidate_an_inflight_check() {
    run_cleanup_recovery(true).await;
    run_cleanup_recovery(false).await;
}

async fn run_cleanup_recovery(purge: bool) {
    let server = server(vec![]).await;
    *server.state.match_snapshot.lock().unwrap() = Some(Vec::new());
    *server.state.position_snapshot.lock().unwrap() = Some(Vec::new());
    let (mut client, mut rx, cache, _) = submissions::setup_client_with_clock(&server, OBSERVED);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    let retained = if purge { 1000 } else { 10017 };
    for index in 0..retained {
        let mut retired = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(trader_id())
            .client_order_id(format!("RECOVERY-RETIRED-{index}").into())
            .instrument_id(instrument_id(1))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100))
            .price(Price::from("0.010"))
            .build();
        let hash = VenueOrderId::new(format!("0x{:064x}", index + 1));
        retired
            .apply(factory.generate_order_submitted(&retired, 1.into()))
            .unwrap();
        retired
            .apply(factory.generate_order_accepted(&retired, hash, 2.into(), 2.into()))
            .unwrap();
        retired
            .apply(factory.generate_order_canceled(&retired, Some(hash), 3.into(), 3.into()))
            .unwrap();
        cache
            .borrow_mut()
            .add_order(retired, None, None, false)
            .unwrap();
    }
    cache.borrow_mut().build_index();
    connect_reported(&mut client, &mut rx).await;
    let sockets_before = server.state.sockets.lock().unwrap().len();
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
        while client.is_submission_ready()
            || server.state.sockets.lock().unwrap().len() == sockets_before
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Follow the existing recovery tests: restart the drained execution consumer
    // and await its authenticated subscription before taking a recovery snapshot.
    client.disconnect().await.unwrap();
    connect_reported(&mut client, &mut rx).await;
    assert!(!client.is_submission_ready());
    if purge {
        cache
            .borrow_mut()
            .purge_closed_orders(UnixNanos::from(10_000_000_000), 0);
    }
    assert_eq!(
        cache
            .borrow()
            .orders_total_count(None, None, None, None, None),
        if purge { 0 } else { retained }
    );
    let gate = Arc::new(tokio::sync::Notify::new());
    *server.state.match_gate.lock().unwrap() = Some(gate.clone());
    let query = || {
        QueryAccount::new(
            trader_id(),
            Some(ClientId::from("PREDICT")),
            account_id(),
            UUID4::new(),
            OBSERVED.into(),
            None,
            None,
        )
    };
    let mut engine = submissions::engine(&cache);
    let mut ready_published = false;
    let matches_before = server.state.match_requests.lock().unwrap().len();
    client.query_account(query()).unwrap();
    timeout(Duration::from_secs(3), async {
        while server.state.match_requests.lock().unwrap().len() == matches_before {
            if let Ok(Some(event)) = timeout(Duration::from_millis(1), rx.recv()).await {
                if let ExecutionEvent::Account(state) = &event {
                    ready_published |= state.info.as_ref().is_some_and(|info| {
                        info.get("predict_reconciliation_ready")
                            .is_some_and(|value| value == true)
                    });
                }
                submissions::apply(&mut engine, &event);
            }
        }
    })
    .await
    .unwrap();
    assert!(!client.is_submission_ready());
    // QueryAccount shares the existing guard; these calls only advance bounded
    // cleanup while the first readiness collection is suspended across IO.
    for _ in 0..8 {
        client.query_account(query()).unwrap();
    }
    *server.state.match_gate.lock().unwrap() = None;
    gate.notify_waiters();
    timeout(Duration::from_secs(3), async {
        while !client.is_submission_ready() {
            let event = next(&mut rx).await;
            if let ExecutionEvent::Account(state) = &event {
                ready_published |= state.info.as_ref().is_some_and(|info| {
                    info.get("predict_reconciliation_ready")
                        .is_some_and(|value| value == true)
                });
            }
            submissions::apply(&mut engine, &event);
        }
    })
    .await
    .unwrap();
    assert_eq!(
        server.state.match_requests.lock().unwrap().len() - matches_before,
        2,
        "one recovery collection and its fence, without cleanup-induced retries"
    );
    assert!(server.state.creations.lock().unwrap().is_empty());
    while let Ok(event) = rx.try_recv() {
        if let ExecutionEvent::Account(state) = &event {
            ready_published |= state.info.as_ref().is_some_and(|info| {
                info.get("predict_reconciliation_ready")
                    .is_some_and(|value| value == true)
            });
        }
        submissions::apply(&mut engine, &event);
    }
    assert!(
        ready_published,
        "subscriber must receive recovery-ready AccountState"
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn ready_publication_reuses_open_and_keeps_next_cancel_projection_synchronous() {
    run_ready_projection(false).await;
    run_ready_projection(true).await;
}

async fn run_ready_projection(clamped: bool) {
    let server = server(vec![]).await;
    *server.state.match_snapshot.lock().unwrap() = Some(vec![]);
    *server.state.position_snapshot.lock().unwrap() = Some(vec![]);
    server
        .state
        .create_order_ids
        .lock()
        .unwrap()
        .push_back("12345".into());
    let (mut client, mut rx, cache, _portfolio, original) =
        submissions::connected(&server, 1).await;
    let mut engine = submissions::engine(&cache);
    client.submit_order(submissions::submit(&original)).unwrap();
    submissions::apply(&mut engine, &next(&mut rx).await);
    exit_orders::barrier("one signed order POST", || {
        server.state.creations.lock().unwrap().len() == 1
    })
    .await;
    let body = server.state.creations.lock().unwrap()[0].clone();
    let mut accepted = event("orderAccepted", 1);
    accepted["orderHash"] = body["data"]["order"]["hash"].clone();
    server.wallet(accepted.clone());
    timeout(Duration::from_secs(3), async {
        while cache
            .borrow()
            .account(&account_id())
            .unwrap()
            .balance_locked(Some(Currency::USDT()))
            != Some(Money::from("62 USDT"))
        {
            submissions::apply(&mut engine, &next(&mut rx).await);
        }
    })
    .await
    .unwrap();
    if clamped {
        server
            .state
            .create_order_ids
            .lock()
            .unwrap()
            .push_back("12346".into());
        let second = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(trader_id())
            .client_order_id("SECOND-BUY".into())
            .instrument_id(instrument_id(1))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100))
            .price(Price::from("0.620"))
            .post_only(true)
            .build();
        cache
            .borrow_mut()
            .add_order(second.clone(), None, None, false)
            .unwrap();
        client.submit_order(submissions::submit(&second)).unwrap();
        submissions::apply(&mut engine, &next(&mut rx).await);
        exit_orders::barrier("second signed order POST", || {
            server.state.creations.lock().unwrap().len() == 2
        })
        .await;
        let second_body = server.state.creations.lock().unwrap()[1].clone();
        let mut second_ack = accepted.clone();
        second_ack["orderHash"] = second_body["data"]["order"]["hash"].clone();
        second_ack["orderId"] = json!("12346");
        server.wallet(second_ack);
        timeout(Duration::from_secs(3), async {
            while cache
                .borrow()
                .account(&account_id())
                .unwrap()
                .balance_locked(Some(Currency::USDT()))
                != Some(Money::from("124 USDT"))
            {
                submissions::apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .unwrap();
        let mut second_row = live_node::rest_row(&second_body, "OPEN");
        second_row["id"] = json!("12346");
        server.state.open_orders.lock().unwrap().push(second_row);
        server.state.rpc_frame.lock().unwrap().0 = 100_000_000_000_000_000_000;
    }
    let mut row = live_node::rest_row(&body, "OPEN");
    row["id"] = json!("12345");
    server.state.open_orders.lock().unwrap().push(row);
    server
        .state
        .sockets
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .send(Message::Close(None))
        .unwrap();
    exit_orders::barrier("wallet gap", || !client.is_submission_ready()).await;
    client.disconnect().await.unwrap();
    connect_reported(&mut client, &mut rx).await;
    if clamped {
        // Equal clamped balances can hide older uncapped Native account metadata.
        let mut stale = cache
            .borrow()
            .account(&account_id())
            .unwrap()
            .last_event()
            .unwrap();
        stale.event_id = UUID4::new();
        stale
            .info
            .as_mut()
            .unwrap()
            .insert("predict_requested_lock".into(), json!("100"));
        submissions::apply(&mut engine, &ExecutionEvent::Account(stale));
    }
    let gate = Arc::new(tokio::sync::Notify::new());
    *server.state.match_gate.lock().unwrap() = Some(gate.clone());
    let matches = server.state.match_requests.lock().unwrap().len();
    let open_reads = server.state.order_list_log.lock().unwrap().len();
    client
        .query_account(QueryAccount::new(
            trader_id(),
            Some(ClientId::from("PREDICT")),
            account_id(),
            UUID4::new(),
            OBSERVED.into(),
            None,
            None,
        ))
        .unwrap();
    timeout(Duration::from_secs(3), async {
        while server.state.match_requests.lock().unwrap().len() == matches {
            if let Ok(Some(event)) = timeout(Duration::from_millis(1), rx.recv()).await {
                submissions::apply(&mut engine, &event);
            }
        }
    })
    .await
    .unwrap();
    // Readiness must retain its fresher post-holdings cash block, rather than
    // the first refresh's block, for subsequent local reservation projections.
    *server.state.rpc_frame.lock().unwrap() = (
        if clamped {
            100_000_000_000_000_000_000
        } else {
            1_000_000_000_000_000_000_000
        },
        2,
        2,
    );
    *server.state.match_gate.lock().unwrap() = None;
    gate.notify_waiters();
    loop {
        let event = next(&mut rx).await;
        let ready = matches!(&event, ExecutionEvent::Account(state) if state.info.as_ref().is_some_and(|info| info.get("predict_reconciliation_ready") == Some(&json!(true))));
        if ready {
            let ExecutionEvent::Account(ref state) = event else {
                unreachable!()
            };
            assert_eq!(
                Decimal::from_str_exact(
                    state.info.as_ref().unwrap()["predict_requested_lock"]
                        .as_str()
                        .unwrap()
                )
                .unwrap(),
                Decimal::from(if clamped { 124 } else { 62 })
            );
        }
        submissions::apply(&mut engine, &event);
        if ready {
            break;
        }
    }
    assert!(client.is_submission_ready());
    assert_eq!(
        server.state.order_list_log.lock().unwrap().len() - open_reads,
        2,
        "one shared OPEN collection plus one cross-IO fence"
    );
    assert_eq!(
        server.state.match_requests.lock().unwrap().len() - matches,
        2
    );
    while let Ok(event) = rx.try_recv() {
        submissions::apply(&mut engine, &event);
    }
    let rpc_before = server.state.rpc_methods.lock().unwrap().len();
    let gate = Arc::new(tokio::sync::Notify::new());
    *server.state.rpc_gate.lock().unwrap() = Some(gate.clone());
    server
        .state
        .open_orders
        .lock()
        .unwrap()
        .retain(|row| row["order"]["hash"] != body["data"]["order"]["hash"]);
    accepted["type"] = json!("orderCancelled");
    accepted["timestamp"] = json!(2);
    server.wallet(accepted);
    let cancel = next(&mut rx).await;
    assert!(matches!(
        cancel,
        ExecutionEvent::Order(OrderEventAny::Canceled(_))
    ));
    submissions::apply(&mut engine, &cancel);
    let local = rx
        .try_recv()
        .expect("confirmed cancellation projects cash without yielding to a funding task");
    let ExecutionEvent::Account(ref state) = local else {
        panic!("expected local account child");
    };
    assert_eq!(
        state.balances[0].locked,
        Money::from(if clamped { "62 USDT" } else { "0 USDT" })
    );
    assert_eq!(
        Decimal::from_str_exact(
            state.info.as_ref().unwrap()["predict_requested_lock"]
                .as_str()
                .unwrap()
        )
        .unwrap(),
        Decimal::from(if clamped { 62 } else { 0 })
    );
    assert_eq!(state.info.as_ref().unwrap()["predict_block_number"], 2);
    submissions::apply(&mut engine, &local);
    assert_eq!(
        cache
            .borrow()
            .account(&account_id())
            .unwrap()
            .balance_free(Some(Currency::USDT())),
        Some(Money::from(if clamped { "38 USDT" } else { "1000 USDT" }))
    );
    assert_eq!(
        server.state.open_orders.lock().unwrap().len(),
        usize::from(clamped),
        "another venue OPEN order must remain live"
    );
    assert_eq!(
        cache
            .borrow()
            .orders_open(None, None, None, None, None)
            .len(),
        usize::from(clamped)
    );
    assert_eq!(server.state.rpc_methods.lock().unwrap().len(), rpc_before);
    *server.state.rpc_gate.lock().unwrap() = None;
    gate.notify_waiters();
    client.disconnect().await.unwrap();
}
