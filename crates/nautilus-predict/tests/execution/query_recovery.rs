// Copyright (C) 2026 yfclark and contributors. LGPL-3.0.
//! Authenticated hash queries recover missing acknowledgements, never resubmit intent.
use nautilus_common::messages::execution::QueryOrder;
use nautilus_model::events::OrderEventAny;

use super::*;
#[path = "../support/query_strategy.rs"]
pub(super) mod strategy;

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

fn response(body: &Value) -> Value {
    let mut row = rest_order_data("OPEN", 1, OrderSide::Buy);
    row["order"] = body["data"]["order"].clone();
    json!({"success":true,"data":row})
}

#[tokio::test]
async fn missing_wallet_ack_query_accepts_once_and_cancel_releases_cash_without_resubmit() {
    run("open", true).await;
}

#[tokio::test]
async fn altered_contract_fields_or_unsettled_fills_never_accept_unknown_submission() {
    for case in [
        "nonce",
        "fee",
        "exchange",
        "hash",
        "filled",
        "terminal",
        "not_found",
    ] {
        run(case, true).await;
    }
}

#[tokio::test]
async fn stale_open_query_cannot_reopen_wallet_canceled_order() {
    run("wallet_wins", true).await;
}

#[tokio::test]
#[ignore = "explicitly create a new synthetic JSON capture, never overwrite historical evidence"]
async fn record_query_recovery_json() {
    let mut sessions = Vec::new();
    for case in [
        "open",
        "nonce",
        "fee",
        "exchange",
        "hash",
        "filled",
        "terminal",
        "not_found",
        "wallet_wins",
    ] {
        sessions.push(run(case, false).await);
    }
    sessions.push(run_node(false).await);
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/query_recovery_synthetic_20261015.json");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file, &json!({"source":"synthetic loopback HTTP/wallet and native ExecutionEngine, no mainnet orders", "sessions":sessions})).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_query_recovers_acceptance_then_modify_reject_and_cancel_callbacks() {
    run_node(true).await;
}

async fn run_node(from_saved: bool) -> Value {
    use nautilus_core::time::get_atomic_clock_realtime;
    use nautilus_predict::{PredictDataClientConfig, PredictSubmissionConfig};

    use super::exit_orders::{PublicState, barrier, exit_socket};
    let private = server(vec![]).await;
    private
        .state
        .create_replies
        .lock()
        .unwrap()
        .push_back((500, 0, vec![]));
    let mut cfg = config(&private);
    cfg.account_state_mode = PredictAccountStateMode::Reported;
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", private.http).into(),
    ));
    cfg.orderbook_removal_enabled = true;
    cfg.account_refresh_interval_secs = 1;
    cfg.submission = Some(PredictSubmissionConfig {
        signer: Arc::new(
            nautilus_predict::signing::PredictAuthSigner::new(
                submissions::PUBLIC_KEY.into(),
                PredictEnvironment::BnbMainnet,
                Some(WALLET),
            )
            .unwrap(),
        ),
        max_order_usdt: Decimal::from(62),
        max_total_usdt: Decimal::from(62),
        require_post_only: true,
        market_slippage_bps: 100,
    });
    private.state.rpc_frame.lock().unwrap().2 =
        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000;
    let public = PublicState(Arc::new(Mutex::new(vec![])));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/v1/markets/42", get(super::live_node::public_market))
                .route("/ws", get(exit_socket))
                .with_state(public.clone()),
        )
        .into_future(),
    );
    let mut live = node::build_node(
        trader_id(),
        PredictDataClientConfig {
            api_key: KEY.into(),
            market_ids: vec![42],
            base_url_http: Some(format!("http://{addr}")),
            base_url_ws: Some(format!("ws://{addr}/ws")),
            http_timeout_secs: 2,
            ws_timeout_secs: 2,
            dynamic_refresh_secs: 30,
            transport_backend: TransportBackend::Tungstenite,
        },
        cfg,
    )
    .unwrap();
    let cache = live.kernel().cache.clone();
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    assert!(
        cache
            .borrow()
            .orders(None, None, None, None, None)
            .is_empty()
    );
    let journal = Rc::new(RefCell::new(vec![]));
    let ready = Rc::new(std::cell::Cell::new(false));
    live.add_strategy(strategy::QueryStrategy::new(
        instrument_id(1),
        Rc::clone(&ready),
        Rc::clone(&journal),
    ))
    .unwrap();
    let handle = live.handle();
    let driver = async {
        barrier("public/wallet streams", || {
            !public.0.lock().unwrap().is_empty()
                && !private.state.sockets.lock().unwrap().is_empty()
        })
        .await;
        let book = json!({"marketId":42,"updateTimestampMs":get_atomic_clock_realtime().get_time_ns().as_u64()/1_000_000,"orderCount":2,"bids":[["0.630","1000"]],"asks":[["0.640","1000"]]});
        for sender in public.0.lock().unwrap().iter() {
            sender
                .send(Message::Text(
                    json!({"type":"M","topic":"predictOrderbook/42","data":book})
                        .to_string()
                        .into(),
                ))
                .unwrap();
        }
        barrier("one native POST", || {
            private.state.creations.lock().unwrap().len() == 1
        })
        .await;
        let body = private.state.creations.lock().unwrap()[0].clone();
        // Query and cancellation each independently validate the authenticated OPEN row.
        let row = replay_response(
            "rust_strategy_live_node",
            &body,
            response(&body),
            from_saved,
        );
        private
            .state
            .orders
            .lock()
            .unwrap()
            .extend([row.clone(), row.clone()]);
        private
            .state
            .open_orders
            .lock()
            .unwrap()
            .push(row["data"].clone());
        private.state.remove_replies.lock().unwrap().push_back((
            200,
            json!({"success":true,"removed":["12345"],"noop":[]}),
            0,
        ));
        ready.set(true);
        barrier("strategy cancel callback", || {
            journal
                .borrow()
                .iter()
                .any(|r| r["callback"] == "on_order_canceled")
        })
        .await;
        barrier("reported cash released", || {
            cache
                .borrow()
                .account_owned(&account_id())
                .unwrap()
                .balance_locked(Some(Currency::USDT()))
                .unwrap()
                .as_decimal()
                .is_zero()
        })
        .await;
        assert_eq!(private.state.creations.lock().unwrap().len(), 1);
        assert_eq!(private.state.removals.lock().unwrap().len(), 1);
        assert!(private.state.open_orders.lock().unwrap().is_empty());
        assert!(
            cache
                .borrow()
                .positions(None, None, None, None, None)
                .is_empty()
        );
        let orders = cache
            .borrow()
            .orders(None, None, None, None, None)
            .into_iter()
            .map(|o| o.clone())
            .collect::<Vec<_>>();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].status(), OrderStatus::Canceled);
        assert!(orders[0].filled_qty().is_zero());
        for name in [
            "on_order_accepted",
            "on_order_modify_rejected",
            "on_order_canceled",
        ] {
            assert_eq!(
                journal
                    .borrow()
                    .iter()
                    .filter(|r| r["callback"] == name)
                    .count(),
                1
            );
        }
        assert!(
            journal
                .borrow()
                .iter()
                .any(|r| r["type"] == "StrategyQueryOrder"
                    && r["cachedStatus"] == "SUBMITTED"
                    && r["venueOrderId"].is_null())
        );
        handle.stop();
        json!({"case":"rust_strategy_live_node","requests":[body],"orderResponse":row,"queryPaths":private.state.order_requests.lock().unwrap().clone(),"removals":private.state.removals.lock().unwrap().clone(),"nativeFeedback":journal.borrow().clone(),"postCount":1,"finalStatus":"CANCELED","positionCount":0,"finalCashLocked":"0"})
    };
    let result = timeout(Duration::from_secs(20), async {
        let (result, node_result) = tokio::join!(driver, live.run());
        node_result.unwrap();
        result
    })
    .await
    .unwrap();
    task.abort();
    result
}

async fn run(case: &str, from_saved: bool) -> Value {
    let server = server(vec![]).await;
    // HTTP 500 leaves the transmitted intent unresolved. No wallet acknowledgement.
    server
        .state
        .create_replies
        .lock()
        .unwrap()
        .push_back((500, 0, vec![]));
    let (mut client, mut rx, cache, _portfolio, order) = submissions::connected(&server, 1).await;
    let callbacks = Rc::new(RefCell::new(Vec::new()));
    let received = Rc::clone(&callbacks);
    let handler = msgbus::TypedHandler::from(move |event: &OrderEventAny| {
        received
            .borrow_mut()
            .push(format!("{:?}", event.event_type()));
    });
    msgbus::subscribe_order_events("events.order.*".into(), handler.clone(), None);
    let mut engine = submissions::engine(&cache);
    assert!(client.query_order(query(&order)).is_err());
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
    let hash = body["data"]["order"]["hash"].clone();
    let mut row = response(&body);
    match case {
        "nonce" => row["data"]["order"]["nonce"] = json!("123"),
        "fee" => row["data"]["order"]["feeRateBps"] = json!("123"),
        "exchange" => row["data"]["isNegRisk"] = json!(true),
        "hash" => row["data"]["order"]["hash"] = json!(HASH),
        "filled" => row["data"]["amountFilled"] = json!("1"),
        "terminal" => row["data"]["status"] = json!("CANCELLED"),
        "not_found" => row = json!({"success":false,"message":"order not found"}),
        "open" | "wallet_wins" => {}
        _ => panic!("unknown test case"),
    }
    let row = replay_response(case, &body, row, from_saved);
    server
        .state
        .order_replies
        .lock()
        .unwrap()
        .push_back((if case == "not_found" { 404 } else { 200 }, row.clone()));
    let gate = Arc::new(tokio::sync::Notify::new());
    if case == "wallet_wins" {
        *server.state.order_gate.lock().unwrap() = Some(Arc::clone(&gate));
    }
    // User-supplied venue hashes cannot override local transmission ownership.
    let mut wrong = query(&order);
    wrong.venue_order_id = Some(VenueOrderId::new(HASH));
    assert!(client.query_order(wrong).is_err());
    client.query_order(query(&order)).unwrap();
    timeout(Duration::from_secs(3), async {
        while server.state.order_requests.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    if case == "wallet_wins" {
        for kind in ["orderAccepted", "orderCancelled"] {
            let mut payload = event(kind, 1);
            payload["orderHash"] = hash.clone();
            server.wallet(payload);
            let expected = if kind == "orderAccepted" {
                OrderStatus::Accepted
            } else {
                OrderStatus::Canceled
            };
            timeout(Duration::from_secs(3), async {
                while cache
                    .borrow()
                    .order(&order.client_order_id())
                    .unwrap()
                    .status()
                    != expected
                {
                    submissions::apply(&mut engine, &next(&mut rx).await);
                }
            })
            .await
            .unwrap();
        }
        gate.notify_one();
    }
    if case == "open" {
        timeout(Duration::from_secs(3), async {
            while cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status()
                != OrderStatus::Accepted
            {
                submissions::apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .unwrap();
        let native = cache
            .borrow()
            .order_owned(&order.client_order_id())
            .unwrap();
        assert_eq!(
            native.venue_order_id().unwrap().as_str(),
            hash.as_str().unwrap()
        );
        assert_eq!(native.ts_accepted(), Some(UnixNanos::from(OBSERVED)));
        // Repeated OPEN evidence does not produce another OrderAccepted callback.
        server
            .state
            .orders
            .lock()
            .unwrap()
            .push_back(response(&body));
        client.query_order(query(&native)).unwrap();
        timeout(Duration::from_secs(3), async {
            loop {
                let event = next(&mut rx).await;
                let report = matches!(event, ExecutionEvent::Report(_));
                submissions::apply(&mut engine, &event);
                if report {
                    break;
                }
            }
        })
        .await
        .unwrap();
        let mut cancel = response(&body);
        cancel["data"]["status"] = json!("CANCELLED");
        server.state.orders.lock().unwrap().push_back(cancel);
        client.query_order(query(&native)).unwrap();
        timeout(Duration::from_secs(3), async {
            while cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status()
                != OrderStatus::Canceled
            {
                submissions::apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .unwrap();
    } else {
        // Failure is observable as preserved Submitted/reserved state, never a fabricated reject.
        assert!(
            timeout(Duration::from_millis(150), async {
                loop {
                    let event = next(&mut rx).await;
                    assert!(!matches!(
                        event,
                        ExecutionEvent::Report(_) | ExecutionEvent::Order(_)
                    ));
                    submissions::apply(&mut engine, &event);
                }
            })
            .await
            .is_err()
        );
    }
    let native = cache
        .borrow()
        .order_owned(&order.client_order_id())
        .unwrap();
    let resolved = case == "open" || case == "wallet_wins";
    assert_eq!(
        native.status(),
        if resolved {
            OrderStatus::Canceled
        } else {
            OrderStatus::Submitted
        }
    );
    assert_eq!(
        callbacks
            .borrow()
            .iter()
            .filter(|s| s.as_str() == "Accepted")
            .count(),
        usize::from(resolved)
    );
    assert!(native.filled_qty().is_zero());
    assert!(
        cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
    );
    assert_eq!(server.state.creations.lock().unwrap().len(), 1);
    assert!(client.submit_order(submissions::submit(&order)).is_err());
    let expected_locked = if resolved {
        Decimal::ZERO
    } else {
        Decimal::from(62)
    };
    timeout(Duration::from_secs(3), async {
        while cache
            .borrow()
            .account_owned(&account_id())
            .unwrap()
            .balance_locked(Some(Currency::USDT()))
            .unwrap()
            .as_decimal()
            != expected_locked
        {
            submissions::apply(&mut engine, &next(&mut rx).await);
        }
    })
    .await
    .unwrap();
    let result = json!({"case":case,"request":body,"orderResponse":row,"queryPaths":server.state.order_requests.lock().unwrap().clone(),
        "callbacks":callbacks.borrow().clone(),"finalStatus":native.status().to_string(),"postCount":1,"filledQty":native.filled_qty().to_string(),"positionCount":0,"finalCashLocked":expected_locked.to_string()});
    msgbus::unsubscribe_order_events("events.order.*".into(), &handler);
    client.disconnect().await.unwrap();
    if resolved {
        client.reset().unwrap();
    } else {
        assert!(client.reset().is_err());
    }
    result
}

/// Rebind dynamic signed request fields; preserve captured response differences and economics.
fn replay_response(case: &str, request: &Value, generated: Value, from_saved: bool) -> Value {
    if !from_saved {
        return generated;
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/query_recovery_synthetic_20261015.json");
    let fixture: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let session = fixture["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["case"] == case)
        .unwrap();
    let original = if session["request"].is_object() {
        &session["request"]
    } else {
        &session["requests"][0]
    };
    let mut row = session["orderResponse"].clone();
    if row["data"]["order"].is_object() {
        for (key, value) in request["data"]["order"].as_object().unwrap() {
            if row["data"]["order"][key] == original["data"]["order"][key] {
                row["data"]["order"][key] = value.clone();
            }
        }
    }
    assert_eq!(row, generated, "saved response evidence changed for {case}");
    row
}
