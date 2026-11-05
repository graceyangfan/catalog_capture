// Copyright (C) 2026 yfclark and contributors. Licensed under LGPL-3.0.
//! A rejected WS settlement recovers through a standard Native query, including closed owners.
use super::*;
use nautilus_common::{
    messages::execution::TradingCommand,
    msgbus::{TypedHandler, TypedIntoHandler},
};
use nautilus_model::events::OrderEventAny;

#[tokio::test(flavor = "current_thread")]
async fn rejected_settlement_recovers_canceled_order_net_inventory_once_after_rest_lag() {
    let captured: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/settlement_recovery_review_20261105.json"
    ))
    .unwrap();
    let actual_fee = captured["match"]["fee_wei"].as_str().unwrap();
    let actual_net = Quantity::from_u256(
        captured["position"]["amount_wei"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap(),
        18,
    )
    .unwrap();
    let private = server(vec![]).await;
    let (_, mut rx, cache) = client(&private);
    cache
        .borrow_mut()
        .add_instrument(instrument(1).into())
        .unwrap();
    let _portfolio = portfolio(&cache);
    let mut row = rest_order_data("CANCELLED", 1, OrderSide::Buy);
    row["order"]["feeRateBps"] = json!("200");
    let fields = serde_json::from_value(row["order"].clone()).unwrap();
    let hash = nautilus_predict::signing::PredictOrderHasher::new(PredictEnvironment::BnbMainnet)
        .order_hash(
            &fields,
            nautilus_predict::common::contracts::PredictExchangeType::Standard,
        );
    row["order"]["hash"] = json!(hash);
    let original = order(1);
    let mut own = OrderAny::try_from(original.init_event().clone()).unwrap();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    own.apply(factory.generate_order_submitted(&own, 10.into()))
        .unwrap();
    own.apply(factory.generate_order_accepted(
        &own,
        VenueOrderId::new(format!("{hash:#x}")),
        100.into(),
        100.into(),
    ))
    .unwrap();
    own.apply(factory.generate_order_canceled(&own, own.venue_order_id(), 101.into(), 101.into()))
        .unwrap();
    cache
        .borrow_mut()
        .add_order(own.clone(), None, Some(ClientId::from("PREDICT")), false)
        .unwrap();
    cache
        .borrow_mut()
        .add_venue_order_id(
            &own.client_order_id(),
            &own.venue_order_id().unwrap(),
            false,
        )
        .unwrap();
    *private.state.order_snapshot.lock().unwrap() = Some(json!({"success":true,"data":row}));
    *private.state.match_snapshot.lock().unwrap() = Some(vec![]);
    let core = ExecutionClientCore::new(
        trader_id(),
        ClientId::from("PREDICT"),
        *PREDICT_VENUE,
        OmsType::Netting,
        account_id(),
        AccountType::Cash,
        None,
        cache.clone(),
    );
    let mut cfg = config(&private);
    cfg.account_state_mode = PredictAccountStateMode::Reported;
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", private.http).into(),
    ));
    // Automatic cash recovery is disabled; settlement recovery must still run.
    cfg.reconnect_reconciliation_interval_secs = None;
    let clock = Box::leak(Box::new(AtomicTime::new(false, OBSERVED.into())));
    let timer = Rc::new(RefCell::new(VirtualClock::new()));
    let mut client = PredictExecutionClient::new_with_clock(core, cfg, clock)
        .unwrap()
        .with_reconciliation_clock(timer.clone());
    let commands = Rc::new(RefCell::new(Vec::new()));
    msgbus::register_trading_command_endpoint(
        MessagingSwitchboard::exec_engine_queue_execute(),
        TypedIntoHandler::from({
            let commands = commands.clone();
            move |command: TradingCommand| commands.borrow_mut().push(command)
        }),
    );
    connect_reported(&mut client, &mut rx).await;
    let mut engine = cancellations::engine(&cache);
    let callbacks = Rc::new(RefCell::new(Vec::new()));
    msgbus::subscribe_order_events(
        format!("events.order.{}", own.strategy_id()).into(),
        TypedHandler::from({
            let callbacks = callbacks.clone();
            move |event: &OrderEventAny| {
                if let OrderEventAny::Filled(fill) = event {
                    callbacks.borrow_mut().push(fill.clone());
                }
            }
        }),
        None,
    );
    let mut submitted = event("orderTransactionSubmitted", 1);
    submitted["orderHash"] = json!(hash);
    submitted["settlementId"] = json!("incident-replay");
    submitted["kind"] = json!("PURCHASE");
    submitted["fill"] = json!({"executedPriceWei":"540000000000000000", "executedSizeWei":"5000000000000000000", "executedValueWei":"2700000000000000000"});
    let mut success = submitted.clone();
    success["type"] = json!("orderTransactionSuccess");
    success["isMaker"] = json!(false);
    success["fill"] = json!({"executedPriceWei":"540000000000000000", "executedSizeWei":"4910000000000000000", "executedValueWei":"2651400000000000000"});
    success["fee"] = json!({"type":"SHARES", "amountWei":actual_fee});
    let mut changed = success.clone();
    changed["type"] = json!("orderTransactionSubmitted");
    private.wallet(submitted);
    private.wallet(changed);
    private.wallet(success.clone());
    // A different owned order supplies a causal WS barrier without a sleep.
    let (barrier_id, barrier) = wallet_lifecycle::control(&mut engine, &success);
    private.wallet(barrier);
    loop {
        let incoming = next(&mut rx).await;
        let done = matches!(&incoming, ExecutionEvent::Order(OrderEventAny::Canceled(e)) if e.venue_order_id == Some(barrier_id));
        assert!(!matches!(
            &incoming,
            ExecutionEvent::Report(ExecutionReport::Fill(_))
        ));
        submissions::apply(&mut engine, &incoming);
        if done {
            break;
        }
    }
    assert!(callbacks.borrow().is_empty());
    assert!(
        cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
    );
    // Refresh cash first: it must not erase the order's recovery reason.
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
    apply_account(&mut rx).await;
    let fire = |time: u64| {
        clock.set_time(time.into());
        let events = timer.borrow_mut().advance_time(time.into(), true);
        let handlers = timer.borrow().match_handlers(events);
        for handler in handlers {
            handler.run();
        }
    };
    fire(1_000_000_000);
    let TradingCommand::QueryOrder(query) =
        commands.borrow_mut().pop().expect("closed order recovery")
    else {
        panic!("only order recovery enabled")
    };
    assert_eq!(query.client_order_id, own.client_order_id());
    let gate = Arc::new(tokio::sync::Notify::new());
    *private.state.order_gate.lock().unwrap() = Some(gate.clone());
    client.query_order(query.clone()).unwrap();
    client.query_order(query).unwrap();
    exit_orders::barrier("one in-flight order read", || {
        private.state.order_requests.lock().unwrap().len() == 1
    })
    .await;
    let (control_id, control) = wallet_lifecycle::control(&mut engine, &success);
    private.wallet(control);
    loop {
        let incoming = next(&mut rx).await;
        let done = matches!(&incoming, ExecutionEvent::Order(OrderEventAny::Canceled(e)) if e.venue_order_id == Some(control_id));
        submissions::apply(&mut engine, &incoming);
        if done {
            break;
        }
    }
    fire(2_000_000_000);
    assert!(
        commands.borrow().is_empty(),
        "slow REST must neither block wallet feedback nor overlap queries"
    );
    *private.state.order_gate.lock().unwrap() = None;
    gate.notify_one();
    exit_orders::barrier("REST history read despite zero cumulative", || {
        !private.state.match_response_log.lock().unwrap().is_empty()
    })
    .await;
    // Wait for the async query's completion by consuming its scheduler retry.
    let mut recovered = matches::match_row(1);
    recovered["settlementId"] = json!("incident-replay");
    recovered["taker"]["hash"] = json!(hash);
    recovered["taker"]["amount"] = json!("4910000000000000000");
    recovered["taker"]["price"] = json!("540000000000000000");
    recovered["taker"]["fee"] = json!({"type":"SHARES", "amount":actual_fee});
    row["amountFilled"] = json!("4910000000000000000");
    *private.state.order_snapshot.lock().unwrap() = Some(json!({"success":true,"data":row}));
    *private.state.match_snapshot.lock().unwrap() = Some(vec![recovered]);
    timeout(Duration::from_secs(3), async {
        let mut time = 1_000_000_000;
        loop {
            tokio::task::yield_now().await;
            time += 1_000_000_000;
            fire(time);
            let Some(command) = commands.borrow_mut().pop() else {
                continue;
            };
            let TradingCommand::QueryOrder(query) = command else {
                panic!("unexpected cash query")
            };
            client.query_order(query).unwrap();
            break;
        }
    })
    .await
    .unwrap();
    let report = loop {
        let incoming = next(&mut rx).await;
        submissions::apply(&mut engine, &incoming);
        if let ExecutionEvent::Report(report @ ExecutionReport::OrderWithFills(_, _)) = incoming {
            break report;
        }
    };
    assert_eq!(callbacks.borrow().len(), 1);
    let observed = callbacks.borrow()[0].clone();
    assert!(observed.reconciliation);
    assert_eq!(observed.last_qty, Quantity::from("4.91"));
    assert_eq!(
        observed.quantity_commission,
        Some(Quantity::from_u256(actual_fee.parse().unwrap(), 18).unwrap())
    );
    assert_eq!(
        cache.borrow().positions(None, None, None, None, None)[0].quantity,
        actual_net
    );
    assert_eq!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Canceled
    );
    fire(300_000_000_000);
    assert!(
        commands.borrow().is_empty(),
        "Native application retires the intent"
    );
    // An explicit replay uses the same Native deduplication as wallet fills.
    engine.reconcile_execution_report(&report);
    assert_eq!(callbacks.borrow().len(), 1);
    assert_eq!(
        cache.borrow().positions(None, None, None, None, None)[0].quantity,
        actual_net
    );
    client.disconnect().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn reconciled_settlement_drives_existing_strategy_take_profit_at_net_inventory() {
    use super::exit_orders::{PublicState, barrier, exit_socket, strategy};
    use nautilus_core::time::get_atomic_clock_realtime;
    use nautilus_predict::PredictDataClientConfig;
    let private = server(vec![]).await;
    let now = get_atomic_clock_realtime().get_time_ns();
    private.state.rpc_frame.lock().unwrap().2 = now.as_u64() / 1_000_000_000;
    *private.state.sell_approved.lock().unwrap() = true;
    *private.state.sell_inventory.lock().unwrap() = U256::from(196_000_000_000_000_000_000u128);
    *private.state.market_fee.lock().unwrap() = 200;
    let mut cfg = config(&private);
    cfg.account_state_mode = PredictAccountStateMode::Reported;
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", private.http).into(),
    ));
    cfg.submission_signer = Some(Arc::new(
        nautilus_predict::signing::PredictAuthSigner::new(
            submissions::PUBLIC_KEY.into(),
            PredictEnvironment::BnbMainnet,
            Some(WALLET),
        )
        .unwrap(),
    ));
    let public = PublicState(Arc::new(Mutex::new(vec![])));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
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
            base_url_http: Some(format!("http://{address}")),
            base_url_ws: Some(format!("ws://{address}/ws")),
            http_timeout_secs: 2,
            ws_timeout_secs: 2,
            dynamic_refresh_secs: 30,
            transport_backend: TransportBackend::Tungstenite,
        },
        cfg,
    )
    .unwrap();
    let mut row = rest_order_data("CANCELLED", 1, OrderSide::Buy);
    row["order"]["feeRateBps"] = json!("200");
    row["order"]["makerAmount"] = json!("2000000000000000000");
    row["order"]["takerAmount"] = json!("200000000000000000000");
    row["amountFilled"] = json!("200000000000000000000");
    let fields = serde_json::from_value(row["order"].clone()).unwrap();
    let hash = nautilus_predict::signing::PredictOrderHasher::new(PredictEnvironment::BnbMainnet)
        .order_hash(
            &fields,
            nautilus_predict::common::contracts::PredictExchangeType::Standard,
        );
    row["order"]["hash"] = json!(hash);
    *private.state.order_snapshot.lock().unwrap() = Some(json!({"success":true,"data":row}));
    let mut history = matches::match_row(1);
    history["settlementId"] = json!("strategy-recovery");
    history["taker"]["hash"] = json!(hash);
    history["taker"]["amount"] = json!("200000000000000000000");
    history["taker"]["price"] = json!("10000000000000000");
    history["taker"]["fee"] = json!({"type":"SHARES","amount":"4000000000000000000"});
    history["executedAt"] =
        json!(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true));
    *private.state.match_snapshot.lock().unwrap() = Some(vec![history]);
    let mut own = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(trader_id())
        .strategy_id("PREDICT-EXIT-001".into())
        .client_order_id("EXIT-ENTRY-001".into())
        .instrument_id(instrument_id(1))
        .side(OrderSide::Buy)
        .quantity(Quantity::from(200))
        .price(Price::from("0.010"))
        .build();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    own.apply(factory.generate_order_submitted(&own, now))
        .unwrap();
    own.apply(factory.generate_order_accepted(&own, format!("{hash:#x}").into(), now, now))
        .unwrap();
    own.apply(factory.generate_order_canceled(&own, own.venue_order_id(), now, now))
        .unwrap();
    let cache = live.kernel().cache.clone();
    cache
        .borrow_mut()
        .add_instrument(instrument(1).into())
        .unwrap();
    cache
        .borrow_mut()
        .add_order(own.clone(), None, Some(ClientId::from("PREDICT")), false)
        .unwrap();
    cache
        .borrow_mut()
        .add_venue_order_id(
            &own.client_order_id(),
            &own.venue_order_id().unwrap(),
            false,
        )
        .unwrap();
    let journal = Rc::new(RefCell::new(vec![]));
    live.add_strategy(
        strategy::ExitStrategy::new(
            instrument_id(1),
            1,
            journal.clone(),
            strategy::ExitSettings::parse(include_str!(
                "../../../../examples/predict-exit-strategy.toml.example"
            ))
            .unwrap(),
            200,
        )
        .with_live_identity(42, account_id(), 0)
        .without_shutdown_cancels_for_restart_replay(),
    )
    .unwrap();
    let handle = live.handle();
    let driver = async {
        barrier("streams connected", || {
            !public.0.lock().unwrap().is_empty()
                && !private.state.sockets.lock().unwrap().is_empty()
        })
        .await;
        let mut success = fill(1);
        success["orderHash"] = json!(hash);
        success["timestamp"] =
            json!(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000);
        success["settlementId"] = json!("strategy-recovery");
        success["fill"] = json!({"executedPriceWei":"10000000000000000", "executedSizeWei":"200000000000000000000", "executedValueWei":"2000000000000000000"});
        success["fee"] = json!({"type":"SHARES","amountWei":"4000000000000000000"});
        let mut submitted = success.clone();
        submitted["type"] = json!("orderTransactionSubmitted");
        submitted["kind"] = json!("PURCHASE");
        submitted["fill"] = json!({"executedPriceWei":"10000000000000000", "executedSizeWei":"199000000000000000000", "executedValueWei":"1990000000000000000"});
        private.wallet(submitted);
        private.wallet(success.clone());
        barrier("Native reconciled inventory", || {
            cache
                .borrow()
                .positions_open(None, Some(&instrument_id(1)), None, None, None)
                .first()
                .is_some_and(|position| position.quantity == Quantity::from("196"))
        })
        .await;
        assert!(
            journal
                .borrow()
                .iter()
                .any(|entry| entry["callback"] == "on_order_filled")
        );
        let book = json!({"marketId":42,"updateTimestampMs":get_atomic_clock_realtime().get_time_ns().as_u64()/1_000_000,
            "orderCount":2,"bids":[["0.012","1000"]],"asks":[["0.014","1000"]]});
        *private.state.execution_book.lock().unwrap() = book.clone();
        for sender in public.0.lock().unwrap().iter() {
            sender
                .send(Message::Text(
                    json!({"type":"M","topic":"predictOrderbook/42","data":book})
                        .to_string()
                        .into(),
                ))
                .unwrap();
        }
        barrier("strategy maker exit", || {
            !private.state.creations.lock().unwrap().is_empty()
        })
        .await;
        let maker = private.state.creations.lock().unwrap()[0].clone();
        assert_eq!(maker["data"]["strategy"], "LIMIT");
        assert_eq!(maker["data"]["isPostOnly"], true);
        assert_eq!(maker["data"]["pricePerShare"], "13000000000000000");
        assert_eq!(
            maker["data"]["order"]["makerAmount"],
            "196000000000000000000"
        );
        private.wallet(success);
        private.wallet(exit_orders::wire_event(
            &maker,
            "orderAccepted",
            1,
            Decimal::ZERO,
        ));
        private.wallet(exit_orders::settlement(
            &maker,
            1,
            196,
            196,
            "0.013",
            "exit-after-recovery",
            true,
        ));
        barrier("take profit fill closes reconciled inventory", || {
            cache
                .borrow()
                .positions_open(None, Some(&instrument_id(1)), None, None, None)
                .is_empty()
        })
        .await;
        handle.stop();
    };
    timeout(Duration::from_secs(12), async {
        let (result, ()) = tokio::join!(live.run(), driver);
        result.unwrap();
    })
    .await
    .unwrap_or_else(|_| panic!("recovery exit timed out: {:?}", journal.borrow()));
    assert_eq!(private.state.creations.lock().unwrap().len(), 1);
    task.abort();
}
