// Copyright (C) 2026 yfclark and contributors. Licensed under LGPL-3.0.

use nautilus_model::events::OrderEventAny;

use super::*;

pub(super) fn cancel(order: &OrderAny) -> CancelOrder {
    CancelOrder::new(
        trader_id(),
        Some(ClientId::new("PREDICT")),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        order.venue_order_id(),
        UUID4::new(),
        UnixNanos::from(OBSERVED),
        None,
        None,
    )
}

#[tokio::test]
async fn wallet_gap_during_cancel_preflight_still_allows_authenticated_removal() {
    let server = server(vec![]).await;
    removal(
        &server,
        200,
        json!({"success":true,"removed":["12345"],"noop":[]}),
        0,
    );
    let (mut client, mut rx, cache, _portfolio, order) = setup(&server).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    *server.state.order_gate.lock().unwrap() = Some(gate.clone());
    client.cancel_order(cancel(&order)).unwrap();
    super::exit_orders::barrier("cancel preflight", || {
        !server.state.order_requests.lock().unwrap().is_empty()
    })
    .await;
    server
        .state
        .sockets
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .send(Message::Close(None))
        .unwrap();
    super::exit_orders::barrier("wallet disconnected", || !client.is_connected()).await;
    gate.notify_one();
    let ExecutionEvent::Order(event @ OrderEventAny::Canceled(_)) = next(&mut rx).await else {
        panic!("definitive REST cancellation")
    };
    engine(&cache).process(&event);
    assert_eq!(server.state.removals.lock().unwrap().len(), 1);
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Canceled
    );
    client.disconnect().await.unwrap();
}

async fn setup(
    server: &Server,
) -> (
    PredictExecutionClient,
    mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
    Portfolio,
    OrderAny,
) {
    let row = rest_order_data("OPEN", 1, OrderSide::Buy);

    server
        .state
        .orders
        .lock()
        .unwrap()
        .push_back(json!({"success":true,"data":row}));
    let (mut client, mut rx, cache) = reported_client_with_removal(server, true);
    let portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let order = order(1);
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    (client, rx, cache, portfolio, order)
}

pub(super) fn engine(cache: &Rc<RefCell<Cache>>) -> ExecutionEngine {
    let mut clock = VirtualClock::new();
    clock.advance_time(UnixNanos::from(OBSERVED), true);
    ExecutionEngine::new(Rc::new(RefCell::new(clock)), cache.clone(), None)
}

fn removal(server: &Server, status: u16, body: Value, delay: u64) {
    server
        .state
        .remove_replies
        .lock()
        .unwrap()
        .push_back((status, body, delay));
}

async fn wait_for_post(server: &Server) {
    timeout(Duration::from_secs(2), async {
        loop {
            if !server.state.removals.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn acknowledged_removal_flows_through_native_engine_and_portfolio() {
    let server = server(vec![]).await;
    removal(
        &server,
        200,
        json!({"success":true,"removed":["12345"],"noop":[]}),
        0,
    );
    let (mut client, mut rx, cache, _portfolio, order) = setup(&server).await;
    assert_eq!(
        cache
            .borrow()
            .account(&account_id())
            .unwrap()
            .balance_locked(Some(Currency::USDT())),
        Some(Money::from("62 USDT"))
    );
    // The SDK represents no requested expiry as 2100-01-01, including GTC orders.
    server.state.orders.lock().unwrap()[0]["data"]["order"]["expiration"] = json!(4102444800i64);
    client.cancel_order(cancel(&order)).unwrap();
    assert!(client.cancel_order(cancel(&order)).is_err());
    let ExecutionEvent::Order(event @ OrderEventAny::Canceled(_)) = next(&mut rx).await else {
        panic!("native canceled event");
    };
    assert_eq!(event.client_order_id(), order.client_order_id());
    // Until the engine applies the event, native state and command ownership stay active.
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Accepted
    );
    assert!(client.cancel_order(cancel(&order)).is_err());
    let mut engine = engine(&cache);
    engine.process(&event);
    apply_account(&mut rx).await;
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Canceled
    );
    assert_eq!(
        cache
            .borrow()
            .account(&account_id())
            .unwrap()
            .balance_free(Some(Currency::USDT())),
        Some(Money::from("1000 USDT"))
    );
    assert!(
        cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
    );
    assert_eq!(
        *server.state.removals.lock().unwrap(),
        [json!({"data":{"ids":["12345"]}})]
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn removed_ack_preserves_confirmed_partial_fill_and_native_position() {
    let server = server(vec![]).await;
    removal(
        &server,
        200,
        json!({"success":true,"removed":["12345"],"noop":[]}),
        150,
    );
    let (mut client, mut rx, cache, _portfolio, order) = setup(&server).await;
    let mut engine = engine(&cache);
    client.cancel_order(cancel(&order)).unwrap();
    wait_for_post(&server).await;
    *server.state.rpc_frame.lock().unwrap() = (989_360_000_000_000_000_000u128, 2, 2);
    server.wallet(fill(1));
    let mut fill_seen = false;
    let mut cancel_seen = false;
    let mut unlocked = false;
    timeout(Duration::from_secs(3), async {
        while !fill_seen || !cancel_seen || !unlocked {
            match rx.recv().await.unwrap() {
                ExecutionEvent::Report(report) => {
                    engine.reconcile_execution_report(&report);
                    fill_seen = true;
                }
                ExecutionEvent::Order(event @ OrderEventAny::Canceled(_)) => {
                    engine.process(&event);
                    cancel_seen = true;
                }
                ExecutionEvent::Account(state) => {
                    msgbus::send_account_state(
                        MessagingSwitchboard::portfolio_update_account(),
                        &state,
                    );
                    unlocked = state.balances[0].locked == Money::from("0 USDT");
                }
                _ => panic!("unexpected execution event"),
            }
        }
    })
    .await
    .unwrap();
    {
        let cache = cache.borrow();
        let native = cache.order(&order.client_order_id()).unwrap();
        assert_eq!(native.status(), OrderStatus::Canceled);
        assert_eq!(native.filled_qty().as_decimal(), Decimal::new(17, 0));
        assert_eq!(
            cache.positions(None, None, None, None, None)[0]
                .quantity
                .as_decimal(),
            Decimal::new(17, 0)
        );
        assert_eq!(
            cache
                .account(&account_id())
                .unwrap()
                .balance_free(Some(Currency::USDT())),
            Some(Money::from("989.36 USDT"))
        );
    }
    assert_eq!(server.state.removals.lock().unwrap().len(), 1);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn only_definitive_rejection_emits_cancel_rejected_and_ambiguous_writes_stay_pending() {
    for case in 0..6 {
        let server = server(vec![]).await;
        let (status, reply, delay) = match case {
            0 => (400, json!({"error":"fixture-rejection"}), 0),
            1 => (500, json!({"error":"fixture-unknown"}), 0),
            2 => (200, json!({"success":true,"removed":[],"noop":[]}), 0),
            3 => (
                200,
                json!({"success":true,"removed":["99999"],"noop":[]}),
                0,
            ),
            4 => (
                200,
                json!({"success":true,"removed":["12345"],"noop":[]}),
                1500,
            ),
            _ => (
                200,
                json!({"success":false,"removed":["12345"],"noop":[]}),
                0,
            ),
        };
        removal(&server, status, reply, delay);
        let (mut client, mut rx, cache, _portfolio, order) = setup(&server).await;
        client.cancel_order(cancel(&order)).unwrap();
        wait_for_post(&server).await;
        if case == 0 {
            let ExecutionEvent::Order(event @ OrderEventAny::CancelRejected(_)) =
                next(&mut rx).await
            else {
                panic!("native cancel rejected");
            };
            engine(&cache).process(&event);
        } else {
            assert!(
                timeout(
                    Duration::from_millis(if case == 4 { 1700 } else { 100 }),
                    rx.recv()
                )
                .await
                .is_err()
            );
            assert!(client.cancel_order(cancel(&order)).is_err());
        }
        assert_eq!(
            cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Accepted
        );
        assert_eq!(
            cache
                .borrow()
                .account(&account_id())
                .unwrap()
                .balance_locked(Some(Currency::USDT())),
            Some(Money::from("62 USDT"))
        );
        assert_eq!(server.state.removals.lock().unwrap().len(), 1);
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn command_identity_and_signed_preflight_terms_are_checked_before_writing() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache, _portfolio, order) = setup(&server).await;
    for field in 0..5 {
        let mut command = cancel(&order);
        match field {
            0 => command.trader_id = TraderId::new("OTHER-001"),
            1 => command.strategy_id = "OTHER-001".into(),
            2 => command.instrument_id = instrument_id(2),
            3 => command.venue_order_id = Some(VenueOrderId::new(format!("0x{}", "22".repeat(32)))),
            _ => command.client_id = Some(ClientId::new("OTHER")),
        }
        assert!(client.cancel_order(command).is_err());
    }
    let mut invalid = server.state.orders.lock().unwrap().pop_front().unwrap();
    invalid["data"]["order"]["makerAmount"] = json!("63000000000000000000");
    server.state.orders.lock().unwrap().push_back(invalid);
    client.cancel_order(cancel(&order)).unwrap();
    let ExecutionEvent::Order(event @ OrderEventAny::CancelRejected(_)) = next(&mut rx).await
    else {
        panic!("invalid preflight rejection");
    };
    engine(&cache).process(&event);
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Accepted
    );
    assert!(server.state.removals.lock().unwrap().is_empty());
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn stop_cancels_in_flight_removal_without_publishing_a_late_ack() {
    let server = server(vec![]).await;
    removal(
        &server,
        200,
        json!({"success":true,"removed":["12345"],"noop":[]}),
        250,
    );
    let (mut client, mut rx, cache, _portfolio, order) = setup(&server).await;
    client.cancel_order(cancel(&order)).unwrap();
    wait_for_post(&server).await;
    client.disconnect().await.unwrap();
    assert!(!client.is_connected());
    assert!(
        timeout(Duration::from_millis(350), rx.recv())
            .await
            .is_err()
    );
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Accepted
    );
    assert_eq!(server.state.removals.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn wallet_terminal_event_wins_over_late_removal_ack() {
    let server = server(vec![]).await;
    removal(
        &server,
        200,
        json!({"success":true,"removed":["12345"],"noop":[]}),
        250,
    );
    let (mut client, mut rx, cache, _portfolio, order) = setup(&server).await;
    client.cancel_order(cancel(&order)).unwrap();
    wait_for_post(&server).await;
    server.wallet(event("orderCancelled", 1));
    let ExecutionEvent::Order(event @ OrderEventAny::Canceled(_)) = next(&mut rx).await else {
        panic!("native wallet cancellation");
    };
    engine(&cache).process(&event);
    apply_account(&mut rx).await;
    assert!(
        timeout(Duration::from_millis(350), rx.recv())
            .await
            .is_err()
    );
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Canceled
    );
    assert_eq!(server.state.removals.lock().unwrap().len(), 1);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn wallet_disconnect_preserves_definitive_removal_publication() {
    let server = server(vec![]).await;
    removal(
        &server,
        200,
        json!({"success":true,"removed":["12345"],"noop":[]}),
        250,
    );
    let (mut client, mut rx, cache, _portfolio, order) = setup(&server).await;
    client.cancel_order(cancel(&order)).unwrap();
    wait_for_post(&server).await;
    server
        .state
        .sockets
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .send(Message::Close(None))
        .unwrap();
    timeout(Duration::from_secs(2), async {
        while client.is_connected() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let ExecutionEvent::Order(event @ OrderEventAny::Canceled(_)) = next(&mut rx).await else {
        panic!("validated REST removal")
    };
    engine(&cache).process(&event);
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Canceled
    );
    assert_eq!(server.state.removals.lock().unwrap().len(), 1);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn rest_removal_is_disabled_by_default() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let order = order(1);
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    client.cancel_order(cancel(&order)).unwrap();
    let ExecutionEvent::Order(OrderEventAny::CancelRejected(event)) = next(&mut rx).await else {
        panic!("disabled removal must publish native rejection");
    };
    assert!(event.reason.contains("not enabled"));
    assert!(server.state.removals.lock().unwrap().is_empty());
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn settlement_after_removed_ack_updates_position_without_reopening_cancelled_order() {
    let server = server(vec![]).await;
    removal(
        &server,
        200,
        json!({"success":true,"removed":["12345"],"noop":[]}),
        0,
    );
    let (mut client, mut rx, cache, _portfolio, order) = setup(&server).await;
    client.cancel_order(cancel(&order)).unwrap();
    let ExecutionEvent::Order(event @ OrderEventAny::Canceled(_)) = next(&mut rx).await else {
        panic!("native canceled event");
    };
    let mut engine = engine(&cache);
    engine.process(&event);
    apply_account(&mut rx).await;
    *server.state.rpc_frame.lock().unwrap() = (989_360_000_000_000_000_000u128, 2, 2);
    server.wallet(fill(1));
    let ExecutionEvent::Report(report) = next(&mut rx).await else {
        panic!("late confirmed fill");
    };
    engine.reconcile_execution_report(&report);
    apply_account(&mut rx).await;
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Canceled
    );
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .filled_qty()
            .as_decimal(),
        Decimal::new(17, 0)
    );
    assert_eq!(
        cache.borrow().positions(None, None, None, None, None)[0]
            .quantity
            .as_decimal(),
        Decimal::new(17, 0)
    );
    assert_eq!(
        cache
            .borrow()
            .account(&account_id())
            .unwrap()
            .balance_locked(Some(Currency::USDT())),
        Some(Money::from("0 USDT"))
    );
    assert_eq!(
        cache
            .borrow()
            .account(&account_id())
            .unwrap()
            .balance_free(Some(Currency::USDT())),
        Some(Money::from("989.36 USDT"))
    );
    assert!(client.is_connected());
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn disconnected_owned_commands_publish_rejections_without_writes() {
    use nautilus_common::messages::execution::ModifyOrder;
    let server = server(vec![]).await;
    let (mut client, mut rx, cache, _portfolio, order) = setup(&server).await;
    client.stop().unwrap();
    client.cancel_order(cancel(&order)).unwrap();
    let ExecutionEvent::Order(event @ OrderEventAny::CancelRejected(_)) = next(&mut rx).await
    else {
        panic!("disconnected cancel rejection");
    };
    let mut engine = engine(&cache);
    engine.process(&event);
    client
        .modify_order(ModifyOrder::new(
            trader_id(),
            Some(ClientId::new("PREDICT")),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            order.venue_order_id(),
            None,
            Some(Price::from("0.610")),
            None,
            UUID4::new(),
            UnixNanos::from(OBSERVED),
            None,
            None,
        ))
        .unwrap();
    let ExecutionEvent::Order(event @ OrderEventAny::ModifyRejected(_)) = next(&mut rx).await
    else {
        panic!("disconnected modify rejection");
    };
    engine.process(&event);
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Accepted
    );
    assert!(server.state.removals.lock().unwrap().is_empty());
    assert!(server.state.creations.lock().unwrap().is_empty());
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn verified_wallet_numeric_id_skips_cancel_get_and_survives_wallet_gap() {
    let server = server(vec![]).await;
    removal(
        &server,
        200,
        json!({"success":true,"removed":["12345"],"noop":[]}),
        0,
    );
    let (mut client, mut rx, cache, _portfolio, order) = setup(&server).await;
    server.wallet(fill(1));
    let ExecutionEvent::Report(report @ ExecutionReport::Fill(_)) = next(&mut rx).await else {
        panic!("verified wallet fill")
    };
    engine(&cache).reconcile_execution_report(&report);
    // Route is registered before enqueue; a subsequent socket gap cannot invalidate REST identity.
    server
        .state
        .sockets
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .send(Message::Close(None))
        .unwrap();
    super::exit_orders::barrier("wallet gap", || !client.is_connected()).await;
    client.cancel_order(cancel(&order)).unwrap();
    let ExecutionEvent::Order(event @ OrderEventAny::Canceled(_)) = next(&mut rx).await else {
        panic!("REST removal")
    };
    engine(&cache).process(&event);
    assert!(server.state.order_requests.lock().unwrap().is_empty());
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Canceled
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn cached_batch_leg_is_removed_while_another_leg_waits_for_its_rest_id() {
    use nautilus_common::messages::execution::BatchCancelOrders;
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = reported_client_with_removal(&server, true);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let mut orders = Vec::new();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    for (name, hash, id) in [
        ("FAST", HASH.to_owned(), "12345"),
        ("LOOKUP", format!("0x{}", "22".repeat(32)), "12346"),
    ] {
        let mut order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(trader_id())
            .strategy_id("CANCEL-001".into())
            .client_order_id(name.into())
            .instrument_id(instrument_id(1))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100))
            .price(Price::from("0.620"))
            .build();
        order
            .apply(factory.generate_order_submitted(&order, UnixNanos::from(1)))
            .unwrap();
        order
            .apply(factory.generate_order_accepted(
                &order,
                hash.as_str().into(),
                UnixNanos::from(2),
                UnixNanos::from(2),
            ))
            .unwrap();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, Some("PREDICT".into()), false)
            .unwrap();
        let mut row = rest_order_data("OPEN", 1, OrderSide::Buy);
        row["id"] = json!(id);
        row["order"]["hash"] = json!(hash);
        if name == "FAST" {
            server.state.open_orders.lock().unwrap().push(row.clone());
        }
        server
            .state
            .orders_by_hash
            .lock()
            .unwrap()
            .insert(hash, json!({"success":true,"data":row}));
        orders.push(order);
    }
    connect_reported(&mut client, &mut rx).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    *server.state.order_gate.lock().unwrap() = Some(gate.clone());
    for _ in 0..2 {
        removal(&server, 200, json!({"echoRemoved":true}), 0);
    }
    client
        .batch_cancel_orders(BatchCancelOrders::new(
            trader_id(),
            Some("PREDICT".into()),
            orders[0].strategy_id(),
            instrument_id(1),
            orders.iter().map(cancel).collect(),
            UUID4::new(),
            UnixNanos::from(OBSERVED),
            None,
            None,
        ))
        .unwrap();
    let mut engine = engine(&cache);
    // The lookup gate remains held: waiting for all preflights would strand this callback.
    loop {
        if let ExecutionEvent::Order(event @ OrderEventAny::Canceled(_)) = next(&mut rx).await {
            assert_eq!(event.client_order_id(), orders[0].client_order_id());
            engine.process(&event);
            break;
        }
    }
    assert_eq!(
        *server.state.removals.lock().unwrap(),
        [json!({"data":{"ids":["12345"]}})]
    );
    gate.notify_one();
    loop {
        if let ExecutionEvent::Order(event @ OrderEventAny::Canceled(_)) = next(&mut rx).await {
            assert_eq!(event.client_order_id(), orders[1].client_order_id());
            engine.process(&event);
            break;
        }
    }
    assert!(
        cache
            .borrow()
            .orders_open(None, None, None, None, None)
            .is_empty()
    );
    assert_eq!(server.state.removals.lock().unwrap().len(), 2);
    assert!(
        server
            .state
            .order_requests
            .lock()
            .unwrap()
            .iter()
            .all(|path| !path.contains(HASH))
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn late_removed_ack_releases_native_only_cancellation_without_another_account_read() {
    let server = server(vec![]).await;
    server
        .state
        .open_orders
        .lock()
        .unwrap()
        .push(rest_order_data("OPEN", 1, OrderSide::Buy));
    removal(
        &server,
        200,
        json!({"success":true,"removed":["12345"],"noop":[]}),
        0,
    );
    let (mut client, mut rx, cache, _portfolio, order) = setup(&server).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    *server.state.remove_gate.lock().unwrap() = Some(gate.clone());
    client.cancel_order(cancel(&order)).unwrap();
    wait_for_post(&server).await;
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    let synthetic =
        factory.generate_order_canceled(&order, order.venue_order_id(), 400.into(), 400.into());
    engine(&cache).process(&synthetic);
    let uncertain = apply_account(&mut rx).await;
    assert_eq!(
        uncertain.balances[0].locked,
        Money::from("62 USDT"),
        "Native-only Canceled does not establish removal"
    );
    let rpc = server.state.rpc_methods.lock().unwrap().len();
    let open = server.state.requests.lock().unwrap().len();
    gate.notify_one();
    let confirmed = apply_account(&mut rx).await;
    assert_eq!(confirmed.balances[0].free, Money::from("1000 USDT"));
    assert_eq!(server.state.rpc_methods.lock().unwrap().len(), rpc);
    assert_eq!(server.state.requests.lock().unwrap().len(), open);
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Canceled
    );
    // apply_account rejects any duplicate order event before the account publication.
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn validated_query_reuses_rest_route_for_cancel_without_another_lookup() {
    let server = server(vec![]).await;
    removal(
        &server,
        200,
        json!({"success":true,"removed":["12345"],"noop":[]}),
        0,
    );
    let mut row = rest_order_data("OPEN", 1, OrderSide::Buy);
    let hash = nautilus_predict::signing::PredictOrderHasher::new(PredictEnvironment::BnbMainnet)
        .order_hash(
            &serde_json::from_value(row["order"].clone()).unwrap(),
            nautilus_predict::common::contracts::PredictExchangeType::Standard,
        );
    let venue_id = VenueOrderId::new(format!("{hash:#x}"));
    row["order"]["hash"] = json!(venue_id.to_string());
    server
        .state
        .orders_by_hash
        .lock()
        .unwrap()
        .insert(venue_id.to_string(), json!({"success":true,"data":row}));
    let (mut client, mut rx, cache) = reported_client_with_removal(&server, true);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let original = order(1);
    let mut native = OrderAny::try_from(original.init_event().clone()).unwrap();
    for event in original.events().into_iter().skip(1) {
        let mut event = event.clone();
        if let OrderEventAny::Accepted(event) = &mut event {
            event.venue_order_id = venue_id;
        }
        native.apply(event).unwrap();
    }
    cache
        .borrow_mut()
        .add_order(native.clone(), None, None, false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let mut engine = engine(&cache);
    client
        .query_order(nautilus_common::messages::execution::QueryOrder::new(
            trader_id(),
            Some(ClientId::new("PREDICT")),
            native.strategy_id(),
            native.instrument_id(),
            native.client_order_id(),
            Some(venue_id),
            UUID4::new(),
            OBSERVED.into(),
            None,
            None,
        ))
        .unwrap();
    loop {
        let event = next(&mut rx).await;
        let report = matches!(&event, ExecutionEvent::Report(_));
        submissions::apply(&mut engine, &event);
        if report {
            break;
        }
    }
    assert_eq!(server.state.order_requests.lock().unwrap().len(), 1);
    client.cancel_order(cancel(&native)).unwrap();
    loop {
        let event = next(&mut rx).await;
        let canceled = matches!(&event, ExecutionEvent::Order(OrderEventAny::Canceled(_)));
        submissions::apply(&mut engine, &event);
        if canceled {
            break;
        }
    }
    assert_eq!(
        cache
            .borrow()
            .order(&native.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Canceled
    );
    assert_eq!(server.state.removals.lock().unwrap().len(), 1);
    assert_eq!(
        server.state.order_requests.lock().unwrap().len(),
        1,
        "validated QueryOrder already supplied the removal route"
    );
    assert!(server.state.match_requests.lock().unwrap().is_empty());
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn noop_cancel_does_not_start_an_adapter_owned_recovery_query() {
    let server = server(vec![]).await;
    removal(
        &server,
        200,
        json!({"success":true,"removed":[],"noop":["12345"]}),
        0,
    );
    let (mut client, mut rx, cache, _portfolio, order) = setup(&server).await;
    server.wallet(fill(1));
    let ExecutionEvent::Report(report @ ExecutionReport::Fill(_)) = next(&mut rx).await else {
        panic!("verified wallet fill");
    };
    let mut engine = engine(&cache);
    engine.reconcile_execution_report(&report);
    client.cancel_order(cancel(&order)).unwrap();
    wait_for_post(&server).await;
    let _ = timeout(Duration::from_millis(100), async {
        loop {
            let event = next(&mut rx).await;
            assert!(
                matches!(&event, ExecutionEvent::Account(_)),
                "noop must not invent a terminal event"
            );
            submissions::apply(&mut engine, &event);
        }
    })
    .await;
    assert!(
        server.state.order_requests.lock().unwrap().is_empty(),
        "Native owns recovery of the ambiguous noop"
    );
    assert_eq!(server.state.removals.lock().unwrap().len(), 1);
    assert!(
        !cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .is_closed()
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn route_lookup_terminal_is_published_without_remove_or_fill_queries() {
    for (remote, expected) in [
        ("CANCELLED", OrderStatus::Canceled),
        ("EXPIRED", OrderStatus::Expired),
    ] {
        let private = server(vec![]).await;
        let (mut client, mut rx, cache, _portfolio, own) = setup(&private).await;
        private.state.orders.lock().unwrap().clear();
        let mut terminal = rest_order_data(remote, 1, OrderSide::Buy);
        terminal["amountFilled"] = json!("17000000000000000000");
        private
            .state
            .orders
            .lock()
            .unwrap()
            .push_back(json!({"success":true,"data":terminal}));
        let callbacks = Rc::new(RefCell::new(Vec::new()));
        let handler = nautilus_common::msgbus::TypedHandler::from({
            let callbacks = callbacks.clone();
            let cache = cache.clone();
            let id = own.client_order_id();
            move |event: &OrderEventAny| {
                if event.client_order_id() == id {
                    assert!(!matches!(event, OrderEventAny::CancelRejected(_)));
                    if matches!(
                        event,
                        OrderEventAny::Canceled(_) | OrderEventAny::Expired(_)
                    ) {
                        assert_eq!(cache.borrow().order(&id).unwrap().status(), expected);
                        callbacks.borrow_mut().push(event.event_type());
                    }
                }
            }
        });
        let topic = nautilus_common::msgbus::switchboard::get_event_order_topic(own.strategy_id());
        msgbus::subscribe_order_events(topic.into(), handler.clone(), None);
        let before = private.state.order_requests.lock().unwrap().len();
        client.cancel_order(cancel(&own)).unwrap();
        let mut engine = engine(&cache);
        timeout(Duration::from_secs(2), async {
            while callbacks.borrow().is_empty() {
                submissions::apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .expect("verified terminal lookup resolves the actual Native order");
        assert_eq!(
            private.state.order_requests.lock().unwrap().len(),
            before + 1
        );
        assert!(private.state.removals.lock().unwrap().is_empty());
        assert!(private.state.match_requests.lock().unwrap().is_empty());
        assert!(
            cache
                .borrow()
                .order(&own.client_order_id())
                .unwrap()
                .filled_qty()
                .is_zero()
        );
        msgbus::unsubscribe_order_events(topic.into(), &handler);
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn native_batch_modify_fanout_resolves_each_pending_update_without_writes() {
    use nautilus_common::messages::execution::{BatchModifyOrders, ModifyOrder};
    use nautilus_model::{events::OrderPendingUpdate, identifiers::ClientOrderId};

    let server = server(vec![]).await;
    let (client, mut rx, cache) = client(&server);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    let mut orders = Vec::new();
    let mut modifies = Vec::new();
    for n in 0..2 {
        let mut order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(trader_id())
            .client_order_id(ClientOrderId::new(format!("NATIVE-BATCH-MODIFY-{n}")))
            .instrument_id(instrument_id(1))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100))
            .price(Price::from("0.620"))
            .build();
        order
            .apply(factory.generate_order_submitted(&order, 10.into()))
            .unwrap();
        let venue_id = VenueOrderId::new(format!("0x{:064x}", n + 1));
        order
            .apply(factory.generate_order_accepted(&order, venue_id, 100.into(), 100.into()))
            .unwrap();
        order
            .apply(OrderEventAny::PendingUpdate(OrderPendingUpdate::new(
                trader_id(),
                order.strategy_id(),
                order.instrument_id(),
                order.client_order_id(),
                Some(account_id()),
                UUID4::new(),
                OBSERVED.into(),
                OBSERVED.into(),
                false,
                Some(venue_id),
            )))
            .unwrap();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();
        modifies.push(ModifyOrder::new(
            trader_id(),
            Some(ClientId::new("PREDICT")),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            Some(venue_id),
            None,
            Some(Price::from("0.610")),
            None,
            UUID4::new(),
            OBSERVED.into(),
            None,
            None,
        ));
        orders.push(order);
    }
    let callbacks = Rc::new(RefCell::new(Vec::new()));
    let callback_ids = Rc::clone(&callbacks);
    let callback_cache = Rc::clone(&cache);
    let handler = msgbus::TypedHandler::from(move |event: &OrderEventAny| {
        if let OrderEventAny::ModifyRejected(event) = event {
            let cache = callback_cache.borrow();
            let order = cache.order(&event.client_order_id).unwrap();
            assert_eq!(order.status(), OrderStatus::Accepted);
            assert_eq!(order.price(), Some(Price::from("0.620")));
            callback_ids.borrow_mut().push(event.client_order_id);
        }
    });
    let topic = format!("events.order.{}", orders[0].strategy_id());
    msgbus::subscribe_order_events(topic.as_str().into(), handler.clone(), None);
    client
        .batch_modify_orders(BatchModifyOrders::new(
            trader_id(),
            Some(ClientId::new("PREDICT")),
            orders[0].strategy_id(),
            instrument_id(1),
            modifies,
            UUID4::new(),
            OBSERVED.into(),
            None,
            None,
        ))
        .unwrap();
    let mut engine = engine(&cache);
    for _ in 0..orders.len() {
        let ExecutionEvent::Order(event @ OrderEventAny::ModifyRejected(_)) = next(&mut rx).await
        else {
            panic!("each Native batch child must receive ModifyRejected");
        };
        engine.process(&event);
    }
    assert_eq!(
        *callbacks.borrow(),
        orders
            .iter()
            .map(Order::client_order_id)
            .collect::<Vec<_>>()
    );
    assert!(rx.try_recv().is_err());
    assert!(server.state.creations.lock().unwrap().is_empty());
    assert!(server.state.removals.lock().unwrap().is_empty());
    assert!(server.state.requests.lock().unwrap().is_empty());
    msgbus::unsubscribe_order_events(topic.as_str().into(), &handler);
}
