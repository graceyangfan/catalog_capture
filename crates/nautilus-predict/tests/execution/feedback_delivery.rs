// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/lgpl-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Confirmed facts reach Native independently of ancillary routing and recovery IO.
use super::*;
use nautilus_common::msgbus::{TypedHandler, switchboard};
use nautilus_model::{enums::TimeInForce, events::OrderEventAny};

#[tokio::test]
async fn recorded_market_params_and_execution_survive_held_post_and_cash_refresh() {
    let saved: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/native_market_fill_post_only_rejected_20261025.json"
    ))
    .unwrap();
    let private = server(vec![]).await;
    *private.state.market_fee.lock().unwrap() = 200;
    let (mut client, mut rx, cache) = submissions::setup_client(&private, OBSERVED);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(instrument(2).into())
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let own = OrderTestBuilder::new(OrderType::Market)
        .trader_id(trader_id())
        .instrument_id(instrument_id(2))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("3.5"))
        .time_in_force(TimeInForce::Fok)
        .build();
    cache
        .borrow_mut()
        .add_order(own.clone(), None, None, false)
        .unwrap();
    let post_gate = Arc::new(tokio::sync::Notify::new());
    *private.state.create_gate.lock().unwrap() = Some(post_gate.clone());
    let cash_gate = Arc::new(tokio::sync::Notify::new());
    *private.state.rpc_gate.lock().unwrap() = Some(cash_gate.clone());
    let rpc_before = private.state.rpc_methods.lock().unwrap().len();
    let nonce_before = private.state.nonce_requests.lock().unwrap().len();
    client
        .submit_order(market_preparation::priced(
            &own,
            saved["expected"]["referencePrice"].clone(),
            saved["expected"]["slippageBps"]
                .as_u64()
                .unwrap()
                .try_into()
                .unwrap(),
        ))
        .unwrap();
    let mut engine = submissions::engine(&cache);
    submissions::apply(&mut engine, &next(&mut rx).await);
    exit_orders::barrier("recorded MARKET POST without preflight", || {
        !private.state.creations.lock().unwrap().is_empty()
    })
    .await;
    assert_eq!(private.state.rpc_methods.lock().unwrap().len(), rpc_before);
    let body = private.state.creations.lock().unwrap()[0].clone();
    let original = &saved["nativeEntry"]["httpCreate"]["requestTerms"];
    assert_eq!(body["data"]["slippageBps"], "300");
    assert_eq!(body["data"]["pricePerShare"], "390000000000000000");
    assert_eq!(body["data"]["isFillOrKill"], true);
    assert_eq!(body["data"]["isMinAmountOut"], false);
    for key in ["makerAmount", "takerAmount"] {
        assert_eq!(body["data"]["order"][key], original[key], "{key}");
    }
    let callbacks = Rc::new(RefCell::new(Vec::new()));
    let topic = switchboard::get_event_order_topic(own.strategy_id());
    let handler = TypedHandler::from({
        let callbacks = callbacks.clone();
        let cache = cache.clone();
        let id = own.client_order_id();
        move |event: &OrderEventAny| {
            if let OrderEventAny::Filled(fill) = event
                && fill.client_order_id == id
            {
                assert_eq!(fill.last_qty, Quantity::from("3.5"));
                assert_eq!(fill.last_px, Price::from("0.40"));
                assert_eq!(fill.quantity_commission, Some(Quantity::from("0.07")));
                let state = cache.borrow();
                assert_eq!(state.order(&id).unwrap().status(), OrderStatus::Filled);
                assert_eq!(
                    state.positions_open(None, None, None, None, None)[0].quantity,
                    Quantity::from("3.43")
                );
                callbacks.borrow_mut().push(fill.trade_id);
            }
        }
    });
    msgbus::subscribe_order_events(topic.into(), handler.clone(), None);
    for raw in saved["walletEvents"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| {
            matches!(
                e["type"].as_str(),
                Some("orderAccepted" | "orderTransactionSubmitted" | "orderTransactionSuccess")
            )
        })
    {
        // Rebind identity only. Actual quantity, execution price, fee and settlement IDs stay intact.
        let mut raw = raw.clone();
        raw["walletAddress"] = json!(WALLET);
        raw["orderHash"] = body["data"]["order"]["hash"].clone();
        raw["details"]["marketId"] = json!(42);
        private.wallet(raw.clone());
        private.wallet(raw);
    }
    timeout(Duration::from_secs(2), async {
        while callbacks.borrow().is_empty() {
            submissions::apply(&mut engine, &next(&mut rx).await);
        }
    })
    .await
    .expect("actual confirmed fill cannot wait for POST or cash IO");
    wallet_processed(&private).await;
    assert_eq!(callbacks.borrow().len(), 1);
    assert_eq!(
        private.state.nonce_requests.lock().unwrap().len(),
        nonce_before
    );
    assert!(private.state.order_requests.lock().unwrap().is_empty());
    assert!(private.state.match_requests.lock().unwrap().is_empty());
    assert_eq!(private.state.creations.lock().unwrap().len(), 1);
    msgbus::unsubscribe_order_events(topic.into(), &handler);
    client.disconnect().await.unwrap();
    post_gate.notify_one();
    cash_gate.notify_one();
}

#[tokio::test]
async fn closed_optional_settlement_channel_does_not_suppress_execution_feedback() {
    let private = server(vec![]).await;
    let (data_tx, data_rx) = mpsc::unbounded_channel();
    nautilus_common::live::runner::replace_data_event_sender(data_tx);
    drop(data_rx);
    let (mut client, mut rx, cache, _portfolio, own) = submissions::connected(&private, 1).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    *private.state.create_gate.lock().unwrap() = Some(gate.clone());
    client.submit_order(submissions::submit(&own)).unwrap();
    let mut engine = submissions::engine(&cache);
    submissions::apply(&mut engine, &next(&mut rx).await);
    exit_orders::barrier("POST registered", || {
        !private.state.creations.lock().unwrap().is_empty()
    })
    .await;
    let body = private.state.creations.lock().unwrap()[0].clone();
    let callbacks = Rc::new(RefCell::new(Vec::new()));
    let topic = switchboard::get_event_order_topic(own.strategy_id());
    let handler = TypedHandler::from({
        let callbacks = callbacks.clone();
        let cache = cache.clone();
        let id = own.client_order_id();
        move |event: &OrderEventAny| {
            if event.client_order_id() == id {
                let expected = match event {
                    OrderEventAny::Accepted(_) => OrderStatus::Accepted,
                    OrderEventAny::Filled(_) => OrderStatus::PartiallyFilled,
                    OrderEventAny::Canceled(_) => OrderStatus::Canceled,
                    _ => panic!("unexpected execution callback"),
                };
                assert_eq!(cache.borrow().order(&id).unwrap().status(), expected);
                callbacks.borrow_mut().push(event.event_type());
            }
        }
    });
    msgbus::subscribe_order_events(topic.into(), handler.clone(), None);
    let mut success = fill(1);
    success["orderHash"] = body["data"]["order"]["hash"].clone();
    success["isMaker"] = json!(true);
    success["fee"]["amountWei"] = json!("0");
    let mut submitted = success.clone();
    submitted["type"] = json!("orderTransactionSubmitted");
    submitted["kind"] = json!("PURCHASE");
    submitted.as_object_mut().unwrap().remove("fee");
    submitted.as_object_mut().unwrap().remove("isMaker");
    private.wallet(submitted);
    timeout(Duration::from_secs(2), async {
        while cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .status()
            != OrderStatus::Accepted
        {
            submissions::apply(&mut engine, &next(&mut rx).await);
        }
    })
    .await
    .expect("settlement Submitted still proves Native acceptance");
    assert!(
        cache
            .borrow()
            .positions_open(None, None, None, None, None)
            .is_empty()
    );
    private.wallet(success);
    let mut canceled = event("orderCancelled", 1);
    canceled["orderHash"] = body["data"]["order"]["hash"].clone();
    canceled["details"]["quantityFilled"] = json!("17");
    private.wallet(canceled);
    timeout(Duration::from_secs(2), async {
        while cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .status()
            != OrderStatus::Canceled
        {
            submissions::apply(&mut engine, &next(&mut rx).await);
        }
    })
    .await
    .expect("fill and cancel callbacks remain deliverable");
    use nautilus_model::events::OrderEventType;
    assert_eq!(
        &*callbacks.borrow(),
        &[
            OrderEventType::Accepted,
            OrderEventType::Filled,
            OrderEventType::Canceled
        ]
    );
    assert_eq!(
        cache.borrow().positions_open(None, None, None, None, None)[0].quantity,
        Quantity::from(17)
    );
    assert!(private.state.order_requests.lock().unwrap().is_empty());
    assert!(private.state.match_requests.lock().unwrap().is_empty());
    assert_eq!(private.state.creations.lock().unwrap().len(), 1);
    msgbus::unsubscribe_order_events(topic.into(), &handler);
    client.disconnect().await.unwrap();
    gate.notify_one();
}

#[tokio::test]
async fn terminal_before_acceptance_and_after_instrument_expiry_does_not_wait_for_post_or_fills() {
    for (kind, status) in [
        ("orderCancelled", OrderStatus::Canceled),
        ("orderExpired", OrderStatus::Expired),
    ] {
        let private = server(vec![]).await;
        let (mut client, mut rx, cache, _portfolio, own) =
            submissions::connected(&private, 1).await;
        let gate = Arc::new(tokio::sync::Notify::new());
        *private.state.create_gate.lock().unwrap() = Some(gate.clone());
        client.submit_order(submissions::submit(&own)).unwrap();
        let mut engine = submissions::engine(&cache);
        submissions::apply(&mut engine, &next(&mut rx).await);
        exit_orders::barrier("POST registered before terminal", || {
            private.state.creations.lock().unwrap().len() == 1
        })
        .await;
        let body = private.state.creations.lock().unwrap()[0].clone();
        let mut expired = instrument(1);
        expired.expiration_ns = OBSERVED.into();
        let expired = InstrumentAny::BinaryOption(expired);
        cache.borrow_mut().add_instrument(expired.clone()).unwrap();
        client.on_instrument(expired);
        let callbacks = Rc::new(RefCell::new(Vec::new()));
        let handler = TypedHandler::from({
            let callbacks = callbacks.clone();
            let cache = cache.clone();
            let id = own.client_order_id();
            move |event: &OrderEventAny| {
                if event.client_order_id() != id {
                    return;
                }
                if matches!(
                    event,
                    OrderEventAny::Canceled(_)
                        | OrderEventAny::Expired(_)
                        | OrderEventAny::Filled(_)
                ) {
                    let native = cache.borrow();
                    let current = native.order(&id).unwrap();
                    if matches!(event, OrderEventAny::Filled(_)) {
                        assert_eq!(current.filled_qty(), Quantity::from(17));
                    } else {
                        assert_eq!(current.status(), status);
                        assert!(current.filled_qty().is_zero());
                    }
                    callbacks.borrow_mut().push(event.event_type());
                }
            }
        });
        let topic = switchboard::get_event_order_topic(own.strategy_id());
        msgbus::subscribe_order_events(topic.into(), handler.clone(), None);
        let mut terminal = event(kind, 1);
        terminal["orderHash"] = body["data"]["order"]["hash"].clone();
        terminal["orderId"] = json!("not-a-numeric-route");
        terminal["details"]["quantityFilled"] = json!("17");
        private.wallet(terminal);
        timeout(Duration::from_secs(2), async {
            while callbacks.borrow().is_empty() {
                submissions::apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .expect("confirmed terminal must not wait for acceptance, POST receipt or trades");
        assert!(
            cache
                .borrow()
                .order(&own.client_order_id())
                .unwrap()
                .venue_order_id()
                .is_some()
        );
        assert!(private.state.order_requests.lock().unwrap().is_empty());
        assert!(private.state.match_requests.lock().unwrap().is_empty());
        let mut settled = fill(1);
        settled["orderHash"] = body["data"]["order"]["hash"].clone();
        settled["orderId"] = json!("not-a-numeric-route");
        settled["isMaker"] = json!(true);
        settled["fee"]["amountWei"] = json!("0");
        private.wallet(settled.clone());
        private.wallet(settled);
        timeout(Duration::from_secs(2), async {
            while callbacks.borrow().len() < 2 {
                submissions::apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .expect("late confirmed fill remains deliverable after terminal");
        assert_eq!(
            cache.borrow().positions_open(None, None, None, None, None)[0].quantity,
            Quantity::from(17)
        );
        assert_eq!(callbacks.borrow().len(), 2);
        msgbus::unsubscribe_order_events(topic.into(), &handler);
        client.disconnect().await.unwrap();
        gate.notify_one();
    }
}

#[tokio::test]
async fn confirmed_fill_preserves_reported_price_and_liquidity_despite_intent_and_route_anomalies()
{
    for conflict in [false, true] {
        let private = server(vec![]).await;
        let (mut client, mut rx, cache) = client(&private);
        let mut definition = instrument(1);
        definition.expiration_ns = OBSERVED.into();
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(definition))
            .unwrap();
        let own = order_with_terms(1, OrderSide::Buy, true);
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        seed_account(&cache);
        client.connect().await.unwrap();
        if conflict {
            private.wallet(event("orderAccepted", 1));
            wallet_processed(&private).await;
        }
        let callbacks = Rc::new(RefCell::new(Vec::new()));
        let handler = TypedHandler::from({
            let callbacks = callbacks.clone();
            let cache = cache.clone();
            let id = own.client_order_id();
            move |event: &OrderEventAny| {
                if let OrderEventAny::Filled(fill) = event
                    && fill.client_order_id == id
                {
                    assert_eq!(fill.last_px, Price::from("0.630"));
                    assert_eq!(
                        fill.liquidity_side,
                        nautilus_model::enums::LiquiditySide::Taker
                    );
                    assert_eq!(
                        cache.borrow().order(&id).unwrap().filled_qty(),
                        Quantity::from(17)
                    );
                    callbacks.borrow_mut().push(fill.trade_id);
                }
            }
        });
        let topic = switchboard::get_event_order_topic(own.strategy_id());
        msgbus::subscribe_order_events(topic.into(), handler.clone(), None);
        let mut settled = fill(1);
        settled["orderId"] = json!(if conflict { "999999" } else { "not-a-number" });
        settled["fill"]["executedPriceWei"] = json!("630000000000000000");
        settled["fill"]["executedValueWei"] = json!("10710000000000000000");
        settled["fee"]["amountWei"] = json!("0");
        private.wallet(settled);
        let mut engine = submissions::engine(&cache);
        timeout(Duration::from_secs(2), async {
            while callbacks.borrow().is_empty() {
                submissions::apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .expect("real execution is not an admission policy decision");
        assert_eq!(
            cache.borrow().positions_open(None, None, None, None, None)[0].quantity,
            Quantity::from(17)
        );
        assert!(private.state.order_requests.lock().unwrap().is_empty());
        assert!(private.state.match_requests.lock().unwrap().is_empty());
        msgbus::unsubscribe_order_events(topic.into(), &handler);
        client.disconnect().await.unwrap();
    }
}

fn native_from_row(row: &mut Value) -> OrderAny {
    let hash = nautilus_predict::signing::PredictOrderHasher::new(PredictEnvironment::BnbMainnet)
        .order_hash(
            &serde_json::from_value(row["order"].clone()).unwrap(),
            nautilus_predict::common::contracts::PredictExchangeType::Standard,
        );
    row["order"]["hash"] = json!(hash);
    let original = order(1);
    let mut native = OrderAny::try_from(original.init_event().clone()).unwrap();
    for event in original.events().iter().skip(1) {
        let mut event = (*event).clone();
        if let OrderEventAny::Accepted(event) = &mut event {
            event.venue_order_id = VenueOrderId::new(format!("{hash:#x}"));
        }
        native.apply(event).unwrap();
    }
    native
}

#[tokio::test]
async fn later_native_query_is_not_swallowed_by_earlier_trade_collection() {
    let private = server(vec![]).await;
    let mut row = rest_order_data("OPEN", 1, OrderSide::Buy);
    row["amountFilled"] = json!("17000000000000000000");
    let own = native_from_row(&mut row);
    let mut trade = matches::match_row(1);
    trade["taker"]["hash"] = json!(own.venue_order_id().unwrap().to_string());
    trade["taker"]["fee"] = json!({"type":"SHARES","amount":"340000000000000000"});
    *private.state.match_snapshot.lock().unwrap() = Some(vec![trade]);
    let (mut client, mut rx, cache) = reported_client(&private);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(instrument(1).into())
        .unwrap();
    cache
        .borrow_mut()
        .add_order(own.clone(), None, None, false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    *private.state.order_snapshot.lock().unwrap() =
        Some(json!({"success":true,"data":row.clone()}));
    let gate = Arc::new(tokio::sync::Notify::new());
    *private.state.match_gate.lock().unwrap() = Some(gate.clone());
    let query = || {
        nautilus_common::messages::execution::QueryOrder::new(
            trader_id(),
            Some(ClientId::new("PREDICT")),
            own.strategy_id(),
            own.instrument_id(),
            own.client_order_id(),
            own.venue_order_id(),
            UUID4::new(),
            OBSERVED.into(),
            None,
            None,
        )
    };
    client.query_order(query()).unwrap();
    exit_orders::barrier("first Native query is reading trade history", || {
        private.state.match_requests.lock().unwrap().len() == 1
    })
    .await;
    let observed = Rc::new(RefCell::new(Vec::new()));
    let fills = Rc::new(RefCell::new(Vec::new()));
    let topic = switchboard::get_event_order_topic(own.strategy_id());
    let handler = TypedHandler::from({
        let observed = observed.clone();
        let fills = fills.clone();
        let cache = cache.clone();
        let id = own.client_order_id();
        move |event: &OrderEventAny| {
            if let OrderEventAny::Canceled(event) = event
                && event.client_order_id == id
            {
                assert_eq!(
                    cache.borrow().order(&id).unwrap().status(),
                    OrderStatus::Canceled
                );
                observed.borrow_mut().push(event.event_id);
            }
            if let OrderEventAny::Filled(event) = event
                && event.client_order_id == id
            {
                assert_eq!(event.last_qty, Quantity::from(17));
                assert_eq!(
                    cache.borrow().positions_open(None, None, None, None, None)[0].quantity,
                    Quantity::from("16.66")
                );
                fills.borrow_mut().push(event.trade_id);
            }
        }
    });
    msgbus::subscribe_order_events(topic.into(), handler.clone(), None);
    row["status"] = json!("CANCELLED");
    *private.state.order_snapshot.lock().unwrap() = Some(json!({"success":true,"data":row}));
    client.query_order(query()).unwrap();
    let mut engine = submissions::engine(&cache);
    timeout(Duration::from_secs(2), async {
        while observed.borrow().is_empty() {
            submissions::apply(&mut engine, &next(&mut rx).await);
        }
    }).await.expect("the second Native query must deliver a current terminal fact while earlier history is held");
    assert_eq!(observed.borrow().len(), 1);
    assert_eq!(private.state.order_requests.lock().unwrap().len(), 2);
    assert!(private.state.creations.lock().unwrap().is_empty());
    assert!(private.state.removals.lock().unwrap().is_empty());
    assert!(
        cache
            .borrow()
            .positions_open(None, None, None, None, None)
            .is_empty()
    );
    *private.state.match_gate.lock().unwrap() = None;
    gate.notify_waiters();
    let mut bundles = 0;
    timeout(Duration::from_secs(2), async {
        while bundles < 2 {
            let event = next(&mut rx).await;
            if matches!(
                &event,
                ExecutionEvent::Report(ExecutionReport::OrderWithFills(..))
            ) {
                bundles += 1;
            }
            submissions::apply(&mut engine, &event);
        }
    })
    .await
    .expect("confirmed trades remain applicable after the newer cancellation");
    assert_eq!(fills.borrow().len(), 1);
    assert_eq!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .filled_qty(),
        Quantity::from(17)
    );
    assert_eq!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Canceled
    );
    msgbus::unsubscribe_order_events(topic.into(), &handler);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn rest_terminal_precedes_held_trade_history_and_trade_result_survives_wallet_reconnect() {
    for (status, expected) in [
        ("CANCELLED", OrderStatus::Canceled),
        ("EXPIRED", OrderStatus::Expired),
    ] {
        let private = server(vec![]).await;
        let mut row = rest_order_data(status, 1, OrderSide::Buy);
        row["amountFilled"] = json!("17000000000000000000");
        let own = native_from_row(&mut row);
        let (mut client, mut rx, cache) = reported_client(&private);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        private
            .state
            .orders
            .lock()
            .unwrap()
            .push_back(json!({"success":true,"data":row}));
        let gate = Arc::new(tokio::sync::Notify::new());
        *private.state.match_gate.lock().unwrap() = Some(gate.clone());
        let mut traded = matches::match_row(1);
        traded["executedAt"] = json!("1970-01-01T00:00:00.003Z"); // venue clock ahead of local observation
        traded["taker"]["hash"] = json!(own.venue_order_id().unwrap().to_string());
        private.state.match_pages.lock().unwrap().push_back((
            200,
            json!({"success":true,"cursor":null,"data":[traded]}),
            0,
        ));
        let callbacks = Rc::new(RefCell::new(Vec::new()));
        let handler = TypedHandler::from({
            let callbacks = callbacks.clone();
            let cache = cache.clone();
            let id = own.client_order_id();
            move |event: &OrderEventAny| {
                if event.client_order_id() == id
                    && matches!(
                        event,
                        OrderEventAny::Canceled(_)
                            | OrderEventAny::Expired(_)
                            | OrderEventAny::Filled(_)
                    )
                {
                    let native = cache.borrow();
                    let order = native.order(&id).unwrap();
                    if matches!(event, OrderEventAny::Filled(_)) {
                        assert_eq!(order.filled_qty(), Quantity::from(17));
                    } else {
                        assert_eq!(order.status(), expected);
                        assert!(order.filled_qty().is_zero());
                    }
                    callbacks.borrow_mut().push(event.event_type());
                }
            }
        });
        let topic = switchboard::get_event_order_topic(own.strategy_id());
        msgbus::subscribe_order_events(topic.into(), handler.clone(), None);
        client
            .query_order(nautilus_common::messages::execution::QueryOrder::new(
                trader_id(),
                Some(ClientId::new("PREDICT")),
                own.strategy_id(),
                own.instrument_id(),
                own.client_order_id(),
                own.venue_order_id(),
                UUID4::new(),
                OBSERVED.into(),
                None,
                None,
            ))
            .unwrap();
        let mut engine = submissions::engine(&cache);
        timeout(Duration::from_secs(2), async {
            while callbacks.borrow().is_empty() {
                submissions::apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .expect("terminal callback must precede match history");
        exit_orders::barrier("trade history held", || {
            private.state.match_requests.lock().unwrap().len() == 1
        })
        .await;
        private
            .state
            .sockets
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .send(Message::Close(None))
            .unwrap();
        exit_orders::barrier("replacement wallet", || {
            private.state.sockets.lock().unwrap().len() == 2
        })
        .await;
        wallet_processed(&private).await;
        gate.notify_one();
        timeout(Duration::from_secs(2), async {
            while callbacks.borrow().len() < 2 {
                submissions::apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .expect("authenticated HTTP trades remain valid across WS epochs");
        assert_eq!(
            private.state.order_requests.lock().unwrap().len(),
            1,
            "no repeat GET to demand a static order"
        );
        assert_eq!(callbacks.borrow().len(), 2);
        msgbus::unsubscribe_order_events(topic.into(), &handler);
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn incremental_rest_history_preserves_applied_and_queued_wallet_fills_without_voiding() {
    for (queued, bulk) in [(false, false), (true, false), (false, true), (true, true)] {
        let private = server(vec![]).await;
        let mut row = rest_order_data("OPEN", 1, OrderSide::Buy);
        row["amount"] = json!("100000000000000000000");
        row["amountFilled"] = json!(if queued {
            "51000000000000000000"
        } else {
            "34000000000000000000"
        });
        let own = native_from_row(&mut row);
        let (mut client, mut rx, cache) = reported_client(&private);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        client.disconnect().await.unwrap();
        let mut old = fill(1);
        old["orderHash"] = json!(own.venue_order_id().unwrap().to_string());
        old["settlementId"] = json!("previous-connection-settlement");
        let parsed: PredictWalletEvent = serde_json::from_value(old.clone()).unwrap();
        let report = parse_wallet_report(
            &parsed,
            &own,
            &instrument(1),
            account_id(),
            WALLET.parse().unwrap(),
            OBSERVED.into(),
        )
        .unwrap()
        .unwrap();
        let mut engine = submissions::engine(&cache);
        engine.reconcile_execution_report(&report);
        assert_eq!(
            cache
                .borrow()
                .order(&own.client_order_id())
                .unwrap()
                .filled_qty(),
            Quantity::from(17)
        );
        connect_reported(&mut client, &mut rx).await;
        let amounts = Rc::new(RefCell::new(Vec::new()));
        let handler = TypedHandler::from({
            let amounts = amounts.clone();
            let cache = cache.clone();
            let id = own.client_order_id();
            move |event: &OrderEventAny| {
                if event.client_order_id() != id {
                    return;
                }
                assert!(
                    !matches!(event, OrderEventAny::FillVoided(_)),
                    "incomplete REST history must never revoke real wallet execution"
                );
                if matches!(event, OrderEventAny::Filled(_)) {
                    amounts
                        .borrow_mut()
                        .push(cache.borrow().order(&id).unwrap().filled_qty().as_decimal());
                }
            }
        });
        let topic = switchboard::get_event_order_topic(own.strategy_id());
        msgbus::subscribe_order_events(topic.into(), handler.clone(), None);
        let pending = if queued {
            old["settlementId"] = json!("current-wallet-settlement");
            private.wallet(old);
            Some(loop {
                let event = next(&mut rx).await;
                if matches!(&event, ExecutionEvent::Report(ExecutionReport::Fill(_))) {
                    break event;
                }
                submissions::apply(&mut engine, &event);
            })
        } else {
            None
        };
        let mut fresh = matches::match_row(1);
        fresh["taker"]["hash"] = json!(own.venue_order_id().unwrap().to_string());
        fresh["settlementId"] = json!("new-rest-settlement");
        // The endpoint omits the already-applied historical trade and the queued wallet trade.
        *private.state.match_snapshot.lock().unwrap() = Some(vec![fresh]);
        *private.state.order_snapshot.lock().unwrap() = Some(json!({"success":true,"data":row}));
        let query = || {
            nautilus_common::messages::execution::QueryOrder::new(
                trader_id(),
                Some(ClientId::new("PREDICT")),
                own.strategy_id(),
                own.instrument_id(),
                own.client_order_id(),
                own.venue_order_id(),
                UUID4::new(),
                OBSERVED.into(),
                None,
                None,
            )
        };
        let bulk_command = nautilus_common::messages::execution::GenerateOrderStatusReports::new(
            UUID4::new(),
            OBSERVED.into(),
            false,
            None,
            None,
            None,
            None,
            None,
        );
        if bulk {
            let error = client
                .generate_order_status_reports(&bulk_command)
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("defer status until applied"),
                "{error:#}"
            );
        } else {
            client.query_order(query()).unwrap();
        }
        let bundle = timeout(Duration::from_secs(2), async {
            loop {
                let event = next(&mut rx).await;
                if matches!(
                    &event,
                    ExecutionEvent::Report(ExecutionReport::OrderWithFills(_, _))
                ) {
                    break event;
                }
                submissions::apply(&mut engine, &event);
            }
        })
        .await
        .expect("an unseen real trade must not wait for an applied history prefix");
        if let Some(pending) = pending {
            submissions::apply(&mut engine, &pending);
        }
        submissions::apply(&mut engine, &bundle);
        let expected = if queued { 51 } else { 34 };
        assert_eq!(
            cache
                .borrow()
                .order(&own.client_order_id())
                .unwrap()
                .filled_qty(),
            Quantity::from(expected)
        );
        assert_eq!(
            cache.borrow().positions_open(None, None, None, None, None)[0].quantity,
            Quantity::from(expected)
        );
        assert_eq!(amounts.borrow().len(), if queued { 2 } else { 1 });
        if bulk {
            let reports = client
                .generate_order_status_reports(&bulk_command)
                .await
                .unwrap();
            assert_eq!(reports.len(), 1);
            engine
                .reconcile_execution_report(&ExecutionReport::Order(Box::new(reports[0].clone())));
        } else {
            client.query_order(query()).unwrap();
            timeout(Duration::from_secs(2), async {
                loop {
                    let event = next(&mut rx).await;
                    let repeated = matches!(
                        &event,
                        ExecutionEvent::Report(ExecutionReport::OrderWithFills(_, _))
                    );
                    submissions::apply(&mut engine, &event);
                    if repeated {
                        break;
                    }
                }
            })
            .await
            .unwrap();
        }
        assert_eq!(
            cache
                .borrow()
                .order(&own.client_order_id())
                .unwrap()
                .filled_qty(),
            Quantity::from(expected)
        );
        assert_eq!(amounts.borrow().len(), if queued { 2 } else { 1 });
        assert_eq!(private.state.order_requests.lock().unwrap().len(), 2);
        msgbus::unsubscribe_order_events(topic.into(), &handler);
        client.disconnect().await.unwrap();
    }
}
