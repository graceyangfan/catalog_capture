// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Known-order wallet feedback uses the native event port, as in Polymarket dispatch.
use nautilus_common::msgbus::{TypedHandler, switchboard};
use nautilus_model::events::OrderEventAny;

use super::*;

#[tokio::test]
async fn wallet_terminal_events_preserve_reasons_and_publish_after_native_cache_update() {
    for (kind, reason, expected) in [
        (
            "orderCancelled",
            Some("selfTradePrevention"),
            OrderStatus::Canceled,
        ),
        ("orderExpired", None, OrderStatus::Expired),
        (
            "orderNotAccepted",
            Some("rejectedPostOnly"),
            OrderStatus::Rejected,
        ),
    ] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = client(&server);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        let own = order(1);
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        seed_account(&cache);
        client.connect().await.unwrap();
        let events = Rc::new(RefCell::new(Vec::new()));
        let handler = TypedHandler::from({
            let cache = cache.clone();
            let events = events.clone();
            move |event: &OrderEventAny| {
                assert!(match event {
                    OrderEventAny::Canceled(event) => !event.reconciliation,
                    OrderEventAny::Expired(event) => !event.reconciliation,
                    OrderEventAny::Rejected(event) => !event.reconciliation,
                    _ => false,
                });
                assert_eq!(
                    cache
                        .borrow()
                        .order(&event.client_order_id())
                        .unwrap()
                        .status(),
                    expected
                );
                events.borrow_mut().push(event.clone());
            }
        });
        let topic = switchboard::get_event_order_topic(own.strategy_id());
        msgbus::subscribe_order_events(topic.into(), handler.clone(), None);
        let mut wire = event(kind, 1);
        if let Some(reason) = reason {
            wire["reason"] = json!(reason);
        }
        // Both copies arrive before the Native cache observer is allowed to run.
        server.wallet(wire.clone());
        server.wallet(wire);
        let ExecutionEvent::Order(first) = next(&mut rx).await else {
            panic!("native wallet event")
        };
        let ExecutionEvent::Order(second) = next(&mut rx).await else {
            panic!("native duplicate")
        };
        match &first {
            OrderEventAny::Canceled(event) => assert_eq!(
                event.reason.unwrap().as_str(),
                "Predict self-trade prevention"
            ),
            OrderEventAny::Rejected(event) => {
                assert_eq!(event.reason.as_str(), "Predict post-only order rejected");
                assert!(event.due_post_only);
            }
            OrderEventAny::Expired(_) => {}
            _ => panic!("wrong native lifecycle event"),
        }
        let mut engine = cancellations::engine(&cache);
        engine.process(&first);
        engine.process(&second);
        assert_eq!(
            events.borrow().len(),
            1,
            "Native owns terminal duplicate suppression"
        );
        assert_eq!(
            cache
                .borrow()
                .order(&own.client_order_id())
                .unwrap()
                .filled_qty(),
            own.filled_qty()
        );
        assert!(
            cache
                .borrow()
                .positions(None, None, None, None, None)
                .is_empty()
        );
        assert!(server.state.order_requests.lock().unwrap().is_empty());
        assert!(server.state.creations.lock().unwrap().is_empty());
        msgbus::unsubscribe_order_events(topic.into(), &handler);
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn wallet_acceptance_burst_is_emitted_once_before_native_observer_runs() {
    let server = server(vec![]).await;
    server.state.create_replies.lock().unwrap().push_back((
        201,
        0,
        vec!["orderAccepted".into(), "orderAccepted".into()],
    ));
    let (mut client, mut rx, cache, _portfolio, own) = submissions::connected(&server, 1).await;
    let mut engine = cancellations::engine(&cache);
    client.submit_order(submissions::submit(&own)).unwrap();
    let first = loop {
        let incoming = next(&mut rx).await;
        if let ExecutionEvent::Order(OrderEventAny::Accepted(event)) = incoming {
            break event;
        }
        assert!(
            !matches!(incoming, ExecutionEvent::Report(_)),
            "known WS acceptance must bypass reconciliation"
        );
        submissions::apply(&mut engine, &incoming);
    };
    // A different lifecycle event is the causal barrier that the duplicate was consumed.
    let mut cancel = event("orderCancelled", 1);
    cancel["orderHash"] = json!(first.venue_order_id.as_str());
    cancel["timestamp"] = json!(2);
    server.wallet(cancel);
    let terminal = loop {
        match next(&mut rx).await {
            ExecutionEvent::Order(event @ OrderEventAny::Canceled(_)) => break event,
            ExecutionEvent::Account(state) => {
                msgbus::send_account_state(MessagingSwitchboard::portfolio_update_account(), &state)
            }
            other => panic!("duplicate acceptance or report before cancellation: {other:?}"),
        }
    };
    assert_eq!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Submitted
    );
    engine.process(&OrderEventAny::Accepted(first));
    engine.process(&terminal);
    assert_eq!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Canceled
    );
    assert!(server.state.order_requests.lock().unwrap().is_empty());
    assert_eq!(server.state.creations.lock().unwrap().len(), 1);
    client.disconnect().await.unwrap();
}

/// A separate zero-price order is a causal WS barrier when replayed facts are
/// intentionally suppressed. It never submits a command, trades or locks cash.
pub(super) fn control(engine: &mut ExecutionEngine, reference: &Value) -> (VenueOrderId, Value) {
    let nonce = UUID4::new().to_string();
    let hash = VenueOrderId::new(format!(
        "{:#x}",
        alloy::primitives::keccak256(nonce.as_bytes())
    ));
    let id = format!(
        "{}-{}.PREDICT",
        reference["details"]["marketId"].as_u64().unwrap(),
        reference["details"]["outcomeIndex"].as_u64().unwrap()
    )
    .parse()
    .unwrap();
    let account = {
        let cache = engine.cache().borrow();
        let client = cache
            .client_order_id(&VenueOrderId::new(reference["orderHash"].as_str().unwrap()))
            .unwrap();
        cache.order(client).unwrap().account_id().unwrap()
    };
    let factory = OrderEventFactory::new(trader_id(), account, AccountType::Cash, None);
    let mut own = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(trader_id())
        .strategy_id("WALLET-BARRIER-001".into())
        .client_order_id(nonce.into())
        .instrument_id(id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from(1))
        .price(Price::from("0.000"))
        .build();
    own.apply(factory.generate_order_submitted(&own, 10.into()))
        .unwrap();
    engine
        .cache()
        .borrow_mut()
        .add_order(own.clone(), None, Some(ClientId::new("PREDICT")), false)
        .unwrap();
    engine.process(&factory.generate_order_accepted(&own, hash, 100.into(), 100.into()));
    let mut wire = reference.clone();
    wire["type"] = json!("orderCancelled");
    wire["orderId"] = json!("987654321");
    wire["orderHash"] = json!(hash.as_str());
    wire["details"]["quoteType"] = json!("BID");
    wire["details"]["strategyType"] = json!("LIMIT");
    wire["details"]["quantity"] = json!("1");
    wire["details"]["quantityFilled"] = json!("0");
    wire["details"]["price"] = json!("0.000");
    wire["details"]["value"] = json!("0");
    wire["details"]["valueFilled"] = json!("0");
    (hash, wire)
}

#[tokio::test]
async fn wallet_ack_after_queued_fill_cannot_revert_native_partial_fill_status() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache, _portfolio, own) = submissions::connected(&server, 1).await;
    let mut engine = cancellations::engine(&cache);
    client.submit_order(submissions::submit(&own)).unwrap();
    loop {
        let incoming = next(&mut rx).await;
        let submitted = matches!(incoming, ExecutionEvent::Order(OrderEventAny::Submitted(_)));
        submissions::apply(&mut engine, &incoming);
        if submitted {
            break;
        }
    }
    exit_orders::barrier("create request registered", || {
        !server.state.creations.lock().unwrap().is_empty()
    })
    .await;
    let hash = server.state.creations.lock().unwrap()[0]["data"]["order"]["hash"].clone();
    let mut settled = fill(1);
    settled["orderHash"] = hash.clone();
    settled["timestamp"] = json!(2);
    settled["isMaker"] = json!(true);
    settled["fee"]["amountWei"] = json!("0");
    server.wallet(settled.clone());
    server.wallet(settled);
    for kind in ["orderAccepted", "orderCancelled"] {
        let mut wire = event(kind, 1);
        wire["orderHash"] = hash.clone();
        wire["timestamp"] = json!(2);
        wire["details"]["quantityFilled"] = json!(if kind == "orderAccepted" { "0" } else { "17" });
        server.wallet(wire);
    }
    let mut accepted = None;
    let fill = loop {
        match next(&mut rx).await {
            ExecutionEvent::Order(event @ OrderEventAny::Accepted(_)) => {
                assert!(accepted.replace(event).is_none());
            }
            ExecutionEvent::Order(event @ OrderEventAny::Filled(_)) => break event,
            ExecutionEvent::Account(state) => {
                msgbus::send_account_state(MessagingSwitchboard::portfolio_update_account(), &state)
            }
            other => panic!("expected confirmed fill: {other:?}"),
        }
    };
    // Do not apply the fill until the following lifecycle burst has been consumed.
    let terminal = loop {
        match next(&mut rx).await {
            ExecutionEvent::Order(event @ OrderEventAny::Canceled(_)) => break event,
            ExecutionEvent::Account(state) => {
                msgbus::send_account_state(MessagingSwitchboard::portfolio_update_account(), &state)
            }
            other => panic!("ACK must not queue acceptance behind a pending fill: {other:?}"),
        }
    };
    engine.process(&accepted.expect("first fill proves acceptance before Filled"));
    engine.process(&fill);
    assert_eq!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::PartiallyFilled
    );
    engine.process(&terminal);
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
    assert_eq!(
        cache.borrow().positions(None, None, None, None, None)[0].quantity,
        Quantity::from(17)
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn owned_order_after_wallet_gap_or_transport_restart_keeps_recovery_reports() {
    for restart in [false, true] {
        let server = server(vec![]).await;
        server.state.create_replies.lock().unwrap().push_back((
            201,
            0,
            vec!["orderAccepted".into()],
        ));
        let (mut client, mut rx, cache, _portfolio, own) = submissions::connected(&server, 1).await;
        let mut engine = cancellations::engine(&cache);
        client.submit_order(submissions::submit(&own)).unwrap();
        loop {
            let event = next(&mut rx).await;
            let accepted = matches!(event, ExecutionEvent::Order(OrderEventAny::Accepted(_)));
            submissions::apply(&mut engine, &event);
            if accepted {
                break;
            }
        }
        let body = server.state.creations.lock().unwrap()[0].clone();
        if restart {
            server
                .state
                .open_orders
                .lock()
                .unwrap()
                .push(live_node::rest_row(&body, "OPEN"));
            client.disconnect().await.unwrap();
            connect_reported(&mut client, &mut rx).await;
        } else {
            server.state.sockets.lock().unwrap()[0]
                .send(Message::Close(None))
                .unwrap();
            exit_orders::barrier("new wallet subscription epoch", || {
                server.state.sockets.lock().unwrap().len() >= 2 && !client.is_connected()
            })
            .await;
        }
        let mut settled = fill(1);
        settled["orderHash"] = body["data"]["order"]["hash"].clone();
        settled["timestamp"] = json!(3);
        settled["isMaker"] = json!(true);
        settled["fee"]["amountWei"] = json!("0");
        server.wallet(settled);
        let report = loop {
            match next(&mut rx).await {
                ExecutionEvent::Report(ExecutionReport::Fill(report)) => break report,
                ExecutionEvent::Account(state) => msgbus::send_account_state(
                    MessagingSwitchboard::portfolio_update_account(),
                    &state,
                ),
                event => {
                    panic!("recovery must retain Native snapshot replay protection: {event:?}")
                }
            }
        };
        assert_eq!(report.client_order_id, Some(own.client_order_id()));
        engine.reconcile_execution_report(&ExecutionReport::Fill(report));
        assert_eq!(
            cache
                .borrow()
                .order(&own.client_order_id())
                .unwrap()
                .filled_qty(),
            Quantity::from(17)
        );
        assert_eq!(
            cache.borrow().positions(None, None, None, None, None)[0].quantity,
            Quantity::from(17)
        );
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn native_position_snapshot_before_owned_wallet_fill_does_not_double_inventory() {
    use nautilus_live::execution::{config::ExecutionManagerConfig, manager::ExecutionManager};
    use nautilus_model::{enums::PositionSide, reports::PositionStatusReport};
    let server = server(vec![]).await;
    server
        .state
        .create_replies
        .lock()
        .unwrap()
        .push_back((201, 0, vec!["orderAccepted".into()]));
    let (mut client, mut rx, cache, _portfolio, own) = submissions::connected(&server, 1).await;
    let mut engine = cancellations::engine(&cache);
    client.submit_order(submissions::submit(&own)).unwrap();
    loop {
        let event = next(&mut rx).await;
        let accepted = matches!(event, ExecutionEvent::Order(OrderEventAny::Accepted(_)));
        submissions::apply(&mut engine, &event);
        if accepted {
            break;
        }
    }
    cache
        .borrow_mut()
        .register_external_order_claims(own.strategy_id(), &[own.instrument_id()])
        .unwrap();
    let mut virtual_clock = VirtualClock::new();
    virtual_clock.advance_time(UnixNanos::from(OBSERVED + 10_000_000), true);
    let clock = Rc::new(RefCell::new(virtual_clock));
    let mut manager = ExecutionManager::new(
        clock,
        cache.clone(),
        ExecutionManagerConfig {
            position_check_threshold_ns: nautilus_core::DurationNanos::default(),
            ..Default::default()
        },
    )
    .unwrap();
    let check = manager.prepare_position_report_check(UUID4::new(), &[&client]);
    let report = PositionStatusReport::new(
        account_id(),
        own.instrument_id(),
        PositionSide::Long,
        Quantity::from(17),
        (OBSERVED + 10_000_000).into(),
        (OBSERVED + 10_000_000).into(),
        None,
        None,
        Some(Decimal::new(13, 2)),
    );
    let events = manager.reconcile_position_reports(
        &check,
        vec![report],
        &std::iter::once(client.client_id()).collect(),
        &Default::default(),
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, OrderEventAny::Filled(_))),
        "nonempty Native synthetic snapshot adjustment"
    );
    for event in events {
        engine.process(&event);
    }
    assert_eq!(
        cache.borrow().positions(None, None, None, None, None)[0].quantity,
        Quantity::from(17)
    );
    assert_eq!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .filled_qty(),
        Quantity::from(0)
    );
    let mut settled = fill(1);
    settled["orderHash"] =
        server.state.creations.lock().unwrap()[0]["data"]["order"]["hash"].clone();
    settled["timestamp"] = json!(3);
    settled["isMaker"] = json!(true);
    settled["fee"]["amountWei"] = json!("0");
    server.wallet(settled);
    let fill = loop {
        match next(&mut rx).await {
            ExecutionEvent::Order(event @ OrderEventAny::Filled(_)) => break event,
            ExecutionEvent::Account(state) => {
                msgbus::send_account_state(MessagingSwitchboard::portfolio_update_account(), &state)
            }
            event => panic!("own live fill stays a direct Native event: {event:?}"),
        }
    };
    engine.process(&fill);
    assert_eq!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .filled_qty(),
        Quantity::from(17)
    );
    assert_eq!(
        cache.borrow().positions(None, None, None, None, None)[0].quantity,
        Quantity::from(17),
        "not 34 shares"
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn wallet_message_market_and_outcome_must_match_native_order() {
    for outcome in [1, 2] {
        for foreign_market in [false, true] {
            let server = server(vec![]).await;
            let (mut client, mut rx, cache) = client(&server);
            for index in [1, 2] {
                cache
                    .borrow_mut()
                    .add_instrument(InstrumentAny::BinaryOption(instrument(index)))
                    .unwrap();
            }
            let mut foreign = instrument(outcome);
            foreign.id = format!("43-{outcome}.PREDICT").parse().unwrap();
            cache
                .borrow_mut()
                .add_instrument(InstrumentAny::BinaryOption(foreign))
                .unwrap();
            let own = order(outcome);
            cache
                .borrow_mut()
                .add_order(own.clone(), None, None, false)
                .unwrap();
            seed_account(&cache);
            client.connect().await.unwrap();
            let mut wrong = event("orderCancelled", outcome);
            if foreign_market {
                wrong["details"]["marketId"] = json!(43);
            } else {
                let other = 3 - outcome;
                wrong["details"]["outcome"] = json!(if other == 1 { "YES" } else { "NO" });
                wrong["details"]["outcomeIndex"] = json!(other);
            }
            server.wallet(wrong);
            exit_orders::barrier("mismatched wallet identity requires reconciliation", || {
                requires_reconciliation(&client)
            })
            .await;
            assert!(rx.try_recv().is_err());
            assert_eq!(
                cache
                    .borrow()
                    .order(&own.client_order_id())
                    .unwrap()
                    .status(),
                own.status()
            );
            // The same consumer must still deliver a matching terminal fact.
            server.wallet(event("orderCancelled", outcome));
            let ExecutionEvent::Order(terminal @ OrderEventAny::Canceled(_)) = next(&mut rx).await
            else {
                panic!("matching cancellation must remain deliverable")
            };
            let mut engine = cancellations::engine(&cache);
            engine.process(&terminal);
            assert_eq!(
                cache
                    .borrow()
                    .order(&own.client_order_id())
                    .unwrap()
                    .status(),
                OrderStatus::Canceled
            );
            client.disconnect().await.unwrap();
        }
    }
}
