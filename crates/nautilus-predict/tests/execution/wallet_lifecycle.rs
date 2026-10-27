// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Known-order lifecycle uses native events and confirmed fills use reports, as in Polymarket dispatch.
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
        (
            "orderNotAccepted",
            Some("noMarketMatch"),
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
                let post_only = reason == Some("rejectedPostOnly");
                assert_eq!(
                    event.reason.as_str(),
                    if post_only {
                        "Predict post-only order rejected"
                    } else {
                        "Predict no market match"
                    }
                );
                assert_eq!(event.due_post_only, post_only);
            }
            OrderEventAny::Expired(_) => {}
            _ => panic!("wrong native lifecycle event"),
        }
        let mut engine = super::cancellations::engine(&cache);
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
    let mut engine = super::cancellations::engine(&cache);
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
    let mut engine = super::cancellations::engine(&cache);
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
            ExecutionEvent::Report(ExecutionReport::Fill(report)) => break report,
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
    engine.reconcile_execution_report(&ExecutionReport::Fill(fill));
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
        let mut engine = super::cancellations::engine(&cache);
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
    snapshot_before_wallet_fill(false).await;
}

#[tokio::test]
async fn share_fee_snapshot_coverage_preserves_inventory_but_new_settlement_still_applies() {
    snapshot_before_wallet_fill(true).await;
}

async fn snapshot_before_wallet_fill(charged: bool) {
    use nautilus_live::execution::{config::ExecutionManagerConfig, manager::ExecutionManager};
    use nautilus_model::{enums::PositionSide, reports::PositionStatusReport};
    let expected_net = Quantity::from(if charged { "16.66" } else { "17" });
    let server = server(vec![]).await;
    server
        .state
        .create_replies
        .lock()
        .unwrap()
        .push_back((201, 0, vec!["orderAccepted".into()]));
    let (mut client, mut rx, cache, _portfolio, mut own) = submissions::connected(&server, 1).await;
    if charged {
        own = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(trader_id())
            .client_order_id("SNAPSHOT-TAKER".into())
            .instrument_id(instrument_id(1))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100))
            .price(Price::from("0.620"))
            .post_only(false)
            .build();
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
    }
    let mut engine = super::cancellations::engine(&cache);
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
        expected_net,
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
        expected_net
    );
    assert_eq!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .filled_qty(),
        Quantity::from(0)
    );
    let before =
        serde_json::to_string(&*cache.borrow().positions(None, None, None, None, None)[0]).unwrap();
    let position_events = Rc::new(RefCell::new(0usize));
    let handler = nautilus_common::msgbus::TypedHandler::from({
        let count = position_events.clone();
        move |_: &nautilus_model::events::PositionEvent| {
            *count.borrow_mut() += 1;
        }
    });
    msgbus::subscribe_position_events("events.position.*".into(), handler.clone(), None);
    let mut settled = fill(1);
    settled["orderHash"] =
        server.state.creations.lock().unwrap()[0]["data"]["order"]["hash"].clone();
    settled["timestamp"] = json!(3);
    settled["isMaker"] = json!(!charged);
    settled["fee"] = json!({"type": if charged { "SHARES" } else { "COLLATERAL" }, "amountWei": if charged { "340000000000000000" } else { "0" }});
    server.wallet(settled.clone());
    let fill = loop {
        match next(&mut rx).await {
            ExecutionEvent::Report(ExecutionReport::Fill(report)) => break report,
            ExecutionEvent::Account(state) => {
                msgbus::send_account_state(MessagingSwitchboard::portfolio_update_account(), &state)
            }
            event => panic!("own live fill uses Native report ingestion: {event:?}"),
        }
    };
    let stale = ExecutionReport::Fill(fill);
    engine.reconcile_execution_report(&stale);
    engine.reconcile_execution_report(&stale);
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
        expected_net,
        "snapshot-covered inventory must not be applied twice"
    );
    assert_eq!(
        *position_events.borrow(),
        0,
        "covered fills must not publish another position increase"
    );
    assert_eq!(
        serde_json::to_string(&*cache.borrow().positions(None, None, None, None, None)[0]).unwrap(),
        before
    );
    // A different execution after the snapshot cutoff must still grow inventory once.
    settled["settlementId"] = json!("post-snapshot-settlement");
    settled["timestamp"] = json!(13);
    settled["details"]["quantityFilled"] = json!("34");
    server.wallet(settled);
    let later = loop {
        match next(&mut rx).await {
            ExecutionEvent::Report(ExecutionReport::Fill(report)) => break report,
            ExecutionEvent::Account(state) => {
                msgbus::send_account_state(MessagingSwitchboard::portfolio_update_account(), &state)
            }
            event => panic!("expected later execution: {event:?}"),
        }
    };
    assert_eq!(later.ts_event, UnixNanos::from(13_000_000u64));
    let later = ExecutionReport::Fill(later);
    engine.reconcile_execution_report(&later);
    engine.reconcile_execution_report(&later);
    assert_eq!(
        cache.borrow().positions(None, None, None, None, None)[0]
            .quantity
            .as_decimal(),
        expected_net.as_decimal() * Decimal::from(2)
    );
    assert_eq!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .filled_qty(),
        Quantity::from(34)
    );
    assert_eq!(*position_events.borrow(), 1);
    msgbus::unsubscribe_position_events("events.position.*".into(), &handler);
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
            wallet_processed(&server).await;
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
            let mut engine = super::cancellations::engine(&cache);
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

#[tokio::test]
async fn wallet_terminal_callback_precedes_missing_fill_without_http_or_fabricated_inventory() {
    for (kind, expected) in [
        ("orderCancelled", OrderStatus::Canceled),
        ("orderExpired", OrderStatus::Expired),
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
        let callbacks = Rc::new(RefCell::new(Vec::new()));
        let handler = TypedHandler::from({
            let cache = cache.clone();
            let callbacks = callbacks.clone();
            move |event: &OrderEventAny| {
                if matches!(
                    event,
                    OrderEventAny::Canceled(_) | OrderEventAny::Expired(_)
                ) {
                    let cache = cache.borrow();
                    let order = cache.order(&event.client_order_id()).unwrap();
                    assert_eq!(order.status(), expected);
                    assert!(
                        order.filled_qty().is_zero(),
                        "terminal callback cannot invent the missing fill"
                    );
                    assert!(cache.positions(None, None, None, None, None).is_empty());
                    callbacks.borrow_mut().push(event.event_type());
                }
            }
        });
        let topic = switchboard::get_event_order_topic(own.strategy_id());
        msgbus::subscribe_order_events(topic.into(), handler.clone(), None);
        let mut terminal = event(kind, 1);
        terminal["details"]["quantityFilled"] = json!("17");
        server.wallet(terminal);
        let ExecutionEvent::Order(terminal) = next(&mut rx).await else {
            panic!("confirmed terminal fact must arrive before any fill or query");
        };
        let mut engine = super::cancellations::engine(&cache);
        engine.process(&terminal);
        assert_eq!(callbacks.borrow().len(), 1);
        assert!(server.state.order_requests.lock().unwrap().is_empty());
        assert!(server.state.match_requests.lock().unwrap().is_empty());
        assert!(server.state.requests.lock().unwrap().is_empty());
        server.wallet(fill(1));
        let ExecutionEvent::Report(report @ ExecutionReport::Fill(_)) = next(&mut rx).await else {
            panic!("late confirmed fill remains independently deliverable");
        };
        engine.reconcile_execution_report(&report);
        engine.reconcile_execution_report(&report);
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
                .positions_open(None, Some(&instrument_id(1)), None, None, None)[0]
                .quantity,
            Quantity::from(17)
        );
        assert_eq!(callbacks.borrow().len(), 1);
        msgbus::unsubscribe_order_events(topic.into(), &handler);
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn wallet_acceptance_callback_does_not_wait_for_missing_settlement() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache, _portfolio, own) = submissions::connected(&server, 1).await;
    let mut engine = super::cancellations::engine(&cache);
    client.submit_order(submissions::submit(&own)).unwrap();
    exit_orders::barrier("signed POST registered", || {
        !server.state.creations.lock().unwrap().is_empty()
    })
    .await;
    let hash = server.state.creations.lock().unwrap()[0]["data"]["order"]["hash"].clone();
    let mut accepted = event("orderAccepted", 1);
    accepted["orderHash"] = hash.clone();
    accepted["timestamp"] = json!(2);
    accepted["details"]["quantityFilled"] = json!("17");
    server.wallet(accepted);
    loop {
        let event = timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("acceptance before missing fill")
            .unwrap();
        let accepted = matches!(&event, ExecutionEvent::Order(OrderEventAny::Accepted(_)));
        assert!(
            !matches!(&event, ExecutionEvent::Report(_)),
            "acceptance does not invent a fill or require recovery"
        );
        submissions::apply(&mut engine, &event);
        if accepted {
            break;
        }
    }
    assert_eq!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Accepted
    );
    assert!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .filled_qty()
            .is_zero()
    );
    assert!(
        cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
    );
    assert!(server.state.order_requests.lock().unwrap().is_empty());
    assert!(server.state.match_requests.lock().unwrap().is_empty());
    let mut settled = fill(1);
    settled["orderHash"] = hash;
    settled["timestamp"] = json!(2);
    settled["isMaker"] = json!(true);
    settled["fee"]["amountWei"] = json!("0");
    server.wallet(settled);
    loop {
        let event = timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("independent late fill after acceptance")
            .unwrap();
        let filled = matches!(&event, ExecutionEvent::Report(ExecutionReport::Fill(_)));
        submissions::apply(&mut engine, &event);
        if filled {
            break;
        }
    }
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
            .positions_open(None, Some(&instrument_id(1)), None, None, None)[0]
            .quantity,
        Quantity::from(17)
    );
    client.disconnect().await.unwrap();
}
