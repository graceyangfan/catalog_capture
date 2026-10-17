// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Reapply actual Native input reports to the unmodified engine, offline.
use nautilus_model::{events::OrderEventAny, reports::OrderStatusReport};

use super::*;

fn replay() -> Value {
    let source: Vec<Value> = serde_json::from_str(include_str!("../fixtures/private_api/live_resting_batch-restart_20261016T153745Z/native_input_and_callbacks.json")).unwrap();
    replay_source(
        source,
        "live_resting_batch-restart_20261016T153745Z/native_input_and_callbacks.json",
    )
}
fn replay_source(source: Vec<Value>, primary: &str) -> Value {
    let mut sessions = vec![];
    for rest_first in [true, false] {
        let cache = Rc::new(RefCell::new(Cache::default()));
        msgbus::set_message_bus(Rc::new(RefCell::new(MessageBus::new(
            trader_id(),
            UUID4::new(),
            None,
            None,
        ))));
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        let mut engine = cancellations::engine(&cache);
        let mut orders = vec![];
        for initial in source.iter().filter(|r| {
            r["phase"] == "recover"
                && r["type"] == "RawOrderStatusReport"
                && r["report"]["order_status"] == "ACCEPTED"
        }) {
            let mut accepted: OrderStatusReport =
                serde_json::from_value(initial["report"].clone()).unwrap();
            // Only local route/account identity changes; actual hash, terms, timestamps survive.
            accepted.account_id = account_id();
            accepted.instrument_id = instrument_id(1);
            engine.reconcile_execution_report(&nautilus_common::messages::ExecutionReport::Order(
                Box::new(accepted.clone()),
            ));
            let id = cache
                .borrow()
                .client_order_id(&accepted.venue_order_id)
                .copied()
                .unwrap();
            let actual = source
                .iter()
                .find(|r| {
                    r["phase"] == "recover"
                        && r["type"] == "RawOrderStatusReport"
                        && r["report"]["order_status"] == "CANCELED"
                        && r["report"]["venue_order_id"] == accepted.venue_order_id.to_string()
                })
                .unwrap();
            let mut canceled: OrderStatusReport =
                serde_json::from_value(actual["report"].clone()).unwrap();
            canceled.account_id = account_id();
            canceled.instrument_id = instrument_id(1);
            canceled.client_order_id = Some(id);
            if rest_first {
                let current = cache.borrow().order(&id).unwrap().clone();
                let actual_rest = source
                    .iter()
                    .find(|r| {
                        r["phase"] == "recover"
                            && r["type"] == "NativeOrderEvent"
                            && r["kind"] == "Canceled"
                            && r["venueOrderId"] == accepted.venue_order_id.to_string()
                    })
                    .unwrap();
                let timestamp = UnixNanos::from(actual_rest["tsEvent"].as_u64().unwrap());
                let factory =
                    OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
                engine.process(&factory.generate_order_canceled(
                    &current,
                    Some(accepted.venue_order_id),
                    timestamp,
                    timestamp,
                ));
            }
            for _ in 0..3 {
                engine.reconcile_execution_report(
                    &nautilus_common::messages::ExecutionReport::Order(Box::new(canceled.clone())),
                );
            }
            // When WS wins, the adapter's existing closed-order check suppresses the late REST ack.
            let cache = cache.borrow();
            let order = cache.order(&id).unwrap();
            assert_eq!(order.status(), OrderStatus::Canceled);
            assert!(order.filled_qty().is_zero());
            let cancels = order
                .events()
                .iter()
                .filter(|e| matches!(e, OrderEventAny::Canceled(_)))
                .count();
            assert_eq!(cancels, 1);
            orders.push(json!({"hash":accepted.venue_order_id.to_string(),"canceledEvents":cancels,"replayedWalletReports":3,"quantity":order.quantity().to_string(),"filledQty":order.filled_qty().to_string()}));
        }
        assert_eq!(orders.len(), 2);
        assert!(
            cache
                .borrow()
                .positions_open(None, None, None, None, None)
                .is_empty()
        );
        sessions.push(json!({"restFirst":rest_first,"orders":orders,"positions":0}));
    }
    json!({"source":"derived offline unmodified Native engine replay of actual Native reports; duplicate messages and reverse arrival order injected","primarySource":primary,"transforms":["local account and instrument routing rebound; economic terms/hash/timestamps preserved","both arrival orders injected using recorded Native cancellation event time; not evidence of actual REST timing; late REST skipped by closed-order guard"],"sessions":sessions,"networkCalls":0})
}
#[test]
fn actual_rest_ws_cancellations_replay_once_in_both_arrival_orders() {
    replay();
}
#[test]
#[ignore = "explicit offline capture; create_new only"]
fn record_actual_batch_replay() {
    let out = replay();
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/batch_native_replay_20261016.json");
    serde_json::to_writer_pretty(
        std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .unwrap(),
        &out,
    )
    .unwrap();
}

fn replay_http_batch() -> Value {
    let source=serde_json::from_str(include_str!("../fixtures/private_api/live_resting_http-batch_20261016T160926Z/native_input_and_callbacks.json")).unwrap();
    replay_source(
        source,
        "live_resting_http-batch_20261016T160926Z/native_input_and_callbacks.json",
    )
}
#[test]
fn actual_multi_id_batch_reports_replay_once_in_both_arrival_orders() {
    replay_http_batch();
}
#[test]
#[ignore = "explicit offline capture of actual HTTP batch reports; create_new only"]
fn record_http_batch_actual_replay() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/http_batch_native_actual_replay_20261017.json");
    serde_json::to_writer_pretty(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap(),
        &replay_http_batch(),
    )
    .unwrap();
}
