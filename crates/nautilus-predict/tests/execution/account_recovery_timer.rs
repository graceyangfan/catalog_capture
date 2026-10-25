// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/LICENSE-3.0.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Account-only timer contract; LiveNode retains ownership of order reconciliation.
use nautilus_common::{messages::execution::TradingCommand, msgbus::TypedIntoHandler};

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn account_recovery_timer_does_not_duplicate_order_queries_and_cancels_on_disconnect() {
    let private = server(vec![]).await;
    let (_, mut rx, cache) = client(&private);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    cache
        .borrow_mut()
        .add_order(order(1), None, None, false)
        .unwrap();
    let _portfolio = portfolio(&cache);
    let core = ExecutionClientCore::new(
        trader_id(),
        ClientId::new("PREDICT"),
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
    cfg.reconnect_reconciliation_interval_secs = Some(1);
    let virtual_clock = Rc::new(RefCell::new(VirtualClock::new()));
    let clock = Box::leak(Box::new(AtomicTime::new(false, UnixNanos::from(OBSERVED))));
    let mut client = PredictExecutionClient::new_with_clock(core, cfg, clock)
        .unwrap()
        .with_reconciliation_clock(virtual_clock.clone());
    let commands = Rc::new(RefCell::new(Vec::new()));
    let captured = commands.clone();
    msgbus::register_trading_command_endpoint(
        MessagingSwitchboard::exec_engine_queue_execute(),
        TypedIntoHandler::from(move |command: TradingCommand| captured.borrow_mut().push(command)),
    );
    connect_reported(&mut client, &mut rx).await;
    let fire = |to: u64| {
        let events = virtual_clock
            .borrow_mut()
            .advance_time(UnixNanos::from(to), true);
        let handlers = virtual_clock.borrow().match_handlers(events);
        for handler in handlers {
            handler.run();
        }
    };
    fire(1_000_000_000);
    assert!(
        commands.borrow().is_empty(),
        "healthy sessions need no recovery commands"
    );
    private
        .state
        .sockets
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .send(Message::Close(None))
        .unwrap();
    let mut time = 1_000_000_000u64;
    timeout(Duration::from_secs(3), async {
        loop {
            tokio::task::yield_now().await;
            if private.state.sockets.lock().unwrap().len() < 2 {
                continue;
            }
            time += 1_000_000_000;
            fire(time);
            if !commands.borrow().is_empty() {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(
        client.is_connected(),
        "authenticated transport does not await account refresh"
    );
    assert_eq!(
        cache
            .borrow()
            .orders_refs(Some(&PREDICT_VENUE), None, None, Some(&account_id()), None)
            .into_iter()
            .filter(|o| !o.is_closed())
            .count(),
        1
    );
    assert_eq!(commands.borrow().len(), 1);
    assert!(
        matches!(commands.borrow()[0], TradingCommand::QueryAccount(_)),
        "an active order must not cause the account timer to queue QueryOrder"
    );
    let events = virtual_clock
        .borrow_mut()
        .advance_time(UnixNanos::from(time + 1_000_000_000), true);
    let stale = virtual_clock.borrow().match_handlers(events);
    assert_eq!(stale.len(), 1);
    client.disconnect().await.unwrap();
    for handler in stale {
        handler.run();
    }
    assert_eq!(
        commands.borrow().len(),
        1,
        "an old generation callback cannot issue a query"
    );
    assert!(virtual_clock.borrow().timer_names().is_empty());
    assert!(private.state.creations.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn recorded_flat_history_does_not_block_readonly_reconnect_query_account() {
    let source: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/session_readonly_20261017T045044Z_68958.json"
    ))
    .unwrap();
    let rows = source["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["path"] == "/v1/orders/matches")
        .unwrap()["responseBody"]["data"]
        .clone();
    assert_eq!(rows.as_array().unwrap().len(), 6);
    let rows = rows
        .to_string()
        .replace("0x0000000000000000000000000000000000000022", WALLET);
    let private = server(vec![]).await;
    *private.state.match_snapshot.lock().unwrap() = Some(serde_json::from_str(&rows).unwrap());
    *private.state.position_snapshot.lock().unwrap() = Some(vec![]);
    let (_, mut rx, cache) = client(&private);
    let _portfolio = portfolio(&cache);
    let core = ExecutionClientCore::new(
        trader_id(),
        ClientId::new("PREDICT"),
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
    cfg.reconnect_reconciliation_interval_secs = None;
    cfg.account_refresh_interval_secs = 300;
    let mut client = PredictExecutionClient::new(core, cfg).unwrap();
    connect_reported(&mut client, &mut rx).await;
    let mut bulk = nautilus_common::messages::execution::GenerateOrderStatusReports::new(
        UUID4::new(),
        nautilus_core::time::get_atomic_clock_realtime().get_time_ns(),
        false,
        None,
        None,
        None,
        None,
        None,
    );
    bulk.start = Some(UnixNanos::from(1));
    assert!(
        client
            .generate_order_status_reports(&bulk)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        private
            .state
            .order_list_log
            .lock()
            .unwrap()
            .iter()
            .all(|e| e["query"]["status"] == "OPEN")
    );

    private
        .state
        .sockets
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .send(Message::Close(None))
        .unwrap();
    timeout(Duration::from_secs(3), async {
        while private.state.sockets.lock().unwrap().len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    timeout(Duration::from_secs(3), async {
        while private
            .state
            .ws_requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r["method"] == "subscribe")
            .count()
            < 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    private
        .state
        .sockets
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .send(Message::Text(
            json!({"type":"M","topic":"heartbeat","data":"ready-ack-barrier"})
                .to_string()
                .into(),
        ))
        .unwrap();
    timeout(Duration::from_secs(3), async {
        while !private
            .state
            .ws_requests
            .lock()
            .unwrap()
            .iter()
            .any(|r| r["method"] == "heartbeat" && r["data"] == "ready-ack-barrier")
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(client.is_connected());
    let matches_before = private.state.match_requests.lock().unwrap().len();
    client
        .query_account(QueryAccount::new(
            trader_id(),
            None,
            account_id(),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
    let mut ready = false;
    timeout(Duration::from_secs(5), async {
        while !ready || !client.is_connected() {
            let ExecutionEvent::Account(state) = rx.recv().await.unwrap() else {
                panic!("history must not produce order/fill events")
            };
            ready |= state
                .info
                .as_ref()
                .is_some_and(|i| i.get("predict_connection_epoch") == Some(&json!(1)));
            msgbus::send_account_state(MessagingSwitchboard::portfolio_update_account(), &state);
        }
    })
    .await
    .unwrap();
    assert!(ready);
    assert!(
        cache
            .borrow()
            .orders(None, None, None, None, None)
            .is_empty()
    );
    assert!(
        cache
            .borrow()
            .positions_open(None, None, None, None, None)
            .is_empty()
    );
    assert_eq!(
        private.state.match_requests.lock().unwrap().len(),
        matches_before,
        "account refresh does not scan historical matches"
    );
    assert!(private.state.creations.lock().unwrap().is_empty());
    assert!(private.state.removals.lock().unwrap().is_empty());
    client.disconnect().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn ambiguous_wallet_rejection_requests_one_native_query_without_retrying_or_closing() {
    let private = server(vec![]).await;
    let (_, mut rx, cache) = client(&private);
    let own = order(1);
    cache
        .borrow_mut()
        .add_instrument(instrument(1).into())
        .unwrap();
    cache
        .borrow_mut()
        .add_order(own.clone(), None, None, false)
        .unwrap();
    cache
        .borrow_mut()
        .add_venue_order_id(
            &own.client_order_id(),
            &own.venue_order_id().unwrap(),
            false,
        )
        .unwrap();
    let _portfolio = portfolio(&cache);
    let core = ExecutionClientCore::new(
        trader_id(),
        ClientId::new("PREDICT"),
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
    cfg.reconnect_reconciliation_interval_secs = Some(1);
    let virtual_clock = Rc::new(RefCell::new(VirtualClock::new()));
    let clock = Box::leak(Box::new(AtomicTime::new(false, OBSERVED.into())));
    let mut client = PredictExecutionClient::new_with_clock(core, cfg, clock)
        .unwrap()
        .with_reconciliation_clock(virtual_clock.clone());
    let commands = Rc::new(RefCell::new(Vec::new()));
    msgbus::register_trading_command_endpoint(
        MessagingSwitchboard::exec_engine_queue_execute(),
        TypedIntoHandler::from({
            let commands = commands.clone();
            move |command: TradingCommand| commands.borrow_mut().push(command)
        }),
    );
    connect_reported(&mut client, &mut rx).await;
    let fire = |to: u64| {
        let events = virtual_clock.borrow_mut().advance_time(to.into(), true);
        let handlers = virtual_clock.borrow().match_handlers(events);
        for handler in handlers {
            handler.run();
        }
    };
    let mut engine = cancellations::engine(&cache);
    let mut duplicate = event("orderNotAccepted", 1);
    duplicate["reason"] = json!("rejectedDuplicate");
    private.wallet(duplicate.clone());
    private.wallet(duplicate.clone());
    let (control_hash, control) = wallet_lifecycle::control(&mut engine, &duplicate);
    private.wallet(control);
    loop {
        let event = next(&mut rx).await;
        let barrier = matches!(&event, ExecutionEvent::Order(nautilus_model::events::OrderEventAny::Canceled(e)) if e.venue_order_id == Some(control_hash));
        submissions::apply(&mut engine, &event);
        if barrier {
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
    fire(1_000_000_000);
    assert_eq!(commands.borrow().len(), 1);
    {
        let captured = commands.borrow();
        let TradingCommand::QueryOrder(query) = &captured[0] else {
            panic!("Native order query")
        };
        assert_eq!(query.client_order_id, own.client_order_id());
        assert_eq!(query.strategy_id, own.strategy_id());
        assert_eq!(query.client_id, Some(ClientId::new("PREDICT")));
    }
    fire(2_000_000_000);
    assert_eq!(commands.borrow().len(), 1, "no adapter retry loop");
    assert!(private.state.creations.lock().unwrap().is_empty());
    assert!(private.state.removals.lock().unwrap().is_empty());
    client.disconnect().await.unwrap();
    fire(3_000_000_000);
    assert_eq!(commands.borrow().len(), 1);
}
