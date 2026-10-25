// -------------------------------------------------------------------------------------------------
// Copyright (C) 2026 yfclark and contributors. Licensed under LGPL-3.0-or-later.
// See https://www.gnu.org/licenses/lgpl-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Native strategy cancellation before acceptance; loopback HTTP/WS/RPC only.
use nautilus_common::{actor::DataActor, msgbus::TypedHandler};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_live::node::config::LiveExecutionEngineConfig;
use nautilus_model::{
    data::CustomData,
    events::{OrderEventAny, OrderSubmitted},
    identifiers::StrategyId,
};
use nautilus_predict::{PredictDataClientConfig, predict_orderbook_data_type};
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};

use super::*;

#[derive(Debug)]
struct EarlyCancelStrategy {
    core: StrategyCore,
    submitted: bool,
    cancel_all: bool,
    journal: Rc<RefCell<Vec<Value>>>,
    account_handler: Option<TypedHandler<AccountState>>,
}
impl DataActor for EarlyCancelStrategy {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let journal = self.journal.clone();
        let handler = TypedHandler::from(move |state: &AccountState| {
            journal
                .borrow_mut()
                .push(json!({"callback":"account_state","balances":state.balances}));
        });
        msgbus::subscribe_account_state(
            format!("events.account.{}", account_id()).into(),
            handler.clone(),
            None,
        );
        self.account_handler = Some(handler);
        self.subscribe_data(
            predict_orderbook_data_type(42),
            Some(ClientId::new("PREDICT")),
            None,
        );
        Ok(())
    }
    fn on_stop(&mut self) -> anyhow::Result<()> {
        if let Some(handler) = self.account_handler.take() {
            msgbus::unsubscribe_account_state(
                format!("events.account.{}", account_id()).into(),
                &handler,
            );
        }
        Ok(())
    }
    fn on_data(&mut self, _: &CustomData) -> anyhow::Result<()> {
        if self.submitted {
            return Ok(());
        }
        self.submitted = true;
        let order = self.order().limit(
            instrument_id(1),
            OrderSide::Buy,
            Quantity::from(100),
            Price::from("0.010"),
            None,
            None,
            Some(true),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        self.submit_order(order, None, Some(ClientId::new("PREDICT")), None)
    }
}
nautilus_strategy!(EarlyCancelStrategy, {
    fn on_order_submitted(&mut self, event: OrderSubmitted) {
        assert!(
            self.cache()
                .order(&event.client_order_id)
                .unwrap()
                .ts_accepted()
                .is_none()
        );
        self.journal
            .borrow_mut()
            .push(json!({"callback":"cancel_before_accepted"}));
        if self.cancel_all {
            self.cancel_all_orders(
                event.instrument_id,
                None,
                Some(ClientId::new("PREDICT")),
                false,
                None,
            )
            .unwrap();
        } else {
            self.cancel_order(event.client_order_id, Some(ClientId::new("PREDICT")), None)
                .unwrap();
        }
        // Native Strategy suppresses a second cancel while PendingCancel.
        self.cancel_order(event.client_order_id, Some(ClientId::new("PREDICT")), None)
            .unwrap();
    }
    fn on_position_event(&mut self, event: nautilus_model::events::PositionEvent) {
        self.journal
            .borrow_mut()
            .push(json!({"callback":"position_event","event":format!("{event:?}")}));
    }
    fn on_order_event(&mut self, event: OrderEventAny) {
        self.journal.borrow_mut().push(json!({"type":format!("{:?}",event.event_type()),"status":self.cache().order(&event.client_order_id()).unwrap().status().to_string()}));
    }
});

fn path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/early_cancel_native_feedback_synthetic_20261016.json")
}

async fn run(mode: &str, saved: Option<&Value>) -> Value {
    let filled = if mode == "query_partial" {
        33
    } else if mode == "query_filled" {
        100
    } else {
        0
    };
    let expected_cash = Money::new(1000.0 - f64::from(filled) * 0.01, Currency::USDT());
    let expect_remove = mode != "rejected" && mode != "query_filled";
    let private = server(vec![]).await;
    *private.state.position_snapshot.lock().unwrap() = Some(vec![]);
    *private.state.match_snapshot.lock().unwrap() = Some(vec![]);
    private.state.rpc_frame.lock().unwrap().2 =
        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000;
    let gate = Arc::new(tokio::sync::Notify::new());
    *private.state.create_gate.lock().unwrap() = Some(gate.clone());
    private.state.create_replies.lock().unwrap().push_back((
        if mode == "rejected" {
            400
        } else if mode.starts_with("query") {
            500
        } else {
            201
        },
        0,
        vec![],
    ));
    private
        .state
        .remove_replies
        .lock()
        .unwrap()
        .push_back((200, json!({"echoRemoved":true}), 0));
    let (http, ws, public_task) = live_node::public_server(1).await;
    let mut cfg = config(&private);
    cfg.account_state_mode = PredictAccountStateMode::Reported;
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", private.http).into(),
    ));
    cfg.orderbook_removal_enabled = true;
    cfg.account_refresh_interval_secs = 1;
    cfg.submission_signer = Some(Arc::new(
        nautilus_predict::signing::PredictAuthSigner::new(
            submissions::PUBLIC_KEY.into(),
            PredictEnvironment::BnbMainnet,
            Some(WALLET),
        )
        .unwrap(),
    ));
    let mut live = node::build_node_with_engine(
        trader_id(),
        PredictDataClientConfig {
            api_key: KEY.into(),
            market_ids: vec![42],
            base_url_http: Some(http),
            base_url_ws: Some(ws),
            http_timeout_secs: 2,
            ws_timeout_secs: 2,
            dynamic_refresh_secs: 30,
            transport_backend: TransportBackend::Tungstenite,
        },
        cfg,
        LiveExecutionEngineConfig {
            reconciliation: mode.starts_with("query"),
            reconciliation_startup_delay_secs: 0.0,
            inflight_check_interval_ms: if mode.starts_with("query") { 20 } else { 0 },
            inflight_check_threshold_ms: 500,
            single_order_query_delay_ms: 0,
            open_check_interval_secs: None,
            position_check_interval_secs: None,
            ..Default::default()
        },
    )
    .unwrap();
    let cache = live.kernel().cache.clone();
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let journal = Rc::new(RefCell::new(vec![]));
    live.add_strategy(EarlyCancelStrategy {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::new("EARLY-CANCEL-001")),
            ..Default::default()
        }),
        submitted: false,
        cancel_all: mode == "cancel_all",
        journal: journal.clone(),
        account_handler: None,
    })
    .unwrap();
    let handle = live.handle();
    let mut wallet = vec![];
    let driver = async {
        exit_orders::barrier("submitted POST held before acknowledgement", || {
            !private.state.creations.lock().unwrap().is_empty()
        })
        .await;
        exit_orders::barrier("strategy requested early cancel", || {
            journal
                .borrow()
                .iter()
                .any(|e| e["type"] == "PendingCancel")
        })
        .await;
        assert!(
            !journal
                .borrow()
                .iter()
                .any(|e| e["type"] == "CancelRejected"),
            "early cancel must be retained, not rejected"
        );
        assert!(private.state.removals.lock().unwrap().is_empty());
        let body = private.state.creations.lock().unwrap()[0].clone();
        let mut row = live_node::rest_row(&body, if filled == 100 { "FILLED" } else { "OPEN" });
        if filled > 0 {
            let amount =
                (U256::from(filled) * U256::from(1_000_000_000_000_000_000u64)).to_string();
            row["amountFilled"] = json!(amount);
            let mut matched = matches::match_row(1);
            matched["executedAt"] = json!(
                chrono::DateTime::from_timestamp_nanos(
                    get_atomic_clock_realtime().get_time_ns().as_u64() as i64
                )
                .to_rfc3339()
            );
            matched["amountFilled"] = json!(amount);
            matched["taker"]["hash"] = body["data"]["order"]["hash"].clone();
            matched["taker"]["price"] = json!("10000000000000000");
            matched["taker"]["amount"] = json!(amount);
            matched["taker"]["fee"] = json!({"type":"COLLATERAL","amount":"0"});
            matched["makers"][0] = matched["taker"].clone();
            matched["taker"]["signer"] = json!("0x2222222222222222222222222222222222222222");
            matched["taker"]["hash"] = json!(format!("0x{}", "22".repeat(32)));
            matched["taker"]["outcome"] = holding(2)["outcome"].clone();
            matched["taker"]["price"] = json!("990000000000000000");
            matched["priceExecuted"] = json!("990000000000000000");
            *private.state.match_snapshot.lock().unwrap() = Some(vec![matched.clone(), matched]);
            let mut position = holding(1);
            position["amount"] = json!(amount);
            *private.state.position_snapshot.lock().unwrap() = Some(vec![position]);
            private.state.rpc_frame.lock().unwrap().1 += 1;
            private.state.rpc_frame.lock().unwrap().2 =
                get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000 + 1;
            private.state.rpc_frame.lock().unwrap().0 = 1_000_000_000_000_000_000_000u128
                - u128::from(filled as u32) * 10_000_000_000_000_000u128;
        }
        if let Some(saved) = saved.filter(|_| mode != "rejected") {
            let mut replay = saved["orderResponses"][0]["body"]["data"].clone();
            replay["order"] = body["data"]["order"].clone();
            assert_eq!(replay, row);
            row = replay;
            if filled > 0 {
                let mut replay = saved["matches"].as_array().unwrap().clone();
                let generated = private
                    .state
                    .match_snapshot
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap();
                for matched in &mut replay {
                    matched["executedAt"] = generated[0]["executedAt"].clone();
                    matched["makers"][0]["hash"] = body["data"]["order"]["hash"].clone();
                    // This replay represents active inventory, rather than the saved market's
                    // later LOST status. Keep all economic and settlement identities unchanged.
                    for outcome in matched["market"]["outcomes"].as_array_mut().unwrap() {
                        outcome["status"] = Value::Null;
                    }
                    matched["taker"]["outcome"]["status"] = Value::Null;
                    for maker in matched["makers"].as_array_mut().unwrap() {
                        maker["outcome"]["status"] = Value::Null;
                    }
                }
                assert_eq!(replay, generated);
                *private.state.match_snapshot.lock().unwrap() = Some(replay);
            }
        }
        private.state.orders_by_hash.lock().unwrap().insert(
            body["data"]["order"]["hash"].as_str().unwrap().into(),
            json!({"success":true,"data":row.clone()}),
        );
        if mode != "rejected" && filled != 100 {
            private.state.open_orders.lock().unwrap().push(row);
        }
        gate.notify_one();
        if mode == "wallet" || mode == "cancel_all" {
            let mut accepted = exit_orders::wire_event(&body, "orderAccepted", 1, Decimal::ZERO);
            if let Some(saved) = saved {
                let mut replay = saved["wallet"][0].clone();
                replay["orderHash"] = accepted["orderHash"].clone();
                replay["timestamp"] = accepted["timestamp"].clone();
                assert_eq!(replay, accepted);
                accepted = replay;
            }
            wallet.extend([accepted.clone(), accepted.clone()]);
            private.wallet(accepted.clone());
            private.wallet(accepted);
        }
        exit_orders::barrier("terminal native order", || {
            cache
                .borrow()
                .orders(None, None, None, None, None)
                .iter()
                .any(|o| o.is_closed())
        })
        .await;
        exit_orders::barrier("native funds unlocked", || {
            cache.borrow().account(&account_id()).is_some_and(|a| {
                a.balance_locked(Some(Currency::USDT())) == Some(Money::from("0 USDT"))
                    && a.balance_total(Some(Currency::USDT())) == Some(expected_cash)
            })
        })
        .await;
        handle.stop();
    };
    timeout(Duration::from_secs(12), async {
        tokio::join!(driver, live.run())
    })
    .await
    .unwrap()
    .1
    .unwrap();
    let events = journal.borrow().clone();
    assert_eq!(private.state.creations.lock().unwrap().len(), 1);
    assert_eq!(
        private.state.removals.lock().unwrap().len(),
        usize::from(expect_remove)
    );
    assert_eq!(
        events.iter().filter(|e| e["type"] == "Accepted").count(),
        usize::from(mode != "rejected" && filled == 0)
    );
    assert!(!events.iter().any(|e| e["type"] == "CancelRejected"));
    assert!(events.iter().any(|e| e["callback"] == "account_state"));
    assert_eq!(
        events
            .iter()
            .filter(|e| e["callback"] == "position_event")
            .count(),
        usize::from(filled > 0)
    );
    if mode.starts_with("query") {
        assert!(!private.state.order_requests.lock().unwrap().is_empty());
    }
    assert!(
        cache
            .borrow()
            .orders_open(None, None, None, None, None)
            .is_empty()
    );
    assert!(
        cache
            .borrow()
            .orders_inflight(None, None, None, None, None)
            .is_empty()
    );
    assert_eq!(
        cache.borrow().positions(None, None, None, None, None).len(),
        usize::from(filled > 0)
    );
    assert_eq!(
        events.iter().filter(|e| e["type"] == "Filled").count(),
        usize::from(filled > 0)
    );
    if filled > 0 {
        assert_eq!(
            cache.borrow().positions_open(None, None, None, None, None)[0]
                .quantity
                .as_decimal(),
            Decimal::from(filled)
        );
        assert_eq!(
            cache.borrow().orders(None, None, None, None, None)[0]
                .filled_qty()
                .as_decimal(),
            Decimal::from(filled)
        );
    }
    assert_eq!(
        cache
            .borrow()
            .account(&account_id())
            .unwrap()
            .balance_total(Some(Currency::USDT())),
        Some(expected_cash)
    );
    if let Some(saved) = saved {
        assert_eq!(saved["mode"], mode);
        assert_eq!(
            saved["feedback"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|v| v["type"].as_str())
                .collect::<Vec<_>>(),
            events
                .iter()
                .filter_map(|v| v["type"].as_str())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            saved["removalResponses"],
            json!(private.state.removal_responses.lock().unwrap().clone())
        );
    }
    let result = json!({"mode":mode,"source":"synthetic loopback Rust Strategy + LiveNode; no real account access","feedback":events,"requests":private.state.creations.lock().unwrap().clone(),"wallet":wallet,"orderResponses":private.state.order_response_log.lock().unwrap().clone(),"removalResponses":private.state.removal_responses.lock().unwrap().clone(),"finalOpen":0,"finalInflight":0,"finalPositions":usize::from(filled>0),"filledQuantity":filled,"finalLocked":"0","finalCash":expected_cash.as_decimal().to_string(),"matches":private.state.match_snapshot.lock().unwrap().clone()});
    live.dispose();
    public_task.abort();
    result
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_cancel_before_accepted_uses_native_query_or_wallet_without_retry_timer() {
    let saved: Value = serde_json::from_slice(&std::fs::read(path()).unwrap()).unwrap();
    for mode in [
        "wallet",
        "query",
        "query_partial",
        "query_filled",
        "rejected",
    ] {
        run(
            mode,
            Some(
                saved["sessions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|s| s["mode"] == mode)
                    .unwrap(),
            ),
        )
        .await;
    }
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline synthetic recording, create_new only"]
async fn record_early_cancel_json() {
    let mut sessions = vec![];
    for mode in [
        "wallet",
        "query",
        "query_partial",
        "query_filled",
        "rejected",
    ] {
        sessions.push(run(mode, None).await);
    }
    serde_json::to_writer_pretty(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path())
            .unwrap(),
        &json!({"source":"synthetic, not mainnet","sessions":sessions}),
    )
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn untracked_submitted_order_is_rejected_instead_of_retaining_foreign_cancel_intent() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = reported_client_with_removal(&server, true);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let mut order = OrderAny::try_from(order(1).init_event().clone()).unwrap();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    order
        .apply(factory.generate_order_submitted(&order, UnixNanos::from(OBSERVED)))
        .unwrap();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, Some(ClientId::new("PREDICT")), false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    client.cancel_order(cancellations::cancel(&order)).unwrap();
    let event = timeout(Duration::from_secs(2), async {
        loop {
            if let ExecutionEvent::Order(OrderEventAny::CancelRejected(event)) =
                rx.recv().await.unwrap()
            {
                break event;
            }
        }
    })
    .await
    .unwrap();
    assert!(event.reason.contains("locally owned pending submission"));
    assert!(server.state.creations.lock().unwrap().is_empty());
    assert!(server.state.removals.lock().unwrap().is_empty());
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Submitted
    );
    client.disconnect().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_cancel_all_includes_submitted_orders_and_defers_until_accepted() {
    let result = run("cancel_all", None).await;
    assert_eq!(result["finalOpen"], 0);
    assert_eq!(result["finalInflight"], 0);
    assert!(
        result["feedback"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["type"] == "Canceled")
    );
}
