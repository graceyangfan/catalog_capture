// -------------------------------------------------------------------------------------------------
// Copyright (C) 2026 yfclark and contributors. Licensed under LGPL-3.0-or-later.
// See https://www.gnu.org/licenses/lgpl-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Unmodified Nautilus ExecTester drives Predict through loopback LiveNode factories.
use nautilus_common::msgbus::TypedHandler;
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_live::node::config::LiveExecutionEngineConfig;
use nautilus_model::{data::QuoteTick, events::OrderEventAny};
use nautilus_predict::{PredictDataClientConfig, PredictSubmissionConfig};
use nautilus_testkit::testers::{ExecTester, ExecTesterConfig};

use super::{
    exit_orders::{PublicState, barrier, exit_socket},
    *,
};

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Profile {
    node: StopConfig,
    execution_engine: LiveExecutionEngineConfig,
    strategy: ExecTesterConfig,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StopConfig {
    delay_post_stop_secs: u64,
}
fn profile() -> Profile {
    let raw: toml::Value = toml::from_str(include_str!(
        "../../../../examples/predict-native-exec-tester.toml.example"
    ))
    .unwrap();
    assert!(raw["strategy"].get("instrument_id").is_none());
    assert!(raw["strategy"].get("client_id").is_none());
    let profile: Profile = raw.try_into().unwrap();
    assert!(profile.strategy.dry_run);
    assert!(
        profile
            .execution_engine
            .reconciliation_lookback_mins
            .is_none()
    );
    profile
}

const CASES: [&str; 8] = [
    "cancel_all",
    "individual",
    "batch",
    "modify_rejected",
    "cancel_replace",
    "dry_run",
    "risk_denied",
    "venue_rejected",
];
fn path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/native_exec_tester_profile_synthetic_20261016.json")
}
fn book(bid: &str, ts: u64) -> Value {
    json!({"type":"M","topic":"predictOrderbook/42","data":{"marketId":42,"updateTimestampMs":ts,"orderCount":2,"bids":[[bid,"1000"]],"asks":[["0.640","1000"]]}})
}
async fn run(case: &str, saved: Option<&Value>) -> Value {
    let profile = profile();
    let dry = case == "dry_run";
    let denied = case == "risk_denied";
    let rejected = case == "venue_rejected";
    let replaces = case == "cancel_replace";
    let expected_posts = if dry || denied {
        0
    } else if replaces {
        2
    } else {
        1
    };
    let expected_cash = if denied { "0.5 USDT" } else { "1000 USDT" };
    let private = server(vec![]).await;
    *private.state.position_snapshot.lock().unwrap() = Some(vec![]);
    *private.state.match_snapshot.lock().unwrap() = Some(vec![]);
    private.state.rpc_frame.lock().unwrap().2 =
        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000;
    if denied {
        private.state.rpc_frame.lock().unwrap().0 = 500_000_000_000_000_000;
    }
    private
        .state
        .create_order_ids
        .lock()
        .unwrap()
        .extend(["12345".into(), "12346".into()]);
    if rejected {
        private
            .state
            .create_replies
            .lock()
            .unwrap()
            .push_back((400, 0, vec![]));
    }
    for _ in 0..2 {
        private.state.remove_replies.lock().unwrap().push_back((
            200,
            json!({"echoRemoved":true}),
            0,
        ));
    }
    let mut cfg = config(&private);
    cfg.account_state_mode = PredictAccountStateMode::Reported;
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", private.http).into(),
    ));
    cfg.account_refresh_interval_secs = 1;
    cfg.orderbook_removal_enabled = true;
    cfg.submission = Some(PredictSubmissionConfig {
        signer: Arc::new(
            nautilus_predict::signing::PredictAuthSigner::new(
                submissions::PUBLIC_KEY.into(),
                PredictEnvironment::BnbMainnet,
                Some(WALLET),
            )
            .unwrap(),
        ),
        max_order_usdt: Decimal::new(12, 1),
        max_total_usdt: Decimal::from(3),
        require_post_only: true,
        market_slippage_bps: 100,
    });
    let public = PublicState(Arc::new(Mutex::new(vec![])));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let public_task = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/v1/markets/42", get(live_node::public_market))
                .route("/ws", get(exit_socket))
                .with_state(public.clone()),
        )
        .into_future(),
    );
    let mut live = node::build_node_with_engine_and_stop_delay(
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
        profile.execution_engine,
        profile.node.delay_post_stop_secs,
    )
    .unwrap();
    let cache = live.kernel().cache.clone();
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let mut tester_cfg = profile.strategy;
    // Fixture identities represent the provider and client factory; no market ID in TOML.
    tester_cfg.instrument_id = instrument_id(1);
    tester_cfg.client_id = Some(ClientId::new("PREDICT"));
    tester_cfg.test_modify_rejected = case == "modify_rejected";
    tester_cfg.cancel_replace_orders_to_maintain_tob_offset = replaces;
    tester_cfg.use_individual_cancels_on_stop = case == "individual";
    tester_cfg.use_batch_cancel_on_stop = case == "batch";
    tester_cfg.dry_run = dry;
    tester_cfg.validate().unwrap();
    let tester_config = serde_json::to_value(&tester_cfg).unwrap();
    live.add_strategy(ExecTester::new(tester_cfg)).unwrap();
    let events = Rc::new(RefCell::new(vec![]));
    let handler = TypedHandler::from({
        let events = events.clone();
        let cache = cache.clone();
        move |e: &OrderEventAny| {
            let cache = cache.borrow();
            let o = cache.order(&e.client_order_id()).unwrap();
            let reason = match e {
                OrderEventAny::Denied(e) => Some(e.reason.as_str()),
                OrderEventAny::Rejected(e) => Some(e.reason.as_str()),
                OrderEventAny::ModifyRejected(e) => Some(e.reason.as_str()),
                OrderEventAny::CancelRejected(e) => Some(e.reason.as_str()),
                _ => None,
            };
            events.borrow_mut().push(json!({"type":format!("{:?}",e.event_type()),"status":o.status().to_string(),"price":o.price().map(|p|p.to_string()),"reason":reason,"clientOrderId":e.client_order_id().to_string()}));
        }
    });
    msgbus::subscribe_order_events("events.order.NATIVE-EXEC-001".into(), handler.clone(), None);
    let quotes = Rc::new(RefCell::new(vec![]));
    let quote_handler = TypedHandler::from({
        let quotes = quotes.clone();
        move |q: &QuoteTick| {
            quotes
                .borrow_mut()
                .push(json!({"bid":q.bid_price.to_string(),"ask":q.ask_price.to_string()}));
        }
    });
    msgbus::subscribe_quotes(
        nautilus_common::msgbus::switchboard::get_quotes_topic(instrument_id(1)).into(),
        quote_handler.clone(),
        None,
    );
    let accounts = Rc::new(RefCell::new(vec![]));
    let account_handler = TypedHandler::from({
        let accounts = accounts.clone();
        move |a: &AccountState| {
            accounts.borrow_mut().push(json!({"balances":a.balances}));
        }
    });
    msgbus::subscribe_account_state(
        "events.account.PREDICT-fixture".into(),
        account_handler.clone(),
        None,
    );
    let handle = live.handle();
    let mut wallet = vec![];
    let mut frames = vec![];
    let driver = async {
        barrier("ExecTester standard quote wire subscription", || {
            !public.0.lock().unwrap().is_empty()
        })
        .await;
        let ts = get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000;
        let mut first = book("0.630", ts);
        if let Some(saved) = saved {
            let mut replay = saved["orderbooks"][0].clone();
            replay["data"]["updateTimestampMs"] = json!(ts);
            assert_eq!(replay, first);
            first = replay;
        }
        frames.push(first.clone());
        public.0.lock().unwrap()[0]
            .send(Message::Text(first.to_string().into()))
            .unwrap();
        barrier("native quote reaches ExecTester", || {
            !quotes.borrow().is_empty()
        })
        .await;
        for n in 0..expected_posts {
            barrier("ExecTester SubmitOrder", || {
                private.state.creations.lock().unwrap().len() > n
            })
            .await;
            if rejected {
                break;
            }
            let body = private.state.creations.lock().unwrap()[n].clone();
            let mut row = live_node::rest_row(&body, "OPEN");
            row["id"] = json!((12345 + n).to_string());
            if let Some(saved) = saved {
                let mut replay = saved["orderRows"][n].clone();
                replay["order"] = body["data"]["order"].clone();
                assert_eq!(replay, row);
                row = replay;
            }
            private.state.orders_by_hash.lock().unwrap().insert(
                body["data"]["order"]["hash"].as_str().unwrap().into(),
                json!({"success":true,"data":row.clone()}),
            );
            private.state.open_orders.lock().unwrap().push(row);
            let mut accepted = exit_orders::wire_event(&body, "orderAccepted", 1, Decimal::ZERO);
            accepted["orderId"] = json!((12345 + n).to_string());
            if let Some(saved) = saved {
                let mut replay = saved["wallet"][n].clone();
                replay["orderHash"] = accepted["orderHash"].clone();
                replay["timestamp"] = accepted["timestamp"].clone();
                assert_eq!(replay, accepted);
                accepted = replay;
            }
            wallet.push(accepted.clone());
            private.wallet(accepted);
            barrier("ExecTester acceptance", || {
                events
                    .borrow()
                    .iter()
                    .filter(|e| e["type"] == "Accepted")
                    .count()
                    > n
            })
            .await;
            if n == 0 && (replaces || case == "modify_rejected") {
                let mut second = book("0.631", ts + 1);
                if let Some(saved) = saved {
                    let mut replay = saved["orderbooks"][1].clone();
                    replay["data"]["updateTimestampMs"] = json!(ts + 1);
                    assert_eq!(replay, second);
                    second = replay;
                }
                frames.push(second.clone());
                public.0.lock().unwrap()[0]
                    .send(Message::Text(second.to_string().into()))
                    .unwrap();
                if case == "modify_rejected" {
                    barrier("ExecTester native modify rejection", || {
                        events
                            .borrow()
                            .iter()
                            .any(|e| e["type"] == "ModifyRejected")
                    })
                    .await;
                    assert_eq!(
                        cache.borrow().orders_open(None, None, None, None, None)[0].price(),
                        Some(Price::from("0.010"))
                    );
                }
            }
        }
        if denied {
            barrier("native RiskEngine Denied", || {
                events.borrow().iter().any(|e| e["type"] == "Denied")
            })
            .await;
        }
        if rejected {
            barrier("venue OrderRejected", || {
                events.borrow().iter().any(|e| e["type"] == "Rejected")
            })
            .await;
        }
        handle.stop(); // Unmodified ExecTester.on_stop issues cancel via native node shutdown.
    };
    timeout(Duration::from_secs(15), async {
        tokio::join!(driver, live.run())
    })
    .await
    .unwrap()
    .1
    .unwrap();
    let observed = events.borrow().clone();
    assert_eq!(
        private.state.creations.lock().unwrap().len(),
        expected_posts
    );
    let accepted = if rejected { 0 } else { expected_posts };
    assert_eq!(private.state.removals.lock().unwrap().len(), accepted);
    assert_eq!(
        observed.iter().filter(|e| e["type"] == "Canceled").count(),
        accepted
    );
    assert_eq!(
        observed.iter().filter(|e| e["type"] == "Accepted").count(),
        accepted
    );
    assert_eq!(
        observed
            .iter()
            .filter(|e| e["type"] == "ModifyRejected")
            .count(),
        usize::from(case == "modify_rejected")
    );
    assert_eq!(
        observed.iter().filter(|e| e["type"] == "Denied").count(),
        usize::from(denied)
    );
    assert_eq!(
        observed.iter().filter(|e| e["type"] == "Rejected").count(),
        usize::from(rejected)
    );
    assert!(
        !observed
            .iter()
            .any(|e| e["type"] == "Filled" || e["type"] == "CancelRejected")
    );
    if denied {
        assert!(
            observed.iter().find(|e| e["type"] == "Denied").unwrap()["reason"]
                .as_str()
                .unwrap()
                .starts_with("NOTIONAL_EXCEEDS_FREE_BALANCE")
        );
    }
    if case == "modify_rejected" {
        assert!(
            observed
                .iter()
                .find(|e| e["type"] == "ModifyRejected")
                .unwrap()["reason"]
                .as_str()
                .unwrap()
                .contains("atomic order modification is unsupported")
        );
    }
    if replaces {
        let posts = private.state.creations.lock().unwrap();
        assert_eq!(posts[0]["data"]["pricePerShare"], "10000000000000000");
        assert_eq!(posts[1]["data"]["pricePerShare"], "11000000000000000");
        assert_ne!(
            posts[0]["data"]["order"]["hash"],
            posts[1]["data"]["order"]["hash"]
        );
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
    assert!(
        cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
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
            .balance_total(Some(Currency::USDT())),
        Some(Money::from(expected_cash))
    );
    if let Some(saved) = saved {
        assert_eq!(saved["testerConfig"], tester_config);
        assert_eq!(
            saved["feedback"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| e["type"].clone())
                .collect::<Vec<_>>(),
            observed
                .iter()
                .map(|e| e["type"].clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            saved["removalResponses"],
            json!(private.state.removal_responses.lock().unwrap().clone())
        );
    }
    let rows: Vec<Value> = wallet
        .iter()
        .map(|ack| {
            private
                .state
                .orders_by_hash
                .lock()
                .unwrap()
                .get(ack["orderHash"].as_str().unwrap())
                .unwrap()["data"]
                .clone()
        })
        .collect();
    let output = json!({"case":case,"source":"synthetic loopback; unmodified Nautilus Rust ExecTester + LiveNode; no real account access","testerConfig":tester_config,"requests":private.state.creations.lock().unwrap().clone(),"orderRows":rows,"removalResponses":private.state.removal_responses.lock().unwrap().clone(),"orderResponses":private.state.order_response_log.lock().unwrap().clone(),"feedback":observed,"wallet":wallet,"orderbooks":frames,"quotes":quotes.borrow().clone(),"accountFeedback":accounts.borrow().clone(),"finalAccountSnapshot":cache.borrow().account(&account_id()).unwrap().last_event().unwrap().balances,"nativeRiskBypassed":false,"nativeReconciliation":true,"reconciliationLookbackMins":null,"stopGraceSecs":1,"finalOpenOrders":0,"finalInflightOrders":0,"finalPositions":0,"finalLocked":"0","finalCash":expected_cash});
    msgbus::unsubscribe_order_events("events.order.NATIVE-EXEC-001".into(), &handler);
    msgbus::unsubscribe_quotes(
        nautilus_common::msgbus::switchboard::get_quotes_topic(instrument_id(1)).into(),
        &quote_handler,
    );
    msgbus::unsubscribe_account_state("events.account.PREDICT-fixture".into(), &account_handler);
    live.dispose();
    public_task.abort();
    output
}

#[tokio::test(flavor = "current_thread")]
async fn unmodified_native_exec_tester_quotes_rejections_cancel_replace_and_shutdown() {
    let saved: Value = serde_json::from_slice(&std::fs::read(path()).unwrap()).unwrap();
    for case in CASES {
        run(
            case,
            Some(
                saved["sessions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|s| s["case"] == case)
                    .unwrap(),
            ),
        )
        .await;
    }
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit synthetic loopback capture; create_new preserves historical JSON"]
async fn record_native_exec_tester_json() {
    let mut sessions = vec![];
    for case in CASES {
        sessions.push(run(case, None).await);
    }
    serde_json::to_writer_pretty(std::fs::OpenOptions::new().write(true).create_new(true).open(path()).unwrap(),&json!({"source":"synthetic offline unmodified native ExecTester, not mainnet","sessions":sessions})).unwrap();
}
