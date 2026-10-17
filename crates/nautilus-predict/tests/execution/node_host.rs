// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Exercise the actual product host; fixture services/strategies remain in tests.
#[allow(dead_code)]
#[path = "../../../catalog-capture-cli/src/predict_node.rs"]
mod host;
#[path = "../support/snapshot_maker.rs"]
mod snapshot_maker;
#[path = "snapshot_multi.rs"]
mod snapshot_multi;
use axum::extract::Path as RoutePath;
use nautilus_common::msgbus::TypedHandler;
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{data::QuoteTick, events::OrderEventAny, identifiers::StrategyId};
use nautilus_predict::signing::PredictAuthSigner;
use nautilus_testkit::testers::{ExecTester, ExecTesterConfig};
use nautilus_trading::StrategyConfig;

use super::{
    exit_orders::{PublicState, barrier, exit_socket},
    *,
};

#[test]
fn product_profile_reuses_native_config_and_rejects_venue_incompatible_overrides() {
    let source = include_str!("../../../../examples/predict-node.toml.example");
    let profile = host::NodeProfile::parse(source).unwrap();
    assert!(profile.predict.submission.is_none());
    assert!(!profile.predict.orderbook_removal_enabled);
    assert!(!profile.node.risk_engine.bypass);
    assert!(
        profile
            .node
            .exec_engine
            .reconciliation_lookback_mins
            .is_none()
    );
    assert_eq!(profile.node.delay_post_stop, Duration::from_secs(10));
    let bounded = host::NodeProfile::parse(&source.replace(
        "reconciliation = true",
        "reconciliation = true\nreconciliation_lookback_mins = 10",
    ))
    .unwrap();
    assert_eq!(
        bounded.node.exec_engine.reconciliation_lookback_mins,
        Some(10)
    );
    for change in [
        source.replace("bypass = false", "bypass = true"),
        source.replace("environment = \"mainnet\"", "environment = \"testnet\""),
        source.replace("reconciliation = true", "reconciliation = false"),
        source.replace("interval_secs = 300", "interval_secs = 300\nmarket_id = 42"),
        source.replace(
            "reconciliation = true",
            "reconciliation = true\nreconciliation_instrument_ids = [\"42-1.PREDICT\"]",
        ),
    ] {
        assert!(host::NodeProfile::parse(&change).is_err());
    }
}

#[derive(Clone)]
struct Public {
    market: Value,
    trace: Arc<Mutex<Vec<Value>>>,
}
async fn category(RoutePath(slug): RoutePath<String>, State(s): State<Public>) -> Response {
    s.trace
        .lock()
        .unwrap()
        .push(json!({"kind":"category","slug":slug}));
    json_response(json!({"success":true,"data":{"markets":[s.market["data"].clone()]}}))
}
async fn market(RoutePath(id): RoutePath<u64>, State(s): State<Public>) -> Response {
    assert!([42, 43].contains(&id));
    let mut definition = s.market.clone();
    definition["data"]["id"] = json!(id);
    if id == 43 {
        definition["data"]["outcomes"][0]["onChainId"] = json!("33");
        definition["data"]["outcomes"][1]["onChainId"] = json!("44");
    }
    s.trace
        .lock()
        .unwrap()
        .push(json!({"kind":"market","body":definition}));
    json_response(definition)
}
const CHALLENGE: &str = "offline Predict node authentication challenge";
async fn challenge(headers: HeaderMap, State(s): State<Public>) -> Response {
    assert_eq!(headers["x-api-key"], KEY);
    s.trace
        .lock()
        .unwrap()
        .push(json!({"kind":"challenge","body":{"success":true,"data":{"message":CHALLENGE}}}));
    json_response(json!({"success":true,"data":{"message":CHALLENGE}}))
}
async fn authenticate(headers: HeaderMap, State(s): State<Public>, body: Bytes) -> Response {
    assert_eq!(headers["x-api-key"], KEY);
    let request: Value = serde_json::from_slice(&body).unwrap();
    let signer = PredictAuthSigner::new(
        submissions::PUBLIC_KEY.into(),
        PredictEnvironment::BnbMainnet,
        Some(WALLET),
    )
    .unwrap();
    assert_eq!(request["message"], CHALLENGE);
    assert_eq!(request["signer"], WALLET);
    assert_eq!(
        request["signature"],
        signer.sign_auth_message(CHALLENGE).unwrap().expose_secret()
    );
    s.trace
        .lock()
        .unwrap()
        .push(json!({"kind":"authenticate","request":request}));
    json_response(json!({"success":true,"data":{"token":JWT}}))
}
fn definition() -> Value {
    let mut row: Value =
        serde_json::from_str(include_str!("../../test_data/market_2859346.json")).unwrap();
    let now = get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000;
    let start = chrono::DateTime::from_timestamp((now / 300 * 300) as i64, 0)
        .unwrap()
        .with_timezone(&chrono_tz::America::New_York);
    let end = start + chrono::Duration::seconds(300);
    row["data"]["id"] = json!(42);
    row["data"]["title"] = json!(format!(
        "Bitcoin Up or Down - {}, {}-{} ET",
        start.format("%B %-d"),
        start.format("%-I:%M%p"),
        end.format("%-I:%M%p")
    ));
    row["data"]["tradingStatus"] = json!("OPEN");
    row["data"]["status"] = json!("OPEN");
    row["data"]["decimalPrecision"] = json!(3);
    row["data"]["feeRateBps"] = json!(0);
    row["data"]["isNegRisk"] = json!(false);
    row["data"]["isYieldBearing"] = json!(false);
    row["data"]["variantData"]["priceFeedSymbol"] = json!("BTCUSDT");
    row["data"]["outcomes"][0]["onChainId"] = json!("11");
    row["data"]["outcomes"][1]["onChainId"] = json!("22");
    row
}
fn profile(origin: &str, writes: bool) -> String {
    let mut text = format!(
        r#"
[predict]
account_id = "PREDICT-fixture"
privy_credentials_file = "unused-loopback.toml"
environment = "mainnet"
rpc_url = "{origin}/rpc"
http_url = "{origin}"
public_ws_url = "{}"
wallet_ws_url = "{}"
orderbook_removal_enabled = {writes}
[discovery]
price_feed_symbol = "BTC/USDT"
title_asset = "Bitcoin"
interval_secs = 300
[node]
trader_id = "TESTER-001"
delay_post_stop = {{ secs = 1, nanos = 0 }}
[node.risk_engine]
bypass = false
[node.exec_engine]
reconciliation = true
reconciliation_startup_delay_secs = 0.0
inflight_check_interval_ms = 50
inflight_check_threshold_ms = 5000
open_check_interval_secs = 10.0
position_check_interval_secs = 30.0
"#,
        origin.replace("http://", "ws://") + "/public-ws",
        origin.replace("http://", "ws://") + "/ws"
    );
    if writes {
        text.push_str(
            r#"
[predict.submission]
max_order_usdt = "1.2"
max_total_usdt = "3"
require_post_only = true
market_slippage_bps = 100
"#,
        );
    }
    text
}
fn path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/thin_node_native_bootstrap_synthetic_20261016.json")
}
async fn run(writes: bool, saved: Option<&Value>) -> Value {
    run_case(writes, saved, false).await
}
#[derive(Clone, Copy)]
enum Scenario {
    Observe,
    NativeTester,
    SnapshotCancel,
    SnapshotReplace,
}
async fn run_case(writes: bool, saved: Option<&Value>, custom: bool) -> Value {
    run_scenario(
        if custom {
            Scenario::SnapshotCancel
        } else if writes {
            Scenario::NativeTester
        } else {
            Scenario::Observe
        },
        saved,
    )
    .await
}
#[derive(Clone)]
struct RpcGate {
    state: VenueState,
    paused: Arc<std::sync::atomic::AtomicBool>,
    blocked: Arc<std::sync::atomic::AtomicUsize>,
    release: Arc<tokio::sync::Semaphore>,
}
async fn gated_rpc(State(gate): State<RpcGate>, headers: HeaderMap, body: Bytes) -> Response {
    if gate.paused.load(std::sync::atomic::Ordering::Acquire) {
        gate.blocked
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let _ = gate.release.acquire().await;
    }
    collateral_rpc(State(gate.state), headers, body).await
}
fn ack_from_fixture(body: &Value, saved: Option<&Value>, index: usize) -> Value {
    let ack = exit_orders::wire_event(body, "orderAccepted", 1, Decimal::ZERO);
    if let Some(saved) = saved {
        let mut replay = saved["wallet"][index].clone();
        replay["orderHash"] = ack["orderHash"].clone();
        replay["timestamp"] = ack["timestamp"].clone();
        assert_eq!(replay, ack);
        replay
    } else {
        ack
    }
}
struct HostFixture {
    backing: Server,
    public: PublicState,
    trace: Arc<Mutex<Vec<Value>>>,
    def: Value,
    rpc_gate: RpcGate,
    config: String,
    task: JoinHandle<std::io::Result<()>>,
}
async fn fixture(writes: bool, removals: usize) -> HostFixture {
    let backing = server(vec![]).await;
    *backing.state.position_snapshot.lock().unwrap() = Some(vec![]);
    *backing.state.match_snapshot.lock().unwrap() = Some(vec![]);
    backing.state.rpc_frame.lock().unwrap().2 =
        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000;
    for _ in 0..removals {
        backing.state.remove_replies.lock().unwrap().push_back((
            200,
            json!({"echoRemoved":true}),
            0,
        ));
    }
    let rpc_gate = RpcGate {
        state: backing.state.clone(),
        paused: Arc::new(false.into()),
        blocked: Arc::new(0.into()),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    let public = PublicState(Arc::new(Mutex::new(vec![])));
    let trace = Arc::new(Mutex::new(vec![]));
    let def = definition();
    let source = Public {
        market: def.clone(),
        trace: trace.clone(),
    };
    let router = Router::new()
        .route("/v1/account", get(account))
        .route("/v1/positions", get(positions))
        .route("/v1/orders/matches", get(matches_history))
        .route("/v1/orders/{hash}", get(rest_order))
        .route(
            "/v1/orders",
            get(open_orders).post(mock_venue::create_order),
        )
        .route("/v1/orders/remove", post(remove_orders))
        .route("/ws", get(websocket))
        .with_state(backing.state.clone())
        .merge(
            Router::new()
                .route("/rpc", post(gated_rpc))
                .with_state(rpc_gate.clone()),
        )
        .merge(
            Router::new()
                .route("/v1/categories/{slug}", get(category))
                .route("/v1/markets/{id}", get(market))
                .route("/v1/auth/message", get(challenge))
                .route("/v1/auth", post(authenticate))
                .with_state(source),
        )
        .merge(
            Router::new()
                .route("/public-ws", get(exit_socket))
                .with_state(public.clone()),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(axum::serve(listener, router).into_future());
    let config = profile(&origin, writes);
    HostFixture {
        backing,
        public,
        trace,
        def,
        rpc_gate,
        config,
        task,
    }
}
async fn run_scenario(scenario: Scenario, saved: Option<&Value>) -> Value {
    let writes = !matches!(scenario, Scenario::Observe);
    let custom = matches!(
        scenario,
        Scenario::SnapshotCancel | Scenario::SnapshotReplace
    );
    let replace = matches!(scenario, Scenario::SnapshotReplace);
    let HostFixture {
        backing,
        public,
        trace,
        def,
        rpc_gate,
        config,
        task,
    } = fixture(writes, if replace { 2 } else { 1 }).await;
    let signer = Arc::new(
        PredictAuthSigner::new(
            submissions::PUBLIC_KEY.into(),
            PredictEnvironment::BnbMainnet,
            Some(WALLET),
        )
        .unwrap(),
    );
    let mut prepared = host::prepare(
        host::NodeProfile::parse(&config).unwrap(),
        KEY.into(),
        signer,
    )
    .await
    .unwrap();
    assert_eq!(prepared.market_id, 42);
    let cache = prepared.node.kernel().cache.clone();
    assert!(
        cache
            .borrow()
            .instrument(&prepared.instruments[0])
            .is_none(),
        "host must not preload the native instrument cache"
    );
    let strategy_callbacks = Rc::new(RefCell::new(vec![]));
    if custom {
        let strategy = snapshot_maker::SnapshotMaker::new(
            prepared.market_id,
            prepared.instruments[0],
            strategy_callbacks.clone(),
        );
        prepared
            .node
            .add_strategy(if replace {
                strategy.with_cancel_replace()
            } else {
                strategy
            })
            .unwrap();
    } else if writes {
        let cfg = ExecTesterConfig::builder()
            .base(StrategyConfig {
                strategy_id: Some(StrategyId::new("HOST-EXEC-001")),
                use_uuid_client_order_ids: true,
                ..Default::default()
            })
            .instrument_id(prepared.instruments[0])
            .client_id(ClientId::new("PREDICT"))
            .order_qty(Quantity::from(100))
            .subscribe_quotes(true)
            .subscribe_trades(false)
            .enable_limit_buys(true)
            .enable_limit_sells(false)
            .use_post_only(true)
            .tob_offset_ticks(620)
            .close_positions_on_stop(false)
            .cancel_orders_on_stop(true)
            .dry_run(false)
            .log_data(false)
            .build()
            .unwrap();
        prepared.node.add_strategy(ExecTester::new(cfg)).unwrap();
    }
    let quotes = Rc::new(RefCell::new(vec![]));
    let qh = TypedHandler::from({
        let quotes = quotes.clone();
        move |q: &QuoteTick| {
            quotes.borrow_mut().push(json!({"instrument":q.instrument_id.to_string(),"bid":q.bid_price.to_string(),"ask":q.ask_price.to_string()}));
        }
    });
    msgbus::subscribe_quotes("data.quotes.PREDICT.*".into(), qh.clone(), None);
    let events = Rc::new(RefCell::new(vec![]));
    let eh = TypedHandler::from({
        let events = events.clone();
        move |e: &OrderEventAny| {
            events
                .borrow_mut()
                .push(json!({"type":format!("{:?}",e.event_type())}));
        }
    });
    msgbus::subscribe_order_events("events.order.HOST-EXEC-001".into(), eh.clone(), None);
    let accounts = Rc::new(RefCell::new(vec![]));
    let ah = TypedHandler::from({
        let accounts = accounts.clone();
        move |state: &AccountState| {
            accounts
                .borrow_mut()
                .push(json!({"balances":state.balances.iter().map(|b| json!({
                "currency":b.currency.code,"total":b.total.as_decimal().to_string(),
                "free":b.free.as_decimal().to_string(),"locked":b.locked.as_decimal().to_string()
            })).collect::<Vec<_>>() }));
        }
    });
    msgbus::subscribe_account_state("events.account.PREDICT-fixture".into(), ah.clone(), None);
    let handle = prepared.node.handle();
    let mut frames = vec![];
    let mut wallet = vec![];
    let driver = async {
        barrier("host standard quote subscription", || {
            !public.0.lock().unwrap().is_empty()
        })
        .await;
        let ts = get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000;
        let mut frame = json!({"type":"M","topic":"predictOrderbook/42","data":{"marketId":42,"updateTimestampMs":ts,"orderCount":2,"bids":[["0.630","1000"]],"asks":[["0.640","1000"]]}});
        if let Some(saved) = saved {
            let mut replay = saved["books"][0].clone();
            replay["data"]["updateTimestampMs"] = json!(ts);
            assert_eq!(replay, frame);
            frame = replay;
        }
        frames.push(frame.clone());
        public.0.lock().unwrap()[0]
            .send(Message::Text(frame.to_string().into()))
            .unwrap();
        barrier("host both outcome quotes", || quotes.borrow().len() >= 2).await;
        assert!(
            cache
                .borrow()
                .instrument(&prepared.instruments[0])
                .is_some()
        );
        assert!(
            cache
                .borrow()
                .instrument(&prepared.instruments[1])
                .is_some()
        );
        if writes {
            barrier("Rust strategy submits through host Native node", || {
                backing.state.creations.lock().unwrap().len() == 1
            })
            .await;
            let body = backing.state.creations.lock().unwrap()[0].clone();
            let row = live_node::rest_row(&body, "OPEN");
            backing.state.open_orders.lock().unwrap().push(row.clone());
            backing.state.orders_by_hash.lock().unwrap().insert(
                body["data"]["order"]["hash"].as_str().unwrap().into(),
                json!({"success":true,"data":row}),
            );
            if replace {
                rpc_gate
                    .paused
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            let ack = ack_from_fixture(&body, saved, 0);
            wallet.push(ack.clone());
            backing.wallet(ack.clone());
            barrier("Native strategy accepted via host", || {
                events.borrow().iter().any(|e| e["type"] == "Accepted")
            })
            .await;
            if replace {
                barrier("first canceled and strategy QueryAccount sent", || {
                    strategy_callbacks
                        .borrow()
                        .iter()
                        .any(|e| e["callback"] == "query_account")
                        && rpc_gate.blocked.load(std::sync::atomic::Ordering::Acquire) > 0
                })
                .await;
                assert_eq!(backing.state.creations.lock().unwrap().len(), 1);
                assert_eq!(backing.state.removals.lock().unwrap().len(), 1);
                assert_eq!(
                    strategy_callbacks
                        .borrow()
                        .iter()
                        .filter(|e| e["callback"] == "on_order_modify_rejected")
                        .count(),
                    1,
                    "strategy must receive exactly one standard modify rejection"
                );
                // A late duplicate acknowledgment cannot revive the canceled order.
                wallet.push(ack.clone());
                backing.wallet(ack);
                frames.push(frame.clone());
                public.0.lock().unwrap()[0]
                    .send(Message::Text(frame.to_string().into()))
                    .unwrap();
                barrier(
                    "snapshot defers replacement while account refresh is held",
                    || {
                        strategy_callbacks
                            .borrow()
                            .iter()
                            .any(|e| e["callback"] == "ReplacementDeferred")
                    },
                )
                .await;
                assert_eq!(backing.state.creations.lock().unwrap().len(), 1);
                let next_block = {
                    let mut rpc = backing.state.rpc_frame.lock().unwrap();
                    rpc.1 += 1;
                    rpc.2 = get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000;
                    rpc.1
                };
                rpc_gate
                    .paused
                    .store(false, std::sync::atomic::Ordering::Release);
                rpc_gate.release.close();
                barrier("fresh reported account reaches native Cache", || {
                    let cache = cache.borrow();
                    let account = cache.account(&account_id()).unwrap();
                    account.balance_locked(Some(Currency::USDT())) == Some(Money::from("0 USDT"))
                        && account.last_event().and_then(|e| {
                            e.info
                                .as_ref()
                                .and_then(|i| i.get("predict_block_number"))
                                .and_then(Value::as_u64)
                        }) == Some(next_block)
                })
                .await;
                frame["data"]["bids"] = json!([["0.631", "1000"]]);
                frame["data"]["asks"] = json!([["0.641", "1000"]]);
                frame["data"]["updateTimestampMs"] =
                    json!(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000);
                if let Some(saved) = saved {
                    let mut replay = saved["books"][2].clone();
                    replay["data"]["updateTimestampMs"] =
                        frame["data"]["updateTimestampMs"].clone();
                    assert_eq!(replay, frame);
                    frame = replay;
                }
                frames.push(frame.clone());
                public.0.lock().unwrap()[0]
                    .send(Message::Text(frame.to_string().into()))
                    .unwrap();
                barrier("fresh custom snapshot submits one replacement", || {
                    backing.state.creations.lock().unwrap().len() == 2
                })
                .await;
                let second = backing.state.creations.lock().unwrap()[1].clone();
                let row = live_node::rest_row(&second, "OPEN");
                backing.state.open_orders.lock().unwrap().push(row.clone());
                backing.state.orders_by_hash.lock().unwrap().insert(
                    second["data"]["order"]["hash"].as_str().unwrap().into(),
                    json!({"success":true,"data":row}),
                );
                assert_ne!(
                    body["data"]["order"]["hash"],
                    second["data"]["order"]["hash"]
                );
                assert_eq!(body["data"]["pricePerShare"], "10000000000000000");
                assert_eq!(second["data"]["pricePerShare"], "11000000000000000");
                let ack = ack_from_fixture(&second, saved, 2);
                wallet.push(ack.clone());
                backing.wallet(ack);
                barrier(
                    "replacement canceled and funds released before stop",
                    || {
                        strategy_callbacks
                            .borrow()
                            .iter()
                            .filter(|e| e["callback"] == "on_order_canceled")
                            .count()
                            == 2
                            && cache
                                .borrow()
                                .account(&account_id())
                                .unwrap()
                                .balance_locked(Some(Currency::USDT()))
                                == Some(Money::from("0 USDT"))
                    },
                )
                .await;
                frames.push(frame.clone());
                public.0.lock().unwrap()[0]
                    .send(Message::Text(frame.to_string().into()))
                    .unwrap();
                barrier("duplicate snapshot after completed replacement", || {
                    strategy_callbacks
                        .borrow()
                        .iter()
                        .filter(|e| e["callback"] == "on_data")
                        .count()
                        == 4
                })
                .await;
                assert_eq!(backing.state.creations.lock().unwrap().len(), 2);
                assert_eq!(backing.state.removals.lock().unwrap().len(), 2);
            } else if custom {
                barrier("snapshot strategy canceled before stopping node", || {
                    strategy_callbacks
                        .borrow()
                        .iter()
                        .any(|e| e["callback"] == "on_order_canceled")
                })
                .await;
                assert_eq!(backing.state.removals.lock().unwrap().len(), 1);
                barrier(
                    "native account refresh releases canceled order funds",
                    || {
                        cache
                            .borrow()
                            .account(&account_id())
                            .unwrap()
                            .balance_locked(Some(Currency::USDT()))
                            == Some(Money::from("0 USDT"))
                    },
                )
                .await;
                // Repeated book delivery after a completed cycle must not submit again.
                public.0.lock().unwrap()[0]
                    .send(Message::Text(frame.to_string().into()))
                    .unwrap();
                barrier("second custom snapshot reaches strategy", || {
                    strategy_callbacks
                        .borrow()
                        .iter()
                        .filter(|e| e["callback"] == "on_data")
                        .count()
                        == 2
                })
                .await;
                assert_eq!(backing.state.creations.lock().unwrap().len(), 1);
            }
        }
        handle.stop();
    };
    timeout(Duration::from_secs(15), async {
        tokio::join!(driver, prepared.node.run())
    })
    .await
    .unwrap()
    .1
    .unwrap();
    assert_eq!(
        backing.state.creations.lock().unwrap().len(),
        if replace { 2 } else { usize::from(writes) }
    );
    assert_eq!(
        backing.state.removals.lock().unwrap().len(),
        if replace { 2 } else { usize::from(writes) }
    );
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
        Some(Money::from("1000 USDT"))
    );
    assert!(
        trace
            .lock()
            .unwrap()
            .iter()
            .any(|r| r["kind"] == "category")
    );
    assert!(
        trace
            .lock()
            .unwrap()
            .iter()
            .any(|r| r["kind"] == "authenticate")
    );
    if let Some(saved) = saved {
        assert_eq!(saved["feedback"], json!(events.borrow().clone()));
    }
    let mut out = json!({"source":"synthetic loopback actual product Rust host + Native LiveNode; not mainnet","writesEnabledInFixture":writes,"nativeRiskBypassed":false,"discoveredMarket":42,"marketDefinition":def,"trace":trace.lock().unwrap().clone(),"books":frames,"wallet":wallet,"quotes":quotes.borrow().clone(),"requests":backing.state.creations.lock().unwrap().clone(),"feedback":events.borrow().clone(),"removalResponses":backing.state.removal_responses.lock().unwrap().clone(),"hostPreloadedInstruments":false,"nativeBootstrapLoadedBothInstruments":true,"finalOpen":0,"finalInflight":0,"finalPositions":0,"finalLocked":"0","finalCash":"1000"});
    if custom {
        let callbacks = strategy_callbacks.borrow();
        assert_eq!(
            callbacks
                .iter()
                .filter(|e| e["callback"] == "submit_order")
                .count(),
            if replace { 2 } else { 1 }
        );
        assert_eq!(
            callbacks
                .iter()
                .filter(|e| e["callback"] == "on_order_accepted")
                .count(),
            if replace { 2 } else { 1 }
        );
        assert_eq!(
            callbacks
                .iter()
                .filter(|e| e["callback"] == "on_order_canceled")
                .count(),
            if replace { 2 } else { 1 }
        );
        assert!(
            !callbacks
                .iter()
                .any(|e| (e["callback"].as_str().unwrap().contains("rejected")
                    && !(replace && e["callback"] == "on_order_modify_rejected"))
                    || e["callback"] == "on_order_denied")
        );
        if let Some(saved) = saved {
            assert_eq!(saved["strategyCallbacks"], json!(*callbacks));
        }
        let account_events = accounts.borrow();
        assert!(account_events.iter().any(|event| {
            event["balances"].as_array().unwrap().iter().any(|b| {
                b["currency"] == "USDT"
                    && b["locked"].as_str().unwrap().parse::<Decimal>().unwrap() == Decimal::ONE
            })
        }));
        out["accountFeedback"] = json!(*account_events);
        out["strategyCallbacks"] = json!(*callbacks);
        out["signalInput"] = json!("custom_snapshot");
        out["canceledBeforeNodeStop"] = json!(true);
        if replace {
            out["scenario"] = json!("custom_snapshot_modify_rejected_cancel_replace");
            out["accountRefreshHeldBeforeReplacement"] = json!(true);
            out["nativeAccountAtEnd"] = json!({"total":"1000","locked":"0","block":backing.state.rpc_frame.lock().unwrap().1});
            let cache = cache.borrow();
            let all_orders = cache.orders(None, None, None, None, None);
            assert_eq!(all_orders.len(), 2);
            assert!(
                all_orders
                    .iter()
                    .all(|o| o.status() == OrderStatus::Canceled)
            );
        }
    }
    msgbus::unsubscribe_quotes("data.quotes.PREDICT.*".into(), &qh);
    msgbus::unsubscribe_order_events("events.order.HOST-EXEC-001".into(), &eh);
    msgbus::unsubscribe_account_state("events.account.PREDICT-fixture".into(), &ah);
    prepared.node.dispose();
    task.abort();
    out
}
#[tokio::test(flavor = "current_thread")]
async fn product_host_discovers_authenticates_and_runs_native_strategy_with_native_shutdown() {
    let f: Value = serde_json::from_slice(&std::fs::read(path()).unwrap()).unwrap();
    for writes in [false, true] {
        run(
            writes,
            Some(
                f["sessions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|s| s["writesEnabledInFixture"] == writes)
                    .unwrap(),
            ),
        )
        .await;
    }
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit synthetic capture, create_new only"]
async fn record_thin_node_host_json() {
    let sessions = vec![run(false, None).await, run(true, None).await];
    serde_json::to_writer_pretty(std::fs::OpenOptions::new().write(true).create_new(true).open(path()).unwrap(),&json!({"source":"synthetic local product host, not real wallet/orders","sessions":sessions})).unwrap();
}

fn snapshot_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
        "tests/fixtures/private_api/snapshot_strategy_account_feedback_synthetic_20261016.json",
    )
}
#[tokio::test(flavor = "current_thread")]
async fn custom_snapshot_strategy_submits_and_cancels_on_product_host() {
    let saved: Value = serde_json::from_slice(&std::fs::read(snapshot_path()).unwrap()).unwrap();
    run_case(true, Some(&saved), true).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline custom snapshot capture; create_new only"]
async fn record_snapshot_strategy_host_json() {
    let out = run_case(true, None, true).await;
    serde_json::to_writer_pretty(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(snapshot_path())
            .unwrap(),
        &out,
    )
    .unwrap();
}

fn replace_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/snapshot_modify_cancel_replace_synthetic_20261016.json")
}
#[tokio::test(flavor = "current_thread")]
async fn snapshot_modify_rejection_query_account_and_cancel_replace() {
    let saved: Value = serde_json::from_slice(&std::fs::read(replace_path()).unwrap()).unwrap();
    run_scenario(Scenario::SnapshotReplace, Some(&saved)).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit synthetic modify/cancel-replace capture; create_new only"]
async fn record_snapshot_modify_cancel_replace_json() {
    let out = run_scenario(Scenario::SnapshotReplace, None).await;
    serde_json::to_writer_pretty(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(replace_path())
            .unwrap(),
        &out,
    )
    .unwrap();
}

/// Own workspace fixture files only. Never touches operator configs or shared Redis.
struct LocalFiles(std::path::PathBuf);
impl LocalFiles {
    fn new() -> Self {
        let p = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("../../.predict-node-fixture-{}", UUID4::new()));
        std::fs::create_dir(&p).unwrap();
        Self(p.canonicalize().unwrap())
    }
    fn write(&self, name: &str, text: &str) -> std::path::PathBuf {
        let p = self.0.join(name);
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        use std::io::Write;
        options
            .open(&p)
            .unwrap()
            .write_all(text.as_bytes())
            .unwrap();
        p
    }
}
impl Drop for LocalFiles {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
#[test]
fn product_privy_loader_validates_owner_permissions_and_redacts_bad_toml() {
    let files = LocalFiles::new();
    let signer = PredictAuthSigner::new(
        submissions::PUBLIC_KEY.into(),
        PredictEnvironment::BnbMainnet,
        Some(WALLET),
    )
    .unwrap();
    let source = format!(
        "[privy]\nprivate_key = {:?}\npredict_account = {:?}\nowner_address = {:?}\n",
        submissions::PUBLIC_KEY,
        WALLET,
        signer.owner_address().to_string()
    );
    let p = files.write("privy.toml", &source);
    let loaded = host::load_signer(&p, PredictEnvironment::BnbMainnet).unwrap();
    assert_eq!(loaded.owner_address(), signer.owner_address());
    std::fs::write(
        &p,
        source.replace(
            &signer.owner_address().to_string(),
            "0x2222222222222222222222222222222222222222",
        ),
    )
    .unwrap();
    let err = host::load_signer(&p, PredictEnvironment::BnbMainnet)
        .unwrap_err()
        .to_string();
    assert!(err.contains("owner check"));
    assert!(!err.contains(submissions::PUBLIC_KEY));
    std::fs::write(&p, "[privy]\nprivate_key = \"SECRET_INVALID_TOML\n").unwrap();
    let err = host::load_signer(&p, PredictEnvironment::BnbMainnet)
        .unwrap_err()
        .to_string();
    assert!(!err.contains("SECRET_INVALID_TOML"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(
            host::load_signer(&p, PredictEnvironment::BnbMainnet)
                .unwrap_err()
                .to_string()
                .contains("chmod 600")
        );
    }
}

#[cfg(unix)]
async fn cli(record: bool) {
    use std::process::Stdio;
    let binary = std::env::var("PREDICT_NODE_TEST_BINARY")
        .expect("explicit built CLI path required for this ignored loopback test");
    let backing = server(vec![]).await;
    *backing.state.position_snapshot.lock().unwrap() = Some(vec![]);
    *backing.state.match_snapshot.lock().unwrap() = Some(vec![]);
    backing.state.rpc_frame.lock().unwrap().2 =
        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000;
    let public = PublicState(Arc::new(Mutex::new(vec![])));
    let trace = Arc::new(Mutex::new(vec![]));
    let def = definition();
    let source = Public {
        market: def.clone(),
        trace: trace.clone(),
    };
    let router = Router::new()
        .route("/v1/account", get(account))
        .route("/v1/positions", get(positions))
        .route("/v1/orders/matches", get(matches_history))
        .route("/v1/orders/{hash}", get(rest_order))
        .route(
            "/v1/orders",
            get(open_orders).post(mock_venue::create_order),
        )
        .route("/v1/orders/remove", post(remove_orders))
        .route("/rpc", post(collateral_rpc))
        .route("/ws", get(websocket))
        .with_state(backing.state.clone())
        .merge(
            Router::new()
                .route("/v1/categories/{slug}", get(category))
                .route("/v1/markets/{id}", get(market))
                .route("/v1/auth/message", get(challenge))
                .route("/v1/auth", post(authenticate))
                .with_state(source),
        )
        .merge(
            Router::new()
                .route("/public-ws", get(exit_socket))
                .with_state(public.clone()),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let service = tokio::spawn(axum::serve(listener, router).into_future());
    let files = LocalFiles::new();
    files.write(".env", "");
    let api_file = files.write("api.toml", &format!("api_key = {KEY:?}\n"));
    files.write(
        "privy.toml",
        &format!(
            "[privy]\nprivate_key = {:?}\npredict_account = {:?}\n",
            submissions::PUBLIC_KEY,
            WALLET
        ),
    );
    let mut config = profile(&origin, false).replace("unused-loopback.toml", "privy.toml");
    config.push_str("\n[node.logging]\nstdout_level = \"DEBUG\"\n");
    let config_file = files.write("node.toml", &config);
    let stdout = files.write("stdout.log", "");
    let stderr = files.write("stderr.log", "");
    let mut child = tokio::process::Command::new(&binary)
        .args(["predict-node", "--config", config_file.to_str().unwrap()])
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap())
        .env("PREDICT_CREDENTIALS_FILE", api_file)
        .current_dir(&files.0)
        .stdin(Stdio::null())
        .stdout(Stdio::from(std::fs::File::create(&stdout).unwrap()))
        .stderr(Stdio::from(std::fs::File::create(&stderr).unwrap()))
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let connected = timeout(Duration::from_secs(30), async {
        loop {
            if !public.0.lock().unwrap().is_empty() {
                break;
            }
            if let Some(status) = child.try_wait().unwrap() {
                panic!(
                    "CLI exited {status}: {}",
                    std::fs::read_to_string(&stderr).unwrap()
                );
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        connected.is_ok(),
        "CLI connection timeout: {}",
        std::fs::read_to_string(&stdout).unwrap()
    );
    let frame = json!({"type":"M","topic":"predictOrderbook/42","data":{"marketId":42,"updateTimestampMs":get_atomic_clock_realtime().get_time_ns().as_u64()/1_000_000,"orderCount":2,"bids":[["0.630","1000"]],"asks":[["0.640","1000"]]}});
    public.0.lock().unwrap()[0]
        .send(Message::Text(frame.to_string().into()))
        .unwrap();
    barrier("actual CLI actor consumes YES and NO quotes", || {
        let log = std::fs::read_to_string(&stdout).unwrap();
        log.contains("42-1.PREDICT bid=") && log.contains("42-2.PREDICT bid=")
    })
    .await;
    let pid = child.id().unwrap();
    assert!(
        tokio::process::Command::new("kill")
            .args(["-INT", &pid.to_string()])
            .status()
            .await
            .unwrap()
            .success()
    );
    let status = timeout(Duration::from_secs(15), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(
        status.success(),
        "CLI SIGINT shutdown failed: {}",
        std::fs::read_to_string(&stderr).unwrap()
    );
    assert!(backing.state.creations.lock().unwrap().is_empty());
    assert!(backing.state.removals.lock().unwrap().is_empty());
    let out = json!({"source":"synthetic CLI child, loopback only; no operator files/env or real accounts used","command":["catalog-capture-cli","predict-node","--config","node.toml"],"environmentIsolated":true,"nativeSigintShutdown":true,"exitSuccess":status.success(),"quotesObserved":["42-1.PREDICT","42-2.PREDICT"],"marketDefinition":def,"discoveryAndAuth":trace.lock().unwrap().clone(),"book":frame,"startupOrderReports":backing.state.order_list_log.lock().unwrap().clone(),"positionReports":backing.state.position_response_log.lock().unwrap().clone(),"rpcMethods":backing.state.rpc_methods.lock().unwrap().clone(),"orderPosts":0,"removalPosts":0});
    let p = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/thin_node_native_cli_synthetic_20261016.json");
    if record {
        serde_json::to_writer_pretty(
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(p)
                .unwrap(),
            &out,
        )
        .unwrap();
    } else {
        let saved: Value = serde_json::from_slice(&std::fs::read(p).unwrap()).unwrap();
        assert_eq!(saved["command"], out["command"]);
        assert_eq!(saved["quotesObserved"], out["quotesObserved"]);
        assert_eq!(saved["exitSuccess"], out["exitSuccess"]);
    }
    service.abort();
}
#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit built CLI loopback capture; environment isolated"]
async fn record_thin_node_cli_json() {
    cli(true).await;
}
#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit built CLI replay; environment isolated"]
async fn actual_cli_discovers_authenticates_observes_and_stops_on_native_sigint() {
    cli(false).await;
}

#[tokio::test]
async fn startup_market_scope_uses_current_account_pages_without_history_or_writes() {
    let mut held = holding(1);
    held["market"]["id"] = json!(43);
    let mut zero = holding(2);
    zero["market"]["id"] = json!(99);
    zero["amount"] = json!("0");
    let s = server(vec![
        json!({"success":true,"cursor":"holdings-next","data":[held.clone(),zero]}),
        json!({"success":true,"cursor":null,"data":[held]}),
    ])
    .await;
    let first = rest_order_data("OPEN", 1, OrderSide::Buy);
    let mut second = first.clone();
    second["marketId"] = json!(44);
    *s.state.open_pages.lock().unwrap() = [
        json!({"success":true,"cursor":"orders-next","data":[first]}),
        json!({"success":true,"cursor":null,"data":[second]}),
    ]
    .into();
    let ids = nautilus_predict::execution::reconciliation_market_ids(&config(&s))
        .await
        .unwrap();
    assert_eq!(ids, vec![42, 43, 44]);
    assert!(s.state.match_requests.lock().unwrap().is_empty());
    assert!(s.state.creations.lock().unwrap().is_empty());
    assert!(s.state.removals.lock().unwrap().is_empty());
    let requests = s.state.requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .any(|q| q.get("after").map(String::as_str) == Some("holdings-next"))
    );
    assert!(
        requests
            .iter()
            .any(|q| q.get("after").map(String::as_str) == Some("orders-next"))
    );
}
#[tokio::test]
async fn startup_market_scope_fails_on_incomplete_pages_and_terminal_open_rows() {
    for terminal in [false, true] {
        let s = server(vec![]).await;
        *s.state.position_snapshot.lock().unwrap() = Some(vec![]);
        if terminal {
            s.state
                .open_orders
                .lock()
                .unwrap()
                .push(rest_order_data("EXPIRED", 1, OrderSide::Buy));
        } else {
            *s.state.open_pages.lock().unwrap() = [
                json!({"success":true,"cursor":"repeat","data":[]}),
                json!({"success":true,"cursor":"repeat","data":[]}),
            ]
            .into();
        }
        let error = nautilus_predict::execution::reconciliation_market_ids(&config(&s))
            .await
            .unwrap_err();
        assert!(error.to_string().contains(if terminal {
            "terminal order"
        } else {
            "repeated a cursor"
        }));
        assert!(s.state.creations.lock().unwrap().is_empty());
    }
}

#[path = "../support/host_observer.rs"]
mod host_observer;

async fn account_instrument_host(with_take_profit: bool) -> Value {
    let f = fixture(false, 0).await;
    let mut held = holding(1);
    held["market"]["id"] = json!(43);
    held["outcome"]["onChainId"] = json!("33");
    held["outcome"]["status"] = Value::Null;
    held["market"]["outcomes"] = json!([held["outcome"].clone()]);

    held["amount"] = json!("31360000000000000000");
    let mut buy = super::cold_start::row(0);
    buy["marketId"] = json!(43);
    buy["order"]["tokenId"] = json!("33");
    let fields = serde_json::from_value(buy["order"].clone()).unwrap();
    let hash = nautilus_predict::signing::PredictOrderHasher::new(PredictEnvironment::BnbMainnet)
        .order_hash(
            &fields,
            nautilus_predict::common::contracts::PredictExchangeType::YieldBearing,
        );
    buy["order"]["hash"] = json!(format!("{hash:#x}"));
    let mut matched = super::cold_start::match_row(&buy);
    matched["market"]["id"] = json!(43);
    for (i, token) in ["33", "44"].iter().enumerate() {
        matched["market"]["outcomes"][i]["onChainId"] = json!(token);
    }
    matched["taker"]["outcome"]["onChainId"] = json!("33");
    for maker in matched["makers"].as_array_mut().unwrap() {
        maker["outcome"]["onChainId"] = json!(if maker["outcome"]["indexSet"] == 1 {
            "33"
        } else {
            "44"
        });
    }
    f.backing
        .state
        .orders_by_hash
        .lock()
        .unwrap()
        .insert(format!("{hash:#x}"), json!({"success":true,"data":buy}));
    *f.backing.state.match_snapshot.lock().unwrap() = Some(vec![matched.clone()]);
    *f.backing.state.position_snapshot.lock().unwrap() = Some(vec![held.clone()]);
    let take_profit = if with_take_profit {
        let mut tp = super::cold_start::row(3);
        tp["marketId"] = json!(43);
        tp["order"]["tokenId"] = json!("33");
        let fields = serde_json::from_value(tp["order"].clone()).unwrap();
        let hash =
            nautilus_predict::signing::PredictOrderHasher::new(PredictEnvironment::BnbMainnet)
                .order_hash(
                    &fields,
                    nautilus_predict::common::contracts::PredictExchangeType::YieldBearing,
                );
        tp["order"]["hash"] = json!(format!("{hash:#x}"));
        f.backing.state.open_orders.lock().unwrap().push(tp.clone());
        f.backing
            .state
            .orders_by_hash
            .lock()
            .unwrap()
            .insert(format!("{hash:#x}"), json!({"success":true,"data":tp}));
        Some(tp)
    } else {
        None
    };
    let signer = Arc::new(
        PredictAuthSigner::new(
            submissions::PUBLIC_KEY.into(),
            PredictEnvironment::BnbMainnet,
            Some(WALLET),
        )
        .unwrap(),
    );
    let prepared = host::prepare(
        host::NodeProfile::parse(&f.config).unwrap(),
        KEY.into(),
        signer,
    )
    .await
    .unwrap();
    assert_eq!(prepared.market_id, 42);
    let mut live = prepared.node;
    let handle = live.handle();
    let journal = Rc::new(RefCell::new(vec![]));
    live.add_strategy(host_observer::HostObserver {
        core: nautilus_trading::StrategyCore::new(StrategyConfig::default()),
        market_id: 42,
        journal: journal.clone(),
    })
    .unwrap();
    let driver = async {
        barrier(
            "product host restored holdings before strategy start",
            || !journal.borrow().is_empty(),
        )
        .await;
        handle.stop();
    };
    let (result, ()) = tokio::join!(live.run(), driver);
    result.unwrap();
    let feedback = journal.borrow().clone();
    let ids = feedback[0]["instruments"].as_array().unwrap();
    assert_eq!(ids.len(), 4);
    assert!(ids.contains(&json!("43-1.PREDICT")));
    assert_eq!(feedback[0]["positions"][0]["instrumentId"], "43-1.PREDICT");
    assert_eq!(
        feedback[0]["positions"][0]["quantity"]
            .as_str()
            .unwrap()
            .parse::<Decimal>()
            .unwrap(),
        Decimal::new(3136, 2)
    );
    assert_eq!(feedback[0]["openOrders"], usize::from(with_take_profit));
    assert!(f.backing.state.creations.lock().unwrap().is_empty());
    assert!(f.backing.state.removals.lock().unwrap().is_empty());
    f.task.abort();
    json!({"source":"offline synthetic product host; not actual nonempty account capture","discoveredMarketId":42,"currentHolding":held,"activeTakeProfit":take_profit,"confirmedMatch":matched,"nativeFeedback":feedback,"discoveryAndMarketRequests":f.trace.lock().unwrap().clone(),"orderWrites":0,"removalWrites":0})
}
#[tokio::test(flavor = "current_thread")]
async fn product_host_loads_held_market_before_native_startup_reconciliation() {
    for with_take_profit in [false, true] {
        account_instrument_host(with_take_profit).await;
    }
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline product host capture, create_new only"]
async fn record_account_instrument_host() {
    let out = json!({"sessions":[account_instrument_host(false).await,account_instrument_host(true).await]});
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/host_account_scope_with_orders_synthetic_20261016.json");
    serde_json::to_writer_pretty(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .unwrap(),
        &out,
    )
    .unwrap();
}

#[path = "node_cancellations.rs"]
mod cancellations;
