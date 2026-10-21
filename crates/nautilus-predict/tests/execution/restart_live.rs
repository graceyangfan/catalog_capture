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

//! New LiveNode/Cache/strategy lifecycles using the real native Redis backing.
use std::process::Stdio;

use nautilus_common::{cache::CacheConfig, enums::Environment};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_infrastructure::redis::cache::RedisCacheConfig;
use nautilus_live::node::{LiveNode, config::LiveExecutionEngineConfig};
use nautilus_model::identifiers::{ClientOrderId, StrategyId};
use nautilus_predict::{
    PredictDataClientConfig, PredictDataClientFactory, PredictSubmissionConfig,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{
    exit_orders::{
        PublicState, barrier, exit_socket, settlement,
        strategy::{ExitSettings, ExitStrategy},
        wire_event,
    },
    strategy_lifecycle::{book, callback, cash, native_id, reconnect_snapshot, rest, stock},
    *,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    ActiveStop,
    PartialStop,
    FilledOffline,
    MissingIntent,
    WrongRole,
}
impl Case {
    fn refused(self) -> bool {
        matches!(self, Self::MissingIntent | Self::WrongRole)
    }
}

struct Redis {
    child: tokio::process::Child,
    port: u16,
}
impl Redis {
    async fn start() -> Self {
        let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = reservation.local_addr().unwrap().port();
        drop(reservation);
        let child = tokio::process::Command::new("redis-server")
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--save",
                "",
                "--appendonly",
                "no",
                "--maxmemory",
                "32mb",
                "--maxmemory-policy",
                "noeviction",
                "--databases",
                "1",
                "--maxclients",
                "32",
                "--daemonize",
                "no",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("explicit persistence integration requires redis-server");
        timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(mut stream) = tokio::net::TcpStream::connect(("127.0.0.1", port)).await {
                    stream.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
                    let mut reply = [0; 7];
                    stream.read_exact(&mut reply).await.unwrap();
                    assert_eq!(&reply, b"+PONG\r\n");
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        Self { child, port }
    }
    async fn stop(mut self) {
        self.child.kill().await.unwrap();
    }
}

fn node(private: &Server, public_port: u16, redis_port: u16, read_only: bool) -> LiveNode {
    let mut cfg = config(private);
    cfg.account_state_mode = PredictAccountStateMode::Reported;
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", private.http).into(),
    ));
    cfg.account_refresh_interval_secs = 1;
    cfg.orderbook_removal_enabled = true;
    cfg.submission = (!read_only).then(|| PredictSubmissionConfig {
        signer: Arc::new(
            nautilus_predict::signing::PredictAuthSigner::new(
                submissions::PUBLIC_KEY.into(),
                PredictEnvironment::BnbMainnet,
                Some(WALLET),
            )
            .unwrap(),
        ),

        market_slippage_bps: 100,
    });
    LiveNode::builder(trader_id(), Environment::Live)
        .unwrap()
        .with_name("PredictNativeRestartReplay")
        .with_cache_config(CacheConfig {
            use_instance_id: false,
            flush_on_start: false,
            buffer_interval_ms: Some(5),
            ..Default::default()
        })
        .with_cache_database_factory(Box::new(RedisCacheConfig {
            host: Some("127.0.0.1".into()),
            port: Some(redis_port),
            connection_timeout: 2,
            response_timeout: 2,
            number_of_retries: 0,
            ..Default::default()
        }))
        .with_load_state(true)
        .with_save_state(true)
        .with_exec_engine_config(LiveExecutionEngineConfig {
            load_cache: true,
            reconciliation: true,
            reconciliation_startup_delay_secs: 0.0,
            reconciliation_lookback_mins: None,
            inflight_check_interval_ms: 0,
            open_check_interval_secs: None,
            position_check_interval_secs: None,
            ..Default::default()
        })
        .with_timeout_connection(10)
        .with_timeout_portfolio(10)
        .with_delay_post_stop_secs(0)
        .with_timeout_disconnection_secs(5)
        .add_data_client(
            Some("PREDICT".into()),
            Box::new(PredictDataClientFactory::new()),
            Box::new(PredictDataClientConfig {
                api_key: KEY.into(),
                market_ids: vec![42],
                base_url_http: Some(format!("http://127.0.0.1:{public_port}")),
                base_url_ws: Some(format!("ws://127.0.0.1:{public_port}/ws")),
                http_timeout_secs: 2,
                ws_timeout_secs: 2,
                dynamic_refresh_secs: 30,
                transport_backend: TransportBackend::Tungstenite,
            }),
        )
        .unwrap()
        .add_exec_client(
            Some("PREDICT".into()),
            Box::new(PredictExecutionClientFactory::new()),
            Box::new(cfg),
        )
        .unwrap()
        .build()
        .unwrap()
}
fn strategy(journal: Rc<RefCell<Vec<Value>>>) -> ExitStrategy {
    ExitStrategy::new(
        instrument_id(1),
        1,
        journal,
        ExitSettings::parse(include_str!(
            "../../../../examples/predict-exit-strategy.toml.example"
        ))
        .unwrap(),
        200,
    )
    .with_native_entry(Price::from("0.010"), Quantity::from(250))
    .with_restart_recovery()
}
fn deliver(private: &Server, events: &mut Vec<Value>, event: Value) {
    events.push(event.clone());
    private.wallet(event.clone());
    private.wallet(event);
}
fn net(cache: &Rc<RefCell<Cache>>) -> Decimal {
    cache
        .borrow()
        .positions_open(None, Some(&instrument_id(1)), None, None, None)
        .iter()
        .map(|p| p.quantity.as_decimal())
        .sum()
}

async fn run(case: Case) -> Value {
    let redis = Redis::start().await;
    let private = server(vec![]).await;
    *private.state.position_snapshot.lock().unwrap() = Some(vec![]);
    *private.state.match_snapshot.lock().unwrap() = Some(vec![]);
    private.state.rpc_frame.lock().unwrap().2 =
        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000;
    *private.state.sell_approved.lock().unwrap() = true;
    *private.state.market_fee.lock().unwrap() = 200;
    let public = PublicState(Arc::new(Mutex::new(vec![])));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public_port = listener.local_addr().unwrap().port();
    let public_task = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/v1/markets/42", get(super::live_node::public_market))
                .route("/ws", get(exit_socket))
                .with_state(public.clone()),
        )
        .into_future(),
    );
    let mut first = node(&private, public_port, redis.port, false);
    let cache = first.kernel().cache.clone();
    assert!(
        cache
            .borrow()
            .orders_refs(None, None, None, None, None)
            .is_empty()
    );
    let first_journal = Rc::new(RefCell::new(vec![]));
    first
        .add_strategy(strategy(first_journal.clone()).without_shutdown_cancels_for_restart_replay())
        .unwrap();
    let handle = first.handle();
    let mut events = vec![];
    let mut books = vec![];
    let prepare = async {
        barrier("first subscriptions", || {
            !public.0.lock().unwrap().is_empty()
                && !private.state.sockets.lock().unwrap().is_empty()
        })
        .await;
        books.push(book(&private, &public, "0.009", None));
        barrier("entry POST", || {
            private.state.creations.lock().unwrap().len() == 1
        })
        .await;
        let entry = private.state.creations.lock().unwrap()[0].clone();
        rest(&private, &entry);
        deliver(
            &private,
            &mut events,
            wire_event(&entry, "orderAccepted", 1, Decimal::ZERO),
        );
        barrier("entry accepted and modify rejected", || {
            callback(&first_journal, "on_order_modify_rejected")
        })
        .await;
        let gate = Arc::new(tokio::sync::Notify::new());
        *private.state.remove_gate.lock().unwrap() = Some(gate.clone());
        private.state.remove_replies.lock().unwrap().push_back((
            200,
            json!({"success":true,"removed":["12345"],"noop":[]}),
            0,
        ));
        for (size, cumulative, id) in [(67, 67, "restart-entry-1"), (133, 200, "restart-entry-2")] {
            cash(&private, -Decimal::from(size) * Decimal::new(1, 2));
            stock(&private, cumulative);
            deliver(
                &private,
                &mut events,
                settlement(&entry, 1, size, cumulative, "0.010", id, true),
            );
            barrier("entry fill", || {
                cache
                    .borrow()
                    .order(&ClientOrderId::new("EXIT-ENTRY-001"))
                    .unwrap()
                    .filled_qty()
                    .as_decimal()
                    == Decimal::from(cumulative)
            })
            .await;
        }
        barrier("entry cancel transmitted", || {
            private.state.removals.lock().unwrap().len() == 1
        })
        .await;
        books.push(book(&private, &public, "0.012", None));
        gate.notify_one();
        barrier("entry remainder canceled", || {
            cache
                .borrow()
                .order(&ClientOrderId::new("EXIT-ENTRY-001"))
                .unwrap()
                .status()
                == OrderStatus::Canceled
        })
        .await;
        *private.state.remove_gate.lock().unwrap() = None;
        reconnect_snapshot(&private, &events, &[(entry.clone(), "CANCELLED", 200)], 200);
        books.push(book(&private, &public, "0.012", None));
        barrier("take profit POST", || {
            private.state.creations.lock().unwrap().len() == 2
        })
        .await;
        let maker = private.state.creations.lock().unwrap()[1].clone();
        rest(&private, &maker);
        deliver(
            &private,
            &mut events,
            wire_event(&maker, "orderAccepted", 1, Decimal::ZERO),
        );
        barrier("take profit accepted", || {
            cache
                .borrow()
                .orders_refs(None, None, None, None, None)
                .iter()
                .any(|o| {
                    o.venue_order_id().is_some_and(|id| {
                        id.as_str() == maker["data"]["order"]["hash"].as_str().unwrap()
                    }) && o.status() == OrderStatus::Accepted
                })
        })
        .await;
        reconnect_snapshot(
            &private,
            &events,
            &[
                (entry.clone(), "CANCELLED", 200),
                (maker.clone(), "OPEN", 0),
            ],
            200,
        );
        assert_eq!(net(&cache), Decimal::from(200));
        handle.stop();
        (entry, maker)
    };
    let ((entry, maker), result) = timeout(Duration::from_secs(20), async {
        tokio::join!(prepare, async {
            let result = first.run().await;
            assert!(result.is_ok(), "first node failed: {result:?}");
            result
        })
    })
    .await
    .unwrap();
    result.unwrap();
    let maker_id = native_id(&cache, &maker);
    assert!(
        first_journal
            .borrow()
            .iter()
            .any(|r| r["type"] == "StrategyStateSaved")
    );
    if case.refused() {
        let state = if case == Case::MissingIntent {
            indexmap::IndexMap::new()
        } else {
            indexmap::IndexMap::from([("predict_exit_intent".into(),serde_json::to_vec(&json!({"version":1,"instrument":instrument_id(1),"entry":"EXIT-ENTRY-001","maker":"EXIT-ENTRY-001","taker":null,"stop":false,"stop_attempts":0})).unwrap())])
        };
        // Corrupt only fixture intent through the native persistence API.
        cache
            .borrow()
            .update_strategy_state(&StrategyId::new("PREDICT-EXIT-001"), &state)
            .unwrap();
    }
    first.dispose(); // Native Redis close drains buffered writes before a new reader opens.
    drop(first);
    public.0.lock().unwrap().clear();
    let offline_fill = match case {
        Case::PartialStop => 46,
        Case::FilledOffline => 200,
        _ => 0,
    };
    if offline_fill > 0 {
        cash(&private, Decimal::from(offline_fill) * Decimal::new(13, 3));
        stock(&private, 200 - offline_fill);
        events.push(settlement(
            &maker,
            1,
            offline_fill,
            offline_fill,
            "0.013",
            "restart-offline-profit",
            true,
        ));
    }
    reconnect_snapshot(
        &private,
        &events,
        &[
            (entry.clone(), "CANCELLED", 200),
            (
                maker.clone(),
                if offline_fill == 200 {
                    "FILLED"
                } else {
                    "OPEN"
                },
                offline_fill,
            ),
        ],
        200 - offline_fill,
    );
    let first_feedback = first_journal.borrow().clone();
    let mut second = node(
        &private,
        public_port,
        redis.port,
        case == Case::FilledOffline,
    );
    let restored_cache = second.kernel().cache.clone();
    assert!(!Rc::ptr_eq(&cache, &restored_cache));
    assert!(
        restored_cache
            .borrow()
            .orders_refs(None, None, None, None, None)
            .is_empty()
    );
    assert!(
        restored_cache
            .borrow()
            .positions_open(None, None, None, None, None)
            .is_empty()
    );
    drop(cache);
    let journal = Rc::new(RefCell::new(vec![]));
    second.add_strategy(strategy(journal.clone())).unwrap();
    let handle = second.handle();
    let wallet_count = private.state.sockets.lock().unwrap().len();
    let engine = second.kernel().exec_engine.clone();
    let driver = async {
        barrier("restart ownership decision", || {
            journal
                .borrow()
                .iter()
                .any(|r| r["type"] == "RestartAdopted" || r["type"] == "RestartRefused")
        })
        .await;
        assert_eq!(
            private.state.creations.lock().unwrap().len(),
            2,
            "restart must not create another entry or TP"
        );
        if case.refused() {
            assert!(
                journal
                    .borrow()
                    .iter()
                    .any(|r| r["type"] == "RestartRefused")
            );
            assert!(
                !journal
                    .borrow()
                    .iter()
                    .any(|r| r["type"] == "RestartAdopted")
            );
            handle.stop();
            return;
        }
        assert!(private.state.sockets.lock().unwrap().len() > wallet_count);
        assert!(
            journal
                .borrow()
                .iter()
                .any(|r| r["type"] == "StrategyStateLoaded" && r["maker"] == maker_id.to_string())
        );
        let adopted = journal
            .borrow()
            .iter()
            .find(|r| r["type"] == "RestartAdopted")
            .unwrap()
            .clone();
        assert_eq!(
            Decimal::from_str_exact(adopted["netPosition"].as_str().unwrap()).unwrap(),
            Decimal::from(200 - offline_fill)
        );
        assert_eq!(
            Decimal::from_str_exact(adopted["entryCost"].as_str().unwrap()).unwrap(),
            Decimal::from(2)
        );
        if offline_fill < 200 {
            assert_eq!(adopted["maker"], maker_id.to_string());
        } else {
            assert!(adopted["maker"].is_null());
        }
        if case == Case::PartialStop {
            let late = events.last().unwrap().clone();
            private.wallet(late.clone());
            private.wallet(late);
        }
        // A native re-query repeats history for filled orders without reapplying trades.
        let order = restored_cache.borrow().order_owned(&maker_id).unwrap();
        let reads = private.state.order_requests.lock().unwrap().len();
        second_query(&engine, &order);
        barrier("repeated native maker query", || {
            private.state.order_requests.lock().unwrap().len()
                >= reads + if offline_fill > 0 { 2 } else { 1 }
        })
        .await;
        assert_eq!(
            restored_cache
                .borrow()
                .order(&maker_id)
                .unwrap()
                .filled_qty()
                .as_decimal(),
            Decimal::from(offline_fill)
        );
        assert_eq!(net(&restored_cache), Decimal::from(200 - offline_fill));
        books.push(book(&private, &public, "0.012", None));
        if case == Case::FilledOffline {
            assert_eq!(private.state.creations.lock().unwrap().len(), 2);
            handle.stop();
            return;
        }
        let gate = Arc::new(tokio::sync::Notify::new());
        *private.state.remove_gate.lock().unwrap() = Some(gate.clone());
        private.state.remove_replies.lock().unwrap().push_back((
            200,
            json!({"success":true,"removed":["12345"],"noop":[]}),
            0,
        ));
        books.push(book(&private, &public, "0.008", None));
        barrier("strategy cancels original TP", || {
            private.state.removals.lock().unwrap().len() == 2
        })
        .await;
        assert_eq!(
            private.state.creations.lock().unwrap().len(),
            2,
            "stop must wait for TP cancellation"
        );
        assert_eq!(
            restored_cache.borrow().order(&maker_id).unwrap().status(),
            OrderStatus::PendingCancel
        );
        gate.notify_one();
        barrier("one stop POST", || {
            private.state.creations.lock().unwrap().len() == 3
        })
        .await;
        let stop = private.state.creations.lock().unwrap()[2].clone();
        let quantity = 200 - offline_fill;
        assert_eq!(stop["data"]["strategy"], "MARKET");
        assert_ne!(
            native_id(&restored_cache, &maker).to_string(),
            restored_cache
                .borrow()
                .orders_refs(None, None, None, None, None)
                .iter()
                .find(|o| o.order_type() == OrderType::Market)
                .unwrap()
                .client_order_id()
                .to_string(),
            "fresh stop ID must not overwrite the persisted TP"
        );
        assert_eq!(
            stop["data"]["order"]["makerAmount"],
            (u128::from(quantity) * 1_000_000_000_000_000_000).to_string()
        );
        assert_eq!(
            restored_cache.borrow().order(&maker_id).unwrap().status(),
            OrderStatus::Canceled
        );
        deliver(
            &private,
            &mut events,
            wire_event(&stop, "orderAccepted", 1, Decimal::ZERO),
        );
        for (size, cumulative, id) in [
            (54, 54, "restart-stop-1"),
            (quantity - 54, quantity, "restart-stop-2"),
        ] {
            cash(
                &private,
                Decimal::from(size) * Decimal::new(8, 3) * Decimal::new(98, 2),
            );
            stock(&private, quantity - cumulative);
            deliver(
                &private,
                &mut events,
                settlement(&stop, 1, size, cumulative, "0.008", id, false),
            );
            barrier("stop partial fill", || {
                restored_cache
                    .borrow()
                    .orders_refs(None, None, None, None, None)
                    .iter()
                    .any(|o| {
                        o.venue_order_id().is_some_and(|id| {
                            id.as_str() == stop["data"]["order"]["hash"].as_str().unwrap()
                        }) && o.filled_qty().as_decimal() == Decimal::from(cumulative)
                    })
            })
            .await;
        }
        barrier("restart strategy position closed", || {
            net(&restored_cache).is_zero() && callback(&journal, "on_position_closed")
        })
        .await;
        let expected_cash = Decimal::from(998)
            + Decimal::from(offline_fill) * Decimal::new(13, 3)
            + Decimal::from(quantity) * Decimal::new(8, 3) * Decimal::new(98, 2);
        barrier("fresh final reported cash", || {
            restored_cache
                .borrow()
                .account(&account_id())
                .is_some_and(|a| {
                    a.balance_total(Some(Currency::USDT()))
                        .unwrap()
                        .as_decimal()
                        == expected_cash
                        && a.balance_locked(Some(Currency::USDT())).unwrap().is_zero()
                })
        })
        .await;
        handle.stop();
    };
    let ((), result) = timeout(Duration::from_secs(20), async {
        tokio::join!(driver, second.run())
    })
    .await
    .unwrap();
    let startup_error = result.err().map(|e| e.to_string());
    if !case.refused() {
        assert!(startup_error.is_none(), "{startup_error:?}");
    }
    let feedback = journal.borrow().clone();
    if !case.refused() {
        assert!(!feedback.iter().any(
            |r| r["type"] == "MakerTakeProfitSubmitted" || r["type"] == "NativeEntrySubmitted"
        ));
        assert_eq!(
            private.state.creations.lock().unwrap().len(),
            if case == Case::FilledOffline { 2 } else { 3 }
        );
        assert!(
            restored_cache
                .borrow()
                .orders_open(None, None, None, None, None)
                .is_empty()
        );
        assert_eq!(net(&restored_cache), Decimal::ZERO);
    }
    let native_orders = restored_cache.borrow().orders_refs(None,None,None,None,None).iter()
        .map(|o| json!({"clientOrderId":o.client_order_id().to_string(),"status":o.status().to_string(),
            "venueOrderId":o.venue_order_id().map(|id|id.to_string()),"filledQuantity":o.filled_qty().to_string(),
            "tradeIds":o.trade_ids().into_iter().map(|id|id.to_string()).collect::<Vec<_>>()})).collect::<Vec<_>>();
    if !case.refused() {
        assert_eq!(
            restored_cache
                .borrow()
                .order(&maker_id)
                .unwrap()
                .filled_qty()
                .as_decimal(),
            Decimal::from(offline_fill)
        );
        assert_eq!(
            restored_cache
                .borrow()
                .order(&maker_id)
                .unwrap()
                .trade_ids()
                .len(),
            usize::from(offline_fill > 0)
        );
        assert_eq!(
            restored_cache
                .borrow()
                .orders_refs(None, None, None, None, None)
                .len(),
            if case == Case::FilledOffline { 2 } else { 3 }
        );
    }
    let result = json!({"finalNativeOrders":native_orders,"lateWalletReplays":if case==Case::PartialStop{2}else{0},"case":format!("{case:?}"),"source":"synthetic two new Rust Strategy + LiveNode lifecycles using native Redis cache and on_save/on_load; same test process, not OS crash recovery","mainnetReads":0,"mainnetWrites":0,"nativeCacheInitiallyEmpty":true,"cachePointersDifferent":true,"makerClientOrderId":maker_id.to_string(),"offlineFillShares":offline_fill,"firstFeedback":first_feedback,"nativeFeedback":feedback,"startupError":startup_error,"walletEvents":events,"orderbooks":books,"requests":private.state.creations.lock().unwrap().clone(),"removals":private.state.removals.lock().unwrap().clone(),"orderResponses":private.state.order_response_log.lock().unwrap().clone(),"orderLists":private.state.order_list_log.lock().unwrap().clone(),"matchResponses":private.state.match_response_log.lock().unwrap().clone(),"positionResponses":private.state.position_response_log.lock().unwrap().clone(),"finalNetPosition":net(&restored_cache).to_string(),"finalCash":restored_cache.borrow().account(&account_id()).unwrap().balance_total(Some(Currency::USDT())).unwrap().as_decimal().to_string(),"finalLockedCash":restored_cache.borrow().account(&account_id()).unwrap().balance_locked(Some(Currency::USDT())).unwrap().as_decimal().to_string(),"finalOpenOrders":restored_cache.borrow().orders_open(None,None,None,None,None).len()});
    second.dispose();
    drop(second);
    public_task.abort();
    redis.stop().await;
    result
}

fn second_query(engine: &Rc<RefCell<ExecutionEngine>>, order: &OrderAny) {
    engine
        .borrow()
        .get_client(&ClientId::new("PREDICT"))
        .unwrap()
        .query_order(nautilus_common::messages::execution::QueryOrder::new(
            trader_id(),
            Some(ClientId::new("PREDICT")),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            order.venue_order_id(),
            UUID4::new(),
            get_atomic_clock_realtime().get_time_ns(),
            None,
            None,
        ))
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit offline native persistence integration requires local redis-server"]
async fn native_restart_adopts_original_take_profit_then_strategy_stops() {
    run(Case::ActiveStop).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit offline native persistence integration requires local redis-server"]
async fn native_restart_recovers_offline_partial_and_full_profit_without_duplicate_exit() {
    run(Case::PartialStop).await;
    run(Case::FilledOffline).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit offline native persistence integration requires local redis-server"]
async fn native_restart_refuses_missing_intent_and_wrong_exit_role() {
    run(Case::MissingIntent).await;
    run(Case::WrongRole).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit synthetic native Redis restart recording; never overwrite historical JSON"]
async fn record_native_restart_json() {
    let mut sessions = vec![];
    for case in [
        Case::ActiveStop,
        Case::PartialStop,
        Case::FilledOffline,
        Case::MissingIntent,
        Case::WrongRole,
    ] {
        sessions.push(run(case).await);
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/private_api/native_strategy_restart_synthetic_20261016.json"),
        )
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic offline native Redis + fresh LiveNode/Cache/strategy lifecycles; not mainnet or OS crash evidence","sessions":sessions})).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit local Redis integration, no mainnet"]
async fn record_read_only_native_restart_json() {
    let session = run(Case::FilledOffline).await;
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/readonly_native_restart_synthetic_20261016.json");
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file, &json!({
        "source":"synthetic offline Rust strategy + LiveNode + native Redis startup; not mainnet",
        "restartSubmissionEnabled":false,"session":session,
    })).unwrap();
}
