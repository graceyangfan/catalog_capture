// -------------------------------------------------------------------------------------------------
// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
// See https://www.gnu.org/licenses/lgpl-3.0.html for license details.
// -------------------------------------------------------------------------------------------------
//! Actual mainnet economics with explicitly rebound loopback identities and causal timestamps.
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_live::node::config::LiveExecutionEngineConfig;
use nautilus_predict::PredictDataClientConfig;

use super::{
    exit_orders::{PublicState, barrier, exit_socket},
    live_node::strategy::{
        CalibrationInputs, CalibrationStrategy, Observations, Phase, StrategySettings,
    },
    *,
};

fn source() -> Value {
    serde_json::from_str(include_str!(
        "../fixtures/private_api/live_fill_20261016T123240Z_independent/calibration.json"
    ))
    .unwrap()
}
fn history(source: &Value) -> Vec<Value> {
    let mut rows = source["after"]["matches"].as_array().unwrap().clone();
    for row in &mut rows {
        if row["taker"]["signer"] == "0x0000000000000000000000000000000000000022" {
            row["taker"]["signer"] = json!(WALLET);
        }
        for leg in row["makers"].as_array_mut().unwrap() {
            if leg["signer"] == "0x0000000000000000000000000000000000000022" {
                leg["signer"] = json!(WALLET);
            }
        }
    }
    assert_eq!(rows.len(), 6);
    rows
}
async fn recorded_public_market() -> Response {
    let mut market = source()["after"]["matches"][0]["market"].clone();
    market["id"] = json!(42);
    for (i, token) in ["11", "22"].into_iter().enumerate() {
        market["outcomes"][i]["onChainId"] = json!(token);
    }
    json_response(json!({"success":true,"data":market}))
}
fn observation_ns(source: &Value) -> u64 {
    chrono::DateTime::parse_from_rfc3339(source["after"]["observedAt"].as_str().unwrap())
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap() as u64
        + 120_000_000_000
}

#[tokio::test]
async fn actual_flat_account_startup_scopes_history_without_loading_expired_instruments() {
    for case in [
        "bounded",
        "unbounded",
        "wide",
        "boundary",
        "foreign",
        "active",
        "overflow",
    ] {
        let source = source();
        let now = observation_ns(&source);
        let private = server(vec![]).await;
        *private.state.position_snapshot.lock().unwrap() = Some(vec![]);
        *private.state.match_snapshot.lock().unwrap() = Some(history(&source));
        *private.state.rpc_frame.lock().unwrap() = (
            source["after"]["cashWei"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap(),
            1,
            now / 1_000_000_000,
        );
        let (mut client, mut rx, cache) = submissions::setup_client(&private, now);
        let _portfolio = portfolio(&cache);
        let reported = connect_reported(&mut client, &mut rx).await;
        assert_eq!(
            reported.balances[0].total.as_decimal(),
            Decimal::from_str_exact("48.21630840").unwrap()
        );
        match case {
            "boundary" => {
                private
                    .state
                    .match_snapshot
                    .lock()
                    .unwrap()
                    .as_mut()
                    .unwrap()[0]["executedAt"] = json!(
                    chrono::DateTime::from_timestamp_nanos((now - 60_000_000_000) as i64)
                        .to_rfc3339()
                )
            }
            "foreign" => {
                let mut rows = private.state.match_snapshot.lock().unwrap();
                for row in rows.as_mut().unwrap() {
                    row["taker"]["signer"] = json!("0x9999999999999999999999999999999999999999");
                    for leg in row["makers"].as_array_mut().unwrap() {
                        leg["signer"] = json!("0x9999999999999999999999999999999999999999");
                    }
                }
            }
            "active" => private
                .state
                .open_orders
                .lock()
                .unwrap()
                .push(rest_order_data("OPEN", 1, OrderSide::Buy)),
            _ => {}
        }
        let window = match case {
            "unbounded" => None,
            "wide" => Some(60),
            "overflow" => Some(u64::MAX),
            _ => Some(1),
        };
        let result = client.generate_mass_status(window).await;
        if matches!(case, "bounded" | "unbounded" | "wide" | "boundary") {
            let mass = result.unwrap().unwrap();
            assert!(mass.reports_complete());
            assert_eq!(
                mass.lookback_start(),
                window.map(|mins| UnixNanos::from(now - mins * 60_000_000_000))
            );
            assert!(
                mass.order_reports().is_empty()
                    && mass.fill_reports().is_empty()
                    && mass.position_reports().is_empty()
            );
        } else {
            assert!(
                result.is_err(),
                "{case} cannot be reported as a successful empty account"
            );
        }
        assert!(
            cache
                .borrow()
                .orders(None, None, None, None, None)
                .is_empty()
        );
        assert!(
            cache
                .borrow()
                .positions(None, None, None, None, None)
                .is_empty()
        );
        assert!(private.state.creations.lock().unwrap().is_empty());
        client.disconnect().await.unwrap();
    }
}

fn history_at_realtime(source: &Value) -> Vec<Value> {
    let offset =
        get_atomic_clock_realtime().get_time_ns().as_u64() as i64 - observation_ns(source) as i64;
    let mut rows = history(source);
    for row in &mut rows {
        let ts = chrono::DateTime::parse_from_rfc3339(row["executedAt"].as_str().unwrap())
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap();
        row["executedAt"] = json!(chrono::DateTime::from_timestamp_nanos(ts + offset).to_rfc3339());
    }
    rows
}
async fn run_node() -> Value {
    let source = source();
    let private = server(vec![]).await;
    *private.state.position_snapshot.lock().unwrap() = Some(vec![]);
    *private.state.match_snapshot.lock().unwrap() = Some(history_at_realtime(&source));
    *private.state.rpc_frame.lock().unwrap() = (
        source["after"]["cashWei"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap(),
        1,
        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000,
    );
    let mut cfg = config(&private);
    cfg.account_state_mode = PredictAccountStateMode::Reported;
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", private.http).into(),
    ));
    assert!(cfg.submission.is_none());
    let public = PublicState(Arc::new(Mutex::new(vec![])));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/v1/markets/42", get(recorded_public_market))
                .route("/ws", get(exit_socket))
                .with_state(public.clone()),
        )
        .into_future(),
    );
    let mut live = node::build_node_with_engine(
        trader_id(),
        PredictDataClientConfig {
            api_key: KEY.into(),
            market_ids: vec![42],
            base_url_http: Some(format!("http://{address}")),
            base_url_ws: Some(format!("ws://{address}/ws")),
            http_timeout_secs: 2,
            ws_timeout_secs: 2,
            dynamic_refresh_secs: 30,
            transport_backend: TransportBackend::Tungstenite,
        },
        cfg,
        LiveExecutionEngineConfig {
            reconciliation: true,
            reconciliation_startup_delay_secs: 0.0,
            reconciliation_lookback_mins: Some(1),
            inflight_check_interval_ms: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let cache = live.kernel().cache.clone();
    let handle = live.handle();
    let observations = Rc::new(RefCell::new(Observations::default()));
    live.add_strategy(CalibrationStrategy::new(
        CalibrationInputs {
            account_id: account_id(),
            settings: StrategySettings {
                phase: Phase::Observe,
                run_secs: 5,
                stale_after_ms: 2000,
                minimum_rebound_ticks: 1,
            },
            market_id: 42,
            instrument_id: instrument_id(1),
            outcome: 1,
            quantity: Quantity::from(32),
            maximum_price: Decimal::new(4, 2),
            maximum_order_cost: Decimal::new(150, 2),
            maximum_total_cost: Decimal::new(150, 2),
            execution_enabled: false,
            fee_rate_bps: 200,
        },
        observations.clone(),
        handle.clone(),
    ))
    .unwrap();
    let driver = async {
        barrier("startup reconciled before subscriptions", || {
            !public.0.lock().unwrap().is_empty()
        })
        .await;
        let admission: Value = serde_json::from_str(include_str!(
            "../fixtures/private_api/live_fill_20261016T123240Z/admission.json"
        ))
        .unwrap();
        let mut book = admission["candidate"]["book"].clone();
        book["marketId"] = json!(42);
        book["orderCount"] = json!(0); // REST capture has no count; test envelope metadata only.
        if let Some(last) = book.get_mut("lastOrderSettled") {
            last["marketId"] = json!(42);
        }
        book["updateTimestampMs"] =
            json!(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000);
        for tx in public.0.lock().unwrap().iter() {
            tx.send(Message::Text(
                json!({"type":"M","topic":"predictOrderbook/42","data":book})
                    .to_string()
                    .into(),
            ))
            .unwrap();
        }
        barrier("actual book and reported account reach strategy", || {
            observations.borrow().snapshots > 0
                && observations.borrow().account_reports_observed > 0
        })
        .await;
        handle.stop();
    };
    timeout(Duration::from_secs(10), async {
        let (result, ()) = tokio::join!(live.run(), driver);
        result.unwrap();
    })
    .await
    .unwrap();
    task.abort();
    let cache = cache.borrow();
    assert!(cache.orders(None, None, None, None, None).is_empty());
    assert!(cache.positions(None, None, None, None, None).is_empty());
    assert_eq!(observations.borrow().submitted, 0);
    let report = json!({"source":"offline Rust LiveNode startup replay derived from retained actual mainnet JSON; not a new live observation", "realSource":"live_fill_20261016T123240Z_independent/calibration.json","identityTransform":"own account rebound; public market 10835 -> test 42; book receipt time rebound; historical economics unchanged; all historical execution times shifted by one constant to preserve ages against the Native realtime clock", "reconciliation":true,"reconciliationLookbackMins":1,"historicalMatches":6,"historicalOrdersImported":0,"snapshots":observations.borrow().snapshots,"accountReportsObserved":observations.borrow().account_reports_observed,"nativeCash":cache.account(&account_id()).unwrap().last_event().unwrap().balances[0].total.as_decimal().to_string(),"nativeOrders":0,"nativePositions":0,"submitted":0,"networkScope":"loopback only"});
    assert_eq!(report["nativeCash"], "48.21630840");
    report
}
#[tokio::test(flavor = "current_thread")]
async fn actual_history_bootstraps_native_livenode_before_read_only_strategy() {
    run_node().await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline actual-JSON-derived startup capture; create_new only"]
async fn record_actual_startup_replay_json() {
    let result = run_node().await;
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/private_api/actual_startup_native_replay_20261016_v2.json"),
        )
        .unwrap();
    serde_json::to_writer_pretty(file, &result).unwrap();
}

fn push_book(private: &Server, public: &PublicState) {
    let admission: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/live_fill_20261016T123240Z/admission.json"
    ))
    .unwrap();
    let mut book = admission["candidate"]["book"].clone();
    book["marketId"] = json!(42);
    book["orderCount"] = json!(0);
    book["updateTimestampMs"] =
        json!(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000);
    book["lastOrderSettled"]["marketId"] = json!(42);
    *private.state.execution_book.lock().unwrap() = book.clone();
    for tx in public.0.lock().unwrap().iter() {
        tx.send(Message::Text(
            json!({"type":"M","topic":"predictOrderbook/42","data":book})
                .to_string()
                .into(),
        ))
        .unwrap();
    }
}
fn wallet(private: &Server, original: &Value, body: &Value) -> Value {
    let mut event = original.clone();
    event["walletAddress"] = json!(WALLET);
    event["orderHash"] = body["data"]["order"]["hash"].clone();
    event["orderId"] = json!("12345");
    event["details"]["marketId"] = json!(42);
    event["timestamp"] = json!(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000);
    private.wallet(event.clone());
    private.wallet(event.clone());
    event
}
fn rest_state(private: &Server, body: &Value, status: &str, filled: &str) {
    let mut row = super::live_node::rest_row(body, status);
    row["isYieldBearing"] = json!(true);
    row["amountFilled"] = json!(filled);
    let hash = body["data"]["order"]["hash"].as_str().unwrap().to_owned();
    private
        .state
        .orders_by_hash
        .lock()
        .unwrap()
        .insert(hash.clone(), json!({"success":true,"data":row.clone()}));
    let mut open = private.state.open_orders.lock().unwrap();
    open.retain(|old| old["order"]["hash"] != hash);
    if status == "OPEN" {
        open.push(row);
    }
}
fn match_receipt(original: &Value, body: &Value, wallet: &Value) -> Value {
    let mut row = original.clone();
    row["market"]["id"] = json!(42);
    for o in row["market"]["outcomes"].as_array_mut().unwrap() {
        o["onChainId"] = json!(if o["indexSet"] == 1 { "11" } else { "22" });
    }
    row["taker"]["hash"] = body["data"]["order"]["hash"].clone();
    row["taker"]["signer"] = json!(WALLET);
    row["taker"]["outcome"]["onChainId"] = json!("11");
    for leg in row["makers"].as_array_mut().unwrap() {
        leg["outcome"]["onChainId"] = json!(if leg["outcome"]["indexSet"] == 1 {
            "11"
        } else {
            "22"
        });
    }
    row["executedAt"] = json!(
        chrono::DateTime::from_timestamp_millis(wallet["timestamp"].as_i64().unwrap())
            .unwrap()
            .to_rfc3339()
    );
    row
}

pub(super) async fn run_trade_flow() -> Value {
    run_trade_flow_variant(false).await
}

pub(super) async fn run_trade_flow_variant(partial_entry: bool) -> Value {
    use nautilus_model::types::Price;
    use nautilus_predict::{PredictSubmissionConfig, signing::PredictAuthSigner};

    use super::exit_orders::strategy::{ExitSettings, ExitStrategy};
    let source = source();
    let private = server(vec![]).await;
    *private.state.rpc_frame.lock().unwrap() = (
        source["before"]["cashWei"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap(),
        1,
        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000,
    );
    let mut prior = history_at_realtime(&source);
    let current = source["wallet"]["events"].as_array().unwrap();
    // The collector retains successful cancel status and canceled WS state, not its body.
    // Supply the documented removed shape with the loopback venue identity.
    if partial_entry {
        private
            .state
            .remove_replies
            .lock()
            .unwrap()
            .push_back((200, json!({"echoNoop":true}), 0));
    }
    private.state.remove_replies.lock().unwrap().push_back((
        200,
        json!({"success":true,"removed":["12345"],"noop":[]}),
        0,
    ));
    let buy_hash = current[0]["orderHash"].as_str().unwrap();
    let sell_hash = current[5]["orderHash"].as_str().unwrap();
    prior.retain(|r| r["taker"]["hash"] != buy_hash && r["taker"]["hash"] != sell_hash);
    assert_eq!(prior.len(), 4);
    *private.state.match_snapshot.lock().unwrap() = Some(prior);
    *private.state.position_snapshot.lock().unwrap() = Some(vec![]);
    *private.state.sell_approved.lock().unwrap() = true;
    *private.state.market_fee.lock().unwrap() = 200;
    *private.state.market_yield_bearing.lock().unwrap() = true;
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
            PredictAuthSigner::new(
                submissions::PUBLIC_KEY.into(),
                PredictEnvironment::BnbMainnet,
                Some(WALLET),
            )
            .unwrap(),
        ),

        market_slippage_bps: 100,
    });
    let public = PublicState(Arc::new(Mutex::new(vec![])));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/v1/markets/42", get(recorded_public_market))
                .route("/ws", get(exit_socket))
                .with_state(public.clone()),
        )
        .into_future(),
    );
    let mut live = node::build_node_with_engine(
        trader_id(),
        PredictDataClientConfig {
            api_key: KEY.into(),
            market_ids: vec![42],
            base_url_http: Some(format!("http://{address}")),
            base_url_ws: Some(format!("ws://{address}/ws")),
            http_timeout_secs: 2,
            ws_timeout_secs: 2,
            dynamic_refresh_secs: 30,
            transport_backend: TransportBackend::Tungstenite,
        },
        cfg,
        LiveExecutionEngineConfig {
            reconciliation: true,
            reconciliation_startup_delay_secs: 0.0,
            reconciliation_lookback_mins: None,
            inflight_check_interval_ms: 0,
            position_check_interval_secs: if cfg!(feature = "native-quantity-commission") {
                None
            } else {
                Some(0.05)
            },
            position_check_lookback_mins: 60,
            position_check_threshold_ms: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let cache = live.kernel().cache.clone();
    let handle = live.handle();
    let journal = Rc::new(RefCell::new(vec![]));
    let exit = Rc::new(std::cell::Cell::new(false));
    live.add_strategy(
        ExitStrategy::new(
            instrument_id(1),
            1,
            journal.clone(),
            ExitSettings::parse(include_str!(
                "../../../../examples/predict-exit-strategy.toml.example"
            ))
            .unwrap(),
            200,
        )
        .with_live_identity(42, account_id(), u64::MAX)
        .without_entry_amend()
        .with_native_entry(Price::from("0.040"), Quantity::from(32))
        .with_taker_entry()
        .with_native_quotes()
        .with_recorded_exit_signal(exit.clone()),
    )
    .unwrap();
    let mut recorded = vec![];
    let driver = async {
        barrier("native startup then custom/quote subscriptions", || {
            !public.0.lock().unwrap().is_empty()
        })
        .await;
        push_book(&private, &public);
        barrier("actual-size BUY request", || {
            private.state.creations.lock().unwrap().len() == 1
        })
        .await;
        let buy = private.state.creations.lock().unwrap()[0].clone();
        assert_eq!(buy["data"]["order"]["makerAmount"], "1280000000000000000");
        rest_state(&private, &buy, "OPEN", "0");
        recorded.push(wallet(&private, &current[0], &buy));
        recorded.push(wallet(&private, &current[1], &buy));
        rest_state(&private, &buy, "FILLED", "32000000000000000000");
        let entry_cost = if partial_entry {
            Decimal::from_str_exact("1.262999999999999999").unwrap()
        } else {
            Decimal::new(128, 2)
        };
        super::strategy_lifecycle::cash(&private, -entry_cost);
        *private.state.sell_inventory.lock().unwrap() = if partial_entry {
            "31360000000000000001"
        } else {
            "31360000000000000000"
        }
        .parse()
        .unwrap();
        let mut position = holding(1);
        position["amount"] = json!(private.state.sell_inventory.lock().unwrap().to_string());
        *private.state.position_snapshot.lock().unwrap() = Some(vec![position]);
        if partial_entry {
            for (index, size, price, value, fee, filled) in [
                (
                    1,
                    "17000000000000000099",
                    "39000000000000000",
                    "663000000000000003",
                    "340000000000000001",
                    "17.000000000000000099",
                ),
                (
                    2,
                    "14999999999999999901",
                    "40000000000000000",
                    "599999999999999996",
                    "299999999999999998",
                    "32",
                ),
            ] {
                let mut event = current[2].clone();
                event["settlementId"] = json!(format!("offline-partial-{index}"));
                event["fill"] = json!({"executedSizeWei":size,"executedPriceWei":price,"executedValueWei":value});
                event["fee"] = json!({"type":"SHARES","amountWei":fee});
                event["details"]["quantityFilled"] = json!(filled);
                recorded.push(wallet(&private, &event, &buy));
                if index == 1 {
                    barrier(
                        "first different-price partial reaches native strategy",
                        || {
                            cache
                                .borrow()
                                .orders(None, None, None, None, None)
                                .iter()
                                .any(|order| order.filled_qty().as_decimal() == Decimal::from(17))
                        },
                    )
                    .await;
                    barrier("entry removal noop awaits confirmed settlement", || {
                        !private.state.removals.lock().unwrap().is_empty()
                    })
                    .await;
                }
            }
        } else {
            let buy_event = wallet(&private, &current[2], &buy);
            recorded.push(buy_event.clone());
            let original = source["after"]["matches"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["taker"]["hash"] == buy_hash)
                .unwrap();
            private
                .state
                .match_snapshot
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .push(match_receipt(original, &buy, &buy_event));
        }
        // Position reconciliation and market delivery are independent. Under load
        // the initial book can expire before net holdings arrive; send a fresh
        // market observation after the explicit holdings checkpoint, as live data
        // would, rather than depending on the removed submit-preflight delay.
        barrier(
            "confirmed net holdings before the next market observation",
            || {
                let net: Decimal = cache
                    .borrow()
                    .positions_open(None, Some(&instrument_id(1)), None, None, None)
                    .iter()
                    .map(|position| position.quantity.as_decimal())
                    .sum();
                (net - Decimal::new(3136, 2)).abs() <= Decimal::new(1, 12)
            },
        )
        .await;
        push_book(&private, &public);
        barrier("net position then actual-size maker TP", || {
            super::exit_orders::assert_no_submission_failure(&journal);
            private.state.creations.lock().unwrap().len() == 2
        })
        .await;
        let maker = private.state.creations.lock().unwrap()[1].clone();
        assert_eq!(
            maker["data"]["order"]["makerAmount"],
            "31360000000000000000"
        );
        if !partial_entry {
            assert_eq!(maker["data"]["order"]["takerAmount"], "1348480000000000000");
        }
        rest_state(&private, &maker, "OPEN", "0");
        recorded.push(wallet(&private, &current[3], &maker));
        barrier("TP accepted", || {
            let cache = cache.borrow();
            cache
                .client_order_id(&VenueOrderId::new(
                    maker["data"]["order"]["hash"].as_str().unwrap(),
                ))
                .and_then(|id| cache.order(id))
                .is_some_and(|o| o.status() == OrderStatus::Accepted)
        })
        .await;
        exit.set(true);
        push_book(&private, &public);
        barrier("recorded exit decision cancels maker", || {
            private.state.removals.lock().unwrap().len() == if partial_entry { 2 } else { 1 }
        })
        .await;
        rest_state(&private, &maker, "CANCELLED", "0");
        recorded.push(wallet(&private, &current[4], &maker));
        barrier("one net-inventory MARKET SELL", || {
            private.state.creations.lock().unwrap().len() == 3
        })
        .await;
        let sell = private.state.creations.lock().unwrap()[2].clone();
        assert_eq!(sell["data"]["order"]["makerAmount"], "31360000000000000000");
        rest_state(&private, &sell, "OPEN", "0");
        for row in &current[5..8] {
            recorded.push(wallet(&private, row, &sell));
        }
        rest_state(&private, &sell, "FILLED", "31360000000000000000");
        super::strategy_lifecycle::cash(&private, Decimal::from_str_exact("1.1985792").unwrap());
        *private.state.sell_inventory.lock().unwrap() = U256::from(u8::from(partial_entry));
        *private.state.position_snapshot.lock().unwrap() = if partial_entry {
            let mut dust = holding(1);
            dust["amount"] = json!("1");
            Some(vec![dust])
        } else {
            Some(vec![])
        };
        if partial_entry {
            let small = U256::from(6_282_051_282_052u64);
            let full = U256::from(31_360_000_000_000_000_000u128);
            let mut cumulative = U256::ZERO;
            for (index, size) in [small, small, full - small - small].into_iter().enumerate() {
                cumulative += size;
                let mut event = current[8].clone();
                event["settlementId"] = json!(format!("offline-sell-partial-{index}"));
                event["fill"] = json!({"executedSizeWei":size.to_string(),"executedPriceWei":"39000000000000000",
                    "executedValueWei":(size*U256::from(39)/U256::from(1000)).to_string()});
                event["fee"] = json!({"type":"COLLATERAL","amountWei":(size*U256::from(780)/U256::from(1_000_000)).to_string()});
                event["details"]["quantityFilled"] = json!(
                    (Decimal::from_str_exact(&cumulative.to_string()).unwrap()
                        / Decimal::from(1_000_000_000_000_000_000u64))
                    .to_string()
                );
                recorded.push(wallet(&private, &event, &sell));
                if index == 1 {
                    barrier(
                        "small SELL fees retain Native fixed-point precision",
                        || {
                            cache
                                .borrow()
                                .orders(None, None, None, None, None)
                                .iter()
                                .any(|order| {
                                    order.order_side() == OrderSide::Sell
                                        && order.commissions().get(&Currency::USDT())
                                            == Some(
                                                &Money::from_str_exact("0.0000000098 USDT")
                                                    .unwrap(),
                                            )
                                })
                        },
                    )
                    .await;
                }
            }
        } else {
            let sell_event = wallet(&private, &current[8], &sell);
            recorded.push(sell_event.clone());
            let original = source["after"]["matches"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["taker"]["hash"] == sell_hash)
                .unwrap();
            private
                .state
                .match_snapshot
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .push(match_receipt(original, &sell, &sell_event));
        }
        let finished = timeout(Duration::from_secs(6), async {
            loop {
                if super::strategy_lifecycle::callback(&journal, "on_position_closed")
                    && cache
                        .borrow()
                        .account(&account_id())
                        .and_then(|a| a.last_event())
                        .is_some_and(|s| {
                            s.balances[0].total.as_decimal()
                                == Decimal::from_str_exact(if partial_entry {
                                    "48.23330840"
                                } else {
                                    "48.21630840"
                                })
                                .unwrap()
                        })
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            finished.is_ok(),
            "callback={}, balance={:?}, positions={:?}",
            super::strategy_lifecycle::callback(&journal, "on_position_closed"),
            cache
                .borrow()
                .account(&account_id())
                .unwrap()
                .balance_total(Some(Currency::USDT()))
                .map(|m| m.as_decimal()),
            cache
                .borrow()
                .positions(None, None, None, None, None)
                .iter()
                .map(|p| (p.side, p.quantity, p.signed_qty))
                .collect::<Vec<_>>()
        );
        handle.stop();
    };
    timeout(Duration::from_secs(12), async {
        let (result, ()) = tokio::join!(live.run(), driver);
        result.unwrap();
    })
    .await
    .unwrap();
    task.abort();
    let cache = cache.borrow();
    assert_eq!(private.state.creations.lock().unwrap().len(), 3);
    assert!(
        cache
            .positions_open(None, None, None, None, None)
            .is_empty()
    );
    let events = journal.borrow();
    let final_position = events
        .iter()
        .rev()
        .find(|r| r["type"] == "NativePositionApplied" && r["quantity"] == "0")
        .unwrap();
    let expected_pnl = if partial_entry {
        "-0.06442080 USDT"
    } else {
        "-0.08142080 USDT"
    };
    if partial_entry {
        // Native Position rounds price PnL per fill at the currency scale. Exact
        // subcurrency fees must not be rounded away to force the old PnL string.
        let actual =
            Money::from_str_exact(final_position["realizedPnl"].as_str().unwrap()).unwrap();
        let expected = Money::from_str_exact(expected_pnl).unwrap();
        assert!((actual.as_decimal() - expected.as_decimal()).abs() <= Decimal::new(1, 8));
    } else {
        assert_eq!(final_position["realizedPnl"], expected_pnl);
    }
    assert_eq!(
        events
            .iter()
            .filter(|r| r["callback"] == "on_position_closed")
            .count(),
        1
    );
    let mut result = json!({"source":"offline Rust Strategy + LiveNode replay derived from actual recorded economic facts; no real order writes","primaryEvidence":"live_fill_20261016T123240Z_independent/calibration.json","transforms":["account/token/market identifiers and signed hashes rebound to loopback signer","event times rebound to causal receipt times","recorded exit decision injected after TP acceptance, without waiting wall-clock hold duration","REST book wrapped in WS test envelope; orderCount unavailable -> 0","historical match times shifted together, preserving ages"],"marketFlags":{"isNegRisk":false,"isYieldBearing":true},"reconciliation":true,"reconciliationLookbackMins":null,"positionCheckLookbackMins":60,"requests":private.state.creations.lock().unwrap().clone(),"walletEvents":recorded,"nativeFeedback":*events,"finalCash":"48.21630840","cycleRealizedPnl":"-0.08142080 USDT","netSharesBeforeSell":"31.36","finalOpenOrders":cache.orders_open(None,None,None,None,None).len(),"finalPositions":0});
    if cfg!(feature = "native-quantity-commission") {
        let fills = events
            .iter()
            .filter(|r| r["callback"] == "on_order_filled")
            .collect::<Vec<_>>();
        assert_eq!(
            fills.len(),
            if partial_entry { 5 } else { 2 },
            "no synthetic fee fills"
        );
        assert_eq!(
            Decimal::from_str_exact(
                fills[usize::from(partial_entry)]["filledQuantity"]
                    .as_str()
                    .unwrap()
            )
            .unwrap(),
            Decimal::from(32)
        );
        assert_eq!(
            Decimal::from_str_exact(
                fills[usize::from(partial_entry)]["cachedNetQuantity"]
                    .as_str()
                    .unwrap()
            )
            .unwrap(),
            Decimal::new(3136, 2)
        );
        let native = cache
            .orders(None, None, None, None, None)
            .into_iter()
            .flat_map(|o| {
                o.events()
                    .into_iter()
                    .filter_map(|e| match e {
                        nautilus_model::events::OrderEventAny::Filled(f) => Some(f.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(native.len(), if partial_entry { 5 } else { 2 });
        assert_eq!(
            native
                .iter()
                .filter_map(|fill| fill.quantity_commission)
                .sum::<Decimal>(),
            if partial_entry {
                Decimal::from_str_exact("0.639999999999999999").unwrap()
            } else {
                Decimal::new(64, 2)
            }
        );

        result["quantityCommissionPrototype"] = json!(true);
        result["cycleRealizedPnl"] = final_position["realizedPnl"].clone();
        result["pnlConvention"] = json!(
            "Native inventory cost and quote fees are included in realized PnL; cash flow is independently verified"
        );
        result["periodicPositionChecks"] = Value::Null;
        let currency_fees = native
            .iter()
            .filter_map(|fill| fill.commission)
            .map(|fee| fee.as_decimal())
            .sum::<Decimal>();
        assert_eq!(
            currency_fees,
            Decimal::from_str_exact("0.02446080").unwrap()
        );
        result["nativeCurrencyCommissions"] = json!(currency_fees.to_string());
        result["nativeFills"] = json!(native);
        result["feeSummaries"] = json!(cache.positions(None,None,None,None,None).iter().map(|p| {
            let totals = p.quantity_commission_total.unwrap();
            json!({"fees":totals,"quantityResidual":super::native_fee_paths::inventory_fee_residual(totals, p.size_precision),
                "realizedPnl":p.realized_pnl,"inventoryCost":p.quantity_commission_cost.unwrap().to_string()})
        }).collect::<Vec<_>>());
    }
    if partial_entry {
        result["source"] = json!(
            "synthetic different-price partial-entry Rust Strategy + LiveNode BUY/maker-TP/cancel/SELL; not actual exchange trades"
        );
        result["syntheticPartialEntry"] = json!(true);
        result["syntheticPartialExit"] = json!(true);
        result["rawCollateralFeesWei"] = json!("24460799999999999");
        result["rawMinusNativeCollateralFee"] = json!("-0.000000000000000001");
        result["duplicateWalletPackets"] = json!(true);
        result["finalCash"] = json!("48.23330840");
        result["cycleRealizedPnl"] = final_position["realizedPnl"].clone();
        result["venueResidualSharesWei"] = json!("1");
        result["matchesCoverage"] = json!(
            "synthetic entry and exit partials tested through wallet WS; complete REST matches replay covered by separate LiveNode cases"
        );
    }
    assert_eq!(result["finalOpenOrders"], 0);
    result["cancelRestResponses"] = json!(
        "documented removed fixture shape rebound to recorded successful HTTP cancel status and WS terminal state"
    );
    result["cancelRequests"] = json!(*private.state.removals.lock().unwrap());
    assert_eq!(
        private.state.removals.lock().unwrap().len(),
        if partial_entry { 2 } else { 1 }
    );
    result
}
#[tokio::test(flavor = "current_thread")]
async fn actual_buy_tp_cancel_market_sell_replay_updates_orders_positions_and_account() {
    run_trade_flow().await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "offline actual-economic-facts Rust Strategy + LiveNode capture; create_new only"]
async fn record_actual_trade_flow_json() {
    let value = run_trade_flow().await;
    let file =
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
                "tests/fixtures/private_api/actual_trade_flow_native_replay_20261017_v3.json",
            ))
            .unwrap();
    serde_json::to_writer_pretty(file, &value).unwrap();
}

#[cfg(feature = "native-quantity-commission")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit recorded-economics Native quantity-fee prototype capture, create_new only"]
async fn record_quantity_commission_trade_flow_json() {
    let value = run_trade_flow().await;
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/native_quantity_commission_recorded_flow_20261017.json");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file, &value).unwrap();
}

#[cfg(feature = "native-quantity-commission")]
#[tokio::test(flavor = "current_thread")]
async fn partial_prices_entry_tp_cancel_sell_updates_native_strategy_and_account() {
    run_trade_flow_variant(true).await;
}

#[cfg(feature = "native-quantity-commission")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit synthetic partial-entry and exit Rust Strategy + LiveNode JSON capture"]
async fn record_partial_prices_entry_tp_cancel_sell_json() {
    let result = run_trade_flow_variant(true).await;
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/native_quantity_commission_quantity_only_partial_exit_20261017.json");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file, &result).unwrap();
}

#[cfg(feature = "native-quantity-commission")]
fn recorded_settlement_truth() -> Value {
    let source = source();
    let scale = U256::from(1_000_000_000_000_000_000u64);
    let integer = |v: &Value| v.as_str().unwrap().parse::<U256>().unwrap();
    let decimal = |v: U256| {
        Decimal::from_str_exact(&v.to_string()).unwrap()
            / Decimal::from(1_000_000_000_000_000_000u64)
    };
    let rows = source["after"]["matches"].as_array().unwrap();
    let checks = source["comparisons"].as_array().unwrap();
    assert_eq!(checks.len(), 6);
    for check in checks {
        let row = rows
            .iter()
            .find(|r| r["settlementId"] == check["settlementId"])
            .unwrap();
        assert_eq!(row["taker"]["hash"], check["orderHash"]);
        assert_eq!(row["amountFilled"], check["grossWei"]);
        assert_eq!(row["taker"]["fee"]["amount"], check["feeWei"]);
        assert_eq!(row["taker"]["fee"]["type"], check["feeType"]);
        let gross = integer(&check["grossWei"]);
        let fee = integer(&check["feeWei"]);
        let price = integer(&row["priceExecuted"]);
        assert_eq!(gross * price / scale, integer(&check["collateralWei"]));
        let bps = U256::from(row["market"]["feeRateBps"].as_u64().unwrap());
        let denominator = if check["feeType"] == "SHARES" {
            price
        } else {
            scale
        };
        assert_eq!(
            fee,
            gross * bps * price.min(scale - price) / (U256::from(10_000) * denominator)
        );
        let net = Decimal::from_str_exact(check["netTransferWei"].as_str().unwrap()).unwrap();
        let expected = if check["side"] == "BUY" {
            decimal(gross - fee)
        } else {
            -decimal(gross)
        };
        assert_eq!(net / Decimal::from(1_000_000_000_000_000_000u64), expected);
    }
    let settled = source["wallet"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["type"] == "orderTransactionSuccess")
        .collect::<Vec<_>>();
    assert_eq!(settled.len(), 2);
    let mut cash = Decimal::ZERO;
    for event in &settled {
        let check = checks
            .iter()
            .find(|c| c["settlementId"] == event["settlementId"])
            .unwrap();
        let row = rows
            .iter()
            .find(|r| r["settlementId"] == event["settlementId"])
            .unwrap();
        assert_eq!(event["orderHash"], check["orderHash"]);
        assert_eq!(event["fill"]["executedSizeWei"], check["grossWei"]);
        assert_eq!(event["fill"]["executedPriceWei"], row["priceExecuted"]);
        assert_eq!(event["fill"]["executedValueWei"], check["collateralWei"]);
        assert_eq!(event["fee"]["amountWei"], check["feeWei"]);
        let value = decimal(integer(&check["collateralWei"]));
        cash += if check["side"] == "BUY" {
            -value
        } else {
            value - decimal(integer(&check["feeWei"]))
        };
    }
    let before = decimal(integer(&source["before"]["cashWei"]));
    let after = decimal(integer(&source["after"]["cashWei"]));
    assert_eq!(cash, after - before);
    assert_eq!(cash, Decimal::from_str_exact("-0.08142080").unwrap());
    json!({"source":"previous mainnet JSON only; no network calls; chain comparisons are retained collector evidence, not re-fetched receipts",
        "primary":"live_fill_20261016T123240Z_independent/calibration.json",
        "matchedRestChainComparisons":checks.len(),"currentWalletSettlements":settled,
        "cashBefore":before.to_string(),"cashAfter":after.to_string(),"cashPnl":cash.to_string(),
        "buyGross":"32","buySharesFee":"0.64","buyNet":"31.36","buyPrice":"0.04",
        "sellGross":"31.36","sellPrice":"0.039","sellCollateralFee":"0.02446080",
        "nativeQuoteRealizedPnl":"-0.08142080 USDT","inventoryCostWrittenOff":"0.0256"})
}

#[cfg(feature = "native-quantity-commission")]
async fn recorded_lifecycle_audit() -> Value {
    let truth = recorded_settlement_truth();
    let actual = run_trade_flow().await;
    let fills = actual["nativeFills"].as_array().unwrap();
    assert_eq!(fills.len(), 2, "no historical or duplicate synthetic fills");
    let buy: nautilus_model::events::OrderFilled =
        serde_json::from_value(fills[0].clone()).unwrap();
    let sell: nautilus_model::events::OrderFilled =
        serde_json::from_value(fills[1].clone()).unwrap();
    assert_eq!(buy.last_qty.as_decimal(), Decimal::from(32));
    assert_eq!(buy.last_px.as_decimal(), Decimal::new(4, 2));
    assert_eq!(buy.quantity_commission, Some(Decimal::new(64, 2)));
    assert!(buy.commission.unwrap().is_zero());
    assert_eq!(sell.last_qty.as_decimal(), Decimal::new(3136, 2));
    assert_eq!(sell.last_px.as_decimal(), Decimal::new(39, 3));
    assert_eq!(sell.commission, Some(Money::from("0.02446080 USDT")));
    assert!(sell.quantity_commission.is_none());
    assert_eq!(actual["cycleRealizedPnl"], truth["nativeQuoteRealizedPnl"]);
    let raw_cash = Decimal::from_str_exact(truth["cashAfter"].as_str().unwrap()).unwrap();
    let native_cash = Decimal::from_str_exact(actual["finalCash"].as_str().unwrap()).unwrap();
    assert_eq!(
        native_cash,
        Money::from_decimal(raw_cash, Currency::USDT())
            .unwrap()
            .as_decimal()
    );
    assert_eq!(raw_cash - native_cash, Decimal::new(1, 18));
    assert_eq!(actual["finalPositions"], 0);
    assert_eq!(actual["finalOpenOrders"], 0);
    let host_cash =
        super::fill_session::cycle_cash_flow(actual["nativeFeedback"].as_array().unwrap()).unwrap();
    assert_eq!(
        host_cash,
        Decimal::from_str_exact(truth["cashPnl"].as_str().unwrap()).unwrap()
    );
    // With this single-price BUY the removed inventory basis is fee * entry price.
    // Do not generalize that conversion to mixed-price accumulation.
    let position_pnl = actual["feeSummaries"][0]["realizedPnl"]
        .as_str()
        .unwrap()
        .parse::<Money>()
        .unwrap();
    let removed_basis = buy.quantity_commission.unwrap() * buy.last_px.as_decimal();
    assert_eq!(removed_basis, Decimal::new(256, 4));
    assert_eq!(position_pnl.as_decimal(), host_cash);
    assert_eq!(
        Decimal::from_str_exact(actual["feeSummaries"][0]["inventoryCost"].as_str().unwrap())
            .unwrap(),
        removed_basis
    );
    let settled = super::fill_session::SessionObservation {
        entry_closed: buy.last_qty.as_decimal() == Decimal::from(32),
        entry_filled: buy.last_qty.as_decimal() > Decimal::ZERO,
        entry_window_elapsed: true,
        has_entry: true,
        flat: actual["finalPositions"] == 0,
        orders_terminal: actual["finalOpenOrders"] == 0,
        cash_current: native_cash
            == Money::from_decimal(raw_cash, Currency::USDT())
                .unwrap()
                .as_decimal(),
    };
    assert!(settled.completed());
    assert!(
        !super::fill_session::SessionObservation {
            cash_current: false,
            ..settled
        }
        .completed()
    );
    let profit = super::exit_orders::run_exit_case(true, false, false, false, false).await;
    let feedback = profit["nativeFeedback"].as_array().unwrap();
    assert_eq!(
        super::fill_session::cycle_cash_flow(feedback).unwrap(),
        Decimal::new(548, 3)
    );
    assert!(
        feedback
            .iter()
            .any(|r| r["type"] == "MakerTakeProfitSubmitted")
    );
    assert!(!feedback.iter().any(|r| r["type"] == "StopTriggered"
        || r["type"] == "TakerStopSubmitted"
        || r["type"] == "EntryHoldingDeadline"));
    assert!(
        feedback
            .iter()
            .any(|r| r["callback"] == "on_position_closed")
    );
    json!({"source":"previous real mainnet economic facts replayed through Rust Strategy + LiveNode; profitable exit is a separate synthetic scenario",
        "mainnetWrites":0,"privateReads":0,"recordedTruth":truth,"recordedLifecycle":actual,
        "rawMinusNativeCash":(raw_cash-native_cash).to_string(),
        "hostCashFlowUSDT":host_cash.to_string(),
        "pnlComparison":{"nativeQuoteRealized":position_pnl,"cashProfit":host_cash.to_string(),
            "removedInventoryBasis":removed_basis.to_string(),"nativeMinusCash":(position_pnl.as_decimal()-host_cash).to_string()},
        "hostCompletion":{"flatAndCashCurrent":true,"staleCashAccepted":false,"holdingDeadline":null},
        "expiredEntryDeadlineMakerProfitScenario":profit,
        "limits":["identities/timestamps rebound to loopback","historical exit decision injected","maker profit scenario is not actual market profit","legacy bounded mainnet supervisor is not approved for indefinite holding"]})
}

#[cfg(feature = "native-quantity-commission")]
#[tokio::test(flavor = "current_thread")]
async fn recorded_wallet_rest_chain_economics_match_native_lifecycle_and_maker_profit() {
    recorded_lifecycle_audit().await;
}

#[cfg(feature = "native-quantity-commission")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit previous-mainnet JSON and loopback lifecycle audit; no network orders"]
async fn record_recorded_lifecycle_fee_profit_json() {
    let audit = recorded_lifecycle_audit().await;
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../data/predict-validation")
        .join(UUID4::new().to_string());
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("recorded_lifecycle_fee_profit_audit.json");
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .unwrap();
    serde_json::to_writer_pretty(file, &audit).unwrap();
    eprintln!(
        "Saved recorded-economics lifecycle audit: {}",
        path.display()
    );
}

#[cfg(feature = "native-quantity-commission")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline continuous-host lifecycle JSON; no mainnet access"]
async fn record_continuous_host_lifecycle_json() {
    let mut audit = recorded_lifecycle_audit().await;
    audit["limits"] = json!([
        "economic facts from retained actual recording; identities and time rebound",
        "profitable exit scenario is synthetic",
        "no real continuous session has run; JWT expiry/reconnect remain live evidence gaps"
    ]);
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/continuous_host_recorded_lifecycle_20261017.json");
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file, &audit).unwrap();
}
