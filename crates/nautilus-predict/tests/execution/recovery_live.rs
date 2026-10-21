// Copyright (C) 2026 yfclark and contributors. Licensed under LGPL-3.0.
//! Saved-JSON replay of uncertain submissions and read-only stream-gap reconciliation.
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_live::node::config::LiveExecutionEngineConfig;
use nautilus_model::identifiers::{ClientOrderId, StrategyId};
use nautilus_predict::{PredictDataClientConfig, PredictSubmissionConfig};

use super::{
    exit_orders::{PublicState, barrier, exit_socket},
    query_recovery::strategy::QueryStrategy,
    *,
};

fn path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/matches_gap_native_feedback_synthetic_20261015.json")
}
fn page(private: &Server, body: &Value, rows: Vec<Value>, full: bool) {
    let mut row = rest_order_data(if full { "FILLED" } else { "OPEN" }, 1, OrderSide::Buy);
    row["order"] = body["data"]["order"].clone();
    row["strategy"] = body["data"]["strategy"].clone();
    row["amountFilled"] = json!(if full {
        "100000000000000000000"
    } else {
        "33000000000000000005"
    });
    private.state.open_orders.lock().unwrap().clear();
    if !full {
        private.state.open_orders.lock().unwrap().push(row.clone());
    }
    private.state.orders.lock().unwrap().extend([
        json!({"success":true,"data":row.clone()}),
        json!({"success":true,"data":row.clone()}),
    ]);
    *private.state.order_snapshot.lock().unwrap() =
        Some(json!({"success":true,"data":row.clone()}));
    *private.state.match_snapshot.lock().unwrap() = Some(rows);
}
fn snapshot(private: &Server, net: &str) {
    let mut row = holding(1);
    row["amount"] = json!(net);
    *private.state.position_snapshot.lock().unwrap() = Some(vec![row]);
}
fn quantity(cache: &Rc<RefCell<Cache>>) -> Decimal {
    cache
        .borrow()
        .positions_open(
            None,
            Some(&instrument_id(1)),
            None,
            Some(&account_id()),
            None,
        )
        .iter()
        .map(|p| p.quantity.as_decimal())
        .sum()
}
#[tokio::test(flavor = "current_thread")]
async fn native_strategy_recovers_unknown_partial_and_filled_order_across_wallet_gap() {
    run(false, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn imported_taker_history_replays_share_fees_to_net_position_without_double_cash_debit() {
    run(true, true).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit loopback synthetic capture only, never overwrite historical JSON"]
async fn record_matches_gap_json() {
    let sessions = vec![run(false, false).await, run(true, false).await];
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path())
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic offline Rust Strategy + LiveNode; not mainnet trades; imported MARKET BUY is not an enabled submit path","sessions":sessions})).unwrap();
}

async fn run(shares: bool, from_saved: bool) -> Value {
    run_case(shares, from_saved, false).await
}

async fn run_case(shares: bool, from_saved: bool, resume: bool) -> Value {
    run_mode(shares, from_saved, resume, 0).await
}
async fn run_mode(shares: bool, from_saved: bool, resume: bool, native: u8) -> Value {
    run_mode_inner(shares, from_saved, resume, native, false).await
}
async fn run_mode_inner(
    shares: bool,
    from_saved: bool,
    resume: bool,
    native: u8,
    read_only: bool,
) -> Value {
    assert!(!read_only || shares && !resume);
    let saved = if from_saved {
        let f: Value = serde_json::from_slice(&std::fs::read(path()).unwrap()).unwrap();
        Some(
            f["sessions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|s| s["sharesFee"] == shares)
                .unwrap()
                .clone(),
        )
    } else {
        None
    };
    let private = server(vec![]).await;
    *private.state.position_snapshot.lock().unwrap() = Some(vec![]);
    *private.state.match_snapshot.lock().unwrap() = Some(vec![]);
    if native == 3 {
        *private.state.order_snapshot.lock().unwrap() =
            Some(json!({"success":false,"message":"synthetic venue unavailable"}));
        private.state.order_replies.lock().unwrap().push_back((
            503,
            json!({"success":false,"message":"synthetic venue unavailable"}),
        ));
    }
    private
        .state
        .create_replies
        .lock()
        .unwrap()
        .push_back((500, 0, vec![]));
    let signer = Arc::new(
        nautilus_predict::signing::PredictAuthSigner::new(
            submissions::PUBLIC_KEY.into(),
            PredictEnvironment::BnbMainnet,
            Some(WALLET),
        )
        .unwrap(),
    );
    let mut cfg = config(&private);
    cfg.account_state_mode = PredictAccountStateMode::Reported;
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", private.http).into(),
    ));
    cfg.account_refresh_interval_secs = 1;
    cfg.reconnect_reconciliation_interval_secs = (resume && native > 0).then_some(1);
    cfg.orderbook_removal_enabled = resume;
    cfg.submission = (!read_only).then(|| PredictSubmissionConfig {
        signer: signer.clone(),

        market_slippage_bps: 100,
    });
    private.state.rpc_frame.lock().unwrap().2 =
        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000;
    let public = PublicState(Arc::new(Mutex::new(vec![])));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
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
    let mut live = node::build_node_with_engine(
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
        LiveExecutionEngineConfig {
            reconciliation: native > 0,
            reconciliation_startup_delay_secs: 0.0,
            inflight_check_interval_ms: if native > 0 { 20 } else { 0 },
            inflight_check_threshold_ms: 0,
            inflight_check_retries: if native == 3 {
                2
            } else {
                LiveExecutionEngineConfig::default().inflight_check_retries
            },
            open_check_interval_secs: (native > 0 && native != 3).then_some(0.05),
            open_check_threshold_ms: 0,
            open_check_lookback_mins: None,
            open_check_open_only: false,
            open_check_missing_retries: 1,
            single_order_query_delay_ms: 0,
            position_check_interval_secs: (shares || native == 2).then_some(0.05),
            position_check_threshold_ms: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let cache = live.kernel().cache.clone();
    let engine = live.kernel().exec_engine.clone();
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let imported = if shares {
        let mut order = OrderTestBuilder::new(OrderType::Market)
            .trader_id(trader_id())
            .strategy_id(StrategyId::new("PREDICT-QUERY-001"))
            .client_order_id(ClientOrderId::new("IMPORTED-TAKER-001"))
            .instrument_id(instrument_id(1))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100))
            .build();
        let mut fields: nautilus_predict::http::order_models::PredictOrder =
            serde_json::from_value(rest_order_data("OPEN", 1, OrderSide::Buy)["order"].clone())
                .unwrap();
        fields.fee_rate_bps = U256::from(200);
        fields.expiration =
            U256::from(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000 + 300);
        let signed = nautilus_predict::signing::PredictOrderSigner::new(signer)
            .sign_order(
                &fields,
                nautilus_predict::common::contracts::PredictExchangeType::Standard,
            )
            .unwrap();
        let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
        let now = get_atomic_clock_realtime().get_time_ns();
        order
            .apply(factory.generate_order_submitted(&order, now))
            .unwrap();
        order
            .apply(factory.generate_order_accepted(
                &order,
                VenueOrderId::new(format!("{:#x}", signed.hash())),
                now,
                now,
            ))
            .unwrap();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();
        Some((
            order,
            json!({"data":{"strategy":"MARKET","order":serde_json::to_value(signed.contract_order().unwrap()).unwrap()}}),
        ))
    } else {
        None
    };
    let ready = Rc::new(std::cell::Cell::new(false));
    let cash_query = Rc::new(std::cell::Cell::new(false));
    let resume_submit = Rc::new(std::cell::Cell::new(false));
    let journal = Rc::new(RefCell::new(vec![]));
    let diagnostics = Rc::new(RefCell::new(Vec::new()));
    let diagnostic_handler = nautilus_common::msgbus::ShareableMessageHandler::from_typed({
        let diagnostics = diagnostics.clone();
        move |event: &nautilus_live::execution::submission::SubmissionRecoveryExhausted| {
            diagnostics.borrow_mut().push(json!({"clientOrderId":event.client_order_id.to_string(),"source":format!("{:?}",event.source),"retryCount":event.retry_count}));
        }
    });
    if native == 3 {
        msgbus::subscribe_any(
            MessagingSwitchboard::submission_recovery_exhausted_topic().into(),
            diagnostic_handler.clone(),
            None,
        );
    }
    live.add_strategy(
        QueryStrategy::new(instrument_id(1), ready.clone(), journal.clone())
            .recover_fills(imported.as_ref().map(|(o, _)| o.client_order_id()))
            .recovery_commands(cash_query.clone(), resume_submit.clone()),
    )
    .unwrap();
    let handle = live.handle();
    let driver = async {
        barrier("subscribed streams", || {
            !public.0.lock().unwrap().is_empty()
                && !private.state.sockets.lock().unwrap().is_empty()
        })
        .await;
        let body = if let Some((_, body)) = &imported {
            body.clone()
        } else {
            for tx in public.0.lock().unwrap().iter() {
                tx.send(Message::Text(json!({"type":"M","topic":"predictOrderbook/42","data":{"marketId":42,"updateTimestampMs":get_atomic_clock_realtime().get_time_ns().as_u64()/1_000_000,"orderCount":2,"bids":[["0.630","1000"]],"asks":[["0.640","1000"]]}}).to_string().into())).unwrap();
            }
            barrier("native unknown POST", || {
                private.state.creations.lock().unwrap().len() == 1
            })
            .await;
            private.state.creations.lock().unwrap()[0].clone()
        };
        let id = if let Some((order, _)) = &imported {
            order.client_order_id()
        } else {
            cache.borrow().orders(None, None, None, None, None)[0].client_order_id()
        };
        if native == 3 {
            barrier("native submission recovery exhausted", || {
                !diagnostics.borrow().is_empty()
            })
            .await;
            assert_eq!(
                cache.borrow().order(&id).unwrap().status(),
                OrderStatus::Submitted,
                "unknown venue outcome must not become a synthetic timeout rejection"
            );
            assert_eq!(private.state.creations.lock().unwrap().len(), 1);
            journal.borrow_mut().push(json!({"type":"NativeRecoveryRetained","status":cache.borrow().order(&id).unwrap().status().to_string(),"simulatedSubmitPosts":1}));
            assert!(!journal.borrow().iter().any(|r| r["type"] == "Rejected"));
        }
        let timestamp = chrono::DateTime::from_timestamp_nanos(
            get_atomic_clock_realtime().get_time_ns().as_u64() as i64,
        )
        .to_rfc3339();
        let mut rows = Vec::new();
        for (index, amount, fee) in [
            (1, "33000000000000000005", "660000000000000000"),
            (2, "66999999999999999995", "1340000000000000000"),
        ] {
            let mut row = super::matches::match_row(1);
            row["settlementId"] = json!(format!("gap-settlement-{index}"));
            row["executedAt"] = json!(timestamp);
            row["transactionHash"] = json!(if index == 1 {
                format!("0x{}", "33".repeat(32))
            } else {
                format!("0x{}", "44".repeat(32))
            });
            row["amountFilled"] = json!(amount);
            for outcome in row["market"]["outcomes"].as_array_mut().unwrap() {
                outcome["status"] = Value::Null;
            }
            row["taker"]["outcome"]["status"] = Value::Null;
            row["makers"][0]["outcome"]["status"] = Value::Null;
            row["taker"]["hash"] = body["data"]["order"]["hash"].clone();
            row["taker"]["amount"] = json!(amount);
            row["taker"]["fee"] = json!({"type":"SHARES","amount":if shares{fee}else{"0"}});
            if !shares {
                row["makers"][0] = row["taker"].clone();
                row["taker"]["signer"] = json!("0x2222222222222222222222222222222222222222");
                row["taker"]["hash"] = json!(format!("0x{}", "22".repeat(32)));
                row["taker"]["outcome"] = holding(2)["outcome"].clone();
                row["taker"]["outcome"]["status"] = Value::Null;
                row["taker"]["price"] = json!("380000000000000000");
                row["priceExecuted"] = json!("380000000000000000");
            }
            if let Some(saved) = &saved {
                let mut captured = saved["matches"][index - 1].clone();
                captured["executedAt"] = json!(timestamp);
                for leg in std::iter::once(&mut captured["taker"]) {
                    if leg["signer"] == WALLET {
                        leg["hash"] = body["data"]["order"]["hash"].clone();
                    }
                }
                for leg in captured["makers"].as_array_mut().unwrap() {
                    if leg["signer"] == WALLET {
                        leg["hash"] = body["data"]["order"]["hash"].clone();
                    }
                }
                assert_eq!(captured, row);
                row = captured;
            }
            rows.push(row);
        }
        page(
            &private,
            &body,
            vec![rows[0].clone(), rows[0].clone()],
            false,
        );
        *private.state.rpc_frame.lock().unwrap() = (
            979_539_999_999_999_999_996,
            2,
            get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000 + 1,
        );
        if native > 1 {
            snapshot(&private, "33000000000000000005");
        }
        if native == 0 || native == 3 {
            ready.set(true);
        }
        barrier("native recovered partial fill", || {
            cache.borrow().order(&id).unwrap().filled_qty().as_decimal() == Decimal::from(33)
        })
        .await;
        if shares {
            snapshot(&private, "32340000000000000005");
            barrier("partial net position", || {
                (quantity(&cache) - Decimal::new(3234, 2)).abs() <= Decimal::new(1, 12)
            })
            .await;
        }
        // Replaying the same complete history cannot apply this trade twice.
        page(
            &private,
            &body,
            vec![rows[0].clone(), rows[0].clone()],
            false,
        );
        if native == 0 || native == 3 {
            ready.set(true);
        }
        barrier("second targeted query", || {
            private.state.order_requests.lock().unwrap().len() >= 4
        })
        .await;
        let wallet_connections = private.state.sockets.lock().unwrap().len();
        // A real WebSocket close exercises the transport's epoch/resubscription boundary.
        private
            .state
            .sockets
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .send(Message::Close(None))
            .unwrap();
        barrier("wallet gap stops write readiness", || {
            !engine
                .borrow()
                .get_client(&ClientId::new("PREDICT"))
                .unwrap()
                .is_connected()
        })
        .await;
        // Final fills happened during the gap; reads remain available while submissions stay blocked.
        page(
            &private,
            &body,
            vec![rows[1].clone(), rows[0].clone(), rows[1].clone()],
            true,
        );
        *private.state.rpc_frame.lock().unwrap() = (
            938_000_000_000_000_000_000,
            3,
            get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000 + 1,
        );
        if shares {
            snapshot(&private, "98000000000000000000");
        } else if native > 1 {
            snapshot(&private, "100000000000000000000");
        }
        if native == 0 || native == 3 {
            ready.set(true);
        }
        barrier("terminal history fills", || {
            cache.borrow().order(&id).unwrap().status() == OrderStatus::Filled
        })
        .await;
        barrier("net holdings and cash", || {
            (quantity(&cache) - Decimal::from(if shares { 98 } else { 100 })).abs()
                <= Decimal::new(1, 12)
                && cache
                    .borrow()
                    .account_owned(&account_id())
                    .unwrap()
                    .balance_total(Some(Currency::USDT()))
                    .unwrap()
                    .as_decimal()
                    == Decimal::from(938)
        })
        .await;
        let callbacks = journal
            .borrow()
            .iter()
            .filter(|r| r["callback"] == "on_order_filled" && r["clientOrderId"] == id.to_string())
            .count();
        assert_eq!(callbacks, 2);
        assert_eq!(
            private.state.creations.lock().unwrap().len(),
            usize::from(!shares)
        );
        assert!(private.state.open_orders.lock().unwrap().is_empty());
        assert_eq!(
            cache.borrow().order(&id).unwrap().filled_qty().as_decimal(),
            Decimal::from(100)
        );
        assert_eq!(
            cache
                .borrow()
                .account_owned(&account_id())
                .unwrap()
                .balance_locked(Some(Currency::USDT()))
                .unwrap()
                .as_decimal(),
            Decimal::ZERO
        );
        barrier("strategy observes final reported cash snapshot", || {
            journal.borrow().iter().any(|r| {
                r["type"] == "AccountSnapshot"
                    && r["balances"][0]["total"]
                        .as_str()
                        .and_then(|s| Decimal::from_str_exact(s).ok())
                        == Some(Decimal::from(938))
            })
        })
        .await;
        let query = cache.borrow().order_owned(&id).unwrap();
        assert!(
            engine
                .borrow()
                .get_client(&ClientId::new("PREDICT"))
                .unwrap()
                .submit_order(submissions::submit(&query))
                .is_err()
        );
        let mut resumed_request = Value::Null;
        if resume {
            // Native checks recover orders; the adapter retries only QueryAccount.
            if native == 0 {
                // Native net-position adjustments can be newer than the previous mock block.
                // Reconnect against a fresh canonical block, keeping the production lag guard.
                {
                    let mut frame = private.state.rpc_frame.lock().unwrap();
                    frame.1 += 1;
                    frame.2 =
                        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000 + 1;
                }
                barrier("wallet transport reconnects", || {
                    private.state.sockets.lock().unwrap().len() > wallet_connections
                })
                .await;
            }
            let original_row = private
                .state
                .order_response_log
                .lock()
                .unwrap()
                .last()
                .unwrap()["body"]
                .clone();
            // Account readiness no longer consumes single-order fixtures. Resolve
            // later explicit order/cancel queries by hash, not by incidental read count.
            private.state.orders.lock().unwrap().clear();
            private.state.orders_by_hash.lock().unwrap().insert(
                body["data"]["order"]["hash"].as_str().unwrap().to_owned(),
                original_row.clone(),
            );
            snapshot(
                &private,
                if shares {
                    "98000000000000000000"
                } else {
                    "100000000000000000000"
                },
            );
            if native == 0 {
                cash_query.set(true);
            }
            barrier("verified account snapshot releases gate", || {
                // Subscription acknowledgement and account collection complete asynchronously.
                // The caller retries the explicit Native query until verification succeeds;
                // never borrow a running node's client across disconnect/connect awaits.
                if native == 0 {
                    cash_query.set(true);
                }
                if native > 0 {
                    engine
                        .borrow()
                        .get_client(&ClientId::new("PREDICT"))
                        .unwrap()
                        .is_connected()
                } else {
                    journal
                        .borrow()
                        .iter()
                        .any(|r| r["type"] == "AccountSnapshot" && r["reconciliationReady"] == true)
                }
            })
            .await;
            // Same-balance account reports can supersede one-shot info before strategy timers.
            // Connectivity includes the verified epoch; capture that current native observation.
            journal.borrow_mut().push(json!({"type":"ExecutionConnectivityVerified","afterNativeReconciliation":native>0,"cashTotal":"938"}));
            // Repeat the query; no fills or order writes may be replayed.
            let count = private.state.creations.lock().unwrap().len();
            let reads_before = private.state.rpc_methods.lock().unwrap().len();
            cash_query.set(true);
            barrier("repeated account query applied", || {
                private.state.rpc_methods.lock().unwrap().len() > reads_before
            })
            .await;
            assert_eq!(private.state.creations.lock().unwrap().len(), count);
            resume_submit.set(true);
            barrier("strategy resumes one fresh order", || {
                private.state.creations.lock().unwrap().len() == count + 1
            })
            .await;
            resumed_request = private
                .state
                .creations
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .clone();
            let mut row = rest_order_data("OPEN", 1, OrderSide::Buy);
            row["order"] = resumed_request["data"]["order"].clone();
            if native > 0 {
                private.state.orders.lock().unwrap().clear();
                let mut by_hash = private.state.orders_by_hash.lock().unwrap();
                by_hash.insert(
                    body["data"]["order"]["hash"].as_str().unwrap().to_owned(),
                    original_row.clone(),
                );
                by_hash.insert(
                    resumed_request["data"]["order"]["hash"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                    json!({"success":true,"data":row}),
                );
            } else {
                private
                    .state
                    .orders
                    .lock()
                    .unwrap()
                    .push_back(json!({"success":true,"data":row}));
            }
            private.state.remove_replies.lock().unwrap().push_back((
                200,
                json!({"success":true,"removed":["12345"],"noop":[]}),
                0,
            ));
            private.wallet(super::exit_orders::wire_event(
                &resumed_request,
                "orderAccepted",
                1,
                Decimal::ZERO,
            ));
            barrier("resumed strategy order canceled", || {
                cache
                    .borrow()
                    .order(&ClientOrderId::new("RESUMED-001"))
                    .is_some_and(|o| o.status() == OrderStatus::Canceled)
            })
            .await;
            assert_eq!(
                cache.borrow().order(&id).unwrap().filled_qty().as_decimal(),
                Decimal::from(100)
            );
            assert_eq!(private.state.removals.lock().unwrap().len(), 1);
            barrier("resumed order cash lock released", || {
                cache
                    .borrow()
                    .account_owned(&account_id())
                    .unwrap()
                    .balance_locked(Some(Currency::USDT()))
                    .unwrap()
                    .as_decimal()
                    .is_zero()
            })
            .await;
            assert!(private.state.open_orders.lock().unwrap().is_empty());
        }
        handle.stop();
        json!({"readOnly":read_only,"nativeReconciliationStage":native,"explicitStrategyOrderQueries":native==0 || native==3,"submissionRecoveryDiagnostics":diagnostics.borrow().clone(),"mainnetWrites":0,"matchResponses":private.state.match_response_log.lock().unwrap().clone(),"positionResponses":private.state.position_response_log.lock().unwrap().clone(),"customRecoveryTimerEnabled":resume && native>0,"customTimerQueriesOrders":false,"orderLists":private.state.order_list_log.lock().unwrap().clone(),"sharesFee":shares,"request":body,"orderResponses":private.state.order_response_log.lock().unwrap().clone(),"matches":rows,"nativeFeedback":journal.borrow().clone(),"primaryFillCallbacks":callbacks,"grossFilled":"100","netPosition":quantity(&cache).to_string(),"cashTotal":"938","cashLocked":"0","orderPosts":private.state.creations.lock().unwrap().len(),"streamGap":true,"newSubmissionsBlocked":!resume,"resumedRequest":resumed_request})
    };
    let result = timeout(Duration::from_secs(20), async {
        let (value, result) = tokio::join!(driver, live.run());
        result.unwrap();
        value
    })
    .await
    .unwrap();
    if native == 3 {
        msgbus::unsubscribe_any(
            MessagingSwitchboard::submission_recovery_exhausted_topic().into(),
            &diagnostic_handler,
        );
    }
    public_task.abort();
    result
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_reconnect_verifies_applied_state_then_resumes_and_cancels_once() {
    run_case(false, true, true).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline resume capture, never overwrite evidence"]
async fn record_resume_json() {
    let sessions = vec![
        run_case(false, true, true).await,
        run_case(true, true, true).await,
    ];
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
            if cfg!(feature = "native-quantity-commission") {
                "tests/fixtures/private_api/native_quantity_commission_reconnect_20261017.json"
            } else {
                "tests/fixtures/private_api/reconciliation_ready_live_node_synthetic_20261016.json"
            },
        ))
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic offline Rust Strategy + LiveNode resume; no mainnet writes","sessions":sessions})).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_reconnect_verifies_share_fee_net_position_before_resuming() {
    run_case(true, true, true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_startup_inflight_and_open_checks_recover_without_custom_query_timer() {
    run_mode(false, true, false, 1).await;
}
#[tokio::test(flavor = "current_thread")]
async fn native_position_checks_join_startup_and_order_recovery_without_duplicate_fills() {
    run_mode(false, true, false, 2).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline Rust Strategy + LiveNode native reconciliation recording"]
async fn record_native_reconciliation_json() {
    let sessions = vec![
        run_mode(false, true, false, 1).await,
        run_mode(false, true, false, 2).await,
    ];
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/native_reconciliation_live_node_synthetic_20261016.json");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic offline Rust Strategy + LiveNode, not mainnet execution","sessions":sessions})).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn native_reconciliation_then_account_verification_resumes_strategy_and_cancels() {
    run_mode(false, true, true, 2).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline native reconciliation/account resume recording"]
async fn record_native_account_resume_json() {
    let sessions = vec![run_mode(false, true, true, 2).await];
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/private_api/native_account_resume_synthetic_20261016.json"),
        )
        .unwrap();
    serde_json::to_writer_pretty(file, &json!({"source":"synthetic Rust Strategy + LiveNode; native checks and account-only recovery timer; no mainnet writes","sessions":sessions})).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn native_unknown_submission_exhaustion_retains_order_until_venue_evidence() {
    run_mode(false, true, false, 3).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline native submission retention recording"]
async fn record_native_retention_json() {
    let session = run_mode(false, true, false, 3).await;
    let file =
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
                "tests/fixtures/private_api/native_submission_retention_synthetic_20261016.json",
            ))
            .unwrap();
    serde_json::to_writer_pretty(file, &json!({"source":"synthetic Rust Strategy + LiveNode native recovery exhaustion; no mainnet writes","sessions":[session]})).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn read_only_native_strategy_recovers_share_fees_without_submission_signer() {
    let session = run_mode_inner(true, true, false, 0, true).await;
    assert_eq!(session["orderPosts"], 0);
    assert_eq!(
        Decimal::from_str_exact(session["netPosition"].as_str().unwrap()).unwrap(),
        Decimal::from(98)
    );
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit loopback read-only SHARES capture; never overwrite JSON"]
async fn record_read_only_share_recovery_json() {
    let session = run_mode_inner(true, true, false, 0, true).await;
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/readonly_share_recovery_synthetic_20261016.json");
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file, &json!({"source":"synthetic offline Rust strategy + LiveNode, submission=None; not mainnet", "sessions":[session]})).unwrap();
}
