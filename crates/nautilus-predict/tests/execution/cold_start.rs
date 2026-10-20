// -------------------------------------------------------------------------------------------------
// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
// See https://www.gnu.org/licenses/lgpl-3.0.html for license details.
// -------------------------------------------------------------------------------------------------
//! No persisted orders/positions. Actual order economics, explicitly derived historical snapshots.
use std::cell::Cell;

use nautilus_common::actor::DataActor;
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_live::{
    execution::{config::ExecutionManagerConfig, manager::ExecutionManager},
    node::config::LiveExecutionEngineConfig,
};
use nautilus_model::{data::CustomData, identifiers::StrategyId};
use nautilus_predict::{
    PredictDataClientConfig, common::contracts::PredictExchangeType, signing::PredictOrderHasher,
};
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};

use super::{
    exit_orders::{PublicState, barrier, exit_socket},
    *,
};

fn primary() -> Value {
    serde_json::from_str(include_str!(
        "../fixtures/private_api/cold_start_readonly_20261016T133220Z.json"
    ))
    .unwrap()
}
fn trade_source() -> Value {
    serde_json::from_str(include_str!(
        "../fixtures/private_api/live_fill_20261016T123240Z_independent/calibration.json"
    ))
    .unwrap()
}
fn original_hash(index: usize) -> String {
    trade_source()["wallet"]["events"][index]["orderHash"]
        .as_str()
        .unwrap()
        .into()
}
fn market() -> Value {
    let mut row = primary()["markets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == 10835)
        .unwrap()
        .clone();
    row["id"] = json!(42);
    for (i, token) in ["11", "22"].into_iter().enumerate() {
        row["outcomes"][i]["onChainId"] = json!(token);
    }
    row
}
async fn public_market() -> Response {
    json_response(json!({"success":true,"data":market()}))
}
pub(super) fn row(index: usize) -> Value {
    let hash = original_hash(index);
    let mut row = primary()["ordersByHash"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["order"]["hash"] == hash)
        .unwrap()
        .clone();
    row["marketId"] = json!(42);
    row["order"]["maker"] = json!(WALLET);
    row["order"]["signer"] = json!(WALLET);
    row["order"]["tokenId"] = json!("11");
    let fields = serde_json::from_value(row["order"].clone()).unwrap();
    let hash = PredictOrderHasher::new(PredictEnvironment::BnbMainnet)
        .order_hash(&fields, PredictExchangeType::YieldBearing);
    row["order"]["hash"] = json!(format!("{hash:#x}"));
    if index == 3 {
        row["status"] = json!("OPEN");
    } // Derived time slice at TP acceptance, not a captured OPEN response.
    row
}
pub(super) fn match_row(buy: &Value) -> Value {
    let source = trade_source();
    let mut row = source["after"]["matches"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["taker"]["hash"] == original_hash(0))
        .unwrap()
        .clone();
    row["market"] = market();
    row["taker"]["hash"] = buy["order"]["hash"].clone();
    row["taker"]["signer"] = json!(WALLET);
    row["taker"]["outcome"]["onChainId"] = json!("11");
    for leg in row["makers"].as_array_mut().unwrap() {
        leg["outcome"]["onChainId"] = json!(if leg["outcome"]["indexSet"] == 1 {
            "11"
        } else {
            "22"
        });
    }
    // A held instrument must recover its older prefix even outside a one-minute lookback.
    row["executedAt"] = json!(
        chrono::DateTime::from_timestamp_nanos(
            (get_atomic_clock_realtime().get_time_ns().as_u64() - 120_000_000_000) as i64
        )
        .to_rfc3339()
    );
    row
}
fn position() -> Value {
    json!({"id":"derived-holding-from-confirmed-chain-transfer","market":market(),"outcome":market()["outcomes"][0],"amount":"31360000000000000000","valueUsd":"0","averageBuyPriceUsd":"0","pnlUsd":"0"})
}

#[derive(Debug)]
struct Observer {
    core: StrategyCore,
    journal: Rc<RefCell<Vec<Value>>>,
    cancel_on_snapshot: Rc<Cell<bool>>,
    cancel_requested: bool,
}
nautilus_strategy!(Observer, {
    fn external_order_instrument_ids(&self) -> Option<Vec<InstrumentId>> {
        self.core.config.external_order_instrument_ids.clone()
    }
    fn on_order_event(&mut self, event: nautilus_model::events::OrderEventAny) {
        self.journal
            .borrow_mut()
            .push(json!({"type":if self.cancel_requested {"RecoveryOrderCallback"} else {"UnexpectedOrderCallback"},
            "clientOrderId":event.client_order_id().to_string(),"nativeEvent":event}));
    }
    fn on_order_canceled(&mut self, event: &nautilus_model::events::OrderCanceled) {
        assert!(self.cancel_requested);
        assert_eq!(
            self.cache().order(&event.client_order_id).unwrap().status(),
            OrderStatus::Canceled
        );
        self.journal
            .borrow_mut()
            .push(json!({"type":"RecoveryCanceledCallback","event":event}));
    }
    fn on_order_filled(&mut self, event: &nautilus_model::events::OrderFilled) {
        assert!(self.cancel_requested);
        self.journal
            .borrow_mut()
            .push(json!({"type":"RecoveryFilledCallback","event":event}));
    }
    fn on_position_event(&mut self, event: nautilus_model::events::PositionEvent) {
        self.journal
            .borrow_mut()
            .push(json!({"type":if self.cancel_requested {
            "RecoveryPositionCallback"
        } else { "UnexpectedPositionCallback" },"event":event}));
    }
});
impl DataActor for Observer {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let cache = self.cache();
        let orders = cache.orders_open(None, None, None, None, None);
        let positions = cache.positions_open(None, None, None, None, None);
        let balance = cache
            .account(&account_id())
            .unwrap()
            .last_event()
            .unwrap()
            .balances[0];
        self.journal.borrow_mut().push(json!({"type":"StrategyOnStart","openOrders":orders.len(),"netPositions":positions.iter().map(|p|p.quantity.to_string()).collect::<Vec<_>>(),"owners":orders.iter().map(|o|o.strategy_id().to_string()).collect::<Vec<_>>(),"cash":balance.total.to_string(),"locked":balance.locked.to_string()}));
        drop(cache);
        self.subscribe_data(
            nautilus_predict::predict_orderbook_data_type(42),
            Some(ClientId::new("PREDICT")),
            None,
        );
        Ok(())
    }
    fn on_data(&mut self, _: &CustomData) -> anyhow::Result<()> {
        self.journal
            .borrow_mut()
            .push(json!({"type":"CustomSnapshot"}));
        if self.cancel_on_snapshot.replace(false) {
            let order = self.cache().orders_open(None, None, None, None, None)[0].clone();
            assert!(
                !order.is_post_only(),
                "REST must not invent a post-only intent"
            );
            self.cancel_requested = true;
            self.cancel_order(
                order.client_order_id(),
                Some(ClientId::new("PREDICT")),
                None,
            )?;
        }
        Ok(())
    }
}

async fn run(claim: bool, holding_only: bool) -> Value {
    run_with_closed_history(claim, holding_only, false).await
}

async fn run_with_closed_history(claim: bool, holding_only: bool, older_cycle: bool) -> Value {
    run_case(claim, holding_only, older_cycle, false).await
}

async fn run_case(claim: bool, holding_only: bool, older_cycle: bool, exit_only: bool) -> Value {
    run_lifecycle(
        claim,
        holding_only,
        older_cycle,
        exit_only,
        RecoveryCheck::None,
    )
    .await
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RecoveryCheck {
    None,
    Cancel,
    Purge,
}

#[expect(
    clippy::await_holding_refcell_ref,
    reason = "Native startup is driven on one core"
)]
async fn run_lifecycle(
    claim: bool,
    holding_only: bool,
    older_cycle: bool,
    exit_only: bool,
    recovery: RecoveryCheck,
) -> Value {
    let recover_and_cancel = recovery != RecoveryCheck::None;
    let private = server(vec![]).await;
    let buy = row(0);
    let tp = row(3);
    let confirmed = match_row(&buy);
    for row in [&buy, &tp]
        .into_iter()
        .take(if holding_only { 1 } else { 2 })
    {
        private.state.orders_by_hash.lock().unwrap().insert(
            row["order"]["hash"].as_str().unwrap().into(),
            json!({"success":true,"data":row}),
        );
    }
    let open = if holding_only {
        vec![]
    } else {
        vec![tp.clone()]
    };
    *private.state.open_orders.lock().unwrap() = open.clone();
    // The actual default list may omit terminal orders; recovery must query BUY by hash.
    for _ in 0..3 {
        private
            .state
            .open_pages
            .lock()
            .unwrap()
            .push_back(json!({"success":true,"cursor":null,"data":open}));
    }
    let mut history = vec![confirmed.clone(), confirmed.clone()];
    let old_buy_hash = format!("0x{}", "44".repeat(32));
    let old_sell_hash = format!("0x{}", "55".repeat(32));
    if older_cycle {
        let mut old_buy = confirmed.clone();
        old_buy["taker"]["hash"] = json!(old_buy_hash);
        old_buy["settlementId"] = json!("derived-earlier-closed-buy");
        old_buy["executedAt"] = json!(
            chrono::DateTime::from_timestamp_nanos(
                (get_atomic_clock_realtime().get_time_ns().as_u64() - 240_000_000_000) as i64
            )
            .to_rfc3339()
        );
        let mut old_sell = old_buy.clone();
        old_sell["taker"]["hash"] = json!(old_sell_hash);
        old_sell["taker"]["quoteType"] = json!("Ask");
        old_sell["taker"]["amount"] = json!("31360000000000000000");
        old_sell["taker"]["price"] = json!("39000000000000000");
        old_sell["taker"]["fee"] = json!({"type":"COLLATERAL","amount":"24460800000000000"});
        old_sell["settlementId"] = json!("derived-earlier-closed-sell");
        old_sell["executedAt"] = json!(
            chrono::DateTime::from_timestamp_nanos(
                (get_atomic_clock_realtime().get_time_ns().as_u64() - 180_000_000_000) as i64
            )
            .to_rfc3339()
        );
        history.extend([old_buy.clone(), old_sell.clone(), old_buy, old_sell]);
        history.reverse();
    }
    *private.state.match_snapshot.lock().unwrap() = Some(history);
    *private.state.position_snapshot.lock().unwrap() = Some(vec![position()]);
    *private.state.rpc_frame.lock().unwrap() = (
        47_017_729_200_000_000_001,
        1,
        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000,
    );
    let mut cfg = config(&private);
    cfg.account_state_mode = PredictAccountStateMode::Reported;
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", private.http).into(),
    ));
    assert!(cfg.submission.is_none() && !cfg.orderbook_removal_enabled);
    if recover_and_cancel {
        cfg.orderbook_removal_enabled = true;
        cfg.reconnect_reconciliation_interval_secs = Some(1);
        private.state.remove_replies.lock().unwrap().push_back((
            200,
            json!({"echoRemoved":true}),
            0,
        ));
    }
    let public = PublicState(Arc::new(Mutex::new(vec![])));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/v1/markets/42", get(public_market))
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
    let engine = live.kernel().exec_engine.clone();
    let clock = live.kernel().clock.clone();
    let handle = live.handle();
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
    assert!(cache.borrow().instruments(&PREDICT_VENUE, None).is_empty());
    let journal = Rc::new(RefCell::new(vec![]));
    let cancel_on_snapshot = Rc::new(Cell::new(false));
    let strategy_id = StrategyId::from(if exit_only {
        "PREDICT-EXIT-001"
    } else {
        "COLD-001"
    });
    if exit_only {
        use super::exit_orders::strategy::{ExitSettings, ExitStrategy};
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
            .with_exit_only_recovery(),
        )
        .unwrap();
    } else {
        live.add_strategy(Observer {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(strategy_id),
                external_order_instrument_ids: claim.then(|| vec![instrument_id(1)]),
                ..Default::default()
            }),
            journal: journal.clone(),
            cancel_on_snapshot: cancel_on_snapshot.clone(),
            cancel_requested: false,
        })
        .unwrap();
    }
    let mut replayed = None;
    let driver = async {
        barrier("reconciled strategy started", || {
            !public.0.lock().unwrap().is_empty()
        })
        .await;
        {
            let cache = cache.borrow();
            let positions = cache.positions_open(None, None, None, None, None);
            assert_eq!(positions.len(), 1);
            assert_eq!(positions[0].quantity.as_decimal(), Decimal::new(3136, 2));
            assert_eq!(
                positions[0].strategy_id,
                if claim {
                    strategy_id
                } else {
                    StrategyId::from("EXTERNAL")
                }
            );
            assert_eq!(
                cache.orders_open(None, None, None, None, None).len(),
                usize::from(!holding_only)
            );
        }
        // Query after Native adoption now has real client IDs, with no fabricated local order.
        let mass = {
            let mut engine = engine.borrow_mut();
            engine
                .generate_mass_status(&ClientId::new("PREDICT"), Some(1))
                .await
                .unwrap()
                .unwrap()
        };
        let before = cache.borrow().orders(None, None, None, None, None).len();
        let mut manager =
            ExecutionManager::new(clock, cache.clone(), ExecutionManagerConfig::default()).unwrap();
        let repeated = manager.reconcile_execution_mass_status(&mass, &engine);
        assert!(repeated.unresolved_positions.is_empty());
        assert_eq!(
            cache.borrow().orders(None, None, None, None, None).len(),
            before
        );
        assert_eq!(
            cache.borrow().positions_open(None, None, None, None, None)[0]
                .quantity
                .as_decimal(),
            Decimal::new(3136, 2)
        );
        replayed = Some(
            json!({"massStatus":mass,"repeatedEvents":repeated.events.len(),"orderCount":before}),
        );
        let mut book: Value = serde_json::from_str(include_str!(
            "../fixtures/private_api/live_fill_20261016T123240Z/admission.json"
        ))
        .unwrap();
        let book = &mut book["candidate"]["book"];
        book["marketId"] = json!(42);
        book["orderCount"] = json!(0);
        book["lastOrderSettled"]["marketId"] = json!(42);
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
        barrier("custom snapshot after startup", || {
            journal.borrow().iter().any(|r| {
                r["type"] == "CustomSnapshot" || (exit_only && r["type"] == "ExitOnlyAdopted")
            })
        })
        .await;
        if recover_and_cancel {
            // Default execution can create a local native position-correction fill at
            // startup. Advance this fixed RPC fixture as a subsequent block would.
            {
                let mut block = private.state.rpc_frame.lock().unwrap();
                block.1 += 1;
                block.2 = get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000 + 1;
            }
            // Native already restored the completed entry and active TP. Account
            // readiness must not re-query either hash while recovering this epoch.
            private.state.order_requests.lock().unwrap().clear();
            let connections = private.state.sockets.lock().unwrap().len();
            private
                .state
                .sockets
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .send(Message::Close(None))
                .unwrap();
            barrier(
                "recovered external LIMIT is gated during disconnect",
                || {
                    !engine
                        .borrow()
                        .get_client(&ClientId::new("PREDICT"))
                        .unwrap()
                        .is_connected()
                },
            )
            .await;
            barrier("external LIMIT wallet resubscribes", || {
                private.state.sockets.lock().unwrap().len() > connections
            })
            .await;
            barrier(
                "QueryAccount verifies non-post-only LIMIT after native recovery",
                || {
                    engine
                        .borrow()
                        .get_client(&ClientId::new("PREDICT"))
                        .unwrap()
                        .is_connected()
                },
            )
            .await;
            assert!(
                !journal
                    .borrow()
                    .iter()
                    .any(|r| r["type"] == "UnexpectedOrderCallback"
                        || r["type"] == "UnexpectedPositionCallback")
            );
            assert!(
                private.state.order_requests.lock().unwrap().is_empty(),
                "QueryAccount readiness must reuse OPEN and applied fills, not scan cached hashes"
            );
            journal.borrow_mut().push(json!({
                "type":"AccountReadinessScopeVerified",
                "singleOrderRequests":0,
                "nativeTerminalOrders":cache.borrow().orders(None, None, None, None, None)
                    .iter().filter(|o| o.is_closed()).count(),
                "nativeActiveOrders":cache.borrow().orders_open(None, None, None, None, None).len(),
            }));
            if recovery == RecoveryCheck::Purge {
                // Retire the completed BUY while Native still owns the active SELL
                // and net position. Position cost/SHARES fees must not depend on a
                // retained entry OrderAny in the adapter.
                cache
                    .borrow_mut()
                    .purge_closed_orders(get_atomic_clock_realtime().get_time_ns(), 0);
                assert_eq!(
                    cache
                        .borrow()
                        .orders_open(None, None, None, None, None)
                        .len(),
                    1
                );
                let before = cache
                    .borrow()
                    .account(&account_id())
                    .unwrap()
                    .last_event()
                    .unwrap()
                    .event_id;
                engine
                    .borrow()
                    .get_client(&ClientId::new("PREDICT"))
                    .unwrap()
                    .query_account(QueryAccount::new(
                        trader_id(),
                        Some(ClientId::new("PREDICT")),
                        account_id(),
                        UUID4::new(),
                        get_atomic_clock_realtime().get_time_ns(),
                        None,
                        None,
                    ))
                    .unwrap();
                barrier(
                    "entry retirement preserves current holding and active SELL",
                    || {
                        cache
                            .borrow()
                            .account(&account_id())
                            .unwrap()
                            .last_event()
                            .unwrap()
                            .event_id
                            != before
                    },
                )
                .await;
                assert_eq!(
                    cache.borrow().positions_open(None, None, None, None, None)[0]
                        .quantity
                        .as_decimal(),
                    Decimal::new(3136, 2)
                );
                journal
                    .borrow_mut()
                    .push(json!({"type":"EntryRetiredWhileHolding",
                    "nativeActiveOrders":1,"netShares":"31.36","entrySharesFee":"0.64"}));
            }
            cancel_on_snapshot.set(true);
            for tx in public.0.lock().unwrap().iter() {
                tx.send(Message::Text(
                    json!({"type":"M","topic":"predictOrderbook/42","data":book})
                        .to_string()
                        .into(),
                ))
                .unwrap();
            }
            barrier(
                "strategy receives native Canceled for restored LIMIT",
                || {
                    journal.borrow().iter().any(|r| {
                        r["type"] == "RecoveryOrderCallback"
                            && r["nativeEvent"].get("Canceled").is_some()
                    })
                },
            )
            .await;
            assert!(
                cache
                    .borrow()
                    .orders_open(None, None, None, None, None)
                    .is_empty()
            );
            assert_eq!(private.state.removals.lock().unwrap().len(), 1);
            if recovery == RecoveryCheck::Purge {
                let quantity =
                    Decimal::from_str_exact(tp["order"]["makerAmount"].as_str().unwrap()).unwrap();
                let value =
                    Decimal::from_str_exact(tp["order"]["takerAmount"].as_str().unwrap()).unwrap();
                let body = json!({"data":{"order":tp["order"],"strategy":tp["strategy"],
                    "pricePerShare":(value / quantity * Decimal::from(1_000_000_000_000_000_000u64)).normalize().to_string()}});
                let mut duplicate =
                    super::exit_orders::wire_event(&body, "orderCancelled", 1, Decimal::ZERO);
                duplicate["orderId"] = tp["id"].clone();
                // Retained closed orders must still accept a real late settlement.
                // Queue duplicate cancellation first, then a full SELL and its replay.
                // Observing the fill/position callbacks is the causal processing barrier.
                private.wallet(duplicate.clone());
                private.wallet(duplicate.clone());
                let scale = Decimal::from(1_000_000_000_000_000_000u64);
                let size = quantity / scale;
                let mut late =
                    super::exit_orders::wire_event(&body, "orderTransactionSuccess", 1, size);
                late["orderId"] = tp["id"].clone();
                late["settlementId"] = json!("derived-late-retained-tp");
                late["isMaker"] = json!(false);
                late["fee"] = json!({"type":"COLLATERAL","amountWei":(value / Decimal::from(50)).normalize().to_string()});
                late["fill"] = json!({"executedSizeWei":quantity.normalize().to_string(),
                    "executedPriceWei":body["data"]["pricePerShare"],"executedValueWei":value.normalize().to_string()});
                super::strategy_lifecycle::cash(&private, value / scale * Decimal::new(98, 2));
                *private.state.position_snapshot.lock().unwrap() = Some(vec![]);
                serde_json::from_value::<PredictWalletEvent>(late.clone()).unwrap();
                private.wallet(late.clone());
                private.wallet(late.clone());
                barrier(
                    "late retained TP fill closes Native position exactly once",
                    || {
                        cache
                            .borrow()
                            .positions_open(None, None, None, None, None)
                            .is_empty()
                            && journal
                                .borrow()
                                .iter()
                                .any(|e| e["type"] == "RecoveryFilledCallback")
                    },
                )
                .await;
                journal.borrow_mut().push(json!({"type":"RetirementWalletReplay",
                    "source":"derived late settlement using recorded TP terms; not an observed live race",
                    "cancellation":duplicate,"lateSellAndReplay":late}));
                let before = cache.borrow().orders(None, None, None, None, None).len();
                cache
                    .borrow_mut()
                    .purge_closed_orders(get_atomic_clock_realtime().get_time_ns(), 0);
                assert!(
                    cache
                        .borrow()
                        .orders(None, None, None, None, None)
                        .is_empty()
                );
                let account_before = cache
                    .borrow()
                    .account(&account_id())
                    .unwrap()
                    .last_event()
                    .unwrap()
                    .event_id;
                engine
                    .borrow()
                    .get_client(&ClientId::new("PREDICT"))
                    .unwrap()
                    .query_account(QueryAccount::new(
                        trader_id(),
                        Some(ClientId::new("PREDICT")),
                        account_id(),
                        UUID4::new(),
                        get_atomic_clock_realtime().get_time_ns(),
                        None,
                        None,
                    ))
                    .unwrap();
                barrier(
                    "account query remains usable after Native order purge",
                    || {
                        cache
                            .borrow()
                            .account(&account_id())
                            .unwrap()
                            .last_event()
                            .unwrap()
                            .event_id
                            != account_before
                    },
                )
                .await;
                assert!(
                    cache
                        .borrow()
                        .positions_open(None, None, None, None, None)
                        .is_empty()
                );
                assert!(
                    cache
                        .borrow()
                        .account(&account_id())
                        .unwrap()
                        .balance_locked(Some(Currency::USDT()))
                        .unwrap()
                        .is_zero()
                );
                let expected_cash = Money::from_decimal(
                    Decimal::from_str_exact(&private.state.rpc_frame.lock().unwrap().0.to_string())
                        .unwrap()
                        / Decimal::from(1_000_000_000_000_000_000u64),
                    Currency::USDT(),
                )
                .unwrap();
                assert_eq!(
                    cache
                        .borrow()
                        .account(&account_id())
                        .unwrap()
                        .balance_total(Some(Currency::USDT())),
                    Some(expected_cash)
                );
                // Beyond Native retention, no local owner remains. Do not infer a fresh
                // order/fill from a late message or create an adapter tombstone ledger.
                private.wallet(late);
                barrier(
                    "purged unknown wallet identity gates rather than resurrecting order",
                    || {
                        engine
                            .borrow()
                            .get_client(&ClientId::new("PREDICT"))
                            .unwrap()
                            .submit_order(submissions::submit(&order(1)))
                            .is_err_and(|e| e.to_string().contains("stream gap"))
                    },
                )
                .await;
                journal
                    .borrow_mut()
                    .push(json!({"type":"NativeOrderContextsRetired",
                    "purgedNativeOrders":before,"remainingNativeOrders":0,"netPosition":"0",
                    "locked":"0","lateUnknownMessageGated":true,"newOrders":0}));
            }
        }
        handle.stop();
    };
    timeout(Duration::from_secs(12), async {
        tokio::join!(
            async {
                live.run().await.unwrap();
            },
            driver
        );
    })
    .await
    .unwrap();
    task.abort();
    // Native startup restores Cache before on_start, without replaying startup fills
    // into a running strategy. Repeated mass status must remain callback-idempotent.
    assert!(!journal.borrow().iter().any(|row| {
        row["type"] == "UnexpectedOrderCallback" || row["type"] == "UnexpectedPositionCallback"
    }));
    assert!(
        private.state.creations.lock().unwrap().is_empty()
            && private.state.removals.lock().unwrap().len() == usize::from(recover_and_cancel)
    );
    if recover_and_cancel {
        assert_eq!(
            journal
                .borrow()
                .iter()
                .filter(|r| r["type"] == "RecoveryCanceledCallback")
                .count(),
            1
        );
        assert_eq!(
            journal
                .borrow()
                .iter()
                .filter(|r| {
                    r["type"] == "RecoveryOrderCallback"
                        && r["nativeEvent"].get("Canceled").is_some()
                })
                .count(),
            1,
            "restored order cancellation must reach the strategy once"
        );
    }
    assert!(
        !private
            .state
            .order_requests
            .lock()
            .unwrap()
            .iter()
            .any(|h| h == &old_buy_hash || h == &old_sell_hash)
    );
    if recovery == RecoveryCheck::Purge {
        assert_eq!(
            journal
                .borrow()
                .iter()
                .filter(|e| e["type"] == "RecoveryFilledCallback")
                .count(),
            1
        );
        assert_eq!(
            journal
                .borrow()
                .iter()
                .filter(|e| e["type"] == "RecoveryPositionCallback")
                .count(),
            1
        );
    }
    if exit_only {
        let events = journal.borrow();
        let adopted = events
            .iter()
            .find(|e| e["type"] == "ExitOnlyAdopted")
            .unwrap();
        assert_eq!(adopted["entryQuantity"], "32.0000000000000000");
        assert_eq!(
            Decimal::from_str_exact(adopted["entryCost"].as_str().unwrap()).unwrap(),
            Decimal::new(128, 2)
        );
        assert_eq!(
            Decimal::from_str_exact(adopted["entrySharesFee"].as_str().unwrap()).unwrap(),
            Decimal::new(64, 2)
        );
        assert!(!adopted["maker"].is_null());
        assert!(!events.iter().any(|e| e["type"] == "StrategyCallback"
            || e["type"] == "NativeEntrySubmitted"
            || e["type"] == "MakerTakeProfitSubmitted"));
    }
    let cache = live.kernel().cache.borrow();
    assert_eq!(
        cache.positions(None, None, None, None, None)[0].realized_pnl,
        Some(Money::from(if recovery == RecoveryCheck::Purge {
            "0.04151040 USDT"
        } else {
            "-0.0256 USDT"
        }))
    );
    json!({"source":"offline Native cold-start with actual signed order economics and confirmed match/chain transfer; derived time slice, not actual nonempty REST snapshot","olderSyntheticClosedCycle":older_cycle,"history":*private.state.match_snapshot.lock().unwrap(),"claimed":claim,"holdingOnly":holding_only,"transforms":["market/token/wallet rebind and pure SDK-compatible hash recalculation","TP CANCELLED snapshot replayed at earlier Accepted point as OPEN","position shape derived from confirmed net ERC1155 receipt; display value/cost fields unused zero placeholders","historical fill shifted to 120s before startup"],"primaryOrderSource":"cold_start_readonly_20261016T133220Z.json","buy":buy,"takeProfit":tp,"confirmedMatch":confirmed,"position":position(),"nativeFeedback":*journal.borrow(),"repeated":replayed,"initialCacheOrders":0,"initialCachePositions":0,"initialCacheInstruments":0,"finalNetShares":if recovery == RecoveryCheck::Purge { "0" } else { "31.36" },"orderWrites":usize::from(recover_and_cancel),"finalRealizedPnl":cache.positions(None, None, None, None, None)[0].realized_pnl.map(|p|p.to_string()),"finalAccount":cache.account(&account_id()).unwrap().last_event()})
}
#[tokio::test(flavor = "current_thread")]
async fn cold_start_current_inventory_excludes_older_same_market_closed_cycle() {
    run_with_closed_history(true, false, true).await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "offline same-market history scope JSON; create_new only"]
async fn record_current_inventory_scope_json() {
    let result = run_with_closed_history(true, false, true).await;
    let file = std::fs::OpenOptions::new().create_new(true).write(true).open(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/private_api/current_inventory_history_scope_native_derived_20261017.json")
    ).unwrap();
    serde_json::to_writer_pretty(file, &result).unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn cold_start_recovers_external_orders_and_net_position() {
    run(false, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn cold_start_reconnects_non_post_only_limit_then_strategy_cancels() {
    run_lifecycle(true, false, false, false, RecoveryCheck::Cancel).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "offline account readiness scope evidence; create_new only"]
async fn record_account_readiness_scope_json() {
    let result = run_lifecycle(true, false, false, false, RecoveryCheck::Cancel).await;
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "tests/fixtures/private_api/account_readiness_scope_native_derived_20261017.json",
        ))
        .unwrap();
    serde_json::to_writer_pretty(file, &result).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn native_purge_releases_wallet_context_without_replaying_strategy_activity() {
    run_lifecycle(true, false, false, false, RecoveryCheck::Purge).await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "offline Native Cache retirement evidence; create_new only"]
async fn record_native_cache_retirement_json() {
    let result = run_lifecycle(true, false, false, false, RecoveryCheck::Purge).await;
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/private_api/native_cache_retirement_derived_20261017.json"),
        )
        .unwrap();
    serde_json::to_writer_pretty(file, &result).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cold_start_claims_apply_before_strategy_start() {
    run(true, false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn cold_start_holding_recovers_older_fill_prefix_without_open_orders() {
    run(true, true).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "offline startup callback-gate evidence; create_new only"]
async fn record_startup_callback_gate_json() {
    let mut sessions = vec![];
    for (claim, holding) in [(false, false), (true, false), (true, true)] {
        sessions.push(run(claim, holding).await);
    }
    let file =
        std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
                "tests/fixtures/private_api/startup_callback_gate_native_derived_20261017.json",
            ))
            .unwrap();
    serde_json::to_writer_pretty(
        file,
        &json!({"source":"offline actual-economic derived cold-start; not new venue evidence",
        "startupBusinessCallbacks":0,"sessions":sessions}),
    )
    .unwrap();
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "offline cold-start actual-economic derived JSON; create_new only"]
async fn record_cold_start_json() {
    let mut sessions = vec![];
    for (claim, holding) in [(false, false), (true, false), (true, true)] {
        sessions.push(run(claim, holding).await);
    }
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/private_api/cold_start_native_actual_derived_20261016.json"),
        )
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"offline derived historical time slices; not new real trades","sessions":sessions})).unwrap();
}

#[tokio::test]
async fn cold_reports_validate_external_identity_terms_and_complete_fills_before_native_mutation() {
    for case in [
        "valid",
        "partial",
        "partial_missing_prefix",
        "bad_hash",
        "foreign",
        "missing_matches",
        "overfilled",
        "market_buy",
        "missing_instrument",
    ] {
        let private = server(vec![]).await;
        let buy = row(0);
        let mut selected = buy.clone();
        let mut matched = match_row(&buy);
        let mut held = position();
        match case {
            "partial" | "partial_missing_prefix" => {
                selected["status"] = json!("OPEN");
                selected["amountFilled"] = json!("16000000000000000000");
                held["amount"] = json!("15680000000000000000");
                matched["taker"]["amount"] = json!(if case == "partial" {
                    "16000000000000000000"
                } else {
                    "8000000000000000000"
                });
                matched["taker"]["fee"]["amount"] = json!(if case == "partial" {
                    "320000000000000000"
                } else {
                    "160000000000000000"
                });
            }
            "bad_hash" => selected["order"]["nonce"] = json!("123"),
            "foreign" => {
                selected["order"]["maker"] = json!("0x9999999999999999999999999999999999999999")
            }
            "overfilled" => matched["taker"]["amount"] = json!("33000000000000000000"),
            "market_buy" => selected["strategy"] = json!("MARKET"),
            _ => {}
        }
        let hash = buy["order"]["hash"].as_str().unwrap();
        private
            .state
            .orders_by_hash
            .lock()
            .unwrap()
            .insert(hash.into(), json!({"success":true,"data":selected}));
        *private.state.match_snapshot.lock().unwrap() = Some(if case == "missing_matches" {
            vec![]
        } else {
            vec![matched]
        });
        *private.state.position_snapshot.lock().unwrap() = Some(vec![held]);
        *private.state.rpc_frame.lock().unwrap() = (
            47_017_729_200_000_000_001,
            1,
            get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000,
        );
        let (mut client, mut rx, cache) = submissions::setup_client_with_policy(
            &private,
            get_atomic_clock_realtime().get_time_ns().as_u64(),
            false,
        );
        let _portfolio = portfolio(&cache);
        if case != "missing_instrument" {
            cache
                .borrow_mut()
                .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
                .unwrap();
        }
        connect_reported(&mut client, &mut rx).await;
        let result = client.generate_mass_status(Some(1)).await;
        if case == "valid" || case == "partial" {
            let mass = result.unwrap().unwrap();
            assert_eq!(mass.order_reports().len(), 1);
            assert_eq!(mass.fill_reports().values().map(Vec::len).sum::<usize>(), 1);
            let reports = mass.order_reports();
            let report = reports.values().next().unwrap();
            assert_eq!(report.client_order_id, None);
            assert_eq!(report.quantity.as_decimal(), Decimal::from(32));
            assert_eq!(
                report.order_status,
                if case == "partial" {
                    OrderStatus::PartiallyFilled
                } else {
                    OrderStatus::Filled
                }
            );
            assert_eq!(
                report.filled_qty.as_decimal(),
                Decimal::from(if case == "partial" { 16 } else { 32 })
            );
        } else {
            assert!(
                result.is_err(),
                "{case} must not produce a successful snapshot"
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

#[cfg(feature = "native-quantity-commission")]
#[tokio::test(flavor = "current_thread")]
async fn exit_only_cold_start_adopts_existing_take_profit_without_buy_or_duplicate_exit() {
    run_case(true, false, false, true).await;
}

#[cfg(feature = "native-quantity-commission")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "offline exit-only adoption JSON; create_new only"]
async fn record_exit_only_cold_start() {
    let result = run_case(true, false, false, true).await;
    let output = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/exit_only_cold_start_offline_20261017.json");
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output)
        .unwrap();
    serde_json::to_writer_pretty(file, &result).unwrap();
}
