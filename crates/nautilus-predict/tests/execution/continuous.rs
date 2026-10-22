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

//! Warm-node callback-immediate command replays. All services are loopback.
use super::*;
use nautilus_common::actor::DataActor;
use nautilus_model::{
    data::CustomData,
    enums::TimeInForce,
    events::{
        OrderAccepted, OrderCanceled, OrderDenied, OrderFilled, OrderRejected, OrderSubmitted,
    },
    identifiers::ClientOrderId,
    orders::Order,
};
use nautilus_predict::{
    PredictSettlementUpdate,
    latency::{LatencyMetric, PredictLatency},
    predict_orderbook_data_type, predict_settlement_data_type,
};
use nautilus_trading::{Strategy, StrategyCore, nautilus_strategy};
use std::time::Instant;

#[derive(Debug, Default)]
struct Journal {
    rows: Vec<Value>,
    callbacks: Vec<Instant>,
    decisions: Vec<Instant>,
    submitted: Vec<Instant>,
}
#[derive(Debug)]
struct Continuous {
    core: StrategyCore,
    instrument: InstrumentId,
    current: Option<ClientOrderId>,
    round: usize,
    rounds: usize,
    taker: bool,
    hold: bool,
    books_seen: usize,
    journal: Rc<RefCell<Journal>>,
    probe: PredictLatency,
}
impl Continuous {
    fn new(
        instrument: InstrumentId,
        rounds: usize,
        taker: bool,
        hold: bool,
        journal: Rc<RefCell<Journal>>,
        probe: PredictLatency,
    ) -> Self {
        Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from(if hold {
                    "CONT-OTHER-002"
                } else {
                    "CONT-FAST-001"
                })),
                use_uuid_client_order_ids: true,
                ..Default::default()
            }),
            instrument,
            current: None,
            round: 0,
            rounds,
            taker,
            hold,
            books_seen: 0,
            journal,
            probe,
        }
    }
    fn submit_next(&mut self) {
        if self.round == self.rounds {
            return;
        }
        let order = if self.taker {
            self.order().market(
                self.instrument,
                if self.round.is_multiple_of(2) {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                },
                Quantity::from(100),
                Some(TimeInForce::Gtc),
                Some(false),
                Some(false),
                None,
                None,
                None,
                None,
            )
        } else {
            self.order().limit(
                self.instrument,
                OrderSide::Buy,
                Quantity::from(100),
                Price::from("0.010"),
                Some(TimeInForce::Gtc),
                None,
                Some(true),
                Some(false),
                Some(false),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
        };
        self.current = Some(order.client_order_id());
        let at = Instant::now();
        self.probe.decision(order.client_order_id(), at);
        self.journal.borrow_mut().decisions.push(at);
        let mut params = nautilus_core::Params::new();
        params.insert("market_price".into(), json!("0.020"));
        self.submit_order(
            order,
            None,
            Some(ClientId::from("PREDICT")),
            self.taker.then_some(params),
        )
        .unwrap();
    }
}
impl DataActor for Continuous {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_quotes(self.instrument, Some(ClientId::from("PREDICT")), None);
        self.subscribe_data(
            predict_orderbook_data_type(42),
            Some(ClientId::from("PREDICT")),
            None,
        );
        self.subscribe_data(
            predict_settlement_data_type(account_id(), self.strategy_id().unwrap()),
            None,
            None,
        );
        Ok(())
    }
    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.unsubscribe_quotes(self.instrument, Some(ClientId::from("PREDICT")), None);
        self.unsubscribe_data(
            predict_orderbook_data_type(42),
            Some(ClientId::from("PREDICT")),
            None,
        );
        self.unsubscribe_data(
            predict_settlement_data_type(account_id(), self.strategy_id().unwrap()),
            None,
            None,
        );
        Ok(())
    }
    fn on_data(&mut self, data: &CustomData) -> anyhow::Result<()> {
        if let Some(update) = data.data.as_any().downcast_ref::<PredictSettlementUpdate>() {
            self.probe.strategy_settlement_callback(update);
            assert_eq!(
                data.data_type,
                predict_settlement_data_type(account_id(), self.strategy_id().unwrap())
            );
            assert_eq!(update.strategy_id, self.strategy_id().unwrap());
            let qty = self
                .cache()
                .order(&update.client_order_id)
                .unwrap()
                .filled_qty()
                .as_decimal();
            self.journal.borrow_mut().rows.push(json!({"type":"settlement","id":update.client_order_id.to_string(),"status":format!("{:?}",update.status),"settlement":update.settlement_id,"nativeFilledQty":qty.to_string(),"rawSize":update.executed_size_wei}));
        } else if data.data_type == predict_orderbook_data_type(42) {
            self.books_seen += 1;
            if self.current.is_none() {
                if !self.hold || self.books_seen > 1 {
                    self.submit_next();
                }
            } else if self.hold {
                self.cancel_order(self.current.unwrap(), Some(ClientId::from("PREDICT")), None)?;
            }
        }
        Ok(())
    }
}
nautilus_strategy!(Continuous, {
    fn on_order_submitted(&mut self, _event: OrderSubmitted) {
        self.journal.borrow_mut().submitted.push(Instant::now());
    }
    fn on_order_accepted(&mut self, event: OrderAccepted) {
        self.probe.strategy_callback(&event);
        self.journal
            .borrow_mut()
            .rows
            .push(json!({"type":"accepted","id":event.client_order_id.to_string()}));
        if !self.taker && !self.hold {
            self.probe
                .cancel_decision(event.client_order_id, Instant::now());
            self.cancel_order(event.client_order_id, Some(ClientId::from("PREDICT")), None)
                .unwrap();
        }
    }
    fn on_order_canceled(&mut self, event: &OrderCanceled) {
        self.probe.strategy_callback(event);
        self.journal.borrow_mut().callbacks.push(Instant::now());
        self.journal
            .borrow_mut()
            .rows
            .push(json!({"type":"canceled","id":event.client_order_id.to_string()}));
        self.round += 1;
        if !self.hold {
            self.submit_next();
        }
    }
    fn on_order_filled(&mut self, event: &OrderFilled) {
        self.probe.strategy_callback(event);
        self.journal.borrow_mut().callbacks.push(Instant::now());
        self.journal.borrow_mut().rows.push(json!({"type":"filled","id":event.client_order_id.to_string(),"trade":event.trade_id.to_string(),"qty":event.last_qty.to_string()}));
        if self
            .cache()
            .order(&event.client_order_id)
            .unwrap()
            .is_closed()
        {
            self.round += 1;
            self.submit_next();
        }
    }
    fn on_order_denied(&mut self, event: OrderDenied) {
        panic!("continuous Native denial: {}", event.reason);
    }
    fn on_order_rejected(&mut self, event: OrderRejected) {
        panic!("continuous venue rejection: {}", event.reason);
    }
});

fn count(journal: &Rc<RefCell<Journal>>, kind: &str) -> usize {
    journal
        .borrow()
        .rows
        .iter()
        .filter(|row| row["type"] == kind)
        .count()
}
fn notices(journal: &Rc<RefCell<Journal>>, status: &str) -> usize {
    journal
        .borrow()
        .rows
        .iter()
        .filter(|row| row["type"] == "settlement" && row["status"] == status)
        .count()
}
fn open_reads(server: &Server) -> usize {
    server
        .state
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|row| row.get("status").is_some_and(|s| s == "OPEN"))
        .count()
}
pub(super) fn send_book(public: &PublicState) {
    public.0.lock().unwrap()[0].send(Message::Text(json!({"type":"M","topic":"predictOrderbook/42","data":{"marketId":42,"updateTimestampMs":get_atomic_clock_realtime().get_time_ns().as_u64()/1_000_000,"orderCount":2,"bids":[["0.019","10000"]],"asks":[["0.020","10000"]]}}).to_string().into())).unwrap();
}
fn ack(server: &Server, body: &Value, index: usize, outcome: u8) {
    let mut event = exit_orders::wire_event(body, "orderAccepted", outcome, Decimal::ZERO);
    event["orderId"] = json!((10001 + index).to_string());
    server.wallet(event);
}
fn success(body: &Value, index: usize, size: u32, cumulative: u32, id: &str) -> Value {
    let mut event = exit_orders::settlement(body, 1, size, cumulative, "0.020", id, false);
    event["orderId"] = json!((10001 + index).to_string());
    event["fee"]["amountWei"] = json!("0");
    event
}

async fn run(taker: bool, rounds: usize) -> Value {
    run_case(taker, rounds, false).await
}

async fn run_case(taker: bool, rounds: usize, check_submitted_open: bool) -> Value {
    run_active(taker, rounds, check_submitted_open, 0).await
}

async fn run_active(
    taker: bool,
    rounds: usize,
    check_submitted_open: bool,
    background: usize,
) -> Value {
    run_interleaved(taker, rounds, check_submitted_open, background, false).await
}

async fn run_interleaved(
    taker: bool,
    rounds: usize,
    check_submitted_open: bool,
    background: usize,
    held_refresh: bool,
) -> Value {
    assert!(!held_refresh || !taker);
    let HostFixture {
        backing,
        public,
        config: profile,
        task,
        ..
    } = fixture(true, rounds + 1).await;
    // Unique numeric venue IDs make routing retention part of this continuous run.
    backing
        .state
        .create_order_ids
        .lock()
        .unwrap()
        .extend((0..rounds + 1).map(|n| (10001 + n).to_string()));
    let external = usize::from(held_refresh);
    if held_refresh {
        let mut row = rest_order_data("OPEN", 1, OrderSide::Buy);
        row["order"]["hash"] = json!(format!("0x{}", "aa".repeat(32)));
        row["order"]["makerAmount"] = json!("1000000000000000000");
        row["id"] = json!("90001");
        backing.state.open_orders.lock().unwrap().push(row);
    }
    backing.state.rpc_frame.lock().unwrap().0 =
        (if taker { 1000u128 } else { 2 } + background as u128 + external as u128)
            * 1_000_000_000_000_000_000;
    let profile = host::NodeProfile::parse(&profile).unwrap();
    let (probe, mut collector) = PredictLatency::channel(trader_id(), account_id(), 16384).unwrap();
    let mut exec = config(&backing);
    exec.latency = Some(probe.clone());
    exec.account_state_mode = PredictAccountStateMode::Reported;
    exec.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", backing.http).into(),
    ));
    exec.orderbook_removal_enabled = true;
    exec.submission = Some(nautilus_predict::PredictSubmissionConfig {
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
    // An explicit benchmark control; ordinary tests retain Native's INFO default.
    let stdout_level = std::env::var("PREDICT_CONTINUOUS_LOG_LEVEL")
        .unwrap_or_else(|_| "INFO".into())
        .parse::<log::LevelFilter>()
        .expect("PREDICT_CONTINUOUS_LOG_LEVEL must be a Native log level");
    // Native still performs risk checks. Only the deterministic replay's rate budget is raised.
    let mut node = nautilus_live::node::LiveNode::builder(
        trader_id(),
        nautilus_common::enums::Environment::Live,
    )
    .unwrap()
    .with_logging(nautilus_common::logging::logger::LoggerConfig {
        stdout_level,
        ..Default::default()
    })
    .with_risk_engine_config(nautilus_live::node::config::LiveRiskEngineConfig {
        max_order_submit_rate: "10000/00:00:01".into(),
        ..Default::default()
    })
    .with_exec_engine_config(nautilus_live::node::config::LiveExecutionEngineConfig {
        reconciliation: false,
        inflight_check_interval_ms: 0,
        open_check_interval_secs: None,
        position_check_interval_secs: None,
        ..Default::default()
    })
    .with_delay_post_stop_secs(0)
    .add_data_client(
        Some("PREDICT".into()),
        Box::new(nautilus_predict::PredictDataClientFactory::new()),
        Box::new(nautilus_predict::PredictDataClientConfig {
            api_key: KEY.into(),
            market_ids: vec![42],
            base_url_http: profile.predict.http_url,
            base_url_ws: profile.predict.public_ws_url,
            http_timeout_secs: 3,
            ws_timeout_secs: 3,
            dynamic_refresh_secs: 30,
            transport_backend: TransportBackend::Tungstenite,
        }),
    )
    .unwrap()
    .add_exec_client(
        Some("PREDICT".into()),
        Box::new(nautilus_predict::PredictExecutionClientFactory::new()),
        Box::new(exec),
    )
    .unwrap()
    .build()
    .unwrap();
    let target = Rc::new(RefCell::new(Journal::default()));
    let other = Rc::new(RefCell::new(Journal::default()));
    node.add_strategy(Continuous::new(
        instrument_id(1),
        rounds,
        taker,
        false,
        target.clone(),
        probe.clone(),
    ))
    .unwrap();
    node.add_strategy(Continuous::new(
        instrument_id(2),
        1,
        false,
        true,
        other.clone(),
        probe,
    ))
    .unwrap();
    let cache = node.kernel().cache.clone();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    for index in 0..background {
        let mut order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(trader_id())
            .strategy_id(StrategyId::from("CONT-BACKGROUND-003"))
            .client_order_id(format!("BACKGROUND-{index}").into())
            .instrument_id(instrument_id(1))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100))
            .price(Price::from("0.010"))
            .build();
        let hash = VenueOrderId::new(format!("0x{:064x}", index + 1));
        order
            .apply(factory.generate_order_submitted(&order, 1.into()))
            .unwrap();
        order
            .apply(factory.generate_order_accepted(&order, hash, 2.into(), 2.into()))
            .unwrap();
        cache
            .borrow_mut()
            .add_order(order, None, Some(ClientId::from("PREDICT")), false)
            .unwrap();
    }
    if background > 0 {
        cache.borrow_mut().build_index();
    }
    let engine = node.kernel().exec_engine.clone();
    let handle = node.handle();
    let mut cancel_post_ns = Vec::new();
    let mut fill_post_ns = Vec::new();
    let mut cancel_admitted_ns = Vec::new();
    let mut fill_admitted_ns = Vec::new();
    let mut submitted_to_post_ns = Vec::new();
    let mut submitted_callback_after_post = 0;
    let mut cleanup = Value::Null;
    let driver = async {
        barrier("continuous public subscribed", || {
            !public.0.lock().unwrap().is_empty()
        })
        .await;
        send_book(&public);
        barrier(
            "first Submitted and POST precede the second strategy",
            || {
                backing.state.creations.lock().unwrap().len() == 1
                    && target.borrow().submitted.len() == 1
            },
        )
        .await;
        assert_eq!(
            cache
                .borrow()
                .account(&account_id())
                .unwrap()
                .balance_locked(Some(Currency::USDT()))
                .unwrap()
                .as_decimal(),
            Decimal::from(background + external),
            "Cash RiskEngine owns local Submitted reservation; reported cash must not deduct it twice"
        );
        if check_submitted_open {
            let first = backing.state.creations.lock().unwrap()[0].clone();
            let mut row = live_node::rest_row(&first, "OPEN");
            row["id"] = json!("10001");
            backing.state.open_orders.lock().unwrap().push(row);
            let before = open_reads(&backing);
            let account_event = cache
                .borrow()
                .account(&account_id())
                .unwrap()
                .last_event()
                .unwrap()
                .event_id;
            engine
                .borrow()
                .get_client(&ClientId::from("PREDICT"))
                .unwrap()
                .query_account(QueryAccount::new(
                    trader_id(),
                    Some(ClientId::from("PREDICT")),
                    account_id(),
                    nautilus_core::UUID4::new(),
                    get_atomic_clock_realtime().get_time_ns(),
                    None,
                    None,
                ))
                .unwrap();
            barrier(
                "explicit account query consumes a known Submitted venue OPEN row",
                || {
                    open_reads(&backing) > before
                        && cache
                            .borrow()
                            .account(&account_id())
                            .unwrap()
                            .last_event()
                            .unwrap()
                            .event_id
                            != account_event
                },
            )
            .await;
            assert_eq!(
                cache
                    .borrow()
                    .account(&account_id())
                    .unwrap()
                    .balance_locked(Some(Currency::USDT()))
                    .unwrap()
                    .as_decimal(),
                Decimal::from(background + external),
                "the registered local hash must not be reclassified as an unmanaged OPEN reservation"
            );
        }
        send_book(&public);
        barrier("continuous initial two orders", || {
            backing.state.creations.lock().unwrap().len() == 2
        })
        .await;
        let initial = backing.state.creations.lock().unwrap().clone();
        let companion = initial
            .iter()
            .position(|body| body["data"]["order"]["tokenId"] == "22")
            .unwrap();
        ack(&backing, &initial[companion], companion, 2);
        barrier("companion remains accepted", || {
            count(&other, "accepted") == 1
        })
        .await;
        if !taker {
            barrier(
                "companion is OPEN; Native risk still reserves the target Submitted order",
                || {
                    cache
                        .borrow()
                        .account(&account_id())
                        .unwrap()
                        .balance_locked(Some(Currency::USDT()))
                        .unwrap()
                        .as_decimal()
                        == Decimal::from(background + external + 1)
                },
            )
            .await;
        }
        let refresh_gate = Arc::new(tokio::sync::Notify::new());
        let refresh_rpc_start = backing.state.rpc_methods.lock().unwrap().len();
        if held_refresh {
            let open_before = open_reads(&backing);
            *backing.state.rpc_latest_gate.lock().unwrap() = Some(refresh_gate.clone());
            engine
                .borrow()
                .get_client(&ClientId::from("PREDICT"))
                .unwrap()
                .query_account(QueryAccount::new(
                    trader_id(),
                    Some(ClientId::from("PREDICT")),
                    account_id(),
                    UUID4::new(),
                    get_atomic_clock_realtime().get_time_ns(),
                    None,
                    None,
                ))
                .unwrap();
            barrier("remote OPEN collected while cash RPC remains held", || {
                open_reads(&backing) > open_before
                    && backing.state.rpc_methods.lock().unwrap().len() > refresh_rpc_start
                    && backing.state.rpc_latest_gate.lock().unwrap().is_none()
            })
            .await;
        }
        let baseline_open = open_reads(&backing);
        let baseline_rpc = backing.state.rpc_methods.lock().unwrap().len();
        // This Node uses backing RPC directly, so park the existing mock RPC gate for maker mode.
        let gate = Arc::new(tokio::sync::Notify::new());
        if !taker && !held_refresh {
            *backing.state.rpc_gate.lock().unwrap() = Some(gate.clone());
        }
        for round in 0..rounds {
            barrier("callback creates next order without a new book", || {
                backing
                    .state
                    .creations
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|body| body["data"]["order"]["tokenId"] == "11")
                    .count()
                    > round
            })
            .await;
            let all = backing.state.creations.lock().unwrap().clone();
            let index = all
                .iter()
                .enumerate()
                .filter(|(_, body)| body["data"]["order"]["tokenId"] == "11")
                .nth(round)
                .unwrap()
                .0;
            let body = &all[index];
            if !taker || round > 0 {
                ack(&backing, body, index, 1);
            }
            if taker {
                if round == 0 {
                    let mut failed = success(body, index, 1, 0, "continuous-failed-attempt");
                    failed["type"] = json!("orderTransactionSubmitted");
                    failed["kind"] = json!("PURCHASE");
                    failed.as_object_mut().unwrap().remove("fee");
                    failed.as_object_mut().unwrap().remove("isMaker");
                    backing.wallet(failed.clone());
                    barrier("matched pending attempt before failure", || {
                        notices(&target, "Submitted") == 1
                    })
                    .await;
                    assert_eq!(count(&target, "filled"), 0);
                    failed["type"] = json!("orderTransactionFailed");
                    backing.wallet(failed.clone());
                    backing.wallet(failed);
                    barrier("failed settlement is a strategy fact, not a Filled", || {
                        notices(&target, "Failed") == 1
                    })
                    .await;
                    assert_eq!(count(&target, "filled"), 0);
                }
                let parts = if round == 0 {
                    vec![(40, 40), (60, 100)]
                } else {
                    vec![(100, 100)]
                };
                for (part, (size, cumulative)) in parts.into_iter().enumerate() {
                    let expected_fills = if round == 0 { part } else { round + 1 };
                    let expected_matches = expected_fills + 2; // one earlier failed attempt
                    let id = format!("continuous-{round}-{part}");
                    let terminal = success(body, index, size, cumulative, &id);
                    let mut submitted = terminal.clone();
                    submitted["type"] = json!("orderTransactionSubmitted");
                    submitted["kind"] = json!(if round.is_multiple_of(2) {
                        "PURCHASE"
                    } else {
                        "SALE"
                    });
                    submitted.as_object_mut().unwrap().remove("fee");
                    submitted.as_object_mut().unwrap().remove("isMaker");
                    backing.wallet(submitted.clone());
                    backing.wallet(submitted.clone());
                    barrier("early match is visible before settlement", || {
                        notices(&target, "Submitted") == expected_matches
                    })
                    .await;
                    assert_eq!(
                        count(&target, "filled"),
                        expected_fills,
                        "Submitted must not apply a Filled"
                    );
                    assert_eq!(
                        notices(&other, "Submitted"),
                        0,
                        "account/strategy topic isolation"
                    );
                    let debit = if round.is_multiple_of(2) {
                        u128::from(cumulative) * 20_000_000_000_000_000
                    } else {
                        0
                    };
                    *backing.state.rpc_frame.lock().unwrap() = (
                        (1000 + background as u128) * 1_000_000_000_000_000_000u128 - debit,
                        (round * 2 + part) as u64 + 2,
                        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000 + 1,
                    );
                    backing.wallet(terminal.clone());
                    backing.wallet(terminal);
                    barrier("actual strategy fill callback", || {
                        count(&target, "filled") == expected_fills + 1
                    })
                    .await;
                    barrier("confirmed notification", || {
                        notices(&target, "Confirmed") == expected_fills + 1
                    })
                    .await;
                    backing.wallet(submitted); // late pending cannot downgrade a confirmed settlement
                    if cumulative < 100 {
                        assert_eq!(
                            backing
                                .state
                                .creations
                                .lock()
                                .unwrap()
                                .iter()
                                .filter(|body| body["data"]["order"]["tokenId"] == "11")
                                .count(),
                            round + 1,
                            "a partial fill must not create another entry/exit"
                        );
                    }
                }
                if round + 1 < rounds {
                    barrier("fill callback immediately sends next command", || {
                        target.borrow().submitted.len() > round + 1
                            && backing
                                .state
                                .creations
                                .lock()
                                .unwrap()
                                .iter()
                                .filter(|body| body["data"]["order"]["tokenId"] == "11")
                                .count()
                                > round + 1
                    })
                    .await;
                    let next_index = backing
                        .state
                        .creations
                        .lock()
                        .unwrap()
                        .iter()
                        .enumerate()
                        .filter(|(_, body)| body["data"]["order"]["tokenId"] == "11")
                        .nth(round + 1)
                        .unwrap()
                        .0;
                    let posted = backing.state.create_times.lock().unwrap()[next_index];
                    let callback = *target.borrow().callbacks.last().unwrap();
                    let admitted = target.borrow().submitted[round + 1];
                    fill_admitted_ns.push((admitted - callback).as_nanos() as u64);
                    if let Some(duration) = posted.checked_duration_since(admitted) {
                        submitted_to_post_ns.push(duration.as_nanos() as u64);
                    } else {
                        submitted_callback_after_post += 1;
                    }
                    assert!(callback <= posted);
                    fill_post_ns.push((posted - callback).as_nanos() as u64);
                }
            } else {
                barrier("actual strategy canceled callback", || {
                    count(&target, "canceled") == round + 1
                })
                .await;
                if round + 1 < rounds {
                    barrier("cancel callback replacement POST", || {
                        target.borrow().submitted.len() > round + 1
                            && backing
                                .state
                                .creations
                                .lock()
                                .unwrap()
                                .iter()
                                .filter(|body| body["data"]["order"]["tokenId"] == "11")
                                .count()
                                > round + 1
                    })
                    .await;
                    let confirmed = backing.state.removed_times.lock().unwrap()[round];
                    let next_index = backing
                        .state
                        .creations
                        .lock()
                        .unwrap()
                        .iter()
                        .enumerate()
                        .filter(|(_, body)| body["data"]["order"]["tokenId"] == "11")
                        .nth(round + 1)
                        .unwrap()
                        .0;
                    let posted = backing.state.create_times.lock().unwrap()[next_index];
                    let admitted = target.borrow().submitted[round + 1];
                    cancel_admitted_ns.push((admitted - confirmed).as_nanos() as u64);
                    if let Some(duration) = posted.checked_duration_since(admitted) {
                        submitted_to_post_ns.push(duration.as_nanos() as u64);
                    } else {
                        submitted_callback_after_post += 1;
                    }
                    cancel_post_ns.push((posted - confirmed).as_nanos() as u64);
                }
            }
        }
        assert_eq!(
            open_reads(&backing),
            baseline_open,
            "continuous callbacks must not collect OPEN"
        );
        if !taker {
            assert_eq!(
                backing.state.rpc_methods.lock().unwrap().len(),
                baseline_rpc,
                "cancel replacements must do zero RPCs"
            );
        }
        if held_refresh {
            let before = cache
                .borrow()
                .account_ref(&account_id())
                .unwrap()
                .last_event()
                .unwrap()
                .event_id;
            refresh_gate.notify_one();
            barrier(
                "fresh query cash applies after callback-immediate replacements",
                || {
                    let cache = cache.borrow();
                    let account = cache.account_ref(&account_id()).unwrap();
                    account.last_event().unwrap().event_id != before
                        && account
                            .balance_locked(Some(Currency::USDT()))
                            .unwrap()
                            .as_decimal()
                            == Decimal::from(background + external + 1)
                },
            )
            .await;
            assert_eq!(
                backing.state.rpc_methods.lock().unwrap().len() - refresh_rpc_start,
                3,
                "the explicit query completes once, without lifecycle-induced retries"
            );
            let rows = backing.state.open_orders.lock().unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0]["id"], "90001",
                "target cancellation must preserve the external OPEN order"
            );
        }
        send_book(&public);
        barrier("companion cleanup", || count(&other, "canceled") == 1).await;
        assert_eq!(
            notices(&target, "Submitted"),
            if taker { rounds + 2 } else { 0 }
        );
        assert_eq!(
            cache
                .borrow()
                .orders_open(None, None, None, None, None)
                .len(),
            background
        );
        assert_eq!(
            cache
                .borrow()
                .positions_open(None, None, None, None, None)
                .len(),
            0
        );
        let before = cache
            .borrow()
            .orders_total_count(None, None, None, None, None);
        let purge_started = Instant::now();
        cache.borrow_mut().purge_closed_orders(
            get_atomic_clock_realtime().get_time_ns() + nautilus_core::DurationNanos::from_secs(1),
            0,
        );
        assert_eq!(
            cache
                .borrow()
                .orders_total_count(None, None, None, None, None),
            background,
            "Native purge bounds retained order history"
        );
        let order_purge_ns = purge_started.elapsed().as_nanos() as u64;
        assert!(before > rounds);
        let position_purge_started = Instant::now();
        cache.borrow_mut().purge_closed_positions(
            get_atomic_clock_realtime().get_time_ns() + nautilus_core::DurationNanos::from_secs(1),
            0,
        );
        assert_eq!(
            cache
                .borrow()
                .positions_total_count(None, None, None, None, None),
            0
        );
        let position_purge_ns = position_purge_started.elapsed().as_nanos() as u64;
        let worker_cleanup_started = Instant::now();
        // Explicit Native query is a lifecycle cleanup boundary, after measured callbacks.
        *backing.state.rpc_gate.lock().unwrap() = None;
        engine
            .borrow()
            .get_client(&ClientId::from("PREDICT"))
            .unwrap()
            .query_account(QueryAccount::new(
                trader_id(),
                Some(ClientId::from("PREDICT")),
                account_id(),
                nautilus_core::UUID4::new(),
                get_atomic_clock_realtime().get_time_ns(),
                None,
                None,
            ))
            .unwrap();
        cleanup = json!({"native_orders_before":before,"native_order_purge_ns":order_purge_ns,"native_position_purge_ns":position_purge_ns,"query_account_cleanup_boundary_ns":worker_cleanup_started.elapsed().as_nanos() as u64});
        gate.notify_waiters();
        handle.stop();
    };
    timeout(Duration::from_secs(60), async {
        let (result, ()) = tokio::join!(node.run(), driver);
        result.unwrap();
    })
    .await
    .unwrap();
    let snapshot = collector.snapshot();
    if taker {
        assert_eq!(
            snapshot.distributions[&LatencyMetric::WsFillToStrategyCallback].completed_observations,
            (rounds + 1) as u64
        );
        assert_eq!(
            snapshot.distributions[&LatencyMetric::WsMatchToStrategyCallback]
                .completed_observations,
            (rounds + 2) as u64
        );
    }
    assert_eq!(snapshot.counters["create_calls"], (rounds + 1) as u64);
    assert_eq!(snapshot.counters["metadata_hit"], (rounds + 1) as u64);
    assert_eq!(snapshot.counters["metadata_miss"], 0);
    assert_eq!(snapshot.counters["orderbook_requests"], 0);
    assert_eq!(
        snapshot.counters["remove_calls"],
        if taker { 1 } else { (rounds + 1) as u64 }
    );
    assert_eq!(snapshot.dropped_samples, 0);
    assert_eq!(snapshot.invalid_intervals, 0);
    node.dispose();
    task.abort();
    json!({"source":"one warmed Native LiveNode, two real Rust strategies, loopback only; callback-immediate commands, no next-signal wait","taker":taker,"rounds":rounds,"stdout_level":stdout_level.to_string(),"background_active_orders":background,"cleanup":cleanup,"callbacks":target.borrow().rows,"cancel_confirmation_to_post_ns":cancel_post_ns,"fill_callback_to_post_ns":fill_post_ns,"cancel_confirmation_to_native_submitted_callback_ns":cancel_admitted_ns,"fill_callback_to_native_submitted_callback_ns":fill_admitted_ns,"native_submitted_callback_to_post_ns":submitted_to_post_ns,"submitted_callback_after_post":submitted_callback_after_post,"metrics":snapshot,"final_native_orders_after_purge":background,"final_native_open_orders":background,"final_native_positions":0})
}
#[tokio::test(flavor = "current_thread")]
async fn warmed_maker_replaces_inside_cancel_callback_with_tight_cash_and_no_rpc() {
    run(false, 10).await;
}
#[tokio::test(flavor = "current_thread")]
async fn known_submitted_rest_open_does_not_double_cash_risk_reservation() {
    run_case(false, 2, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn warmed_market_buy_sell_delivers_100_early_and_filled_strategy_callbacks() {
    run(true, 100).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit warmed callback capture; PREDICT_CONTINUOUS_PATH is create_new JSON"]
async fn benchmark_warmed_callback_immediate_maker_and_taker() {
    let sessions = vec![run(false, 100).await, run(true, 200).await];
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_CONTINUOUS_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file, &json!({"sessions":sessions})).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn maker_replacement_keeps_other_native_active_orders_and_tight_cash() {
    run_active(false, 10, false, 100).await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit active-order callback scaling; PREDICT_ACTIVE_PATH is create_new JSON"]
async fn benchmark_active_order_callback_scaling() {
    let mut sessions = Vec::new();
    for background in [1, 100, 1000, 10000] {
        sessions.push(run_active(false, 100, false, background).await);
        sessions.push(run_active(true, 100, false, background).await);
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_ACTIVE_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file, &json!({"source":"actual Rust strategies; synthetic venue; known accepted background orders are preserved, not canceled; setup excluded from measured spans", "sessions":sessions})).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn maker_replacement_keeps_external_open_during_suspended_account_refresh() {
    run_interleaved(false, 20, true, 1000, true).await;
}
