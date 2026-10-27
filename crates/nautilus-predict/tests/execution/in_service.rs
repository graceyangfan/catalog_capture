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

//! Actual Native-thread pauses while an independent worker sends market data.
use super::*;
use nautilus_common::actor::DataActor;
use nautilus_model::{
    data::CustomData, enums::LiquiditySide, events::OrderEventAny, position::Position,
};
use nautilus_predict::{PredictOrderbookSnapshot, predict_orderbook_data_type};
use nautilus_trading::{StrategyCore, nautilus_strategy};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

#[derive(Debug)]
struct PauseObserver {
    core: StrategyCore,
    samples: Rc<RefCell<Vec<[u64; 2]>>>,
    owner: std::thread::ThreadId,
    settlements: Rc<std::cell::Cell<usize>>,
}
impl DataActor for PauseObserver {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_data(
            nautilus_predict::predict_settlement_data_type(
                account_id(),
                "MAINT-HISTORY-999".into(),
            ),
            None,
            None,
        );
        self.subscribe_data(
            predict_orderbook_data_type(42),
            Some(ClientId::from("PREDICT")),
            None,
        );
        Ok(())
    }
    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.unsubscribe_data(
            nautilus_predict::predict_settlement_data_type(
                account_id(),
                "MAINT-HISTORY-999".into(),
            ),
            None,
            None,
        );
        self.unsubscribe_data(
            predict_orderbook_data_type(42),
            Some(ClientId::from("PREDICT")),
            None,
        );
        Ok(())
    }
    fn on_data(&mut self, data: &CustomData) -> anyhow::Result<()> {
        assert_eq!(std::thread::current().id(), self.owner);
        if data
            .data
            .as_any()
            .is::<nautilus_predict::PredictSettlementUpdate>()
        {
            self.settlements.set(self.settlements.get() + 1);
            return Ok(());
        }
        let book = data
            .data
            .as_any()
            .downcast_ref::<PredictOrderbookSnapshot>()
            .unwrap();
        self.samples.borrow_mut().push([
            book.ts_init.as_u64(),
            get_atomic_clock_realtime().get_time_ns().as_u64(),
        ]);
        Ok(())
    }
}
nautilus_strategy!(PauseObserver, {});

async fn run_pause_case(owners: usize, eligible: usize) -> Value {
    run_pause_case_with_history(owners, eligible, 0).await
}

async fn run_pause_case_with_history(
    owners: usize,
    eligible: usize,
    fills_per_order: usize,
) -> Value {
    run_pause_case_with_evidence(owners, eligible, fills_per_order, None).await
}

#[expect(
    clippy::await_holding_refcell_ref,
    reason = "setup only: no order commands/fills; accounts use Portfolio; Native wrapper owns reports"
)]
async fn run_pause_case_with_evidence(
    owners: usize,
    eligible: usize,
    fills_per_order: usize,
    evidence_mode: Option<bool>,
) -> Value {
    let HostFixture {
        backing,
        public,
        config: profile,
        task,
        ..
    } = fixture(true, 1).await;
    let profile = host::NodeProfile::parse(&profile).unwrap();
    backing.state.rpc_frame.lock().unwrap().0 = 1000 * 1_000_000_000_000_000_000;
    let mut exec = config(&backing);
    exec.account_state_mode = PredictAccountStateMode::Reported;
    exec.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", backing.http).into(),
    ));
    exec.reconnect_reconciliation_interval_secs =
        (fills_per_order == 0 || evidence_mode.is_some()).then_some(1);
    exec.account_refresh_interval_secs = if fills_per_order == 0 || evidence_mode.is_some() {
        1
    } else {
        300
    };
    if evidence_mode.is_some() {
        exec.max_report_pages = owners * fills_per_order / 100 + 2;
        exec.request_timeout_secs = 120;
    }
    assert!(exec.submission_signer.is_none() && !exec.orderbook_removal_enabled);
    let mut node = nautilus_live::node::LiveNode::builder(
        trader_id(),
        nautilus_common::enums::Environment::Live,
    )
    .unwrap()
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
    let samples = Rc::new(RefCell::new(Vec::new()));
    let owner = std::thread::current().id();
    let settlements = Rc::new(std::cell::Cell::new(0));
    node.add_strategy(PauseObserver {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some("PREDICT-PAUSE-001".into()),
            order_id_tag: Some("P".into()),
            ..Default::default()
        }),
        samples: samples.clone(),
        owner,
        settlements: settlements.clone(),
    })
    .unwrap();
    let cache = node.kernel().cache.clone();
    let engine = node.kernel().exec_engine.clone();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    let as_of = get_atomic_clock_realtime().get_time_ns();
    if fills_per_order > 0 {
        // Restored Native histories/positions; this case measures disposal only,
        // not settlement recovery against an empty mock REST history.
        backing.state.rpc_frame.lock().unwrap().2 = as_of.as_u64() / 1_000_000_000;
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
    }

    let oldest = as_of.as_u64() - 3_600_000_000_000 - eligible as u64 * 600_000_000;
    let mut match_rows = Vec::new();
    // Many short, canceled maker orders are the primary retention workload.
    for i in 0..owners {
        let close = if eligible == owners {
            as_of.as_u64() - 3_601_000_000_000
        } else {
            oldest + (i as u64 + 1) * 600_000_000
        };
        let mut order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(trader_id())
            .strategy_id("MAINT-HISTORY-999".into())
            .client_order_id(format!("MAINT-{i}").as_str().into())
            .instrument_id(instrument_id(1))
            .side(OrderSide::Buy)
            .quantity(Quantity::from((fills_per_order + 100) as u64))
            .price(Price::from("0.010"))
            .build();
        let hash = VenueOrderId::new(format!("0x{:064x}", i + 1));
        order
            .apply(
                factory
                    .generate_order_submitted(&order, (close - fills_per_order as u64 - 2).into()),
            )
            .unwrap();
        order
            .apply(factory.generate_order_accepted(
                &order,
                hash,
                (close - fills_per_order as u64 - 1).into(),
                (close - fills_per_order as u64 - 1).into(),
            ))
            .unwrap();
        let position_id = format!("MAINT-POSITION-{i}").as_str().into();
        let mut position: Option<Position> = None;
        for index in 0..fills_per_order {
            let timestamp = (close - fills_per_order as u64 + index as u64).into();
            let event = factory.generate_order_filled(
                &order,
                hash,
                Some(position_id),
                if evidence_mode.is_some() {
                    settlement_trade_id(
                        &format!("MAINT-FILL-{i}-{index}"),
                        hash.as_str().parse().unwrap(),
                    )
                } else {
                    format!("MAINT-FILL-{i}-{index}").as_str().into()
                },
                Quantity::from(1),
                Price::from("0.010"),
                Currency::USDT(),
                Some(Money::zero(Currency::USDT())),
                if evidence_mode.is_some() {
                    LiquiditySide::Taker
                } else {
                    LiquiditySide::Maker
                },
                timestamp,
                timestamp,
            );
            let OrderEventAny::Filled(fill) = &event else {
                unreachable!()
            };
            if let Some(position) = &mut position {
                position.apply(fill);
            } else {
                position = Some(Position::new(
                    &InstrumentAny::BinaryOption(instrument(1)),
                    fill.clone(),
                ));
            }
            order.apply(event).unwrap();
            if evidence_mode.is_some() {
                let mut row = super::matches::match_row(1);
                row["taker"]["hash"] = json!(hash.as_str());
                row["taker"]["amount"] = json!("1000000000000000000");
                row["taker"]["price"] = json!("10000000000000000");
                row["taker"]["fee"] = json!({"type":"COLLATERAL","amount":"0"});
                row["amountFilled"] = json!("1000000000000000000");
                row["priceExecuted"] = json!("10000000000000000");
                row["settlementId"] = json!(format!("MAINT-FILL-{i}-{index}"));
                row["executedAt"] = json!(
                    chrono::DateTime::from_timestamp_nanos(timestamp.as_u64() as i64).to_rfc3339()
                );
                match_rows.push(row);
            }
        }
        if let Some(mut position) = position {
            // The closing order was already retired; its fill remains in Native
            // position history, as it can with independent retention schedules.
            let OrderEventAny::Filled(mut closing) = factory.generate_order_filled(
                &order,
                VenueOrderId::new(format!("0x{:064x}", owners + i + 1)),
                Some(position_id),
                format!("MAINT-CLOSE-{i}").as_str().into(),
                Quantity::from(fills_per_order as u64),
                Price::from("0.010"),
                Currency::USDT(),
                Some(Money::zero(Currency::USDT())),
                LiquiditySide::Maker,
                (close + 1).into(),
                (close + 1).into(),
            ) else {
                unreachable!()
            };
            closing.client_order_id = format!("MAINT-CLOSE-{i}").as_str().into();
            closing.order_side = OrderSide::Sell;
            position.apply(&closing);
            assert!(position.is_closed());
            cache
                .borrow_mut()
                .add_position(&position, OmsType::Netting)
                .unwrap();
        }
        order
            .apply(factory.generate_order_canceled(&order, Some(hash), close.into(), close.into()))
            .unwrap();
        let position_id = order.position_id();
        cache
            .borrow_mut()
            .add_order(order, position_id, None, false)
            .unwrap();
    }
    cache.borrow_mut().build_index();
    let handle = node.handle();
    let sent = Arc::new(AtomicU64::new(0));
    let producer_owner = Arc::new(Mutex::new(None));
    let mut operations = Vec::new();
    let mut timer_probes = 0usize;
    let timer_clock = node.kernel().clock.clone();
    let driver = async {
        barrier("maintenance public socket connected", || {
            !public.0.lock().unwrap().is_empty()
        })
        .await;
        let wire = public.clone();
        let count = sent.clone();
        let worker = producer_owner.clone();
        let producer = tokio::spawn(async move {
            *worker.lock().unwrap() = Some(std::thread::current().id());
            let mut cadence = tokio::time::interval(Duration::from_millis(1));
            cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                cadence.tick().await;
                continuous::send_book(&wire);
                count.fetch_add(1, Ordering::Release);
            }
        });
        barrier("actual strategy receives continuous data", || {
            samples.borrow().len() >= 8
        })
        .await;
        assert_ne!(
            *producer_owner.lock().unwrap(),
            Some(owner),
            "feed production must not share the paused Native thread"
        );
        if let Some(warm) = evidence_mode {
            let total = match_rows.len();
            let chunks = match_rows.chunks(100).collect::<Vec<_>>();
            for (index, rows) in chunks.iter().enumerate() {
                backing.state.match_pages.lock().unwrap().push_back((200,json!({"success":true,"data":rows,"cursor":if index+1==chunks.len() { None } else { Some(format!("history-{index}")) }}),0));
            }
            let request = GenerateFillReports::new(
                UUID4::new(),
                get_atomic_clock_realtime().get_time_ns(),
                Some(instrument_id(1)),
                None,
                None,
                None,
                None,
                None,
            );
            // The public live wrapper owns report collection/finalization.
            // Setup has no order commands/fills; account events use Portfolio.
            let start_ns = get_atomic_clock_realtime().get_time_ns().as_u64();
            let reports = {
                let engine = engine.borrow();
                engine
                    .get_client(&ClientId::from("PREDICT"))
                    .unwrap()
                    .generate_fill_reports(request)
                    .await
                    .unwrap()
            };
            assert_eq!(
                reports.len(),
                total,
                "all raw facts populated through actual REST parsing"
            );
            drop(reports);
            operations.push(json!({"stage":"fill_report_collection_and_finish_window","start_ns":start_ns,"end_ns":get_atomic_clock_realtime().get_time_ns().as_u64(),"scope":"public Native report call including worker IO, core finalization and result disposal; not CPU-only time"}));
            if warm {
                let start_ns = get_atomic_clock_realtime().get_time_ns().as_u64();
                for i in 0..owners {
                    let mut ack = maintenance_wallet("orderCancelled", i, fills_per_order);
                    backing.wallet(ack.clone());
                    ack["type"] = json!("orderTransactionSuccess");
                    ack["settlementId"] = json!(format!("MAINT-FILL-{i}-{}", fills_per_order - 1));
                    ack["fill"] = json!({"executedSizeWei":"1000000000000000000","executedPriceWei":"10000000000000000","executedValueWei":"10000000000000000"});
                    ack["fee"] = json!({"type":"COLLATERAL","amountWei":"0"});
                    ack["isMaker"] = json!(false);
                    backing.wallet(ack);
                }
                barrier(
                    "warm lifecycle followed by actual strategy settlement marker",
                    || settlements.get() == owners,
                )
                .await;
                operations.push(json!({"stage":"wallet_history_validation_window","start_ns":start_ns,"end_ns":get_atomic_clock_realtime().get_time_ns().as_u64(),"scope":"wallet dispatch through strategy settlement marker; not CPU-only time"}));
                assert!(
                    engine
                        .borrow()
                        .get_client(&ClientId::from("PREDICT"))
                        .unwrap()
                        .is_connected()
                );
            }
            drop(match_rows);
        }
        if fills_per_order == 0 {
            let subscriptions = backing.state.ws_requests.lock().unwrap().len();
            let recovery_start = get_atomic_clock_realtime().get_time_ns().as_u64();
            backing
                .state
                .sockets
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .send(Message::Close(None))
                .unwrap();
            barrier("wallet discontinuity is visible", || {
                !engine
                    .borrow()
                    .get_client(&ClientId::from("PREDICT"))
                    .unwrap()
                    .is_connected()
            })
            .await;
            barrier("wallet subscription reached the independent server", || {
                backing.state.ws_requests.lock().unwrap().len() > subscriptions
            })
            .await;
            let start_ns = get_atomic_clock_realtime().get_time_ns().as_u64();
            let started = Instant::now();
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
            operations.push(json!({"stage":"query_account_core","start_ns":start_ns,"end_ns":get_atomic_clock_realtime().get_time_ns().as_u64(),"elapsed_ns":started.elapsed().as_nanos() as u64}));
            barrier(
                "Native readiness finishes with retained maker histories",
                || {
                    engine
                        .borrow()
                        .get_client(&ClientId::from("PREDICT"))
                        .unwrap()
                        .is_connected()
                },
            )
            .await;
            operations.push(json!({"stage":"recovery_window","start_ns":recovery_start,"end_ns":get_atomic_clock_realtime().get_time_ns().as_u64()}));
        }
        let start_ns = get_atomic_clock_realtime().get_time_ns().as_u64();
        let started = Instant::now();
        cache.borrow_mut().purge_closed_orders(as_of, 3600);
        operations.push(json!({"stage":"native_purge","start_ns":start_ns,"end_ns":get_atomic_clock_realtime().get_time_ns().as_u64(),"elapsed_ns":started.elapsed().as_nanos() as u64}));
        assert_eq!(
            cache
                .borrow()
                .orders_total_count(None, None, None, None, None),
            owners - eligible
        );
        if fills_per_order > 0 && evidence_mode.is_none() {
            let start_ns = get_atomic_clock_realtime().get_time_ns().as_u64();
            let started = Instant::now();
            let definition = cache
                .borrow()
                .instrument(&instrument_id(1))
                .unwrap()
                .clone();
            // LiveExecutionClient delegates instrument callbacks synchronously.
            // In manual mode this executes the actual adapter retirement path.
            engine
                .borrow_mut()
                .get_client_adapter_mut(&ClientId::from("PREDICT"))
                .unwrap()
                .on_instrument(definition);
            operations.push(json!({"stage":"adapter_instrument_retirement_core","start_ns":start_ns,"end_ns":get_atomic_clock_realtime().get_time_ns().as_u64(),"elapsed_ns":started.elapsed().as_nanos() as u64}));
        }
        if fills_per_order > 0 {
            assert_eq!(
                cache
                    .borrow()
                    .positions_closed(None, None, None, None, None)
                    .len(),
                owners
            );
            let start_ns = get_atomic_clock_realtime().get_time_ns().as_u64();
            let started = Instant::now();
            cache.borrow_mut().purge_closed_positions(as_of, 3600);
            operations.push(json!({"stage":"native_position_purge","start_ns":start_ns,"end_ns":get_atomic_clock_realtime().get_time_ns().as_u64(),"elapsed_ns":started.elapsed().as_nanos() as u64}));
            assert!(
                cache
                    .borrow()
                    .positions(None, None, None, None, None)
                    .is_empty()
            );
        }
        if evidence_mode.is_some() {
            let name = "predict-execution-maintenance-PREDICT";
            let next_tick = timer_clock
                .borrow()
                .next_time_ns(name)
                .expect("real recovery timer installed");
            let start_ns = get_atomic_clock_realtime().get_time_ns().as_u64();
            barrier("installed live recovery timer advances after purge", || {
                timer_clock
                    .borrow()
                    .next_time_ns(name)
                    .is_some_and(|next| next > next_tick)
            })
            .await;
            let account_reads = backing.state.rpc_methods.lock().unwrap().len();
            *backing.state.rpc_failure.lock().unwrap() = true;
            // Clock advancement can precede dispatch of its queued callback.
            // Historical ACK probes do not warm prefixes or mutate a retained order.
            let mut probes = tokio::time::interval(Duration::from_millis(10));
            timeout(Duration::from_secs(5), async {
                while backing.state.rpc_methods.lock().unwrap().len() == account_reads {
                    probes.tick().await;
                    backing.wallet(maintenance_wallet("orderAccepted", 0, fills_per_order));
                    timer_probes += 1;
                }
            })
            .await
            .expect("actual timer must retire the owner before the unknown-ACK query");
            operations.push(json!({"stage":"live_timer_retirement_window","start_ns":start_ns,"end_ns":get_atomic_clock_realtime().get_time_ns().as_u64(),"scope":"wall-clock waiting window; not CPU callback duration"}));
        }
        let after = samples.borrow().len();
        barrier("post-maintenance data reaches actual strategy", || {
            samples.borrow().len() >= after + 8
        })
        .await;
        producer.abort();
        let _ = producer.await;
        let expected = sent.load(Ordering::Acquire) as usize;
        barrier("all produced snapshots reach the strategy", || {
            samples.borrow().len() == expected
        })
        .await;
        assert!(backing.state.creations.lock().unwrap().is_empty());
        assert!(backing.state.removals.lock().unwrap().is_empty());
        assert!(
            cache
                .borrow()
                .orders_open(None, None, None, None, None)
                .is_empty()
        );
        assert!(
            cache
                .borrow()
                .positions_open(None, None, None, None, None)
                .is_empty()
        );
        handle.stop();
    };
    timeout(
        Duration::from_secs(if evidence_mode.is_some() { 120 } else { 20 }),
        async {
            let (result, ()) = tokio::join!(node.run(), driver);
            result.unwrap();
        },
    )
    .await
    .unwrap();
    let callbacks = samples.borrow().clone();
    assert!(callbacks.iter().all(|row| row[1] >= row[0]));
    let result = json!({"source":"real Rust strategy and Native LiveNode on owner thread; independent worker sends 1ms loopback book updates; old canceled orders preloaded before connect; startup excluded; operations explicitly listed; large-history case restores Native fills/positions and measures disposal without REST recovery","owners":owners,"eligible_orders":eligible,"retained_orders":owners-eligible,"fills_per_order":fills_per_order,"evidence_mode":evidence_mode,"rest_fact_count":if evidence_mode.is_some() { owners*fills_per_order } else { 0 },"rest_match_requests":backing.state.match_requests.lock().unwrap().len(),"settlement_markers":settlements.get(),"timer_probes":timer_probes,"operations":operations,"callbacks_receive_and_strategy_ns":callbacks,"sent_frames":sent.load(Ordering::Acquire),"account_requests":*backing.state.account_requests.lock().unwrap(),"rpc_requests":backing.state.rpc_methods.lock().unwrap().len(),"order_requests":backing.state.creations.lock().unwrap().len(),"remove_requests":backing.state.removals.lock().unwrap().len()});
    node.dispose();
    task.abort();
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_service_native_maintenance_preserves_actual_strategy_delivery() {
    run_pause_case(200, 100).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit in-service strategy pause capture; PREDICT_INSERVICE_PAUSE_PATH create_new JSON"]
async fn benchmark_in_service_strategy_pauses() {
    let sessions = vec![
        run_pause_case(6100, 100).await,
        run_pause_case(10000, 10000).await,
        run_pause_case_with_history(1, 1, 100000).await,
    ];
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_INSERVICE_PAUSE_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file, &json!({"sessions":sessions})).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_retired_history_preserves_actual_strategy_delivery_and_position_cleanup() {
    run_pause_case_with_history(1, 1, 1000).await;
}

fn maintenance_wallet(kind: &str, owner: usize, fills: usize) -> Value {
    let mut ack = event(kind, 1);
    ack["timestamp"] = json!(if kind == "orderAccepted" {
        1
    } else {
        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000
    });
    ack["orderHash"] = json!(format!("0x{:064x}", owner + 1));
    ack["orderId"] = json!((owner + 1).to_string());
    ack["details"]["quantity"] = json!((fills + 100).to_string());
    ack["details"]["quantityFilled"] = json!(fills.to_string());
    ack["details"]["price"] = json!("0.010");
    ack["details"]["value"] = json!((Decimal::from(fills + 100) / Decimal::from(100)).to_string());
    ack["details"]["valueFilled"] = json!((Decimal::from(fills) / Decimal::from(100)).to_string());
    ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_timer_retires_rest_evidence_with_cold_and_verified_histories() {
    for warm in [false, true] {
        run_pause_case_with_evidence(1, 1, 100, Some(warm)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit real timer/evidence strategy capture; PREDICT_LIVE_TIMER_PATH create_new JSON"]
async fn benchmark_real_timer_evidence_strategy_delivery() {
    let mut sessions = Vec::new();
    for warm in [false, true] {
        sessions.push(run_pause_case_with_evidence(1, 1, 100000, Some(warm)).await);
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_LIVE_TIMER_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"REST-populated raw evidence; actual LiveClock recovery timer; independent WS books and real Rust strategy; timer window is not CPU callback duration", "sessions":sessions})).unwrap();
}
