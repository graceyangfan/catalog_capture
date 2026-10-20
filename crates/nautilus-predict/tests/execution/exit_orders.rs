// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------
//! Inventory-backed native SELL command regressions; all endpoints are loopback.
use nautilus_model::identifiers::ClientOrderId;

use super::*;

#[tokio::test]
async fn native_sell_limit_market_inventory_approval_reservations_and_minimum() {
    for case in [
        "limit",
        "market",
        "no",
        "inventory",
        "approval",
        "reserved",
        "minimum",
        "future",
        "quiet",
        "depth",
        "reduce_only",
        "zero_cash",
    ] {
        let private = server(vec![]).await;
        let outcome = if case == "no" { 2 } else { 1 };
        let observed = if case == "quiet" {
            103_714_000_000
        } else {
            OBSERVED
        };
        let (mut client, mut rx, cache) =
            submissions::setup_client_with_policy(&private, observed, false);
        let portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(outcome)))
            .unwrap();
        if case == "quiet" {
            *private.state.rpc_frame.lock().unwrap() = (100_000_000_000_000_000_000u128, 2, 103);
        }
        if case == "zero_cash" {
            *private.state.rpc_frame.lock().unwrap() = (0, 1, 2);
        }
        connect_reported(&mut client, &mut rx).await;
        *private.state.sell_inventory.lock().unwrap() =
            U256::from(if case == "inventory" { 99u128 } else { 200 })
                * U256::from(1_000_000_000_000_000_000u64);
        *private.state.sell_approved.lock().unwrap() = case != "approval";
        *private.state.market_fee.lock().unwrap() = 200;
        *private.state.execution_book.lock().unwrap() = json!({"marketId":42,"updateTimestampMs":if case == "future" {99} else {2},
            "bids":[[if case == "minimum" {"0.009"} else {"0.020"},if case == "depth" {"99"} else {"1000"}]],"asks":[["0.980","1000"]]});
        if case == "reserved" {
            let mut reserved = rest_order_data("OPEN", outcome, OrderSide::Sell);
            reserved["order"]["makerAmount"] = json!("201000000000000000000");
            private.state.open_orders.lock().unwrap().push(reserved);
        }
        let kind = if matches!(case, "limit" | "reduce_only") {
            OrderType::Limit
        } else {
            OrderType::Market
        };
        let order = OrderTestBuilder::new(kind)
            .trader_id(trader_id())
            .instrument_id(instrument_id(outcome))
            .client_order_id(ClientOrderId::new("SELL-ADMISSION-001"))
            .side(OrderSide::Sell)
            .quantity(Quantity::from(100))
            .price(Price::from("0.020"))
            .post_only(kind == OrderType::Limit)
            .reduce_only(case == "reduce_only")
            .build();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();
        client.submit_order(submissions::submit(&order)).unwrap();
        let mut engine = submissions::engine(&cache);
        let success = matches!(case, "limit" | "market" | "no" | "zero_cash" | "quiet");
        timeout(Duration::from_secs(3), async {
            loop {
                if let Ok(event) = rx.try_recv() {
                    submissions::apply(&mut engine, &event);
                }
                let status = cache
                    .borrow()
                    .order(&order.client_order_id())
                    .unwrap()
                    .status();
                if matches!(status, OrderStatus::Denied | OrderStatus::Rejected)
                    || success && !private.state.creations.lock().unwrap().is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("SELL case timed out: {case}"));
        let posts = private.state.creations.lock().unwrap().clone();
        assert_eq!(posts.len(), usize::from(success), "{case}");
        if success {
            assert_eq!(posts[0]["data"]["order"]["side"], 1);
            assert_eq!(
                posts[0]["data"]["order"]["makerAmount"],
                "100000000000000000000"
            );
            assert_eq!(
                posts[0]["data"]["order"]["takerAmount"],
                if kind == OrderType::Limit {
                    "2000000000000000000"
                } else {
                    "1980000000000000000"
                }
            );
            assert_eq!(
                posts[0]["data"]["strategy"],
                if kind == OrderType::Limit {
                    "LIMIT"
                } else {
                    "MARKET"
                }
            );
            assert_eq!(
                posts[0]["data"]["isPostOnly"],
                if kind == OrderType::Limit {
                    json!(true)
                } else {
                    Value::Null
                }
            );
        }
        client.disconnect().await.unwrap();
        drop(portfolio);
    }
}

#[path = "../support/exit_strategy.rs"]
pub(super) mod strategy;

#[derive(Clone)]
pub(super) struct PublicState(pub(super) Arc<Mutex<Vec<mpsc::UnboundedSender<Message>>>>);
pub(super) async fn exit_socket(
    ws: WebSocketUpgrade,
    State(state): State<PublicState>,
) -> Response {
    ws.on_upgrade(move |socket|async move {
        let (mut sink,mut stream)=socket.split();
        let (tx,mut rx)=mpsc::unbounded_channel();
        loop {
            tokio::select! {
                msg=stream.next()=>match msg {
                    Some(Ok(Message::Text(text)))=>{
                        let cmd:Value=serde_json::from_str(&text).unwrap();
                        if cmd["method"]=="subscribe" {
                            sink.send(Message::Text(json!({"type":"R","requestId":cmd["requestId"],"data":{"success":true}}).to_string().into())).await.unwrap();
                            state.0.lock().unwrap().push(tx.clone());
                        }
                    },
                    Some(Ok(Message::Ping(_)|Message::Pong(_)))=>{},
                    _=>break,
                },
                Some(msg)=rx.recv()=>{if sink.send(msg).await.is_err(){break;}},
            }
        }
    })
}

pub(super) fn assert_no_submission_failure(journal: &Rc<RefCell<Vec<Value>>>) {
    let rows = journal.borrow();
    let rejected = rows
        .iter()
        .find(|row| row["type"] == "Rejected" || row["type"] == "Denied");
    assert!(
        rejected.is_none(),
        "unexpected Native admission failure before HTTP: {rejected:?}"
    );
}

pub(super) async fn barrier(label: &str, mut predicate: impl FnMut() -> bool) {
    timeout(Duration::from_secs(5), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("exit replay causal barrier timed out: {label}"));
}

pub(super) fn wire_event(body: &Value, kind: &str, outcome: u8, filled: Decimal) -> Value {
    let data = &body["data"];
    let side = data["order"]["side"].as_u64().unwrap();
    let px = Decimal::from_str_exact(data["pricePerShare"].as_str().unwrap()).unwrap()
        / Decimal::from(1_000_000_000_000_000_000u64);
    let qty_wei = if side == 1 {
        &data["order"]["makerAmount"]
    } else {
        &data["order"]["takerAmount"]
    };
    let qty = Decimal::from_str_exact(qty_wei.as_str().unwrap()).unwrap()
        / Decimal::from(1_000_000_000_000_000_000u64);
    let mut row = event(kind, outcome);
    row["orderHash"] = data["order"]["hash"].clone();
    row["orderId"] = json!("12345");
    row["timestamp"] = json!(
        nautilus_core::time::get_atomic_clock_realtime()
            .get_time_ns()
            .as_u64()
            / 1_000_000
    );
    row["details"]["strategyType"] = data["strategy"].clone();
    row["details"]["quoteType"] = json!(if side == 1 { "ASK" } else { "BID" });
    row["details"]["price"] = json!(px.to_string());
    row["details"]["quantity"] = json!(qty.to_string());
    row["details"]["quantityFilled"] = json!(filled.to_string());
    row["details"]["value"] = json!((px * qty).to_string());
    row
}

pub(super) fn settlement(
    body: &Value,
    outcome: u8,
    size: u32,
    cumulative: u32,
    px: &str,
    id: &str,
    maker: bool,
) -> Value {
    let mut row = wire_event(
        body,
        "orderTransactionSuccess",
        outcome,
        Decimal::from(cumulative),
    );
    let size = Decimal::from(size);
    let px = Decimal::from_str_exact(px).unwrap();
    let value = size * px;
    let fee = if maker {
        Decimal::ZERO
    } else {
        value * Decimal::new(2, 2)
    };
    let wei = |v: Decimal| {
        (v * Decimal::from(1_000_000_000_000_000_000u64))
            .normalize()
            .to_string()
    };
    row["settlementId"] = json!(id);
    row["isMaker"] = json!(maker);
    row["fee"] = json!({"type":"COLLATERAL","amountWei":wei(fee)});
    row["fill"] = json!({"executedSizeWei":wei(size),"executedPriceWei":wei(px),"executedValueWei":wei(value)});
    row
}

#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_maker_take_profit_cancel_taker_stop_and_duplicate_replay() {
    run_exit(false, false, false, false).await;
    run_exit(true, false, false, false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_cancel_rejection_never_overlaps_taker_sell() {
    run_exit(false, true, false, false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_subminimum_stop_reports_rejection_without_overselling() {
    run_exit(false, false, true, false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_full_take_profit_fill_during_cancel_suppresses_stop() {
    run_exit(false, false, false, true).await;
}

async fn run_exit(profit: bool, cancel_reject: bool, minimum: bool, full_race: bool) -> Value {
    run_exit_case(profit, cancel_reject, minimum, full_race, true).await
}

fn fixture_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/exit_strategy_synthetic_20261015.json")
}

fn send_wallet(private: &Server, wallet: &mut Vec<Value>, generated: Value, saved: Option<&Value>) {
    let event = if let Some(saved) = saved {
        let source = saved["walletEvents"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| {
                row["type"] == generated["type"]
                    && row["details"]["strategyType"] == generated["details"]["strategyType"]
                    && row["settlementId"] == generated["settlementId"]
            })
            .expect("saved economic event missing");
        let mut event = source.clone();
        // Bind only transport identity and wall time. All economic terms and settlement
        // identities are read from the retained JSON, not regenerated for replay.
        for field in ["orderHash", "orderId", "timestamp"] {
            event[field] = generated[field].clone();
        }
        assert_eq!(event["details"], generated["details"]);
        assert_eq!(event["fill"], generated["fill"]);
        assert_eq!(event["fee"], generated["fee"]);
        event
    } else {
        generated
    };
    wallet.push(event.clone());
    private.wallet(event.clone());
    private.wallet(event);
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline synthetic capture; loopback endpoints and public test key only"]
async fn record_exit_strategy_json() {
    let mut sessions = vec![];
    for (profit, cancel_reject, minimum, full_race) in [
        (false, false, false, false),
        (true, false, false, false),
        (false, true, false, false),
        (false, false, true, false),
        (false, false, false, true),
    ] {
        sessions.push(run_exit_case(profit, cancel_reject, minimum, full_race, false).await);
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(fixture_path())
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic offline Rust Strategy + LiveNode; no operator credentials, no mainnet orders","sessions":sessions})).unwrap();
}

pub(super) async fn run_exit_case(
    profit: bool,
    cancel_reject: bool,
    minimum: bool,
    full_race: bool,
    from_saved: bool,
) -> Value {
    let saved = if from_saved {
        let fixture: Value =
            serde_json::from_slice(&std::fs::read(fixture_path()).unwrap()).unwrap();
        Some(
            fixture["sessions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| {
                    r["profit"] == profit
                        && r["cancelRejected"] == cancel_reject
                        && r["subminimum"] == minimum
                        && r["fullFillDuringCancel"] == full_race
                })
                .unwrap()
                .clone(),
        )
    } else {
        None
    };

    use nautilus_core::time::get_atomic_clock_realtime;
    use nautilus_live::node::config::LiveExecutionEngineConfig;
    use nautilus_predict::{PredictDataClientConfig, PredictSubmissionConfig};
    let private = server(vec![]).await;
    let now = get_atomic_clock_realtime().get_time_ns();
    *private.state.rpc_frame.lock().unwrap() = (
        1_000_000_000_000_000_000_000,
        1,
        now.as_u64() / 1_000_000_000,
    );
    *private.state.sell_approved.lock().unwrap() = true;
    *private.state.market_fee.lock().unwrap() = 200;
    let mut cfg = config(&private);
    cfg.account_state_mode = PredictAccountStateMode::Reported;
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", private.http).into(),
    ));
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
        max_order_usdt: Decimal::from(2),
        max_total_usdt: Decimal::from(2),
        require_post_only: false,
        market_slippage_bps: 100,
    });
    let public = PublicState(Arc::new(Mutex::new(vec![])));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(
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
            base_url_http: Some(format!("http://{address}")),
            base_url_ws: Some(format!("ws://{address}/ws")),
            http_timeout_secs: 2,
            ws_timeout_secs: 2,
            dynamic_refresh_secs: 30,
            transport_backend: TransportBackend::Tungstenite,
        },
        cfg,
        LiveExecutionEngineConfig {
            reconciliation: false,
            inflight_check_interval_ms: 0,
            open_check_interval_secs: None,
            position_check_interval_secs: Some(0.05),
            position_check_threshold_ms: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let cache = live.kernel().cache.clone();
    let entry_id = ClientOrderId::new("EXIT-ENTRY-001");
    let mut entry = OrderTestBuilder::new(OrderType::Market)
        .trader_id(trader_id())
        .strategy_id(nautilus_model::identifiers::StrategyId::new(
            "PREDICT-EXIT-001",
        ))
        .instrument_id(instrument_id(1))
        .client_order_id(entry_id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from(200))
        .build();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    entry
        .apply(factory.generate_order_submitted(&entry, now))
        .unwrap();
    entry
        .apply(factory.generate_order_accepted(&entry, VenueOrderId::new(HASH), now, now))
        .unwrap();
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    cache
        .borrow_mut()
        .add_order(entry, None, None, false)
        .unwrap();
    let journal = Rc::new(RefCell::new(vec![]));
    live.add_strategy(
        strategy::ExitStrategy::new(
            instrument_id(1),
            1,
            journal.clone(),
            strategy::ExitSettings::parse(include_str!(
                "../../../../examples/predict-exit-strategy.toml.example"
            ))
            .unwrap(),
            200,
        )
        .with_live_identity(42, account_id(), 0),
    )
    .unwrap();
    let handle = live.handle();
    let mut wallet = vec![];
    let mut books = vec![];
    let driver = async {
        barrier("line 184", || {
            !public.0.lock().unwrap().is_empty()
                && !private.state.sockets.lock().unwrap().is_empty()
        })
        .await;
        for (index, size) in [(1, 67u128), (2, 133)] {
            let body = json!({"data":{"strategy":"MARKET","pricePerShare":"10000000000000000","order":{"side":0,"hash":HASH,"takerAmount":"200000000000000000000"}}});
            let mut event = settlement(
                &body,
                1,
                size as u32,
                if index == 1 { 67 } else { 200 },
                "0.010",
                &format!("exit-entry-{index}"),
                false,
            );
            event["fee"] =
                json!({"type":"SHARES","amountWei":(size*20_000_000_000_000_000u128).to_string()});
            {
                let mut block = private.state.rpc_frame.lock().unwrap();
                block.0 -= size * 10_000_000_000_000_000u128;
                block.1 += 1;
                block.2 = get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000 + 1;
            }
            send_wallet(&private, &mut wallet, event, saved.as_ref());
            barrier("line 191", || {
                cache
                    .borrow()
                    .order(&entry_id)
                    .unwrap()
                    .filled_qty()
                    .as_decimal()
                    >= Decimal::from(if index == 1 { 67 } else { 200 })
            })
            .await;
        }
        let history = wallet
            .iter()
            .map(|event| {
                let mut row = matches::match_row(1);
                row["taker"]["price"] = event["fill"]["executedPriceWei"].clone();
                row["taker"]["amount"] = event["fill"]["executedSizeWei"].clone();
                row["taker"]["fee"] = json!({"type":"SHARES","amount":event["fee"]["amountWei"]});
                row["settlementId"] = event["settlementId"].clone();
                row["executedAt"] =
                    json!(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true));
                row
            })
            .collect::<Vec<_>>();
        private.state.match_pages.lock().unwrap().extend(vec![
            (
                200,
                json!({"success":true,"data":history,"cursor":null}),
                0
            );
            500
        ]);
        let mut row = holding(1);
        row["amount"] = json!("196000000000000000000");
        private
            .state
            .pages
            .lock()
            .unwrap()
            .extend(vec![json!({"success":true,"data":[row]}); 500]);
        *private.state.sell_inventory.lock().unwrap() =
            U256::from(196u128) * U256::from(1_000_000_000_000_000_000u64);
        barrier("line 206", || {
            cache
                .borrow()
                .positions_open(None, Some(&instrument_id(1)), None, None, None)
                .first()
                .is_some_and(|p| p.quantity.as_decimal() == Decimal::from(196))
        })
        .await;
        let send_book = |bid: &str| {
            let mut book = json!({"marketId":42,"updateTimestampMs":get_atomic_clock_realtime().get_time_ns().as_u64()/1_000_000,"orderCount":2,"bids":[[bid,"1000"]],"asks":[["0.014","1000"]]});
            if let Some(saved) = &saved {
                let source = saved["orderbooks"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|r| r["bids"][0][0] == bid)
                    .unwrap();
                let timestamp = book["updateTimestampMs"].clone();
                book = source.clone();
                book["updateTimestampMs"] = timestamp;
            }
            *private.state.execution_book.lock().unwrap() = book.clone();
            for tx in public.0.lock().unwrap().iter() {
                tx.send(Message::Text(
                    json!({"type":"M","topic":"predictOrderbook/42","data":book})
                        .to_string()
                        .into(),
                ))
                .unwrap();
            }
            book
        };
        books.push(send_book("0.012"));
        barrier("line 214", || {
            private.state.creations.lock().unwrap().len() == 1
        })
        .await;
        let maker = private.state.creations.lock().unwrap()[0].clone();
        assert_eq!(maker["data"]["strategy"], "LIMIT");
        assert_eq!(maker["data"]["isPostOnly"], true);
        assert_eq!(
            maker["data"]["order"]["makerAmount"],
            "196000000000000000000"
        );
        assert_eq!(maker["data"]["pricePerShare"], "13000000000000000");
        let mut rest = super::live_node::rest_row(&maker, "OPEN");
        rest["amount"] = json!("196");
        rest["amountFilled"] = json!("0");
        private
            .state
            .orders
            .lock()
            .unwrap()
            .push_back(json!({"success":true,"data":rest.clone()}));
        private.state.open_orders.lock().unwrap().push(rest);
        let accepted = wire_event(&maker, "orderAccepted", 1, Decimal::ZERO);
        send_wallet(&private, &mut wallet, accepted, saved.as_ref());
        barrier("line 223", || {
            cache
                .borrow()
                .orders(None, None, None, None, None)
                .iter()
                .any(|o| o.order_side() == OrderSide::Sell && o.status() == OrderStatus::Accepted)
        })
        .await;
        if profit {
            let event = settlement(&maker, 1, 196, 196, "0.013", "exit-profit", true);
            send_wallet(&private, &mut wallet, event, saved.as_ref());
            private.state.open_orders.lock().unwrap().clear();
            {
                let mut block = private.state.rpc_frame.lock().unwrap();
                block.0 += 2_548_000_000_000_000_000;
                block.1 += 1;
                block.2 = get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000 + 1;
            }
        } else {
            let cancel_gate = Arc::new(tokio::sync::Notify::new());
            if !cancel_reject {
                *private.state.remove_gate.lock().unwrap() = Some(cancel_gate.clone());
            }
            private.state.remove_replies.lock().unwrap().push_back((if cancel_reject {400} else {200},if cancel_reject {json!({"success":false,"code":400,"error":"bad_request","message":"fixture cancel rejected"})} else {json!({"success":true,"removed":["12345"],"noop":[]})},0));
            books.push(send_book(if minimum { "0.005" } else { "0.008" }));
            barrier("line 234", || {
                !private.state.removals.lock().unwrap().is_empty()
            })
            .await;
            if cancel_reject {
                barrier("line 236", || {
                    journal
                        .borrow()
                        .iter()
                        .any(|r| r["type"] == "CancelRejected")
                })
                .await;
                assert_eq!(private.state.creations.lock().unwrap().len(), 1);
                handle.stop();
                return;
            }
            // Fill while removal is awaiting its reply; the native cancel barrier must
            // recalculate remaining inventory, and a full fill suppresses the stop.
            let size = if full_race { 196 } else { 46 };
            let event = settlement(&maker, 1, size, size, "0.013", "exit-cancel-race", true);
            send_wallet(&private, &mut wallet, event, saved.as_ref());
            *private.state.sell_inventory.lock().unwrap() =
                U256::from(196 - size) * U256::from(1_000_000_000_000_000_000u64);
            {
                let mut block = private.state.rpc_frame.lock().unwrap();
                block.0 += u128::from(size) * 13_000_000_000_000_000;
                block.1 += 1;
                block.2 = get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000 + 1;
            }
            let mut row = holding(1);
            row["amount"] = json!(((196 - size) as u128 * 1_000_000_000_000_000_000).to_string());
            {
                let mut pages = private.state.pages.lock().unwrap();
                pages.clear();
                pages.extend(vec![json!({"success":true,"data":[row]}); 500]);
            }
            barrier("line 249", || {
                cache
                    .borrow()
                    .positions_open(None, Some(&instrument_id(1)), None, None, None)
                    .first()
                    .map_or(Decimal::ZERO, |p| p.quantity.as_decimal())
                    == Decimal::from(196 - size)
            })
            .await;
            cancel_gate.notify_one();
            if minimum {
                // Native risk now owns the instrument minimum-notional check.
                barrier(
                    "Native denies the subminimum stop before submission",
                    || {
                        journal.borrow().iter().any(|r| {
                            r["type"] == "Denied"
                                && r["reason"]
                                    .as_str()
                                    .is_some_and(|reason| reason.contains("NOTIONAL_BELOW_MINIMUM"))
                        })
                    },
                )
                .await;
                assert_eq!(private.state.creations.lock().unwrap().len(), 1);
                assert_eq!(
                    cache
                        .borrow()
                        .positions_open(None, Some(&instrument_id(1)), None, None, None)[0]
                        .quantity
                        .as_decimal(),
                    Decimal::from(150)
                );
                barrier("subminimum retains updated reported cash", || {
                    cache.borrow().account(&account_id()).is_some_and(|a| {
                        a.balance_total(Some(Currency::USDT()))
                            .unwrap()
                            .as_decimal()
                            == Decimal::new(998598, 3)
                    })
                })
                .await;
                handle.stop();
                return;
            }
            if !full_race {
                barrier("line 258", || {
                    private.state.creations.lock().unwrap().len() == 2
                })
                .await;
                let stop = private.state.creations.lock().unwrap()[1].clone();
                assert_eq!(stop["data"]["strategy"], "MARKET");
                assert_eq!(stop["data"]["slippageBps"], "100");
                assert_eq!(stop["data"]["isMinAmountOut"], false);
                assert!(stop["data"].get("isPostOnly").is_none());
                assert_eq!(
                    stop["data"]["order"]["makerAmount"],
                    "150000000000000000000"
                );
                assert_eq!(stop["data"]["order"]["takerAmount"], "1188000000000000000");
                let event = wire_event(&stop, "orderAccepted", 1, Decimal::ZERO);
                send_wallet(&private, &mut wallet, event, saved.as_ref());
                let event = settlement(&stop, 1, 150, 150, "0.008", "exit-stop", false);
                send_wallet(&private, &mut wallet, event, saved.as_ref());
                {
                    let mut block = private.state.rpc_frame.lock().unwrap();
                    block.0 += 1_176_000_000_000_000_000;
                    block.1 += 1;
                    block.2 =
                        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000 + 1;
                }
            }
        }
        *private.state.sell_inventory.lock().unwrap() = U256::ZERO;
        {
            let mut pages = private.state.pages.lock().unwrap();
            pages.clear();
            pages.extend(vec![json!({"success":true,"data":[]}); 500]);
        }
        barrier("line 271", || {
            cache
                .borrow()
                .positions_open(None, Some(&instrument_id(1)), None, None, None)
                .is_empty()
        })
        .await;
        let final_cash = if profit || full_race {
            Decimal::from_str_exact("1000.548").unwrap()
        } else {
            Decimal::from_str_exact("999.774").unwrap()
        };
        barrier("line 273", || {
            cache.borrow().account(&account_id()).is_some_and(|a| {
                a.balance_total(Some(Currency::USDT()))
                    .unwrap()
                    .as_decimal()
                    == final_cash
            })
        })
        .await;
        handle.stop();
    };
    let watchdog = async {
        tokio::time::sleep(Duration::from_secs(15)).await;
        handle.stop();
    };
    tokio::select! {
        result=async{let (result,())=tokio::join!(live.run(),driver);result.unwrap();}=>{let ()=result;},
        ()=watchdog=>panic!("exit replay watchdog: {:?}",journal.borrow()),
    }
    task.abort();
    let report = json!({"source":"synthetic loopback Rust strategy and LiveNode; no mainnet transactions","profit":profit,"cancelRejected":cancel_reject,"subminimum":minimum,"fullFillDuringCancel":full_race,
        "walletEvents":wallet,"orderbooks":books,"requests":private.state.creations.lock().unwrap().clone(),"removals":private.state.removals.lock().unwrap().clone(),"nativeFeedback":journal.borrow().clone(),
        "remainingVenueOrders":private.state.open_orders.lock().unwrap().len(),
        "activeNativeOrders":cache.borrow().orders_open(None,Some(&instrument_id(1)),None,None,None).len(),
        "finalPositions":cache.borrow().positions_open(None,Some(&instrument_id(1)),None,None,None).iter().map(|p|p.quantity.to_string()).collect::<Vec<_>>(),"finalCash":cache.borrow().account(&account_id()).unwrap().balance_total(Some(Currency::USDT())).unwrap().as_decimal().to_string()});
    assert!(
        journal
            .borrow()
            .iter()
            .any(|r| r["type"] == "MakerTakeProfitSubmitted")
    );
    assert!(
        journal
            .borrow()
            .iter()
            .any(|r| r["type"] == "NativeAccountReported")
    );
    let creates = private.state.creations.lock().unwrap();
    assert_eq!(
        creates.len(),
        if profit || cancel_reject || minimum || full_race {
            1
        } else {
            2
        }
    );
    if !profit && !cancel_reject {
        let events = journal.borrow();
        let trigger = events
            .iter()
            .position(|r| r["type"] == "StopTriggered")
            .unwrap();
        let cancel = events
            .iter()
            .position(|r| r["type"] == "CancelTakeProfitRequested")
            .unwrap();
        assert!(trigger < cancel);
        if !full_race {
            let canceled = events.iter().position(|r| r["type"] == "Canceled").unwrap();
            let taker = events
                .iter()
                .position(|r| r["type"] == "TakerStopSubmitted")
                .unwrap();
            assert!(cancel < canceled && canceled < taker);
            assert_eq!(events[taker]["quantity"], "150.0000000000000000");
        }
    }
    if profit || full_race || !cancel_reject && !minimum {
        let cache = cache.borrow();
        let positions = cache.positions(None, Some(&instrument_id(1)), None, None, None);
        assert_eq!(positions.len(), 1);
        assert!(positions[0].is_closed());
        assert_eq!(
            positions[0].realized_pnl.unwrap().as_decimal(),
            // Entry inventory cost and the actual exit commission are each included once.
            if profit || full_race {
                Decimal::new(548, 3)
            } else {
                Decimal::new(-226, 3)
            }
        );
    }
    if cancel_reject {
        assert!(
            journal
                .borrow()
                .iter()
                .any(|r| r["callback"] == "on_order_cancel_rejected")
        );
    }
    if minimum {
        assert_eq!(report["finalCash"], "998.59800000");
        assert!(journal.borrow().iter().any(|r| {
            r["reason"]
                .as_str()
                .is_some_and(|v| v.contains("NOTIONAL_BELOW_MINIMUM"))
        }));
    }
    report
}
