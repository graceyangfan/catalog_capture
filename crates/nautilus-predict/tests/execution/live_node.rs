// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Actual LiveNode + RiskEngine + Rust Strategy + Predict HTTP/WebSocket loopback replay.
use super::*;
#[path = "../support/strategy.rs"]
pub(super) mod strategy;
use nautilus_common::enums::Environment;
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_live::node::{LiveNode, config::LiveExecutionEngineConfig};
use nautilus_predict::{
    PredictDataClientConfig, PredictDataClientFactory, PredictSubmissionConfig,
};
use strategy::{CalibrationInputs, CalibrationStrategy, Observations, Phase, StrategySettings};

pub(super) async fn public_market() -> Response {
    let mut reply: Value =
        serde_json::from_str(include_str!("../../test_data/market_2859346.json")).unwrap();
    let row = &mut reply["data"];
    row["id"] = json!(42);
    row["title"] = json!("Replay calibration");
    row["tradingStatus"] = json!("OPEN");
    row["status"] = Value::Null;
    row["decimalPrecision"] = json!(3);
    row["isNegRisk"] = json!(false);
    row["isYieldBearing"] = json!(false);
    row["feeRateBps"] = json!(0);
    row.as_object_mut().unwrap().remove("variantData");
    for (i, token) in ["11", "22"].into_iter().enumerate() {
        row["outcomes"][i]["onChainId"] = json!(token);
        row["outcomes"][i]["status"] = Value::Null;
    }
    json_response(reply)
}

#[derive(Clone, Copy)]
enum ReplayBook {
    Rebound(u8),
    CapturedResting,
}

async fn public_socket(ws: WebSocketUpgrade, State(source): State<ReplayBook>) -> Response {
    ws.on_upgrade(move |socket| async move {
        let (mut sink, mut stream) = socket.split();
        let captured_quotes = if matches!(source, ReplayBook::CapturedResting) {
            let feedback: Value = serde_json::from_str(include_str!(
                "../fixtures/private_api/live_resting_fixed_20261015/native_feedback.json"
            )).unwrap();
            let decision = feedback.as_array().unwrap().iter()
                .find(|row| row["type"] == "SubmissionDecision").unwrap();
            let no_bid = Decimal::from_str_exact(decision["bestBid"].as_str().unwrap()).unwrap();
            let no_ask = Decimal::from_str_exact(decision["bestAsk"].as_str().unwrap()).unwrap();
            Some(((Decimal::ONE-no_ask).to_string(), (Decimal::ONE-no_bid).to_string()))
        } else {None};
        let mut subscribed = false;
        let mut frames = 0;
        let mut tick = tokio::time::interval(Duration::from_millis(75));
        loop {
            tokio::select! {
                message = stream.next() => match message {
                    Some(Ok(Message::Text(text))) => {
                        let command: Value = serde_json::from_str(&text).unwrap();
                        if command["method"] == "subscribe" {
                            assert_eq!(command["params"], json!(["predictOrderbook/42"]));
                            subscribed = true;
                            if sink.send(Message::Text(json!({"type":"R","requestId":command["requestId"],"data":{"success":true}}).to_string().into())).await.is_err() { break; }
                        } else if command["method"] == "unsubscribe" { subscribed = false; }
                    },
                    Some(Ok(Message::Ping(_)|Message::Pong(_))) => {},
                    _ => break,
                },
                _ = tick.tick(), if subscribed && frames < if matches!(source, ReplayBook::CapturedResting) { 100 } else { 2 } => {
                    let bid = if frames == 0 { "0.006" } else { "0.007" };
                    frames += 1;
                    let (yes_bid, yes_ask) = match source {
                        ReplayBook::Rebound(1) => (bid.to_string(), "0.010".to_string()),
                        ReplayBook::Rebound(_) => ("0.990".to_string(), if frames == 1 { "0.994" } else { "0.993" }.to_string()),
                        ReplayBook::CapturedResting => captured_quotes.clone().unwrap(),
                    };
                    let book = json!({"marketId":42,"updateTimestampMs":get_atomic_clock_realtime().get_time_ns().as_u64()/1_000_000,
                        "orderCount":2,"bids":[[yes_bid,"1000"]],"asks":[[yes_ask,"1000"]]});
                    if sink.send(Message::Text(json!({"type":"M","topic":"predictOrderbook/42","data":book}).to_string().into())).await.is_err() { break; }
                }
            }
        }
    })
}

pub(super) async fn public_server(outcome: u8) -> (String, String, JoinHandle<()>) {
    public_server_with_book(ReplayBook::Rebound(outcome)).await
}

pub(super) async fn captured_resting_public_server() -> (String, String, JoinHandle<()>) {
    public_server_with_book(ReplayBook::CapturedResting).await
}

async fn public_server_with_book(source: ReplayBook) -> (String, String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/v1/markets/42", get(public_market))
                .route("/ws", get(public_socket))
                .with_state(source),
        )
        .await
        .unwrap();
    });
    (
        format!("http://{address}"),
        format!("ws://{address}/ws"),
        task,
    )
}

pub(super) fn rest_row(body: &Value, status: &str) -> Value {
    let mut row = rest_order_data(status, 1, OrderSide::Buy);
    row["order"] = body["data"]["order"].clone();
    row
}

#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_resting_amend_rejection_cancel_replace_feedback() {
    run_replay(Phase::Resting).await;
}

#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_cheap_rebound_partial_fill_position_and_account_feedback() {
    run_replay(Phase::Fill).await;
}

#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_observe_never_sends_orders_even_with_an_enabled_client() {
    run_replay(Phase::Observe).await;
}

#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_no_outcome_uses_complement_book_and_native_position() {
    run_replay_outcome(Phase::Fill, 2).await;
}

async fn run_replay(phase: Phase) {
    run_replay_outcome(phase, 1).await;
}

async fn run_replay_outcome(phase: Phase, outcome: u8) {
    run_replay_with_position_poll(phase, outcome, false, None).await;
}

#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_position_poll_preserves_state_when_fill_recovery_is_unavailable() {
    for outcome in [1, 2] {
        run_replay_with_position_poll(Phase::Fill, outcome, true, None).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_recovers_share_fee_fills_and_reconciles_net_position_once() {
    for outcome in [1, 2] {
        for quantity in [17, 100] {
            run_replay_with_position_poll(Phase::Fill, outcome, true, Some(quantity)).await;
        }
    }
}

async fn run_replay_with_position_poll(
    phase: Phase,
    outcome: u8,
    reconcile: bool,
    recovered_quantity: Option<u32>,
) {
    run_replay_extended(phase, outcome, reconcile, recovered_quantity, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_wallet_share_fees_atomic_partial_carry_and_net_reconciliation() {
    for (outcome, quantity) in [(1, 100), (2, 100), (1, 22), (2, 22)] {
        run_replay_extended(Phase::Fill, outcome, true, Some(quantity), true).await;
    }
}

async fn run_replay_extended(
    phase: Phase,
    outcome: u8,
    reconcile: bool,
    recovered_quantity: Option<u32>,
    wallet_precision: bool,
) {
    run_replay_variant(
        phase,
        outcome,
        reconcile,
        recovered_quantity,
        wallet_precision,
        false,
        false,
    )
    .await;
}

pub(super) async fn run_replay_variant(
    phase: Phase,
    outcome: u8,
    reconcile: bool,
    recovered_quantity: Option<u32>,
    wallet_precision: bool,
    multiple_prices: bool,
    interleaved: bool,
) -> Value {
    let quantity_mode = cfg!(feature = "native-quantity-commission");
    let history = recovered_quantity.is_some();
    let gross = recovered_quantity.unwrap_or(17);
    let carry = if wallet_precision && gross < 100 && !multiple_prices {
        Decimal::new(1, 16)
    } else {
        Decimal::ZERO
    };
    let expected_gross = Decimal::from(gross) + carry;
    let net = Decimal::from(gross) * Decimal::new(98, 2) + carry;
    // The differing-price scenario is synthetic: 17 shares at .009, remainder at .010.
    // Venue-reported per-fill fees, not a guessed fee formula, enter the adapter.
    let cash_debit = Decimal::from(gross) * Decimal::new(1, 2)
        - if multiple_prices {
            Decimal::new(17, 3)
        } else {
            Decimal::ZERO
        };
    let fee_value = Money::from_decimal(cash_debit * Decimal::new(2, 2), Currency::USDT()).unwrap();
    // Explicit rows only: the adapter does not claim absent holdings imply flat positions.
    let pages = if reconcile {
        vec![json!({"success":true,"data":[]}); 1000]
    } else {
        vec![]
    };
    let private = server(pages).await;
    if phase == Phase::Resting {
        *private.state.market_fee.lock().unwrap() = 200;
    }
    private.state.rpc_frame.lock().unwrap().2 =
        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000;
    let (http, ws, public_task) = public_server(outcome).await;
    let (quantity, order_cap, total_cap) = if phase == Phase::Resting {
        // Two resting prices below the replay bid must each meet the venue minimum.
        (250, Decimal::new(125, 2), Decimal::new(25, 1))
    } else {
        (100, Decimal::ONE, Decimal::from(2))
    };
    let mut exec = config(&private);
    exec.account_state_mode = PredictAccountStateMode::Reported;
    exec.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", private.http).into(),
    ));
    exec.orderbook_removal_enabled = true;
    exec.submission = Some(PredictSubmissionConfig {
        signer: Arc::new(
            nautilus_predict::signing::PredictAuthSigner::new(
                submissions::PUBLIC_KEY.into(),
                PredictEnvironment::BnbMainnet,
                Some(WALLET),
            )
            .unwrap(),
        ),
        max_order_usdt: order_cap,
        max_total_usdt: total_cap,
        require_post_only: phase == Phase::Resting,
        market_slippage_bps: 100,
    });
    let mut node = LiveNode::builder(trader_id(), Environment::Live)
        .unwrap()
        .with_name("PredictRustStrategyReplay")
        .with_exec_engine_config(LiveExecutionEngineConfig {
            reconciliation: false,
            inflight_check_interval_ms: 0,
            open_check_interval_secs: None,
            position_check_interval_secs: reconcile.then_some(0.05),
            position_check_threshold_ms: 0,
            ..Default::default()
        })
        .with_timeout_connection(3)
        .with_timeout_portfolio(3)
        .with_delay_post_stop_secs(0)
        .with_timeout_disconnection_secs(2)
        .add_data_client(
            Some("PREDICT".into()),
            Box::new(PredictDataClientFactory::new()),
            Box::new(PredictDataClientConfig {
                api_key: KEY.into(),
                market_ids: vec![42],
                base_url_http: Some(http),
                base_url_ws: Some(ws),
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
            Box::new(exec),
        )
        .unwrap()
        .build()
        .unwrap();
    let handle = node.handle();
    let observations = Rc::new(RefCell::new(Observations::default()));
    let phase_name = match phase {
        Phase::Resting => "resting",
        Phase::Fill => "fill",
        Phase::Observe => "observe",
    };
    let source = format!(
        "[strategy]\nphase = \"{phase_name}\"\nrun_secs = 10\nstale_after_ms = 2000\nminimum_rebound_ticks = 1"
    );
    let inputs = CalibrationInputs {
        account_id: account_id(),
        settings: StrategySettings::parse(&source).unwrap(),
        market_id: 42,
        instrument_id: instrument_id(outcome),
        outcome,
        quantity: Quantity::from(quantity),
        maximum_price: Decimal::new(1, 2),
        maximum_order_cost: order_cap,
        maximum_total_cost: total_cap,
        execution_enabled: true,
        fee_rate_bps: if phase == Phase::Resting { 200 } else { 0 },
    };
    let strategy = CalibrationStrategy::new(inputs, observations.clone(), handle.clone());
    let strategy = if phase == Phase::Fill {
        strategy.with_expected_cash_total(Decimal::from(1000) - cash_debit)
    } else {
        strategy
    };
    let strategy = if reconcile {
        strategy.with_expected_reconciled_quantity(net)
    } else {
        strategy
    };
    let strategy = if wallet_precision {
        strategy.with_fill_replay_target(
            Quantity::from_decimal_dp(expected_gross, FIXED_PRECISION).unwrap(),
        )
    } else {
        strategy
    };
    node.add_strategy(strategy).unwrap();
    let replies = match phase {
        Phase::Resting => 2,
        Phase::Fill => 1,
        Phase::Observe => 0,
    };
    for _ in 0..replies {
        private
            .state
            .create_replies
            .lock()
            .unwrap()
            .push_back((201, 0, vec![]));
        private.state.remove_replies.lock().unwrap().push_back((
            200,
            json!({"success":true,"removed":["12345"],"noop":[]}),
            0,
        ));
    }
    let mut wallet_frames = vec![];
    let driver = async {
        for n in 0..replies {
            timeout(Duration::from_secs(5), async {
                while private.state.creations.lock().unwrap().len() <= n {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let body = private.state.creations.lock().unwrap()[n].clone();
            private
                .state
                .orders
                .lock()
                .unwrap()
                .push_back(json!({"success":true,"data":rest_row(&body,"OPEN")}));
            let price = Decimal::from_str_exact(body["data"]["pricePerShare"].as_str().unwrap())
                .unwrap()
                / Decimal::from(1_000_000_000_000_000_000u64);
            let mut accepted = event("orderAccepted", outcome);
            accepted["orderHash"] = body["data"]["order"]["hash"].clone();
            accepted["details"]["price"] = json!(price.to_string());
            accepted["details"]["quantity"] = json!(quantity.to_string());
            accepted["details"]["value"] = json!((price * Decimal::from(quantity)).to_string());
            accepted["timestamp"] =
                json!(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000);
            private.wallet(accepted.clone());
            if phase == Phase::Fill {
                // Recorded-shape synthetic settlement, explicitly zero-fee for native accounting.
                let mut settled = accepted;
                settled["type"] = json!("orderTransactionSuccess");
                settled["settlementId"] = json!("replay-settlement");
                settled["fill"] = json!({"executedPriceWei":body["data"]["pricePerShare"],
                    "executedSizeWei":"17000000000000000000",
                    "executedValueWei":(price*Decimal::from(17)*Decimal::from(1_000_000_000_000_000_000u64)).normalize().to_string()});
                settled["isMaker"] = json!(false);
                settled["fee"] = json!({"type":"COLLATERAL","amountWei":"0"});
                {
                    let mut block = private.state.rpc_frame.lock().unwrap();
                    let initial_debit = if interleaved {
                        Decimal::new(153, 3)
                    } else {
                        cash_debit
                    };
                    block.0 -= (initial_debit * Decimal::from(1_000_000_000_000_000_000u64))
                        .normalize()
                        .to_string()
                        .parse::<u128>()
                        .unwrap();
                    block.1 += 1;
                    block.2 =
                        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000 + 1;
                }
                if !history {
                    private.wallet(settled);
                }
                if history {
                    let mut row = matches::match_row(outcome);
                    row["taker"]["hash"] = body["data"]["order"]["hash"].clone();
                    row["taker"]["price"] = body["data"]["pricePerShare"].clone();
                    row["taker"]["amount"] =
                        json!((u128::from(gross) * 1_000_000_000_000_000_000u128).to_string());
                    row["taker"]["fee"] = json!({"type":"SHARES","amount":(u128::from(gross)*20_000_000_000_000_000u128).to_string()});
                    row["settlementId"] = json!("replay-settlement");
                    row["executedAt"] = json!(
                        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
                    );
                    let rows = if wallet_precision {
                        let first = U256::from(if multiple_prices {
                            17_000_000_000_000_000_000u128
                        } else {
                            17_000_000_000_000_000_099u128
                        });
                        let second = if multiple_prices {
                            U256::from(u128::from(gross) * 1_000_000_000_000_000_000u128) - first
                        } else if gross == 100 {
                            U256::from(100_000_000_000_000_000_000u128) - first
                        } else {
                            U256::from(5_000_000_000_000_000_099u128)
                        };
                        let mut rows = vec![];
                        for (index, raw) in [(1, first), (2, second)] {
                            if interleaved && index == 2 {
                                let mut block = private.state.rpc_frame.lock().unwrap();
                                block.0 -= u128::from(gross - 17) * 10_000_000_000_000_000u128;
                                block.1 += 1;
                                block.2 = get_atomic_clock_realtime().get_time_ns().as_u64()
                                    / 1_000_000_000
                                    + 1;
                            }
                            let mut leg = row.clone();
                            let fee = raw * U256::from(2) / U256::from(100);
                            let fill_price = if multiple_prices && index == 1 {
                                Decimal::new(9, 3)
                            } else {
                                price
                            };
                            leg["taker"]["amount"] = json!(raw.to_string());
                            leg["taker"]["price"] = json!(
                                (fill_price * Decimal::from(1_000_000_000_000_000_000u64))
                                    .normalize()
                                    .to_string()
                            );
                            leg["taker"]["fee"]["amount"] = json!(fee.to_string());
                            leg["settlementId"] = json!(format!("wallet-precision-{index}"));
                            let mut wallet = event("orderTransactionSuccess", outcome);
                            wallet["orderHash"] = body["data"]["order"]["hash"].clone();
                            wallet["details"]["price"] = json!(price.to_string());
                            wallet["details"]["value"] =
                                json!((price * Decimal::from(100)).to_string());
                            wallet["timestamp"] = json!(
                                get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000
                            );
                            wallet["settlementId"] = leg["settlementId"].clone();
                            wallet["isMaker"] = json!(false);
                            wallet["fee"] = json!({"type":"SHARES","amountWei":fee.to_string()});
                            let px =
                                U256::from_str_radix(leg["taker"]["price"].as_str().unwrap(), 10)
                                    .unwrap();
                            wallet["fill"] = json!({"executedPriceWei":px.to_string(),"executedSizeWei":raw.to_string(),"executedValueWei":(raw*px/U256::from(1_000_000_000_000_000_000u64)).to_string()});
                            wallet_frames.extend([wallet.clone(), wallet.clone()]);
                            private.wallet(wallet.clone());
                            private.wallet(wallet);
                            rows.push(leg.clone());
                            rows.push(leg);
                            if interleaved {
                                *private.state.match_snapshot.lock().unwrap() = Some(rows.clone());
                                if index == 1 {
                                    let mut partial = holding(outcome);
                                    partial["amount"] = json!("16660000000000000000");
                                    private.state.pages.lock().unwrap().clear();
                                    *private.state.position_snapshot.lock().unwrap() =
                                        Some(vec![partial]);
                                    timeout(Duration::from_secs(2), async {
                                        loop {
                                            if observations.borrow().last_position_qty.is_some_and(|q| q.as_decimal() == Decimal::new(1666, 2)) {
                                                break;
                                            }
                                            tokio::task::yield_now().await;
                                        }
                                    }).await.expect("first partial net-position correction before the next fill");
                                }
                            }
                        }
                        rows.reverse();
                        rows
                    } else {
                        vec![row.clone(), row]
                    };
                    // Repeat both within the page and across position-check queries.
                    private.state.match_pages.lock().unwrap().extend(vec![
                        (
                            200,
                            json!({"success":true,"data":rows,"cursor":null}),
                            0
                        );
                        1000
                    ]);
                }
                if reconcile {
                    // Synthetic position discrepancy, not evidence of wallet SHARES wire units.
                    let mut row = holding(outcome);
                    row["amount"] = json!(
                        (u128::from(gross) * 980_000_000_000_000_000u128
                            + if wallet_precision && !multiple_prices {
                                if gross == 100 { 1 } else { 196 }
                            } else {
                                0
                            })
                        .to_string()
                    );
                    let mut pages = private.state.pages.lock().unwrap();
                    pages.clear();
                    if interleaved {
                        *private.state.position_snapshot.lock().unwrap() = Some(vec![row.clone()]);
                    }
                    pages.extend(vec![json!({"success":true,"data":[row]}); 1000]);
                }
            }
        }
    };
    let deadline_handle = handle.clone();
    let deadline = async {
        let _ = timeout(
            Duration::from_secs(if phase == Phase::Observe {
                1
            } else if reconcile {
                2
            } else {
                8
            }),
            async {
                while !deadline_handle.should_stop() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            },
        )
        .await;
        deadline_handle.stop();
    };
    let run = async {
        node.run().await.unwrap();
    };
    let ((), (), ()) = tokio::join!(driver, run, deadline);
    let observed = observations.borrow();
    assert!(observed.snapshots > 0);
    assert_eq!(observed.submitted, replies);
    assert_eq!(
        observed.accepted,
        replies
            + if quantity_mode {
                0
            } else {
                usize::from(history) + usize::from(interleaved)
            }
    );
    assert!(observed.events_with_cached_order >= replies * 3);
    assert!(observed.account_reports_observed >= 1);
    if phase == Phase::Observe {
        assert_eq!(observed.fills, 0);
        assert_eq!(observed.position_events, 0);
        assert_eq!(private.state.creations.lock().unwrap().len(), 0);
        assert_eq!(private.state.removals.lock().unwrap().len(), 0);
        public_task.abort();
        return Value::Null;
    }
    assert!(observed.account_events > 0);
    assert!(observed.account_reports_observed >= 2);
    if phase == Phase::Resting {
        for kind in ["Accepted", "ModifyRejected", "Canceled"] {
            assert_eq!(
                observed
                    .recorded_feedback
                    .iter()
                    .filter(|row| row["type"] == kind)
                    .count(),
                2
            );
        }
        assert!(
            observed
                .recorded_feedback
                .iter()
                .any(|row| row["type"] == "AccountStateApplied")
        );
        assert!(!observed.feedback_write_failed);
        assert_eq!(observed.modify_rejected, 2);
        assert_eq!(observed.modify_terms_preserved, 2);
        assert_eq!(observed.canceled, 2);
        assert_eq!(observed.fills, 0);
        assert_eq!(observed.last_cash_total, Some(Decimal::from(1000)));
        assert_eq!(observed.position_events, 0);
        assert!(
            node.kernel()
                .cache
                .borrow()
                .positions(None, None, None, None, None)
                .is_empty()
        );
    } else {
        assert_eq!(
            observed.fills,
            if quantity_mode {
                if wallet_precision { 2 } else { 1 }
            } else if interleaved {
                4
            } else if wallet_precision {
                3
            } else if history {
                2
            } else {
                1
            }
        );
        assert_eq!(observed.canceled, usize::from(gross < 100));
        assert_eq!(
            observed.last_cash_total,
            Some(Decimal::from(1000) - cash_debit)
        );
        assert!(observed.position_events > 0);
        assert!(
            (observed.last_position_qty.unwrap().as_decimal()
                - if history { net } else { Decimal::from(17) })
            .abs()
                <= Decimal::new(1, 12)
        );
    }
    if reconcile {
        // Successful history permits correction; unavailable history must preserve state.
        assert_eq!(
            observed.position_events,
            if quantity_mode {
                if wallet_precision { 2 } else { 1 }
            } else if interleaved {
                4
            } else if wallet_precision {
                3
            } else if history {
                2
            } else {
                1
            }
        );
        assert!(!private.state.requests.lock().unwrap().is_empty());
        let cache = node.kernel().cache.borrow();
        let orders = cache.orders(None, None, None, None, None);
        assert_eq!(
            orders.len(),
            if quantity_mode {
                1
            } else if interleaved {
                3
            } else if history {
                2
            } else {
                1
            }
        );
        if history {
            assert_eq!(observed.buy_fill_quantity_sum, expected_gross);
            assert_eq!(
                observed.buy_commission_sum,
                if quantity_mode {
                    Decimal::ZERO
                } else {
                    fee_value.as_decimal()
                }
            );
            let positions = cache.positions(None, None, None, None, None);
            assert_eq!(positions.len(), 1);
            if quantity_mode {
                let fees = orders
                    .iter()
                    .flat_map(|o| o.events())
                    .filter_map(|e| match e {
                        nautilus_model::events::OrderEventAny::Filled(f) => f.quantity_commission,
                        _ => None,
                    })
                    .sum::<Decimal>();
                let totals = positions[0].quantity_commission_total.unwrap();
                assert_eq!(totals, fees);
                assert_eq!(
                    super::native_fee_paths::project_inventory_fee(totals, 16),
                    expected_gross - positions[0].quantity.as_decimal()
                );
                assert!(
                    super::native_fee_paths::inventory_fee_residual(totals, 16)
                        < Decimal::new(1, 16)
                );
                assert_eq!(
                    positions[0].realized_pnl.unwrap(),
                    Money::from_decimal(
                        -positions[0].quantity_commission_cost.unwrap(),
                        Currency::USDT()
                    )
                    .unwrap()
                );
                assert!(
                    positions[0]
                        .adjustments
                        .iter()
                        .all(|a| a.pnl_change.is_some())
                );
            } else {
                assert_eq!(positions[0].realized_pnl.unwrap(), -fee_value);
            }
            assert!(
                private.state.match_requests.lock().unwrap().len()
                    >= if quantity_mode {
                        usize::from(!wallet_precision)
                    } else if wallet_precision {
                        1
                    } else {
                        2
                    }
            );
        }
        assert!(
            orders
                .iter()
                .any(|order| order.filled_qty().as_decimal() == expected_gross
                    && order.status()
                        == if gross == 100 {
                            OrderStatus::Filled
                        } else {
                            OrderStatus::Canceled
                        })
        );
    }
    assert!(observed.complete);
    assert_eq!(observed.last_cash_locked, Some(Decimal::ZERO));
    assert_eq!(private.state.creations.lock().unwrap().len(), replies);
    assert_eq!(
        private.state.removals.lock().unwrap().len(),
        replies - usize::from(history && gross == 100)
    );
    let mut result = if multiple_prices {
        let cache = node.kernel().cache.borrow();
        let order = cache
            .orders(None, None, None, None, None)
            .into_iter()
            .find(|o| o.order_side() == OrderSide::Buy)
            .unwrap();
        let fills = order.events().iter().filter_map(|event| {
            if let nautilus_model::events::OrderEventAny::Filled(fill) = event {
                Some(json!({"tradeId":fill.trade_id,"quantity":fill.last_qty.to_string(),
                    "price":fill.last_px.to_string(),"commission":fill.commission.map(|v|v.to_string())}))
            } else { None }
        }).collect::<Vec<_>>();
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0]["price"], "0.009");
        assert_eq!(fills[1]["price"], "0.010");
        let expected_avg = cash_debit / Decimal::from(gross);
        assert_eq!(order.avg_px().unwrap(), expected_avg);
        let positions = cache.positions(None, None, None, None, None);
        let position = &positions[0];
        let position_average = Decimal::from_str_exact(&position.avg_px_open.to_string()).unwrap();
        let basis_with_fees = net * position_average
            + if quantity_mode {
                -position.realized_pnl.unwrap().as_decimal()
            } else {
                fee_value.as_decimal()
            };
        // Characterize a real Native boundary, rather than claiming net quantity alone proves PnL.
        assert_eq!(
            (basis_with_fees - cash_debit).abs() > Decimal::new(1, 8),
            !quantity_mode && interleaved
        );
        // A native Position-only close probe, separate from the LiveNode replay.
        // No order is submitted: measure how this basis would affect realized PnL.
        let mut closed = (**position).clone();
        let mut exit = order
            .events()
            .iter()
            .find_map(|event| {
                if let nautilus_model::events::OrderEventAny::Filled(fill) = event {
                    Some(fill.clone())
                } else {
                    None
                }
            })
            .unwrap();
        exit.order_side = OrderSide::Sell;
        exit.last_qty = position.quantity;
        exit.last_px = Price::from("0.010");
        exit.commission = Some(Money::from("0 USDT"));
        exit.quantity_commission = None;
        exit.client_order_id = "POSITION-ONLY-EXIT-PROBE".into();
        exit.trade_id = "POSITION-ONLY-EXIT-PROBE".into();
        exit.event_id = UUID4::new();
        exit.ts_event = UnixNanos::from(position.ts_last.as_u64() + 1);
        exit.ts_init = exit.ts_event;
        closed.try_apply(&exit).unwrap();
        assert!(closed.is_closed());
        let expected_pnl = net * Decimal::new(1, 2) - cash_debit;
        let native_pnl = closed.realized_pnl.unwrap().as_decimal();
        assert_eq!(
            (native_pnl - expected_pnl).abs() > Decimal::new(1, 8),
            !quantity_mode && interleaved
        );
        json!({"source":"synthetic different-price partial-fill Rust Strategy + LiveNode replay; not actual venue fills",
            "outcome":outcome,"submittedQuantity":quantity,"grossFilled":gross,"netPosition":net.to_string(),
            "nativeFilledQuantity":order.filled_qty().to_string(),"nativeOrderStatus":order.status().to_string(),
            "nativeAveragePrice":order.avg_px(),"cashDebit":cash_debit.to_string(),
            "commissionUSDT":fee_value.to_string(),"strategyFilledCallbacks":observed.fills,
            "interleavedFeeCorrection":interleaved,"positionAverage":position_average.to_string(),
            "positionBasisPlusCommission":basis_with_fees.to_string(),
            "basisResidualUSDT":(basis_with_fees-cash_debit).to_string(),
            "pnlConvention":if quantity_mode {"Native cumulative inventory cost is included in quote realized"} else {"legacy quote-fee path"},
            "basisMatchesCash":(basis_with_fees-cash_debit).abs() <= Decimal::new(1,8),
            "positionOnlyExitProbe":{"source":"offline cloned Native Position close; not a strategy or venue SELL",
                "sellPrice":"0.010","sellFee":"0","expectedCashPnL":expected_pnl.to_string(),
                "nativeRealizedPnL":native_pnl.to_string(),"pnlMatchesCash":(native_pnl-expected_pnl).abs() <= Decimal::new(1,8)},
            "walletFrames":wallet_frames,
            "matchesResponses":*private.state.match_response_log.lock().unwrap(),
            "positionResponses":*private.state.position_response_log.lock().unwrap(),
            "fills":fills,"nativeFeedback":observed.recorded_feedback,
            "duplicateWalletPackets":true,"duplicateRestRows":true})
    } else {
        Value::Null
    };
    if quantity_mode && phase == Phase::Fill {
        let cache = node.kernel().cache.borrow();
        if result.is_null() {
            result = json!({"source":"synthetic 18-decimal wallet partial-fill Rust Strategy + LiveNode replay",
                "walletFrames":wallet_frames,"nativeFeedback":observed.recorded_feedback,
                "duplicateWalletPackets":true,"grossFilled":expected_gross.to_string()});
        }
        result["feeSummaries"] = json!(cache.positions(None,None,None,None,None).iter().map(|p| {
            let Some(totals) = p.quantity_commission_total else {
                return json!({"nativeQuantity":p.quantity.as_decimal().to_string(),"fees":null});
            };
            json!({"nativeQuantity":p.quantity.as_decimal().to_string(),"signedDecimalQuantity":p.signed_decimal_qty().to_string(),
                "fees":totals,"quantityResidual":super::native_fee_paths::inventory_fee_residual(totals, p.size_precision).to_string(),
                "realizedPnl":p.realized_pnl})
        }).collect::<Vec<_>>());
        result["orderQuantityCommissions"] = json!(
            cache
                .orders(None, None, None, None, None)
                .iter()
                .map(|o| super::native_fee_paths::inventory_fees(o.events()).to_string())
                .collect::<Vec<_>>()
        );
    }
    public_task.abort();
    result
}

#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_different_price_partial_share_fees_preserve_gross_and_net() {
    for (outcome, gross) in [(1, 32), (2, 32), (1, 100), (2, 100)] {
        run_replay_variant(Phase::Fill, outcome, true, Some(gross), true, true, false).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_interleaved_share_fees_reconcile_cost_basis() {
    run_replay_variant(Phase::Fill, 1, true, Some(32), true, true, true).await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline synthetic differing-price partial-fill JSON capture; create_new only"]
async fn record_different_price_partial_share_fees_json() {
    let mut sessions = vec![];
    for (outcome, gross) in [(1, 32), (2, 32), (1, 100), (2, 100)] {
        sessions.push(
            run_replay_variant(Phase::Fill, outcome, true, Some(gross), true, true, false).await,
        );
    }
    sessions.push(run_replay_variant(Phase::Fill, 1, true, Some(32), true, true, true).await);
    let file = std::fs::OpenOptions::new().create_new(true).write(true).open(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "tests/fixtures/private_api/different_price_partial_share_fees_native_synthetic_20261017_v2.json"
        )).unwrap();
    serde_json::to_writer_pretty(file, &json!({"source":"offline synthetic scenarios; no exchange calls or real orders","sessions":sessions})).unwrap();
}
