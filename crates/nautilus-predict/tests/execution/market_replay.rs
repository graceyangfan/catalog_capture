// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Synthetic recovery replay, not MARKET strategy submission or mainnet fill evidence.
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_live::node::config::LiveExecutionEngineConfig;
use nautilus_model::{
    enums::TimeInForce,
    identifiers::{ClientOrderId, StrategyId},
};
use nautilus_predict::PredictDataClientConfig;

use super::{
    live_node::{
        public_server,
        strategy::{CalibrationInputs, CalibrationStrategy, Observations, Phase, StrategySettings},
    },
    node, *,
};

fn market_order(outcome: u8) -> OrderAny {
    market_order_with_tif(outcome, TimeInForce::Gtc)
}

fn market_order_with_tif(outcome: u8, tif: TimeInForce) -> OrderAny {
    market_order_with_sizing(outcome, tif, false)
}

fn market_order_with_sizing(outcome: u8, tif: TimeInForce, quote: bool) -> OrderAny {
    let mut order = OrderTestBuilder::new(OrderType::Market)
        .trader_id(trader_id())
        .strategy_id(StrategyId::new("PREDICT-CALIBRATION-001"))
        .client_order_id(ClientOrderId::new("RECOVERED-MARKET-001"))
        .instrument_id(instrument_id(outcome))
        .side(OrderSide::Buy)
        .quantity(if quote {
            Quantity::from("2.02")
        } else {
            Quantity::from(100)
        })
        .quote_quantity(quote)
        .time_in_force(tif)
        .build();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    let now = get_atomic_clock_realtime().get_time_ns();
    if quote {
        let update = nautilus_model::events::OrderUpdated::new(
            order.trader_id(),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            Quantity::from(100),
            UUID4::new(),
            now,
            now,
            false,
            None,
            None,
            None,
            None,
            None,
            false,
        );
        order
            .apply(nautilus_model::events::OrderEventAny::Updated(update))
            .unwrap();
    }
    order
        .apply(factory.generate_order_submitted(&order, now))
        .unwrap();
    order
        .apply(factory.generate_order_accepted(&order, VenueOrderId::new(HASH), now, now))
        .unwrap();
    order
}

#[test]
fn recovered_market_rest_report_preserves_strategy_and_does_not_invent_a_limit_price() {
    use nautilus_predict::{
        execution::parse::parse_rest_order_report, http::account_models::PredictOrderData,
    };
    let order = market_order(1);
    let mut row = rest_order_data("OPEN", 1, OrderSide::Buy);
    row["strategy"] = json!("MARKET");
    row["order"]["makerAmount"] = json!("2020000000000000000");
    row["order"]["takerAmount"] = json!("100000000000000000000");
    let observed = get_atomic_clock_realtime().get_time_ns();
    let valid: PredictOrderData = serde_json::from_value(row.clone()).unwrap();
    let report = parse_rest_order_report(
        &valid,
        &order,
        &instrument(1),
        account_id(),
        WALLET.parse().unwrap(),
        observed,
    )
    .unwrap();
    assert_eq!(report.order_type, OrderType::Market);
    assert_eq!(report.quantity, Quantity::from(100));
    assert!(report.price.is_none());
    for field in ["strategy", "shares", "collateral", "hash"] {
        let mut wrong = row.clone();
        match field {
            "strategy" => wrong["strategy"] = json!("LIMIT"),
            "shares" => wrong["order"]["takerAmount"] = json!("99000000000000000000"),
            "collateral" => wrong["order"]["makerAmount"] = json!("101000000000000000000"),
            _ => {
                wrong["order"]["hash"] =
                    json!("0x4444444444444444444444444444444444444444444444444444444444444444")
            }
        }
        let wrong: PredictOrderData = serde_json::from_value(wrong).unwrap();
        assert!(
            parse_rest_order_report(
                &wrong,
                &order,
                &instrument(1),
                account_id(),
                WALLET.parse().unwrap(),
                observed
            )
            .is_err()
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn recovered_market_wallet_partial_duplicates_reconcile_net_position_and_reported_cash() {
    for outcome in [1, 2] {
        run(outcome, None).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn recovered_market_sell_exit_closes_net_inventory_and_updates_cash_without_double_fee() {
    for (outcome, price) in [(1, "0.011"), (2, "0.009")] {
        run(outcome, Some(Decimal::from_str_exact(price).unwrap())).await;
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "offline synthetic JSON capture only; loopback servers; no operator credentials"]
async fn record_synthetic_market_recovery_json() {
    let mut sessions = vec![];
    for (outcome, px) in [(1, None), (2, None), (1, Some("0.011")), (2, Some("0.009"))] {
        sessions.push(
            run_case(
                outcome,
                px.map(|p| Decimal::from_str_exact(p).unwrap()),
                false,
            )
            .await,
        );
    }
    std::fs::write(std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/market_recovery_synthetic_20261015.json"),
        serde_json::to_vec_pretty(&json!({"source":"explicitly generated synthetic loopback sessions; not mainnet transactions", "sessions":sessions})).unwrap()).unwrap();
}

async fn run(outcome: u8, exit_price: Option<Decimal>) -> Value {
    run_case(outcome, exit_price, true).await
}

async fn run_case(outcome: u8, exit_price: Option<Decimal>, from_saved: bool) -> Value {
    run_case_with_tif(outcome, exit_price, from_saved, TimeInForce::Gtc).await
}

#[tokio::test(flavor = "current_thread")]
async fn recovered_fok_multiple_settlements_preserve_gross_fills_net_fees_and_native_tif() {
    run_case_with_tif(1, None, true, TimeInForce::Fok).await;
}

async fn run_case_with_tif(
    outcome: u8,
    exit_price: Option<Decimal>,
    from_saved: bool,
    tif: TimeInForce,
) -> Value {
    run_case_with_sizing(outcome, exit_price, from_saved, tif, false).await
}

#[tokio::test(flavor = "current_thread")]
async fn restored_quote_buy_uses_normalized_shares_for_synthetic_settlements() {
    run_case_with_sizing(1, None, true, TimeInForce::Fok, true).await;
}

async fn run_case_with_sizing(
    outcome: u8,
    exit_price: Option<Decimal>,
    from_saved: bool,
    tif: TimeInForce,
    quote: bool,
) -> Value {
    let saved = if from_saved {
        let fixture: Value = serde_json::from_str(
            &std::fs::read_to_string(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/private_api/market_recovery_synthetic_20261015.json"),
            )
            .unwrap(),
        )
        .unwrap();
        let session = fixture["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| {
                row["outcome"] == outcome
                    && row["exitPrice"] == json!(exit_price.map(|p| p.to_string()))
            })
            .unwrap();
        Some(session["walletEvents"].as_array().unwrap().clone())
    } else {
        None
    };
    let private = server(vec![]).await;
    *private.state.rpc_frame.lock().unwrap() = (
        1_000_000_000_000_000_000_000,
        1,
        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000,
    );
    let mut execution = config(&private);
    execution.account_state_mode = nautilus_predict::PredictAccountStateMode::Reported;
    execution.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", private.http).into(),
    ));
    assert!(execution.submission_signer.is_none());
    let (http, ws, public_task) = public_server(outcome).await;
    let mut live = node::build_node_with_engine(
        trader_id(),
        PredictDataClientConfig {
            api_key: KEY.into(),
            market_ids: vec![42],
            base_url_http: Some(http),
            base_url_ws: Some(ws),
            http_timeout_secs: 2,
            ws_timeout_secs: 2,
            dynamic_refresh_secs: 30,
            transport_backend: TransportBackend::Tungstenite,
        },
        execution,
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
    let recovered = market_order_with_sizing(outcome, tif, quote);
    if quote {
        assert!(recovered.init_event().quote_quantity);
        assert_eq!(recovered.init_event().quantity, Quantity::from("2.02"));
        assert!(!recovered.is_quote_quantity());
        assert_eq!(recovered.quantity(), Quantity::from(100));
    }
    let shared_cache = live.kernel().cache.clone();
    {
        let mut cache = live.kernel().cache.borrow_mut();
        cache
            .add_instrument(InstrumentAny::BinaryOption(instrument(outcome)))
            .unwrap();
        cache
            .add_order(recovered.clone(), None, None, false)
            .unwrap();
    }
    let observed = Rc::new(RefCell::new(Observations::default()));
    let handle = live.handle();
    live.add_strategy(
        CalibrationStrategy::new(
            CalibrationInputs {
                account_id: account_id(),
                settings: StrategySettings {
                    phase: Phase::Fill,
                    run_secs: 10,
                    stale_after_ms: 2000,
                    minimum_rebound_ticks: 1,
                },
                market_id: 42,
                instrument_id: instrument_id(outcome),
                outcome,
                quantity: Quantity::from(100),
                maximum_price: Decimal::new(1, 2),
                maximum_order_cost: Decimal::ONE,
                maximum_total_cost: Decimal::ONE,
                execution_enabled: false,
                fee_rate_bps: 200,
            },
            observed.clone(),
            handle.clone(),
        )
        .with_expected_reconciled_quantity(if exit_price.is_some() {
            Decimal::ZERO
        } else {
            Decimal::from(98)
        })
        .with_fill_replay_target(Quantity::from(100)),
    )
    .unwrap();
    let mut raw_wallet = vec![];
    let driver = async {
        timeout(Duration::from_secs(5), async {
            while observed.borrow().snapshots == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut history = vec![];
        for (index, gross) in [(1, 17u128), (2, 83u128)] {
            let mut wallet = event("orderTransactionSuccess", outcome);
            wallet["details"]["strategyType"] = json!("MARKET");
            wallet["details"]["price"] = json!("0.010");
            wallet["details"]["value"] = json!("1.00");
            wallet["timestamp"] =
                json!(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000);
            wallet["settlementId"] = json!(format!("market-recovery-{index}"));
            wallet["isMaker"] = json!(false);
            wallet["fee"] =
                json!({"type":"SHARES","amountWei":(gross*20_000_000_000_000_000u128).to_string()});
            wallet["fill"] = json!({"executedSizeWei":(gross*1_000_000_000_000_000_000u128).to_string(),
                "executedPriceWei":"10000000000000000","executedValueWei":(gross*10_000_000_000_000_000u128).to_string()});
            if let Some(saved) = &saved {
                wallet = saved[index - 1].clone();
                // Event wall time is rebound to this node generation; economic facts
                // and settlement/hash identities come from the saved JSON unchanged.
                wallet["timestamp"] =
                    json!(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000);
            }
            let mut row = matches::match_row(outcome);
            row["taker"]["price"] = json!("10000000000000000");
            row["taker"]["amount"] = wallet["fill"]["executedSizeWei"].clone();
            row["taker"]["fee"] = json!({"type":"SHARES","amount":wallet["fee"]["amountWei"]});
            row["settlementId"] = wallet["settlementId"].clone();
            row["executedAt"] =
                json!(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true));
            history.push(row.clone());
            history.push(row);
            {
                let mut block = private.state.rpc_frame.lock().unwrap();
                block.0 -= gross * 10_000_000_000_000_000u128;
                block.1 += 1;
                block.2 = get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000 + 1;
            }
            raw_wallet.push(wallet.clone());
            private.wallet(wallet.clone());
            private.wallet(wallet);
            timeout(Duration::from_secs(3), async {
                while observed.borrow().buy_fill_quantity_sum
                    < Decimal::from(if index == 1 { 17 } else { 100 })
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        history.reverse();
        private.state.match_pages.lock().unwrap().extend(vec![
            (
                200,
                json!({"success":true,"data":history.clone(),"cursor":null}),
                0
            );
            1000
        ]);
        let mut row = holding(outcome);
        row["amount"] = json!("98000000000000000000");
        private
            .state
            .pages
            .lock()
            .unwrap()
            .extend(vec![json!({"success":true,"data":[row]}); 1000]);
        if let Some(px) = exit_price {
            let wallet = replay_exit(
                &private,
                &shared_cache,
                &observed,
                outcome,
                px,
                history,
                saved.as_ref().map(|rows| rows[2].clone()),
            )
            .await;
            raw_wallet.push(wallet);
        }
    };
    let deadline = async {
        if timeout(Duration::from_secs(9), async {
            while !handle.should_stop() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_err()
        {
            handle.stop();
        }
    };
    let (result, (), ()) = tokio::join!(live.run(), driver, deadline);
    result.unwrap();
    let observed = observed.borrow();
    assert_eq!(observed.submitted, 0);
    assert_eq!(observed.denied, 0);
    assert_eq!(observed.buy_fill_quantity_sum, Decimal::from(100));
    assert_eq!(observed.buy_commission_sum, Decimal::ZERO);
    let final_cash = exit_price.map_or(Decimal::from(999), |price| {
        Decimal::from(999) + price * Decimal::from(98) * Decimal::new(98, 2)
    });
    assert_eq!(observed.last_cash_total, Some(final_cash));
    assert_eq!(observed.last_cash_locked, Some(Decimal::ZERO));
    assert_eq!(observed.fills, 2 + usize::from(exit_price.is_some()));
    assert_eq!(
        observed.last_position_qty.unwrap().as_decimal(),
        if exit_price.is_some() {
            Decimal::ZERO
        } else {
            Decimal::from(98)
        }
    );
    let cache = live.kernel().cache.borrow();
    let order = cache.order(&recovered.client_order_id()).unwrap();
    assert_eq!(order.order_type(), OrderType::Market);
    assert_eq!(order.time_in_force(), tif);
    assert_eq!(order.status(), OrderStatus::Filled);
    assert_eq!(order.quantity(), Quantity::from(100));
    assert_eq!(order.filled_qty(), Quantity::from(100));
    {
        let fees = order
            .events()
            .iter()
            .filter_map(|event| match event {
                nautilus_model::events::OrderEventAny::Filled(fill) => {
                    fill.quantity_commission.map(|fee| fee.as_decimal())
                }
                _ => None,
            })
            .sum::<Decimal>();
        assert_eq!(fees, Decimal::from(2));
    }
    if exit_price.is_some() {
        let positions = cache.positions(None, None, None, None, None);
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].quantity, Quantity::from(0));
        assert_eq!(
            positions[0].realized_pnl.unwrap().as_decimal(),
            final_cash - Decimal::from(1000)
        );
        let sell = cache
            .order(&ClientOrderId::new("RECOVERED-MARKET-EXIT-001"))
            .unwrap();
        assert_eq!(sell.filled_qty(), Quantity::from(98));
        assert_eq!(sell.status(), OrderStatus::Filled);
    }
    assert!(private.state.creations.lock().unwrap().is_empty());
    assert!(private.state.removals.lock().unwrap().is_empty());
    public_task.abort();
    json!({"source":"synthetic loopback Rust Strategy + LiveNode recovery; not native strategy submission, not mainnet fills", "outcome":outcome,
        "entryGross":"100","entryNet":"98","entryCommissionUsdt":"0.02", "exitPrice":exit_price.map(|p|p.to_string()),
        "walletEvents":raw_wallet,"nativeFeedback":observed.recorded_feedback,"finalCashUsdt":final_cash.to_string(),
        "finalNetPosition":observed.last_position_qty.unwrap().as_decimal().to_string(),
        "strategySubmissions":0,"venueCreates":0,"venueRemovals":0})
}

async fn replay_exit(
    private: &Server,
    cache: &Rc<RefCell<Cache>>,
    observed: &Rc<RefCell<Observations>>,
    outcome: u8,
    px: Decimal,
    mut history: Vec<Value>,
    saved_wallet: Option<Value>,
) -> Value {
    timeout(Duration::from_secs(4), async {
        while observed
            .borrow()
            .last_position_qty
            .is_none_or(|q| q.as_decimal() != Decimal::from(98))
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Replay an externally accepted SELL after independent holdings confirm net 98.
    // This deliberately exercises recovery, not the currently denied SELL submit path.
    let exit_id = ClientOrderId::new("RECOVERED-MARKET-EXIT-001");
    let hash = "0x4444444444444444444444444444444444444444444444444444444444444444";
    let sell = OrderTestBuilder::new(OrderType::Market)
        .trader_id(trader_id())
        .strategy_id(StrategyId::new("PREDICT-CALIBRATION-001"))
        .client_order_id(exit_id)
        .instrument_id(instrument_id(outcome))
        .side(OrderSide::Sell)
        .quantity(Quantity::from(98))
        .build();
    cache
        .borrow_mut()
        .add_order(sell.clone(), None, None, false)
        .unwrap();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    let sender = nautilus_common::live::runner::try_get_exec_event_sender().unwrap();
    let now = get_atomic_clock_realtime().get_time_ns();
    sender
        .send(ExecutionEvent::Order(
            factory.generate_order_submitted(&sell, now),
        ))
        .unwrap();
    sender
        .send(ExecutionEvent::Order(factory.generate_order_accepted(
            &sell,
            VenueOrderId::new(hash),
            now,
            now,
        )))
        .unwrap();
    timeout(Duration::from_secs(3), async {
        while cache.borrow().order(&exit_id).unwrap().status() != OrderStatus::Accepted {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let scale = Decimal::from(1_000_000_000_000_000_000u64);
    let value = px * Decimal::from(98);
    let fee = value * Decimal::new(2, 2);
    let mut wallet = event("orderTransactionSuccess", outcome);
    wallet["orderHash"] = json!(hash);
    wallet["details"]["strategyType"] = json!("MARKET");
    wallet["details"]["quoteType"] = json!("ASK");
    wallet["details"]["price"] = json!(px.to_string());
    wallet["details"]["quantity"] = json!("98");
    wallet["details"]["value"] = json!(value.to_string());
    wallet["timestamp"] = json!(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000);
    wallet["settlementId"] = json!("market-recovery-exit");
    wallet["isMaker"] = json!(false);
    wallet["fee"] = json!({"type":"COLLATERAL","amountWei":(fee*scale).normalize().to_string()});
    wallet["fill"] = json!({"executedSizeWei":"98000000000000000000",
        "executedPriceWei":(px*scale).normalize().to_string(),
        "executedValueWei":(value*scale).normalize().to_string()});
    if let Some(saved) = saved_wallet {
        wallet = saved;
        wallet["timestamp"] = json!(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000);
    }
    let mut row = matches::match_row(outcome);
    row["taker"]["hash"] = json!(hash);
    row["taker"]["quoteType"] = json!("Ask");
    row["taker"]["amount"] = wallet["fill"]["executedSizeWei"].clone();
    row["taker"]["price"] = wallet["fill"]["executedPriceWei"].clone();
    row["taker"]["fee"] = json!({"type":"COLLATERAL","amount":wallet["fee"]["amountWei"]});
    row["settlementId"] = wallet["settlementId"].clone();
    row["executedAt"] =
        json!(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true));
    history.push(row.clone());
    history.push(row);
    {
        let mut pages = private.state.match_pages.lock().unwrap();
        pages.clear();
        pages.extend(vec![
            (
                200,
                json!({"success":true,"data":history,"cursor":null}),
                0
            );
            1000
        ]);
    }
    {
        let mut pages = private.state.pages.lock().unwrap();
        pages.clear();
        let mut flat = holding(outcome);
        flat["amount"] = json!("0");
        pages.extend(vec![json!({"success":true,"data":[flat]}); 1000]);
    }
    {
        let mut block = private.state.rpc_frame.lock().unwrap();
        block.0 += ((value - fee) * scale)
            .normalize()
            .to_string()
            .parse::<u128>()
            .unwrap();
        block.1 += 1;
        block.2 = get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000 + 1;
    }
    private.wallet(wallet.clone());
    private.wallet(wallet.clone());
    wallet
}
