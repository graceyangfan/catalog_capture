// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Explicit mainnet calibration hosts; strategy and Native engines own trading state.
use nautilus_live::node::config::LiveExecutionEngineConfig;
use nautilus_model::{
    identifiers::InstrumentId,
    types::{Price, Quantity},
};
use nautilus_network::websocket::TransportBackend;
use nautilus_predict::{
    PredictDataClientConfig, PredictSubmissionConfig, blockchain::PredictChainClient,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use super::*;
#[allow(dead_code)]
#[path = "exit_strategy.rs"]
mod exits;

#[tokio::test(flavor = "current_thread")]
#[ignore = "MAINNET FILLS: supervisor-only, one BUY <=1.50 USDT, maker TP then bounded taker exit"]
async fn mainnet_bounded_round_trip() -> anyhow::Result<()> {
    run_round_trip(false, false).await
}

#[cfg(feature = "native-quantity-commission")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "MAINNET FILLS: explicit supervisor approval required; wait for strategy exit or manual stop"]
async fn mainnet_wait_for_strategy_exit() -> anyhow::Result<()> {
    run_round_trip(true, false).await
}

#[cfg(feature = "native-quantity-commission")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "MAINNET SELL ONLY: recover existing inventory and exit, no BUY"]
async fn mainnet_exit_only_restart() -> anyhow::Result<()> {
    run_round_trip(true, true).await
}

async fn run_round_trip(wait_for_exit: bool, exit_only: bool) -> anyhow::Result<()> {
    let settings =
        PredictExecutionSettings::load(&root().join("env/predict-fill-calibration.toml"))?;
    anyhow::ensure!(
        settings.test.allow_order_writes
            && !settings.test.require_post_only
            && settings.test.max_order_usdt == Decimal::new(150, 2)
            && settings.test.max_total_usdt == Decimal::new(150, 2),
        "fixed one-round BUY caps required"
    );
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(root().join("logs/predict-fill-proxy.json"))?)?;
    anyhow::ensure!(
        manifest["waitForStrategyExit"].as_bool().unwrap_or(false) == wait_for_exit,
        "supervisor/node lifecycle mode mismatch"
    );
    anyhow::ensure!(
        manifest["exitOnly"].as_bool().unwrap_or(false) == exit_only,
        "supervisor/node entry mode mismatch"
    );
    let output = PathBuf::from(
        manifest["output"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing output"))?,
    );
    anyhow::ensure!(
        !output.join("native_checkpoint.json").exists()
            && !output.join("native_roundtrip.json").exists(),
        "refusing to reuse prior node evidence"
    );
    let candidate = &manifest["candidate"];
    let market_id = candidate["marketId"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("missing discovered market"))?;
    let outcome = candidate["outcome"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("missing outcome"))? as u8;
    anyhow::ensure!([1, 2].contains(&outcome), "invalid outcome");
    let price = Decimal::from_str_exact(candidate["plan"]["entryPrice"].as_str().unwrap_or(""))?;
    let quantity =
        Decimal::from_str_exact(candidate["plan"]["grossQuantity"].as_str().unwrap_or(""))?;
    anyhow::ensure!(
        price > Decimal::ZERO
            && price <= Decimal::new(1, 1)
            && quantity * price <= Decimal::new(150, 2),
        "entry cap exceeded"
    );
    let mut config = settings.private_config()?;
    let origin = manifest["origin"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing relay"))?;
    anyhow::ensure!(
        origin.starts_with("http://127.0.0.1:"),
        "loopback relay required"
    );
    config.base_url_http = Some(origin.to_owned());
    let signer = settings.signer()?;
    let session = nautilus_predict::http::PredictPrivateHttpClient::new(config)?
        .authenticate(&signer)
        .await?;
    let market = session.request_execution_market(market_id).await?;
    let token = market
        .outcomes
        .iter()
        .find(|o| o.index_set == outcome)
        .ok_or_else(|| anyhow::anyhow!("missing token"))?
        .on_chain_id;
    let (_, approved, _, _) = PredictChainClient::new(settings.chain_config())?
        .request_sell_admission(
            session.account_address(),
            market.exchange(),
            token,
            get_atomic_clock_realtime(),
        )
        .await?;
    anyhow::ensure!(
        approved,
        "SELL not approved; no entry and no approval transaction"
    );
    let account = AccountId::new("PREDICT-calibration");
    let mut exec =
        PredictExecutionClientConfig::new(account, session, settings.websocket_config()?);
    exec.auth_signer = Some(signer.clone());
    exec.chain = Some(settings.chain_config());
    exec.orderbook_removal_enabled = true;
    exec.account_refresh_interval_secs = 1;
    exec.report_timeout_secs = settings.network.timeout_secs;
    exec.submission = Some(PredictSubmissionConfig {
        signer,

        market_slippage_bps: 100,
    });
    let mut live = node::build_node_with_engine_and_stop_delay(
        TraderId::new("PREDICT-FILL-001"),
        PredictDataClientConfig {
            api_key: settings
                .private_config()?
                .api_key
                .ok_or_else(|| anyhow::anyhow!("API key required"))?,
            market_ids: vec![market_id],
            base_url_http: None,
            base_url_ws: None,
            http_timeout_secs: 15,
            ws_timeout_secs: 15,
            dynamic_refresh_secs: 30,
            transport_backend: TransportBackend::Tungstenite,
        },
        exec,
        LiveExecutionEngineConfig {
            // Independent supervisor proves a flat initial account; retain native runtime checks.
            // Full startup history cannot adopt earlier manually-owned orders in this calibration.
            reconciliation: exit_only,
            // Supervisor proves flat state and no settlements inside this native lookback.
            position_check_lookback_mins: 1,
            reconciliation_startup_delay_secs: 0.0,
            inflight_check_interval_ms: 1000,
            open_check_interval_secs: Some(2.0),
            open_check_lookback_mins: None,
            open_check_open_only: true,
            position_check_interval_secs: Some(0.5),
            position_check_threshold_ms: 0,
            ..Default::default()
        },
        3,
    )?;
    let journal = Rc::new(RefCell::new(vec![]));
    let instrument = InstrumentId::from_as_ref(format!("{market_id}-{outcome}.PREDICT"))?;
    let handle = live.handle();
    let cache = live.kernel().cache.clone();
    let deadline = get_atomic_clock_realtime().get_time_ns().as_u64() + 60_000_000_000;
    let strategy = exits::ExitStrategy::new(
        instrument,
        outcome,
        journal.clone(),
        exits::ExitSettings::parse(include_str!(
            "../../../../examples/predict-exit-strategy.toml.example"
        ))?,
        market.fee_rate_bps,
    )
    .with_live_identity(market_id, account, deadline)
    .without_entry_amend()
    .with_native_entry(
        Price::from_decimal_dp(price, market.decimal_precision)?,
        Quantity::from_decimal_dp(quantity, 5)?,
    )
    .with_taker_entry()
    .with_native_quotes();
    let strategy = if exit_only && manifest["exitImmediately"] == true {
        strategy.with_requested_exit()
    } else {
        strategy
    };
    live.add_strategy(if exit_only {
        strategy.with_exit_only_recovery()
    } else {
        strategy
    })?;
    let observe = || -> anyhow::Result<Value> {
        use nautilus_model::orders::Order;
        let cache = cache.borrow();
        let events = journal.borrow();
        let orders = cache.orders(None, Some(&instrument), None, Some(&account), None);
        let positions = cache.positions_open(None, Some(&instrument), None, Some(&account), None);
        let state = cache.account(&account).and_then(|a| a.last_event());
        let entry = if exit_only {
            orders
                .iter()
                .find(|o| o.order_side() == nautilus_model::enums::OrderSide::Buy)
                .and_then(|o| cache.order(&o.client_order_id()))
        } else {
            cache.order(&nautilus_model::identifiers::ClientOrderId::new(
                "EXIT-ENTRY-001",
            ))
        };
        let baseline = events
            .iter()
            .find(|e| e["type"] == "NativeAccountSnapshot")
            .and_then(|e| e["balances"].as_array())
            .and_then(|rows| rows.iter().find(|b| b["currency"] == "USDT"))
            .and_then(|b| b["total"].as_str())
            .map(Decimal::from_str_exact)
            .transpose()?;
        let flow = fill_session::cycle_cash_flow(&events)?;
        let expected = baseline
            .map(|value| value + flow)
            .map(|value| nautilus_model::types::Money::from_decimal(value, Currency::USDT()))
            .transpose()?;
        let actual = cache
            .account(&account)
            .and_then(|a| a.balance_total(Some(Currency::USDT())));
        let latest_fill = orders
            .iter()
            .flat_map(|o| o.events())
            .filter_map(|e| {
                if let nautilus_model::events::OrderEventAny::Filled(f) = e {
                    Some(f.ts_event)
                } else {
                    None
                }
            })
            .max();
        let cash_current = expected.is_some()
            && expected == actual
            && latest_fill.is_some_and(|ts| state.as_ref().is_some_and(|s| s.ts_event >= ts));
        let complete = fill_session::SessionObservation {
            entry_closed: entry.as_ref().is_some_and(|o| o.is_closed()),
            entry_filled: entry
                .as_ref()
                .is_some_and(|o| o.filled_qty().as_decimal() > Decimal::ZERO),
            entry_window_elapsed: get_atomic_clock_realtime().get_time_ns().as_u64() >= deadline,
            has_entry: entry.is_some(),
            flat: positions.is_empty(),
            orders_terminal: orders.iter().all(|o| o.is_closed()),
            cash_current,
        }
        .completed();
        Ok(
            json!({"source":"actual mainnet Rust Strategy + LiveNode observation", "marketId":market_id,"outcome":outcome,
            "waitForStrategyExit":wait_for_exit,"exitOnly":exit_only,"sessionComplete":complete,
            "entryHasFill":entry.as_ref().is_some_and(|o|o.filled_qty().as_decimal()>Decimal::ZERO),
            "cashFlowUSDT":flow.to_string(),"expectedCash":expected.map(|v|v.to_string()),"cashCurrent":cash_current,
            "nativeFeedback":*events,"nativeOpenOrders":orders.iter().filter(|o|!o.is_closed()).count(),
            "nativeNetQuantity":positions.iter().map(|p|p.quantity.as_decimal()).sum::<Decimal>().to_string(),
            "balances":state.map(|s|s.balances.iter().map(|b|json!({"currency":b.currency.code,"total":b.total.to_string(),"locked":b.locked.to_string(),"free":b.free.to_string()})).collect::<Vec<_>>())}),
        )
    };
    let monitor = async {
        let mut observations = 0;
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let report = observe()?;
            fill_session::checkpoint(&output.join("native_checkpoint.json"), &report)?;
            observations += 1;
            anyhow::ensure!(
                !fill_session::strategy_failed(
                    report["nativeFeedback"].as_array().unwrap(),
                    "EXIT-ENTRY-001"
                ),
                "strategy cannot continue; inspect checkpoint and remaining inventory"
            );
            if report["sessionComplete"] == true || (!wait_for_exit && observations >= 300) {
                return Ok::<(), anyhow::Error>(());
            }
        }
    };
    let mut running = std::pin::pin!(live.run());
    let result = tokio::select! {
        result = &mut running => result,
        observed = monitor => {
            handle.stop();
            let result = running.await;
            observed.and(result)
        }
    };
    let mut report = observe()?;
    report["nodeFailed"] = json!(result.is_err());
    fill_session::checkpoint(&output.join("native_checkpoint.json"), &report)?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output.join("native_roundtrip.json"))?;
    std::io::Write::write_all(&mut file, &serde_json::to_vec_pretty(&report)?)?;
    result?;
    anyhow::ensure!(
        report["sessionComplete"] == true && report["entryHasFill"] == true,
        "round trip incomplete; inspect evidence and remaining inventory; supervisor cleans own orders"
    );
    anyhow::ensure!(
        journal
            .borrow()
            .iter()
            .any(|r| r["callback"] == "on_order_filled")
            && journal
                .borrow()
                .iter()
                .any(|r| r["callback"] == "on_position_closed"),
        "no proven filled/closed callbacks"
    );
    Ok(())
}
