// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Two real post-only orders, empty-cache startup, native batch cancellation. Explicit opt-in only.
use std::{cell::Cell, time::Duration};

use nautilus_live::node::config::LiveExecutionEngineConfig;
use nautilus_model::identifiers::{InstrumentId, StrategyId};
use nautilus_predict::{PredictDataClientConfig, blockchain::PredictChainClient};
use nautilus_trading::{StrategyConfig, StrategyCore};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use super::{batch_probe::BatchProbe, native_input_journal::NativeInputJournal, *};

fn probe(
    phase: &'static str,
    market: u64,
    outcome: u8,
    handle: nautilus_live::node::LiveNodeHandle,
    rows: Rc<RefCell<Vec<Value>>>,
    confirmation: Rc<RefCell<Option<strategy::RestingBookConfirmation>>>,
    complete: Rc<Cell<bool>>,
) -> BatchProbe {
    let instrument = InstrumentId::from(format!("{market}-{outcome}.PREDICT").as_str());
    BatchProbe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::new("RESTING-BATCH-001")),
            use_uuid_client_order_ids: true,
            external_order_instrument_ids: Some(vec![instrument]),
            ..Default::default()
        }),
        instrument,
        market_id: market,
        outcome,
        phase,
        place: phase == "place",
        handle,
        confirmation,
        journal: rows,
        complete,
        ids: vec![],
        book: None,
        modified: false,
        batch_sent: false,
        single_cancel: false,
        resume_cancel: None,
        query_closed_before_cancel: false,
        modify_rejections: 0,
        last_account: None,
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "MAINNET two <=1USDT post-only orders; explicit supervisor; cold-start and native batch cleanup"]
async fn mainnet_rust_strategy_batch_restart() -> anyhow::Result<()> {
    let settings =
        PredictExecutionSettings::load(&root().join("env/predict-resting-calibration.toml"))?;
    anyhow::ensure!(
        settings.test.allow_order_writes
            && settings.test.require_post_only
            && settings.test.max_order_usdt == Decimal::ONE
            && settings.test.max_total_usdt == Decimal::from(2),
        "bounded write profile required"
    );
    let manifest: Value = serde_json::from_str(&std::fs::read_to_string(
        root().join("logs/predict-private-http-proxy.json"),
    )?)?;
    anyhow::ensure!(
        matches!(
            manifest["case"].as_str(),
            Some("batch-restart" | "http-batch")
        ),
        "explicit batch supervisor required"
    );
    let origin = manifest["origin"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("relay origin missing"))?;
    anyhow::ensure!(
        origin
            .strip_prefix("http://127.0.0.1:")
            .is_some_and(|s| s.parse::<u16>().is_ok_and(|p| p > 0)),
        "loopback recorder required"
    );
    let candidate = discovery::PublicApi::new(&settings)?
        .discover_quiet_resting_outcome(&settings, None)
        .await?;
    let signer = settings.signer()?;
    let mut private = settings.private_config()?;
    private.base_url_http = Some(origin.to_owned());
    let session = nautilus_predict::http::PredictPrivateHttpClient::new(private)?
        .authenticate(&signer)
        .await?;
    anyhow::ensure!(
        resting_live::open_orders(&session).await?.is_empty(),
        "preexisting orders; no writes allowed"
    );
    let chain = PredictChainClient::new(settings.chain_config())?
        .request_buy_admission(
            session.account_address(),
            candidate.market.exchange(),
            get_atomic_clock_realtime(),
        )
        .await?
        .0;
    anyhow::ensure!(
        chain.balance_wei >= alloy::primitives::U256::from(2_000_000_000_000_000_000u64)
            && chain
                .allowances
                .iter()
                .any(|(kind, v)| *kind == candidate.market.exchange()
                    && *v >= alloy::primitives::U256::from(2_000_000_000_000_000_000u64)),
        "funds/allowance insufficient; no approval sent"
    );
    let rows = Rc::new(RefCell::new(vec![]));
    let result = async {
        let mut execution = PredictExecutionClientConfig::new(
            AccountId::new("PREDICT-calibration"),
            session.clone(),
            settings.websocket_config()?,
        );
        execution.chain = Some(settings.chain_config());
        execution.account_refresh_interval_secs = 1;
        execution.report_timeout_secs = settings.network.timeout_secs;
        execution.orderbook_removal_enabled = true;
        execution.submission_signer = Some(signer.clone());
        let key = settings
            .private_config()?
            .api_key
            .ok_or_else(|| anyhow::anyhow!("API key required"))?;
        let data = PredictDataClientConfig {
            api_key: key.clone(),
            market_ids: vec![candidate.market.id],
            base_url_http: None,
            base_url_ws: None,
            http_timeout_secs: settings.network.timeout_secs,
            ws_timeout_secs: settings.network.timeout_secs,
            dynamic_refresh_secs: 30,
            transport_backend: nautilus_network::websocket::TransportBackend::Tungstenite,
        };
        let engine = LiveExecutionEngineConfig {
            reconciliation: true,
            reconciliation_startup_delay_secs: 0.0,
            reconciliation_lookback_mins: Some(1),
            ..Default::default()
        };
        let mut first = node::build_node_with_engine(
            TraderId::new("PREDICT-CALIBRATION-001"),
            data,
            execution,
            engine,
        )?;
        let audit = NativeInputJournal::attach("place", rows.clone());
        let confirmation = Rc::new(RefCell::new(None));
        let complete = Rc::new(Cell::new(false));
        let handle = first.handle();
        first.add_strategy(probe(
            "place",
            candidate.market.id,
            candidate.outcome,
            handle.clone(),
            rows.clone(),
            confirmation.clone(),
            complete.clone(),
        ))?;
        let finished = Cell::new(false);
        let api = discovery::PublicApi::new(&settings)?;
        let run = async {
            let result = first.run().await;
            finished.set(true);
            result
        };
        let drive = async {
            let started = std::time::Instant::now();
            while !finished.get() {
                if started.elapsed() > Duration::from_secs(50) {
                    handle.stop();
                    break;
                }
                if let Ok((bid, ask)) = api.confirm_book(&candidate).await {
                    *confirmation.borrow_mut() = Some(strategy::RestingBookConfirmation {
                        market_id: candidate.market.id,
                        outcome: candidate.outcome,
                        bid,
                        ask,
                        observed_ns: get_atomic_clock_realtime().get_time_ns().as_u64(),
                    });
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        };
        let (ran, ()) = tokio::join!(run, drive);
        let place_ok = complete.get();
        first.dispose();
        drop(first);
        audit.detach();
        ran?;
        anyhow::ensure!(
            place_ok,
            "place phase incomplete; outer supervisor will clean up"
        );
        let pending = resting_live::open_orders(&session).await?;
        anyhow::ensure!(
            pending.len() == 2
                && pending
                    .iter()
                    .all(|row| row.market_id == candidate.market.id && row.amount_filled == "0"),
            "real pending snapshot differs from two untouched orders"
        );
        let handover = pending
            .iter()
            .map(|row| {
                json!({
                    "hash": row.order.hash.map(|hash| format!("{hash:#x}")),
                    "id": row.id, "status": "OPEN", "amountFilled": row.amount_filled,
                    "amount": row.amount,
                })
            })
            .collect::<Vec<_>>();
        rows.borrow_mut().push(json!({
            "phase":"handover", "type":"RealOpenOrdersBeforeColdStartup", "orders":handover,
        }));
        let mut profile = product_host::NodeProfile::parse(include_str!(
            "../../../../examples/predict-node.toml.example"
        ))?;
        profile.predict.http_url = Some(origin.to_owned());
        profile.predict.rpc_url = settings.network.rpc_url.expose_secret().to_owned();
        profile.predict.timeout_secs = settings.network.timeout_secs;
        profile.predict.orderbook_removal_enabled = true;
        profile.node.exec_engine.reconciliation_lookback_mins = Some(1);
        profile.node.exec_engine.reconciliation_startup_delay_secs = 0.0;
        profile.node.delay_post_stop = Duration::from_secs(1);
        anyhow::ensure!(
            !profile.predict.submission_enabled,
            "restart must not install a submission signer"
        );
        let prepared = product_host::prepare(profile, key, signer).await?;
        let mut second = prepared.node;
        anyhow::ensure!(
            second
                .kernel()
                .cache
                .borrow()
                .orders(None, None, None, None, None)
                .is_empty(),
            "restart Cache must be empty"
        );
        rows.borrow_mut().push(json!({
            "phase":"recover", "type":"BeforeNativeRun", "cachedOrders":0,
            "submissionSignerInstalled":false, "autoLoadedAccountMarket":candidate.market.id,
        }));
        let audit = NativeInputJournal::attach("recover", rows.clone());
        let complete = Rc::new(Cell::new(false));
        let handle = second.handle();
        second.add_strategy(probe(
            "recover",
            candidate.market.id,
            candidate.outcome,
            handle.clone(),
            rows.clone(),
            Rc::new(RefCell::new(None)),
            complete.clone(),
        ))?;
        let drive = async {
            let result = tokio::time::timeout(Duration::from_secs(50), async {
                loop {
                    let ws_cancels = rows
                        .borrow()
                        .iter()
                        .filter(|row| {
                            row["phase"] == "recover"
                                && row["type"] == "RawOrderStatusReport"
                                && row["report"]["order_status"] == "CANCELED"
                        })
                        .count();
                    if complete.get() && ws_cancels >= 2 {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await;
            handle.stop();
            result
        };
        let (ran, checked) = tokio::join!(second.run(), drive);
        second.dispose();
        drop(second);
        audit.detach();
        ran?;
        checked.map_err(|_| anyhow::anyhow!("batch/duplicate WS/account evidence incomplete"))?;
        anyhow::ensure!(complete.get(), "batch completion not observed");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    // Never return from a write-capable test before attempting independently verified cleanup.
    let cleanup = resting_live::cleanup(&session, &candidate).await;
    let output = root().join("crates/nautilus-predict/tests/fixtures/private_api/live_resting");
    let journal = rows.borrow().clone();
    serde_json::to_writer_pretty(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output.join("native_input_and_callbacks.json"))?,
        &journal,
    )?;
    let callbacks = |phase: &str, kind: &str| {
        journal
            .iter()
            .filter(|r| {
                r["phase"] == phase && r["type"] == "StrategyOrderCallback" && r["kind"] == kind
            })
            .count()
    };
    let report = json!({"source":"actual Rust strategy + Native LiveNode cold startup and batch cancel","case":manifest["case"],"marketId":candidate.market.id,"outcome":candidate.outcome,"submitted":2,"accepted":callbacks("place","Accepted"),"canceled":callbacks("recover","Canceled"),"modifyRejected":callbacks("recover","ModifyRejected"),"modifyTermsPreserved":journal.iter().filter(|r|r["type"]=="ModifyTermsPreserved").count(),"rejected":callbacks("place","Rejected")+callbacks("recover","Rejected"),"denied":callbacks("place","Denied")+callbacks("recover","Denied"),"fills":callbacks("place","Filled")+callbacks("recover","Filled"),"nodeFailed":result.is_err(),"cleanup":cleanup.as_ref().ok(),"cleanupFailed":cleanup.is_err(),"originalError":result.as_ref().err().map(ToString::to_string)});
    std::fs::write(
        output.join("result.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    cleanup?;
    result?;
    anyhow::ensure!(
        report["accepted"] == 2
            && report["canceled"] == 2
            && report["modifyRejected"] == 2
            && report["fills"] == 0,
        "unexpected lifecycle totals"
    );
    Ok(())
}
