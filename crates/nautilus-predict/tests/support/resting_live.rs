// -------------------------------------------------------------------------------------------------
// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
// See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------
//! Explicit mainnet test only. Native strategy owns lifecycle; REST verifies final cleanup.
use alloy::primitives::U256;
use nautilus_model::{enums::TimeInForce, identifiers::InstrumentId, types::Quantity};
use nautilus_network::websocket::TransportBackend;
use nautilus_predict::{
    PredictDataClientConfig, PredictSubmissionConfig,
    blockchain::PredictChainClient,
    http::{
        PredictAuthenticatedHttpClient, account_models::PredictRestOrder,
        query::PredictOrderStatusFilter,
    },
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use strategy::{CalibrationInputs, CalibrationStrategy, Observations, Phase, StrategySettings};

use super::*;

pub(super) async fn open_orders(
    session: &PredictAuthenticatedHttpClient,
) -> anyhow::Result<Vec<nautilus_predict::http::account_models::PredictOrderData>> {
    let mut cursor = None;
    let mut seen = std::collections::HashSet::new();
    let mut orders = vec![];
    for _ in 0..20 {
        let page = session
            .request_orders(&PredictOrdersQuery {
                first: Some(100),
                after: cursor.as_deref(),
                status: Some(PredictOrderStatusFilter::Open),
            })
            .await?;
        orders.extend(page.data);
        cursor = page.cursor;
        if let Some(next) = &cursor {
            anyhow::ensure!(seen.insert(next.clone()), "cleanup pagination cursor cycle");
        } else {
            return Ok(orders);
        }
    }
    anyhow::bail!("cleanup order pagination incomplete")
}

fn matching_terms(
    order: &PredictRestOrder,
    candidate: &discovery::Candidate,
    account: alloy::primitives::Address,
) -> bool {
    let fields = &order.fields;
    fields.maker == account
        && fields.signer == account
        && fields.side == nautilus_predict::common::enums::PredictOrderSide::Buy
        && candidate
            .market
            .outcomes
            .iter()
            .any(|o| o.index_set == candidate.outcome && o.on_chain_id == fields.token_id)
        && fields.maker_amount == U256::from(1_000_000_000_000_000_000u64)
        && [
            100_000_000_000_000_000_000u128,
            50_000_000_000_000_000_000u128,
        ]
        .into_iter()
        .any(|v| fields.taker_amount == U256::from(v))
}

pub(super) async fn cleanup(
    session: &PredictAuthenticatedHttpClient,
    candidate: &discovery::Candidate,
) -> anyhow::Result<Value> {
    // The test relay journals identity before forwarding, even when the POST reply is lost.
    // Same-price/quantity orders belonging to another run never grant removal authority.
    let journal_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/live_resting/http_responses.json");
    let journal: Value = serde_json::from_slice(&std::fs::read(journal_path)?)?;
    let submitted = submitted_hashes(&journal)?;
    let mut removed = vec![];
    for _ in 0..5 {
        let orders = open_orders(session).await?;
        let owned = orders
            .iter()
            .filter(|row| row.order.hash.is_some_and(|hash| submitted.contains(&hash)))
            .collect::<Vec<_>>();
        if owned.is_empty() {
            return Ok(
                json!({"verifiedOpenOrders":orders.len(),"verifiedRunOpenOrders":0,
                "unrelatedOpenOrders":orders.len(),"fallbackRemovedIds":removed}),
            );
        }
        let mut ids = vec![];
        for row in owned {
            anyhow::ensure!(
                matching_terms(&row.order, candidate, session.account_address())
                    && row.market_id == candidate.market.id,
                "unrelated open order found; refusing to cancel it as a calibration order"
            );
            let hash = row
                .order
                .hash
                .ok_or_else(|| anyhow::anyhow!("cleanup order has no hash"))?;
            let verified = session.request_order(hash).await?;
            anyhow::ensure!(
                verified.id == row.id
                    && verified.order.hash == Some(hash)
                    && verified.market_id == candidate.market.id
                    && matching_terms(&verified.order, candidate, session.account_address()),
                "cleanup order identity mismatch"
            );
            ids.push(row.id.parse::<U256>()?);
        }
        session.request_remove_orders(&ids).await?;
        removed.extend(ids.iter().map(ToString::to_string));
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    anyhow::bail!("cleanup did not establish zero open orders")
}

fn submitted_hashes(
    journal: &Value,
) -> anyhow::Result<std::collections::HashSet<alloy::primitives::B256>> {
    let events = journal["events"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("missing HTTP journal events"))?;
    events
        .iter()
        .filter(|event| event["method"] == "POST" && event["path"] == "/v1/orders")
        .map(|event| {
            serde_json::from_value(event["requestTerms"]["hash"].clone()).map_err(Into::into)
        })
        .collect()
}

#[test]
fn cleanup_hashes_include_unknown_submissions_and_exclude_other_requests() -> anyhow::Result<()> {
    let hash = alloy::primitives::B256::repeat_byte(0xab);
    let journal = json!({"events":[
        {"method":"POST","path":"/v1/orders","status":null,"requestTerms":{"hash":hash}},
        {"method":"GET","path":"/v1/orders","status":200},
        {"method":"POST","path":"/v1/orders/remove","status":200}
    ]});
    assert_eq!(
        submitted_hashes(&journal)?,
        std::collections::HashSet::from([hash])
    );
    assert!(submitted_hashes(&json!({"events":[{"method":"POST","path":"/v1/orders"}]})).is_err());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit mainnet quiet-market preflight; no order writes"]
async fn mainnet_quiet_resting_preflight_read_only() -> anyhow::Result<()> {
    let settings = operator_settings()?;
    let candidate = discovery::PublicApi::new(&settings)?
        .discover_quiet_resting(&settings)
        .await?;
    let (_, session) = settings.authenticate().await?;
    anyhow::ensure!(
        open_orders(&session).await?.is_empty(),
        "existing orders; no calibration allowed"
    );
    let (chain, _, _) = PredictChainClient::new(settings.chain_config())?
        .request_buy_admission(
            session.account_address(),
            candidate.market.exchange(),
            get_atomic_clock_realtime(),
        )
        .await?;
    let cost = U256::from(1_000_000_000_000_000_000u64);
    let allowance = chain
        .allowances
        .iter()
        .find(|(kind, _)| *kind == candidate.market.exchange())
        .map(|(_, v)| *v)
        .unwrap_or_default();
    let report = json!({"source":"mainnet read-only quiet selection and chain admission", "marketId":candidate.market.id,
        "outcome":candidate.outcome,"price":candidate.price.to_string(),"quantity":candidate.quantity.to_string(),
        "bestBid":candidate.best_bid.to_string(),"bestAsk":candidate.best_ask.to_string(),
        "orderUsdt":"1","runUsdt":"2","balanceSufficient":chain.balance_wei>=cost,
        "allowanceSufficient":allowance>=cost,"configuredOrderWrites":settings.test.allow_order_writes,
        "configuredOrderCap":settings.test.max_order_usdt.to_string(),"configuredRunCap":settings.test.max_total_usdt.to_string()});
    std::fs::write(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/private_api/quiet_resting_preflight.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("Quiet resting preflight: {}", report);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "MAINNET ORDER WRITES: explicit TOML opt-in; two post-only orders <=1 USDT each; final REST cleanup"]
async fn mainnet_rust_strategy_quiet_resting_cancel_replace() -> anyhow::Result<()> {
    // A separate 0600 one-run TOML avoids altering the ordinary read-only operator profile.
    let settings =
        PredictExecutionSettings::load(&root().join("env/predict-resting-calibration.toml"))?;
    anyhow::ensure!(
        settings.test.allow_order_writes && settings.test.require_post_only,
        "mainnet writes require explicit TOML opt-in and post-only policy"
    );
    anyhow::ensure!(
        settings.test.max_order_usdt == Decimal::ONE
            && settings.test.max_total_usdt == Decimal::from(2),
        "this calibration requires exactly 1 USDT/order and 2 USDT total; no cap overrides"
    );
    let signer = settings.signer()?;
    // This ignored test alone can use the supervisor's fixed-origin recording relay.
    // The ordinary settings loader and production clients do not read this manifest.
    let manifest: Value = serde_json::from_str(&std::fs::read_to_string(
        root().join("logs/predict-private-http-proxy.json"),
    )?)?;
    let case = manifest["case"].as_str().unwrap_or("auto");
    let measure_latency = manifest["measureLatency"].as_bool().unwrap_or(false);
    let (outcome, time_in_force) = match case {
        "auto" => (None, TimeInForce::Gtc),
        "yes-gtc" => (Some(1), TimeInForce::Gtc),
        "no-gtc" => (Some(2), TimeInForce::Gtc),
        "yes-gtd" => (Some(1), TimeInForce::Gtd),
        "no-gtd" => (Some(2), TimeInForce::Gtd),
        _ => anyhow::bail!("unsupported mainnet resting case"),
    };
    let candidate = discovery::PublicApi::new(&settings)?
        .discover_quiet_resting_outcome(&settings, outcome)
        .await?;
    let origin = manifest["origin"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing private HTTP relay origin"))?;
    anyhow::ensure!(
        origin
            .strip_prefix("http://127.0.0.1:")
            .is_some_and(|port| port.parse::<u16>().is_ok_and(|port| port > 0)),
        "private HTTP relay must use an explicit loopback port"
    );
    let mut private = settings.private_config()?;
    private.base_url_http = Some(origin.to_string());
    let session = nautilus_predict::http::PredictPrivateHttpClient::new(private)?
        .authenticate(&signer)
        .await?;
    session.request_account().await?;
    anyhow::ensure!(
        open_orders(&session).await?.is_empty(),
        "existing orders; no calibration allowed"
    );
    let (chain, _, _) = PredictChainClient::new(settings.chain_config())?
        .request_buy_admission(
            session.account_address(),
            candidate.market.exchange(),
            get_atomic_clock_realtime(),
        )
        .await?;
    let cost = U256::from(1_000_000_000_000_000_000u64);
    anyhow::ensure!(
        chain.balance_wei >= cost
            && chain
                .allowances
                .iter()
                .any(|(kind, v)| *kind == candidate.market.exchange() && *v >= cost),
        "insufficient collateral/allowance; this runner sends no approval transactions"
    );
    let mut execution = PredictExecutionClientConfig::new(
        AccountId::new("PREDICT-calibration"),
        session.clone(),
        settings.websocket_config()?,
    );
    let (latency, mut collector) = if measure_latency {
        let (probe, collector) = nautilus_predict::latency::PredictLatency::channel(
            TraderId::new("PREDICT-CALIBRATION-001"),
            AccountId::new("PREDICT-calibration"),
            4096,
        )?;
        (Some(probe), Some(collector))
    } else {
        (None, None)
    };
    execution.latency = latency.clone();
    execution.chain = Some(settings.chain_config());
    execution.account_refresh_interval_secs = 1;
    execution.report_timeout_secs = settings.network.timeout_secs;
    execution.orderbook_removal_enabled = true;
    execution.submission = Some(PredictSubmissionConfig {
        signer,

        market_slippage_bps: 100,
    });
    let key = settings
        .private_config()?
        .api_key
        .ok_or_else(|| anyhow::anyhow!("API key required"))?;
    let mut live = node::build_node(
        TraderId::new("PREDICT-CALIBRATION-001"),
        PredictDataClientConfig {
            api_key: key,
            market_ids: vec![candidate.market.id],
            base_url_http: None,
            base_url_ws: None,
            http_timeout_secs: settings.network.timeout_secs,
            ws_timeout_secs: settings.network.timeout_secs,
            dynamic_refresh_secs: 30,
            transport_backend: TransportBackend::Tungstenite,
        },
        execution,
    )?;
    let output =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/private_api/live_resting");
    std::fs::create_dir_all(&output)?;
    let observations = Rc::new(RefCell::new(Observations::default()));
    let confirmation = Rc::new(RefCell::new(None));
    let handle = live.handle();
    live.add_strategy(
        CalibrationStrategy::new(
            CalibrationInputs {
                account_id: AccountId::new("PREDICT-calibration"),
                settings: StrategySettings {
                    phase: Phase::Resting,
                    run_secs: 30,
                    stale_after_ms: 2000,
                    minimum_rebound_ticks: 1,
                },
                market_id: candidate.market.id,
                instrument_id: InstrumentId::from_as_ref(format!(
                    "{}-{}.PREDICT",
                    candidate.market.id, candidate.outcome
                ))?,
                outcome: candidate.outcome,
                quantity: Quantity::from_decimal_dp(candidate.quantity, 5)?,
                maximum_price: candidate.price,
                maximum_order_cost: Decimal::ONE,
                maximum_total_cost: Decimal::from(2),
                execution_enabled: true,
                fee_rate_bps: candidate.market.fee_rate_bps,
            },
            observations.clone(),
            handle.clone(),
        )
        .with_latency(latency)
        .with_live_feedback(output.join("native_feedback.json"), candidate.price)
        .with_resting_time_in_force(time_in_force)
        .with_resting_confirmation(confirmation.clone()),
    )?;
    let finished = std::cell::Cell::new(false);
    let api = discovery::PublicApi::new(&settings)?;
    let (result, (), ()) = tokio::join!(
        async {
            let result = live.run().await;
            finished.set(true);
            result
        },
        async {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            handle.stop();
        },
        async {
            while !finished.get() {
                if let Ok((bid, ask)) = api.confirm_book(&candidate).await {
                    *confirmation.borrow_mut() = Some(strategy::RestingBookConfirmation {
                        market_id: candidate.market.id,
                        outcome: candidate.outcome,
                        bid,
                        ask,
                        observed_ns: get_atomic_clock_realtime().get_time_ns().as_u64(),
                    });
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    );
    // Never return/assert on node failure before attempting final independently verified cleanup.
    let cleanup_result = cleanup(&session, &candidate).await;
    let observed = observations.borrow();
    if let Some(collector) = collector.as_mut() {
        // No monitor runs on the LiveNode executor. This bounded run drains only after shutdown.
        let metrics = handle.metrics_snapshot();
        std::fs::write(
            output.join("latency.json"),
            serde_json::to_vec_pretty(&json!({
                "source":"mainnet LiveNode; explicit typed strategy callback hooks; test HTTP relay",
                "profile":if cfg!(debug_assertions) {"debug"} else {"release"},
                "units":"nanoseconds; local monotonic clock",
                "commandPath":"venue_validated",
                "snapshot":collector.snapshot(),
                "nativeRunner":{"execEventQueueDepth":metrics.exec_events.queue_depth,
                    "execCommandQueueDepth":metrics.exec_commands.queue_depth,
                    "execEventsDispatched":metrics.exec_events.dispatched,
                    "execCommandsDispatched":metrics.exec_commands.dispatched,
                    "dispatchBusyNs":metrics.dispatch_busy_ns,"elapsedNs":metrics.elapsed_ns},
                "limitations":["two orders only; p95/p99 are sample maxima, not stable tail estimates",
                    "HTTP client intervals include the recording relay; compare its separately measured upstream intervals",
                    "RPC counters include startup and background account work; there is no per-order funds/OPEN query",
                    "REST-confirmed cancellations need not have a matching WS-origin callback"]
            }))?,
        )?;
        std::fs::write(
            output.join("native_feedback.json"),
            serde_json::to_vec_pretty(&observed.recorded_feedback)?,
        )?;
    }
    let report = json!({"source":"mainnet Rust Strategy + LiveNode; write-enabled test, see submitted count", "case":case,
        "timeInForce":time_in_force.to_string(),"postOnly":true,"marketId":candidate.market.id,
        "outcome":candidate.outcome,"price":candidate.price.to_string(),"quantity":candidate.quantity.to_string(),
        "snapshots":observed.snapshots,"submitted":observed.submitted,"accepted":observed.accepted,
        "rejected":observed.rejected,"denied":observed.denied,"modifyRejected":observed.modify_rejected,
        "cancelRejected":observed.cancel_rejected,"accountReportsObserved":observed.account_reports_observed,
        "positionEvents":observed.position_events,
        "modifyTermsPreserved":observed.modify_terms_preserved,"canceled":observed.canceled,"fills":observed.fills,
        "cashTotal":observed.last_cash_total.map(|v|v.to_string()),"cashLocked":observed.last_cash_locked.map(|v|v.to_string()),
        "cleanup":cleanup_result.as_ref().ok(),"cleanupFailed":cleanup_result.is_err(),"nodeFailed":result.is_err(),
        "feedbackWriteFailed":observed.feedback_write_failed});
    std::fs::write(
        output.join("result.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    cleanup_result?;
    result?;
    anyhow::ensure!(!observed.feedback_write_failed, "feedback journal failed");
    anyhow::ensure!(
        observed.submitted == 2
            && observed.accepted == 2
            && observed.canceled == 2
            && observed.modify_rejected == 2
            && observed.modify_terms_preserved == 2,
        "live lifecycle incomplete; see sanitized result.json; cleanup was attempted first"
    );
    anyhow::ensure!(
        observed.fills == 0 && observed.last_cash_locked == Some(Decimal::ZERO),
        "unexpected fill/locked funds; cleanup completed, inspect reconciliation"
    );
    println!(
        "Mainnet resting calibration: 2 accepted, 2 amend rejections, 2 canceled, no fills; REST zero open orders"
    );
    Ok(())
}
