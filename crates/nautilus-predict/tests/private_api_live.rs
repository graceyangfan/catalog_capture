// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/LICENSE-3.0.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Opt-in mainnet calibration. Read-only tests and a separately gated resting-order test.
#![cfg(feature = "execution")]

#[path = "support/discovery.rs"]
mod discovery;
#[path = "support/fill_live.rs"]
mod fill_live;
#[path = "support/fill_session.rs"]
mod fill_session;
#[path = "support/market_probe_live.rs"]
mod market_probe_live;
#[path = "support/node.rs"]
mod node;
#[path = "support/resting_live.rs"]
mod resting_live;
#[path = "support/settings.rs"]
mod settings;
#[path = "support/strategy.rs"]
mod strategy;

use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    rc::Rc,
};

use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    clock::VirtualClock,
    live::runner::replace_exec_event_sender,
    messages::{ExecutionEvent, execution::GeneratePositionStatusReports},
    msgbus::{self, MessageBus, MessagingSwitchboard},
};
use nautilus_core::{UUID4, time::get_atomic_clock_realtime};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    accounts::Account,
    enums::{AccountType, OmsType},
    identifiers::{AccountId, ClientId, TraderId},
    types::Currency,
};
use nautilus_portfolio::portfolio::Portfolio;
use nautilus_predict::{
    PredictExecutionClient, PredictExecutionClientConfig, PredictHttpClient,
    PredictInstrumentProvider,
    common::PREDICT_VENUE,
    http::query::{PredictOrdersQuery, PredictPositionsQuery},
};
use settings::PredictExecutionSettings;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}
fn operator_settings() -> anyhow::Result<PredictExecutionSettings> {
    PredictExecutionSettings::load(&root().join("env/predict-execution.toml"))
}

#[path = "support/history_survey.rs"]
mod history_survey;

#[tokio::test]
#[ignore = "explicit bounded read-only survey through a write-blocking loopback relay"]
async fn mainnet_record_order_lifecycle_survey_readonly() -> anyhow::Result<()> {
    history_survey::run().await
}

#[tokio::test]
#[ignore = "explicit read-only historical order recording through a write-blocking loopback relay"]
async fn mainnet_record_historical_order_semantics_readonly() -> anyhow::Result<()> {
    use nautilus_predict::signing::PredictOrderHasher;
    use serde_json::{Value, json};
    let origin = std::env::var("PREDICT_HISTORY_READONLY_HTTP_ORIGIN")?;
    anyhow::ensure!(
        origin.starts_with("http://127.0.0.1:"),
        "history recorder requires loopback relay"
    );
    let output = std::env::var("PREDICT_HISTORY_READONLY_FEEDBACK")?;
    let settings = operator_settings()?;
    let wallet = settings.signer()?;
    let mut config = settings.private_config()?;
    config.base_url_http = Some(origin);
    let session = nautilus_predict::http::PredictPrivateHttpClient::new(config)?
        .authenticate(&wallet)
        .await?;
    session.request_account().await?;
    let hasher = PredictOrderHasher::new(session.environment());
    let matrix: Value = serde_json::from_str(include_str!(
        "fixtures/private_api/market_parameter_matrix_no_match_20261024.json"
    ))?;
    let mut targets = matrix["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case["request"]["isMinAmountOut"] != true)
        .map(|case| {
            (
                case["name"].as_str().unwrap().to_owned(),
                case["request"]["hash"].as_str().unwrap().to_owned(),
            )
        })
        .collect::<Vec<_>>();
    let atomic: Value = serde_json::from_str(include_str!(
        "fixtures/private_api/native_market_filled_atomic_remainder_20261025.json"
    ))?;
    targets.push((
        "native_buy_atomic_remainder".into(),
        atomic["orderHash"].as_str().unwrap().into(),
    ));
    let full: Value = serde_json::from_str(include_str!(
        "fixtures/private_api/native_non_fok_full_plan_two_settlements_20261025.json"
    ))?;
    targets.push((
        "native_buy_full_plan".into(),
        full["request"]["data"]["order"]["hash"]
            .as_str()
            .unwrap()
            .into(),
    ));
    let exit = full["sdkExit"]["walletEvents"]
        .as_array()
        .unwrap()
        .iter()
        .find_map(|event| event["orderHash"].as_str())
        .ok_or_else(|| anyhow::anyhow!("recorded SELL hash missing"))?;
    targets.push(("sdk_sell_full_plan".into(), exit.into()));
    let mut feedback = json!({"source":"Read-only own-crate queries of nine previously recorded orders; raw HTTP retained independently by write-blocking relay",
        "orderWrites":0,"chainWrites":0,"orders":[]});
    for (name, hash) in targets {
        let hash = hash.parse()?;
        let result = session.request_order(hash).await;
        let observation = match result {
            Ok(row) => {
                let exchange =
                    nautilus_predict::common::contracts::PredictExchangeType::from_market_flags(
                        row.is_neg_risk,
                        row.is_yield_bearing,
                    );
                anyhow::ensure!(
                    hasher.order_hash(&row.order.fields, exchange) == hash,
                    "historical signed terms do not reproduce requested hash"
                );
                json!({"name":name,"hash":hash,"status":format!("{:?}",row.status),"strategy":format!("{:?}",row.strategy),
                    "side":format!("{:?}",row.order.fields.side),"amount":row.amount,"amountFilled":row.amount_filled,
                    "makerAmount":row.order.fields.maker_amount.to_string(),"takerAmount":row.order.fields.taker_amount.to_string(),
                    "nativeHashVerified":true,"snapshotAtUtc":chrono::Utc::now().to_rfc3339()})
            }
            Err(error) => {
                json!({"name":name,"hash":hash,"queryError":error.to_string(),"interpretation":"No inferred order outcome from a failed historical lookup"})
            }
        };
        feedback["orders"].as_array_mut().unwrap().push(observation);
        std::fs::write(&output, serde_json::to_vec_pretty(&feedback)?)?;
    }
    anyhow::ensure!(
        feedback["orders"].as_array().unwrap().len() == 9,
        "historical target count changed"
    );
    Ok(())
}

#[test]
fn toml_example_has_no_credentials_and_rejects_manual_market_selectors() {
    let example = include_str!("../../../examples/predict-execution.toml.example");
    let settings = PredictExecutionSettings::parse(example).unwrap();
    assert!(!settings.test.allow_order_writes);
    assert!(settings.test.require_post_only);
    assert!(settings.signer().is_err());
    let invalid = format!("{example}\nmarket_id=42\n");
    assert!(PredictExecutionSettings::parse(&invalid).is_err());
    assert!(
        PredictExecutionSettings::parse(
            &example.replace("max_order_usdt = \"0.10\"", "max_order_usdt = 0.10")
        )
        .is_err()
    );
    assert!(
        PredictExecutionSettings::parse(
            &example.replace("max_total_usdt = \"0.20\"", "max_total_usdt = \"0.01\"")
        )
        .is_err()
    );
}

#[test]
fn privy_settings_match_sdk_deposit_identity_and_redact_parser_errors() {
    let public_key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    let example = include_str!("../../../examples/predict-execution.toml.example")
        .replace(
            "private_key = \"\"",
            &format!("private_key = \"{public_key}\""),
        )
        .replace(
            "predict_account = \"\"",
            "predict_account = \"0x1111111111111111111111111111111111111111\"",
        )
        .replace(
            "owner_address = \"\"",
            "owner_address = \"0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266\"",
        );
    let settings = PredictExecutionSettings::parse(&example).unwrap();
    let signer = settings.signer().unwrap();
    assert_ne!(signer.owner_address(), signer.account_address());
    assert_eq!(
        signer.account_address().to_string().to_lowercase(),
        "0x1111111111111111111111111111111111111111"
    );
    assert!(!format!("{settings:?}").contains(public_key));
    assert!(
        PredictExecutionSettings::parse(&example.replace(
            "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
            "0x1111111111111111111111111111111111111111"
        ))
        .unwrap()
        .signer()
        .is_err()
    );
    let malformed = format!("{example}\nunknown_secret=\"credential-sentinel\"\n");
    let error = PredictExecutionSettings::parse(&malformed)
        .unwrap_err()
        .to_string();
    assert!(!error.contains("credential-sentinel") && !error.contains(public_key));
}

#[tokio::test]
#[ignore = "requires the operator's mainnet API-key TOML; read-only discovery"]
async fn mainnet_discovers_markets_without_configured_ids() -> anyhow::Result<()> {
    let settings = operator_settings()?;
    let api = discovery::PublicApi::new(&settings)?;
    let resting = api.discover(&settings, discovery::Phase::Resting).await?;
    println!(
        "Resting candidate: market={} outcome={} price={} shares={} collateral={} best_bid={} best_ask={} fee_bps={}",
        resting.market.id,
        resting.outcome,
        resting.price,
        resting.quantity,
        resting.price * resting.quantity,
        resting.best_bid,
        resting.best_ask,
        resting.market.fee_rate_bps
    );
    match api.discover(&settings, discovery::Phase::Fill).await {
        Ok(fill) => println!(
            "Low-price candidate: market={} outcome={} price={} shares={} collateral={} fee_bps={} native_submission_supported={}",
            fill.market.id,
            fill.outcome,
            fill.price,
            fill.quantity,
            fill.price * fill.quantity,
            fill.market.fee_rate_bps,
            fill.market.fee_rate_bps == 0
        ),
        Err(error) => println!("No eligible low-price candidate: {error}"),
    }
    println!(
        "SDK quantity lower bound only; venue acceptance and minimum notional remain unverified. No orders sent."
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires local Privy key and explicit wallet mode; read-only mainnet calibration"]
async fn mainnet_privy_private_api_and_native_account_read_only() -> anyhow::Result<()> {
    let settings = operator_settings()?;
    let (_signer, session) = settings.authenticate().await?;
    // Private DTOs are observed before interpreting their units or publishing position state.
    let orders = session
        .request_orders(&PredictOrdersQuery {
            first: Some(100),
            ..Default::default()
        })
        .await?;
    let positions = session
        .request_positions(&PredictPositionsQuery {
            first: Some(100),
            include_lost: Some(true),
            ..Default::default()
        })
        .await?;
    let cache = Rc::new(RefCell::new(Cache::default()));
    let trader = TraderId::new("PREDICT-PROBE-001");
    let account_id = AccountId::new("PREDICT-probe");
    msgbus::set_message_bus(Rc::new(RefCell::new(MessageBus::new(
        trader,
        UUID4::new(),
        None,
        None,
    ))));
    let mut clock = VirtualClock::new();
    clock.advance_time(get_atomic_clock_realtime().get_time_ns(), true);
    let _portfolio = Portfolio::new(Rc::new(RefCell::new(clock)), cache.clone(), None);
    let cfg_http = settings.private_config()?;
    let key = cfg_http
        .api_key
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("mainnet API key is required"))?;
    let mut provider = PredictInstrumentProvider::new(PredictHttpClient::new(
        key.expose_secret(),
        None,
        settings.network.timeout_secs,
    )?);
    let mut market_ids = std::collections::HashSet::new();
    for row in &positions.data {
        if market_ids.insert(row.market.id) {
            let (_, defs) = provider
                .load_market_with_definitions(
                    row.market.id,
                    get_atomic_clock_realtime().get_time_ns(),
                )
                .await?;
            for instrument in defs {
                cache.borrow_mut().add_instrument(instrument)?;
            }
        }
    }
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    replace_exec_event_sender(tx);
    let core = ExecutionClientCore::new(
        trader,
        ClientId::new("PREDICT"),
        *PREDICT_VENUE,
        OmsType::Netting,
        account_id,
        AccountType::Cash,
        None,
        cache.clone(),
    );
    let mut config =
        PredictExecutionClientConfig::new(account_id, session, settings.websocket_config()?);
    config.chain = Some(settings.chain_config());
    // This diagnostic host explicitly controls read-only queries and has no LiveNode clock.
    config.reconnect_reconciliation_interval_secs = None;
    config.report_timeout_secs = settings.network.timeout_secs;
    // Native submission stays disabled regardless of the operator's test write flag.
    let mut client = PredictExecutionClient::new(core, config)?;
    client.start()?;
    let (connected, registered) = tokio::join!(client.connect(), async {
        let event = tokio::time::timeout(
            std::time::Duration::from_secs(settings.network.timeout_secs + 2),
            rx.recv(),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("native account channel closed"))?;
        let ExecutionEvent::Account(state) = event else {
            anyhow::bail!("expected native AccountState");
        };
        msgbus::send_account_state(MessagingSwitchboard::portfolio_update_account(), &state);
        Ok::<_, anyhow::Error>(state)
    });
    connected?;
    let state = registered?;
    anyhow::ensure!(
        state.is_reported && !client.get_account().unwrap().calculated_account_state(),
        "expected native reported cash ownership"
    );
    let task = client
        .generate_position_status_reports_task(&GeneratePositionStatusReports::new(
            UUID4::new(),
            get_atomic_clock_realtime().get_time_ns(),
            None,
            None,
            None,
            None,
            None,
        ))
        .ok_or_else(|| anyhow::anyhow!("native position report task unavailable"))?;
    tokio::spawn(task.collection).await?;
    let reports = (task.finish)()?;
    let cash = state
        .balances
        .iter()
        .find(|b| b.currency == Currency::USDT())
        .ok_or_else(|| anyhow::anyhow!("native USDT balance missing"))?;
    println!("Privy JWT, account identity, wallet ACK and native Portfolio registration: OK");
    println!(
        "Reported cash: total={} locked={} free={}; native position reports={}",
        cash.total,
        cash.locked,
        cash.free,
        reports.len()
    );
    println!(
        "First private pages: orders={} positions={}; cursors retained; no empty snapshot inferred as flat",
        orders.data.len(),
        positions.data.len()
    );
    client.disconnect().await?;
    Ok(())
}

#[test]
fn explicit_eoa_settings_use_owner_identity_and_reject_deposit_fields() {
    let example = include_str!("../../../examples/predict-execution.toml.example")
        .replace("mode = \"predict_account\"", "mode = \"eoa\"")
        .replace(
            "private_key = \"\"",
            "private_key = \"0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80\"",
        );
    let settings = PredictExecutionSettings::parse(&example).unwrap();
    let signer = settings.signer().unwrap();
    assert_eq!(signer.owner_address(), signer.account_address());
    let bad = example.replace(
        "predict_account = \"\"",
        "predict_account = \"0x1111111111111111111111111111111111111111\"",
    );
    assert!(
        PredictExecutionSettings::parse(&bad)
            .unwrap()
            .signer()
            .is_err()
    );
}

#[test]
fn captured_custom_snapshots_replay_exact_levels_and_reject_foreign_identity() {
    use nautilus_model::{identifiers::InstrumentId, types::fixed::FIXED_PRECISION};
    use nautilus_predict::{
        PredictOrderbookSnapshot, PredictOrderbookWire, common::PredictMarketContext,
    };
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/private_api/public_snapshots.json")).unwrap();
    let snapshots = fixture["snapshots"].as_array().unwrap();
    assert!(!snapshots.is_empty());
    assert!(snapshots.len() <= 16);
    for source in snapshots {
        let wire: PredictOrderbookWire = serde_json::from_value(source.clone()).unwrap();
        let id = wire.market_id;
        let context = PredictMarketContext {
            market_id: id,
            yes_instrument_id: InstrumentId::from_as_ref(format!("{id}-1.PREDICT")).unwrap(),
            no_instrument_id: InstrumentId::from_as_ref(format!("{id}-2.PREDICT")).unwrap(),
            price_precision: fixture["decimalPrecision"]
                .as_u64()
                .unwrap()
                .try_into()
                .unwrap(),
            size_precision: FIXED_PRECISION,
        };
        let expected_time = wire.update_timestamp_ms * 1_000_000;
        let snapshot =
            PredictOrderbookSnapshot::from_wire(&context, wire.clone(), expected_time.into())
                .unwrap();
        assert!(!snapshot.bids.is_empty() && !snapshot.asks.is_empty());
        assert_eq!(snapshot.ts_event.as_u64(), expected_time);
        for (level, (price, quantity)) in snapshot.bids.iter().zip(&wire.bids) {
            assert_eq!(level.price.as_decimal(), *price);
            assert_eq!(level.quantity.as_decimal(), *quantity);
        }
        let data = snapshot.into_custom_data();
        assert_eq!(
            data.data_type,
            nautilus_predict::predict_orderbook_data_type(id)
        );
        let mut foreign = wire;
        foreign.market_id += 1;
        assert!(
            PredictOrderbookSnapshot::from_wire(&context, foreign, expected_time.into()).is_err()
        );
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "real Privy key/market/RPC read-only preflight and official SDK signature comparison; no order POST"]
async fn mainnet_resting_preflight_and_sdk_signing_read_only() -> anyhow::Result<()> {
    use std::process::Stdio;

    use alloy::primitives::{Address, U256};
    use nautilus_predict::{
        blockchain::PredictChainClient,
        common::enums::{PredictOrderSide, PredictSignatureType},
        http::order_models::PredictOrder,
        signing::PredictOrderSigner,
    };
    use tokio::io::AsyncWriteExt;

    let settings = operator_settings()?;
    let candidate = discovery::PublicApi::new(&settings)?
        .discover(&settings, discovery::Phase::Resting)
        .await?;
    let (wallet, session) = settings.authenticate().await?;
    let market = session
        .request_execution_market(candidate.market.id)
        .await?;
    let clock = get_atomic_clock_realtime();
    let (chain, nonce, _) = PredictChainClient::new(settings.chain_config())?
        .request_buy_admission(session.account_address(), market.exchange(), clock)
        .await?;
    let wei = |value: rust_decimal::Decimal| -> anyhow::Result<U256> {
        let exact = value * rust_decimal::Decimal::from(1_000_000_000_000_000_000u64);
        anyhow::ensure!(
            exact.fract().is_zero(),
            "diagnostic amount is not exact wei"
        );
        Ok(exact.normalize().to_string().parse()?)
    };
    let cost = candidate.price * candidate.quantity;
    anyhow::ensure!(
        cost <= settings.test.max_order_usdt && cost <= settings.test.max_total_usdt,
        "diagnostic order exceeds configured caps"
    );
    let token = market
        .outcomes
        .iter()
        .find(|o| o.index_set == candidate.outcome)
        .ok_or_else(|| anyhow::anyhow!("diagnostic outcome missing"))?
        .on_chain_id;
    let price = wei(candidate.price)?;
    let signed = PredictOrderSigner::new(wallet).sign_order(
        &PredictOrder {
            salt: U256::from(u128::from_be_bytes(UUID4::new().as_bytes()) % 2_147_483_648),
            maker: session.account_address(),
            signer: session.account_address(),
            taker: Address::ZERO,
            token_id: token,
            maker_amount: wei(cost)?,
            taker_amount: wei(candidate.quantity)?,
            expiration: U256::from(4_102_444_800u64),
            nonce,
            fee_rate_bps: U256::from(market.fee_rate_bps),
            side: PredictOrderSide::Buy,
            signature_type: PredictSignatureType::Eoa,
        },
        market.exchange(),
    )?;
    // This live order signature stays on a private pipe and is never sent to the venue.
    let comparison = serde_json::json!({"order":signed.order(), "priceWei":price.to_string(),
        "isNegRisk":market.is_neg_risk,"isYieldBearing":market.is_yield_bearing,
        "hash":signed.hash().to_string(),"signature":signed.signature().expose_secret(),
        "body":nautilus_predict::http::command_models::PredictCreateOrderRequest::limit(&signed,price,true)?});
    let mut child = tokio::process::Command::new("uv")
        .arg("run")
        .arg(root().join("tests/compare_predict_live_signing.py"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let bytes = serde_json::to_vec(&comparison)?;
    let mut input = child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("SDK comparison pipe unavailable"))?;
    input.write_all(&bytes).await?;
    drop(input);
    let output = tokio::time::timeout(std::time::Duration::from_secs(30), child.wait_with_output())
        .await??;
    anyhow::ensure!(
        output.status.success(),
        "official SDK comparison failed; no order was sent"
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    for field in [
        "amounts_match",
        "hash_matches",
        "signature_matches",
        "request_body_matches",
    ] {
        anyhow::ensure!(
            result[field] == true,
            "official SDK parity check failed: {field}"
        );
    }
    let allowance = chain
        .allowances
        .iter()
        .find(|(exchange, _)| *exchange == market.exchange())
        .map(|(_, amount)| *amount)
        .unwrap_or_default();
    println!(
        "Real Privy SDK parity: amounts/hash/signature/full request body OK; fee_bps={} allowance_sufficient={} post_only_maker_model_supported={}; no order POST",
        market.fee_rate_bps,
        allowance >= signed.order().maker_amount,
        market.fee_rate_bps <= 10_000
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit mainnet Rust LiveNode strategy observation; no order or blockchain writes"]
async fn mainnet_rust_strategy_livenode_observe() -> anyhow::Result<()> {
    read_only_strategy_observation("public_snapshots.json", false).await
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "mainnet Rust strategy MARKET readiness observation only; no order or chain writes"]
async fn mainnet_market_readiness_livenode_observe() -> anyhow::Result<()> {
    read_only_strategy_observation("market_read_only_livenode_20261015.json", true).await
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "mainnet read-only Rust strategy observation; saves new JSON, no order writes"]
async fn mainnet_current_livenode_observe() -> anyhow::Result<()> {
    let name = format!(
        "read_only_livenode_{}_{}.json",
        chrono::Utc::now().format("%Y%m%dT%H%M%SZ"),
        std::process::id()
    );
    read_only_strategy_observation(&name, true).await
}

async fn read_only_strategy_observation(
    output_file: &str,
    market_probe: bool,
) -> anyhow::Result<()> {
    read_only_strategy_observation_with_reconciliation(output_file, market_probe, false).await
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "mainnet read-only Native startup reconciliation + Rust strategy; no submission signer"]
async fn mainnet_native_startup_reconciliation_read_only_capture() -> anyhow::Result<()> {
    let name = format!(
        "native_startup_readonly_{}_{}.json",
        chrono::Utc::now().format("%Y%m%dT%H%M%SZ"),
        std::process::id()
    );
    read_only_strategy_observation_with_reconciliation(&name, true, true).await
}

async fn read_only_strategy_observation_with_reconciliation(
    output_file: &str,
    market_probe: bool,
    reconciliation: bool,
) -> anyhow::Result<()> {
    use nautilus_model::{identifiers::InstrumentId, types::Quantity};
    use nautilus_network::websocket::TransportBackend;
    use nautilus_predict::PredictDataClientConfig;
    use strategy::{CalibrationInputs, CalibrationStrategy, Observations, Phase, StrategySettings};

    let settings = operator_settings()?;
    let strategy_settings = StrategySettings::parse(&std::fs::read_to_string(
        root().join("env/predict-strategy.toml"),
    )?)?;
    anyhow::ensure!(
        strategy_settings.phase == Phase::Observe,
        "this mainnet runner supports observe only; resting/fill require dedicated write calibration"
    );
    let candidate = discovery::PublicApi::new(&settings)?
        .discover(&settings, discovery::Phase::Fill)
        .await?;
    let (_, session) = settings.authenticate().await?;
    let mut execution = PredictExecutionClientConfig::new(
        AccountId::new("PREDICT-calibration"),
        session,
        settings.websocket_config()?,
    );
    execution.chain = Some(settings.chain_config());
    execution.report_timeout_secs = settings.network.timeout_secs;
    execution.account_refresh_interval_secs = 5;
    let key = settings
        .private_config()?
        .api_key
        .ok_or_else(|| anyhow::anyhow!("mainnet API key required"))?;
    anyhow::ensure!(
        execution.submission_signer.is_none(),
        "read-only host must not install a submission signer"
    );
    let mut live = node::build_node_with_engine(
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
        nautilus_live::node::config::LiveExecutionEngineConfig {
            reconciliation,
            reconciliation_startup_delay_secs: 0.0,
            reconciliation_lookback_mins: Some(1),
            inflight_check_interval_ms: 0,
            ..Default::default()
        },
    )?;
    let cache = live.kernel().cache.clone();
    let observations = Rc::new(RefCell::new(Observations::default()));
    let handle = live.handle();
    let run_secs = strategy_settings.run_secs;
    live.add_strategy(CalibrationStrategy::new(
        CalibrationInputs {
            account_id: AccountId::new("PREDICT-calibration"),
            settings: strategy_settings,
            market_id: candidate.market.id,
            instrument_id: InstrumentId::from_as_ref(format!(
                "{}-{}.PREDICT",
                candidate.market.id, candidate.outcome
            ))?,
            outcome: candidate.outcome,
            quantity: Quantity::from_decimal_dp(candidate.quantity, 5)?,
            maximum_price: settings.test.max_fill_price,
            maximum_order_cost: settings.test.max_order_usdt,
            maximum_total_cost: settings.test.max_total_usdt,
            execution_enabled: false,
            fee_rate_bps: candidate.market.fee_rate_bps,
        },
        observations.clone(),
        handle.clone(),
    ))?;
    let (result, ()) = tokio::join!(live.run(), async {
        tokio::time::sleep(std::time::Duration::from_secs(run_secs)).await;
        handle.stop();
    });
    result?;
    let observed = observations.borrow();
    anyhow::ensure!(
        observed.snapshots > 0,
        "strategy received no custom orderbook snapshots"
    );
    anyhow::ensure!(
        observed.submitted == 0 && observed.fills == 0,
        "observe mode unexpectedly traded"
    );
    anyhow::ensure!(
        observed.account_reports_observed > 0,
        "strategy observed no applied native account reports"
    );
    println!(
        "Rust LiveNode strategy: snapshots={} account_events={} submitted=0 fills=0",
        observed.snapshots, observed.account_reports_observed
    );
    let mut fixture = serde_json::json!({
        "source":"mainnet Rust LiveNode CustomData callbacks; normalized exact decimal levels",
        "decimalPrecision":candidate.market.decimal_precision,
        "snapshots":observed.recorded_snapshots,
    });
    if market_probe {
        fixture["source"] = serde_json::json!(
            "actual mainnet Rust Strategy + LiveNode observation; normalized exact public snapshots and applied account reports; no order writes"
        );
        fixture["observedAtUtc"] = serde_json::json!(chrono::Utc::now().to_rfc3339());
        fixture["marketId"] = serde_json::json!(candidate.market.id);
        fixture["outcome"] = serde_json::json!(candidate.outcome);
        fixture["feeRateBps"] = serde_json::json!(candidate.market.fee_rate_bps);
        fixture["nativeFeedback"] = serde_json::json!(observed.recorded_feedback);
        fixture["strategySubmissions"] = serde_json::json!(observed.submitted);
        fixture["fills"] = serde_json::json!(observed.fills);
        fixture["accountReportsObserved"] = serde_json::json!(observed.account_reports_observed);
        fixture["cashTotal"] = serde_json::json!(observed.last_cash_total.map(|v| v.to_string()));
        fixture["cashLocked"] = serde_json::json!(observed.last_cash_locked.map(|v| v.to_string()));
        fixture["tradeAllowed"] = serde_json::json!(false);
    }
    fixture["nativeStartupReconciliation"] = serde_json::json!(reconciliation);
    fixture["reconciliationLookbackMins"] = serde_json::json!(1);
    fixture["submissionSignerInstalled"] = serde_json::json!(false);
    fixture["nativeCachedOpenOrders"] = serde_json::json!(
        cache
            .borrow()
            .orders_open(None, None, None, None, None)
            .len()
    );
    fixture["nativeCachedOpenPositions"] = serde_json::json!(
        cache
            .borrow()
            .positions_open(None, None, None, None, None)
            .len()
    );
    let output = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api")
        .join(output_file);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    std::io::Write::write_all(
        &mut file,
        serde_json::to_string_pretty(&fixture)?.as_bytes(),
    )?;
    Ok(())
}

#[test]
fn resting_rest_confirmation_requires_exact_identity_prices_and_freshness() {
    use rust_decimal::Decimal;
    let confirmation = strategy::RestingBookConfirmation {
        market_id: 42,
        outcome: 2,
        bid: Decimal::new(82, 2),
        ask: Decimal::new(99, 2),
        observed_ns: 3_000_000_000,
    };
    assert!(confirmation.matches(
        4_000_000_000,
        42,
        2,
        Decimal::new(82, 2),
        Decimal::new(99, 2),
        2000
    ));
    assert!(!confirmation.matches(
        6_000_000_000,
        42,
        2,
        Decimal::new(82, 2),
        Decimal::new(99, 2),
        2000
    ));
    assert!(!confirmation.matches(
        2_000_000_000,
        42,
        2,
        Decimal::new(82, 2),
        Decimal::new(99, 2),
        2000
    ));
    assert!(!confirmation.matches(
        4_000_000_000,
        43,
        2,
        Decimal::new(82, 2),
        Decimal::new(99, 2),
        2000
    ));
    assert!(!confirmation.matches(
        4_000_000_000,
        42,
        1,
        Decimal::new(82, 2),
        Decimal::new(99, 2),
        2000
    ));
    assert!(!confirmation.matches(
        4_000_000_000,
        42,
        2,
        Decimal::new(81, 2),
        Decimal::new(99, 2),
        2000
    ));
    assert!(!confirmation.matches(
        4_000_000_000,
        42,
        2,
        Decimal::new(82, 2),
        Decimal::new(98, 2),
        2000
    ));
}

#[path = "support/host_observer.rs"]
mod host_observer;
#[allow(dead_code)]
#[path = "../../catalog-capture-cli/src/predict_node.rs"]
mod product_host;

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit mainnet product-host read-only startup, no submission/removal capability"]
async fn mainnet_product_host_account_scope_read_only_capture() -> anyhow::Result<()> {
    use nautilus_trading::{StrategyConfig, StrategyCore};
    let settings = operator_settings()?;
    let mut profile = product_host::NodeProfile::parse(include_str!(
        "../../../examples/predict-node.toml.example"
    ))?;
    profile.predict.rpc_url = settings.network.rpc_url.expose_secret().to_owned();
    profile.predict.timeout_secs = settings.network.timeout_secs;
    profile.node.exec_engine.reconciliation_lookback_mins = Some(1);
    profile.node.exec_engine.reconciliation_startup_delay_secs = 0.0;
    profile.node.delay_post_stop = std::time::Duration::from_secs(1);
    anyhow::ensure!(
        !profile.predict.submission_enabled && !profile.predict.orderbook_removal_enabled,
        "read-only profile required"
    );
    let key = settings
        .private_config()?
        .api_key
        .ok_or_else(|| anyhow::anyhow!("API key required"))?;
    let prepared = product_host::prepare(profile, key, settings.signer()?).await?;
    let mut live = prepared.node;
    let market_id = prepared.market_id;
    let handle = live.handle();
    let journal = Rc::new(RefCell::new(vec![]));
    live.add_strategy(host_observer::HostObserver {
        core: StrategyCore::new(StrategyConfig::default()),
        market_id,
        journal: journal.clone(),
    })?;
    let driver = async {
        let received = tokio::time::timeout(std::time::Duration::from_secs(45), async {
            while !journal
                .borrow()
                .iter()
                .any(|r| r["type"] == "CustomSnapshot")
            {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        })
        .await;
        handle.stop();
        received
    };
    let (result, received) = tokio::join!(live.run(), driver);
    result?;
    received.map_err(|_| {
        anyhow::anyhow!("product host received no strategy snapshot before deadline")
    })?;
    let feedback = journal.borrow().clone();
    anyhow::ensure!(
        feedback[0]["type"] == "StrategyOnStart",
        "Native startup did not complete"
    );
    let balances = feedback[0]["accountBalances"].clone();
    let out = serde_json::json!({"source":"actual mainnet product prepare + Rust strategy + Native LiveNode; read-only","observedAtUtc":chrono::Utc::now().to_rfc3339(),"discoveredMarketId":market_id,"nativeFeedback":feedback,"balances":balances,"nativeStartupReconciliation":true,"reconciliationLookbackMins":1,"submissionSignerInstalled":false,"orderbookRemovalEnabled":false,"orderWrites":0,"removalWrites":0});
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api")
        .join(format!(
            "product_host_readonly_{}_{}.json",
            chrono::Utc::now().format("%Y%m%dT%H%M%SZ"),
            std::process::id()
        ));
    serde_json::to_writer_pretty(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?,
        &out,
    )?;
    Ok(())
}

#[path = "support/query_readonly.rs"]
mod query_readonly;

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit read-only recorder required; no submission/removal capability"]
async fn mainnet_strategy_query_commands_readonly() -> anyhow::Result<()> {
    let path = std::env::var("PREDICT_QUERY_RELAY_CONFIG")
        .map_err(|_| anyhow::anyhow!("use tests/run_predict_queries_readonly.py"))?;
    let config = serde_json::from_str(&std::fs::read_to_string(path)?)?;
    query_readonly::run(config).await
}

#[path = "support/batch_live.rs"]
mod batch_live;
#[path = "support/batch_probe.rs"]
mod batch_probe;
#[path = "support/native_input_journal.rs"]
mod native_input_journal;

#[path = "support/session_readonly.rs"]
mod session_readonly;
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit read-only JWT/WS recorder; no order capabilities"]
async fn mainnet_jwt_wallet_recovery_readonly() -> anyhow::Result<()> {
    let path = std::env::var("PREDICT_SESSION_RELAY_CONFIG")
        .map_err(|_| anyhow::anyhow!("use tests/run_predict_session_readonly.py"))?;
    let config = serde_json::from_slice(&std::fs::read(path)?)?;
    session_readonly::run(config).await
}
