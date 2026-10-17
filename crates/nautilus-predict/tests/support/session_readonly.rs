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

//! Real private-session renewal with Native LiveNode and no order capabilities.
use std::{cell::Cell, sync::Arc, time::Duration};

use nautilus_common::actor::DataActor;
use nautilus_live::node::config::LiveExecutionEngineConfig;
use nautilus_predict::{
    PredictAccountStateMode, PredictChainClientConfig, PredictCryptoUpDownSelector,
    PredictDataClientConfig, PredictWalletWebSocketConfig, discover_crypto_up_down_market,
};
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};
use serde::Deserialize;
use serde_json::{Value, json};

use super::*;

#[derive(Deserialize)]
pub struct RelayConfig {
    http_origin: String,
    rpc_origin: String,
    wallet_origin: String,
    phase_file: PathBuf,
    feedback_path: PathBuf,
    wire_state_file: PathBuf,
    wallet_state_file: PathBuf,
}
#[derive(Debug)]
struct Probe {
    core: StrategyCore,
    query: Rc<Cell<u64>>,
    sent: u64,
    journal: Rc<RefCell<Vec<Value>>>,
    last_account: Option<UUID4>,
}
impl DataActor for Probe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.journal
            .borrow_mut()
            .push(json!({"type":"StrategyStarted"}));
        self.clock().set_timer(
            "SESSION-QUERY",
            Duration::from_millis(100),
            None,
            None,
            None,
            None,
            None,
        )?;
        Ok(())
    }
    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.clock().cancel_timer("SESSION-QUERY");
        Ok(())
    }
    fn on_time_event(&mut self, _: &nautilus_common::timer::TimeEvent) -> anyhow::Result<()> {
        if self.query.get() > self.sent {
            self.sent = self.query.get();
            self.query_account(
                AccountId::new("PREDICT-001"),
                Some(ClientId::new("PREDICT")),
                None,
            )?;
            self.journal
                .borrow_mut()
                .push(json!({"type":"StrategyQueryAccount","sequence":self.sent}));
        }
        if let Some(state) = self
            .cache()
            .account(&AccountId::new("PREDICT-001"))
            .and_then(|a| a.last_event())
            && self.last_account != Some(state.event_id)
        {
            self.last_account = Some(state.event_id);
            self.journal.borrow_mut().push(json!({"type":"StrategyAccountSnapshot","eventId":state.event_id,"balances":state.balances,"info":state.info,"querySequence":self.sent}));
        }
        Ok(())
    }
}
nautilus_strategy!(Probe, {
    fn on_order_event(&mut self, event: nautilus_model::events::OrderEventAny) {
        self.journal
            .borrow_mut()
            .push(json!({"type":"UnexpectedOrderEvent","kind":format!("{:?}",event.event_type())}));
    }
});
async fn until(label: &str, mut condition: impl FnMut() -> bool) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(45), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("session barrier timed out: {label}"))
}
fn read(path: &Path) -> Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}
pub async fn run(config: RelayConfig) -> anyhow::Result<()> {
    let settings = operator_settings()?;
    let mut http_config = settings.private_config()?;
    anyhow::ensure!(
        http_config.environment == nautilus_predict::PredictEnvironment::BnbMainnet,
        "this diagnostic requires mainnet"
    );
    http_config.base_url_http = Some(config.http_origin.clone());
    let api_key = http_config
        .api_key
        .clone()
        .ok_or_else(|| anyhow::anyhow!("API key required"))?;
    let signer = settings.signer()?;
    let private = nautilus_predict::http::PredictPrivateHttpClient::new(http_config)?;
    let session = private.authenticate(&signer).await?;
    let initial_expiry = session.token().expires_at()?;
    let public = PredictHttpClient::new(
        api_key.expose_secret(),
        Some(&config.http_origin),
        settings.network.timeout_secs,
    )?;
    let selected = discover_crypto_up_down_market(
        &public,
        &PredictCryptoUpDownSelector {
            price_feed_symbol: "BTC/USDT".into(),
            title_asset: "Bitcoin".into(),
            interval_secs: 300,
        },
        get_atomic_clock_realtime().get_time_ns().as_u64(),
    )
    .await?;
    let mut execution = PredictExecutionClientConfig::new(
        AccountId::new("PREDICT-001"),
        session.clone(),
        PredictWalletWebSocketConfig {
            api_key: Some(api_key.clone()),
            base_url_ws: Some(config.wallet_origin.clone()),
            ..Default::default()
        },
    );
    execution.auth_signer = Some(Arc::clone(&signer));
    execution.account_state_mode = PredictAccountStateMode::Reported;
    execution.chain = Some(PredictChainClientConfig::new(
        nautilus_predict::PredictEnvironment::BnbMainnet,
        config.rpc_origin.clone().into(),
    ));
    execution.account_refresh_interval_secs = 300;
    execution.reconnect_reconciliation_interval_secs = None; // Exercise the strategy QueryAccount explicitly.
    anyhow::ensure!(
        execution.submission.is_none() && !execution.orderbook_removal_enabled,
        "read-only capabilities required"
    );
    let mut markets = nautilus_predict::execution::reconciliation_market_ids(&execution).await?;
    markets.push(selected.market_id);
    markets.sort_unstable();
    markets.dedup();
    let data = PredictDataClientConfig {
        api_key,
        market_ids: markets,
        base_url_http: Some(config.http_origin.clone()),
        base_url_ws: None,
        http_timeout_secs: settings.network.timeout_secs,
        ws_timeout_secs: settings.network.timeout_secs,
        dynamic_refresh_secs: 30,
        transport_backend: Default::default(),
    };
    let mut live = node::build_node_with_engine(
        TraderId::new("SESSION-001"),
        data,
        execution,
        LiveExecutionEngineConfig {
            reconciliation: true,
            reconciliation_lookback_mins: Some(1),
            reconciliation_startup_delay_secs: 0.0,
            inflight_check_interval_ms: 0,
            open_check_interval_secs: Some(5.0),
            position_check_interval_secs: Some(5.0),
            ..Default::default()
        },
    )?;
    let journal = Rc::new(RefCell::new(vec![]));
    let query = Rc::new(Cell::new(0));
    live.add_strategy(Probe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some("SESSION-PROBE-001".into()),
            ..Default::default()
        }),
        query: query.clone(),
        sent: 0,
        journal: journal.clone(),
        last_account: None,
    })?;
    let cache = live.kernel().cache.clone();
    let engine = live.kernel().exec_engine.clone();
    let connected = || {
        engine
            .borrow()
            .get_client(&ClientId::new("PREDICT"))
            .is_some_and(|c| c.is_connected())
    };
    let account = || {
        cache
            .borrow()
            .account(&AccountId::new("PREDICT-001"))
            .and_then(|a| a.last_event())
    };
    let stop = live.handle();
    let driver = async {
        let result = async {
            until("strategy and initial account",||journal.borrow().iter().any(|r|r["type"]=="StrategyStarted") && account().is_some()).await?;
            anyhow::ensure!(connected(),"initial native connection not ready");
            journal.borrow_mut().push(json!({"type":"NativeStartupComplete","connectionReady":connected(),"orders":cache.borrow().orders(None,None,None,None,None).len(),"positions":cache.borrow().positions_open(None,None,None,None,None).len(),"account":account()}));
            anyhow::ensure!(cache.borrow().orders(None,None,None,None,None).is_empty() && cache.borrow().positions_open(None,None,None,None,None).is_empty(),"this diagnostic requires an empty real order/position account; no writes will be made");
            // Leave Native periodic reports enabled; only the adapter's recovery timer is disabled.
            for cycle in 1..=2u64 {
                std::fs::write(&config.phase_file,format!("renewal-{cycle}"))?;
                let old = session.token();
                session.renew(&signer).await?;
                anyhow::ensure!(old.expose_secret()!=session.token().expose_secret(),"venue returned unchanged JWT; renewal not demonstrated");
                journal.borrow_mut().push(json!({"type":"RealAuthenticationRenewed","cycle":cycle,"previousExp":old.expires_at()?,"newExp":session.token().expires_at()?,"tokenChanged":true}));
                until("new wallet ACK",||read(&config.wallet_state_file)["acknowledgedConnections"].as_array().is_some_and(|rows| rows.len() > cycle as usize)).await?;
                // A real venue heartbeat echo is a causal barrier after Native consumed ACK.
                until("native processed real heartbeat after ACK",||read(&config.wallet_state_file)["heartbeatEchoConnections"].as_array().is_some_and(|rows|rows.contains(&json!(cycle)))).await?;
                until("native replacement remains gated",||!connected()).await?;
                let before = account().unwrap();
                journal.borrow_mut().push(json!({"type":"NewWalletAckBeforeQuery","cycle":cycle,"nativeReady":connected(),"accountEventId":before.event_id}));
                std::fs::write(&config.phase_file,format!("query-account-{cycle}"))?;
                query.set(cycle);
                until("QueryAccount and native readiness",||account().is_some_and(|state|state.event_id!=before.event_id && state.info.as_ref().is_some_and(|i|i.get("predict_reconciliation_ready")==Some(&json!(true)) && i.get("predict_connection_epoch").and_then(Value::as_u64).is_some_and(|epoch|epoch>=cycle))) && connected()).await?;
                until("strategy sees verified account",||journal.borrow().iter().any(|r|r["type"]=="StrategyAccountSnapshot" && r["info"]["predict_connection_epoch"].as_u64().is_some_and(|epoch|epoch>=cycle) && r["info"]["predict_reconciliation_ready"]==true)).await?;
                journal.borrow_mut().push(json!({"type":"NativeReadyRestored","cycle":cycle,"nativeReady":connected(),"account":account(),"wire":read(&config.wire_state_file),"orders":cache.borrow().orders(None,None,None,None,None).len(),"positions":cache.borrow().positions_open(None,None,None,None,None).len()}));
            }
            anyhow::ensure!(!journal.borrow().iter().any(|r|r["type"]=="UnexpectedOrderEvent"),"unexpected order event");
            Ok::<_,anyhow::Error>(())
        }.await;
        stop.stop();
        result
    };
    let (node_result, checks) = tokio::join!(live.run(), driver);
    live.dispose();
    let out = json!({"source":"real venue JWT/WS/REST/RPC with Rust strategy + LiveNode; no trading capabilities","renewalTrigger":"explicit early renewal through production session.renew; automatic expiration timing tested offline separately","initialExpiry":initial_expiry,"nativeReconciliationEnabled":true,"adapterRecoveryTimerEnabled":false,"submissionSignerInstalled":false,"orderbookRemovalEnabled":false,"nodeSucceeded":node_result.is_ok(),"checksSucceeded":checks.is_ok(),"failure":checks.as_ref().err().map(|e|e.to_string()),"strategyJournal":journal.borrow().clone()});
    serde_json::to_writer_pretty(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&config.feedback_path)?,
        &out,
    )?;
    node_result?;
    checks
}
