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

//! Application assembly only. Native LiveNode owns lifecycle, risk, OMS and reconciliation.
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    nautilus_actor,
};
use nautilus_core::{string::secret::SecretString, time::get_atomic_clock_realtime};
use nautilus_live::node::{LiveNode, LiveNodeBuilder, config::LiveNodeConfig};
use nautilus_model::{
    data::QuoteTick,
    identifiers::{AccountId, ActorId, ClientId, InstrumentId},
};
use nautilus_predict::{
    PredictAccountStateMode, PredictChainClientConfig, PredictCryptoUpDownSelector,
    PredictDataClientConfig, PredictDataClientFactory, PredictEnvironment,
    PredictExecutionClientConfig, PredictExecutionClientFactory, PredictHttpClient,
    PredictPrivateClientConfig, PredictSubmissionConfig, PredictWalletWebSocketConfig,
    discover_crypto_up_down_market, http::PredictPrivateHttpClient, instrument_id_from_outcome,
    signing::PredictAuthSigner,
};
use rust_decimal::Decimal;
use serde::Deserialize;

/// Native node fields are deserialized directly, without another engine/risk/cache schema.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeProfile {
    pub node: LiveNodeConfig,
    pub predict: PredictSettings,
    pub discovery: Discovery,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PredictSettings {
    pub account_id: AccountId,
    pub privy_credentials_file: PathBuf,
    pub rpc_url: String,
    #[serde(default)]
    pub environment: Environment,
    pub http_url: Option<String>,
    pub public_ws_url: Option<String>,
    pub wallet_ws_url: Option<String>,
    #[serde(default = "timeout")]
    pub timeout_secs: u64,
    #[serde(default)]
    pub orderbook_removal_enabled: bool,
    pub submission: Option<SubmissionLimits>,
}
fn timeout() -> u64 {
    15
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Environment {
    #[default]
    Mainnet,
    Testnet,
}
impl PredictSettings {
    pub fn environment(&self) -> PredictEnvironment {
        match self.environment {
            Environment::Mainnet => PredictEnvironment::BnbMainnet,
            Environment::Testnet => PredictEnvironment::BnbTestnet,
        }
    }
}
/// Only the non-serializable adapter signer is injected; Native venue admission stays in the adapter.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmissionLimits {
    pub max_order_usdt: Decimal,
    pub max_total_usdt: Decimal,
    pub require_post_only: bool,
    pub market_slippage_bps: u32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Discovery {
    pub price_feed_symbol: String,
    pub title_asset: String,
    pub interval_secs: u64,
}
impl NodeProfile {
    pub fn parse(source: &str) -> Result<Self> {
        // TOML diagnostics may contain source lines, including RPC provider tokens.
        let profile: Self = toml::from_str(source)
            .map_err(|_| anyhow::anyhow!("invalid Predict node TOML schema"))?;
        anyhow::ensure!(
            !profile.node.risk_engine.bypass,
            "Predict node requires native risk checks"
        );
        anyhow::ensure!(
            profile.node.exec_engine.reconciliation,
            "Predict node requires native reconciliation"
        );
        anyhow::ensure!(
            profile
                .node
                .exec_engine
                .position_check_interval_secs
                .is_some(),
            "Predict node requires native position checks for net holdings"
        );
        anyhow::ensure!(
            profile.discovery.interval_secs > 0
                && !profile.discovery.price_feed_symbol.trim().is_empty()
                && !profile.discovery.title_asset.trim().is_empty(),
            "Predict discovery selector is empty or invalid"
        );
        anyhow::ensure!(
            profile
                .node
                .exec_engine
                .reconciliation_instrument_ids
                .is_none(),
            "Predict node instrument scope comes from discovery/cache, not TOML IDs"
        );
        if matches!(profile.predict.environment, Environment::Testnet) {
            anyhow::ensure!(
                profile.predict.public_ws_url.is_some() && profile.predict.wallet_ws_url.is_some(),
                "Predict testnet requires explicit public and wallet WebSocket URLs"
            );
        }
        Ok(profile)
    }
    pub fn load(path: &Path) -> Result<Self> {
        let mut profile =
            Self::parse(&std::fs::read_to_string(path).context("cannot read Predict node TOML")?)?;
        if profile.predict.privy_credentials_file.is_relative() {
            profile.predict.privy_credentials_file = path
                .parent()
                .unwrap_or(Path::new("."))
                .join(&profile.predict.privy_credentials_file);
        }
        Ok(profile)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PrivyFile {
    privy: Privy,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Privy {
    private_key: String,
    predict_account: String,
    #[serde(default)]
    owner_address: String,
}
/// Same exported Privy owner key + separate Predict Account semantics as the adapter signer.
pub fn load_signer(path: &Path, environment: PredictEnvironment) -> Result<Arc<PredictAuthSigner>> {
    let metadata = std::fs::metadata(path).context("cannot stat Predict Privy credential file")?;
    anyhow::ensure!(
        metadata.is_file(),
        "Predict Privy credentials must be a regular file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        anyhow::ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "Predict Privy credential file requires chmod 600"
        );
    }
    let file: PrivyFile = toml::from_str(
        &std::fs::read_to_string(path).context("cannot read Predict Privy credential file")?,
    )
    .map_err(|_| anyhow::anyhow!("invalid Predict Privy credential TOML"))?;
    anyhow::ensure!(
        !file.privy.predict_account.trim().is_empty(),
        "Predict Account/deposit address is required"
    );
    let signer = PredictAuthSigner::new(
        file.privy.private_key.into(),
        environment,
        Some(&file.privy.predict_account),
    )?;
    anyhow::ensure!(
        file.privy.owner_address.trim().is_empty()
            || signer
                .owner_address()
                .to_string()
                .eq_ignore_ascii_case(file.privy.owner_address.trim()),
        "Privy owner check differs from exported private key"
    );
    Ok(Arc::new(signer))
}

pub struct PreparedNode {
    pub node: LiveNode,
    pub market_id: u64,
    pub instruments: [InstrumentId; 2],
}
/// Returns the ordinary Native node so a Rust caller can add its own native strategy.
pub async fn prepare(
    profile: NodeProfile,
    api_key: SecretString,
    signer: Arc<PredictAuthSigner>,
) -> Result<PreparedNode> {
    let environment = profile.predict.environment();
    let builder = LiveNodeBuilder::from_config(profile.node)?;
    let http_origin = profile
        .predict
        .http_url
        .as_deref()
        .unwrap_or(environment.http_url())
        .to_string();
    let private = PredictPrivateHttpClient::new(PredictPrivateClientConfig {
        environment,
        api_key: Some(api_key.clone()),
        base_url_http: profile.predict.http_url.clone(),
        http_timeout_secs: profile.predict.timeout_secs,
    })?;
    let http = PredictHttpClient::new(
        api_key.expose_secret(),
        Some(&http_origin),
        profile.predict.timeout_secs,
    )?;
    let now = get_atomic_clock_realtime().get_time_ns();
    let selected = discover_crypto_up_down_market(
        &http,
        &PredictCryptoUpDownSelector {
            price_feed_symbol: profile.discovery.price_feed_symbol,
            title_asset: profile.discovery.title_asset,
            interval_secs: profile.discovery.interval_secs,
        },
        now.as_u64(),
    )
    .await?;
    let instruments = [
        instrument_id_from_outcome(selected.market_id, 1)?,
        instrument_id_from_outcome(selected.market_id, 2)?,
    ];
    let session = private.authenticate(&signer).await?;
    let wallet = PredictWalletWebSocketConfig {
        environment,
        api_key: Some(api_key.clone()),
        base_url_ws: profile.predict.wallet_ws_url,
        ..Default::default()
    };
    let mut execution =
        PredictExecutionClientConfig::new(profile.predict.account_id, session, wallet);
    execution.auth_signer = Some(signer.clone());
    execution.account_state_mode = PredictAccountStateMode::Reported;
    execution.chain = Some(PredictChainClientConfig::new(
        environment,
        profile.predict.rpc_url.into(),
    ));
    let mut market_ids = nautilus_predict::execution::reconciliation_market_ids(&execution).await?;
    market_ids.push(selected.market_id);
    market_ids.sort_unstable();
    market_ids.dedup();
    execution.orderbook_removal_enabled = profile.predict.orderbook_removal_enabled;
    execution.submission = profile
        .predict
        .submission
        .map(|limits| PredictSubmissionConfig {
            signer,
            max_order_usdt: limits.max_order_usdt,
            max_total_usdt: limits.max_total_usdt,
            require_post_only: limits.require_post_only,
            market_slippage_bps: limits.market_slippage_bps,
        });
    let mut node = builder
        .with_name("PredictLiveNode")
        .add_data_client(
            Some("PREDICT".into()),
            Box::new(PredictDataClientFactory::new()),
            Box::new(PredictDataClientConfig {
                api_key,
                market_ids,
                base_url_http: Some(http_origin),
                base_url_ws: profile.predict.public_ws_url,
                http_timeout_secs: profile.predict.timeout_secs,
                ws_timeout_secs: profile.predict.timeout_secs,
                dynamic_refresh_secs: 30,
                transport_backend: Default::default(),
            }),
        )?
        .add_exec_client(
            Some("PREDICT".into()),
            Box::new(PredictExecutionClientFactory::new()),
            Box::new(execution),
        )?
        .build()?;
    node.add_actor(Quotes {
        core: DataActorCore::new(DataActorConfig {
            actor_id: Some(ActorId::new("PREDICT-QUOTES")),
            ..Default::default()
        }),
        instruments,
    })?;
    Ok(PreparedNode {
        node,
        market_id: selected.market_id,
        instruments,
    })
}
/// CLI mode observes Native quotes/account state; business strategies are supplied by Rust callers.
pub async fn run(
    profile: NodeProfile,
    api_key: SecretString,
    signer: Arc<PredictAuthSigner>,
) -> Result<()> {
    let mut prepared = prepare(profile, api_key, signer).await?;
    log::info!(
        "Predict node observing discovered market {} ({}, {})",
        prepared.market_id,
        prepared.instruments[0],
        prepared.instruments[1]
    );
    let result = prepared.node.run().await;
    prepared.node.dispose();
    result
}
#[derive(Debug)]
struct Quotes {
    core: DataActorCore,
    instruments: [InstrumentId; 2],
}
nautilus_actor!(Quotes);
impl DataActor for Quotes {
    fn on_start(&mut self) -> Result<()> {
        for instrument in self.instruments {
            self.subscribe_quotes(instrument, Some(ClientId::new("PREDICT")), None);
        }
        Ok(())
    }
    fn on_quote(&mut self, quote: &QuoteTick) -> Result<()> {
        log::debug!(
            "{} bid={} ask={}",
            quote.instrument_id,
            quote.bid_price,
            quote.ask_price
        );
        Ok(())
    }
    fn on_stop(&mut self) -> Result<()> {
        for instrument in self.instruments {
            self.unsubscribe_quotes(instrument, Some(ClientId::new("PREDICT")), None);
        }
        Ok(())
    }
}
