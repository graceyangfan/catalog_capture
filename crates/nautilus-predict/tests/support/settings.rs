// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Explicit operator TOML settings. Private values are never included in parser diagnostics.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use alloy::primitives::Address;
use nautilus_core::string::secret::SecretString;
use nautilus_predict::{
    PredictChainClientConfig, PredictPrivateClientConfig, PredictWalletWebSocketConfig,
    common::environment::PredictEnvironment,
    http::{PredictAuthenticatedHttpClient, PredictPrivateHttpClient},
    signing::PredictAuthSigner,
};
use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PredictSettingsEnvironment {
    Mainnet,
    Testnet,
}

impl From<PredictSettingsEnvironment> for PredictEnvironment {
    fn from(value: PredictSettingsEnvironment) -> Self {
        match value {
            PredictSettingsEnvironment::Mainnet => Self::BnbMainnet,
            PredictSettingsEnvironment::Testnet => Self::BnbTestnet,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PredictNetworkSettings {
    pub environment: PredictSettingsEnvironment,
    pub rpc_url: SecretString,
    pub api_key_file: PathBuf,
    pub timeout_secs: u64,
    /// Testnet has no inferred wallet WebSocket endpoint.
    pub websocket_url: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredictWalletMode {
    Eoa,
    #[default]
    PredictAccount,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PredictPrivySettings {
    #[serde(default)]
    pub mode: PredictWalletMode,
    pub private_key: SecretString,
    pub predict_account: String,
    pub owner_address: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PredictTestSettings {
    pub allow_order_writes: bool,
    #[serde(deserialize_with = "decimal_string")]
    pub max_order_usdt: Decimal,
    #[serde(deserialize_with = "decimal_string")]
    pub max_total_usdt: Decimal,
    pub require_post_only: bool,
    #[serde(deserialize_with = "decimal_string")]
    pub max_fill_price: Decimal,
    pub max_discovery_pages: usize,
    pub wait_secs: u64,
}

fn decimal_string<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Decimal, D::Error> {
    let value = String::deserialize(deserializer)?;
    Decimal::from_str_exact(&value).map_err(|_| serde::de::Error::custom("invalid exact decimal"))
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PredictExecutionSettings {
    pub network: PredictNetworkSettings,
    pub privy: PredictPrivySettings,
    pub test: PredictTestSettings,
}

fn read_private_file(path: &Path) -> anyhow::Result<SecretString> {
    let metadata =
        std::fs::metadata(path).map_err(|_| anyhow::anyhow!("Predict TOML file is unavailable"))?;
    anyhow::ensure!(metadata.is_file(), "Predict TOML must be a regular file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        anyhow::ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "Predict TOML must not be group/world readable; run chmod 600"
        );
    }
    std::fs::read_to_string(path)
        .map(SecretString::from)
        .map_err(|_| anyhow::anyhow!("Predict TOML file could not be read"))
}

impl PredictExecutionSettings {
    /// Loads only the named TOML; no environment variable or dotenv override is consulted.
    /// Relative key-file paths are resolved from the operator TOML's directory.
    ///
    /// # Errors
    /// Rejects inaccessible/readable-by-others files, schema errors and invalid financial bounds.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let content = read_private_file(path)?;
        let mut settings = Self::parse(content.expose_secret())?;
        if settings.network.api_key_file.is_relative() {
            settings.network.api_key_file = path
                .parent()
                .unwrap_or(Path::new("."))
                .join(&settings.network.api_key_file);
        }
        Ok(settings)
    }

    /// Parses settings without echoing malformed source, including private keys and RPC URLs.
    ///
    /// # Errors
    /// Returns sanitized schema or validation errors.
    pub fn parse(content: &str) -> anyhow::Result<Self> {
        let settings: Self = toml::from_str(content)
            .map_err(|_| anyhow::anyhow!("invalid Predict execution TOML schema"))?;
        anyhow::ensure!(
            (1..=300).contains(&settings.network.timeout_secs),
            "invalid Predict timeout"
        );
        let test = &settings.test;
        anyhow::ensure!(
            test.max_order_usdt > Decimal::ZERO
                && test.max_total_usdt >= test.max_order_usdt
                && test.max_fill_price > Decimal::ZERO
                && test.max_fill_price <= Decimal::new(1, 2)
                && (1..=10).contains(&test.max_discovery_pages)
                && (1..=60).contains(&test.wait_secs),
            "invalid Predict test bounds"
        );
        nautilus_predict::blockchain::PredictChainClient::new(settings.chain_config())?;
        Ok(settings)
    }

    /// Builds an explicit EOA signer or a Kernel signer bound to the deposit identity.
    ///
    /// # Errors
    /// Empty/invalid keys, deposit addresses and owner mismatch guards fail before any request.
    pub fn signer(&self) -> anyhow::Result<Arc<PredictAuthSigner>> {
        anyhow::ensure!(
            !self.privy.private_key.expose_secret().is_empty(),
            "fill privy.private_key in the local TOML"
        );
        let account = match self.privy.mode {
            PredictWalletMode::Eoa => {
                anyhow::ensure!(
                    self.privy.predict_account.is_empty(),
                    "EOA mode requires an empty predict_account; use owner_address as the wallet guard"
                );
                None
            }
            PredictWalletMode::PredictAccount => {
                anyhow::ensure!(
                    !self.privy.predict_account.is_empty(),
                    "fill privy.predict_account with the deposit address"
                );
                Some(self.privy.predict_account.as_str())
            }
        };
        let signer = PredictAuthSigner::new(
            self.privy.private_key.clone(),
            self.network.environment.into(),
            account,
        )?;
        if !self.privy.owner_address.is_empty() {
            let expected: Address = self
                .privy
                .owner_address
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid Privy owner-address guard"))?;
            anyhow::ensure!(
                signer.owner_address() == expected,
                "Privy key and owner-address guard differ"
            );
        }
        Ok(Arc::new(signer))
    }

    /// Reads the same API-key TOML used by the public recorder.
    ///
    /// # Errors
    /// Rejects unsafe file permissions, malformed content and invalid private transport settings.
    pub fn private_config(&self) -> anyhow::Result<PredictPrivateClientConfig> {
        #[derive(Deserialize)]
        struct Key {
            api_key: SecretString,
        }
        let content = read_private_file(&self.network.api_key_file)?;
        let key: Key = toml::from_str(content.expose_secret())
            .map_err(|_| anyhow::anyhow!("invalid Predict API-key TOML"))?;
        let config = PredictPrivateClientConfig {
            environment: self.network.environment.into(),
            api_key: Some(key.api_key),
            base_url_http: None,
            http_timeout_secs: self.network.timeout_secs,
        };
        PredictPrivateHttpClient::new(config.clone())?;
        Ok(config)
    }

    #[must_use]
    pub fn chain_config(&self) -> PredictChainClientConfig {
        PredictChainClientConfig {
            environment: self.network.environment.into(),
            rpc_url: self.network.rpc_url.clone(),
            timeout_secs: self.network.timeout_secs,
        }
    }

    /// Authenticates without creating orders or performing blockchain transactions.
    ///
    /// # Errors
    /// Local identity errors and sanitized HTTP failures propagate; the JWT stays in memory.
    pub async fn authenticate(
        &self,
    ) -> anyhow::Result<(Arc<PredictAuthSigner>, PredictAuthenticatedHttpClient)> {
        let signer = self.signer()?;
        let transport = PredictPrivateHttpClient::new(self.private_config()?)?;
        let session = transport.authenticate(&signer).await?;
        session.request_account().await?;
        Ok((signer, session))
    }

    /// Builds wallet stream settings with the separately supplied handshake key.
    ///
    /// # Errors
    /// Rejects unavailable credentials and unverified WebSocket endpoints.
    pub fn websocket_config(&self) -> anyhow::Result<PredictWalletWebSocketConfig> {
        let config = PredictWalletWebSocketConfig {
            environment: self.network.environment.into(),
            api_key: self.private_config()?.api_key,
            base_url_ws: self.network.websocket_url.clone(),
            connect_timeout_secs: self.network.timeout_secs,
            subscription_timeout_secs: self.network.timeout_secs,
            ..Default::default()
        };

        Ok(config)
    }
}
