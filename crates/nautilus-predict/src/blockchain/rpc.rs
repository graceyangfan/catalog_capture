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

//! JSON-RPC protocol over Nautilus HTTP; Alloy supplies ABI codecs, not another transport.

use std::collections::HashMap;

use alloy::{
    primitives::{Address, B256, U256},
    sol,
    sol_types::SolCall,
};
use nautilus_core::time::AtomicTime;
use nautilus_network::http::{
    HttpClient, HttpRedirectPolicy, Method, create_standard_nautilus_headers,
};
use serde::Deserialize;
use serde_json::{Value, json};

use super::models::{PredictBalanceSnapshot, PredictCollateralSnapshot};
use crate::{
    common::{
        contracts::{PredictExchangeType, collateral_contract},
        environment::PredictEnvironment,
    },
    config::PredictChainClientConfig,
};

sol! {
    function decimals() external view returns (uint8);
    function balanceOf(address owner) external view returns (uint256);
    function allowance(address owner, address spender) external view returns (uint256);
    function balanceOf(address owner, uint256 id) external view returns (uint256);
    function isApprovedForAll(address owner, address operator) external view returns (bool);
    function nonces(address owner) external view returns (uint256);
}

#[derive(Debug, Deserialize)]
struct Block {
    number: String,
    hash: B256,
    #[serde(default)]
    timestamp: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PredictChainClient {
    config: PredictChainClientConfig,
    client: HttpClient,
}

impl PredictChainClient {
    /// Reads balance, allowances and the selected exchange's current nonce at one block hash.
    /// This never approves tokens or submits a transaction.
    ///
    /// # Errors
    /// Missing block timestamps, wrong chain, ABI errors and reorgs fail closed.
    pub async fn request_buy_admission(
        &self,
        account: Address,
        exchange: PredictExchangeType,
        clock: &'static AtomicTime,
    ) -> anyhow::Result<(PredictCollateralSnapshot, U256, nautilus_core::UnixNanos)> {
        tokio::time::timeout(
            std::time::Duration::from_secs(self.config.timeout_secs),
            async {
                let (snapshot, timestamp) = self.collect(account, clock, true).await?;
                let timestamp = timestamp.ok_or_else(|| {
                    anyhow::anyhow!("Predict admission block timestamp is missing")
                })?;
                let nonce = self
                    .call(
                        exchange.contract(self.environment()),
                        noncesCall { owner: account },
                        snapshot.block_hash,
                    )
                    .await?;
                let current: Block = self
                    .request(
                        "eth_getBlockByNumber",
                        json!([format!("0x{:x}", snapshot.block_number), false]),
                    )
                    .await?;
                anyhow::ensure!(
                    current.hash == snapshot.block_hash
                        && quantity(&current.number)? == snapshot.block_number,
                    "Predict admission block is no longer canonical"
                );
                Ok((snapshot, nonce, timestamp))
            },
        )
        .await
        .map_err(|_| anyhow::anyhow!("Predict admission collection timed out"))?
    }

    /// Reads SELL inventory, ERC-1155 approval and nonce at one canonical block.
    ///
    /// # Errors
    /// Wrong chain, missing block evidence, reorg or invalid ABI fails closed.
    /// The submitter also verifies the returned block timestamp against its current clock.
    pub async fn request_sell_admission(
        &self,
        account: Address,
        exchange: PredictExchangeType,
        token: U256,
        clock: &'static AtomicTime,
    ) -> anyhow::Result<(U256, bool, U256, nautilus_core::UnixNanos)> {
        tokio::time::timeout(
            std::time::Duration::from_secs(self.config.timeout_secs),
            async {
                let (snapshot, timestamp) = self.collect(account, clock, false).await?;
                let timestamp = timestamp
                    .ok_or_else(|| anyhow::anyhow!("Predict SELL block timestamp is missing"))?;
                let ctf = exchange.conditional_tokens(self.environment());
                let balance = self
                    .call(
                        ctf,
                        balanceOf_1Call {
                            owner: account,
                            id: token,
                        },
                        snapshot.block_hash,
                    )
                    .await?;
                let approved = self
                    .call(
                        ctf,
                        isApprovedForAllCall {
                            owner: account,
                            operator: exchange.contract(self.environment()),
                        },
                        snapshot.block_hash,
                    )
                    .await?;
                let nonce = self
                    .call(
                        exchange.contract(self.environment()),
                        noncesCall { owner: account },
                        snapshot.block_hash,
                    )
                    .await?;
                let current: Block = self
                    .request(
                        "eth_getBlockByNumber",
                        json!([format!("0x{:x}", snapshot.block_number), false]),
                    )
                    .await?;
                anyhow::ensure!(
                    current.hash == snapshot.block_hash
                        && quantity(&current.number)? == snapshot.block_number,
                    "Predict SELL block is no longer canonical"
                );
                Ok((balance, approved, nonce, timestamp))
            },
        )
        .await
        .map_err(|_| anyhow::anyhow!("Predict SELL admission timed out"))?
    }

    /// Builds a bounded read-only client. No signer, JWT or automatic approval transaction.
    ///
    /// # Errors
    /// Returns sanitized configuration/transport errors.
    pub fn new(config: PredictChainClientConfig) -> anyhow::Result<Self> {
        config.validate()?;
        let client = HttpClient::builder()
            .headers(create_standard_nautilus_headers().into_iter().collect())
            .redirect_policy(HttpRedirectPolicy::Reject)
            .timeout_secs(config.timeout_secs)
            .build()
            .map_err(|_| anyhow::anyhow!("Predict RPC HTTP initialization failed"))?;
        Ok(Self { config, client })
    }

    #[must_use]
    pub fn environment(&self) -> PredictEnvironment {
        self.config.environment
    }

    /// Reads actual collateral and four exchange allowances for the deposit/account address.
    /// Calls require EIP-1898 canonical block-hash support; no fallback to mixed latest states.
    /// The observation clock is sampled after all reads, not substituted for a block timestamp.
    ///
    /// # Errors
    /// Wrong chain, missing blocks, malformed ABI, changed decimals, RPC errors and timeout fail.
    pub async fn request_collateral_snapshot(
        &self,
        account: Address,
        clock: &'static AtomicTime,
    ) -> anyhow::Result<PredictCollateralSnapshot> {
        self.collect(account, clock, true)
            .await
            .map(|(snapshot, _)| snapshot)
    }

    /// Reads only collateral balance and decimals at a canonical block, without allowance queries.
    ///
    /// # Errors
    /// Rejects unverified chain/block evidence and missing or invalid block timestamps.
    pub async fn request_balance_snapshot(
        &self,
        account: Address,
        clock: &'static AtomicTime,
    ) -> anyhow::Result<PredictBalanceSnapshot> {
        let (snapshot, timestamp) = self.collect(account, clock, false).await?;
        let block_timestamp = timestamp
            .ok_or_else(|| anyhow::anyhow!("Predict balance block timestamp is missing"))?;
        Ok(PredictBalanceSnapshot {
            environment: snapshot.environment,
            account_address: snapshot.account_address,
            collateral: snapshot.collateral,
            block_number: snapshot.block_number,
            block_hash: snapshot.block_hash,
            block_timestamp,
            decimals: snapshot.decimals,
            balance_wei: snapshot.balance_wei,
            ts_observed: snapshot.ts_observed,
        })
    }

    async fn collect(
        &self,
        account: Address,
        clock: &'static AtomicTime,
        include_allowances: bool,
    ) -> anyhow::Result<(PredictCollateralSnapshot, Option<nautilus_core::UnixNanos>)> {
        anyhow::ensure!(
            !account.is_zero(),
            "Predict collateral account cannot be zero"
        );
        tokio::time::timeout(
            std::time::Duration::from_secs(self.config.timeout_secs),
            async {
                let chain: String = self.request("eth_chainId", json!([])).await?;
                anyhow::ensure!(
                    quantity(&chain)? == self.environment().chain_id(),
                    "Predict RPC chain differs from configured environment"
                );
                let block: Block = self
                    .request("eth_getBlockByNumber", json!(["latest", false]))
                    .await?;
                let block_number = quantity(&block.number)?;
                let block_timestamp = block
                    .timestamp
                    .as_deref()
                    .map(|value| {
                        quantity(value)?
                            .checked_mul(1_000_000_000)
                            .map(nautilus_core::UnixNanos::from)
                            .ok_or_else(|| {
                                anyhow::anyhow!("Predict block timestamp exceeds nanosecond range")
                            })
                    })
                    .transpose()?;
                anyhow::ensure!(
                    !block.hash.is_zero(),
                    "Predict RPC returned an unmined block"
                );
                let collateral = collateral_contract(self.environment());
                let token_decimals = self.call(collateral, decimalsCall {}, block.hash).await?;
                anyhow::ensure!(
                    token_decimals == 18,
                    "Predict collateral token decimals differ from the SDK"
                );
                let balance_wei = self
                    .call(collateral, balanceOf_0Call { owner: account }, block.hash)
                    .await?;
                let exchanges = [
                    PredictExchangeType::Standard,
                    PredictExchangeType::NegRisk,
                    PredictExchangeType::YieldBearing,
                    PredictExchangeType::YieldBearingNegRisk,
                ];
                let mut allowances = exchanges.map(|exchange| (exchange, U256::ZERO));
                for (exchange, value) in &mut allowances {
                    if !include_allowances {
                        break;
                    }
                    *value = self
                        .call(
                            collateral,
                            allowanceCall {
                                owner: account,
                                spender: exchange.contract(self.environment()),
                            },
                            block.hash,
                        )
                        .await?;
                }
                let current: Block = self
                    .request("eth_getBlockByNumber", json!([block.number, false]))
                    .await?;
                anyhow::ensure!(
                    current.hash == block.hash && quantity(&current.number)? == block_number,
                    "Predict collateral snapshot block is no longer canonical"
                );
                Ok((
                    PredictCollateralSnapshot {
                        environment: self.environment(),
                        account_address: account,
                        collateral,
                        block_number,
                        block_hash: block.hash,
                        decimals: token_decimals,
                        balance_wei,
                        allowances,
                        ts_observed: clock.get_time_ns(),
                    },
                    block_timestamp,
                ))
            },
        )
        .await
        .map_err(|_| anyhow::anyhow!("Predict collateral snapshot timed out"))?
    }

    async fn call<C: SolCall>(
        &self,
        contract: Address,
        call: C,
        block: B256,
    ) -> anyhow::Result<C::Return> {
        let data = alloy::primitives::hex::encode_prefixed(call.abi_encode());
        let result: String = self.request("eth_call", json!([
            {"to":format!("{contract:#x}"),"data":data}, {"blockHash":format!("{block:#x}"),"requireCanonical":true}
        ])).await?;
        let bytes = alloy::primitives::hex::decode(&result)
            .map_err(|_| anyhow::anyhow!("Predict RPC returned invalid ABI hex"))?;
        anyhow::ensure!(
            result.starts_with("0x") && bytes.len() == 32,
            "Predict RPC returned invalid ABI word length"
        );
        C::abi_decode_returns_validate(&bytes)
            .map_err(|_| anyhow::anyhow!("Predict RPC returned invalid ABI value"))
    }

    async fn request<T: serde::de::DeserializeOwned>(
        &self,
        method: &'static str,
        params: Value,
    ) -> anyhow::Result<T> {
        let body = json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params})
            .to_string()
            .into();
        let headers = HashMap::from([("Content-Type".to_string(), "application/json".to_string())]);
        let response = self
            .client
            .request_with_secret_body(
                Method::POST,
                self.config.rpc_url.expose_secret().to_string(),
                None,
                Some(headers),
                body,
                None,
                None,
            )
            .await
            .map_err(|_| anyhow::anyhow!("Predict RPC HTTP request failed"))?;
        anyhow::ensure!(
            response.status.as_u16() == 200,
            "Predict RPC HTTP status is not successful"
        );
        let value: Value = serde_json::from_slice(&response.body)
            .map_err(|_| anyhow::anyhow!("Predict RPC response is invalid JSON"))?;
        anyhow::ensure!(
            value.get("jsonrpc").and_then(Value::as_str) == Some("2.0")
                && value.get("id").and_then(Value::as_u64) == Some(1)
                && value.get("error").is_none()
                && value.get("result").is_some(),
            "Predict RPC response has an error or mismatched envelope"
        );
        serde_json::from_value(value["result"].clone())
            .map_err(|_| anyhow::anyhow!("Predict RPC result has an invalid shape"))
    }
}

fn quantity(value: &str) -> anyhow::Result<u64> {
    let digits = value
        .strip_prefix("0x")
        .ok_or_else(|| anyhow::anyhow!("Predict RPC quantity lacks a hex prefix"))?;
    anyhow::ensure!(
        !digits.is_empty()
            && (digits.len() == 1 || !digits.starts_with('0'))
            && digits.bytes().all(|b| b.is_ascii_hexdigit()),
        "Predict RPC quantity is not canonical hex"
    );
    u64::from_str_radix(digits, 16).map_err(|_| anyhow::anyhow!("Predict RPC quantity exceeds u64"))
}
