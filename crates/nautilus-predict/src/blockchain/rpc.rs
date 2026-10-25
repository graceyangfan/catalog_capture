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

use std::{collections::HashMap, sync::Arc};

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

#[derive(Debug, thiserror::Error)]
#[error("Predict RPC block is no longer canonical")]
pub(crate) struct PredictBlockChanged;

#[derive(Clone, Debug)]
pub struct PredictChainClient {
    config: PredictChainClientConfig,
    client: HttpClient,
    verified: Arc<tokio::sync::OnceCell<()>>,
    pub(crate) latency: Option<crate::latency::PredictLatency>,
}

impl PredictChainClient {
    /// Reads balance, selected exchange allowance and nonce at one block hash.
    /// Other allowance entries remain zero (unqueried); the full diagnostic queries all four.
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
                anyhow::ensure!(
                    !account.is_zero(),
                    "Predict collateral account cannot be zero"
                );
                let block = self.read_block().await?;
                let block_number = quantity(&block.number)?;
                let timestamp = block_timestamp(&block)?.ok_or_else(|| {
                    anyhow::anyhow!("Predict admission block timestamp is missing")
                })?;
                let collateral = collateral_contract(self.environment());
                // Like SELL admission, all independent ABI reads share one canonical block.
                let (balance_wei, allowance, nonce) = tokio::try_join!(
                    self.call(collateral, balanceOf_0Call { owner: account }, block.hash),
                    self.call(
                        collateral,
                        allowanceCall {
                            owner: account,
                            spender: exchange.contract(self.environment()),
                        },
                        block.hash
                    ),
                    self.call(
                        exchange.contract(self.environment()),
                        noncesCall { owner: account },
                        block.hash,
                    ),
                )?;
                let current: Block = self
                    .request(
                        "eth_getBlockByNumber",
                        json!([format!("0x{block_number:x}"), false]),
                    )
                    .await?;
                anyhow::ensure!(
                    current.hash == block.hash && quantity(&current.number)? == block_number,
                    PredictBlockChanged
                );
                let allowances = ALL_EXCHANGES.map(|kind| {
                    (
                        kind,
                        if kind == exchange {
                            allowance
                        } else {
                            U256::ZERO
                        },
                    )
                });
                let snapshot = PredictCollateralSnapshot {
                    environment: self.environment(),
                    account_address: account,
                    collateral,
                    block_number,
                    block_hash: block.hash,
                    decimals: 18,
                    balance_wei,
                    allowances,
                    ts_observed: clock.get_time_ns(),
                };
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
        _clock: &'static AtomicTime,
    ) -> anyhow::Result<(U256, bool, U256, nautilus_core::UnixNanos)> {
        tokio::time::timeout(
            std::time::Duration::from_secs(self.config.timeout_secs),
            async {
                anyhow::ensure!(!account.is_zero(), "Predict SELL account cannot be zero");
                let block = self.read_block().await?;
                let timestamp = block_timestamp(&block)?
                    .ok_or_else(|| anyhow::anyhow!("Predict SELL block timestamp is missing"))?;
                let ctf = exchange.conditional_tokens(self.environment());
                let (balance, approved, nonce) = tokio::try_join!(
                    self.call(
                        ctf,
                        balanceOf_1Call {
                            owner: account,
                            id: token
                        },
                        block.hash
                    ),
                    self.call(
                        ctf,
                        isApprovedForAllCall {
                            owner: account,
                            operator: exchange.contract(self.environment())
                        },
                        block.hash
                    ),
                    self.call(
                        exchange.contract(self.environment()),
                        noncesCall { owner: account },
                        block.hash
                    ),
                )?;
                let current: Block = self
                    .request(
                        "eth_getBlockByNumber",
                        json!([format!("0x{:x}", quantity(&block.number)?), false]),
                    )
                    .await?;
                anyhow::ensure!(
                    current.hash == block.hash
                        && quantity(&current.number)? == quantity(&block.number)?,
                    PredictBlockChanged
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
        Ok(Self {
            config,
            client,
            verified: Default::default(),
            latency: None,
        })
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
        self.collect(account, clock, &ALL_EXCHANGES, true)
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
        let (snapshot, timestamp) = self.collect(account, clock, &[], true).await?;
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

    // Signing inputs have a contract/account lifetime, independent of OPEN and cash reports.
    // Read only missing entries; a failed contract read must not discard other entries.
    pub(crate) async fn request_signing_nonces(
        &self,
        account: Address,
        missing: [bool; 4],
    ) -> anyhow::Result<[Option<U256>; 4]> {
        anyhow::ensure!(!account.is_zero(), "Predict nonce account cannot be zero");
        tokio::time::timeout(
            std::time::Duration::from_secs(self.config.timeout_secs),
            async {
                let block = self.read_block().await?;
                let values =
                    futures_util::future::join_all(ALL_EXCHANGES.map(|exchange| async move {
                        if !missing[exchange.cache_index()] {
                            return None;
                        }
                        match self
                            .call(
                                exchange.contract(self.environment()),
                                noncesCall { owner: account },
                                block.hash,
                            )
                            .await
                        {
                            Ok(value) => Some(value),
                            Err(error) => {
                                log::warn!(
                                    "Predict signing nonce read failed for {exchange:?}: {error}"
                                );
                                None
                            }
                        }
                    }))
                    .await;
                let current: Block = self
                    .request("eth_getBlockByNumber", json!([block.number, false]))
                    .await?;
                anyhow::ensure!(
                    current.hash == block.hash && current.number == block.number,
                    PredictBlockChanged
                );
                Ok([values[0], values[1], values[2], values[3]])
            },
        )
        .await
        .map_err(|_| anyhow::anyhow!("Predict signing nonce collection timed out"))?
    }

    async fn collect(
        &self,
        account: Address,
        clock: &'static AtomicTime,
        exchanges: &[PredictExchangeType],
        verify_final: bool,
    ) -> anyhow::Result<(PredictCollateralSnapshot, Option<nautilus_core::UnixNanos>)> {
        anyhow::ensure!(
            !account.is_zero(),
            "Predict collateral account cannot be zero"
        );
        tokio::time::timeout(
            std::time::Duration::from_secs(self.config.timeout_secs),
            async {
                let block = self.read_block().await?;
                let block_number = quantity(&block.number)?;
                let block_timestamp = block_timestamp(&block)?;
                let collateral = collateral_contract(self.environment());
                let balance_wei = self
                    .call(collateral, balanceOf_0Call { owner: account }, block.hash)
                    .await?;
                let mut allowances = ALL_EXCHANGES.map(|exchange| (exchange, U256::ZERO));
                for (exchange, value) in &mut allowances {
                    if !exchanges.contains(exchange) {
                        continue;
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
                if verify_final {
                    let current: Block = self
                        .request("eth_getBlockByNumber", json!([block.number, false]))
                        .await?;
                    anyhow::ensure!(
                        current.hash == block.hash && quantity(&current.number)? == block_number,
                        PredictBlockChanged
                    );
                }
                Ok((
                    PredictCollateralSnapshot {
                        environment: self.environment(),
                        account_address: account,
                        collateral,
                        block_number,
                        block_hash: block.hash,
                        decimals: 18,
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

    async fn read_block(&self) -> anyhow::Result<Block> {
        let block: Block = self
            .request("eth_getBlockByNumber", json!(["latest", false]))
            .await?;
        anyhow::ensure!(
            !block.hash.is_zero(),
            "Predict RPC returned an unmined block"
        );
        self.verified
            .get_or_try_init(|| async {
                let chain: String = self.request("eth_chainId", json!([])).await?;
                anyhow::ensure!(
                    quantity(&chain)? == self.environment().chain_id(),
                    "Predict RPC chain differs from configured environment"
                );
                let decimals = self
                    .call(
                        collateral_contract(self.environment()),
                        decimalsCall {},
                        block.hash,
                    )
                    .await?;
                anyhow::ensure!(
                    decimals == 18,
                    "Predict collateral token decimals differ from the SDK"
                );
                Ok::<(), anyhow::Error>(())
            })
            .await?;
        Ok(block)
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
        let timing = self.latency.as_ref().map(|probe| {
            probe.count(crate::latency::LatencyCounter::RpcRequests);
            probe.timer(crate::latency::LatencyMetric::RpcHttp)
        });
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
            .await;
        if let Some(timing) = timing {
            timing.finish();
        }
        let response = response.map_err(|_| anyhow::anyhow!("Predict RPC HTTP request failed"))?;
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

const ALL_EXCHANGES: [PredictExchangeType; 4] = [
    PredictExchangeType::Standard,
    PredictExchangeType::NegRisk,
    PredictExchangeType::YieldBearing,
    PredictExchangeType::YieldBearingNegRisk,
];

fn block_timestamp(block: &Block) -> anyhow::Result<Option<nautilus_core::UnixNanos>> {
    block
        .timestamp
        .as_deref()
        .map(|value| {
            quantity(value)?
                .checked_mul(1_000_000_000)
                .map(nautilus_core::UnixNanos::from)
                .ok_or_else(|| anyhow::anyhow!("Predict block timestamp exceeds nanosecond range"))
        })
        .transpose()
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
