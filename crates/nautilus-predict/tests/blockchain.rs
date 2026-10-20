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

#![cfg(feature = "execution")]

use std::sync::{Arc, Mutex};

use alloy::primitives::{Address, U256};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method},
    response::Response,
};
use nautilus_core::{UnixNanos, time::AtomicTime};
use nautilus_model::{
    enums::CurrencyType,
    types::{Currency, Money},
};
use nautilus_predict::{
    PredictChainClientConfig, PredictEnvironment,
    blockchain::{PredictChainClient, parse::parse_collateral_balance},
    common::contracts::{PredictExchangeType, collateral_contract},
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle};

const ACCOUNT: &str = "0x1111111111111111111111111111111111111111";
const BLOCK: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SENTINEL: &str = "rpc-provider-secret-fixture";

#[derive(Clone)]
struct RpcState {
    environment: PredictEnvironment,
    fault: &'static str,
    requests: Arc<Mutex<Vec<Value>>>,
}
struct Server {
    url: String,
    state: RpcState,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn word(value: U256) -> Value {
    json!(format!("0x{value:064x}"))
}

async fn rpc(
    State(state): State<RpcState>,
    headers: HeaderMap,
    method: Method,
    body: Bytes,
) -> Response {
    assert_eq!(method, Method::POST);
    let request: Value = serde_json::from_slice(&body).unwrap();
    assert!(!headers.contains_key("authorization"));
    assert!(!headers.contains_key("x-api-key"));
    state.requests.lock().unwrap().push(request.clone());
    let method = request["method"].as_str().unwrap();
    let calls = state.requests.lock().unwrap().len();
    let result = match method {
        "eth_chainId" => json!(if state.fault == "chain" {
            "0x1".to_string()
        } else {
            format!("0x{:x}", state.environment.chain_id())
        }),
        "eth_getBlockByNumber" => {
            assert_eq!(request["params"][1], false);
            if state.fault == "block" {
                Value::Null
            } else {
                let mut block = json!({"number":"0x123", "hash":if (state.fault == "reorg" && calls > 2) || (state.fault == "nonce_reorg" && request["params"][0] != "latest") {
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                } else { BLOCK }, "timestamp":if state.fault=="timestamp_overflow" {"0xffffffffffffffff"} else {"0x2"}});
                if state.fault == "timestamp_missing" {
                    block.as_object_mut().unwrap().remove("timestamp");
                }
                block
            }
        }
        "eth_call" => {
            let data = request["params"][0]["data"].as_str().unwrap();
            if data.starts_with("0x7ecebe00") {
                assert_eq!(
                    request["params"][0]["to"],
                    format!(
                        "{:#x}",
                        PredictExchangeType::NegRisk.contract(state.environment)
                    )
                );
            } else if data.starts_with("0x00fdd58e") || data.starts_with("0xe985e9c5") {
                assert_eq!(
                    request["params"][0]["to"],
                    format!(
                        "{:#x}",
                        PredictExchangeType::NegRisk.conditional_tokens(state.environment)
                    )
                );
            } else {
                assert_eq!(
                    request["params"][0]["to"],
                    format!("{:#x}", collateral_contract(state.environment))
                );
            }
            assert_eq!(
                request["params"][1],
                json!({"blockHash":BLOCK,"requireCanonical":true})
            );
            assert!(!data.starts_with("0x095ea7b3"), "no approve transaction");
            if state.fault == "abi" {
                json!("0x01")
            } else if data == "0x313ce567" {
                word(U256::from(if state.fault == "decimals" { 6 } else { 18 }))
            } else if data.starts_with("0x00fdd58e") {
                word(U256::from(17_000_000_000_000_000_000u128))
            } else if data.starts_with("0xe985e9c5") {
                word(U256::from(1))
            } else if data.starts_with("0x70a08231") {
                assert!(data.ends_with(&ACCOUNT[2..]));
                word(U256::from(123_450_000_000_000_000_000u128))
            } else if data.starts_with("0x7ecebe00") {
                assert!(data.ends_with(&ACCOUNT[2..]));
                word(U256::MAX - U256::from(1))
            } else {
                assert!(data.starts_with("0xdd62ed3e"));
                assert_eq!(&data[34..74], &ACCOUNT[2..]);
                word(U256::MAX)
            }
        }
        _ => panic!("unexpected RPC method"),
    };
    if state.fault == "error" {
        return response(
            json!({"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":SENTINEL}}),
        );
    }
    response(json!({"jsonrpc":"2.0","id":if state.fault == "id" { 99 } else { 1 },"result":result}))
}

fn response(value: Value) -> Response {
    Response::builder()
        .header("content-type", "application/json")
        .body(Body::from(value.to_string()))
        .unwrap()
}

async fn server(environment: PredictEnvironment, fault: &'static str) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = RpcState {
        environment,
        fault,
        requests: Arc::new(Mutex::new(vec![])),
    };
    let router = Router::new().fallback(rpc).with_state(state.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Server {
        url: format!("http://{addr}/{SENTINEL}?key={SENTINEL}"),
        state,
        task,
    }
}

fn clock() -> &'static AtomicTime {
    Box::leak(Box::new(AtomicTime::new(
        false,
        UnixNanos::from(123_456u64),
    )))
}

#[tokio::test]
async fn collateral_reads_use_sdk_contracts_account_identity_and_one_canonical_block() {
    for environment in [
        PredictEnvironment::BnbMainnet,
        PredictEnvironment::BnbTestnet,
    ] {
        let server = server(environment, "").await;
        let config = PredictChainClientConfig::new(environment, server.url.clone().into());
        assert!(!format!("{config:?}").contains(SENTINEL));
        let client = PredictChainClient::new(config).unwrap();
        assert!(!format!("{client:?}").contains(SENTINEL));
        let snapshot = client
            .request_collateral_snapshot(ACCOUNT.parse().unwrap(), clock())
            .await
            .unwrap();
        assert_eq!(
            snapshot.account_address,
            ACCOUNT.parse::<Address>().unwrap()
        );
        assert_eq!(snapshot.block_number, 0x123);
        assert_eq!(snapshot.ts_observed, UnixNanos::from(123_456u64));
        assert_eq!(
            snapshot.balance_wei,
            U256::from(123_450_000_000_000_000_000u128)
        );
        let requests = server.state.requests.lock().unwrap();
        assert_eq!(requests.len(), 9);
        for (i, (exchange, amount)) in snapshot.allowances.iter().enumerate() {
            assert_eq!(*amount, U256::MAX);
            assert!(
                requests[4 + i]["params"][0]["data"]
                    .as_str()
                    .unwrap()
                    .ends_with(&format!("{:x}", exchange.contract(environment)))
            );
        }
        assert_eq!(snapshot.allowances[0].0, PredictExchangeType::Standard);
        let balance = parse_collateral_balance(&snapshot, Money::from("23.45 USDT")).unwrap();
        assert_eq!(balance.total, Money::from("123.45 USDT"));
        assert_eq!(balance.locked, Money::from("23.45 USDT"));
        assert_eq!(balance.free, Money::from("100 USDT"));
    }
}

#[tokio::test]
async fn unverifiable_rpc_snapshots_fail_without_leaking_provider_or_rpc_payload() {
    for fault in ["chain", "block", "reorg", "abi", "decimals", "id", "error"] {
        let server = server(PredictEnvironment::BnbMainnet, fault).await;
        let client = PredictChainClient::new(PredictChainClientConfig::new(
            PredictEnvironment::BnbMainnet,
            server.url.clone().into(),
        ))
        .unwrap();
        let error = client
            .request_collateral_snapshot(ACCOUNT.parse().unwrap(), clock())
            .await
            .unwrap_err();
        assert!(!format!("{error:?}").contains(SENTINEL), "{fault}");
    }
}

#[tokio::test]
async fn money_conversion_rejects_dust_overflow_and_invalid_reservations() {
    let server = server(PredictEnvironment::BnbMainnet, "").await;
    let client = PredictChainClient::new(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        server.url.clone().into(),
    ))
    .unwrap();
    let snapshot = client
        .request_collateral_snapshot(ACCOUNT.parse().unwrap(), clock())
        .await
        .unwrap();
    for locked in ["-1 USDT", "124 USDT", "1 USDC"] {
        assert!(parse_collateral_balance(&snapshot, Money::from(locked)).is_err());
    }
    // Native Currency equality compares code only; a different monetary scale must not pass.
    let alternate = Currency::new(
        "USDT",
        Currency::USDT().precision - 1,
        0,
        "fixture",
        CurrencyType::Crypto,
    );
    assert!(parse_collateral_balance(&snapshot, Money::zero(alternate)).is_err());
    for amount in [U256::MAX, U256::from(1)] {
        let mut changed = snapshot.clone();
        changed.balance_wei = amount;
        assert!(parse_collateral_balance(&changed, Money::from("0 USDT")).is_err());
    }
    // Currency display precision does not limit the Native fixed-point amount.
    let mut precise = snapshot.clone();
    precise.balance_wei = U256::from(123_450_000_001_234_560_000u128);
    let locked = Money::from_str_exact("0.00000000000001 USDT").unwrap();
    let balance = parse_collateral_balance(&precise, locked).unwrap();
    assert_eq!(
        balance.total.as_decimal(),
        rust_decimal::Decimal::from_str_exact("123.45000000123456").unwrap()
    );
    assert_eq!(balance.total, balance.locked + balance.free);
    let mut changed = snapshot;
    changed.collateral = Address::ZERO;
    assert!(parse_collateral_balance(&changed, Money::from("0 USDT")).is_err());
    assert!(
        client
            .request_collateral_snapshot(Address::ZERO, clock())
            .await
            .is_err()
    );
}

#[test]
fn rpc_configuration_rejects_unsafe_urls_and_invalid_timeouts() {
    for url in [
        "http://example.com",
        "https://user:secret@example.com",
        "https://example.com/#secret",
        "file:///tmp/socket",
        "invalid",
    ] {
        assert!(
            PredictChainClient::new(PredictChainClientConfig::new(
                PredictEnvironment::BnbMainnet,
                url.into()
            ))
            .is_err()
        );
    }
    let mut config =
        PredictChainClientConfig::new(PredictEnvironment::BnbMainnet, "https://example.com".into());
    config.timeout_secs = 0;
    assert!(PredictChainClient::new(config).is_err());
}

#[tokio::test]
async fn reported_balance_reads_require_block_time_and_skip_all_allowance_calls() {
    for environment in [
        PredictEnvironment::BnbMainnet,
        PredictEnvironment::BnbTestnet,
    ] {
        let server = server(environment, "").await;
        let client = PredictChainClient::new(PredictChainClientConfig::new(
            environment,
            server.url.clone().into(),
        ))
        .unwrap();
        let snapshot = client
            .request_balance_snapshot(ACCOUNT.parse().unwrap(), clock())
            .await
            .unwrap();
        assert_eq!(snapshot.block_timestamp, UnixNanos::from(2_000_000_000u64));
        let requests = server.state.requests.lock().unwrap();
        assert_eq!(requests.len(), 5);
        assert_eq!(
            requests
                .iter()
                .filter(|r| r["method"] == "eth_call")
                .count(),
            2
        );
        assert!(
            requests
                .iter()
                .filter(|r| r["method"] == "eth_call")
                .all(|r| !r["params"][0]["data"]
                    .as_str()
                    .unwrap()
                    .starts_with("0xdd62ed3e"))
        );
    }
    for fault in ["timestamp_missing", "timestamp_overflow"] {
        let server = server(PredictEnvironment::BnbMainnet, fault).await;
        let client = PredictChainClient::new(PredictChainClientConfig::new(
            PredictEnvironment::BnbMainnet,
            server.url.clone().into(),
        ))
        .unwrap();
        assert!(
            client
                .request_balance_snapshot(ACCOUNT.parse().unwrap(), clock())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn buy_admission_reads_full_nonce_on_selected_exchange_and_rechecks_canonical_hash() {
    for environment in [
        PredictEnvironment::BnbMainnet,
        PredictEnvironment::BnbTestnet,
    ] {
        let server = server(environment, "").await;
        let client = PredictChainClient::new(PredictChainClientConfig::new(
            environment,
            server.url.clone().into(),
        ))
        .unwrap();
        let (snapshot, nonce, timestamp) = client
            .request_buy_admission(
                ACCOUNT.parse().unwrap(),
                PredictExchangeType::NegRisk,
                clock(),
            )
            .await
            .unwrap();
        assert_eq!(nonce, U256::MAX - U256::from(1));
        assert_eq!(timestamp.as_u64(), 2_000_000_000);
        assert_eq!(
            snapshot.account_address,
            ACCOUNT.parse::<Address>().unwrap()
        );
        let first_len = server.state.requests.lock().unwrap().len();
        client
            .request_buy_admission(
                ACCOUNT.parse().unwrap(),
                PredictExchangeType::NegRisk,
                clock(),
            )
            .await
            .unwrap();
        assert_eq!(server.state.requests.lock().unwrap().len() - first_len, 5);
        let requests = server.state.requests.lock().unwrap();
        assert_eq!(requests.len(), 12);
        assert_eq!(requests[5]["method"], "eth_call");
        assert_eq!(
            requests[5]["params"][0]["to"],
            format!("{:#x}", PredictExchangeType::NegRisk.contract(environment))
        );
        assert_eq!(
            requests[5]["params"][1],
            json!({"blockHash":BLOCK,"requireCanonical":true})
        );
    }
    let server = server(PredictEnvironment::BnbMainnet, "nonce_reorg").await;
    let client = PredictChainClient::new(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        server.url.clone().into(),
    ))
    .unwrap();
    assert!(
        client
            .request_buy_admission(
                ACCOUNT.parse().unwrap(),
                PredictExchangeType::NegRisk,
                clock()
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn sell_admission_reads_only_inventory_approval_nonce_and_canonical_block() {
    let server = server(PredictEnvironment::BnbMainnet, "").await;
    let client = PredictChainClient::new(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        server.url.clone().into(),
    ))
    .unwrap();
    // Establish immutable chain/token identity through the ordinary account bootstrap.
    client
        .request_balance_snapshot(ACCOUNT.parse().unwrap(), clock())
        .await
        .unwrap();
    let before = server.state.requests.lock().unwrap().len();
    let (balance, approved, nonce, _) = client
        .request_sell_admission(
            ACCOUNT.parse().unwrap(),
            PredictExchangeType::NegRisk,
            U256::from(1),
            clock(),
        )
        .await
        .unwrap();
    assert_eq!(balance, U256::from(17_000_000_000_000_000_000u128));
    assert!(approved);
    assert_eq!(nonce, U256::MAX - U256::from(1));
    let requests = server.state.requests.lock().unwrap();
    let admission = &requests[before..];
    assert_eq!(admission.len(), 5);
    assert_eq!(
        admission
            .iter()
            .filter(|r| r["method"] == "eth_call")
            .count(),
        3
    );
    for call in admission.iter().filter(|r| r["method"] == "eth_call") {
        let data = call["params"][0]["data"].as_str().unwrap();
        assert!(
            !["0x313ce567", "0x70a08231", "0xdd62ed3e"]
                .iter()
                .any(|selector| data.starts_with(selector))
        );
    }
}
