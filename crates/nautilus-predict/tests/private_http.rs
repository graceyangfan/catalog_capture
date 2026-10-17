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

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use alloy::primitives::Address;
use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Method, Uri},
    response::Response,
};
use nautilus_predict::{
    common::{credential::PredictAuthToken, environment::PredictEnvironment},
    config::PredictPrivateClientConfig,
    http::{PredictPrivateHttpClient, error::Error, execution_models::PredictReferralStatus},
    signing::{PredictAuthSigner, PredictChain},
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle};

// Public Hardhat test key, not an operator credential.
const PUBLIC_TEST_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const OWNER: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";
const DEPOSIT: &str = "0x1111111111111111111111111111111111111111";
const API_KEY: &str = "api-key-test-sentinel";
const TOKEN: &str = "jwt-test-sentinel";

#[derive(Debug)]
struct RecordedRequest {
    method: Method,
    path: String,
    query: Option<String>,
    headers: HeaderMap,
    body: Value,
}

struct Reply {
    status: u16,
    body: String,
    headers: Vec<(&'static str, String)>,
}

impl Reply {
    fn json(body: Value) -> Self {
        Self {
            status: 200,
            body: body.to_string(),
            headers: vec![],
        }
    }
}

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

type TestState = (
    Arc<Mutex<Vec<RecordedRequest>>>,
    Arc<Mutex<VecDeque<Reply>>>,
);

async fn respond(
    State((requests, replies)): State<TestState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    requests.lock().unwrap().push(RecordedRequest {
        method,
        path: uri.path().to_string(),
        query: uri.query().map(str::to_string),
        headers,
        body: if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&body).unwrap()
        },
    });
    let reply = replies
        .lock()
        .unwrap()
        .pop_front()
        .expect("unexpected extra request");
    let mut response = Response::builder()
        .status(reply.status)
        .header("content-type", "application/json");
    for (name, value) in reply.headers {
        response = response.header(name, value);
    }
    response.body(Body::from(reply.body)).unwrap()
}

async fn server(replies: Vec<Reply>) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(vec![]));
    let state = (Arc::clone(&requests), Arc::new(Mutex::new(replies.into())));
    let router = Router::new().fallback(respond).with_state(state);
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    Server {
        url,
        requests,
        task,
    }
}

fn transport(server: &Server) -> PredictPrivateHttpClient {
    http_client(
        PredictEnvironment::BnbMainnet,
        Some(API_KEY),
        Some(&server.url),
        5,
    )
    .unwrap()
}

fn http_client(
    environment: PredictEnvironment,
    api_key: Option<&str>,
    base_url: Option<&str>,
    timeout_secs: u64,
) -> Result<PredictPrivateHttpClient, Error> {
    PredictPrivateHttpClient::new(PredictPrivateClientConfig {
        environment,
        api_key: api_key.map(Into::into),
        base_url_http: base_url.map(str::to_string),
        http_timeout_secs: timeout_secs,
    })
}

fn account(address: &str) -> Value {
    json!({"success": true, "data": {
        "name": "fixture", "address": address, "imageUrl": null,
        "referral": {"code": null, "status": "LOCKED"}, "points": {"total": 17.25}
    }})
}

#[tokio::test]
async fn authentication_uses_dynamic_sdk_challenges_and_correct_wallet_identity() {
    let fixtures: Value = serde_json::from_str(include_str!("fixtures/auth_vectors.json")).unwrap();
    // EOA and Predict Account, each with two distinct messages, including Unicode.
    for (deposit, offset) in [(None, 0), (Some(DEPOSIT), 4)] {
        let owner = deposit.unwrap_or(OWNER);
        let mut replies = vec![];
        for index in [offset + 1, offset + 2] {
            replies.push(Reply::json(json!({"success": true, "data": {
                "message": fixtures["vectors"][index]["message"]
            }})));
            replies.push(Reply::json(
                json!({"success": true, "data": {"token": TOKEN}}),
            ));
            replies.push(Reply::json(account(owner)));
        }
        let server = server(replies).await;
        let client = transport(&server);
        let signer =
            PredictAuthSigner::new(PUBLIC_TEST_KEY.into(), PredictChain::BnbMainnet, deposit)
                .unwrap();
        for _ in 0..2 {
            let session = client.authenticate(&signer).await.unwrap();
            let account = session.request_account().await.unwrap();
            assert_eq!(account.address, owner.parse::<Address>().unwrap());
            assert_eq!(account.referral.status, PredictReferralStatus::Locked);
            assert_eq!(account.points.total.to_string(), "17.25");
            assert!(account.image_url.is_none());
            assert!(account.referral.code.is_none());
            let debug = format!("{session:?} {client:?}");
            assert!(!debug.contains(API_KEY));
            assert!(!debug.contains(TOKEN));
            assert!(!debug.contains(PUBLIC_TEST_KEY.trim_start_matches("0x")));
        }
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 6);
        for (group, index) in requests
            .as_chunks::<3>()
            .0
            .iter()
            .zip([offset + 1, offset + 2])
        {
            assert_eq!(group[0].method, Method::GET);
            assert_eq!(group[0].path, "/v1/auth/message");
            assert_eq!(group[1].method, Method::POST);
            assert_eq!(group[1].path, "/v1/auth");
            assert_eq!(group[1].headers["content-type"], "application/json");
            assert_eq!(
                group[1].body,
                json!({
                    "signer": fixtures["vectors"][index]["signer"],
                    "message": fixtures["vectors"][index]["message"],
                    "signature": fixtures["vectors"][index]["signature"]
                })
            );
            for request in group {
                assert_eq!(request.headers["x-api-key"], API_KEY);
            }
            assert!(!group[0].headers.contains_key("authorization"));
            assert!(!group[1].headers.contains_key("authorization"));
            assert_eq!(group[2].method, Method::GET);
            assert_eq!(group[2].path, "/v1/account");
            assert_eq!(group[2].headers["authorization"], format!("Bearer {TOKEN}"));
        }
    }
}

#[tokio::test]
async fn testnet_omits_api_key_and_sessions_keep_separate_tokens() {
    let server = server(vec![
        Reply::json(account(OWNER)),
        Reply::json(account(OWNER)),
    ])
    .await;
    let client = http_client(PredictEnvironment::BnbTestnet, None, Some(&server.url), 5).unwrap();
    let first = client
        .authenticated(
            PredictAuthToken::new("first-token".into()).unwrap(),
            OWNER.parse().unwrap(),
        )
        .unwrap();
    let second = client
        .authenticated(
            PredictAuthToken::new("second-token".into()).unwrap(),
            OWNER.parse().unwrap(),
        )
        .unwrap();
    let (first_result, second_result) =
        tokio::join!(first.request_account(), second.request_account());
    assert!(first_result.is_ok());
    assert!(second_result.is_ok());
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| !request.headers.contains_key("x-api-key"))
    );
    let tokens: Vec<_> = requests
        .iter()
        .map(|request| request.headers["authorization"].to_str().unwrap())
        .collect();
    assert!(tokens.contains(&"Bearer first-token"));
    assert!(tokens.contains(&"Bearer second-token"));
}

#[tokio::test]
async fn account_identity_mismatch_is_rejected() {
    let server = server(vec![Reply::json(account(DEPOSIT))]).await;
    let session = transport(&server)
        .authenticated(
            PredictAuthToken::new(TOKEN.into()).unwrap(),
            OWNER.parse().unwrap(),
        )
        .unwrap();
    assert!(matches!(
        session.request_account().await,
        Err(Error::AccountMismatch)
    ));
}

#[tokio::test]
async fn status_envelope_and_schema_failures_do_not_leak_secrets_or_retry() {
    let cases = [
        (401, "{}"),
        (403, "{}"),
        (429, "{}"),
        (500, "{}"),
        (200, "{\"success\":false}"),
        (200, "{\"success\":true}"),
        (
            200,
            "{\"success\":true,\"data\":{\"token\":\"jwt-test-sentinel\"}}",
        ),
        (200, "not json jwt-test-sentinel"),
        (
            200,
            "{\"success\":false,\"data\":{\"token\":\"jwt-test-sentinel\"}}",
        ),
    ];
    for (index, (status, body)) in cases.into_iter().enumerate() {
        let body = if status != 200 {
            format!("{body} {API_KEY} {TOKEN}")
        } else {
            body.into()
        };
        let server = server(vec![Reply {
            status,
            body,
            headers: vec![("Retry-After", "7".into())],
        }])
        .await;
        let session = transport(&server)
            .authenticated(
                PredictAuthToken::new(TOKEN.into()).unwrap(),
                OWNER.parse().unwrap(),
            )
            .unwrap();
        let error = session.request_account().await.unwrap_err();
        match index {
            0 | 1 => assert!(matches!(error, Error::Authentication { .. })),
            2 => assert!(matches!(
                error,
                Error::RateLimit {
                    retry_after_secs: Some(7),
                    ..
                }
            )),
            3 => assert!(matches!(error, Error::Http { status: 500, .. })),
            4 | 8 => assert!(matches!(error, Error::Venue { .. })),
            _ => assert!(matches!(error, Error::Decode { .. })),
        }
        let debug = format!("{error:?} {error}");
        assert!(!debug.contains(API_KEY));
        assert!(!debug.contains(TOKEN));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn authentication_failure_stops_before_creating_a_session() {
    let signer =
        PredictAuthSigner::new(PUBLIC_TEST_KEY.into(), PredictChain::BnbMainnet, None).unwrap();
    let challenge = Reply::json(json!({"success": true, "data": {"message": "dynamic nonce"}}));
    let server = server(vec![
        challenge,
        Reply {
            status: 401,
            body: format!("rejected signature {TOKEN}"),
            headers: vec![],
        },
    ])
    .await;
    let error = transport(&server).authenticate(&signer).await.unwrap_err();
    assert!(matches!(
        error,
        Error::Authentication {
            endpoint: "/v1/auth",
            status: 401
        }
    ));
    assert_eq!(server.requests.lock().unwrap().len(), 2);

    for token in ["", "bad\r\nheader"] {
        let server = self::server(vec![
            Reply::json(json!({"success": true, "data": {"message": "dynamic nonce"}})),
            Reply::json(json!({"success": true, "data": {"token": token}})),
        ])
        .await;
        let error = transport(&server).authenticate(&signer).await.unwrap_err();
        assert!(matches!(
            error,
            Error::Decode {
                endpoint: "/v1/auth",
                ..
            }
        ));
        assert_eq!(server.requests.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn empty_or_invalid_challenge_does_not_send_a_signature() {
    let signer =
        PredictAuthSigner::new(PUBLIC_TEST_KEY.into(), PredictChain::BnbMainnet, None).unwrap();
    for data in [json!({"message": ""}), json!({"message": 42}), json!({})] {
        let server = server(vec![Reply::json(json!({"success": true, "data": data}))]).await;
        let error = transport(&server).authenticate(&signer).await.unwrap_err();
        assert!(matches!(
            error,
            Error::Decode {
                endpoint: "/v1/auth/message",
                ..
            }
        ));
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn stalled_request_times_out_without_spawning_retries() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let server = Server {
        url,
        requests: Arc::new(Mutex::new(vec![])),
        task,
    };
    let client = http_client(
        PredictEnvironment::BnbMainnet,
        Some(API_KEY),
        Some(&server.url),
        1,
    )
    .unwrap();
    let error = client.request_auth_message().await.unwrap_err();
    assert!(matches!(
        error,
        Error::Timeout {
            endpoint: "/v1/auth/message"
        }
    ));
}

#[tokio::test]
async fn redirect_cannot_forward_credentials() {
    let destination = server(vec![]).await;
    let location = format!("{}/v1/account", destination.url);
    let source = server(vec![Reply {
        status: 307,
        body: "".into(),
        headers: vec![("Location", location)],
    }])
    .await;
    let session = transport(&source)
        .authenticated(
            PredictAuthToken::new(TOKEN.into()).unwrap(),
            OWNER.parse().unwrap(),
        )
        .unwrap();
    assert!(session.request_account().await.is_err());
    assert_eq!(source.requests.lock().unwrap().len(), 1);
    assert!(destination.requests.lock().unwrap().is_empty());
}

#[test]
fn constructor_rejects_invalid_credentials_and_unsafe_urls() {
    use PredictEnvironment::{BnbMainnet, BnbTestnet};
    assert!(http_client(BnbMainnet, None, None, 5).is_err());
    assert!(http_client(BnbTestnet, None, Some("https://api-testnet.predict.fun"), 5).is_ok());
    for url in [
        "invalid",
        "http://api.predict.fun",
        "https://secret@api.predict.fun",
        "https://api.predict.fun?key=secret",
        "https://api.predict.fun#secret",
    ] {
        assert!(http_client(BnbMainnet, Some(API_KEY), Some(url), 5).is_err());
    }
    assert!(http_client(BnbMainnet, Some("bad\r\nkey"), None, 5).is_err());
    assert!(http_client(BnbMainnet, Some(API_KEY), None, 0).is_err());
    assert!(http_client(BnbMainnet, Some(API_KEY), Some(BnbTestnet.http_url()), 5).is_err());
    assert!(http_client(BnbTestnet, None, Some(BnbMainnet.http_url()), 5).is_err());
    assert!(http_client(BnbMainnet, None, Some("http://127.0.0.1:12345"), 5).is_err());
    let client = http_client(BnbMainnet, Some(API_KEY), None, 5).unwrap();
    assert!(
        client
            .authenticated(PredictAuthToken::new(TOKEN.into()).unwrap(), Address::ZERO)
            .is_err()
    );
}

#[tokio::test]
async fn mismatched_signing_environment_is_rejected_before_http() {
    for (transport_env, signing_env) in [
        (
            PredictEnvironment::BnbMainnet,
            PredictEnvironment::BnbTestnet,
        ),
        (
            PredictEnvironment::BnbTestnet,
            PredictEnvironment::BnbMainnet,
        ),
    ] {
        for account in [None, Some(DEPOSIT)] {
            let server = server(vec![]).await;
            let client = http_client(transport_env, Some(API_KEY), Some(&server.url), 5).unwrap();
            let signer =
                PredictAuthSigner::new(PUBLIC_TEST_KEY.into(), signing_env, account).unwrap();
            assert!(matches!(
                client.authenticate(&signer).await,
                Err(Error::Configuration(_))
            ));
            assert!(server.requests.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn testnet_authentication_uses_sdk_domain_without_api_key() {
    let fixtures: Value = serde_json::from_str(include_str!("fixtures/auth_vectors.json")).unwrap();
    let vector = &fixtures["vectors"][13];
    let server = server(vec![
        Reply::json(json!({"success": true, "data": {"message": vector["message"]}})),
        Reply::json(json!({"success": true, "data": {"token": TOKEN}})),
        Reply::json(account(DEPOSIT)),
    ])
    .await;
    let client = http_client(PredictEnvironment::BnbTestnet, None, Some(&server.url), 5).unwrap();
    let signer = PredictAuthSigner::new(
        PUBLIC_TEST_KEY.into(),
        PredictEnvironment::BnbTestnet,
        Some(DEPOSIT),
    )
    .unwrap();
    client
        .authenticate(&signer)
        .await
        .unwrap()
        .request_account()
        .await
        .unwrap();
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(
        requests
            .iter()
            .all(|request| !request.headers.contains_key("x-api-key"))
    );
    assert_eq!(requests[1].body["signature"], vector["signature"]);
}

#[tokio::test]
async fn reordered_envelope_keeps_venue_failures_and_schema_diagnostics_safe() {
    use nautilus_predict::http::error::PredictDecodeErrorKind;
    for (body, venue_failure) in [
        (
            format!(r#"{{"data":{{"unexpected":"{TOKEN}"}},"success":false}}"#),
            true,
        ),
        (
            format!(r#"{{"data":{{"address":"{TOKEN}"}},"success":true}}"#),
            false,
        ),
    ] {
        let server = server(vec![Reply {
            status: 200,
            body,
            headers: vec![],
        }])
        .await;
        let session = transport(&server)
            .authenticated(
                PredictAuthToken::new(TOKEN.into()).unwrap(),
                OWNER.parse().unwrap(),
            )
            .unwrap();
        let e = session.request_account().await.unwrap_err();
        if venue_failure {
            assert!(matches!(e, Error::Venue { .. }));
        } else {
            assert!(
                matches!(e, Error::Decode { kind: PredictDecodeErrorKind::Data, line: 1, column, .. } if column > 0)
            );
        }
        assert!(!format!("{e:?} {e}").contains(TOKEN));
    }
    let server = server(vec![Reply::json(account(OWNER))]).await;
    let session = transport(&server)
        .authenticated(
            PredictAuthToken::new(TOKEN.into()).unwrap(),
            OWNER.parse().unwrap(),
        )
        .unwrap();
    assert_eq!(
        session.request_account().await.unwrap().address,
        OWNER.parse::<Address>().unwrap()
    );
}

#[path = "private_http/queries.rs"]
mod queries;

#[path = "private_http/commands.rs"]
mod commands;

#[path = "private_http/market.rs"]
mod market;

#[tokio::test]
async fn renewal_updates_clones_only_after_identity_validation_and_preserves_other_sessions() {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let now = nautilus_core::time::get_atomic_clock_realtime()
        .get_time_ns()
        .as_u64()
        / 1_000_000_000;
    let renewed = format!(
        "e30.{}.signature",
        URL_SAFE_NO_PAD.encode(json!({"exp":now+3600}).to_string())
    );
    let server = server(vec![
        Reply::json(json!({"success":true,"data":{"message":"renewal challenge"}})),
        Reply::json(json!({"success":true,"data":{"token":renewed}})),
        Reply::json(account(DEPOSIT)),
        Reply::json(account(DEPOSIT)),
        Reply::json(account(DEPOSIT)),
        Reply::json(json!({"success":true,"data":{"message":"rejected identity"}})),
        Reply::json(json!({"success":true,"data":{"token":TOKEN}})),
        Reply::json(account(OWNER)),
    ])
    .await;
    let transport = transport(&server);
    let session = transport
        .authenticated(
            PredictAuthToken::new(TOKEN.into()).unwrap(),
            DEPOSIT.parse().unwrap(),
        )
        .unwrap();
    let clone = session.clone();
    let independent = transport
        .authenticated(
            PredictAuthToken::new(TOKEN.into()).unwrap(),
            DEPOSIT.parse().unwrap(),
        )
        .unwrap();
    let signer = PredictAuthSigner::new(
        PUBLIC_TEST_KEY.into(),
        PredictChain::BnbMainnet,
        Some(DEPOSIT),
    )
    .unwrap();
    session.renew(&signer).await.unwrap();
    clone.request_account().await.unwrap();
    independent.request_account().await.unwrap();
    assert_eq!(clone.token().expose_secret(), renewed);
    assert_eq!(independent.token().expose_secret(), TOKEN);
    assert!(matches!(
        session.renew(&signer).await,
        Err(Error::AccountMismatch)
    ));
    assert_eq!(
        clone.token().expose_secret(),
        renewed,
        "invalid replacement must not publish"
    );
    let wrong_signer =
        PredictAuthSigner::new(PUBLIC_TEST_KEY.into(), PredictChain::BnbMainnet, None).unwrap();
    assert!(session.renew(&wrong_signer).await.is_err());
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 8, "wrong signer must fail without I/O");
    assert_eq!(
        requests[2].headers["authorization"],
        format!("Bearer {renewed}")
    );
    assert_eq!(
        requests[3].headers["authorization"],
        format!("Bearer {renewed}")
    );
    assert_eq!(
        requests[4].headers["authorization"],
        format!("Bearer {TOKEN}")
    );
    assert!(
        requests
            .iter()
            .all(|r| r.method == Method::GET || r.path == "/v1/auth")
    );
    let debug = format!("{session:?}");
    assert!(!debug.contains(&renewed));
}

#[tokio::test]
async fn failed_or_expired_renewal_keeps_the_previous_bearer_without_retrying_operations() {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    for failure in ["rejected", "opaque", "expired"] {
        let replies = if failure == "rejected" {
            vec![
                Reply::json(json!({"success":true,"data":{"message":"challenge"}})),
                Reply {
                    status: 401,
                    body: "unauthorized".into(),
                    headers: vec![],
                },
            ]
        } else {
            let token = if failure == "opaque" {
                TOKEN.to_string()
            } else {
                format!("e30.{}.signature", URL_SAFE_NO_PAD.encode(r#"{"exp":0}"#))
            };
            vec![
                Reply::json(json!({"success":true,"data":{"message":"challenge"}})),
                Reply::json(json!({"success":true,"data":{"token":token}})),
                Reply::json(account(DEPOSIT)),
            ]
        };
        let server = server(replies).await;
        let session = transport(&server)
            .authenticated(
                PredictAuthToken::new(TOKEN.into()).unwrap(),
                DEPOSIT.parse().unwrap(),
            )
            .unwrap();
        let signer = PredictAuthSigner::new(
            PUBLIC_TEST_KEY.into(),
            PredictChain::BnbMainnet,
            Some(DEPOSIT),
        )
        .unwrap();
        assert!(session.renew(&signer).await.is_err(), "{failure}");
        assert_eq!(session.token().expose_secret(), TOKEN);
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), if failure == "rejected" { 2 } else { 3 });
        assert!(
            requests
                .iter()
                .all(|r| r.method == Method::GET || r.path == "/v1/auth")
        );
    }
}
