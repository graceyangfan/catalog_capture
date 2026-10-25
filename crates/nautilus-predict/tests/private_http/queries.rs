// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Schema fault controls and explicitly retained, sanitized private venue responses.

use std::collections::HashMap;

use alloy::primitives::{B256, U256};
use nautilus_network::http::Url;
use nautilus_predict::http::{
    PredictAuthenticatedHttpClient,
    account_models::{PredictFeeType, PredictOrderStatus, PredictOrderStrategy, PredictQuoteType},
    error::PredictDecodeErrorKind,
    query::{
        PredictMatchesQuery, PredictOrderStatusFilter, PredictOrdersQuery, PredictPositionSort,
        PredictPositionsQuery,
    },
};

use super::*;

const NEXT: &str = "cursor+/=&? 中文";
const HASH: &str = "0x17e3c43e261d9a73f845b38845f06f52589bcb64a82f70831f4acbc818df1dfc";
const OTHER_HASH: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[tokio::test]
async fn recorded_market_history_preserves_side_units_and_filled_atomic_remainder() {
    use nautilus_predict::common::enums::PredictOrderSide;
    let saved: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/live_market_history_readonly_20261025.json"
    ))
    .unwrap();
    let rows = saved["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| {
            event["method"] == "GET" && event["path"].as_str().unwrap().starts_with("/v1/orders/0x")
        })
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 9);
    let mut statuses = [0, 0];
    let mut sides = [0, 0];
    let mut atomic_remainders = 0;
    for event in rows {
        assert_eq!(event["status"], 200);
        assert_eq!(event["officialSdkHashVerifiedBeforeRedaction"], true);
        let raw = &event["responseBody"];
        let data = &raw["data"];
        let hash: B256 = data["order"]["hash"].as_str().unwrap().parse().unwrap();
        let server = server(vec![Reply::json(raw.clone())]).await;
        let session = transport(&server)
            .authenticated(
                PredictAuthToken::new(TOKEN.into()).unwrap(),
                data["order"]["maker"].as_str().unwrap().parse().unwrap(),
            )
            .unwrap();
        let row = session.request_order(hash).await.unwrap();
        assert_eq!(row.strategy, PredictOrderStrategy::Market);
        let shares = match row.order.fields.side {
            PredictOrderSide::Buy => {
                sides[0] += 1;
                row.order.fields.taker_amount
            }
            PredictOrderSide::Sell => {
                sides[1] += 1;
                row.order.fields.maker_amount
            }
        };
        assert_eq!(row.amount.parse::<U256>().unwrap(), shares);
        let filled: U256 = row.amount_filled.parse().unwrap();
        match row.status {
            PredictOrderStatus::Cancelled => {
                statuses[0] += 1;
                assert!(filled.is_zero());
            }
            PredictOrderStatus::Filled => {
                statuses[1] += 1;
                assert!(filled <= shares);
                if filled < shares {
                    assert_eq!(shares - filled, U256::from(1));
                    assert_eq!(shares, U256::from(7_650_000_000_000_000_000u64));
                    atomic_remainders += 1;
                }
            }
            _ => panic!("historical terminal changed in the frozen capture"),
        }
        assert_eq!(row.amount, data["amount"].as_str().unwrap());
        assert_eq!(row.amount_filled, data["amountFilled"].as_str().unwrap());
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_authenticated(&requests[0]);
    }
    assert_eq!(statuses, [6, 3]);
    assert_eq!(sides, [8, 1]);
    assert_eq!(atomic_remainders, 1);
    assert_eq!(saved["orderWrites"], 0);
    assert_eq!(saved["chainWrites"], 0);
}

#[tokio::test]
async fn captured_sanitized_private_pages_replay_without_operator_credentials() {
    let captured = [
        include_str!("../fixtures/private_api/account.json"),
        include_str!("../fixtures/private_api/orders.json"),
        include_str!("../fixtures/private_api/positions.json"),
    ];
    let account: Value = serde_json::from_str(captured[0]).unwrap();
    let address = account["data"]["address"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let server = server(
        captured
            .into_iter()
            .map(|text| Reply::json(serde_json::from_str(text).unwrap()))
            .collect(),
    )
    .await;
    let session = transport(&server)
        .authenticated(PredictAuthToken::new(TOKEN.into()).unwrap(), address)
        .unwrap();
    let account = session.request_account().await.unwrap();
    assert_eq!(account.address, address);
    assert_eq!(account.name, "redacted-fixture");
    let orders = session
        .request_orders(&PredictOrdersQuery::default())
        .await
        .unwrap();
    let positions = session
        .request_positions(&PredictPositionsQuery::default())
        .await
        .unwrap();
    assert!(orders.data.is_empty() && orders.cursor.is_none());
    assert!(positions.data.is_empty() && positions.cursor.is_none());
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests.iter().map(|r| r.path.as_str()).collect::<Vec<_>>(),
        ["/v1/account", "/v1/orders", "/v1/positions"]
    );
    for request in requests.iter() {
        assert_authenticated(request);
    }
}

fn session(server: &Server) -> PredictAuthenticatedHttpClient {
    transport(server)
        .authenticated(
            PredictAuthToken::new(TOKEN.into()).unwrap(),
            OWNER.parse().unwrap(),
        )
        .unwrap()
}

fn params(request: &RecordedRequest) -> HashMap<String, String> {
    Url::parse(&format!(
        "http://localhost/?{}",
        request.query.as_deref().unwrap_or_default()
    ))
    .unwrap()
    .query_pairs()
    .map(|(key, value)| (key.into_owned(), value.into_owned()))
    .collect()
}

fn assert_authenticated(request: &RecordedRequest) {
    assert_eq!(request.method, Method::GET);
    assert_eq!(request.headers["x-api-key"], API_KEY);
    assert_eq!(request.headers["Authorization"], format!("Bearer {TOKEN}"));
    assert_eq!(request.body, Value::Null);
    assert!(!request.path.contains(TOKEN));
    assert!(!request.query.as_deref().unwrap_or_default().contains(TOKEN));
}

fn order(status: &str) -> Value {
    let fixtures: Value =
        serde_json::from_str(include_str!("../fixtures/order_vectors.json")).unwrap();
    let vector = &fixtures["vectors"][0];
    let mut contract = vector["order"].clone();
    contract["hash"] = vector["hash"].clone();
    contract["signature"] = vector["signature"].clone();
    json!({"order": contract, "id": "9223372036854775807", "marketId": 42,
        "currency": "USDT", "amount": "100000000000000000001", "amountFilled": "1",
        "isNegRisk": false, "isYieldBearing": true, "strategy": "LIMIT",
        "status": status, "rewardEarningRate": 0.125})
}

fn outcome(token: U256) -> Value {
    json!({"name": "No", "indexSet": 2, "onChainId": token.to_string(), "status": null})
}

fn market() -> Value {
    json!({"id": 42, "isNegRisk": true, "isYieldBearing": true,
        "decimalPrecision": 3, "outcomes": [outcome(U256::MAX)],
        "futureMetadata": {"ignored": true}})
}

#[tokio::test]
async fn execution_metadata_requires_explicit_fees_flags_and_requested_market_identity() {
    let complete = json!({"id":42,"tradingStatus":"OPEN","feeRateBps":0,"isNegRisk":false,"isYieldBearing":true,
        "decimalPrecision":3,"outcomes":[{"indexSet":1,"onChainId":"11"},{"indexSet":2,"onChainId":"22"}]});
    let mut missing_fee = complete.clone();
    missing_fee.as_object_mut().unwrap().remove("feeRateBps");
    let mut missing_flag = complete.clone();
    missing_flag
        .as_object_mut()
        .unwrap()
        .remove("isYieldBearing");
    let mut foreign = complete.clone();
    foreign["id"] = json!(43);
    let server = server(vec![
        Reply::json(json!({"success":true,"data":complete})),
        Reply::json(json!({"success":true,"data":missing_fee})),
        Reply::json(json!({"success":true,"data":missing_flag})),
        Reply::json(json!({"success":true,"data":foreign})),
    ])
    .await;
    let client = session(&server);
    let metadata = client.request_execution_market(42).await.unwrap();
    assert_eq!(metadata.fee_rate_bps, 0);
    assert_eq!(
        metadata.exchange(),
        nautilus_predict::common::contracts::PredictExchangeType::YieldBearing
    );
    for _ in 0..3 {
        assert!(client.request_execution_market(42).await.is_err());
    }
    assert!(client.request_execution_market(0).await.is_err());
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    for request in requests.iter() {
        assert_authenticated(request);
        assert_eq!(request.path, "/v1/markets/42");
    }
}

fn position() -> Value {
    json!({"id": "position-42", "market": market(), "outcome": outcome(U256::MAX),
        "amount": U256::MAX.to_string(), "valueUsd": "1000000000000000000.123456789012345678",
        "averageBuyPriceUsd": "0.621234567890123456", "pnlUsd": "-0.000000000000000001"})
}

fn leg(signer: &str, hash: Option<&str>, fee_type: &str) -> Value {
    let mut data = json!({"quoteType": "Bid", "amount": "17.000000000000000001",
        "price": "0.620000000000000001", "outcome": outcome(U256::MAX), "signer": signer,
        "fee": {"amount": "100000000000000001", "type": fee_type}});
    if let Some(hash) = hash {
        data["hash"] = json!(hash);
    }
    data
}

fn match_event() -> Value {
    json!({"market": market(), "taker": leg(DEPOSIT, Some(OTHER_HASH), "COLLATERAL"),
        "amountFilled": "34.000000000000000002", "priceExecuted": "0.38",
        "makers": [leg(OWNER, Some(HASH), "SHARES"), leg(OWNER, None, "COLLATERAL")],
        "transactionHash": OTHER_HASH, "settlementId": "settlement-42",
        "executedAt": "2026-10-13T01:02:03.123456789Z"})
}

#[tokio::test]
async fn orders_preserve_all_states_and_follow_opaque_cursor_past_an_empty_page() {
    let server = server(vec![
        Reply::json(json!({"success": true, "data": [order("OPEN")], "cursor": NEXT})),
        Reply::json(json!({"success": true, "data": [], "cursor": "last"})),
        Reply::json(json!({"success": true, "cursor": null, "data": [
            order("FILLED"), order("EXPIRED"), order("CANCELLED"), order("INVALIDATED")]})),
    ])
    .await;
    let client = session(&server);
    let page = client
        .request_orders(&PredictOrdersQuery::default())
        .await
        .unwrap();
    assert_eq!(page.cursor.as_deref(), Some(NEXT));
    assert_eq!(page.data[0].status, PredictOrderStatus::Open);
    assert_eq!(page.data[0].id, "9223372036854775807");
    assert_eq!(page.data[0].order.hash, Some(HASH.parse().unwrap()));
    assert_eq!(page.data[0].amount, "100000000000000000001");
    assert_eq!(page.data[0].amount_filled, "1");
    assert_eq!(page.data[0].strategy, PredictOrderStrategy::Limit);
    assert!(!format!("{:?}", page.data[0]).contains("951dbacfa371"));
    let empty = client
        .request_orders(&PredictOrdersQuery {
            first: Some(5),
            after: page.cursor.as_deref(),
            status: Some(PredictOrderStatusFilter::Open),
        })
        .await
        .unwrap();
    assert!(empty.data.is_empty());
    assert_eq!(empty.cursor.as_deref(), Some("last"));
    let last = client
        .request_orders(&PredictOrdersQuery {
            first: Some(5),
            after: empty.cursor.as_deref(),
            status: Some(PredictOrderStatusFilter::Filled),
        })
        .await
        .unwrap();
    assert!(last.cursor.is_none());
    assert_eq!(
        last.data
            .iter()
            .map(|order| order.status)
            .collect::<Vec<_>>(),
        [
            PredictOrderStatus::Filled,
            PredictOrderStatus::Expired,
            PredictOrderStatus::Cancelled,
            PredictOrderStatus::Invalidated,
        ]
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(params(&requests[0]).is_empty());
    assert_eq!(
        params(&requests[1]),
        HashMap::from([
            ("first".into(), "5".into()),
            ("after".into(), NEXT.into()),
            ("status".into(), "OPEN".into()),
        ])
    );
    assert_eq!(params(&requests[2])["status"], "FILLED");
    for request in &*requests {
        assert_eq!(request.path, "/v1/orders");
        assert_authenticated(request);
    }
}

#[tokio::test]
async fn single_order_uses_hash_and_checks_identity_without_inventing_missing_hash() {
    let mut no_hash = order("FILLED");
    no_hash["order"].as_object_mut().unwrap().remove("hash");
    no_hash["order"]["expiration"] = json!(4102444800u64);
    let mut wrong_hash = order("OPEN");
    wrong_hash["order"]["hash"] = json!(OTHER_HASH);
    let server = server(vec![
        Reply::json(json!({"success": true, "data": no_hash})),
        Reply::json(json!({"success": true, "data": wrong_hash})),
        Reply {
            status: 404,
            body: TOKEN.into(),
            headers: vec![],
        },
    ])
    .await;
    let client = session(&server);
    let first = client.request_order(HASH.parse().unwrap()).await.unwrap();
    assert!(first.order.hash.is_none());
    assert_eq!(first.order.fields.expiration, U256::from(4102444800u64));
    assert!(matches!(
        client.request_order(HASH.parse().unwrap()).await,
        Err(Error::Decode {
            kind: PredictDecodeErrorKind::InvalidOrderHash,
            ..
        })
    ));
    let error = client.request_order(B256::ZERO).await.unwrap_err();
    assert!(matches!(
        error,
        Error::Http {
            endpoint: "/v1/orders/{hash}",
            status: 404
        }
    ));
    assert!(!format!("{error:?} {error}").contains(TOKEN));
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests[0].path, format!("/v1/orders/{HASH}"));
    assert!(params(&requests[0]).is_empty());
    assert_authenticated(&requests[0]);
}

#[tokio::test]
async fn positions_preserve_full_width_amounts_and_explicit_false_filters() {
    let server = server(vec![
        Reply::json(json!({"success": true, "cursor": NEXT, "data": [position()]})),
        // PositionsResponse permits an omitted cursor on the terminal page.
        Reply::json(json!({"success": true, "data": []})),
    ])
    .await;
    let client = session(&server);
    let page = client
        .request_positions(&PredictPositionsQuery {
            first: Some(2),
            after: Some(NEXT),
            market_id: Some(42),
            category_id: Some("a&b/中"),
            is_resolved: Some(false),
            include_lost: Some(true),
            sort: Some(PredictPositionSort::EventBlockDesc),
        })
        .await
        .unwrap();
    let holding = &page.data[0];
    assert_eq!(holding.amount, U256::MAX);
    assert_eq!(holding.outcome.on_chain_id, U256::MAX);
    assert_eq!(holding.pnl_usd, "-0.000000000000000001");
    assert_eq!(holding.average_buy_price_usd, "0.621234567890123456");
    assert!(
        client
            .request_positions(&PredictPositionsQuery::default())
            .await
            .unwrap()
            .data
            .is_empty()
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests[0].path, "/v1/positions");
    assert_eq!(
        params(&requests[0]),
        HashMap::from([
            ("first".into(), "2".into()),
            ("after".into(), NEXT.into()),
            ("marketId".into(), "42".into()),
            ("categoryId".into(), "a&b/中".into()),
            ("isResolved".into(), "false".into()),
            ("includeLost".into(), "true".into()),
            ("sort".into(), "EVENT_BLOCK_DESC".into()),
        ])
    );
    assert!(params(&requests[1]).is_empty());
    assert_authenticated(&requests[0]);
}

#[tokio::test]
async fn matches_scope_signer_and_preserve_each_leg_and_fee_asset() {
    let mut old_schema = match_event();
    old_schema.as_object_mut().unwrap().remove("settlementId");
    let server = server(vec![
        Reply::json(json!({"success": true, "cursor": NEXT, "data": [match_event()]})),
        Reply::json(json!({"success": true, "cursor": null, "data": [old_schema]})),
    ])
    .await;
    let client = session(&server);
    let page = client
        .request_matches(&PredictMatchesQuery {
            first: Some(3),
            after: Some(NEXT),
            market_id: Some(42),
            min_value_usdt_wei: Some(U256::MAX),
            is_signer_maker: Some(true),
        })
        .await
        .unwrap();
    let event = &page.data[0];
    assert_eq!(event.makers.len(), 2);
    assert_eq!(event.makers[0].quote_type, PredictQuoteType::Bid);
    assert_eq!(event.makers[0].fee.fee_type, PredictFeeType::Shares);
    assert_eq!(event.makers[1].fee.fee_type, PredictFeeType::Collateral);
    assert_eq!(
        event.makers[0].fee.amount,
        U256::from(100000000000000001u64)
    );
    assert!(event.makers[1].hash.is_none());
    assert_eq!(event.makers[0].amount, "17.000000000000000001");
    assert_eq!(event.makers[0].price, "0.620000000000000001");
    assert_eq!(event.price_executed, "0.38");
    assert_eq!(event.executed_at, "2026-10-13T01:02:03.123456789Z");
    assert_eq!(event.settlement_id.as_deref(), Some("settlement-42"));
    let old = client
        .request_matches(&PredictMatchesQuery::default())
        .await
        .unwrap();
    assert!(old.data[0].settlement_id.is_none());
    let requests = server.requests.lock().unwrap();
    let query = params(&requests[0]);
    assert_eq!(query.len(), 6);
    assert_eq!(query["signerAddress"].to_ascii_lowercase(), OWNER);
    assert_eq!(query["minValueUsdtWei"], U256::MAX.to_string());
    assert_eq!(query["after"], NEXT);
    assert_eq!(query["isSignerMaker"], "true");
    assert_eq!(query["first"], "3");
    assert_eq!(query["marketId"], "42");
    assert_eq!(params(&requests[1]).len(), 1);
    for request in &*requests {
        assert_eq!(request.path, "/v1/orders/matches");
        assert_authenticated(request);
    }
}

#[tokio::test]
async fn foreign_orders_and_matches_are_rejected_instead_of_reconciled() {
    let mut wrong_signer = order("OPEN");
    wrong_signer["order"]["signer"] = json!(DEPOSIT);
    let mut wrong_maker = order("OPEN");
    wrong_maker["order"]["maker"] = json!(DEPOSIT);
    let mut unrelated = match_event();
    for maker in unrelated["makers"].as_array_mut().unwrap() {
        maker["signer"] = json!(DEPOSIT);
    }
    let server = server(vec![
        Reply::json(json!({"success": true, "cursor": null, "data": [wrong_signer]})),
        Reply::json(json!({"success": true, "data": wrong_maker})),
        Reply::json(json!({"success": true, "cursor": null, "data": [unrelated]})),
        Reply::json(json!({"success": true, "cursor": null, "data": [match_event()]})),
    ])
    .await;
    let client = session(&server);
    assert!(matches!(
        client.request_orders(&PredictOrdersQuery::default()).await,
        Err(Error::AccountMismatch)
    ));
    assert!(matches!(
        client.request_order(HASH.parse().unwrap()).await,
        Err(Error::AccountMismatch)
    ));
    assert!(matches!(
        client
            .request_matches(&PredictMatchesQuery::default())
            .await,
        Err(Error::AccountMismatch)
    ));
    assert!(matches!(
        client
            .request_matches(&PredictMatchesQuery {
                is_signer_maker: Some(false),
                ..Default::default()
            })
            .await,
        Err(Error::AccountMismatch)
    ));
    assert_eq!(
        params(&server.requests.lock().unwrap()[3])["isSignerMaker"],
        "false"
    );
}

#[tokio::test]
async fn invalid_filters_fail_before_network_io() {
    let server = server(vec![]).await;
    let client = session(&server);
    for first in [0, u32::MAX] {
        assert!(matches!(
            client
                .request_orders(&PredictOrdersQuery {
                    first: Some(first),
                    ..Default::default()
                })
                .await,
            Err(Error::BadRequest(_))
        ));
        assert!(matches!(
            client
                .request_matches(&PredictMatchesQuery {
                    first: Some(first),
                    ..Default::default()
                })
                .await,
            Err(Error::BadRequest(_))
        ));
        assert!(matches!(
            client
                .request_positions(&PredictPositionsQuery {
                    first: Some(first),
                    ..Default::default()
                })
                .await,
            Err(Error::BadRequest(_))
        ));
    }
    let long_cursor = "x".repeat(1025);
    let long_category = "x".repeat(257);
    for query in [
        PredictPositionsQuery {
            after: Some(&long_cursor),
            ..Default::default()
        },
        PredictPositionsQuery {
            category_id: Some(&long_category),
            ..Default::default()
        },
        PredictPositionsQuery {
            market_id: Some(u64::MAX),
            ..Default::default()
        },
    ] {
        assert!(matches!(
            client.request_positions(&query).await,
            Err(Error::BadRequest(_))
        ));
    }
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn page_envelopes_distinguish_missing_cursor_from_end_and_keep_errors_redacted() {
    let server = server(vec![
        Reply::json(json!({"success": true, "data": []})),
        Reply::json(json!({"success": true, "cursor": null, "data": []})),
        Reply::json(json!({"success": true, "cursor": 1, "data": []})),
        Reply::json(
            json!({"success": false, "cursor": {"echo": TOKEN}, "data": {"echo": API_KEY}}),
        ),
        Reply::json(json!({"success": true, "cursor": null, "data": null})),
        Reply {
            status: 401,
            body: TOKEN.into(),
            headers: vec![],
        },
        Reply {
            status: 429,
            body: API_KEY.into(),
            headers: vec![("retry-after", "7".into())],
        },
    ])
    .await;
    let client = session(&server);
    let query = PredictOrdersQuery::default();
    assert!(matches!(
        client.request_orders(&query).await,
        Err(Error::Decode {
            kind: PredictDecodeErrorKind::MissingCursor,
            ..
        })
    ));
    assert!(
        client
            .request_orders(&query)
            .await
            .unwrap()
            .cursor
            .is_none()
    );
    assert!(matches!(
        client.request_orders(&query).await,
        Err(Error::Decode {
            kind: PredictDecodeErrorKind::Data,
            ..
        })
    ));
    assert!(matches!(
        client.request_orders(&query).await,
        Err(Error::Venue { .. })
    ));
    assert!(matches!(
        client.request_orders(&query).await,
        Err(Error::Decode {
            kind: PredictDecodeErrorKind::MissingData,
            ..
        })
    ));
    let error = client.request_orders(&query).await.unwrap_err();
    assert!(matches!(error, Error::Authentication { status: 401, .. }));
    assert!(!format!("{error:?} {error}").contains(TOKEN));
    let error = client.request_orders(&query).await.unwrap_err();
    assert!(matches!(
        error,
        Error::RateLimit {
            retry_after_secs: Some(7),
            ..
        }
    ));
    assert!(!format!("{error:?} {error}").contains(API_KEY));
    assert_eq!(server.requests.lock().unwrap().len(), 7);
}

#[tokio::test]
async fn uint256_and_fee_schema_failures_do_not_echo_untrusted_values() {
    for (field, value) in [
        ("amount", json!("-1")),
        ("amount", json!("1.1")),
        ("amount", json!(1)),
    ] {
        let mut malformed = position();
        malformed[field] = value;
        let server = server(vec![Reply::json(
            json!({"success": true, "cursor": null, "data": [malformed]}),
        )])
        .await;
        assert!(matches!(
            session(&server)
                .request_positions(&PredictPositionsQuery::default())
                .await,
            Err(Error::Decode {
                kind: PredictDecodeErrorKind::Data,
                ..
            })
        ));
    }
    let mut malformed = match_event();
    malformed["makers"][0]["fee"]["type"] = json!(TOKEN);
    let server = server(vec![Reply::json(
        json!({"success": true, "cursor": null, "data": [malformed]}),
    )])
    .await;
    let error = session(&server)
        .request_matches(&PredictMatchesQuery::default())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Decode {
            kind: PredictDecodeErrorKind::Data,
            ..
        }
    ));
    assert!(!format!("{error:?} {error}").contains(TOKEN));
}

#[tokio::test]
async fn captured_match_history_is_account_scoped_and_matches_chain_gross_and_share_fee_units() {
    let fixture: Value =
        serde_json::from_str(include_str!("../fixtures/private_api/matches.json")).unwrap();
    let chain: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/share_fee_settlement.json"
    ))
    .unwrap();
    let identity: Address = "0x0000000000000000000000000000000000000022"
        .parse()
        .unwrap();
    let server = server(vec![Reply::json(fixture)]).await;
    let session = transport(&server)
        .authenticated(PredictAuthToken::new(TOKEN.into()).unwrap(), identity)
        .unwrap();
    let page = session
        .request_matches(&PredictMatchesQuery {
            first: Some(100),
            is_signer_maker: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.data.len(), 2);
    assert!(page.cursor.is_none());
    let gross: U256 = chain["gross_shares_wei"].as_str().unwrap().parse().unwrap();
    let fee: U256 = chain["fee_shares_wei"].as_str().unwrap().parse().unwrap();
    let matched = page
        .data
        .iter()
        .find(|row| row.taker.amount.parse::<U256>().unwrap() == gross)
        .unwrap();
    assert_eq!(matched.amount_filled.parse::<U256>().unwrap(), gross);
    assert_eq!(matched.taker.fee.fee_type, PredictFeeType::Shares);
    assert_eq!(matched.taker.fee.amount, fee);
    assert_eq!(matched.taker.signer, identity);
    assert!(matched.taker.hash.is_some());
    assert_eq!(page.data[0].taker.hash, page.data[1].taker.hash);
    assert_ne!(page.data[0].transaction_hash, page.data[1].transaction_hash);
    for row in &page.data {
        assert_eq!(row.taker.signer, identity);
        assert!(row.makers.iter().all(|leg| leg.signer != identity));
        assert!(row.makers.iter().all(|leg| leg.fee.amount.is_zero()));
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/v1/orders/matches");
    let query = params(&requests[0]);
    assert_eq!(query["isSignerMaker"], "false");
    assert_eq!(query["signerAddress"], identity.to_string());
}
