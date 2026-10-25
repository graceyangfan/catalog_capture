// -------------------------------------------------------------------------------------------------
// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
// See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Actual bounded REST history; later observations do not recreate wallet callbacks.

use nautilus_predict::http::query::{
    PredictMatchesQuery, PredictOrderStatusFilter, PredictOrdersQuery,
};

use super::*;

fn capture() -> Value {
    serde_json::from_str(include_str!(
        "../fixtures/private_api/live_order_lifecycle_survey_20261025.json"
    ))
    .unwrap()
}

#[tokio::test]
async fn recorded_lifecycle_order_pages_and_lookups_preserve_venue_terminals() {
    let saved = capture();
    let events = saved["events"].as_array().unwrap();
    let own = saved["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event["path"] == "/v1/account")
        .unwrap()["responseBody"]["data"]["address"]
        .as_str()
        .unwrap();
    let reads = events
        .iter()
        .filter(|event| {
            event["path"] == "/v1/orders"
                || event["path"].as_str().unwrap().starts_with("/v1/orders/0x")
        })
        .collect::<Vec<_>>();
    let server = server(
        reads
            .iter()
            .map(|event| Reply::json(event["responseBody"].clone()))
            .collect(),
    )
    .await;
    let session = transport(&server)
        .authenticated(
            PredictAuthToken::new(TOKEN.into()).unwrap(),
            own.parse().unwrap(),
        )
        .unwrap();
    let mut lookups = 0;
    let mut atomic = 0;
    let mut canceled = 0;
    for event in reads {
        assert_eq!(event["status"], 200);
        if event["path"] == "/v1/orders" {
            let first = event["query"]["first"][0]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            let after = event["query"]["after"][0].as_str();
            let status = event["query"]["status"][0]
                .as_str()
                .map(|value| match value {
                    "OPEN" => PredictOrderStatusFilter::Open,
                    "FILLED" => PredictOrderStatusFilter::Filled,
                    _ => panic!("unsupported filter in original recording"),
                });
            let page = session
                .request_orders(&PredictOrdersQuery {
                    first: Some(first),
                    after,
                    status,
                })
                .await
                .unwrap();
            assert_eq!(
                page.cursor.as_deref(),
                event["responseBody"]["cursor"].as_str()
            );
            assert_eq!(
                page.data.len(),
                event["responseBody"]["data"].as_array().unwrap().len()
            );
            for (row, raw) in page
                .data
                .iter()
                .zip(event["responseBody"]["data"].as_array().unwrap())
            {
                assert_eq!(row.amount_filled, raw["amountFilled"].as_str().unwrap());
                assert_eq!(format!("{:?}", row.status).to_uppercase(), raw["status"]);
            }
        } else {
            let raw = &event["responseBody"]["data"];
            let hash = raw["order"]["hash"].as_str().unwrap().parse().unwrap();
            let row = session.request_order(hash).await.unwrap();
            assert_eq!(event["officialSdkHashVerifiedBeforeRedaction"], true);
            assert_eq!(row.amount_filled, raw["amountFilled"].as_str().unwrap());
            assert_eq!(row.amount, raw["amount"].as_str().unwrap());
            lookups += 1;
            if raw["status"] == "CANCELLED" {
                canceled += 1;
                assert_eq!(
                    row.status,
                    nautilus_predict::http::account_models::PredictOrderStatus::Cancelled
                );
                assert_eq!(row.amount_filled, "0");
            } else {
                assert_eq!(
                    row.status,
                    nautilus_predict::http::account_models::PredictOrderStatus::Filled
                );
                let amount: alloy::primitives::U256 = row.amount.parse().unwrap();
                let filled: alloy::primitives::U256 = row.amount_filled.parse().unwrap();
                if filled < amount {
                    atomic += 1;
                    assert_eq!(amount - filled, alloy::primitives::U256::from(1));
                }
            }
        }
        let last = server.requests.lock().unwrap();
        let last = last.last().unwrap();
        assert_eq!(last.method, Method::GET);
        assert_eq!(last.path, event["path"].as_str().unwrap());
        let url = nautilus_network::http::Url::parse(&format!(
            "http://localhost/?{}",
            last.query.as_deref().unwrap_or_default()
        ))
        .unwrap();
        let actual = url
            .query_pairs()
            .into_owned()
            .collect::<std::collections::HashMap<_, _>>();
        let expected = event["query"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), value[0].as_str().unwrap().to_owned()))
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(actual, expected);
    }
    assert_eq!((lookups, canceled, atomic), (24, 1, 2));
    assert_eq!(saved["orderWrites"], 0);
    assert_eq!(saved["chainWrites"], 0);
}

#[tokio::test]
async fn recorded_lifecycle_maker_and_taker_filters_preserve_each_owned_leg() {
    let saved = capture();
    let reads = saved["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["path"] == "/v1/orders/matches")
        .collect::<Vec<_>>();
    let server = server(
        reads
            .iter()
            .map(|event| Reply::json(event["responseBody"].clone()))
            .collect(),
    )
    .await;
    let own = reads[0]["query"]["signerAddress"][0].as_str().unwrap();
    let session = transport(&server)
        .authenticated(
            PredictAuthToken::new(TOKEN.into()).unwrap(),
            own.parse().unwrap(),
        )
        .unwrap();
    let mut counts = [0, 0, 0];
    let mut zero_maker_fees = [0, 0];
    for event in reads {
        let role = event["query"]["isSignerMaker"][0]
            .as_str()
            .map(|value| value.parse().unwrap());
        let page = session
            .request_matches(&PredictMatchesQuery {
                first: Some(8),
                after: event["query"]["after"][0].as_str(),
                is_signer_maker: role,
                ..Default::default()
            })
            .await
            .unwrap();
        let index = match role {
            None => 0,
            Some(true) => 1,
            Some(false) => 2,
        };
        counts[index] += page.data.len();
        assert_eq!(
            page.cursor.as_deref(),
            event["responseBody"]["cursor"].as_str()
        );
        for (row, raw) in page
            .data
            .iter()
            .zip(event["responseBody"]["data"].as_array().unwrap())
        {
            for (leg, wire) in std::iter::once((&row.taker, &raw["taker"]))
                .chain(row.makers.iter().zip(raw["makers"].as_array().unwrap()))
            {
                assert_eq!(leg.amount, wire["amount"].as_str().unwrap());
                assert_eq!(leg.price, wire["price"].as_str().unwrap());
                assert_eq!(
                    leg.fee.amount.to_string(),
                    wire["fee"]["amount"].as_str().unwrap()
                );
                assert!(leg.hash.is_some());
                if role == Some(true) && leg.signer == own.parse::<Address>().unwrap() {
                    assert!(leg.fee.amount.is_zero());
                    match leg.fee.fee_type {
                        nautilus_predict::http::account_models::PredictFeeType::Shares => {
                            zero_maker_fees[0] += 1
                        }
                        nautilus_predict::http::account_models::PredictFeeType::Collateral => {
                            zero_maker_fees[1] += 1
                        }
                    }
                }
            }
        }
        let requests = server.requests.lock().unwrap();
        let request = requests.last().unwrap();
        let url = nautilus_network::http::Url::parse(&format!(
            "http://localhost/?{}",
            request.query.as_deref().unwrap()
        ))
        .unwrap();
        let actual = url
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(actual["signerAddress"], own);
        assert_eq!(
            actual.get("isSignerMaker").map(|v| v.as_ref()),
            event["query"]["isSignerMaker"][0].as_str()
        );
        assert_eq!(
            actual.get("after").map(|v| v.as_ref()),
            event["query"]["after"][0].as_str()
        );
    }
    assert_eq!(counts, [16, 9, 16]);
    assert_eq!(zero_maker_fees, [4, 5]);
}
