// Copyright (C) 2026 yfclark and contributors. Licensed under LGPL-3.0.

use super::*;

#[tokio::test]
async fn official_error_envelope_preserves_safe_diagnostics_without_echoed_credentials() {
    use alloy::primitives::U256;
    use nautilus_predict::http::error::{PredictApiErrorKind, PredictApiErrorReason};
    let captured: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/order_create_internal_error_20261015.json"
    ))
    .unwrap();
    let expiry: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/order_create_expiry_error_20261015.json"
    ))
    .unwrap();
    for (status, body, kind, reason) in [
        (
            500,
            captured["response"]["errorBody"].clone(),
            PredictApiErrorKind::InternalServerError,
            PredictApiErrorReason::OrderCreationFailed,
        ),
        (
            400,
            json!({"success":false,"code":400,"error":"bad_request","message":"invalid signature"}),
            PredictApiErrorKind::InvalidRequest,
            PredictApiErrorReason::InvalidSignature,
        ),
        (
            400,
            expiry["response"]["errorBody"].clone(),
            PredictApiErrorKind::InvalidRequest,
            PredictApiErrorReason::ExpiryTooSoon,
        ),
        (
            422,
            json!({"success":false,"code":422,"error":API_KEY,"message":TOKEN,"trace":PUBLIC_TEST_KEY}),
            PredictApiErrorKind::Unknown,
            PredictApiErrorReason::Unknown,
        ),
    ] {
        let server = server(vec![Reply {
            status,
            body: body.to_string(),
            headers: vec![],
        }])
        .await;
        let signed = limit_signed_order(None, PredictEnvironment::BnbMainnet, false);
        let session = transport(&server)
            .authenticated(
                PredictAuthToken::new(TOKEN.into()).unwrap(),
                OWNER.parse().unwrap(),
            )
            .unwrap();
        let error = session
            .request_create_limit_order(&signed, U256::from(620_000_000_000_000_000u64), true)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, Error::Api {status:actual,code:Some(code),kind:actual_kind,reason:actual_reason,..}
            if *actual==status && *code==i64::from(status) && *actual_kind==kind && *actual_reason==reason)
        );
        let diagnostic = format!("{error:?} {error}");
        for secret in [
            API_KEY,
            TOKEN,
            PUBLIC_TEST_KEY,
            signed.signature().expose_secret(),
        ] {
            assert!(!diagnostic.contains(secret));
        }
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

fn limit_signed_order(
    deposit: Option<&str>,
    environment: PredictEnvironment,
    sell: bool,
) -> nautilus_predict::http::order_models::PredictSignedOrder {
    use alloy::primitives::U256;
    use nautilus_predict::{
        common::{contracts::PredictExchangeType, enums::PredictOrderSide},
        http::order_models::PredictOrder,
        signing::PredictOrderSigner,
    };
    let wallet =
        Arc::new(PredictAuthSigner::new(PUBLIC_TEST_KEY.into(), environment, deposit).unwrap());
    let vectors: Value =
        serde_json::from_str(include_str!("../fixtures/order_vectors.json")).unwrap();
    let mut order: PredictOrder =
        serde_json::from_value(vectors["vectors"][0]["order"].clone()).unwrap();
    order.maker = wallet.account_address();
    order.signer = wallet.account_address();
    order.maker_amount = U256::from(62_000_000_000_000_000_000u128);
    order.taker_amount = U256::from(100_000_000_000_000_000_000u128);
    if sell {
        std::mem::swap(&mut order.maker_amount, &mut order.taker_amount);
        order.side = PredictOrderSide::Sell;
    }
    PredictOrderSigner::new(wallet)
        .sign_order(&order, PredictExchangeType::Standard)
        .unwrap()
}

#[tokio::test]
async fn limit_submission_keeps_signed_payload_exact_and_binds_receipt_to_local_hash() {
    use alloy::primitives::U256;
    let fixtures: Value =
        serde_json::from_str(include_str!("../fixtures/limit_requests.json")).unwrap();
    let vectors = fixtures["vectors"].as_array().unwrap();
    assert_eq!(vectors.len(), 16);
    for pair in vectors.as_chunks::<2>().0 {
        assert_eq!(pair[0]["body"]["data"]["isPostOnly"], true);
        assert_eq!(pair[1]["body"]["data"]["isPostOnly"], false);
        assert_eq!(
            pair[0]["body"]["data"]["order"],
            pair[1]["body"]["data"]["order"]
        );
    }
    for expected in vectors {
        let environment = match expected["chain_id"].as_u64().unwrap() {
            56 => PredictEnvironment::BnbMainnet,
            97 => PredictEnvironment::BnbTestnet,
            _ => panic!("unexpected SDK fixture chain"),
        };
        let deposit = expected["predict_account"].as_str();
        let body = &expected["body"]["data"];
        let signed = limit_signed_order(deposit, environment, body["order"]["side"] == 1);
        let mut reply = Reply::json(
            json!({"success":true,"data":{"code":"opaque-fixture-code","orderId":"12345","orderHash":signed.hash(),"removalLockedUntil":"2026-10-13T10:00:00Z"}}),
        );
        reply.status = 201;
        let server = server(vec![reply]).await;
        let client = http_client(environment, Some(API_KEY), Some(&server.url), 5).unwrap();
        let session = client
            .authenticated(
                PredictAuthToken::new(TOKEN.into()).unwrap(),
                signed.order().maker,
            )
            .unwrap();
        let receipt = session
            .request_create_limit_order(
                &signed,
                U256::from(620_000_000_000_000_000u64),
                body["isPostOnly"].as_bool().unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(receipt.order_hash, signed.hash());
        assert_eq!(receipt.order_id, U256::from(12345));
        assert_eq!(receipt.code, "opaque-fixture-code");
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, Method::POST);
        assert_eq!(requests[0].path, "/v1/orders");
        assert_eq!(
            requests[0].headers["authorization"],
            format!("Bearer {TOKEN}")
        );
        assert_eq!(requests[0].headers["x-api-key"], API_KEY);
        assert_eq!(requests[0].body, expected["body"]);
    }
}

#[tokio::test]
async fn invalid_limit_context_is_rejected_before_any_network_request() {
    use alloy::primitives::U256;
    let server = server(vec![]).await;
    let signed = limit_signed_order(None, PredictEnvironment::BnbMainnet, false);
    let session = transport(&server)
        .authenticated(
            PredictAuthToken::new(TOKEN.into()).unwrap(),
            OWNER.parse().unwrap(),
        )
        .unwrap();
    for price in [
        U256::ZERO,
        U256::from(619_000_000_000_000_000u64),
        U256::MAX,
    ] {
        assert!(matches!(
            session
                .request_create_limit_order(&signed, price, false)
                .await,
            Err(Error::BadRequest(_))
        ));
    }
    for signed in [
        limit_signed_order(Some(DEPOSIT), PredictEnvironment::BnbMainnet, false),
        limit_signed_order(None, PredictEnvironment::BnbTestnet, false),
    ] {
        assert!(matches!(
            session
                .request_create_limit_order(&signed, U256::from(620_000_000_000_000_000u64), false)
                .await,
            Err(Error::BadRequest(_))
        ));
    }
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn malformed_create_acknowledgements_and_server_failures_are_never_retried() {
    use alloy::primitives::U256;
    let signed = limit_signed_order(None, PredictEnvironment::BnbMainnet, false);
    let valid = json!({"success":true,"data":{"code":"opaque-code","orderId":"12345","orderHash":signed.hash()}});
    for case in 0..8 {
        let mut body = valid.clone();
        let mut status = 201;
        match case {
            0 => body["data"]["orderHash"] = json!(format!("0x{}", "22".repeat(32))),
            1 => body["data"]["orderId"] = json!("1e18"),
            2 => body["data"]["code"] = json!(""),
            3 => {
                body.as_object_mut().unwrap().remove("data");
            }
            4 => body["success"] = json!(false),
            5 => status = 500,
            6 => status = 200,
            _ => body["data"]["orderId"] = json!(12),
        }
        let server = server(vec![Reply {
            status,
            body: body.to_string(),
            headers: vec![],
        }])
        .await;
        let session = transport(&server)
            .authenticated(
                PredictAuthToken::new(TOKEN.into()).unwrap(),
                OWNER.parse().unwrap(),
            )
            .unwrap();
        let error = session
            .request_create_limit_order(&signed, U256::from(620_000_000_000_000_000u64), false)
            .await
            .unwrap_err();
        match case {
            4 => assert!(matches!(&error, Error::Venue { .. })),
            5 => assert!(matches!(&error, Error::Http { status: 500, .. })),
            _ => match &error {
                Error::Decode {
                    status: actual_status,
                    ..
                } => assert_eq!(*actual_status, Some(status)),
                other => panic!("expected decoding error, received {other:?}"),
            },
        }
        let diagnostic = format!("{error:?} {error}");
        for secret in [API_KEY, TOKEN, signed.signature().expose_secret()] {
            assert!(!diagnostic.contains(secret));
        }
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn removal_uses_decimal_ids_and_checks_the_entire_removed_noop_partition() {
    use alloy::primitives::U256;
    let server = server(vec![Reply::json(
        json!({"success":true,"removed":["12345"],"noop":[U256::MAX.to_string()]}),
    )])
    .await;
    let session = transport(&server)
        .authenticated(
            PredictAuthToken::new(TOKEN.into()).unwrap(),
            OWNER.parse().unwrap(),
        )
        .unwrap();
    let receipt = session
        .request_remove_orders(&[U256::from(12345), U256::MAX])
        .await
        .unwrap();
    assert_eq!(receipt.removed, ["12345"]);
    assert_eq!(receipt.noop, [U256::MAX.to_string()]);
    assert!(receipt.rejected.is_empty());
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, Method::POST);
    assert_eq!(requests[0].path, "/v1/orders/remove");
    assert_eq!(
        requests[0].body,
        json!({"data":{"ids":["12345",U256::MAX.to_string()]}})
    );
    assert_eq!(
        requests[0].headers["authorization"],
        format!("Bearer {TOKEN}")
    );
}

#[tokio::test]
async fn recorded_single_and_batch_removal_receipts_preserve_real_rest_ids() {
    use alloy::primitives::U256;
    for captured in [
        include_str!(
            "../fixtures/private_api/exit_only_restart_20261017T053156Z/http_responses.json"
        ),
        include_str!(
            "../fixtures/private_api/live_resting_http-batch_20261016T160926Z/http_responses.json"
        ),
    ] {
        let captured: Value = serde_json::from_str(captured).unwrap();
        let removal = captured["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["path"] == "/v1/orders/remove")
            .unwrap();
        assert_eq!(removal["status"], 200);
        let expected = &removal["responseBody"];
        let ids = removal["requestIds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|id| id.as_str().unwrap().parse::<U256>().unwrap())
            .collect::<Vec<_>>();
        let server = server(vec![Reply::json(expected.clone())]).await;
        let session = transport(&server)
            .authenticated(
                PredictAuthToken::new(TOKEN.into()).unwrap(),
                OWNER.parse().unwrap(),
            )
            .unwrap();
        let receipt = session.request_remove_orders(&ids).await.unwrap();
        assert_eq!(json!(receipt.removed), expected["removed"]);
        assert!(receipt.noop.is_empty());
        assert!(receipt.rejected.is_empty());
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].path, "/v1/orders/remove");
        assert_eq!(
            requests[0].body,
            json!({"data":{"ids":removal["requestIds"]}})
        );
    }
}

#[tokio::test]
async fn removal_preserves_each_removed_noop_and_rejected_partition() {
    use alloy::primitives::U256;
    let server = server(vec![Reply::json(
        json!({"success":true,"removed":["1"],"noop":["2"],"rejected":["3"]}),
    )])
    .await;
    let session = transport(&server)
        .authenticated(
            PredictAuthToken::new(TOKEN.into()).unwrap(),
            OWNER.parse().unwrap(),
        )
        .unwrap();
    let receipt = session
        .request_remove_orders(&[U256::from(1), U256::from(2), U256::from(3)])
        .await
        .unwrap();
    assert_eq!(receipt.removed, ["1"]);
    assert_eq!(receipt.noop, ["2"]);
    assert_eq!(receipt.rejected, ["3"]);
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn incomplete_duplicate_or_foreign_removal_receipts_fail_without_retrying() {
    use alloy::primitives::U256;
    for body in [
        json!({"success":true,"removed":[],"noop":[]}),
        json!({"success":true,"removed":["12345"],"noop":[],"rejected":["12345"]}),
        json!({"success":true,"removed":[],"noop":[],"rejected":["99999"]}),
        json!({"success":true,"removed":[],"noop":[],"rejected":["012345"]}),
        json!({"success":true,"removed":[],"noop":[],"rejected":[12345]}),
        json!({"success":true,"removed":["12345"],"noop":["12345"]}),
        json!({"success":true,"removed":["12345","99999"],"noop":[]}),
        json!({"success":true,"removed":["012345"],"noop":[]}),
        json!({"success":true,"removed":[12345],"noop":[]}),
        json!({"success":false,"removed":[],"noop":[]}),
        json!({"success":false,"removed":["12345"],"noop":[]}),
        json!({"success":true,"removed":["12345"]}),
        json!({"success":true,"data":{"removed":["12345"],"noop":[]}}),
    ] {
        let server = server(vec![Reply::json(body)]).await;
        let session = transport(&server)
            .authenticated(
                PredictAuthToken::new(TOKEN.into()).unwrap(),
                OWNER.parse().unwrap(),
            )
            .unwrap();
        assert!(
            session
                .request_remove_orders(&[U256::from(12345)])
                .await
                .is_err()
        );
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
    let server = server(vec![]).await;
    let session = transport(&server)
        .authenticated(
            PredictAuthToken::new(TOKEN.into()).unwrap(),
            OWNER.parse().unwrap(),
        )
        .unwrap();
    for ids in [
        vec![],
        vec![U256::from(12345); 2],
        (0..101).map(U256::from).collect(),
    ] {
        assert!(matches!(
            session.request_remove_orders(&ids).await,
            Err(Error::BadRequest(_))
        ));
    }
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn post_redirects_cannot_forward_wallet_signatures_or_session_credentials() {
    use alloy::primitives::U256;
    for create in [false, true] {
        let destination = server(vec![]).await;
        let location = format!("{}/untrusted-destination", destination.url);
        let source = server(vec![Reply {
            status: 307,
            body: String::new(),
            headers: vec![("Location", location)],
        }])
        .await;
        let signed = limit_signed_order(None, PredictEnvironment::BnbMainnet, false);
        let session = transport(&source)
            .authenticated(
                PredictAuthToken::new(TOKEN.into()).unwrap(),
                OWNER.parse().unwrap(),
            )
            .unwrap();
        let error = if create {
            session
                .request_create_limit_order(&signed, U256::from(620_000_000_000_000_000u64), false)
                .await
                .unwrap_err()
        } else {
            session
                .request_remove_orders(&[U256::from(12345)])
                .await
                .unwrap_err()
        };
        let diagnostic = format!("{error:?} {error}");
        for secret in [API_KEY, TOKEN, signed.signature().expose_secret()] {
            assert!(!diagnostic.contains(secret));
        }
        assert_eq!(source.requests.lock().unwrap().len(), 1);
        assert!(destination.requests.lock().unwrap().is_empty());
    }
}
