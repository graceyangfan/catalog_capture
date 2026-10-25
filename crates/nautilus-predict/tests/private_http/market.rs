// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
use alloy::primitives::U256;
use nautilus_predict::{
    common::{
        contracts::PredictExchangeType, enums::PredictOrderSide, market::PredictMarketOrderAmounts,
    },
    http::order_models::PredictOrder,
    signing::PredictOrderSigner,
};

use super::*;

#[tokio::test]
async fn market_transport_preserves_sdk_request_and_never_retries_ambiguous_create() {
    let fixture: Value =
        serde_json::from_str(include_str!("../fixtures/market_requests.json")).unwrap();
    let vector = &fixture["vectors"][0];
    let order: PredictOrder =
        serde_json::from_value(vector["body"]["data"]["order"].clone()).unwrap();
    let signed = PredictOrderSigner::new(Arc::new(
        PredictAuthSigner::new(PUBLIC_TEST_KEY.into(), PredictEnvironment::BnbMainnet, None)
            .unwrap(),
    ))
    .sign_order(&order, PredictExchangeType::Standard)
    .unwrap();
    let scale = U256::from(1_000_000_000_000_000_000u64);
    let amounts = PredictMarketOrderAmounts::from_levels(
        PredictOrderSide::Buy,
        scale * U256::from(100),
        &[(scale / U256::from(100), scale * U256::from(200))],
        0,
    )
    .unwrap();
    for status in [201, 500] {
        let server = server(vec![Reply {status,
            body: if status == 201 {json!({"success":true,"data":{"code":"OK","orderId":"12345","orderHash":signed.hash()}}).to_string()}
            else {json!({"success":false,"code":500,"error":"internal_server_error","message":"failed to create order"}).to_string()},
            headers:vec![]}]).await;
        let session = transport(&server)
            .authenticated(PredictAuthToken::new(TOKEN.into()).unwrap(), order.maker)
            .unwrap();
        let result = session.request_create_market_order(&signed, &amounts).await;
        if status == 201 {
            assert_eq!(result.unwrap().order_hash, signed.hash());
        } else {
            assert!(matches!(result, Err(Error::Api { status: 500, .. })));
        }
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        // Saved vectors retain the SDK's signed payload and legacy hand-built
        // envelope. Current REST fields have explicit types and mode.
        let mut expected = vector["body"].clone();
        expected["data"]["slippageBps"] = "0".into();
        expected["data"]["isMinAmountOut"] = false.into();
        assert_eq!(requests[0].body, expected);
        assert_eq!(requests[0].method, Method::POST);
        assert_eq!(requests[0].path, "/v1/orders");
    }
}

#[tokio::test]
async fn market_fok_probe_preserves_signed_terms_and_does_not_retry() {
    use nautilus_predict::http::command_models::PredictCreateOrderRequest;
    let fixture: Value =
        serde_json::from_str(include_str!("../fixtures/market_requests.json")).unwrap();
    let scale = U256::from(1_000_000_000_000_000_000u64);
    for side in [PredictOrderSide::Buy, PredictOrderSide::Sell] {
        let amounts = PredictMarketOrderAmounts::from_price(
            side,
            scale * U256::from(100),
            scale / U256::from(100),
            0,
        )
        .unwrap();
        let mut order: PredictOrder =
            serde_json::from_value(fixture["vectors"][0]["body"]["data"]["order"].clone()).unwrap();
        order.side = side;
        order.maker_amount = amounts.maker_amount;
        order.taker_amount = amounts.taker_amount;
        let signed = PredictOrderSigner::new(Arc::new(
            PredictAuthSigner::new(PUBLIC_TEST_KEY.into(), PredictEnvironment::BnbTestnet, None)
                .unwrap(),
        ))
        .sign_order(&order, PredictExchangeType::Standard)
        .unwrap();
        let ordinary =
            serde_json::to_value(PredictCreateOrderRequest::market(&signed, &amounts).unwrap())
                .unwrap();
        assert!(ordinary["data"].get("isFillOrKill").is_none());
        for status in [201, 400, 500] {
            let body = if status == 201 {
                json!({"success":true,"data":{"code":"opaque","orderId":"12345","orderHash":signed.hash()}})
            } else {
                json!({"success":false,"code":status,"error":"bad_request","message":"probe rejection"})
            };
            let server = server(vec![Reply {
                status,
                body: body.to_string(),
                headers: vec![],
            }])
            .await;
            let client = http_client(
                PredictEnvironment::BnbTestnet,
                Some(API_KEY),
                Some(&server.url),
                5,
            )
            .unwrap();
            let session = client
                .authenticated(PredictAuthToken::new(TOKEN.into()).unwrap(), order.maker)
                .unwrap();
            let result = session
                .request_create_market_fok_order(&signed, &amounts)
                .await;
            assert_eq!(result.is_ok(), status == 201);
            let requests = server.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            let mut expected = ordinary.clone();
            expected["data"]["isFillOrKill"] = json!(true);
            assert_eq!(requests[0].body, expected);
            assert_eq!(requests[0].path, "/v1/orders");
        }
        let server = server(vec![]).await;
        let session = transport(&server)
            .authenticated(PredictAuthToken::new(TOKEN.into()).unwrap(), order.maker)
            .unwrap();
        assert!(matches!(
            session
                .request_create_market_fok_order(&signed, &amounts)
                .await,
            Err(Error::BadRequest(_))
        ));
        let mainnet = PredictOrderSigner::new(Arc::new(
            PredictAuthSigner::new(PUBLIC_TEST_KEY.into(), PredictEnvironment::BnbMainnet, None)
                .unwrap(),
        ))
        .sign_order(&order, PredictExchangeType::Standard)
        .unwrap();
        assert!(PredictCreateOrderRequest::market_fok(&mainnet, &amounts).is_ok());
        assert!(server.requests.lock().unwrap().is_empty());
    }
}
