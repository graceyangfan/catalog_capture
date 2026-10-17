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
        assert_eq!(requests[0].body, vector["body"]);
        assert_eq!(requests[0].method, Method::POST);
        assert_eq!(requests[0].path, "/v1/orders");
    }
}
