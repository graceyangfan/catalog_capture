// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Canonical and legacy feed selectors use the same REST category discovery.
#![cfg(feature = "live")]

use std::sync::{Arc, Mutex};

use axum::{Router, extract::State, http::Uri};
use nautilus_predict::{
    PredictCryptoUpDownSelector, PredictHttpClient, discover_crypto_up_down_market,
};
use serde_json::{Value, json};

async fn category(State(requests): State<Arc<Mutex<Vec<String>>>>, uri: Uri) -> String {
    requests.lock().unwrap().push(uri.path().to_string());
    let fixture: Value =
        serde_json::from_str(include_str!("../test_data/market_2859346.json")).unwrap();
    json!({"success": true, "data": {"markets": [fixture["data"].clone()]}}).to_string()
}

#[tokio::test]
async fn canonical_and_legacy_selectors_fetch_categories_without_search() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .fallback(category)
        .with_state(requests.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = PredictHttpClient::new("fixture-key", Some(&origin), 5).unwrap();
    // October 3 2026, 22:00:01 UTC: inside the fixture's 6PM–6:05PM ET window.
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-03T22:00:01Z")
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap() as u64;
    for feed in ["BTCUSDT", "BTC/USDT", " btc/usdt "] {
        let selected = discover_crypto_up_down_market(
            &client,
            &PredictCryptoUpDownSelector {
                price_feed_symbol: feed.into(),
                title_asset: "Bitcoin".into(),
                interval_secs: 300,
            },
            now,
        )
        .await
        .unwrap();
        assert_eq!(selected.market_id, 2_859_346);
        assert!(selected.window.start_ns <= now && now < selected.window.end_ns);
    }
    let start = now / 1_000_000_000 / 300 * 300;
    let expected = [
        format!("/v1/categories/btc-updown-5m-{start}"),
        format!("/v1/categories/btc-updown-5m-{}", start + 300),
    ];
    let actual = requests.lock().unwrap().clone();
    assert_eq!(actual.len(), 6);
    for pair in actual.as_chunks::<2>().0 {
        assert_eq!(pair, expected.as_slice());
    }
    server.abort();
}
