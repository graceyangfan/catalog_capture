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

#[tokio::test]
async fn captured_daily_and_hourly_markets_use_calendar_categories_and_exact_windows() {
    for (source, interval, now, start, end, slug) in [
        (
            include_str!("../test_data/market_3759734_daily.json"),
            86_400,
            "2026-10-26T10:00:00Z",
            "2026-10-25T16:00:00Z",
            "2026-10-26T16:00:00Z",
            "bitcoin-up-or-down-on-october-26-2026",
        ),
        (
            include_str!("../test_data/market_3737148_hourly.json"),
            3600,
            "2026-10-26T10:30:00Z",
            "2026-10-26T10:00:00Z",
            "2026-10-26T11:00:00Z",
            "bitcoin-up-or-down-october-26-2026-6am-et",
        ),
    ] {
        let fixture: Value = serde_json::from_str(source).unwrap();
        let expected_market_id = fixture["data"]["id"].as_u64().unwrap();
        let requests = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen = Arc::clone(&requests);
        let app = Router::new().fallback(move |uri: Uri| {
            let row = fixture["data"].clone();
            let seen = Arc::clone(&seen);
            async move {
                seen.lock().unwrap().push(uri.path().to_string());
                json!({"success":true,"data":{"markets":[row]}}).to_string()
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = PredictHttpClient::new(
            "fixture-key",
            Some(&format!("http://{}", listener.local_addr().unwrap())),
            5,
        )
        .unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let nanos = |value| {
            chrono::DateTime::parse_from_rfc3339(value)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap() as u64
        };
        let selected = discover_crypto_up_down_market(
            &client,
            &PredictCryptoUpDownSelector {
                price_feed_symbol: "BTCUSDT".into(),
                title_asset: "Bitcoin".into(),
                interval_secs: interval,
            },
            nanos(now),
        )
        .await
        .unwrap();
        assert_eq!(selected.market_id, expected_market_id);
        assert_eq!(selected.window.start_ns, nanos(start));
        assert_eq!(selected.window.end_ns, nanos(end));
        let paths = requests.lock().unwrap();
        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0], format!("/v1/categories/{slug}"));
        assert!(paths.iter().all(|path| path.starts_with("/v1/categories/")));
        server.abort();
    }
}

#[tokio::test]
async fn public_requests_preserve_query_values_and_one_category_segment() {
    use axum::{extract::Path, routing::get};
    use std::collections::HashMap;

    fn parameters(uri: &Uri) -> HashMap<String, String> {
        nautilus_network::http::Url::parse(&format!("http://localhost{uri}"))
            .unwrap()
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect()
    }
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let search = Arc::clone(&requests);
    let categories = Arc::clone(&requests);
    let pages = Arc::clone(&requests);
    let cursor = "next /+?&=%汉字";
    let app = Router::new()
        .route(
            "/v1/search",
            get(move |uri: Uri| {
                let query = parameters(&uri);
                search.lock().unwrap().push(json!({"search":query}));
                async { json!({"success":true,"data":{"markets":[]}}).to_string() }
            }),
        )
        .route(
            "/v1/categories/{slug}",
            get(move |Path(slug): Path<String>| {
                categories.lock().unwrap().push(json!({"slug":slug}));
                async { json!({"success":true,"data":{"markets":[]}}).to_string() }
            }),
        )
        .route(
            "/v1/markets",
            get(move |uri: Uri| {
                let query = parameters(&uri);
                pages.lock().unwrap().push(json!({"page":query}));
                async move {
                    let mut fixture: Value =
                        serde_json::from_str(include_str!("../test_data/market_2859346.json"))
                            .unwrap();
                    let next = if query["after"].is_empty() {
                        Some(cursor)
                    } else {
                        assert_eq!(query["after"], cursor);
                        fixture["data"]["id"] = json!(2_859_347);
                        None
                    };
                    json!({"success":true,"cursor":next,"data":[fixture["data"].clone()]})
                        .to_string()
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = PredictHttpClient::new(
        "fixture-key",
        Some(&format!("http://{}", listener.local_addr().unwrap())),
        5,
    )
    .unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    assert!(
        client
            .request_search_markets(" 比特币 & a/b?x=1+2# % ", 200)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        client
            .request_category_markets(" btc /?#+&=汉字 ")
            .await
            .unwrap()
            .is_empty()
    );
    let markets = client.request_open_markets().await.unwrap();
    assert_eq!(
        markets.iter().map(|market| market.id).collect::<Vec<_>>(),
        [2_859_346, 2_859_347]
    );
    assert_eq!(
        *requests.lock().unwrap(),
        vec![
            json!({"search":{"query":"比特币 & a/b?x=1+2# %","includeResolved":"false","limit":"100"}}),
            json!({"slug":"btc /?#+&=汉字"}),
            json!({"page":{"first":"100","status":"OPEN","after":""}}),
            json!({"page":{"first":"100","status":"OPEN","after":cursor}}),
        ]
    );
    server.abort();
}

#[tokio::test]
async fn public_market_pagination_rejects_a_repeated_cursor_without_extra_requests() {
    let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = Arc::clone(&requests);
    let app = Router::new().fallback(move || {
        seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        async { json!({"success":true,"cursor":"repeated","data":[]}).to_string() }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = PredictHttpClient::new(
        "fixture-key",
        Some(&format!("http://{}", listener.local_addr().unwrap())),
        5,
    )
    .unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let error = client.request_open_markets().await.unwrap_err();
    assert!(error.to_string().contains("repeated a cursor"));
    assert_eq!(requests.load(std::sync::atomic::Ordering::Relaxed), 2);
    server.abort();
}
