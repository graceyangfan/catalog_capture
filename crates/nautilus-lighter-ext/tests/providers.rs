// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use axum::{
    Router,
    extract::{Query, State},
    routing::get,
};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_lighter_ext::{LighterBinaryDataClientConfig, http::LighterBinaryHttpClient};
use serde_json::{Value, json};
#[derive(Clone)]
struct Fixture {
    events: Value,
    points: Value,
    boundary_calls: Arc<AtomicUsize>,
}
async fn events_handler(
    Query(query): Query<HashMap<String, String>>,
    State(f): State<Fixture>,
) -> String {
    assert_eq!(query["market_ids"], "4255");
    assert_eq!(query["limit"], "50");
    f.events.to_string()
}
async fn boundary(
    Query(query): Query<HashMap<String, String>>,
    State(f): State<Fixture>,
) -> String {
    f.boundary_calls.fetch_add(1, Ordering::AcqRel);
    assert_eq!(query["symbol"], "eth");
    assert_eq!(query["duration"], "15m");
    assert_eq!(query["resolution"], "1s");
    assert_eq!(query["start_timestamp"], "1792480500");
    assert_eq!(query["end_timestamp"], "1792480500");
    f.points.to_string()
}
#[tokio::test]
async fn provider_rejects_approximate_boundary_errors_and_unsupported_precision() {
    for case in [
        "valid",
        "wrong_second",
        "wrong_feed",
        "missing",
        "precision",
        "future",
        "api_error",
    ] {
        let mut events: Value =
            serde_json::from_str(include_str!("../test_data/event.json")).unwrap();
        let frame: Value =
            serde_json::from_str(include_str!("../test_data/subscribed_up_down_candle.json"))
                .unwrap();
        let mut point = frame["candles"][0].clone();
        point["timestamp"] = json!(1792480500);
        match case {
            "wrong_second" => point["timestamp"] = json!(1792480501),
            "wrong_feed" => point["feed_id"] = json!("another-feed"),
            "precision" => {
                events["events"][0]["order_books"][0]["supported_price_decimals"] = json!(255)
            }
            "future" => {
                let now = get_atomic_clock_realtime().get_time_ms();
                events["events"][0]["start_at"] = json!((now / 1000 + 60) * 1000);
                events["events"][0]["end_at"] = json!((now / 1000 + 960) * 1000);
            }
            "api_error" => events = json!({"code":20001,"message":"invalid param"}),
            _ => {}
        }
        let f = Fixture {
            events,
            points: json!({"code":200,"candles":if case=="missing"{vec![]}else{vec![point]}}),
            boundary_calls: Arc::new(AtomicUsize::new(0)),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = Router::new()
            .route("/events", get(events_handler))
            .route("/upDownCandles", get(boundary))
            .with_state(f.clone());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let config = LighterBinaryDataClientConfig {
            market_ids: vec![4255],
            base_url_http: Some(format!("http://{addr}")),
            ..Default::default()
        };
        let result = LighterBinaryHttpClient::new(&config)
            .unwrap()
            .load_markets(&[4255], get_atomic_clock_realtime().get_time_ns())
            .await;
        assert_eq!(result.is_ok(), case == "valid", "case {case}: {result:?}");
        if case == "valid" {
            assert_eq!(f.boundary_calls.load(Ordering::Acquire), 1);
        }
        if ["future", "api_error"].contains(&case) {
            assert_eq!(f.boundary_calls.load(Ordering::Acquire), 0);
        }
        server.abort();
    }
}
#[test]
fn fixed_market_limits_are_validated_before_transport_setup() {
    assert!(
        LighterBinaryDataClientConfig::default().validate().is_ok(),
        "empty initial IDs enable Native lazy instrument discovery"
    );
    assert!(
        LighterBinaryDataClientConfig {
            market_ids: vec![4255, 4255],
            ..Default::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        LighterBinaryDataClientConfig {
            market_ids: (1
                ..=(nautilus_lighter_ext::upstream::common::consts::SUBSCRIBE_INFLIGHT_MAX as u64
                    / 4
                    + 1))
                .collect(),
            ..Default::default()
        }
        .validate()
        .is_err()
    );
}

#[tokio::test]
async fn rolling_discovery_rejects_truncated_pagination() {
    async fn page(
        Query(query): Query<HashMap<String, String>>,
        State(calls): State<Arc<AtomicUsize>>,
    ) -> String {
        calls.fetch_add(1, Ordering::AcqRel);
        assert_eq!(query["active"], "true");
        assert_eq!(query["tag"], "eth-updown-15m");
        let cursor = query.get("cursor").map_or(0, |v| v.parse::<u64>().unwrap());
        json!({"code":200,"events":[],"next_cursor":cursor+1}).to_string()
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = Router::new()
        .route("/events", get(page))
        .with_state(Arc::clone(&calls));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let config = LighterBinaryDataClientConfig {
        base_url_http: Some(format!("http://{addr}")),
        ..Default::default()
    };
    let result = LighterBinaryHttpClient::new(&config)
        .unwrap()
        .request_universe(
            &["eth-updown-15m".into()],
            30,
            &HashMap::new(),
            get_atomic_clock_realtime().get_time_ns(),
        )
        .await;
    assert!(result.unwrap_err().to_string().contains("twenty pages"));
    assert_eq!(calls.load(Ordering::Acquire), 20);
    server.abort();
}
