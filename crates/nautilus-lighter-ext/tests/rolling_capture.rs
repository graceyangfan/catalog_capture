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

//! Accelerated expiry through real Native requests, subscriptions, cache, actor and catalog.
use axum::{
    Router,
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    routing::get,
};
use catalog_capture_core::{CaptureConfig, CapturePlan};
use catalog_capture_runtime_adapter::{
    CatalogCaptureActor, CatalogCaptureActorConfig, LighterUpDownCaptureConfig,
};
use futures_util::{SinkExt, StreamExt};
use nautilus_common::enums::Environment;
use nautilus_core::UUID4;
use nautilus_lighter_ext::{
    LighterBinaryDataClientConfig, LighterBinaryDataClientFactory, register_lighter_ext_custom_data,
};
use nautilus_live::node::LiveNode;
use nautilus_model::{data::IndexPriceUpdate, identifiers::TraderId, instruments::Instrument};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
const OLD: u64 = 5001;
const NEW: u64 = 5002;
#[derive(Clone)]
struct Venue {
    boundary: u64,
    calls: Arc<AtomicUsize>,
    commands: mpsc::UnboundedSender<Value>,
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
fn event(id: u64, boundary: u64) -> Value {
    let fixture: Value = serde_json::from_str(include_str!("../test_data/event.json")).unwrap();
    let mut event = fixture["events"][0].clone();
    let start = if id == OLD { boundary - 10 } else { boundary };
    let end = if id == OLD { boundary } else { boundary + 900 };
    event["event_id"] = json!(id);
    event["metadata"]["market_id"] = json!(id);
    event["start_at"] = json!(start * 1000);
    event["end_at"] = json!(end * 1000);
    let market = &mut event["order_books"][0];
    market["market_id"] = json!(id);
    market["start_timestamp"] = json!((start - 100) * 1000);
    market["end_timestamp"] = json!(end * 1000);
    market["status"] = json!("active");
    market["symbol"] = json!(format!("ETH-UPDOWN-15M-{start}"));
    event
}
async fn events(State(venue): State<Venue>) -> String {
    if venue.calls.fetch_add(1, Ordering::AcqRel) == 1 {
        tokio::time::sleep(Duration::from_secs(4)).await;
    }
    // Old stays active with non-zero OI: expiry alone must retire it. The second response is stale.
    json!({"code":200,"events":[event(OLD,venue.boundary),event(NEW,venue.boundary)],"next_cursor":0}).to_string()
}
async fn boundary(Query(query): Query<std::collections::HashMap<String, String>>) -> String {
    let frame: Value =
        serde_json::from_str(include_str!("../test_data/subscribed_up_down_candle.json")).unwrap();
    let mut point = frame["candles"][0].clone();
    point["timestamp"] = json!(query["start_timestamp"].parse::<u64>().unwrap());
    json!({"code":200,"candles":[point]}).to_string()
}
async fn socket(ws: WebSocketUpgrade, State(venue): State<Venue>) -> axum::response::Response {
    ws.on_upgrade(move |s| serve(s, venue))
}
async fn serve(socket: WebSocket, venue: Venue) {
    let (mut sink, mut stream) = socket.split();
    let mut oracle = false;
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            message = stream.next() => {
                let Some(Ok(message)) = message else { break; };
                let Message::Text(text) = message else { continue; };
                let command: Value = serde_json::from_str(&text).unwrap();
                venue.commands.send(command.clone()).unwrap();
                let channel = command["channel"].as_str().unwrap_or_default();
                if command["type"] == "unsubscribe" {
                    if channel.starts_with("up_down_candle/") { oracle = false; }
                    continue;
                }
                if command["type"] != "subscribe" { continue; }
                let id = channel.split('/').nth(1).and_then(|id| id.parse::<u64>().ok()).unwrap_or(0);
                let frame = subscription_frame(channel, id, &mut oracle);
                if sink.send(Message::Text(frame.to_string().into())).await.is_err() { break; }
            }
            _ = tick.tick(), if oracle => {
                let mut frame: Value = serde_json::from_str(include_str!("../test_data/update_up_down_candle.json")).unwrap();
                frame["candles"][0]["timestamp"] = json!(now());
                if sink.send(Message::Text(frame.to_string().into())).await.is_err() { break; }
            }
        }
    }
}
fn subscription_frame(channel: &str, id: u64, oracle: &mut bool) -> Value {
    if channel.starts_with("order_book/") {
        let mut frame: Value =
            serde_json::from_str(include_str!("../test_data/subscribed_order_book.json")).unwrap();
        frame["channel"] = json!(format!("order_book:{id}"));
        frame
    } else if channel.starts_with("trade/") {
        let fixture: Value =
            serde_json::from_str(include_str!("../test_data/trades.json")).unwrap();
        let mut trade = fixture["trades"][0].clone();
        trade["market_id"] = json!(id);
        trade["trade_id"] = json!(id);
        trade["trade_id_str"] = json!(id.to_string());
        json!({
            "type": "subscribed/trade", "channel": format!("trade:{id}"),
            "nonce": 1, "trades": [trade],
        })
    } else if channel.starts_with("bo_market_stats/") {
        json!({
            "type": "subscribed/bo_market_stats", "channel": format!("bo_market_stats:{id}"),
            "timestamp": now() * 1000,
            "bo_market_stats": {"market_id": id, "open_interest": "123.4567", "status": "active"},
        })
    } else {
        *oracle = true;
        let mut frame: Value =
            serde_json::from_str(include_str!("../test_data/subscribed_up_down_candle.json"))
                .unwrap();
        frame["candles"][0]["timestamp"] = json!(now());
        frame["candles"].as_array_mut().unwrap().truncate(1);
        frame
    }
}
fn files(root: &Path) -> Vec<PathBuf> {
    if !root.exists() {
        return vec![];
    }
    std::fs::read_dir(root)
        .unwrap()
        .flat_map(|e| {
            let p = e.unwrap().path();
            if p.is_dir() { files(&p) } else { vec![p] }
        })
        .collect()
}
#[tokio::test(flavor = "current_thread")]
async fn expired_market_seals_without_settlement_successor_records_and_stale_response_cannot_resurrect()
 {
    register_lighter_ext_custom_data();
    let root = std::env::temp_dir().join(format!("lighter-roll-{}", UUID4::new()));
    std::fs::create_dir_all(&root).unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let venue = Venue {
        boundary: now() + 3,
        calls: Arc::new(AtomicUsize::new(0)),
        commands: tx,
    };
    let router = Router::new()
        .route("/events", get(events))
        .route("/upDownCandles", get(boundary))
        .route("/stream", get(socket))
        .with_state(venue.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let capture = CaptureConfig {
        catalog_uri: format!("file://{}", root.display()),
        flush_rows: 32,
        flush_interval_ms: 20,
        ..Default::default()
    };
    let mut actor = CatalogCaptureActorConfig::new(capture, CapturePlan::default());
    actor.lighter_updown = vec![LighterUpDownCaptureConfig {
        tag: "eth-updown-15m".into(),
        client_id: "LIGHTER".into(),
        refresh_secs: 1,
        prefetch_secs: 5,
    }];
    let mut node = LiveNode::builder(TraderId::new("CAPTURE-001"), Environment::Live)
        .unwrap()
        .with_name("LIGHTER-ROLL-TEST")
        .with_timeout_connection(10)
        .with_delay_post_stop_secs(1)
        .add_data_client(
            Some("LIGHTER".into()),
            Box::new(LighterBinaryDataClientFactory::new()),
            Box::new(LighterBinaryDataClientConfig {
                base_url_http: Some(format!("http://{addr}")),
                base_url_ws: Some(format!("ws://{addr}/stream")),
                ..Default::default()
            }),
        )
        .unwrap()
        .build()
        .unwrap();
    node.add_actor(CatalogCaptureActor::new(actor).unwrap())
        .unwrap();
    let stop = node.handle();
    let verify = async {
        let result = tokio::time::timeout(Duration::from_secs(18), async {
            loop {
                let audit = std::fs::read_to_string(root.join("lighter_universe_events.jsonl"))
                    .unwrap_or_default();
                if audit.contains("\"kind\":\"cache_purged\"")
                    && audit.contains("\"kind\":\"definition_updated\"")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let audit =
                std::fs::read_to_string(root.join("lighter_universe_events.jsonl")).unwrap();
            let rows: Vec<Value> = audit
                .lines()
                .map(|l| serde_json::from_str(l).unwrap())
                .collect();
            assert!(rows.iter().any(|r| r["kind"] == "cache_purged"
                && r["details"]["cache_purged"] == true
                && r["details"]["cache_contains_instrument"] == false));
            let old = root.join(format!("data/order_book_deltas/{OLD}-BINARY.LIGHTER"));
            assert!(
                files(&old)
                    .iter()
                    .any(|p| p.extension().is_some_and(|e| e == "parquet"))
            );
            assert!(
                !files(&old)
                    .iter()
                    .any(|p| p.to_string_lossy().ends_with(".part"))
            );
            let successor = root.join(format!("data/order_book_deltas/{NEW}-BINARY.LIGHTER"));
            assert!(
                files(&successor)
                    .iter()
                    .any(|p| p.to_string_lossy().ends_with(".part")),
                "successor continues writing while expired file is sealed"
            );
        })
        .await;
        stop.stop();
        result
    };
    let (result, run) = tokio::join!(verify, node.run());
    run.unwrap();
    result.unwrap();
    server.abort();
    let mut commands = Vec::new();
    while let Ok(cmd) = rx.try_recv() {
        commands.push(cmd);
    }
    for kind in ["trade", "order_book", "bo_market_stats"] {
        assert!(
            commands
                .iter()
                .any(|c| c["type"] == "unsubscribe" && c["channel"] == format!("{kind}/{OLD}"))
        );
    }
    assert_eq!(
        commands
            .iter()
            .filter(|c| c["type"] == "subscribe" && c["channel"] == "up_down_candle/eth/15m/1s")
            .count(),
        1,
        "one shared physical oracle across rollover"
    );
    assert!(
        !commands
            .iter()
            .any(|c| c["type"] == "unsubscribe" && c["channel"] == "up_down_candle/eth/15m/1s")
    );
    let mut catalog = ParquetDataCatalog::new(&root, None, None, None, None);
    let instruments = catalog.instruments(None, None, None).unwrap();
    for id in [OLD, NEW] {
        let expected = format!("{id}-BINARY.LIGHTER");
        let instrument = instruments
            .iter()
            .filter(|i| i.id().to_string() == expected)
            .max_by_key(|i| i.ts_init())
            .unwrap();
        assert_eq!(
            instrument.info().unwrap()["start_price"],
            "2392.6418531263384125"
        );
        assert!(
            !catalog
                .trade_ticks(Some(vec![expected.clone()]), None, None)
                .unwrap()
                .is_empty()
        );
        assert!(
            !catalog
                .order_book_deltas(Some(vec![expected]), None, None)
                .unwrap()
                .is_empty()
        );
    }
    let prices = catalog
        .query::<IndexPriceUpdate>(None, None, None, None, None, true)
        .unwrap();
    assert!(prices.iter().all(|p| p.value.precision == 16));
    assert!(prices.iter().any(
        |p| p.instrument_id.to_string() == format!("{NEW}-BINARY.LIGHTER")
            && p.ts_event.as_u64() >= venue.boundary * 1_000_000_000
    ));
    assert!(
        !catalog
            .query_custom_data_dynamic("LighterOpenInterest", None, None, None, None, None, true)
            .unwrap()
            .is_empty()
    );
    assert!(
        files(&root)
            .iter()
            .all(|p| !p.to_string_lossy().ends_with(".part"))
    );
    std::fs::remove_dir_all(root).unwrap();
}
