// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.

//! A small, deterministic recording through the real node, actor, and catalog.
//! The clock is changed only in this isolated test binary, never in production.
#![cfg(feature = "live")]

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use axum::{
    Router,
    extract::{
        Path as RoutePath, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
    routing::get,
};
use catalog_capture_core::{CaptureConfig, CapturePlan, CustomDataCaptureSpec};
use catalog_capture_runtime_adapter::{CatalogCaptureActor, CatalogCaptureActorConfig};
use futures_util::{SinkExt, StreamExt};
use nautilus_common::enums::Environment;
use nautilus_core::{
    Params, UUID4, UnixNanos, string::secret::SecretString, time::get_atomic_clock_realtime,
};
use nautilus_live::node::LiveNode;
use nautilus_model::{
    data::{Data, DataType},
    identifiers::TraderId,
    instruments::Instrument,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_predict::{
    PredictDataClientConfig, PredictDataClientFactory, PredictOrderbookSnapshot,
    register_predict_custom_data,
};
use serde_json::{Value, json};

// Use the product's actual output parser rather than a parallel test interpretation.
mod output {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../catalog-capture-cli/src/config/output.rs"
    ));
}

const ORIGINAL_PROFILE: &str = include_str!("../../../examples/capture.predict-orderbook.toml");
const OLD: u64 = 2_859_346;
const NEW: u64 = OLD + 1;

#[derive(Clone)]
struct Venue {
    start_secs: u64,
}

fn market(id: u64) -> Value {
    let mut market: Value =
        serde_json::from_str(include_str!("../test_data/market_2859346.json")).unwrap();
    market["data"]["id"] = json!(id);
    market["data"]["tradingStatus"] = json!("OPEN");
    market["data"]["status"] = json!("OPEN");
    market["data"]["variantData"]["priceFeedSymbol"] = json!("BTC/USDT");
    if id == NEW {
        market["data"]["title"] = json!("Bitcoin Up or Down - October 3, 6:05PM-6:10PM ET");
    }
    market
}

async fn category(RoutePath(slug): RoutePath<String>, State(venue): State<Venue>) -> String {
    let markets = if slug == format!("btc-updown-5m-{}", venue.start_secs) {
        vec![market(OLD)["data"].clone()]
    } else if slug == format!("btc-updown-5m-{}", venue.start_secs + 300) {
        vec![market(NEW)["data"].clone()]
    } else {
        vec![]
    };
    json!({"success": true, "data": {"markets": markets}}).to_string()
}

async fn definition(RoutePath(id): RoutePath<u64>) -> String {
    assert!([OLD, NEW].contains(&id));
    market(id).to_string()
}

fn snapshot(id: u64, timestamp_ms: u64, order_count: u64) -> Value {
    let mut book: Value =
        serde_json::from_str(include_str!("../test_data/orderbook_2859346.json")).unwrap();
    book["marketId"] = json!(id);
    book["lastOrderSettled"]["marketId"] = json!(id);
    book["updateTimestampMs"] = json!(timestamp_ms);
    book["orderCount"] = json!(order_count);
    json!({"type": "M", "topic": format!("predictOrderbook/{id}"), "data": book})
}

async fn websocket(ws: WebSocketUpgrade, State(venue): State<Venue>) -> Response {
    ws.on_upgrade(move |socket| serve_socket(socket, venue))
}

async fn serve_socket(socket: WebSocket, venue: Venue) {
    let (mut sink, mut stream) = socket.split();
    let boundary_ms = (venue.start_secs + 300) * 1000;
    while let Some(Ok(Message::Text(text))) = stream.next().await {
        let command: Value = serde_json::from_str(&text).unwrap();
        match command["method"].as_str().unwrap() {
            "subscribe" if command["params"][0] == format!("predictOrderbook/{OLD}") => {
                sink.send(Message::Text(
                    snapshot(OLD, boundary_ms - 1000, 41).to_string().into(),
                ))
                .await
                .unwrap();
            }
            "subscribe" if command["params"][0] == format!("predictOrderbook/{NEW}") => {
                // A late old update before the successor is valid and stays in the old segment.
                // An old update after the successor's first frame must be filtered.
                for frame in [
                    snapshot(OLD, boundary_ms - 1, 42),
                    snapshot(NEW, boundary_ms, 43),
                    snapshot(OLD, boundary_ms + 1, 999),
                ] {
                    sink.send(Message::Text(frame.to_string().into()))
                        .await
                        .unwrap();
                }
            }
            "unsubscribe" | "heartbeat" => {}
            _ => panic!("unexpected fixture command {command}"),
        }
    }
}

struct ProbeDirectory(PathBuf);

impl Drop for ProbeDirectory {
    fn drop(&mut self) {
        if self.0.exists() {
            fs::remove_dir_all(&self.0).expect("remove only this test's recording directory");
        }
    }
}

struct TestClock;

impl Drop for TestClock {
    fn drop(&mut self) {
        get_atomic_clock_realtime().make_realtime();
    }
}

fn files(root: &Path) -> Vec<PathBuf> {
    if !root.exists() {
        return vec![];
    }
    let mut found = Vec::new();
    for entry in fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            found.extend(files(&path));
        } else {
            found.push(path);
        }
    }
    found
}

async fn wait_for_file(root: &Path, suffix: &str) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if files(root)
                .iter()
                .any(|p| p.to_string_lossy().ends_with(suffix))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("expected {suffix} under {}", root.display()));
}

#[tokio::test(flavor = "current_thread")]
async fn original_btc_profile_rolls_seals_reads_back_and_removes_recordings() {
    register_predict_custom_data();
    let profile: toml::Value = toml::from_str(ORIGINAL_PROFILE).unwrap();
    let output: output::OutputConfig = profile["output"].clone().try_into().unwrap();
    assert_eq!(output.catalog_uri, "file://./data/predict-orderbook");
    let selectors = profile["capture"]["custom_data"].as_array().unwrap();
    assert_eq!(selectors.len(), 1);
    let selector = &selectors[0];
    let metadata = Params::new(
        selector["metadata"]
            .as_table()
            .unwrap()
            .iter()
            .map(|(key, value)| (key.clone(), json!(value.as_str().unwrap())))
            .collect(),
    );
    assert_eq!(metadata["interval_secs"], "300");
    let plan = CapturePlan {
        custom_data: vec![CustomDataCaptureSpec {
            data_type: DataType::new(
                selector["type_name"].as_str().unwrap(),
                Some(metadata),
                None,
            ),
        }],
        ..Default::default()
    };
    let directory = ProbeDirectory(
        std::env::temp_dir().join(format!("predict-capture-rollover-{}", UUID4::new())),
    );
    fs::create_dir_all(&directory.0).unwrap();
    let capture = CaptureConfig {
        catalog_uri: format!("file://{}", directory.0.display()),
        compression: output::parse_compression(&output.compression).unwrap(),
        layout_compatibility: output::parse_layout_compatibility(&output.layout_compatibility)
            .unwrap(),
        overflow_policy: output::parse_overflow_policy(&output.overflow_policy).unwrap(),
        lifecycle: output.lifecycle,
        queue_capacity: output.queue_capacity,
        flush_rows: output.flush_rows,
        flush_interval_ms: output.flush_interval_ms,
        max_buffer_bytes: output.max_buffer_bytes,
        max_total_buffer_bytes: output.max_total_buffer_bytes,
        max_active_partitions: output.max_active_partitions,
        ..Default::default()
    };
    let start_secs = chrono::DateTime::parse_from_rfc3339("2026-10-03T22:00:00Z")
        .unwrap()
        .timestamp() as u64;
    let clock = get_atomic_clock_realtime();
    clock.make_static();
    clock.set_time(UnixNanos::from((start_secs + 299) * 1_000_000_000));
    let _clock_guard = TestClock;
    let venue = Venue { start_secs };
    let router = Router::new()
        .route("/v1/categories/{slug}", get(category))
        .route("/v1/markets/{id}", get(definition))
        .route("/ws", get(websocket))
        .with_state(venue);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(capture, plan)).unwrap();
    let mut node = LiveNode::builder(TraderId::new("CAPTURE-001"), Environment::Live)
        .unwrap()
        .with_name("PREDICT-LOCAL-ROLLOVER-PROBE")
        .with_timeout_connection(5)
        .with_delay_post_stop_secs(
            profile["runtime"]["delay_post_stop_secs"]
                .as_integer()
                .unwrap() as u64,
        )
        .add_data_client(
            None,
            Box::new(PredictDataClientFactory::new()),
            Box::new(PredictDataClientConfig {
                api_key: SecretString::from("local-fixture-only"),
                market_ids: vec![],
                base_url_http: Some(format!("http://{address}")),
                base_url_ws: Some(format!("ws://{address}/ws")),
                http_timeout_secs: 5,
                ws_timeout_secs: 5,
                dynamic_refresh_secs: 1,
                transport_backend: Default::default(),
            }),
        )
        .unwrap()
        .build()
        .unwrap();
    node.add_actor(actor).unwrap();
    let stop = node.handle();
    let old_folder = directory
        .0
        .join(format!("data/custom/PredictOrderbookSnapshot/{OLD}"));
    let new_folder = directory
        .0
        .join(format!("data/custom/PredictOrderbookSnapshot/{NEW}"));
    let verify_roll = async {
        wait_for_file(&old_folder, ".parquet.part").await;
        assert!(
            !files(&old_folder)
                .iter()
                .any(|p| p.extension().is_some_and(|e| e == "parquet"))
        );
        clock.set_time(UnixNanos::from((start_secs + 300) * 1_000_000_000));
        wait_for_file(&old_folder, ".parquet").await;
        wait_for_file(&new_folder, ".parquet.part").await;
        // The retiring file must be readable while the successor is still recording.
        let mut catalog = ParquetDataCatalog::new(&directory.0, None, None, None, None);
        let ids = vec![OLD.to_string()];
        let rows = catalog
            .query_custom_data_dynamic(
                "PredictOrderbookSnapshot",
                Some(&ids),
                None,
                None,
                None,
                None,
                true,
            )
            .unwrap();
        assert_eq!(
            rows.len(),
            2,
            "first and late valid old snapshots are both sealed"
        );
        assert!(
            !files(&new_folder)
                .iter()
                .any(|p| p.extension().is_some_and(|e| e == "parquet"))
        );
        stop.stop();
    };
    let ((), result) = tokio::join!(verify_roll, node.run());
    result.unwrap();
    server.abort();
    let recorded_files = files(&directory.0);
    assert!(
        recorded_files
            .iter()
            .all(|p| !p.to_string_lossy().ends_with(".part")),
        "graceful shutdown leaves no open segments"
    );
    for folder in [&old_folder, &new_folder] {
        assert_eq!(files(folder).len(), 1, "one sealed file per market");
    }
    let mut catalog = ParquetDataCatalog::new(&directory.0, None, None, None, None);
    let instruments = catalog.instruments(None, None, None).unwrap();
    assert_eq!(instruments.len(), 4);
    for id in [OLD, NEW] {
        for outcome in [1, 2] {
            assert!(
                instruments
                    .iter()
                    .any(|instrument| instrument.id().to_string()
                        == format!("{id}-{outcome}.PREDICT"))
            );
        }
    }
    let rows = catalog
        .query_custom_data_dynamic(
            "PredictOrderbookSnapshot",
            None,
            None,
            None,
            None,
            None,
            true,
        )
        .unwrap();
    assert_eq!(
        rows.len(),
        3,
        "retired unsolicited fourth snapshot is filtered"
    );
    let mut counts = BTreeMap::new();
    for row in rows {
        let Data::Custom(custom) = row else {
            panic!("custom row")
        };
        let book = custom
            .data
            .as_any()
            .downcast_ref::<PredictOrderbookSnapshot>()
            .unwrap();
        counts
            .entry(book.market_id)
            .or_insert_with(Vec::new)
            .push(book.order_count);
        assert_eq!(
            custom.data_type.identifier(),
            Some(book.market_id.to_string().as_str())
        );
        assert_eq!(
            custom.data_type.metadata().unwrap()["predict_price_feed_symbol"],
            "BTCUSDT"
        );
        assert_eq!(
            custom.data_type.metadata().unwrap()["predict_interval_secs"],
            "300"
        );
        assert_eq!(
            book.instrument_id.to_string(),
            format!("{}-1.PREDICT", book.market_id)
        );
        assert_eq!(book.bids[0].price.to_string(), "0.62");
        assert_eq!(book.asks[0].price.to_string(), "0.63");
        assert_eq!(
            book.bids[0].quantity.as_decimal(),
            "1500.25".parse().unwrap()
        );
        assert_eq!(
            book.last_order_settled.as_ref().unwrap().market_id,
            book.market_id
        );
        assert_eq!(book.settlements_pending.as_ref().unwrap().asks.len(), 1);
        let event_ms = (start_secs + 300) * 1000
            - match book.order_count {
                41 => 1000,
                42 => 1,
                43 => 0,
                _ => panic!("unexpected accepted snapshot"),
            };
        assert_eq!(book.ts_event, UnixNanos::from_millis(event_ms));
    }
    counts.get_mut(&OLD).unwrap().sort_unstable();
    assert_eq!(counts[&OLD], vec![41, 42]);
    assert_eq!(counts[&NEW], vec![43]);
    drop(catalog);
    fs::remove_dir_all(&directory.0).unwrap();
    assert!(!directory.0.exists());
    println!(
        "PASS: 2 windows, 4 outcome definitions, 3 exact snapshots, old segment sealed before stop, no .part files, recording directory removed"
    );
}
