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

//! Public protocol fixtures through the actual LiveNode, CaptureActor and ParquetDataCatalog.
use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::Response,
    routing::get,
};
use catalog_capture_core::{
    BookDeltasCaptureSpec, CaptureConfig, CapturePlan, CustomDataCaptureSpec,
    IndexPriceCaptureSpec, InstrumentCaptureSpec, TradeCaptureSpec,
};
use catalog_capture_runtime_adapter::{CatalogCaptureActor, CatalogCaptureActorConfig};
use futures_util::{SinkExt, StreamExt};
use nautilus_common::enums::Environment;
use nautilus_core::UUID4;
use nautilus_lighter_ext::{
    LighterBinaryDataClientConfig, LighterBinaryDataClientFactory, LighterOpenInterest,
    common::{instrument_id, open_interest_data_type},
    register_lighter_ext_custom_data,
};
use nautilus_live::node::LiveNode;
use nautilus_model::{
    data::{Data, IndexPriceUpdate},
    enums::{BookAction, BookType},
    identifiers::TraderId,
    instruments::Instrument,
    orderbook::OrderBook,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;
use serde_json::{Value, json};

fn fixture(name: &str) -> Value {
    let text = match name {
        "event" => include_str!("../test_data/event.json"),
        "book" => include_str!("../test_data/subscribed_order_book.json"),
        "delta" => include_str!("../test_data/update_order_book.json"),
        "oi" => include_str!("../test_data/subscribed_bo_market_stats.json"),
        "oracle" => include_str!("../test_data/subscribed_up_down_candle.json"),
        "trades" => include_str!("../test_data/trades.json"),
        _ => panic!("unknown fixture"),
    };
    serde_json::from_str(text).unwrap()
}
async fn events() -> String {
    fixture("event").to_string()
}
async fn boundary() -> String {
    let mut point = fixture("oracle")["candles"][0].clone();
    point["timestamp"] = json!(1792480500);
    json!({"code":200,"candles":[point]}).to_string()
}
async fn websocket(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(serve_socket)
}
async fn serve_socket(socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();
    while let Some(Ok(Message::Text(text))) = stream.next().await {
        let command: Value = serde_json::from_str(&text).unwrap();
        if command["type"] != "subscribe" {
            continue;
        }
        let channel = command["channel"].as_str().unwrap();
        let frames = match channel {
            "order_book/4255" => vec![fixture("book"), fixture("delta")],
            "trade/4255" => {
                let frame = json!({"type":"subscribed/trade","channel":"trade:4255","nonce":1,"trades":fixture("trades")["trades"]});
                let mut duplicate = frame.clone();
                duplicate["type"] = json!("update/trade");
                vec![frame, duplicate]
            }
            "bo_market_stats/4255" => {
                let initial = fixture("oi");
                let mut zero = initial.clone();
                zero["type"] = json!("update/bo_market_stats");
                zero["timestamp"] = json!(1792480928240_u64);
                zero["bo_market_stats"]["open_interest"] = json!("0.0000");
                let mut missing = zero.clone();
                missing["bo_market_stats"]["open_interest"] = Value::Null;
                vec![initial, zero, missing]
            }
            "up_down_candle/eth/15m/1s" => {
                let frame = fixture("oracle");
                vec![frame.clone(), frame]
            }
            other => panic!("unexpected channel {other}"),
        };
        for frame in frames {
            if sink
                .send(Message::Text(frame.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
        }
    }
}
fn files(root: &Path) -> Vec<PathBuf> {
    if !root.exists() {
        return vec![];
    }
    fs::read_dir(root)
        .unwrap()
        .flat_map(|entry| {
            let path = entry.unwrap().path();
            if path.is_dir() {
                files(&path)
            } else {
                vec![path]
            }
        })
        .collect()
}
struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_market_records_and_reads_back_all_five_families() {
    register_lighter_ext_custom_data();
    let router = Router::new()
        .route("/events", get(events))
        .route("/upDownCandles", get(boundary))
        .route("/stream", get(websocket));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let directory = Directory(std::env::temp_dir().join(format!("lighter-fixed-{}", UUID4::new())));
    fs::create_dir_all(&directory.0).unwrap();
    let id = instrument_id(4255).unwrap();
    let plan = CapturePlan {
        instruments: vec![InstrumentCaptureSpec { instrument_id: id }],
        trades: vec![TradeCaptureSpec { instrument_id: id }],
        book_deltas: vec![BookDeltasCaptureSpec {
            instrument_id: id,
            book_type: BookType::L2_MBP,
            depth: None,
        }],
        index_prices: vec![IndexPriceCaptureSpec { instrument_id: id }],
        custom_data: vec![CustomDataCaptureSpec {
            data_type: open_interest_data_type(4255),
        }],
        ..Default::default()
    };
    let capture = CaptureConfig {
        catalog_uri: format!("file://{}", directory.0.display()),
        flush_rows: 32,
        flush_interval_ms: 20,
        ..Default::default()
    };
    let actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(capture, plan)).unwrap();
    let mut node = LiveNode::builder(TraderId::new("CAPTURE-001"), Environment::Live)
        .unwrap()
        .with_name("LIGHTER-FIXED-CATALOG-TEST")
        .with_timeout_connection(10)
        .with_delay_post_stop_secs(1)
        .add_data_client(
            Some("LIGHTER".into()),
            Box::new(LighterBinaryDataClientFactory::new()),
            Box::new(LighterBinaryDataClientConfig {
                market_ids: vec![4255],
                base_url_http: Some(format!("http://{addr}")),
                base_url_ws: Some(format!("ws://{addr}/stream")),
                ..Default::default()
            }),
        )
        .unwrap()
        .build()
        .unwrap();
    node.add_actor(actor).unwrap();
    let stop = node.handle();
    let wait = async {
        let result = tokio::time::timeout(Duration::from_secs(25), async {
            loop {
                let list = files(&directory.0);
                if [
                    "/trades/",
                    "/order_book_deltas/",
                    "/index_prices/",
                    "/custom/",
                ]
                .iter()
                .all(|kind| list.iter().any(|p| p.to_string_lossy().contains(kind)))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        if result.is_err() {
            eprintln!("catalog files: {:?}", files(&directory.0));
        }
        stop.stop();
        result
    };
    let (wait, result) = tokio::join!(wait, node.run());
    result.unwrap();
    wait.expect("all recording families must become non-empty");
    server.abort();
    let mut catalog = ParquetDataCatalog::new(&directory.0, None, None, None, None);
    let instruments = catalog.instruments(None, None, None).unwrap();
    assert_eq!(instruments.len(), 1);
    let instrument = &instruments[0];
    assert_eq!(instrument.id(), id);
    assert_eq!(instrument.price_precision(), 2);
    let info = instrument.info().unwrap();
    assert_eq!(info["start_price"], "2392.6418531263384125");
    assert_eq!(info["start_price_timestamp"], 1792480500_u64);
    assert_eq!(info["oracle"]["precision"], 16);
    let trades = catalog.trade_ticks(None, None, None).unwrap();
    assert_eq!(
        trades.len(),
        1,
        "duplicate prints and settlement do not create TradeTicks"
    );
    let source = fixture("trades");
    assert_eq!(
        trades[0].trade_id.to_string(),
        source["trades"][0]["trade_id_str"].as_str().unwrap()
    );
    assert_eq!(
        trades[0].price.to_string(),
        source["trades"][0]["price"].as_str().unwrap()
    );
    let deltas = catalog.order_book_deltas(None, None, None).unwrap();
    assert!(deltas.iter().any(|d| d.action == BookAction::Clear));
    assert!(deltas.iter().any(|d| d.action == BookAction::Delete));
    let mut book = OrderBook::new(id, BookType::L2_MBP);
    for delta in &deltas {
        book.apply_delta(delta).unwrap();
    }
    assert_eq!(book.best_bid_price().unwrap().to_string(), "0.38");
    assert_eq!(book.best_ask_price().unwrap().to_string(), "0.41");
    let oracle = catalog
        .query::<IndexPriceUpdate>(None, None, None, None, None, true)
        .unwrap();
    assert_eq!(oracle.len(), 2, "replayed oracle batch must be suppressed");
    for point in &oracle {
        assert_eq!(point.value.precision, 16);
        assert_eq!(point.value.to_string(), "2392.6418531263384125");
        assert_eq!(point.instrument_id, id);
    }
    assert_eq!(oracle[0].ts_event.as_u64(), 1792480925_u64 * 1_000_000_000);
    let rows = catalog
        .query_custom_data_dynamic("LighterOpenInterest", None, None, None, None, None, true)
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "zero OI is recorded, missing OI is not invented"
    );
    let mut values = Vec::new();
    for row in rows {
        let Data::Custom(custom) = row else {
            panic!("expected custom")
        };
        assert_eq!(custom.data_type, open_interest_data_type(4255));
        let oi = custom
            .data
            .as_any()
            .downcast_ref::<LighterOpenInterest>()
            .unwrap();
        assert_eq!(oi.instrument_id, id);
        values.push(oi.open_interest);
    }
    assert_eq!(
        values,
        vec!["440.8476".parse::<Decimal>().unwrap(), Decimal::ZERO]
    );
    assert!(
        files(&directory.0)
            .iter()
            .all(|p| !p.to_string_lossy().ends_with(".part"))
    );
}
