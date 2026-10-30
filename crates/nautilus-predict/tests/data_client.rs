// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Public adapter regression; mainnet probes require explicit ignored-test selection.
#![cfg(feature = "live")]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::HeaderMap,
    response::Response,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use nautilus_common::{
    clients::DataClient,
    live::runner::replace_data_event_sender,
    messages::{
        DataEvent,
        data::{SubscribeCustomData, UnsubscribeCustomData},
    },
};
use nautilus_core::{UUID4, UnixNanos, string::secret::SecretString};
use nautilus_model::{data::Data, identifiers::ClientId, instruments::Instrument};
use nautilus_network::websocket::TransportBackend;
use nautilus_predict::{
    PredictDataClient, PredictDataClientConfig, PredictOrderbookSnapshot,
    predict_orderbook_data_type,
};
use serde_json::{Value, json};
use tokio::sync::mpsc;

const MARKET: &str = include_str!("../test_data/market_2859346.json");
const BOOK: &str = include_str!("../test_data/orderbook_2859346.json");
const MARKET_ID: u64 = 2_859_346;
const HEARTBEAT: &str = "fixture-heartbeat-unchanged";

#[derive(Clone)]
struct VenueState {
    connections: Arc<AtomicUsize>,
    requests: mpsc::UnboundedSender<Value>,
}

async fn market(headers: HeaderMap) -> &'static str {
    assert_eq!(headers["x-api-key"], "fixture-key");
    MARKET
}

async fn websocket(
    ws: WebSocketUpgrade,
    State(state): State<VenueState>,
    headers: HeaderMap,
) -> Response {
    assert_eq!(headers["x-api-key"], "fixture-key");
    ws.on_upgrade(move |socket| serve_socket(socket, state))
}

async fn serve_socket(socket: WebSocket, state: VenueState) {
    let connection = state.connections.fetch_add(1, Ordering::AcqRel);
    let (mut sink, mut stream) = socket.split();
    while let Some(Ok(Message::Text(text))) = stream.next().await {
        let request: Value = serde_json::from_str(&text).unwrap();
        state.requests.send(request.clone()).unwrap();
        match request["method"].as_str().unwrap() {
            "subscribe" => {
                assert_eq!(
                    request["params"],
                    json!([format!("predictOrderbook/{MARKET_ID}")])
                );
                let book: Value = serde_json::from_str(BOOK).unwrap();
                let mut wrong_book = book.clone();
                wrong_book["marketId"] = json!(MARKET_ID + 1);
                let frames = [
                    json!({"type": "R", "requestId": request["requestId"], "data": {"success": true}}),
                    json!({"type": "M", "topic": "informational/topic"}),
                    json!({"type": "M", "topic": format!("predictOrderbook/{}", MARKET_ID + 1), "data": wrong_book}),
                    json!({"type": "M", "topic": format!("predictOrderbook/{MARKET_ID}"), "data": wrong_book}),
                    json!({"type": "M", "topic": format!("predictOrderbook/{MARKET_ID}"), "data": book}),
                    json!({"type": "M", "topic": "heartbeat", "data": HEARTBEAT}),
                ];
                for frame in frames {
                    sink.send(Message::Text(frame.to_string().into()))
                        .await
                        .unwrap();
                }
            }
            "heartbeat" => {
                assert!(request["data"] == HEARTBEAT || request["data"] == "retired-topic-barrier");
                if connection == 0 {
                    // Force a real reconnect and require automatic replay of the active topic.
                    sink.send(Message::Close(None)).await.unwrap();
                    return;
                }
            }
            "unsubscribe" => {
                // A final unsolicited old frame must not be admitted after local retirement.
                sink.send(Message::Text(json!({"type": "M", "topic": format!("predictOrderbook/{MARKET_ID}"), "data": serde_json::from_str::<Value>(BOOK).unwrap()}).to_string().into())).await.unwrap();
                sink.send(Message::Text(
                    json!({"topic": "heartbeat", "data": "retired-topic-barrier"})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            }
            method => panic!("unexpected public method {method}"),
        }
    }
}

fn subscribe(client: &mut PredictDataClient) {
    client
        .subscribe(SubscribeCustomData::new(
            Some(ClientId::new("PREDICT")),
            None,
            predict_orderbook_data_type(MARKET_ID),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
}

fn unsubscribe(client: &mut PredictDataClient) {
    client
        .unsubscribe(&UnsubscribeCustomData::new(
            Some(ClientId::new("PREDICT")),
            None,
            predict_orderbook_data_type(MARKET_ID),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn public_client_preserves_bootstrap_snapshots_heartbeat_reconnect_and_reference_counts() {
    nautilus_predict::register_predict_custom_data();
    let selector =
        nautilus_predict::common::CryptoUpDownSelectorKey::new("BTC/USDT", "Bitcoin", 300).unwrap();
    assert_eq!(
        nautilus_predict::predict_crypto_up_down_orderbook_data_type(MARKET_ID, &selector)
            .identifier(),
        Some("2859346")
    );
    let (request_tx, mut requests) = mpsc::unbounded_channel();
    let state = VenueState {
        connections: Arc::new(AtomicUsize::new(0)),
        requests: request_tx,
    };
    let router = Router::new()
        .route("/v1/markets/{id}", get(market))
        .route("/ws", get(websocket))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let (event_tx, mut events) = mpsc::unbounded_channel();
    replace_data_event_sender(event_tx);
    let config = PredictDataClientConfig {
        api_key: SecretString::from("fixture-key"),
        market_ids: vec![MARKET_ID],
        base_url_http: Some(format!("http://{address}")),
        base_url_ws: Some(format!("ws://{address}/ws")),
        http_timeout_secs: 5,
        ws_timeout_secs: 5,
        dynamic_refresh_secs: 1,
        transport_backend: TransportBackend::default(),
    };
    let mut client = PredictDataClient::new(ClientId::new("PREDICT"), config).unwrap();
    client.connect().await.unwrap();
    assert!(client.is_connected());
    for expected in [
        format!("{MARKET_ID}-1.PREDICT"),
        format!("{MARKET_ID}-2.PREDICT"),
    ] {
        let DataEvent::Data(Data::Instrument(instrument)) = events.recv().await.unwrap() else {
            panic!("definitions precede snapshots")
        };
        assert_eq!(instrument.id().to_string(), expected);
    }
    subscribe(&mut client);
    subscribe(&mut client);
    tokio::time::timeout(Duration::from_secs(10), async {
        for _ in 0..2 {
            let DataEvent::Data(Data::Custom(custom)) = events.recv().await.unwrap() else {
                panic!("only valid custom snapshots")
            };
            let snapshot = custom
                .data
                .as_any()
                .downcast_ref::<PredictOrderbookSnapshot>()
                .unwrap();
            assert_eq!(custom.data_type.identifier(), Some("2859346"));
            assert_eq!(snapshot.instrument_id.to_string(), "2859346-1.PREDICT");
            assert_eq!(snapshot.market_id, MARKET_ID);
            assert_eq!(snapshot.bids[0].price.to_string(), "0.62");
            assert_eq!(
                snapshot.last_order_settled.as_ref().unwrap().id,
                "987654321"
            );
            assert_eq!(snapshot.settlements_pending.as_ref().unwrap().asks.len(), 1);
            assert_eq!(
                snapshot.ts_event,
                UnixNanos::from_millis(
                    serde_json::from_str::<Value>(BOOK).unwrap()["updateTimestampMs"]
                        .as_u64()
                        .unwrap()
                )
            );
        }
    })
    .await
    .expect("snapshot before and after reconnect");
    let mut recorded_requests = tokio::time::timeout(Duration::from_secs(5), async {
        let mut recorded = Vec::new();
        let mut heartbeats = 0;
        while heartbeats < 2 {
            let request = requests.recv().await.unwrap();
            heartbeats += usize::from(request["method"] == "heartbeat");
            recorded.push(request);
        }
        recorded
    })
    .await
    .expect("both connections echo heartbeat");
    unsubscribe(&mut client);
    unsubscribe(&mut client);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let request = requests.recv().await.unwrap();
            let done = request["data"] == "retired-topic-barrier";
            recorded_requests.push(request);
            if done {
                break;
            }
        }
    })
    .await
    .expect("session filters retired snapshot before echoing the barrier");
    assert_eq!(state.connections.load(Ordering::Acquire), 2);
    assert_eq!(
        recorded_requests
            .iter()
            .filter(|r| r["method"] == "subscribe")
            .count(),
        2
    );
    assert_eq!(
        recorded_requests
            .iter()
            .filter(|r| r["method"] == "heartbeat")
            .count(),
        3
    );
    assert_eq!(
        recorded_requests
            .iter()
            .filter(|r| r["method"] == "unsubscribe")
            .count(),
        1
    );
    let ids: Vec<_> = recorded_requests
        .iter()
        .filter_map(|r| r["requestId"].as_u64())
        .collect();
    assert_eq!(ids, vec![1, 2, 3]);
    assert!(
        events.try_recv().is_err(),
        "retired or mismatched snapshots must not publish"
    );
    client.disconnect().await.unwrap();
    assert!(client.is_disconnected());
    client.reset().unwrap();
    client.connect().await.unwrap();
    assert!(client.is_connected());
    client.disconnect().await.unwrap();
    server.abort();
}

#[path = "data_client/quotes.rs"]
mod quotes;

#[path = "data_client/strategy.rs"]
mod strategy;

#[path = "data_client/mainnet.rs"]
mod mainnet;
