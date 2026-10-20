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

use std::time::Duration;

use axum::{
    Router,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use nautilus_common::{
    clients::DataClient,
    live::runner::replace_data_event_sender,
    messages::{
        DataEvent,
        data::{SubscribeBookDeltas, SubscribeTrades, UnsubscribeTrades},
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_lighter_ext::{
    LighterBinaryDataClient, LighterBinaryDataClientConfig, common::instrument_id,
};
use nautilus_model::{data::Data, enums::BookType, identifiers::ClientId};
use serde_json::{Value, json};
async fn event() -> &'static str {
    include_str!("../test_data/event.json")
}
async fn boundary() -> String {
    let frame: Value =
        serde_json::from_str(include_str!("../test_data/subscribed_up_down_candle.json")).unwrap();
    let mut point = frame["candles"][0].clone();
    point["timestamp"] = json!(1792480500);
    json!({"code":200,"candles":[point]}).to_string()
}
async fn websocket(ws: WebSocketUpgrade) -> axum::response::Response {
    ws.on_upgrade(serve)
}
async fn serve(socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();
    while let Some(Ok(Message::Text(text))) = stream.next().await {
        let value: Value = serde_json::from_str(&text).unwrap();
        if value["type"] == "subscribe" {
            assert_eq!(value["channel"], "trade/4255");
            let fixture: Value =
                serde_json::from_str(include_str!("../test_data/trades.json")).unwrap();
            let frame = json!({"type":"subscribed/trade","channel":"trade:4255","nonce":1,"trades":fixture["trades"]});
            if sink
                .send(Message::Text(frame.to_string().into()))
                .await
                .is_err()
            {
                break;
            }
        }
    }
}
#[tokio::test(flavor = "current_thread")]
async fn reset_drains_old_generation_and_allows_identical_trade_in_new_generation() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/events", get(event))
        .route("/upDownCandles", get(boundary))
        .route("/stream", get(websocket));
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = LighterBinaryDataClient::new(
        ClientId::new("LIGHTER"),
        LighterBinaryDataClientConfig {
            market_ids: vec![4255],
            base_url_http: Some(format!("http://{addr}")),
            base_url_ws: Some(format!("ws://{addr}/stream")),
            ..Default::default()
        },
    )
    .unwrap();
    let id = instrument_id(4255).unwrap();
    let mut trade_ids = Vec::new();
    for _ in 0..2 {
        client.connect().await.unwrap();
        assert!(client.is_connected());
        assert!(
            client
                .subscribe_book_deltas(SubscribeBookDeltas::new(
                    id,
                    BookType::L1_MBP,
                    Some(client.client_id()),
                    None,
                    UUID4::new(),
                    UnixNanos::default(),
                    None,
                    true,
                    None,
                    None
                ))
                .is_err()
        );
        let command = SubscribeTrades::new(
            id,
            Some(client.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        client.subscribe_trades(command.clone()).unwrap();
        client.subscribe_trades(command).unwrap();
        let tick = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(DataEvent::Data(Data::Trade(tick))) = rx.recv().await {
                    break tick;
                }
            }
        })
        .await
        .unwrap();
        trade_ids.push(tick.trade_id);
        client
            .unsubscribe_trades(&UnsubscribeTrades::new(
                id,
                Some(client.client_id()),
                None,
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ))
            .unwrap();
        client.reset().unwrap();
        assert!(client.is_disconnected());
        client.stop().unwrap();
        client.disconnect().await.unwrap();
        while rx.try_recv().is_ok() {}
    }
    assert_eq!(trade_ids.len(), 2);
    assert_eq!(
        trade_ids[0], trade_ids[1],
        "new generation must not inherit the old replay suppression map"
    );
    server.abort();
}
