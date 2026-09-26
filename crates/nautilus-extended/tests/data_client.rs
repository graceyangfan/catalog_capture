// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! End-to-end public data-client tests using captured Extended Mainnet fixtures.

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use nautilus_common::{
    clients::DataClient,
    live::runner::replace_data_event_sender,
    messages::{
        data::{
            SubscribeBookDeltas, SubscribeFundingRates, SubscribeIndexPrices, SubscribeMarkPrices,
            SubscribeTrades, UnsubscribeMarkPrices, UnsubscribeTrades,
        },
        DataEvent,
    },
};
use nautilus_core::{UnixNanos, UUID4};
use nautilus_extended::{
    common::instrument_id_from_market, ExtendedDataClient, ExtendedDataClientConfig,
    ExtendedInstrumentProviderConfig,
};
use nautilus_model::{data::Data, enums::BookType, identifiers::ClientId, instruments::Instrument};
use serde_json::{json, Value};

const MARKETS: &str = include_str!("../test_data/http_markets_btc.json");
const BOOK_SNAPSHOT: &str = include_str!("../test_data/ws_orderbook_snapshot_btc.json");
const BOOK_DELTA: &str = include_str!("../test_data/ws_orderbook_delta_btc.json");
const TRADES: &str = include_str!("../test_data/ws_trades_replay_btc.json");
const MARK_PRICE: &str = include_str!("../test_data/ws_mark_price_btc.json");
const INDEX_PRICE: &str = include_str!("../test_data/ws_index_price_btc.json");
const FUNDING_RATE: &str = include_str!("../test_data/ws_funding_rate_btc.json");

#[derive(Clone, Default)]
struct TestState {
    requests: Arc<tokio::sync::Mutex<Vec<(String, String)>>>,
    connections: Arc<AtomicUsize>,
}

async fn markets() -> &'static str {
    MARKETS
}

async fn websocket_upgrade(ws: WebSocketUpgrade, State(state): State<TestState>) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: TestState) {
    state.connections.fetch_add(1, Ordering::AcqRel);
    let (mut sink, mut stream) = socket.split();
    let mut next_sequence = 0_u64;
    let mut pending_mark_unsubscribe = None;

    while let Some(Ok(message)) = stream.next().await {
        let Message::Text(text) = message else {
            continue;
        };
        let Ok(request) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        let Some(method) = request["method"].as_str() else {
            continue;
        };
        if method != "subscribe" && method != "unsubscribe" {
            continue;
        }

        let topic = topic_from_request(&request);
        state
            .requests
            .lock()
            .await
            .push((method.to_string(), topic.clone()));

        if method == "unsubscribe" && topic == "prices.mark.BTC-USD" {
            let mut frame = next_trade_frame(2_103_822_896_206_974_980);
            frame["seq"] = json!(next_sequence);
            next_sequence += 1;
            if sink
                .send(Message::Text(frame.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
            pending_mark_unsubscribe = Some(request["id"].clone());
            continue;
        }

        let result = if method == "subscribe" {
            json!({"method": method, "status": "OK", "subscription": topic})
        } else {
            json!({"method": method, "status": "OK"})
        };
        let response = json!({
            "jsonrpc": "2.0",
            "id": request["id"],
            "result": result,
        });
        if sink
            .send(Message::Text(response.to_string().into()))
            .await
            .is_err()
        {
            break;
        }

        if method == "unsubscribe" {
            continue;
        }

        for mut frame in frames_for_topic(&topic) {
            frame["seq"] = json!(next_sequence);
            next_sequence += 1;
            if sink
                .send(Message::Text(frame.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
        }

        if topic == "prices.mark.BTC-USD" {
            if let Some(request_id) = pending_mark_unsubscribe.take() {
                let stale_response = json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {"method": "unsubscribe", "status": "OK"},
                });
                if sink
                    .send(Message::Text(stale_response.to_string().into()))
                    .await
                    .is_err()
                {
                    return;
                }

                let mut frame = fixture(MARK_PRICE);
                frame["seq"] = json!(next_sequence);
                next_sequence += 1;
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
}

fn topic_from_request(request: &Value) -> String {
    let params = &request["params"];
    let scope = params["scope"].as_str().expect("scope");
    let selector = &params["selector"];
    let market = selector["market"].as_str().expect("market");
    match scope {
        "orderbooks" => format!("orderbooks.{market}"),
        "trades" => format!("trades.{market}"),
        "funding-rates" => format!("funding-rates.{market}"),
        "prices" => format!(
            "prices.{}.{market}",
            selector["type"].as_str().expect("price type")
        ),
        other => panic!("unexpected scope {other}"),
    }
}

fn fixture(raw: &str) -> Value {
    serde_json::from_str(raw).expect("valid fixture")
}

fn next_trade_frame(trade_id: u64) -> Value {
    let replay = fixture(TRADES);
    let mut live = replay.clone();
    live["data"] = json!([replay["data"].as_array().unwrap().last().unwrap()]);
    live["data"][0]["i"] = json!(trade_id);
    live
}

fn frames_for_topic(topic: &str) -> Vec<Value> {
    match topic {
        "orderbooks.BTC-USD" => vec![fixture(BOOK_SNAPSHOT), fixture(BOOK_DELTA)],
        "trades.BTC-USD" => {
            let replay = fixture(TRADES);
            vec![replay, next_trade_frame(2_103_822_896_206_974_979)]
        }
        "prices.mark.BTC-USD" => vec![fixture(MARK_PRICE)],
        "prices.index.BTC-USD" => vec![fixture(INDEX_PRICE)],
        "funding-rates.BTC-USD" => vec![fixture(FUNDING_RATE)],
        other => panic!("unexpected topic {other}"),
    }
}

async fn start_server() -> (SocketAddr, TestState) {
    let state = TestState::default();
    let router = Router::new()
        .route("/api/v1/info/markets", get(markets))
        .route("/v2/rpc", get(websocket_upgrade))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let addr = listener.local_addr().expect("test server address");
    tokio::spawn(async move { axum::serve(listener, router).await.expect("serve fixtures") });
    (addr, state)
}

fn subscribe_standard_families(client: &mut ExtendedDataClient) {
    let instrument_id = instrument_id_from_market("BTC-USD").unwrap();
    let client_id = Some(ClientId::new("EXTENDED"));
    let command_id = UUID4::new;
    let ts_init = UnixNanos::default();

    client
        .subscribe_book_deltas(SubscribeBookDeltas::new(
            instrument_id,
            BookType::L2_MBP,
            client_id,
            None,
            command_id(),
            ts_init,
            None,
            true,
            None,
            None,
        ))
        .unwrap();
    client
        .subscribe_trades(SubscribeTrades::new(
            instrument_id,
            client_id,
            None,
            command_id(),
            ts_init,
            None,
            None,
        ))
        .unwrap();
    client
        .subscribe_mark_prices(SubscribeMarkPrices::new(
            instrument_id,
            client_id,
            None,
            command_id(),
            ts_init,
            None,
            None,
        ))
        .unwrap();
    client
        .subscribe_index_prices(SubscribeIndexPrices::new(
            instrument_id,
            client_id,
            None,
            command_id(),
            ts_init,
            None,
            None,
        ))
        .unwrap();
    client
        .subscribe_funding_rates(SubscribeFundingRates::new(
            instrument_id,
            client_id,
            None,
            command_id(),
            ts_init,
            None,
            None,
        ))
        .unwrap();
}

fn subscribe_trades(client: &mut ExtendedDataClient) {
    client
        .subscribe_trades(SubscribeTrades::new(
            instrument_id_from_market("BTC-USD").unwrap(),
            Some(ClientId::new("EXTENDED")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
}

fn unsubscribe_trades(client: &mut ExtendedDataClient) {
    client
        .unsubscribe_trades(&UnsubscribeTrades::new(
            instrument_id_from_market("BTC-USD").unwrap(),
            Some(ClientId::new("EXTENDED")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
}

fn subscribe_mark_prices(client: &mut ExtendedDataClient) {
    client
        .subscribe_mark_prices(SubscribeMarkPrices::new(
            instrument_id_from_market("BTC-USD").unwrap(),
            Some(ClientId::new("EXTENDED")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
}

fn unsubscribe_mark_prices(client: &mut ExtendedDataClient) {
    client
        .unsubscribe_mark_prices(&UnsubscribeMarkPrices::new(
            instrument_id_from_market("BTC-USD").unwrap(),
            Some(ClientId::new("EXTENDED")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
}

async fn wait_for_request_count(state: &TestState, expected: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if state.requests.lock().await.len() >= expected {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("expected RPC request count");
}

#[tokio::test(flavor = "current_thread")]
async fn data_client_routes_captured_mainnet_fixtures() {
    let (addr, state) = start_server().await;
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(sender);

    let instrument_id = instrument_id_from_market("BTC-USD").unwrap();
    let config = ExtendedDataClientConfig {
        base_url_http: Some(format!("http://{addr}/api/v1")),
        base_url_ws: Some(format!("ws://{addr}/v2/rpc")),
        update_instruments_interval_mins: 0,
        instrument_provider: ExtendedInstrumentProviderConfig {
            load_all: false,
            load_ids: Some(vec![instrument_id]),
        },
        ..ExtendedDataClientConfig::default()
    };
    let mut client = ExtendedDataClient::new(ClientId::new("EXTENDED"), config).unwrap();
    client.connect().await.unwrap();
    assert!(client.is_connected());
    subscribe_standard_families(&mut client);
    subscribe_trades(&mut client);

    let mut instruments = 0;
    let mut books = 0;
    let mut trades = 0;
    let mut marks = 0;
    let mut indexes = 0;
    let mut funding = 0;
    tokio::time::timeout(Duration::from_secs(5), async {
        while instruments < 1 || books < 2 || trades < 1 || marks < 1 || indexes < 1 || funding < 1
        {
            match receiver.recv().await.expect("data event channel open") {
                DataEvent::Instrument(value) if value.id() == instrument_id => instruments += 1,
                DataEvent::Data(Data::BookDeltas(_)) => books += 1,
                DataEvent::Data(Data::Trade(_)) => trades += 1,
                DataEvent::Data(Data::MarkPrice(_)) => marks += 1,
                DataEvent::Data(Data::IndexPrice(_)) => indexes += 1,
                DataEvent::FundingRate(_) => funding += 1,
                _ => {}
            }
        }
    })
    .await
    .expect("all public families should route");

    unsubscribe_trades(&mut client);
    unsubscribe_mark_prices(&mut client);
    subscribe_mark_prices(&mut client);
    let mut marks_after_resubscribe = 0;
    let mut saw_cross_family_trade = false;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match receiver.recv().await.expect("data event channel open") {
                DataEvent::Data(Data::Trade(trade)) => {
                    assert_eq!(trade.trade_id.as_str(), "2103822896206974980");
                    saw_cross_family_trade = true;
                }
                DataEvent::Data(Data::MarkPrice(_)) => marks_after_resubscribe += 1,
                _ => {}
            }
            if saw_cross_family_trade && marks_after_resubscribe == 2 {
                break;
            }
        }
    })
    .await
    .expect("stale unsubscribe ACK must not retire a resubscribed topic");

    wait_for_request_count(&state, 7).await;
    let requests = state.requests.lock().await.clone();
    assert_eq!(
        requests
            .iter()
            .filter(|(method, _)| method == "subscribe")
            .map(|(_, topic)| topic.as_str())
            .collect::<Vec<_>>(),
        vec![
            "orderbooks.BTC-USD",
            "trades.BTC-USD",
            "prices.mark.BTC-USD",
            "prices.index.BTC-USD",
            "funding-rates.BTC-USD",
            "prices.mark.BTC-USD",
        ]
    );
    assert_eq!(
        requests
            .iter()
            .filter(|(method, _)| method == "unsubscribe")
            .map(|(_, topic)| topic.as_str())
            .collect::<Vec<_>>(),
        vec!["prices.mark.BTC-USD"],
    );
    assert_eq!(state.connections.load(Ordering::Acquire), 1);

    unsubscribe_trades(&mut client);
    wait_for_request_count(&state, 8).await;
    assert_eq!(
        state
            .requests
            .lock()
            .await
            .iter()
            .filter(|(method, topic)| method == "unsubscribe" && topic == "trades.BTC-USD")
            .count(),
        1,
    );
    client.disconnect().await.unwrap();
    assert!(client.is_disconnected());
}
