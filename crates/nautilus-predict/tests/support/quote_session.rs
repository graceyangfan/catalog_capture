// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Private session failure assertions live in tests, without a production fault-injection API.
use axum::{
    Router,
    extract::{
        State,
        ws::{Message as WsMessage, WebSocketUpgrade},
    },
    response::Response,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use nautilus_common::{live::sender::EventSender, messages::DataEvent};
use nautilus_core::string::secret::SecretString;
use nautilus_live::task::TaskGroup;
use nautilus_model::data::Data;
use nautilus_network::websocket::TransportBackend;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use super::*;
const ID: u64 = 2859346;
async fn socket(ws: WebSocketUpgrade, State(tx): State<mpsc::UnboundedSender<Value>>) -> Response {
    ws.on_upgrade(move|socket|async move{
        let(mut sink,mut source)=socket.split();
        while let Some(Ok(WsMessage::Text(text)))=source.next().await{
            let command:Value=serde_json::from_str(&text).unwrap();tx.send(command.clone()).unwrap();
            if command["method"]=="subscribe" {
                for index in 0..2 {
                    let mut book:Value=serde_json::from_str(include_str!("../../test_data/orderbook_2859346.json")).unwrap();
                    book["updateTimestampMs"]=json!(1_700_000_000_123u64+index);
                    if index==1{book["bids"]=json!([["0.61","1500.25"]]);}
                    if sink.send(WsMessage::Text(json!({"type":"M","topic":format!("predictOrderbook/{ID}"),"data":book}).to_string().into())).await.is_err(){break;}
                }
            }
        }
    })
}
fn context() -> PredictMarketContext {
    PredictMarketContext {
        market_id: ID,
        yes_instrument_id: crate::instrument_id_from_outcome(ID, 1).unwrap(),
        no_instrument_id: crate::instrument_id_from_outcome(ID, 2).unwrap(),
        price_precision: 2,
        size_precision: nautilus_model::types::fixed::FIXED_PRECISION,
    }
}
async fn session(
    url: &str,
    backend: TransportBackend,
) -> (
    PredictDataSession,
    TaskGroup,
    mpsc::UnboundedReceiver<DataEvent>,
) {
    let tasks = TaskGroup::new();
    let (tx, rx) = mpsc::unbounded_channel();
    let context = context();
    let config = PredictDataClientConfig {
        api_key: SecretString::from("loopback-only"),
        market_ids: vec![],
        base_url_http: Some(url.into()),
        base_url_ws: Some(format!("{}/ws", url.replacen("http", "ws", 1))),
        http_timeout_secs: 2,
        ws_timeout_secs: 2,
        dynamic_refresh_secs: 1,
        transport_backend: backend,
    };
    let (_, session) = PredictDataSession::connect(
        &config,
        PredictSessionInputs {
            contexts: AHashMap::from([(ID, context)]),
            dynamic_registry: Arc::new(RwLock::new(PredictDynamicMarketRegistry::default())),
            dynamic_generation: 0,
            data_sender: EventSender::from(tx),
            clock: nautilus_core::time::get_atomic_clock_realtime(),
            cancellation: tasks.cancellation_token(),
            connected: Arc::new(AtomicBool::new(true)),
            task_spawner: tasks.spawner().unwrap(),
        },
    )
    .await
    .unwrap();
    (session, tasks, rx)
}
async fn run(backend: TransportBackend, removal: bool) -> Value {
    let (tx, mut commands) = mpsc::unbounded_channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/ws", get(socket)).with_state(tx),
        )
        .await
        .unwrap()
    });
    let (mut state, tasks, mut events) = session(&url, backend).await;
    let context = context();
    let id = context.yes_instrument_id;
    let mut raw_frames = vec![];
    let mut pending = None;
    if removal {
        state
            .handle_command(PredictCommand::SubscribeQuotes(id, context.clone()))
            .await
            .unwrap();
        let c = tokio::time::timeout(Duration::from_secs(2), commands.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(c["method"], "subscribe");
        let (epoch, message) = tokio::time::timeout(Duration::from_secs(2), state.raw_rx.recv())
            .await
            .unwrap()
            .unwrap();
        raw_frames.push(serde_json::from_str::<Value>(message.as_text().unwrap()).unwrap());
        state.handle_message(epoch, message).await.unwrap();
        assert!(matches!(
            events.recv().await,
            Some(DataEvent::Data(Data::Quote(_)))
        ));
        assert!(events.try_recv().is_err());
        let frame = tokio::time::timeout(Duration::from_secs(2), state.raw_rx.recv())
            .await
            .unwrap()
            .unwrap();
        raw_frames.push(serde_json::from_str::<Value>(frame.1.as_text().unwrap()).unwrap());
        pending = Some(frame);
    }
    // Real terminal transport closure guarantees an actual send_text failure, not a fake error.
    state.client.disconnect().await;
    let command = if removal {
        PredictCommand::UnsubscribeQuotes(id)
    } else {
        PredictCommand::SubscribeQuotes(id, context.clone())
    };
    let error = state.handle_command(command).await.unwrap_err();
    assert_eq!(state.subscriptions.get(&ID), Some(&1));
    assert_eq!(state.quotes.len(), 1);
    assert_eq!(state.quotes[&id].context.market_id, ID);
    assert_eq!(state.quotes[&id].last_quote.is_some(), removal);
    state.fail_generation(format!("offline injected terminal send failure: {error:#}"));
    assert!(state.cancellation.is_cancelled());
    assert_eq!(state.failed_epoch, Some(state.current_epoch));
    if let Some((epoch, message)) = pending {
        state.handle_message(epoch, message).await.unwrap();
        assert!(
            events.try_recv().is_err(),
            "queued frame cannot publish after generation failure"
        );
    }
    state
        .handle_command(PredictCommand::SubscribeQuotes(id, context.clone()))
        .await
        .unwrap();
    assert_eq!(state.subscriptions.get(&ID), Some(&1));
    assert!(
        commands.try_recv().is_err(),
        "no failed command may appear on the wire"
    );
    tasks.begin_shutdown();
    tasks
        .finish_shutdown(Duration::from_secs(1), Duration::from_secs(1))
        .await
        .unwrap();
    drop(state);
    let (mut fresh, fresh_tasks, mut fresh_events) = session(&url, backend).await;
    assert!(fresh.quotes.is_empty() && fresh.subscriptions.is_empty());
    fresh
        .handle_command(PredictCommand::SubscribeQuotes(id, context))
        .await
        .unwrap();
    let command = tokio::time::timeout(Duration::from_secs(2), commands.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(command["method"], "subscribe");
    assert_eq!(command["requestId"], 1);
    // After selector retirement, only the active quote owner retains this context.
    fresh.contexts.clear();
    fresh.remove_dynamic_market_if_unused(ID);
    assert!(fresh.dynamic_registry.read().unwrap().context(ID).is_none());
    let (epoch, message) = tokio::time::timeout(Duration::from_secs(2), fresh.raw_rx.recv())
        .await
        .unwrap()
        .unwrap();
    fresh.handle_message(epoch, message).await.unwrap();
    assert!(matches!(
        fresh_events.recv().await,
        Some(DataEvent::Data(Data::Quote(_)))
    ));
    fresh
        .handle_command(PredictCommand::UnsubscribeQuotes(id))
        .await
        .unwrap();
    assert!(fresh.quotes.is_empty() && fresh.subscriptions.is_empty() && fresh.contexts.is_empty());
    assert!(fresh.dynamic_registry.read().unwrap().context(ID).is_none());
    let removed = tokio::time::timeout(Duration::from_secs(2), commands.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(removed["method"], "unsubscribe");
    let (epoch, late) = tokio::time::timeout(Duration::from_secs(2), fresh.raw_rx.recv())
        .await
        .unwrap()
        .unwrap();
    fresh.handle_message(epoch, late).await.unwrap();
    assert!(
        fresh_events.try_recv().is_err(),
        "last owner release must drop queued frames"
    );
    fresh
        .handle_command(PredictCommand::UnsubscribeQuotes(id))
        .await
        .unwrap();
    assert!(commands.try_recv().is_err());
    fresh.client.disconnect().await;
    fresh_tasks.begin_shutdown();
    fresh_tasks
        .finish_shutdown(Duration::from_secs(1), Duration::from_secs(1))
        .await
        .unwrap();
    server.abort();
    json!({"backend":format!("{backend:?}"),"stage":if removal{"unsubscribe"}else{"subscribe"},"error":format!("{error:#}"),"frames":raw_frames,"stable":{"retainedQuotes":1,"retainedMarketReferences":1,"terminalGenerationCancelled":true,"queuedLateQuoteEmitted":false,"newGenerationStartsEmpty":true,"newGenerationExplicitSubscribeQuotes":1,"newRequestId":1},"mainnetWrites":0})
}
fn path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/quote_send_failures_synthetic_20261016.json")
}
#[tokio::test]
async fn terminal_quote_sends_keep_intent_atomic_until_new_session() {
    let saved: Value = serde_json::from_slice(&std::fs::read(path()).unwrap()).unwrap();
    for backend in [TransportBackend::default(), TransportBackend::Tungstenite] {
        for removal in [false, true] {
            let actual = run(backend, removal).await;
            let expected = saved["sessions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["backend"] == actual["backend"] && r["stage"] == actual["stage"])
                .unwrap();
            assert_eq!(actual["stable"], expected["stable"]);
            assert_eq!(actual["frames"], expected["frames"]);
        }
    }
}
#[tokio::test]
#[ignore = "explicit synthetic loopback send-failure capture; never overwrite"]
async fn record_quote_send_failures_json() {
    let mut sessions = vec![];
    for backend in [TransportBackend::default(), TransportBackend::Tungstenite] {
        for removal in [false, true] {
            sessions.push(run(backend, removal).await);
        }
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path())
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic real loopback terminal WebSocket send failures; not mainnet feedback","sessions":sessions})).unwrap();
}
