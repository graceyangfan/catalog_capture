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
pub(super) const ID: u64 = 2859346;
pub(super) async fn socket(
    ws: WebSocketUpgrade,
    State(tx): State<mpsc::UnboundedSender<Value>>,
) -> Response {
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
pub(super) fn context() -> PredictMarketContext {
    PredictMarketContext {
        market_id: ID,
        yes_instrument_id: crate::instrument_id_from_outcome(ID, 1).unwrap(),
        no_instrument_id: crate::instrument_id_from_outcome(ID, 2).unwrap(),
        price_precision: 2,
        size_precision: nautilus_model::types::fixed::FIXED_PRECISION,
    }
}
pub(super) async fn session(
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
            http_client: PredictHttpClient::new(
                config.api_key.expose_secret(),
                config.base_url_http.as_deref(),
                config.http_timeout_secs,
            )
            .unwrap(),
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
            .apply_command(PredictCommand::SubscribeQuotes(id, context.clone()))
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
        state.apply_message(epoch, message).await.unwrap();
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
    let error = state.apply_command(command).await.unwrap_err();
    let retained = usize::from(!removal);
    assert_eq!(
        state.subscriptions.get(&ID).map_or(0, |s| s.owners),
        retained
    );
    assert_eq!(state.quotes.len(), retained);
    if !removal {
        assert_eq!(state.quotes[&id].context.market_id, ID);
        assert!(state.quotes[&id].last_quote.is_none());
    }
    state.fail_generation(format!("offline injected terminal send failure: {error:#}"));
    assert!(state.cancellation.is_cancelled());
    assert_eq!(state.failed_epoch, Some(state.current_epoch));
    if let Some((epoch, message)) = pending {
        state.apply_message(epoch, message).await.unwrap();
        assert!(
            events.try_recv().is_err(),
            "queued frame cannot publish after generation failure"
        );
    }
    let retry = state
        .apply_command(PredictCommand::SubscribeQuotes(id, context.clone()))
        .await;
    assert!(retry.is_ok());
    assert_eq!(state.subscriptions.get(&ID).map(|s| s.owners), Some(1));
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
        .apply_command(PredictCommand::SubscribeQuotes(id, context))
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
    fresh.apply_message(epoch, message).await.unwrap();
    assert!(matches!(
        fresh_events.recv().await,
        Some(DataEvent::Data(Data::Quote(_)))
    ));
    fresh
        .apply_command(PredictCommand::UnsubscribeQuotes(id))
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
    fresh.apply_message(epoch, late).await.unwrap();
    assert!(
        fresh_events.try_recv().is_err(),
        "last owner release must drop queued frames"
    );
    fresh
        .apply_command(PredictCommand::UnsubscribeQuotes(id))
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
    json!({"backend":format!("{backend:?}"),"stage":if removal{"unsubscribe"}else{"subscribe"},"error":format!("{error:#}"),"frames":raw_frames,"stable":{"retainedQuotes":retained,"retainedMarketReferences":retained,"terminalGenerationCancelled":true,"queuedLateQuoteEmitted":false,"newGenerationStartsEmpty":true,"newGenerationExplicitSubscribeQuotes":1,"newRequestId":1},"mainnetWrites":0})
}
fn path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/quote_send_failures_synthetic_20261016.json")
}
#[tokio::test]
async fn terminal_quote_sends_preserve_desired_interest_until_last_release() {
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
            // Historical capture retained cancelled owners. Only that policy changes.
            let mut expected_stable = expected["stable"].clone();
            if removal {
                expected_stable["retainedQuotes"] = json!(0);
                expected_stable["retainedMarketReferences"] = json!(0);
            }
            assert_eq!(actual["stable"], expected_stable);
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

#[tokio::test(flavor = "current_thread")]
async fn depth_conflict_preserves_owner_after_registry_retirement() {
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
    let (mut state, tasks, mut events) = session(&url, TransportBackend::Tungstenite).await;
    let context = context();
    let id = context.yes_instrument_id;
    state
        .apply_command(PredictCommand::SubscribeDepth(id, context.clone(), None))
        .await
        .unwrap();
    assert_eq!(commands.recv().await.unwrap()["method"], "subscribe");
    state.contexts.clear();
    state.remove_dynamic_market_if_unused(ID);
    let (epoch, message) = state.raw_rx.recv().await.unwrap();
    state.apply_message(epoch, message).await.unwrap();
    assert!(matches!(
        events.recv().await,
        Some(DataEvent::Data(Data::BookDepth(_)))
    ));
    assert!(
        events.try_recv().is_err(),
        "depth-only must not emit custom snapshots or quotes"
    );
    let (epoch, message) = state.raw_rx.recv().await.unwrap();
    state.apply_message(epoch, message).await.unwrap();
    assert!(matches!(
        events.recv().await,
        Some(DataEvent::Data(Data::BookDepth(_)))
    ));
    let mut older: Value =
        serde_json::from_str(include_str!("../../test_data/orderbook_2859346.json")).unwrap();
    older["updateTimestampMs"] = json!(1_700_000_000_122u64);
    older["bids"] = json!([["0.99", "1"]]);
    let message = || {
        nautilus_network::transport::Message::Text(
            json!({"type":"M","topic":format!("predictOrderbook/{ID}"),"data":older})
                .to_string()
                .into(),
        )
    };
    state.apply_message(epoch, message()).await.unwrap();
    assert!(
        events.try_recv().is_err(),
        "older full depth must not rewind Native managed state"
    );
    state
        .apply_command(PredictCommand::Subscribe(ID))
        .await
        .unwrap();
    state.apply_message(epoch, message()).await.unwrap();
    let Some(DataEvent::Data(Data::Custom(custom))) = events.recv().await else {
        panic!("Custom recording contract is independent of depth ordering")
    };
    assert_eq!(
        custom.data.ts_event(),
        nautilus_core::UnixNanos::from_millis(1_700_000_000_122)
    );
    assert!(events.try_recv().is_err());
    state
        .apply_command(PredictCommand::Unsubscribe(ID))
        .await
        .unwrap();

    assert!(
        state
            .apply_command(PredictCommand::SubscribeDepth(
                id,
                context,
                std::num::NonZeroUsize::new(1)
            ))
            .await
            .is_err()
    );
    assert!(state.depths[&id].depth.is_none());
    assert_eq!(state.subscriptions[&ID].owners, 1);
    assert!(commands.try_recv().is_err());
    state
        .apply_command(PredictCommand::UnsubscribeDepth(id))
        .await
        .unwrap();
    assert!(state.depths.is_empty() && state.subscriptions.is_empty());
    assert_eq!(commands.recv().await.unwrap()["method"], "unsubscribe");
    tasks.begin_shutdown();
    tasks
        .finish_shutdown(Duration::from_secs(1), Duration::from_secs(1))
        .await
        .unwrap();
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn terminal_depth_sends_preserve_desired_interest_until_last_release() {
    for backend in [TransportBackend::default(), TransportBackend::Tungstenite] {
        for removal in [false, true] {
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
            let (mut state, tasks, _events) = session(&url, backend).await;
            let context = context();
            let id = context.yes_instrument_id;
            if removal {
                state
                    .apply_command(PredictCommand::SubscribeDepth(id, context.clone(), None))
                    .await
                    .unwrap();
                assert_eq!(commands.recv().await.unwrap()["method"], "subscribe");
            }
            state.client.disconnect().await;
            let command = if removal {
                PredictCommand::UnsubscribeDepth(id)
            } else {
                PredictCommand::SubscribeDepth(id, context, None)
            };
            assert!(state.apply_command(command).await.is_err());
            let retained = usize::from(!removal);
            assert_eq!(
                state.subscriptions.get(&ID).map_or(0, |s| s.owners),
                retained
            );
            assert_eq!(state.depths.len(), retained);
            if !removal {
                assert_eq!(state.depths[&id].context.market_id, ID);
            }
            assert!(state.quotes.is_empty());
            assert!(commands.try_recv().is_err());
            tasks.begin_shutdown();
            tasks
                .finish_shutdown(Duration::from_secs(1), Duration::from_secs(1))
                .await
                .unwrap();
            server.abort();
        }
    }
}

#[tokio::test]
async fn trades_share_market_topic_without_owning_the_book_and_route_both_outcomes() {
    let (tx, mut commands) = mpsc::unbounded_channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/ws", get(socket)).with_state(tx),
        )
        .await
        .unwrap();
    });
    let (mut state, tasks, mut events) = session(&url, TransportBackend::default()).await;
    let c = context();
    state
        .apply_command(PredictCommand::SubscribeDepth(
            c.yes_instrument_id,
            c.clone(),
            None,
        ))
        .await
        .unwrap();
    state
        .apply_command(PredictCommand::SubscribeTrades(
            c.yes_instrument_id,
            c.clone(),
        ))
        .await
        .unwrap();
    state
        .apply_command(PredictCommand::SubscribeTrades(
            c.yes_instrument_id,
            c.clone(),
        ))
        .await
        .unwrap();
    state
        .apply_command(PredictCommand::SubscribeTrades(
            c.no_instrument_id,
            c.clone(),
        ))
        .await
        .unwrap();
    async fn receive(rx: &mut mpsc::UnboundedReceiver<Value>) -> Value {
        tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
            .await
            .unwrap()
            .unwrap()
    }
    assert_eq!(
        receive(&mut commands).await["params"][0],
        format!("predictOrderbook/{ID}")
    );
    assert_eq!(
        receive(&mut commands).await["params"][0],
        format!("predictTrades/{ID}")
    );
    assert_eq!(state.next_request_id, 3);
    let frame = || {
        nautilus_network::transport::Message::Text(json!({"type":"M", "topic":format!("predictTrades/{ID}"),
        "data":{"marketId":ID,"tsMs":123,"taker":{"fill":"11680700000000000000","price":"320000000000000000","outcomeIndex":2,"side":"Bid"},
        "makers":[{"fill":"11680700000000000000","price":"320000000000000000","outcomeIndex":2,"side":"Ask","matchType":"COMPLEMENTARY"}]}}).to_string().into())
    };
    let epoch = state.current_epoch;
    state.apply_message(epoch, frame()).await.unwrap();
    let mut ticks = vec![];
    for _ in 0..2 {
        let DataEvent::Data(Data::Trade(t)) = events.try_recv().unwrap() else {
            panic!("expected native trade");
        };
        ticks.push(t);
    }
    ticks.sort_by_key(|t| t.instrument_id);
    assert_eq!(ticks[0].instrument_id, c.yes_instrument_id);
    assert_eq!(ticks[0].price, nautilus_model::types::Price::from("0.68"));
    assert_eq!(
        ticks[0].aggressor_side,
        nautilus_model::enums::AggressorSide::Sell
    );
    assert_eq!(
        ticks[1].aggressor_side,
        nautilus_model::enums::AggressorSide::Buy
    );
    assert_eq!(ticks[0].trade_id, ticks[1].trade_id);
    // Equal-looking genuine messages remain distinct local executions.
    state.apply_message(epoch, frame()).await.unwrap();
    let DataEvent::Data(Data::Trade(next)) = events.try_recv().unwrap() else {
        panic!("expected trade");
    };
    assert_ne!(next.trade_id, ticks[0].trade_id);
    events.try_recv().unwrap();
    state
        .apply_command(PredictCommand::UnsubscribeTrades(c.yes_instrument_id))
        .await
        .unwrap();
    assert_eq!(state.next_request_id, 3);
    assert_eq!(
        state.reconnect.request_reconnect(),
        nautilus_network::mode::ReconnectRequestOutcome::Accepted
    );
    let (new_epoch, reconnected) = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (new_epoch, message) = state.raw_rx.recv().await.unwrap();
            if message.as_text() == Some(nautilus_network::RECONNECTED) {
                break (new_epoch, message);
            }
        }
    })
    .await
    .unwrap();
    let stale_completion = state
        .client
        .send_text_on_connection("stale-write".into(), None, epoch)
        .await
        .unwrap_err();
    assert!(matches!(
        stale_completion,
        nautilus_network::error::SendError::ConnectionChanged
    ));
    state.finish_write(epoch, Err(stale_completion), "subscription");
    assert!(
        state.failed_epoch.is_none(),
        "an old send completion cannot invalidate the new connection"
    );
    state.apply_message(epoch, frame()).await.unwrap();
    assert!(
        events.try_recv().is_err(),
        "transport epoch fences old data before RECONNECTED is processed"
    );
    state.apply_message(new_epoch, reconnected).await.unwrap();
    assert_eq!(
        receive(&mut commands).await["params"][0],
        format!("predictOrderbook/{ID}")
    );
    assert_eq!(
        receive(&mut commands).await["params"][0],
        format!("predictTrades/{ID}")
    );
    state.apply_message(epoch, frame()).await.unwrap();
    assert!(events.try_recv().is_err(), "old epoch must not publish");
    state.apply_message(new_epoch, frame()).await.unwrap();
    let DataEvent::Data(Data::Trade(remaining)) = events.try_recv().unwrap() else {
        panic!("expected remaining NO owner");
    };
    assert_eq!(remaining.instrument_id, c.no_instrument_id);
    state
        .apply_command(PredictCommand::UnsubscribeTrades(c.no_instrument_id))
        .await
        .unwrap();
    let command = receive(&mut commands).await;
    assert_eq!(command["method"], "unsubscribe");
    assert_eq!(command["params"][0], format!("predictTrades/{ID}"));
    assert!(state.subscriptions.contains_key(&ID));
    state.apply_message(new_epoch, frame()).await.unwrap();
    assert!(events.try_recv().is_err());
    tasks.begin_shutdown();
    state.client.disconnect().await;
    server.abort();
}

#[tokio::test]
async fn depth_join_replays_the_confirmed_snapshot_without_a_second_wire_subscription() {
    let (tx, mut commands) = mpsc::unbounded_channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/ws", get(socket)).with_state(tx),
        )
        .await
        .unwrap();
    });
    let (mut state, tasks, mut events) = session(&url, TransportBackend::default()).await;
    state
        .apply_command(PredictCommand::Subscribe(ID))
        .await
        .unwrap();
    let request = tokio::time::timeout(std::time::Duration::from_secs(3), commands.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request["params"][0], format!("predictOrderbook/{ID}"));
    let (epoch, frame) = state.raw_rx.recv().await.unwrap();
    state.apply_message(epoch, frame).await.unwrap();
    let DataEvent::Data(Data::Custom(source)) = events.try_recv().unwrap() else {
        panic!("expected source snapshot");
    };
    let expected_ts = source.data.ts_event();
    let c = context();
    state
        .apply_command(PredictCommand::SubscribeDepth(
            c.yes_instrument_id,
            c.clone(),
            None,
        ))
        .await
        .unwrap();
    let DataEvent::Data(Data::BookDepth(depth)) = events.try_recv().unwrap() else {
        panic!("expected native snapshot replay");
    };
    assert_eq!(depth.ts_event, expected_ts);
    assert_eq!(depth.instrument_id, c.yes_instrument_id);
    assert_eq!(
        depth.bids[0].price,
        nautilus_model::types::Price::from("0.62")
    );
    assert_eq!(state.next_request_id, 2);
    state
        .apply_command(PredictCommand::UnsubscribeDepth(c.yes_instrument_id))
        .await
        .unwrap();
    assert!(state.books.contains_key(&ID));
    state
        .apply_command(PredictCommand::Unsubscribe(ID))
        .await
        .unwrap();
    assert!(state.books.is_empty());
    assert!(events.try_recv().is_err());
    tasks.begin_shutdown();
    state.client.disconnect().await;
    server.abort();
}

#[tokio::test]
async fn terminal_trade_sends_preserve_desired_interest_until_last_release() {
    let (tx, _) = mpsc::unbounded_channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/ws", get(socket)).with_state(tx),
        )
        .await
        .unwrap();
    });
    let (mut state, tasks, mut events) = session(&url, TransportBackend::default()).await;
    let c = context();
    state.client.disconnect().await;
    assert!(
        state
            .apply_command(PredictCommand::SubscribeTrades(
                c.yes_instrument_id,
                c.clone()
            ))
            .await
            .is_err()
    );
    assert_eq!(state.trades.len(), 1);
    assert!(state.subscriptions.is_empty());
    state
        .apply_command(PredictCommand::SubscribeTrades(
            c.no_instrument_id,
            c.clone(),
        ))
        .await
        .unwrap();
    assert_eq!(state.trades.len(), 2);
    state
        .apply_command(PredictCommand::UnsubscribeTrades(c.yes_instrument_id))
        .await
        .unwrap();
    assert!(
        state
            .apply_command(PredictCommand::UnsubscribeTrades(c.no_instrument_id))
            .await
            .is_err()
    );
    assert!(state.trades.is_empty());
    assert!(events.try_recv().is_err());
    tasks.begin_shutdown();
    server.abort();
}

// Drive the same send lane as run() when a test steps the private session explicitly.
impl PredictDataSession {
    pub(super) async fn flush_writes(&mut self) -> anyhow::Result<()> {
        loop {
            self.start_subscription_send();
            let Some(send) = self.subscription_send.take() else {
                break;
            };
            let (epoch, result) = send.await;
            if epoch == self.current_epoch {
                result?;
            }
        }
        if let Some(send) = self.control_send.take() {
            let (epoch, result) = send.await;
            if epoch == self.current_epoch {
                result?;
            }
        }
        Ok(())
    }
    pub(super) async fn apply_command(&mut self, command: PredictCommand) -> anyhow::Result<()> {
        self.handle_command(command).await?;
        self.flush_writes().await
    }
    pub(super) async fn apply_message(
        &mut self,
        epoch: u64,
        message: Message,
    ) -> anyhow::Result<()> {
        self.handle_message(epoch, message).await?;
        self.flush_writes().await
    }
    pub(super) async fn apply_discovery(&mut self, result: DiscoveryResult) -> anyhow::Result<()> {
        self.apply_discovery_result(result).await?;
        self.flush_writes().await
    }
}
