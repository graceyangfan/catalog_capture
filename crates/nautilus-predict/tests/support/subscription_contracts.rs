// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/LICENSE-3.0.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Native acceptance, cancellation and source-initialization regressions.

use axum::{Router, routing::get};
use nautilus_common::messages::DataEvent;
use nautilus_core::string::secret::SecretString;
use nautilus_model::data::Data;
use nautilus_network::{transport::Message, websocket::TransportBackend};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use super::quote_session_tests::{ID, context, session, socket};
use super::*;

#[tokio::test]
async fn batch_unsubscribe_reports_accepted_members() {
    use nautilus_common::{
        clients::DataClient,
        live::runner::replace_data_event_sender,
        messages::data::{SubscriptionError, UnsubscribeTrades},
    };
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::identifiers::ClientId;
    let (events, _rx) = mpsc::unbounded_channel();
    replace_data_event_sender(events);
    let mut client = offline_client(TransportBackend::default());
    let (tx, mut queue) = mpsc::channel(1);
    client.command_tx = Some(tx);
    let c = context();
    let command = UnsubscribeTrades::new(
        vec![c.yes_instrument_id, c.no_instrument_id],
        Some(ClientId::from("PREDICT")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    let error = client.unsubscribe_trades(&command).unwrap_err();
    assert!(
        matches!(queue.try_recv().unwrap(), PredictCommand::UnsubscribeTrades(id) if id == c.yes_instrument_id)
    );
    assert!(queue.try_recv().is_err());
    assert_eq!(
        error
            .downcast_ref::<SubscriptionError>()
            .map(|e| e.instrument_ids_accepted.clone()),
        Some(vec![c.yes_instrument_id]),
        "a queued release must remain visible to Native"
    );
}

#[tokio::test]
async fn late_depth_initializes_from_latest_source() {
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
    tokio::time::timeout(std::time::Duration::from_secs(3), commands.recv())
        .await
        .unwrap()
        .unwrap();
    let (epoch, frame) = state.raw_rx.recv().await.unwrap();
    state.apply_message(epoch, frame).await.unwrap();
    let DataEvent::Data(Data::Custom(source)) = events.try_recv().unwrap() else {
        panic!("source")
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
    let DataEvent::Data(Data::BookDepth(first)) = events.try_recv().unwrap() else {
        panic!("initial YES depth")
    };
    assert_eq!(first.ts_event, expected_ts);
    let mut book: Value =
        serde_json::from_str(include_str!("../../test_data/orderbook_2859346.json")).unwrap();
    book["updateTimestampMs"] = json!(1_700_000_000_122u64);
    book["bids"] = json!([["0.50", "9"]]);
    let stale = json!({"type":"M", "topic":format!("predictOrderbook/{ID}"), "data":book});
    state
        .apply_message(epoch, Message::Text(stale.to_string().into()))
        .await
        .unwrap();
    assert!(matches!(
        events.try_recv().unwrap(),
        DataEvent::Data(Data::Custom(_))
    ));
    assert!(
        events.try_recv().is_err(),
        "existing YES depth must reject the older frame"
    );
    state
        .apply_command(PredictCommand::SubscribeDepth(c.no_instrument_id, c, None))
        .await
        .unwrap();
    let DataEvent::Data(Data::BookDepth(late)) = events.try_recv().unwrap() else {
        panic!("late NO depth")
    };
    tasks.begin_shutdown();
    state.client.disconnect().await;
    server.abort();
    assert_eq!(
        late.ts_event, expected_ts,
        "late depth must retain the newest valid source"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn native_subscription_sets_track_partial_acceptance_and_retry() {
    use nautilus_common::{
        live::runner::replace_data_event_sender,
        messages::data::{
            SubscribeCommand, SubscribeTrades, UnsubscribeCommand, UnsubscribeTrades,
        },
    };
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_data::client::DataClientAdapter;
    use nautilus_model::identifiers::{ClientId, InstrumentId};

    let (events, _rx) = mpsc::unbounded_channel();
    replace_data_event_sender(events);
    let client_id = ClientId::from("PREDICT");
    let mut client = offline_client(TransportBackend::default());
    let c = context();
    client
        .dynamic_registry
        .write()
        .unwrap()
        .register(0, c.clone(), &[]);
    let (tx, mut queue) = mpsc::channel(1);
    client.command_tx = Some(tx);
    let mut adapter = DataClientAdapter::new(client_id, None, false, true, Box::new(client));
    let ids = vec![
        c.yes_instrument_id,
        InstrumentId::from("999-1.PREDICT"),
        c.no_instrument_id,
    ];
    let subscribe = SubscribeTrades::new(
        ids,
        Some(client_id),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    adapter.execute_subscribe(SubscribeCommand::Trades(subscribe.clone()));
    assert_eq!(
        adapter
            .subscriptions_trades
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![c.yes_instrument_id]
    );
    assert!(
        matches!(queue.try_recv().unwrap(), PredictCommand::SubscribeTrades(id, _) if id == c.yes_instrument_id)
    );
    // The invalid middle member cannot block a later valid member on retry.
    adapter.execute_subscribe(SubscribeCommand::Trades(subscribe));
    assert_eq!(adapter.subscriptions_trades.len(), 2);
    assert!(
        matches!(queue.try_recv().unwrap(), PredictCommand::SubscribeTrades(id, _) if id == c.no_instrument_id)
    );
    let unsubscribe = UnsubscribeTrades::new(
        vec![c.yes_instrument_id, c.no_instrument_id],
        Some(client_id),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    adapter.execute_unsubscribe(&UnsubscribeCommand::Trades(unsubscribe.clone()));
    assert!(!adapter.subscriptions_trades.contains(&c.yes_instrument_id));
    assert!(adapter.subscriptions_trades.contains(&c.no_instrument_id));
    assert!(
        matches!(queue.try_recv().unwrap(), PredictCommand::UnsubscribeTrades(id) if id == c.yes_instrument_id)
    );
    adapter.execute_unsubscribe(&UnsubscribeCommand::Trades(unsubscribe));
    assert!(adapter.subscriptions_trades.is_empty());
    assert!(
        matches!(queue.try_recv().unwrap(), PredictCommand::UnsubscribeTrades(id) if id == c.no_instrument_id)
    );
    drop(queue);
    adapter.execute_subscribe(SubscribeCommand::Trades(SubscribeTrades::new(
        c.yes_instrument_id,
        Some(client_id),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )));
    assert!(
        adapter.subscriptions_trades.is_empty(),
        "closed admission cannot add a Native subscription"
    );
}

fn source_frame(timestamp_ms: u64, empty: bool) -> Message {
    let mut book: Value =
        serde_json::from_str(include_str!("../../test_data/orderbook_2859346.json")).unwrap();
    book["updateTimestampMs"] = json!(timestamp_ms);
    book["bids"] = if empty {
        json!([])
    } else {
        json!([["0.50", "9"]])
    };
    if empty {
        book["asks"] = json!([]);
    }
    Message::Text(
        json!({"type":"M", "topic":format!("predictOrderbook/{ID}"), "data":book})
            .to_string()
            .into(),
    )
}

#[tokio::test]
async fn source_watermark_survives_reconnect_until_fresh_initialization() {
    for backend in [TransportBackend::default(), TransportBackend::Tungstenite] {
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
        let (mut state, tasks, mut events) = session(&url, backend).await;
        let c = context();
        state
            .apply_command(PredictCommand::SubscribeDepth(
                c.yes_instrument_id,
                c.clone(),
                None,
            ))
            .await
            .unwrap();
        commands.recv().await.unwrap();
        let epoch = state.current_epoch;
        state
            .apply_message(epoch, source_frame(123, false))
            .await
            .unwrap();
        assert!(matches!(
            events.try_recv().unwrap(),
            DataEvent::Data(Data::BookDepth(_))
        ));
        // Equal timestamps can replace levels, including a complete empty book.
        state
            .apply_message(epoch, source_frame(123, true))
            .await
            .unwrap();
        let DataEvent::Data(Data::BookDepth(empty)) = events.try_recv().unwrap() else {
            panic!("empty depth")
        };
        assert!(empty.bids.iter().all(|level| level.size.is_zero()));
        assert!(empty.asks.iter().all(|level| level.size.is_zero()));
        state
            .apply_command(PredictCommand::SubscribeDepth(
                c.no_instrument_id,
                c.clone(),
                None,
            ))
            .await
            .unwrap();
        let DataEvent::Data(Data::BookDepth(late_empty)) = events.try_recv().unwrap() else {
            panic!("late empty depth")
        };
        assert_eq!(late_empty.ts_event, empty.ts_event);
        assert_eq!(late_empty.ts_init, empty.ts_init);
        assert!(late_empty.bids.iter().all(|level| level.size.is_zero()));
        assert!(late_empty.asks.iter().all(|level| level.size.is_zero()));
        state
            .apply_command(PredictCommand::UnsubscribeDepth(c.no_instrument_id))
            .await
            .unwrap();
        state.fail_generation("source regression".into());
        assert!(state.books[&ID].snapshot.is_none());
        let (next_epoch, reconnected) = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let (epoch, frame) = state.raw_rx.recv().await.unwrap();
                if frame.as_text() == Some(nautilus_network::RECONNECTED) {
                    break (epoch, frame);
                }
            }
        })
        .await
        .unwrap();
        state.apply_message(next_epoch, reconnected).await.unwrap();
        assert!(state.books[&ID].snapshot.is_none());
        state
            .apply_message(epoch, source_frame(999, false))
            .await
            .unwrap();
        state
            .apply_message(next_epoch, source_frame(122, false))
            .await
            .unwrap();
        state
            .apply_command(PredictCommand::SubscribeDepth(
                c.no_instrument_id,
                c.clone(),
                None,
            ))
            .await
            .unwrap();
        assert!(
            events.try_recv().is_err(),
            "late owner waits for an admissible current-epoch snapshot"
        );
        state
            .apply_message(next_epoch, source_frame(123, false))
            .await
            .unwrap();
        let depths: Vec<_> = std::iter::from_fn(|| events.try_recv().ok()).collect();
        assert_eq!(depths.len(), 2);
        assert!(depths.iter().all(|event| matches!(event, DataEvent::Data(Data::BookDepth(depth)) if depth.ts_event.as_u64() == 123_000_000)));
        state
            .apply_command(PredictCommand::UnsubscribeDepth(c.yes_instrument_id))
            .await
            .unwrap();
        assert!(
            state.books.contains_key(&ID),
            "remaining owner retains source state"
        );
        state
            .apply_command(PredictCommand::UnsubscribeDepth(c.no_instrument_id))
            .await
            .unwrap();
        assert!(state.books.is_empty());
        state
            .apply_message(next_epoch, source_frame(124, false))
            .await
            .unwrap();
        assert!(state.books.is_empty() && events.try_recv().is_err());
        tasks.begin_shutdown();
        state.client.disconnect().await;
        server.abort();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_depth_release_remains_accepted_after_terminal_write_failure() {
    use nautilus_common::{
        live::runner::replace_data_event_sender,
        messages::data::{
            SubscribeBookDepth, SubscribeCommand, UnsubscribeBookDepth, UnsubscribeCommand,
        },
    };
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_data::client::DataClientAdapter;
    use nautilus_model::{enums::BookType, identifiers::ClientId};

    for backend in [TransportBackend::default(), TransportBackend::Tungstenite] {
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
        let (mut state, tasks, mut events) = session(&url, backend).await;
        let c = context();
        let client_id = ClientId::from("PREDICT");
        let (sender, _rx) = mpsc::unbounded_channel();
        replace_data_event_sender(sender);
        let mut client = offline_client(backend);
        client
            .dynamic_registry
            .write()
            .unwrap()
            .register(0, c.clone(), &[]);
        let (tx, rx) = mpsc::channel(1);
        client.command_tx = Some(tx);
        state.command_rx = rx;
        let mut native = DataClientAdapter::new(client_id, None, false, true, Box::new(client));
        native.execute_subscribe(SubscribeCommand::BookDepth(SubscribeBookDepth::new(
            vec![c.yes_instrument_id, c.no_instrument_id],
            BookType::L1_MBP,
            Some(client_id),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            false,
            None,
            None,
        )));
        assert!(native.subscriptions_book_depth.is_empty());
        assert!(
            state.command_rx.try_recv().is_err(),
            "invalid command-wide options have no effects"
        );
        native.execute_subscribe(SubscribeCommand::BookDepth(SubscribeBookDepth::new(
            c.yes_instrument_id,
            BookType::L2_MBP,
            Some(client_id),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            false,
            None,
            None,
        )));
        assert!(
            native
                .subscriptions_book_depth
                .contains(&c.yes_instrument_id)
        );
        let command = state.command_rx.recv().await.unwrap();
        state.apply_command(command).await.unwrap();
        assert_eq!(commands.recv().await.unwrap()["method"], "subscribe");
        let (epoch, frame) = state.raw_rx.recv().await.unwrap();
        state.apply_message(epoch, frame).await.unwrap();
        assert!(matches!(
            events.try_recv().unwrap(),
            DataEvent::Data(Data::BookDepth(_))
        ));
        state.client.disconnect().await;
        // A physical outage invalidates bootstrap even before RECONNECTED reaches the session.
        state
            .apply_command(PredictCommand::SubscribeDepth(
                c.no_instrument_id,
                c.clone(),
                None,
            ))
            .await
            .unwrap();
        assert!(state.books[&ID].snapshot.is_none());
        assert!(events.try_recv().is_err());
        state
            .apply_command(PredictCommand::UnsubscribeDepth(c.no_instrument_id))
            .await
            .unwrap();
        native.execute_unsubscribe(&UnsubscribeCommand::BookDepth(UnsubscribeBookDepth::new(
            c.yes_instrument_id,
            Some(client_id),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )));
        assert!(native.subscriptions_book_depth.is_empty());
        let command = state.command_rx.recv().await.unwrap();
        let error = state.apply_command(command).await.unwrap_err();
        assert!(
            state.depths.is_empty() && state.subscriptions.is_empty() && state.books.is_empty()
        );
        state.fail_generation(error.to_string());
        assert!(state.cancellation.is_cancelled());
        assert!(
            native.subscriptions_book_depth.is_empty(),
            "async failure cannot restore accepted cancellation"
        );
        tasks.begin_shutdown();
        server.abort();
    }
}

fn offline_client(backend: TransportBackend) -> crate::PredictDataClient {
    crate::PredictDataClient::new(
        nautilus_model::identifiers::ClientId::from("PREDICT"),
        PredictDataClientConfig {
            api_key: SecretString::from("offline-test-only"),
            market_ids: vec![],
            base_url_http: None,
            base_url_ws: None,
            http_timeout_secs: 2,
            ws_timeout_secs: 2,
            dynamic_refresh_secs: 1,
            transport_backend: backend,
        },
    )
    .unwrap()
}
