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

//! Colliding discovery completions exercise the session's custom-data ownership boundary.

use std::{rc::Rc, sync::LazyLock};

use axum::{
    Router,
    extract::{
        Path, State,
        ws::{Message as WsMessage, WebSocketUpgrade},
    },
    http::HeaderMap,
    response::Response,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use nautilus_common::messages::DataEvent;
use nautilus_common::{
    clients::DataClient,
    live::runner::replace_data_event_sender,
    messages::data::{SubscribeCustomData, UnsubscribeCustomData},
};
use nautilus_core::{Params, UUID4, time::AtomicTime};
use nautilus_live::task::TaskGroup;
use nautilus_model::{data::Data, identifiers::ClientId, instruments::Instrument};
use nautilus_network::{RECONNECTED, transport::Message, websocket::TransportBackend};
use serde_json::{Value, json};

use super::*;
use crate::{
    PredictOrderbookSnapshot, SelectedCryptoUpDownMarket, common::CryptoUpDownMarketWindow,
    predict_crypto_up_down_orderbook_data_type, predict_orderbook_data_type,
};

const ID: u64 = 2_859_346;
const NOW: u64 = 1_791_064_900_000_000_000;
static CLOCK: LazyLock<AtomicTime> = LazyLock::new(|| AtomicTime::new(false, NOW.into()));

fn market(id: u64) -> Value {
    let mut value: Value =
        serde_json::from_str(include_str!("../../test_data/market_2859346.json")).unwrap();
    value["data"]["id"] = json!(id);
    value
}

async fn definition(Path(id): Path<u64>, headers: HeaderMap) -> String {
    assert_eq!(headers["x-api-key"], "loopback-only");
    market(id).to_string()
}

async fn category(headers: HeaderMap) -> String {
    assert_eq!(headers["x-api-key"], "loopback-only");
    json!({"success":true,"data":{"markets":[market(ID)["data"].clone()]}}).to_string()
}

type FrameSender = mpsc::UnboundedSender<WsMessage>;

#[derive(Clone)]
struct VenueState {
    sockets: mpsc::UnboundedSender<FrameSender>,
    commands: mpsc::UnboundedSender<Value>,
}

async fn socket(ws: WebSocketUpgrade, State(state): State<VenueState>) -> Response {
    ws.on_upgrade(move |socket| async move {
        let (mut sink, mut source) = socket.split();
        let (tx, mut rx) = mpsc::unbounded_channel();
        state.sockets.send(tx).unwrap();
        loop {
            tokio::select! {
                message = source.next() => {
                    let Some(Ok(WsMessage::Text(text))) = message else { break };
                    let command: Value = serde_json::from_str(&text).unwrap();
                    if state.commands.send(command).is_err() {
                        break;
                    }
                }
                Some(message) = rx.recv() => {
                    let closing = matches!(message, WsMessage::Close(_));
                    if sink.send(message).await.is_err() || closing {
                        break;
                    }
                }
            }
        }
    })
}

struct Venue {
    server: tokio::task::JoinHandle<()>,
    sockets: mpsc::UnboundedReceiver<FrameSender>,
    commands: mpsc::UnboundedReceiver<Value>,
    frames: FrameSender,
    url: String,
}

impl Drop for Venue {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn receive<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("loopback event deadline")
        .expect("loopback channel remains open")
}

impl Venue {
    async fn command(&mut self, method: &str, market: u64) {
        let command = receive(&mut self.commands).await;
        assert_eq!(command["method"], method);
        assert_eq!(
            command["params"],
            json!([format!("predictOrderbook/{market}")])
        );
    }

    async fn no_commands(&mut self, session: &PredictDataSession) {
        // Writer ordering makes the heartbeat a barrier for preceding wire commands
        let barrier = json!({"method":"heartbeat", "data":"custom-owner-barrier"});
        session
            .client
            .send_text(barrier.to_string(), None)
            .await
            .unwrap();
        assert_eq!(receive(&mut self.commands).await, barrier);
    }
}

async fn setup(
    backend: TransportBackend,
) -> (
    Venue,
    PredictDataSession,
    TaskGroup,
    mpsc::UnboundedReceiver<DataEvent>,
) {
    let (socket_tx, mut sockets) = mpsc::unbounded_channel();
    let (command_tx, commands) = mpsc::unbounded_channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/ws", get(socket))
                .route("/v1/markets/{id}", get(definition))
                .route("/v1/categories/{slug}", get(category))
                .with_state(VenueState {
                    sockets: socket_tx,
                    commands: command_tx,
                }),
        )
        .await
        .unwrap();
    });
    let (mut session, tasks, events) = super::quote_session_tests::session(&url, backend).await;
    session.clock = &CLOCK;
    let frames = receive(&mut sockets).await;
    (
        Venue {
            server,
            sockets,
            commands,
            frames,
            url,
        },
        session,
        tasks,
        events,
    )
}

fn selector(feed: &str) -> PredictCryptoUpDownSelector {
    PredictCryptoUpDownSelector {
        price_feed_symbol: feed.into(),
        title_asset: "Bitcoin".into(),
        interval_secs: 300,
    }
}

fn discovery(
    session: &mut PredictDataSession,
    selector: &PredictCryptoUpDownSelector,
    market: u64,
) -> DiscoveryResult {
    let key = dynamic_selector_key(selector).unwrap();
    if !session.dynamic_subscriptions.contains_key(&key) {
        let version = session.discovery.version().unwrap();
        session.dynamic_subscriptions.insert(
            key.clone(),
            DynamicSubscription {
                selector: selector.clone(),
                version,
                current_market_id: None,
                pending_market_id: None,
                next_refresh_ns: u64::MAX,
            },
        );
    }
    let version = session.dynamic_subscriptions[&key].version;
    session
        .discovery
        .inflight
        .insert(key.clone(), (version, session.cancellation.child_token()));
    let mut definition = self::market(market);
    DiscoveryResult {
        key,
        version,
        result: Ok((
            SelectedCryptoUpDownMarket {
                market_id: market,
                window: CryptoUpDownMarketWindow {
                    title_asset: "Bitcoin".into(),
                    start_ns: NOW - 1_000_000_000,
                    end_ns: NOW + 300_000_000_000,
                },
            },
            Some(serde_json::from_value(definition["data"].take()).unwrap()),
        )),
    }
}

fn definitions(events: &mut mpsc::UnboundedReceiver<DataEvent>, market: u64) {
    for outcome in [1, 2] {
        let DataEvent::Instrument(instrument) = events.try_recv().unwrap() else {
            panic!("definitions precede snapshots");
        };
        assert_eq!(
            instrument.id(),
            crate::instrument_id_from_outcome(market, outcome).unwrap()
        );
    }
    assert!(events.try_recv().is_err());
}

fn frame(market: u64, count: u64) -> String {
    let mut book: Value =
        serde_json::from_str(include_str!("../../test_data/orderbook_2859346.json")).unwrap();
    book["marketId"] = json!(market);
    book["lastOrderSettled"]["marketId"] = json!(market);
    book["orderCount"] = json!(count);
    book["updateTimestampMs"] = json!(1_700_000_000_123u64 + count);
    json!({"type":"M", "topic":format!("predictOrderbook/{market}"), "data":book}).to_string()
}

async fn publish(session: &mut PredictDataSession, venue: &Venue, market: u64, count: u64) {
    venue
        .frames
        .send(WsMessage::Text(frame(market, count).into()))
        .unwrap();
    let (epoch, message) = tokio::time::timeout(Duration::from_secs(5), session.raw_rx.recv())
        .await
        .unwrap()
        .unwrap();
    session.handle_message(epoch, message).await.unwrap();
}

fn outputs(
    events: &mut mpsc::UnboundedReceiver<DataEvent>,
    expected: Option<&DataType>,
    market: u64,
    count: u64,
    native: bool,
) {
    let (mut custom, mut quotes, mut depths) = (0, 0, 0);
    while let Ok(event) = events.try_recv() {
        match event {
            DataEvent::Data(Data::Custom(data)) => {
                assert_eq!(&data.data_type, expected.expect("custom interest required"));
                let book = data
                    .data
                    .as_any()
                    .downcast_ref::<PredictOrderbookSnapshot>()
                    .unwrap();
                assert_eq!((book.market_id, book.order_count), (market, count));
                assert_eq!(book.bids[0].price.to_string(), "0.62");
                assert!(!book.asks.is_empty());
                custom += 1;
            }
            DataEvent::Data(Data::Quote(quote)) => {
                assert_eq!(
                    quote.instrument_id,
                    crate::instrument_id_from_outcome(market, 1).unwrap()
                );
                assert_eq!(quote.bid_price.to_string(), "0.62");
                quotes += 1;
            }
            DataEvent::Data(Data::BookDepth(depth)) => {
                assert_eq!(
                    depth.instrument_id,
                    crate::instrument_id_from_outcome(market, 1).unwrap()
                );
                assert!(!depth.bids.is_empty() && !depth.asks.is_empty());
                depths += 1;
            }
            event => panic!("unexpected data event {event:?}"),
        }
    }
    assert_eq!(
        (custom, quotes, depths),
        (
            usize::from(expected.is_some()),
            usize::from(native),
            usize::from(native)
        )
    );
}

async fn native(session: &mut PredictDataSession) {
    let context = super::quote_session_tests::context();
    let id = context.yes_instrument_id;
    session
        .handle_command(PredictCommand::SubscribeQuotes(id, context.clone()))
        .await
        .unwrap();
    session
        .handle_command(PredictCommand::SubscribeDepth(id, context, None))
        .await
        .unwrap();
}

async fn shutdown(session: PredictDataSession, tasks: TaskGroup) {
    tasks.begin_shutdown();
    session.client.disconnect().await;
    tasks
        .finish_shutdown(Duration::from_secs(1), Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn indexed_lookup_preserves_identity_precedence_and_retirement() {
    let (mut venue, mut session, tasks, mut events) = setup(TransportBackend::Tungstenite).await;
    let (sender, mut definitions) = mpsc::unbounded_channel();
    replace_data_event_sender(sender);
    let mut client = crate::PredictDataClient::new(
        ClientId::new("PREDICT"),
        PredictDataClientConfig {
            api_key: "loopback-only".into(),
            market_ids: vec![ID],
            base_url_http: Some(venue.url.clone()),
            base_url_ws: Some(format!("{}/ws", venue.url.replacen("http", "ws", 1))),
            http_timeout_secs: 2,
            ws_timeout_secs: 2,
            dynamic_refresh_secs: 1,
            transport_backend: TransportBackend::Tungstenite,
        },
    )
    .unwrap();
    client.dynamic_registry = Arc::clone(&session.dynamic_registry);
    client.bootstrap_markets().await.unwrap();
    for outcome in [1, 2] {
        let DataEvent::Instrument(definition) = receive(&mut definitions).await else {
            panic!("bootstrap definition missing")
        };
        assert_eq!(
            definition.id(),
            crate::instrument_id_from_outcome(ID, outcome).unwrap()
        );
    }
    for (market_id, precision) in [(ID, 3), (ID + 1, 2), (ID + 2, 4)] {
        let mut raw = market(market_id)["data"].clone();
        raw["decimalPrecision"] = json!(precision);
        let market = serde_json::from_value(raw).unwrap();
        let (context, instruments) =
            crate::providers::instruments_from_market(&market, NOW.into()).unwrap();
        assert!(client.dynamic_registry.write().unwrap().register(
            session.dynamic_generation,
            context,
            &instruments
        ));
    }
    for (market_id, precision) in [(ID, 2), (ID + 1, 2), (ID + 2, 4)] {
        for outcome in [1, 2] {
            let id = crate::instrument_id_from_outcome(market_id, outcome).unwrap();
            let context = client.instrument_context(id).unwrap();
            assert_eq!(
                (context.market_id, context.price_precision),
                (market_id, precision)
            );
            client.publish_instrument(id).unwrap();
            let DataEvent::Instrument(definition) = receive(&mut definitions).await else {
                panic!("lookup publication missing")
            };
            assert_eq!(
                (definition.id(), definition.price_precision()),
                (id, precision)
            );
        }
    }
    for value in [
        "2859349-1.PREDICT",
        "02859346-1.PREDICT",
        "2859346-01.PREDICT",
        "2859346-3.PREDICT",
        "2859346.PREDICT",
        "2859346-1.OTHER",
        "0-1.PREDICT",
    ] {
        let id = value.parse().unwrap();
        assert_eq!(
            client.instrument_context(id).unwrap_err().to_string(),
            format!("Predict instrument {id} was not loaded")
        );
        assert_eq!(
            client.publish_instrument(id).unwrap_err().to_string(),
            format!("Predict instrument {id} is not loaded")
        );
    }
    let id = crate::instrument_id_from_outcome(ID + 1, 1).unwrap();
    let context = client.instrument_context(id).unwrap();
    session
        .handle_command(PredictCommand::SubscribeQuotes(id, context.clone()))
        .await
        .unwrap();
    session
        .handle_command(PredictCommand::SubscribeDepth(id, context, None))
        .await
        .unwrap();
    venue.command("subscribe", ID + 1).await;
    session.remove_dynamic_market_if_unused(ID + 1);
    assert!(client.instrument_context(id).is_err());
    assert!(client.publish_instrument(id).is_err());
    publish(&mut session, &venue, ID + 1, 1).await;
    outputs(&mut events, None, ID + 1, 1, true);
    session
        .handle_command(PredictCommand::UnsubscribeQuotes(id))
        .await
        .unwrap();
    venue.no_commands(&session).await;
    session
        .handle_command(PredictCommand::UnsubscribeDepth(id))
        .await
        .unwrap();
    venue.command("unsubscribe", ID + 1).await;
    publish(&mut session, &venue, ID + 1, 2).await;
    assert!(events.try_recv().is_err());
    client.reset().unwrap();
    for market_id in [ID, ID + 2] {
        for outcome in [1, 2] {
            let id = crate::instrument_id_from_outcome(market_id, outcome).unwrap();
            assert!(client.instrument_context(id).is_err());
            assert!(client.publish_instrument(id).is_err());
        }
    }
    assert!(definitions.try_recv().is_err());
    client.disconnect().await.unwrap();
    shutdown(session, tasks).await;
}

#[tokio::test]
async fn explicit_custom_identity_rejects_selector_and_preserves_native_sharing() {
    for backend in [TransportBackend::default(), TransportBackend::Tungstenite] {
        let (mut venue, mut session, tasks, mut events) = setup(backend).await;
        session
            .handle_command(PredictCommand::Subscribe(ID))
            .await
            .unwrap();
        venue.command("subscribe", ID).await;
        native(&mut session).await;
        let selector = selector("BTCUSDT");
        let result = discovery(&mut session, &selector, ID);
        let key = result.key.clone();
        let e = session.apply_discovery_result(result).await.unwrap_err();
        assert!(e.to_string().contains("custom data"), "{e:#}");
        assert_eq!(session.subscriptions[&ID].owners, 3);
        assert_eq!(session.dynamic_subscriptions[&key].pending_market_id, None);
        assert!(events.try_recv().is_err());
        session
            .handle_command(PredictCommand::StopDynamic(key))
            .await
            .unwrap();
        publish(&mut session, &venue, ID, 1).await;
        outputs(
            &mut events,
            Some(&predict_orderbook_data_type(ID)),
            ID,
            1,
            true,
        );
        venue.no_commands(&session).await;
        session
            .handle_command(PredictCommand::Unsubscribe(ID))
            .await
            .unwrap();
        publish(&mut session, &venue, ID, 2).await;
        outputs(&mut events, None, ID, 2, true);
        let id = crate::instrument_id_from_outcome(ID, 1).unwrap();
        session
            .handle_command(PredictCommand::UnsubscribeQuotes(id))
            .await
            .unwrap();
        venue.no_commands(&session).await;
        session
            .handle_command(PredictCommand::UnsubscribeDepth(id))
            .await
            .unwrap();
        venue.command("unsubscribe", ID).await;
        assert!(session.subscriptions.is_empty());
        shutdown(session, tasks).await;
    }
}

#[tokio::test]
async fn selector_identity_rejects_explicit_subscribe_and_foreign_unsubscribe() {
    for backend in [TransportBackend::default(), TransportBackend::Tungstenite] {
        for confirmed in [false, true] {
            let (mut venue, mut session, tasks, mut events) = setup(backend).await;
            let product = selector("BTCUSDT");
            let result = discovery(&mut session, &product, ID);
            let key = result.key.clone();
            let expected = predict_crypto_up_down_orderbook_data_type(ID, &key);
            session.apply_discovery_result(result).await.unwrap();
            definitions(&mut events, ID);
            venue.command("subscribe", ID).await;
            if confirmed {
                session
                    .handle_command(PredictCommand::Unsubscribe(ID))
                    .await
                    .unwrap();
                publish(&mut session, &venue, ID, 1).await;
                outputs(&mut events, Some(&expected), ID, 1, false);
                assert_eq!(session.subscriptions[&ID].owners, 1);
                venue.no_commands(&session).await;
            }
            native(&mut session).await;
            session
                .handle_command(PredictCommand::StartDynamic(selector(" btc/usdt ")))
                .await
                .unwrap();
            let e = session
                .handle_command(PredictCommand::Subscribe(ID))
                .await
                .unwrap_err();
            assert!(e.to_string().contains("custom data"), "{e:#}");
            session
                .handle_command(PredictCommand::Unsubscribe(ID))
                .await
                .unwrap();
            assert_eq!(session.subscriptions[&ID].owners, 3);
            publish(&mut session, &venue, ID, 2).await;
            outputs(&mut events, Some(&expected), ID, 2, true);
            venue.no_commands(&session).await;
            session
                .handle_command(PredictCommand::StopDynamic(key))
                .await
                .unwrap();
            publish(&mut session, &venue, ID, 3).await;
            outputs(&mut events, None, ID, 3, true);
            session
                .handle_command(PredictCommand::Subscribe(ID))
                .await
                .unwrap();
            publish(&mut session, &venue, ID, 4).await;
            outputs(
                &mut events,
                Some(&predict_orderbook_data_type(ID)),
                ID,
                4,
                true,
            );
            venue.no_commands(&session).await;
            assert!(session.failed_epoch.is_none() && !session.cancellation.is_cancelled());
            shutdown(session, tasks).await;
        }
    }
}

#[tokio::test]
async fn colliding_selector_preserves_pending_handover_and_retries_after_release() {
    for backend in [TransportBackend::default(), TransportBackend::Tungstenite] {
        let (mut venue, mut session, tasks, mut events) = setup(backend).await;
        let first = selector("BTCUSDT");
        let second = selector("BTCUSD");
        let first_key = dynamic_selector_key(&first).unwrap();
        let second_key = dynamic_selector_key(&second).unwrap();
        for (selector, market) in [(&first, ID), (&first, ID + 1), (&second, ID + 2)] {
            let result = discovery(&mut session, selector, market);
            session.apply_discovery_result(result).await.unwrap();
            definitions(&mut events, market);
            venue.command("subscribe", market).await;
            if market == ID {
                publish(&mut session, &venue, ID, 1).await;
                outputs(
                    &mut events,
                    Some(&predict_crypto_up_down_orderbook_data_type(ID, &first_key)),
                    ID,
                    1,
                    false,
                );
            }
        }
        let result = discovery(&mut session, &first, ID + 2);
        assert!(session.apply_discovery_result(result).await.is_err());
        assert_eq!(
            session.dynamic_subscriptions[&first_key].current_market_id,
            Some(ID)
        );
        assert_eq!(
            session.dynamic_subscriptions[&first_key].pending_market_id,
            Some(ID + 1)
        );
        assert_eq!(session.subscriptions.len(), 3);
        assert!(events.try_recv().is_err());
        venue.no_commands(&session).await;
        publish(&mut session, &venue, ID + 2, 2).await;
        outputs(
            &mut events,
            Some(&predict_crypto_up_down_orderbook_data_type(
                ID + 2,
                &second_key,
            )),
            ID + 2,
            2,
            false,
        );
        publish(&mut session, &venue, ID + 1, 3).await;
        outputs(
            &mut events,
            Some(&predict_crypto_up_down_orderbook_data_type(
                ID + 1,
                &first_key,
            )),
            ID + 1,
            3,
            false,
        );
        venue.command("unsubscribe", ID).await;
        session
            .handle_command(PredictCommand::StopDynamic(second_key))
            .await
            .unwrap();
        venue.command("unsubscribe", ID + 2).await;
        let result = discovery(&mut session, &first, ID + 2);
        session.apply_discovery_result(result).await.unwrap();
        definitions(&mut events, ID + 2);
        venue.command("subscribe", ID + 2).await;
        publish(&mut session, &venue, ID + 2, 4).await;
        outputs(
            &mut events,
            Some(&predict_crypto_up_down_orderbook_data_type(
                ID + 2,
                &first_key,
            )),
            ID + 2,
            4,
            false,
        );
        venue.command("unsubscribe", ID + 1).await;
        shutdown(session, tasks).await;
    }
}

#[tokio::test]
async fn retired_rejected_custom_interest_clears_native_dedup_and_can_retry() {
    for backend in [TransportBackend::default(), TransportBackend::Tungstenite] {
        let (mut venue, mut session, tasks, mut events) = setup(backend).await;
        let selector = selector("BTCUSDT");
        let result = discovery(&mut session, &selector, ID + 1);
        let key = result.key.clone();
        let selector_type = predict_crypto_up_down_orderbook_data_type(ID + 1, &key);
        session.apply_discovery_result(result).await.unwrap();
        definitions(&mut events, ID + 1);
        venue.command("subscribe", ID + 1).await;

        let config = PredictDataClientConfig {
            api_key: "loopback-only".into(),
            market_ids: vec![],
            base_url_http: Some(venue.url.clone()),
            base_url_ws: Some(format!("{}/ws", venue.url.replacen("http", "ws", 1))),
            http_timeout_secs: 2,
            ws_timeout_secs: 2,
            dynamic_refresh_secs: 1,
            transport_backend: backend,
        };
        let (sender, _receiver) = mpsc::unbounded_channel();
        replace_data_event_sender(sender);
        let mut client = crate::PredictDataClient::new(ClientId::new("PREDICT"), config).unwrap();
        client.dynamic_registry = Arc::clone(&session.dynamic_registry);
        let (command_tx, mut command_rx) = mpsc::channel(8);
        client.command_tx = Some(command_tx);
        let native = nautilus_data::client::DataClientAdapter::new(
            ClientId::new("PREDICT"),
            None,
            true,
            true,
            Box::new(client),
        );
        let node = nautilus_live::node::LiveNode::builder(
            nautilus_model::identifiers::TraderId::new("CUSTOM-001"),
            nautilus_common::enums::Environment::Live,
        )
        .unwrap()
        .build()
        .unwrap();
        let engine = Rc::clone(&node.kernel().data_engine);
        engine.borrow_mut().register_client(native, None);
        let data_type = predict_orderbook_data_type(ID + 1);
        let subscribe = || {
            SubscribeCustomData::new(
                Some(ClientId::new("PREDICT")),
                None,
                data_type.clone(),
                UUID4::new(),
                NOW.into(),
                None,
                None,
            )
        };
        let unsubscribe = || {
            UnsubscribeCustomData::new(
                Some(ClientId::new("PREDICT")),
                None,
                data_type.clone(),
                UUID4::new(),
                NOW.into(),
                None,
                None,
            )
        };

        engine
            .borrow_mut()
            .execute_subscribe(nautilus_common::messages::SubscribeCommand::Data(
                subscribe(),
            ))
            .unwrap();
        assert!(
            engine
                .borrow()
                .subscribed_custom_data()
                .contains(&data_type)
        );
        assert!(
            session
                .handle_command(command_rx.recv().await.unwrap())
                .await
                .is_err()
        );
        publish(&mut session, &venue, ID + 1, 1).await;
        outputs(&mut events, Some(&selector_type), ID + 1, 1, false);
        session
            .handle_command(PredictCommand::StopDynamic(key))
            .await
            .unwrap();
        venue.command("unsubscribe", ID + 1).await;
        engine
            .borrow_mut()
            .execute_unsubscribe(&nautilus_common::messages::UnsubscribeCommand::Data(
                unsubscribe(),
            ))
            .unwrap();
        assert!(
            !engine
                .borrow()
                .subscribed_custom_data()
                .contains(&data_type)
        );
        session
            .handle_command(command_rx.recv().await.unwrap())
            .await
            .unwrap();
        venue.no_commands(&session).await;

        let result = discovery(&mut session, &selector, ID + 1);
        let key = result.key.clone();
        session.apply_discovery_result(result).await.unwrap();
        definitions(&mut events, ID + 1);
        venue.command("subscribe", ID + 1).await;
        engine
            .borrow_mut()
            .execute_subscribe(nautilus_common::messages::SubscribeCommand::Data(
                subscribe(),
            ))
            .unwrap();
        assert!(
            engine
                .borrow()
                .subscribed_custom_data()
                .contains(&data_type)
        );
        assert!(
            session
                .handle_command(command_rx.recv().await.unwrap())
                .await
                .is_err()
        );
        publish(&mut session, &venue, ID + 1, 2).await;
        outputs(&mut events, Some(&selector_type), ID + 1, 2, false);
        engine
            .borrow_mut()
            .execute_unsubscribe(&nautilus_common::messages::UnsubscribeCommand::Data(
                unsubscribe(),
            ))
            .unwrap();
        session
            .handle_command(command_rx.recv().await.unwrap())
            .await
            .unwrap();
        assert!(
            !engine
                .borrow()
                .subscribed_custom_data()
                .contains(&data_type)
        );
        session
            .handle_command(PredictCommand::StopDynamic(key))
            .await
            .unwrap();
        venue.command("unsubscribe", ID + 1).await;
        shutdown(session, tasks).await;
    }
}

#[tokio::test]
async fn rejected_custom_command_keeps_running_session_and_original_output() {
    let (mut venue, mut session, tasks, mut events) = setup(TransportBackend::Tungstenite).await;
    let result = discovery(&mut session, &selector("BTCUSDT"), ID);
    let expected = predict_crypto_up_down_orderbook_data_type(ID, &result.key);
    session.apply_discovery_result(result).await.unwrap();
    definitions(&mut events, ID);
    venue.command("subscribe", ID).await;
    let (tx, rx) = mpsc::channel(8);
    session.command_rx = rx;
    tasks.spawn(session.run()).unwrap();
    tx.send(PredictCommand::Subscribe(ID)).await.unwrap();
    tx.send(PredictCommand::Unsubscribe(ID)).await.unwrap();
    tx.send(PredictCommand::Subscribe(ID + 1)).await.unwrap();
    venue.command("subscribe", ID + 1).await;
    venue
        .frames
        .send(WsMessage::Text(frame(ID, 1).into()))
        .unwrap();
    let event = receive(&mut events).await;
    let (output, mut received) = mpsc::unbounded_channel();
    output.send(event).unwrap();
    outputs(&mut received, Some(&expected), ID, 1, false);
    assert!(venue.sockets.try_recv().is_err());
    tasks.begin_shutdown();
    tasks
        .finish_shutdown(Duration::from_secs(1), Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test]
async fn custom_identity_survives_reconnect_and_does_not_leak_into_new_session() {
    for backend in [TransportBackend::default(), TransportBackend::Tungstenite] {
        let (mut venue, mut session, tasks, mut events) = setup(backend).await;
        let selector = selector("BTCUSDT");
        let result = discovery(&mut session, &selector, ID);
        let expected = predict_crypto_up_down_orderbook_data_type(ID, &result.key);
        session.apply_discovery_result(result).await.unwrap();
        definitions(&mut events, ID);
        venue.command("subscribe", ID).await;
        publish(&mut session, &venue, ID, 0).await;
        outputs(&mut events, Some(&expected), ID, 0, false);
        assert!(
            session
                .handle_command(PredictCommand::Subscribe(ID))
                .await
                .is_err()
        );
        let old_epoch = session.current_epoch;
        venue.frames.send(WsMessage::Close(None)).unwrap();
        venue.frames = receive(&mut venue.sockets).await;
        let (epoch, message) = tokio::time::timeout(Duration::from_secs(5), session.raw_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.as_text(), Some(RECONNECTED));
        assert!(epoch > old_epoch);
        session.handle_message(epoch, message).await.unwrap();
        venue.command("subscribe", ID).await;
        session
            .handle_message(old_epoch, Message::Text(frame(ID, 1).into()))
            .await
            .unwrap();
        assert!(events.try_recv().is_err());
        publish(&mut session, &venue, ID, 2).await;
        outputs(&mut events, Some(&expected), ID, 2, false);
        let stale = discovery(&mut session, &selector, ID + 1);
        tasks.begin_shutdown();
        session.apply_discovery_result(stale).await.unwrap();
        assert!(events.try_recv().is_err());
        assert_eq!(session.subscriptions.len(), 1);
        assert_eq!(
            session.dynamic_subscriptions[&dynamic_selector_key(&selector).unwrap()]
                .pending_market_id,
            None
        );
        let old_results = session.discovery_tx.clone();
        shutdown(session, tasks).await;
        assert!(old_results.is_closed());

        let (mut fresh_venue, mut fresh, fresh_tasks, mut fresh_events) = setup(backend).await;
        assert!(fresh.subscriptions.is_empty() && fresh.dynamic_subscriptions.is_empty());
        fresh
            .handle_command(PredictCommand::Subscribe(ID))
            .await
            .unwrap();
        fresh_venue.command("subscribe", ID).await;
        publish(&mut fresh, &fresh_venue, ID, 3).await;
        outputs(
            &mut fresh_events,
            Some(&predict_orderbook_data_type(ID)),
            ID,
            3,
            false,
        );
        shutdown(fresh, fresh_tasks).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn client_reset_releases_rejected_identity_and_accepts_explicit_snapshots() {
    let (mut venue, session, tasks, _) = setup(TransportBackend::Tungstenite).await;
    let config = PredictDataClientConfig {
        api_key: "loopback-only".into(),
        market_ids: vec![ID, ID + 1],
        base_url_http: Some(venue.url.clone()),
        base_url_ws: Some(format!("{}/ws", venue.url.replacen("http", "ws", 1))),
        http_timeout_secs: 2,
        ws_timeout_secs: 2,
        dynamic_refresh_secs: 1,
        transport_backend: TransportBackend::Tungstenite,
    };
    shutdown(session, tasks).await;
    let (tx, mut events) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = crate::PredictDataClient::new(ClientId::new("PREDICT"), config).unwrap();
    client.clock = &CLOCK;
    client.connect().await.unwrap();
    venue.frames = receive(&mut venue.sockets).await;
    for _ in 0..4 {
        assert!(matches!(
            receive(&mut events).await,
            DataEvent::Instrument(_)
        ));
    }
    let subscribe = |data_type| {
        SubscribeCustomData::new(
            Some(ClientId::new("PREDICT")),
            None,
            data_type,
            UUID4::new(),
            NOW.into(),
            None,
            None,
        )
    };
    client
        .subscribe(subscribe(DataType::new(
            "PredictCryptoUpDown",
            Some(Params::new(
                [
                    ("price_feed_symbol".into(), json!("BTCUSDT")),
                    ("title_asset".into(), json!("Bitcoin")),
                    ("interval_secs".into(), json!("300")),
                ]
                .into(),
            )),
            None,
        )))
        .unwrap();
    for outcome in [1, 2] {
        let DataEvent::Instrument(instrument) = receive(&mut events).await else {
            panic!("discovered definitions precede snapshots");
        };
        assert_eq!(
            instrument.id(),
            crate::instrument_id_from_outcome(ID, outcome).unwrap()
        );
    }
    venue.command("subscribe", ID).await;
    client
        .subscribe(subscribe(predict_orderbook_data_type(ID)))
        .unwrap();
    client
        .unsubscribe(&UnsubscribeCustomData::new(
            Some(ClientId::new("PREDICT")),
            None,
            predict_orderbook_data_type(ID),
            UUID4::new(),
            NOW.into(),
            None,
            None,
        ))
        .unwrap();
    client
        .subscribe(subscribe(predict_orderbook_data_type(ID + 1)))
        .unwrap();
    venue.command("subscribe", ID + 1).await;
    venue
        .frames
        .send(WsMessage::Text(frame(ID, 1).into()))
        .unwrap();
    let DataEvent::Data(Data::Custom(data)) = receive(&mut events).await else {
        panic!("the selector survives a competing explicit subscribe and unsubscribe");
    };
    let key = dynamic_selector_key(&selector("BTCUSDT")).unwrap();
    assert_eq!(
        data.data_type,
        predict_crypto_up_down_orderbook_data_type(ID, &key)
    );
    client.reset().unwrap();
    client.connect().await.unwrap();
    venue.frames = receive(&mut venue.sockets).await;
    for _ in 0..4 {
        assert!(matches!(
            receive(&mut events).await,
            DataEvent::Instrument(_)
        ));
    }
    client
        .subscribe(subscribe(predict_orderbook_data_type(ID)))
        .unwrap();
    venue.command("subscribe", ID).await;
    venue
        .frames
        .send(WsMessage::Text(frame(ID, 2).into()))
        .unwrap();
    let DataEvent::Data(Data::Custom(data)) = receive(&mut events).await else {
        panic!("the new generation admits the explicit identity");
    };
    assert_eq!(data.data_type, predict_orderbook_data_type(ID));
    assert_eq!(
        data.data
            .as_any()
            .downcast_ref::<PredictOrderbookSnapshot>()
            .unwrap()
            .order_count,
        2
    );
    venue
        .frames
        .send(WsMessage::Text(
            json!({"topic":"heartbeat", "data":"custom-reset-barrier"})
                .to_string()
                .into(),
        ))
        .unwrap();
    assert_eq!(
        receive(&mut venue.commands).await,
        json!({"method":"heartbeat", "data":"custom-reset-barrier"})
    );
    client.disconnect().await.unwrap();
}
