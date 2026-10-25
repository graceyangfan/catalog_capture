// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

#![cfg(feature = "execution")]

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use alloy::primitives::U256;
use axum::{
    Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, Uri},
    response::Response,
    routing::{get, post},
};
use futures_util::{SinkExt, StreamExt};
use nautilus_network::websocket::TransportBackend;
use nautilus_predict::{
    PredictEnvironment, PredictPrivateClientConfig, PredictWalletWebSocketConfig,
    common::credential::PredictAuthToken,
    http::{PredictAuthenticatedHttpClient, PredictPrivateHttpClient},
    websocket::{
        PredictWalletSubscriptionState, PredictWalletWebSocketClient,
        error::{PredictWalletResponseErrorCode, PredictWalletWsError},
        private_messages::{PredictWalletEventKind, PredictWalletStreamMessage},
    },
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;

const API_KEY: &str = "wallet-api-key-secret-sentinel";
const TOKEN: &str = "wallet-jwt-secret-sentinel";
const WALLET: &str = "0x1111111111111111111111111111111111111111";
const FOREIGN: &str = "0x2222222222222222222222222222222222222222";
const HASH: &str = "0x17e3c43e261d9a73f845b38845f06f52589bcb64a82f70831f4acbc818df1dfc";

struct CapturingLogger(Mutex<Vec<String>>, AtomicUsize);
static LOGGER: CapturingLogger = CapturingLogger(Mutex::new(Vec::new()), AtomicUsize::new(0));

impl log::Log for CapturingLogger {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &log::Record<'_>) {
        use nautilus_common::logging::{config::LoggerConfig, logger::should_filter_log};
        static CONFIG: std::sync::OnceLock<LoggerConfig> = std::sync::OnceLock::new();
        let config = CONFIG
            .get_or_init(|| LoggerConfig::from_spec("stdout=Trace;tungstenite::=Warn").unwrap());
        let modules = config
            .module_level
            .iter()
            .map(|(path, level)| (*path, *level))
            .collect::<Vec<_>>();
        if should_filter_log(
            &ustr::Ustr::from(record.target()),
            record.level(),
            &modules,
            &config.component_level,
            false,
        ) {
            self.1.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.0
            .lock()
            .unwrap()
            .push(format!("{} {}", record.target(), record.args()));
    }
    fn flush(&self) {}
}

struct Connection {
    id: usize,
    send: mpsc::UnboundedSender<Message>,
}

impl Connection {
    fn json(&self, value: Value) {
        self.send
            .send(Message::Text(value.to_string().into()))
            .unwrap();
    }

    fn ack(&self, request: &Value) {
        self.json(json!({"type": "R", "requestId": request["requestId"], "success": true}));
    }

    fn wallet(&self, event: Value) {
        self.json(
            json!({"type": "M", "topic": format!("predictWalletEvents/{TOKEN}"), "data": event}),
        );
    }
}

#[derive(Clone)]
struct VenueState {
    connections: mpsc::UnboundedSender<Connection>,
    requests: mpsc::UnboundedSender<(usize, Value)>,
    closes: mpsc::UnboundedSender<usize>,
    count: Arc<AtomicUsize>,
    handshakes: Arc<Mutex<Vec<(HeaderMap, String)>>>,
}

struct Server {
    url: String,
    connections: mpsc::UnboundedReceiver<Connection>,
    requests: mpsc::UnboundedReceiver<(usize, Value)>,
    closes: mpsc::UnboundedReceiver<usize>,
    handshakes: Arc<Mutex<Vec<(HeaderMap, String)>>>,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn websocket(
    ws: WebSocketUpgrade,
    State(state): State<VenueState>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    assert_eq!(headers["x-api-key"], API_KEY);
    state
        .handshakes
        .lock()
        .unwrap()
        .push((headers, uri.to_string()));
    ws.on_upgrade(move |socket| serve_socket(socket, state))
}

async fn serve_socket(socket: WebSocket, state: VenueState) {
    let id = state.count.fetch_add(1, Ordering::AcqRel);
    let (send, mut commands) = mpsc::unbounded_channel();
    if state.connections.send(Connection { id, send }).is_err() {
        return;
    }
    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { break; };
                let closing = matches!(command, Message::Close(_));
                if sink.send(command).await.is_err() || closing { break; }
            }
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        let request = serde_json::from_str(&text).unwrap();
                        if state.requests.send((id, request)).is_err() { break; }
                    }
                    Some(Ok(Message::Ping(_)|Message::Pong(_))) => {},
                    _ => break,
                }
            }
        }
    }
    let _ = state.closes.send(id);
}

fn json_response(value: Value) -> Response {
    Response::builder()
        .header("content-type", "application/json")
        .body(axum::body::Body::from(value.to_string()))
        .unwrap()
}

async fn server() -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/ws", listener.local_addr().unwrap());
    let (connection_tx, connections) = mpsc::unbounded_channel();
    let (request_tx, requests) = mpsc::unbounded_channel();
    let (close_tx, closes) = mpsc::unbounded_channel();
    let handshakes = Arc::new(Mutex::new(vec![]));
    let state = VenueState {
        connections: connection_tx,
        requests: request_tx,
        closes: close_tx,
        count: Arc::new(AtomicUsize::new(0)),
        handshakes: Arc::clone(&handshakes),
    };
    let router = Router::new().route("/ws", get(websocket))
        .route("/v1/auth/message", get(|| async { json_response(json!({"success":true,"data":{"message":"rotation challenge"}})) }))
        .route("/v1/auth", post(|| async {
            use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
            let now = nautilus_core::time::get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000;
            json_response(json!({"success":true,"data":{"token":format!("e30.{}.signature",URL_SAFE_NO_PAD.encode(json!({"exp":now+3600}).to_string()))}}))
        }))
        .route("/v1/account", get(|| async { json_response(json!({"success":true,"data":{"name":"fixture","address":WALLET,"imageUrl":null,"referral":{"code":null,"status":"LOCKED"},"points":{"total":0}}})) }))
        .with_state(state);
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Server {
        url,
        connections,
        requests,
        closes,
        handshakes,
        task,
    }
}

impl Server {
    async fn connection(&mut self) -> Connection {
        timeout(Duration::from_secs(5), self.connections.recv())
            .await
            .unwrap()
            .unwrap()
    }

    async fn request(&mut self) -> (usize, Value) {
        timeout(Duration::from_secs(5), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
}

fn session(environment: PredictEnvironment) -> PredictAuthenticatedHttpClient {
    PredictPrivateHttpClient::new(PredictPrivateClientConfig {
        environment,
        api_key: Some(API_KEY.into()),
        ..Default::default()
    })
    .unwrap()
    .authenticated(
        PredictAuthToken::new(TOKEN.into()).unwrap(),
        WALLET.parse().unwrap(),
    )
    .unwrap()
}

fn config(server: &Server, backend: TransportBackend) -> PredictWalletWebSocketConfig {
    PredictWalletWebSocketConfig {
        api_key: Some(API_KEY.into()),
        base_url_ws: Some(server.url.clone()),
        subscription_timeout_secs: 1,
        transport_backend: backend,
        ..Default::default()
    }
}

async fn connect(server: &Server, backend: TransportBackend) -> PredictWalletWebSocketClient {
    PredictWalletWebSocketClient::connect(
        config(server, backend),
        &session(PredictEnvironment::BnbMainnet),
        CancellationToken::new(),
    )
    .await
    .unwrap()
}

async fn next(client: &mut PredictWalletWebSocketClient) -> PredictWalletStreamMessage {
    timeout(Duration::from_secs(5), client.next_message())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

fn event(kind: &str) -> Value {
    json!({"type": kind, "orderId": "9223372036854775807", "orderHash": HASH,
        "walletAddress": WALLET, "timestamp": 1791849600123u64,
        "details": {"marketId": 42, "outcomeIndex": 1, "marketQuestion": "fixture",
            "outcome": "YES", "quoteType": "BID", "quantity": "100.000000000000000001",
            "quantityFilled": "17.000000000000000001", "price": "0.620", "value": "62.00",
            "valueFilled": "10.54", "strategyType": "LIMIT", "categorySlug": "fixture"}})
}

fn settlement(kind: &str) -> Value {
    let mut event = event(kind);
    event["settlementId"] = json!("settlement-42");
    event["fill"] = json!({"executedPriceWei": "620000000000000000",
        "executedSizeWei": "17000000000000000001", "executedValueWei": U256::MAX.to_string()});
    event
}

#[tokio::test]
async fn subscription_requires_matching_ack_and_retains_events_arriving_before_ack() {
    for backend in [TransportBackend::Tungstenite, TransportBackend::Sockudo] {
        let mut server = server().await;
        let mut client = connect(&server, backend).await;
        let socket = server.connection().await;
        let (id, request) = server.request().await;
        assert_eq!(id, socket.id);
        assert_eq!(request["method"], "subscribe");
        assert_eq!(
            request["params"],
            json!([format!("predictWalletEvents/{TOKEN}")])
        );
        assert!(matches!(
            client.subscription_state(),
            PredictWalletSubscriptionState::Subscribing {
                connection_epoch: 0
            }
        ));
        socket.wallet(event("orderAccepted"));
        socket.json(json!({"type":"R", "requestId": 999, "success": true}));
        socket.json(json!({"type":"M", "topic":"public/irrelevant", "data": TOKEN}));
        // Exact echo, including values too wide to travel through f64.
        socket.json(json!({"type":"M", "topic":"heartbeat", "data": "1791849600123"}));
        let (_, echo) = server.request().await;
        assert_eq!(echo, json!({"method":"heartbeat", "data":"1791849600123"}));
        assert!(matches!(
            client.subscription_state(),
            PredictWalletSubscriptionState::Subscribing { .. }
        ));
        socket.ack(&request);
        assert_eq!(client.wait_until_subscribed().await.unwrap(), 0);
        assert!(matches!(
            next(&mut client).await,
            PredictWalletStreamMessage::Subscribed {
                connection_epoch: 0,
                reconciliation_required: true
            }
        ));
        let PredictWalletStreamMessage::Wallet { event, .. } = next(&mut client).await else {
            panic!("wallet event")
        };
        assert!(matches!(event.kind, PredictWalletEventKind::OrderAccepted));
        assert_eq!(event.order.order_id, "9223372036854775807");
        assert_eq!(
            event.order.order_hash,
            HASH.parse::<alloy::primitives::B256>().unwrap()
        );
        assert_eq!(
            event.order.timestamp_ns().unwrap().as_u64(),
            1791849600123000000
        );
        assert_eq!(
            event.order.details.quantity.to_string(),
            "100.000000000000000001"
        );
        assert!(!format!("{client:?}").contains(TOKEN));
        {
            let handshakes = server.handshakes.lock().unwrap();
            assert_eq!(handshakes[0].1, "/ws");
            assert!(!handshakes[0].0.contains_key("authorization"));
        }
        client.close().await.unwrap();
        assert_eq!(
            client.subscription_state(),
            PredictWalletSubscriptionState::Closed
        );
    }
}

#[tokio::test]
async fn all_lifecycle_and_settlement_events_keep_confirmation_and_optional_fee_semantics() {
    let mut server = server().await;
    let mut client = connect(&server, TransportBackend::Tungstenite).await;
    let socket = server.connection().await;
    let (_, request) = server.request().await;
    socket.ack(&request);
    let _ = next(&mut client).await;
    let mut rejected = event("orderNotAccepted");
    rejected["reason"] = json!("rejectedPostOnly");
    let mut cancelled = event("orderCancelled");
    cancelled["reason"] = json!("selfTradePrevention");
    let mut submitted = settlement("orderTransactionSubmitted");
    submitted["kind"] = json!("PURCHASE");
    let confirmed = settlement("orderTransactionSuccess");
    let failed = settlement("orderTransactionFailed");
    let mut confirmed_with_fee = confirmed.clone();
    confirmed_with_fee["isMaker"] = json!(false);
    confirmed_with_fee["fee"] = json!({"amountWei": U256::MAX.to_string(), "type":"SHARES"});
    for (index, data) in [
        event("orderAccepted"),
        rejected,
        event("orderExpired"),
        cancelled,
        submitted,
        confirmed,
        failed,
        confirmed_with_fee,
    ]
    .into_iter()
    .enumerate()
    {
        socket.wallet(data);
        let PredictWalletStreamMessage::Wallet { event, .. } = next(&mut client).await else {
            panic!("wallet event")
        };
        match (index, event.kind) {
            (0, PredictWalletEventKind::OrderAccepted)
            | (1, PredictWalletEventKind::OrderNotAccepted { .. })
            | (2, PredictWalletEventKind::OrderExpired)
            | (3, PredictWalletEventKind::OrderCancelled { .. }) => {}
            (4, PredictWalletEventKind::OrderTransactionSubmitted { fill, .. })
            | (6, PredictWalletEventKind::OrderTransactionFailed { fill, .. }) => {
                assert_eq!(fill.executed_value_wei, U256::MAX);
            }
            (5, PredictWalletEventKind::OrderTransactionSuccess { is_maker, fee, .. }) => {
                assert!(is_maker.is_none());
                assert!(fee.is_none());
            }
            (7, PredictWalletEventKind::OrderTransactionSuccess { is_maker, fee, .. }) => {
                assert_eq!(is_maker, Some(false));
                let fee = fee.unwrap();
                assert_eq!(fee.amount_wei, U256::MAX);
                assert_eq!(
                    fee.fee_type,
                    nautilus_predict::http::account_models::PredictFeeType::Shares
                );
            }
            unexpected => panic!("wrong event {unexpected:?}"),
        }
    }
    client.close().await.unwrap();
}

#[tokio::test]
async fn reconnect_invalidates_ack_and_requires_fresh_subscription_and_reconciliation() {
    let mut server = server().await;
    let mut client = connect(&server, TransportBackend::Tungstenite).await;
    let first = server.connection().await;
    let (_, request) = server.request().await;
    first.ack(&request);
    let _ = next(&mut client).await;
    // An old queued fact must be retired along with its connection ownership.
    first.wallet(event("orderAccepted"));
    first.json(json!({"type":"M", "topic":"heartbeat", "data":"old-epoch-barrier"}));
    let (_, barrier) = server.request().await;
    assert_eq!(barrier["data"], "old-epoch-barrier");
    first.send.send(Message::Close(None)).unwrap();
    let second = server.connection().await;
    let (_, replay) = server.request().await;
    assert_ne!(replay["requestId"], request["requestId"]);
    assert_eq!(replay["params"], request["params"]);
    assert!(matches!(
        client.subscription_state(),
        PredictWalletSubscriptionState::Subscribing {
            connection_epoch: 1
        }
    ));
    second.ack(&request); // A stale ACK from the previous request cannot authorize this epoch.
    second.json(json!({"type":"M", "topic":"heartbeat", "data":1791849600123u64}));
    let (_, echo) = server.request().await;
    assert_eq!(echo["data"], 1791849600123u64);
    assert!(matches!(
        client.subscription_state(),
        PredictWalletSubscriptionState::Subscribing { .. }
    ));
    second.ack(&replay);
    assert_eq!(client.wait_until_subscribed().await.unwrap(), 1);
    assert!(matches!(
        next(&mut client).await,
        PredictWalletStreamMessage::Subscribed {
            connection_epoch: 1,
            reconciliation_required: true
        }
    ));
    second.wallet(event("orderAccepted"));
    assert!(matches!(
        next(&mut client).await,
        PredictWalletStreamMessage::Wallet {
            connection_epoch: 1,
            ..
        }
    ));
    client.close().await.unwrap();
}

#[tokio::test]
#[ignore = "explicit repeated loopback reconnects, including Native's 10-second backoff cap"]
async fn repeated_reconnects_deliver_each_epoch_once_and_close_the_session() {
    for backend in [TransportBackend::Tungstenite, TransportBackend::Sockudo] {
        let mut server = server().await;
        let mut client = connect(&server, backend).await;
        let mut previous: Option<Value> = None;
        for epoch in 0..16u64 {
            let connection = timeout(Duration::from_secs(15), server.connections.recv())
                .await
                .expect("connection must return within Native's reconnect backoff cap")
                .unwrap();
            let (owner, request) = server.request().await;
            assert_eq!(owner, connection.id);
            assert_eq!(request["method"], "subscribe");
            assert_eq!(
                request["params"],
                json!([format!("predictWalletEvents/{TOKEN}")])
            );
            if let Some(old) = &previous {
                assert_ne!(request["requestId"], old["requestId"]);
                connection.ack(old);
                connection.json(json!({"type":"M","topic":"heartbeat","data":epoch}));
                let (owner, echo) = server.request().await;
                assert_eq!(owner, connection.id);
                assert_eq!(echo["data"], epoch);
                assert!(matches!(
                    client.subscription_state(),
                    PredictWalletSubscriptionState::Subscribing { connection_epoch }
                        if connection_epoch == epoch
                ));
            }
            connection.ack(&request);
            assert_eq!(client.wait_until_subscribed().await.unwrap(), epoch);
            assert!(matches!(
                next(&mut client).await,
                PredictWalletStreamMessage::Subscribed {
                    connection_epoch,
                    reconciliation_required: true,
                } if connection_epoch == epoch
            ));
            connection.wallet(event("orderAccepted"));
            assert!(matches!(
                next(&mut client).await,
                PredictWalletStreamMessage::Wallet { connection_epoch, .. }
                    if connection_epoch == epoch
            ));
            previous = Some(request);
            if epoch < 15 {
                connection.send.send(Message::Close(None)).unwrap();
            }
        }
        client.close().await.unwrap();
        assert_eq!(server.handshakes.lock().unwrap().len(), 16);
        assert!(server.connections.try_recv().is_err());
        assert!(server.requests.try_recv().is_err());
    }
}

#[tokio::test]
async fn rejected_expired_credentials_fail_without_retry_or_echoed_server_text() {
    let mut server = server().await;
    let mut client = connect(&server, TransportBackend::Tungstenite).await;
    let socket = server.connection().await;
    let (_, request) = server.request().await;
    socket.json(
        json!({"type":"R", "requestId":request["requestId"], "success":false,
        "error":{"code":"invalid_credentials", "message":format!("{TOKEN} {API_KEY}")}}),
    );
    let error = client.wait_until_subscribed().await.unwrap_err();
    assert_eq!(
        error,
        PredictWalletWsError::SubscriptionRejected(
            PredictWalletResponseErrorCode::InvalidCredentials
        )
    );
    assert!(!format!("{error:?} {error}").contains(TOKEN));
    assert!(!format!("{client:?}").contains(API_KEY));
    client.close().await.unwrap();
    assert_eq!(server.handshakes.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn subscription_ack_timeout_and_external_cancellation_stop_the_session() {
    let mut server = server().await;
    let mut client = connect(&server, TransportBackend::Tungstenite).await;
    let _socket = server.connection().await;
    let _ = server.request().await;
    assert_eq!(
        timeout(Duration::from_secs(3), client.wait_until_subscribed())
            .await
            .unwrap()
            .unwrap_err(),
        PredictWalletWsError::SubscriptionTimeout
    );
    client.close().await.unwrap();

    let token = CancellationToken::new();
    let mut client = PredictWalletWebSocketClient::connect(
        config(&server, TransportBackend::Tungstenite),
        &session(PredictEnvironment::BnbMainnet),
        token.clone(),
    )
    .await
    .unwrap();
    let socket = server.connection().await;
    let (_, request) = server.request().await;
    socket.ack(&request);
    let _ = next(&mut client).await;
    token.cancel();
    assert!(client.next_message().await.unwrap().is_none());
    client.close().await.unwrap();
}

#[tokio::test]
async fn malformed_foreign_or_unknown_events_fail_closed_without_a_fake_fill() {
    for (mut malformed, expected) in [
        (
            event("orderAccepted"),
            PredictWalletWsError::AccountMismatch,
        ),
        (event("orderAccepted"), PredictWalletWsError::Protocol),
        (
            event("orderFutureState"),
            PredictWalletWsError::UnsupportedEvent,
        ),
        (
            settlement("orderTransactionSuccess"),
            PredictWalletWsError::Decode,
        ),
    ] {
        match expected {
            PredictWalletWsError::AccountMismatch => malformed["walletAddress"] = json!(FOREIGN),
            PredictWalletWsError::Protocol => malformed["timestamp"] = json!(u64::MAX),
            PredictWalletWsError::Decode => malformed["fill"]["executedSizeWei"] = json!(TOKEN),
            _ => {}
        }
        let mut server = server().await;
        let mut client = connect(&server, TransportBackend::Tungstenite).await;
        let socket = server.connection().await;
        let (_, request) = server.request().await;
        socket.ack(&request);
        let _ = next(&mut client).await;
        socket.wallet(malformed);
        let error = timeout(Duration::from_secs(3), client.next_message())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error, expected);
        assert!(!format!("{error:?} {error}").contains(TOKEN));
        client.close().await.unwrap();
    }
}

#[tokio::test]
async fn queue_overflow_becomes_an_observable_terminal_failure() {
    let mut server = server().await;
    let mut cfg = config(&server, TransportBackend::Tungstenite);
    cfg.queue_capacity = 4;
    let mut client = PredictWalletWebSocketClient::connect(
        cfg,
        &session(PredictEnvironment::BnbMainnet),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let socket = server.connection().await;
    let (_, request) = server.request().await;
    socket.ack(&request);
    client.wait_until_subscribed().await.unwrap();
    // Do not drain the consumer queue. Every event is significant; dropping one is fatal.
    for _ in 0..20 {
        socket.wallet(event("orderAccepted"));
    }
    timeout(Duration::from_secs(3), async {
        while !matches!(
            client.subscription_state(),
            PredictWalletSubscriptionState::Failed(_)
        ) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        client.next_message().await.unwrap_err(),
        PredictWalletWsError::QueueOverflow
    );
    client.close().await.unwrap();
}

#[tokio::test]
async fn drop_stops_background_io_without_cancelling_the_parent_token() {
    let mut server = server().await;
    let cancellation = CancellationToken::new();
    let mut client = PredictWalletWebSocketClient::connect(
        config(&server, TransportBackend::Tungstenite),
        &session(PredictEnvironment::BnbMainnet),
        cancellation.clone(),
    )
    .await
    .unwrap();
    let socket = server.connection().await;
    let (_, request) = server.request().await;
    socket.ack(&request);
    let _ = next(&mut client).await;
    drop(client);
    assert_eq!(
        timeout(Duration::from_secs(5), server.closes.recv())
            .await
            .unwrap(),
        Some(socket.id)
    );
    assert!(!cancellation.is_cancelled());
}

#[tokio::test]
async fn configuration_rejects_secret_urls_and_environment_mismatches_before_handshake() {
    let server = server().await;
    let session = session(PredictEnvironment::BnbMainnet);
    for url in [
        "ws://public.example/ws",
        "wss://ws.predict.fun/ws?apiKey=secret",
        "wss://user:secret@ws.predict.fun/ws",
        "wss://ws.predict.fun/other",
        "wss://ws.predict.fun/ws#secret",
    ] {
        let cfg = PredictWalletWebSocketConfig {
            api_key: Some(API_KEY.into()),
            base_url_ws: Some(url.into()),
            ..Default::default()
        };
        assert!(!format!("{cfg:?}").contains("=secret"));
        let error = PredictWalletWebSocketClient::connect(cfg, &session, CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(error, PredictWalletWsError::Configuration(_)));
    }
    let cfg = PredictWalletWebSocketConfig {
        environment: PredictEnvironment::BnbTestnet,
        ..config(&server, TransportBackend::Tungstenite)
    };
    assert!(matches!(
        PredictWalletWebSocketClient::connect(cfg, &session, CancellationToken::new()).await,
        Err(PredictWalletWsError::Configuration(_))
    ));
    assert!(server.handshakes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn nautilus_log_filters_and_stream_close_handling_keep_wallet_credentials_private() {
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(log::LevelFilter::Trace);
    let mut server = server().await;
    let mut client = connect(&server, TransportBackend::Tungstenite).await;
    let first = server.connection().await;
    let (_, request) = server.request().await;
    first.ack(&request);
    let _ = next(&mut client).await;
    first
        .send
        .send(Message::Close(Some(axum::extract::ws::CloseFrame {
            code: 1011,
            reason: format!("{TOKEN} {API_KEY}").into(),
        })))
        .unwrap();
    let second = server.connection().await;
    let (_, request) = server.request().await;
    second.ack(&request);
    let _ = next(&mut client).await;
    second.json(json!({"type":"R", "requestId":-1, "success":false,
        "error":{"code":"invalid_json", "message":format!("{TOKEN} {API_KEY}")}}));
    second.json(json!({"type":"M", "topic":"heartbeat", "data":1791849600123u64}));
    let (_, echo) = server.request().await;
    assert_eq!(echo["method"], "heartbeat");
    client.close().await.unwrap();
    let logs = LOGGER.0.lock().unwrap().clone();
    assert!(
        logs.iter()
            .any(|line| line.contains("Sending text frame once"))
    );
    assert!(
        LOGGER.1.load(Ordering::Relaxed) > 0,
        "native transport diagnostics must be filtered"
    );
    let leaks: Vec<_> = logs
        .iter()
        .filter(|line| line.contains(TOKEN) || line.contains(API_KEY))
        .collect();
    assert!(
        leaks.is_empty(),
        "fixture credential markers appeared in diagnostics: {leaks:?}"
    );
}

#[tokio::test]
async fn rejected_handshake_returns_status_without_echoed_body_or_retries() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let router = Router::new().route(
        "/ws",
        get(move || {
            let count = Arc::clone(&count);
            async move {
                count.fetch_add(1, Ordering::AcqRel);
                (axum::http::StatusCode::UNAUTHORIZED, TOKEN)
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/ws", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let cfg = PredictWalletWebSocketConfig {
        api_key: Some(API_KEY.into()),
        base_url_ws: Some(url),
        transport_backend: TransportBackend::Tungstenite,
        ..Default::default()
    };
    let error = PredictWalletWebSocketClient::connect(
        cfg,
        &session(PredictEnvironment::BnbMainnet),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(error, PredictWalletWsError::HandshakeRejected(401));
    assert!(!format!("{error:?} {error}").contains(TOKEN));
    assert_eq!(calls.load(Ordering::Acquire), 1);
    task.abort();
}

#[tokio::test]
async fn captured_no_wallet_events_use_one_based_outcome_identity_on_both_transports() {
    let captured: Value = serde_json::from_str(include_str!(
        "fixtures/private_api/wallet_resting_actual_20261015.json"
    ))
    .unwrap();
    for backend in [TransportBackend::Tungstenite, TransportBackend::Sockudo] {
        let mut server = server().await;
        let mut client = connect(&server, backend).await;
        let socket = server.connection().await;
        let (_, request) = server.request().await;
        socket.ack(&request);
        client.wait_until_subscribed().await.unwrap();
        assert!(matches!(
            next(&mut client).await,
            PredictWalletStreamMessage::Subscribed { .. }
        ));
        for source in captured["events"].as_array().unwrap() {
            let mut payload = source.clone();
            // Only the already-sanitized wallet identity is bound to this loopback session.
            payload["walletAddress"] = json!(WALLET);
            socket.wallet(payload.clone());
            let PredictWalletStreamMessage::Wallet { event, .. } = next(&mut client).await else {
                panic!("expected captured wallet event")
            };
            assert_eq!(event.order.details.outcome_index, 2);
            assert_eq!(event.order.order_id, source["orderId"]);
            assert_eq!(event.order.order_hash.to_string(), source["orderHash"]);
            if source["type"] == "orderAccepted" {
                assert!(matches!(event.kind, PredictWalletEventKind::OrderAccepted));
            } else {
                assert!(matches!(
                    event.kind,
                    PredictWalletEventKind::OrderCancelled { .. }
                ));
            }
        }
        client.close().await.unwrap();
    }
}

#[tokio::test]
async fn mismatched_wallet_outcome_index_is_rejected_without_normalizing_identity() {
    for index in [0, 1, 3] {
        let mut server = server().await;
        let mut client = connect(&server, TransportBackend::Tungstenite).await;
        let socket = server.connection().await;
        let (_, request) = server.request().await;
        socket.ack(&request);
        client.wait_until_subscribed().await.unwrap();
        let _ = next(&mut client).await;
        let mut payload = event("orderAccepted");
        payload["details"]["outcome"] = json!("NO");
        payload["details"]["outcomeIndex"] = json!(index);
        socket.wallet(payload);
        let error = timeout(Duration::from_secs(2), client.next_message())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error, PredictWalletWsError::Protocol);
        client.close().await.unwrap();
    }
}

#[tokio::test]
async fn actual_market_plan_and_success_replay_keep_wallet_stream_alive() {
    let actual: Value = serde_json::from_str(include_str!(
        "fixtures/private_api/live_fill_20261016T122233Z_independent/calibration.json"
    ))
    .unwrap();
    let mut server = server().await;
    let mut client = connect(&server, TransportBackend::Tungstenite).await;
    let socket = server.connection().await;
    let (_, request) = server.request().await;
    socket.ack(&request);
    let _ = next(&mut client).await;
    let rows = actual["wallet"]["events"].as_array().unwrap();
    let plan = rows
        .iter()
        .position(|e| e["type"] == "orderMarketMatchPlan")
        .unwrap();
    for original in &rows[plan..] {
        let mut row = original.clone();
        row["walletAddress"] = json!(WALLET);
        socket.wallet(row);
        let PredictWalletStreamMessage::Wallet { event, .. } = next(&mut client).await else {
            panic!("wallet event expected")
        };
        assert!(!matches!(event.kind, PredictWalletEventKind::Unsupported));
    }
    assert!(matches!(
        client.subscription_state(),
        PredictWalletSubscriptionState::Subscribed { .. }
    ));
    client.close().await.unwrap();
}

#[tokio::test]
async fn jwt_rotation_replaces_native_connection_and_requires_new_topic_ack() {
    use nautilus_predict::signing::{PredictAuthSigner, PredictChain};
    for backend in [TransportBackend::Tungstenite, TransportBackend::Sockudo] {
        let mut server = server().await;
        let transport = PredictPrivateHttpClient::new(PredictPrivateClientConfig {
            base_url_http: Some(
                server
                    .url
                    .replace("ws://", "http://")
                    .trim_end_matches("/ws")
                    .to_string(),
            ),
            api_key: Some(API_KEY.into()),
            ..Default::default()
        })
        .unwrap();
        let session = transport
            .authenticated(
                PredictAuthToken::new(TOKEN.into()).unwrap(),
                WALLET.parse().unwrap(),
            )
            .unwrap();
        let mut client = PredictWalletWebSocketClient::connect(
            config(&server, backend),
            &session,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let first = server.connection().await;
        let (_, original) = server.request().await;
        first.ack(&original);
        assert_eq!(client.wait_until_subscribed().await.unwrap(), 0);
        let _ = next(&mut client).await;
        first.wallet(event("orderAccepted"));
        // A server echo is a causal barrier: the old event is in the outbound queue.
        first.json(json!({"type":"M","topic":"heartbeat","data":"rotation-barrier"}));
        let (_, barrier) = server.request().await;
        assert_eq!(barrier["data"], "rotation-barrier");
        let signer = PredictAuthSigner::new(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".into(),
            PredictChain::BnbMainnet,
            Some(WALLET),
        )
        .unwrap();
        session.renew(&signer).await.unwrap();
        let second = server.connection().await;
        let (_, replay) = server.request().await;
        assert_ne!(original["params"], replay["params"]);
        assert_ne!(original["requestId"], replay["requestId"]);
        assert!(
            replay["params"][0]
                .as_str()
                .unwrap()
                .ends_with(session.token().expose_secret())
        );
        assert!(!matches!(
            client.subscription_state(),
            PredictWalletSubscriptionState::Subscribed { .. }
        ));
        second.ack(&original);
        second.json(json!({"type":"M","topic":"heartbeat","data":"ack-barrier"}));
        let _ = server.request().await;
        assert!(!matches!(
            client.subscription_state(),
            PredictWalletSubscriptionState::Subscribed { .. }
        ));
        second.ack(&replay);
        assert_eq!(client.wait_until_subscribed().await.unwrap(), 1);
        assert!(matches!(
            next(&mut client).await,
            PredictWalletStreamMessage::Subscribed {
                connection_epoch: 1,
                reconciliation_required: true
            }
        ));
        second.json(json!({"type":"M","topic":replay["params"][0],"data":event("orderAccepted")}));
        assert!(matches!(
            next(&mut client).await,
            PredictWalletStreamMessage::Wallet {
                connection_epoch: 1,
                ..
            }
        ));
        client.close().await.unwrap();
    }
}

#[tokio::test]
async fn execution_lifecycle_renews_readonly_authentication_and_cancels_its_worker() {
    use std::{cell::RefCell, rc::Rc};

    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use nautilus_common::{
        cache::Cache,
        clients::ExecutionClient,
        msgbus::{self, MessageBus},
    };
    use nautilus_core::UUID4;
    use nautilus_live::ExecutionClientCore;
    use nautilus_model::{
        enums::{AccountType, OmsType},
        identifiers::{AccountId, ClientId, TraderId},
    };
    use nautilus_predict::{
        config::{PredictAccountStateMode, PredictExecutionClientConfig},
        execution::PredictExecutionClient,
        signing::{PredictAuthSigner, PredictChain},
    };
    let mut server = server().await;
    let now = nautilus_core::time::get_atomic_clock_realtime()
        .get_time_ns()
        .as_u64()
        / 1_000_000_000;
    let token = PredictAuthToken::new(
        format!(
            "e30.{}.signature",
            URL_SAFE_NO_PAD.encode(json!({"exp":now+2}).to_string())
        )
        .into(),
    )
    .unwrap();
    let transport = PredictPrivateHttpClient::new(PredictPrivateClientConfig {
        base_url_http: Some(
            server
                .url
                .replace("ws://", "http://")
                .trim_end_matches("/ws")
                .to_string(),
        ),
        api_key: Some(API_KEY.into()),
        ..Default::default()
    })
    .unwrap();
    let session = transport
        .authenticated(token, WALLET.parse().unwrap())
        .unwrap();
    let witness = session.clone();
    let trader = TraderId::new("RENEWAL-001");
    let account = AccountId::new("PREDICT-001");
    msgbus::set_message_bus(Rc::new(RefCell::new(MessageBus::new(
        trader,
        UUID4::new(),
        None,
        None,
    ))));
    let core = ExecutionClientCore::new(
        trader,
        ClientId::new("PREDICT"),
        "PREDICT".into(),
        OmsType::Netting,
        account,
        AccountType::Cash,
        None,
        Rc::new(RefCell::new(Cache::default())),
    );
    let mut cfg = PredictExecutionClientConfig::new(
        account,
        session,
        config(&server, TransportBackend::Tungstenite),
    );
    cfg.account_state_mode = PredictAccountStateMode::Disabled;
    cfg.reconnect_reconciliation_interval_secs = None;
    cfg.auth_signer = Some(Arc::new(
        PredictAuthSigner::new(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".into(),
            PredictChain::BnbMainnet,
            Some(WALLET),
        )
        .unwrap(),
    ));
    assert!(cfg.submission_signer.is_none());
    let (sender, _events) = mpsc::unbounded_channel();
    nautilus_common::live::runner::replace_exec_event_sender(sender);
    let mut client = PredictExecutionClient::new(core, cfg).unwrap();
    let (connected, (original, _first)) = tokio::join!(client.connect(), async {
        let first = server.connection().await;
        let (_, request) = server.request().await;
        first.ack(&request);
        (request, first)
    });
    connected.unwrap();
    let second = server.connection().await;
    let (_, replacement) = server.request().await;
    assert_ne!(original["params"], replacement["params"]);
    assert!(
        replacement["params"][0]
            .as_str()
            .unwrap()
            .ends_with(witness.token().expose_secret())
    );
    assert!(
        !client.is_connected(),
        "new connection must await its own ACK"
    );
    second.ack(&replacement);
    second.json(json!({"type":"M","topic":"heartbeat","data":"renewal-confirmed"}));
    let (_, echo) = server.request().await;
    assert_eq!(echo["data"], "renewal-confirmed");
    // Transport-only diagnostics have no reported account to reconcile.
    assert!(
        client.is_connected(),
        "renewed authenticated transport is available"
    );
    client.disconnect().await.unwrap();
    assert!(!client.is_connected());
    assert_eq!(server.handshakes.lock().unwrap().len(), 2);
}

#[test]
fn recorded_market_fok_no_match_preserves_acceptance_then_rejection() {
    use nautilus_predict::websocket::private_messages::{
        PredictWalletEvent, PredictWalletRejectionReason,
    };
    let capture: Value = serde_json::from_str(include_str!(
        "fixtures/private_api/market_fok_no_match_20261024.json"
    ))
    .unwrap();
    let rows = capture["walletEvents"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    let parsed: Vec<PredictWalletEvent> = rows
        .iter()
        .map(|row| {
            let mut row = row.clone();
            // Operational evidence redacts identity; substitute the public mock wallet only.
            row["walletAddress"] = event("orderAccepted")["walletAddress"].clone();
            serde_json::from_value(row).unwrap()
        })
        .collect();
    assert!(matches!(
        parsed[0].kind,
        PredictWalletEventKind::OrderAccepted
    ));
    assert!(matches!(
        parsed[1].kind,
        PredictWalletEventKind::OrderNotAccepted {
            reason: PredictWalletRejectionReason::NoMarketMatch
        }
    ));
    assert_eq!(parsed[0].order.order_hash, parsed[1].order.order_hash);
    assert_eq!(capture["immediateRestStatus"], "OPEN");
    assert_eq!(capture["finalRestStatus"], "CANCELLED");
    assert_eq!(capture["finalAmountFilled"], "0");
    assert_eq!(capture["finalRunOpenOrders"], 0);
}

#[test]
fn recorded_post_only_stp_maker_accept_cancel_is_zero_fill() {
    use nautilus_predict::websocket::private_messages::PredictWalletEvent;
    let capture: Value = serde_json::from_str(include_str!(
        "fixtures/private_api/maker_post_only_stp_cancel_20261024.json"
    ))
    .unwrap();
    let rows = capture["walletEvents"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    let events: Vec<PredictWalletEvent> = rows
        .iter()
        .map(|row| {
            let mut row = row.clone();
            row["walletAddress"] = event("orderAccepted")["walletAddress"].clone();
            serde_json::from_value(row).unwrap()
        })
        .collect();
    assert!(matches!(
        events[0].kind,
        PredictWalletEventKind::OrderAccepted
    ));
    assert!(matches!(
        events[1].kind,
        PredictWalletEventKind::OrderCancelled { .. }
    ));
    assert_eq!(events[0].order.order_hash, events[1].order.order_hash);
    assert_eq!(capture["request"]["isPostOnly"], true);
    assert_eq!(capture["request"]["selfTradePrevention"], "CANCEL_TAKER");
    assert!(capture["removalLockedUntil"].is_null());
    assert_eq!(capture["finalRestStatus"], "CANCELLED");
    assert_eq!(capture["finalAmountFilled"], "0");
    assert_eq!(capture["finalRunOpenOrders"], 0);
}
