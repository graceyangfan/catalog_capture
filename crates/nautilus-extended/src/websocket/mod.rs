// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Extended RPC v2 public WebSocket session.

pub mod messages;
pub mod parse;

use std::{
    collections::VecDeque,
    num::NonZeroU32,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use ahash::{AHashMap, AHashSet};
use anyhow::Context;
use nautilus_common::{live::sender::EventSender, messages::DataEvent};
use nautilus_core::{time::AtomicTime, AtomicMap};
use nautilus_live::{book::snapshot::SnapshotGate, SocketControl};
use nautilus_model::{
    data::{Data, TradeTick},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_network::{
    http::create_standard_nautilus_headers,
    mode::ReconnectRequestOutcome,
    transport::Message,
    websocket::{
        EpochMessageHandler, InitialConnectRetryPolicy, SubscriptionState, WebSocketClient,
        WebSocketConfig, WebSocketReconnectHandle,
    },
    RECONNECTED,
};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use ustr::Ustr;

use crate::{
    config::ExtendedDataClientConfig,
    websocket::{
        messages::{
            ExtendedCommand, ExtendedDataKind, ExtendedTopic, RpcError, RpcResult, StreamEnvelope,
        },
        parse::{parse_stream_message, ParsedMessage},
    },
};

const RAW_QUEUE_CAPACITY: usize = 256;
const COMMAND_QUEUE_CAPACITY: usize = 256;
const MAX_MESSAGE_SIZE_BYTES: usize = 2 * 1024 * 1024;
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(10);
const BOOK_STALE_TIMEOUT: Duration = Duration::from_secs(90);
const TRADE_IDS_RETAINED: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RpcOperation {
    Subscribe,
    Unsubscribe,
}

#[derive(Debug)]
struct PendingRequest {
    epoch: u64,
    operation: RpcOperation,
    topic: Option<ExtendedTopic>,
    deadline: tokio::time::Instant,
}

#[derive(Debug, Default)]
struct BookState {
    gate: SnapshotGate,
    acknowledged: bool,
    ready: bool,
    deadline: Option<tokio::time::Instant>,
}

impl BookState {
    fn begin_subscribe(&mut self) {
        let mut gate = self.gate.lock();
        gate.close();
        self.acknowledged = false;
        self.ready = false;
        self.deadline = None;
    }

    fn write_completed(&self) {
        self.gate.open();
    }

    fn confirm(&mut self) {
        self.acknowledged = true;
        self.deadline = Some(tokio::time::Instant::now() + SNAPSHOT_TIMEOUT);
    }

    fn stop_monitoring(&mut self) {
        self.deadline = None;
    }

    fn accept_snapshot(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.gate.lock().is_closed(), "snapshot gate is closed");
        anyhow::ensure!(self.acknowledged, "snapshot arrived before subscribe ACK");
        self.ready = true;
        self.deadline = Some(tokio::time::Instant::now() + BOOK_STALE_TIMEOUT);
        Ok(())
    }

    fn accept_delta(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(self.ready, "book delta arrived before snapshot");
        self.deadline = Some(tokio::time::Instant::now() + BOOK_STALE_TIMEOUT);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct TradeReplayState {
    initialized: bool,
    ids: AHashSet<u64>,
    order: VecDeque<u64>,
}

impl TradeReplayState {
    fn filter(&mut self, trades: Vec<TradeTick>) -> Vec<TradeTick> {
        if !self.initialized {
            for trade in trades {
                if let Ok(id) = trade.trade_id.as_str().parse() {
                    self.remember(id);
                }
            }
            self.initialized = true;
            return Vec::new();
        }

        trades
            .into_iter()
            .filter(|trade| {
                let Ok(id) = trade.trade_id.as_str().parse::<u64>() else {
                    return false;
                };
                if self.ids.contains(&id) {
                    false
                } else {
                    self.remember(id);
                    true
                }
            })
            .collect()
    }

    fn remember(&mut self, id: u64) {
        if !self.ids.insert(id) {
            return;
        }
        self.order.push_back(id);
        if self.order.len() > TRADE_IDS_RETAINED {
            if let Some(oldest) = self.order.pop_front() {
                self.ids.remove(&oldest);
            }
        }
    }
}

#[derive(Debug)]
pub struct ExtendedWebSocketSession {
    client: WebSocketClient,
    reconnect: WebSocketReconnectHandle,
    raw_rx: mpsc::Receiver<(u64, Message)>,
    command_rx: mpsc::Receiver<ExtendedCommand>,
    overflowed: Arc<AtomicBool>,
    subscriptions: SubscriptionState,
    topics: AHashMap<String, ExtendedTopic>,
    pending: AHashMap<String, PendingRequest>,
    books: AHashMap<String, BookState>,
    book_sequences: AHashMap<InstrumentId, u64>,
    trade_replay: AHashMap<InstrumentId, TradeReplayState>,
    instruments: Arc<AtomicMap<Ustr, InstrumentAny>>,
    data_sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
    cancellation: CancellationToken,
    next_request_id: u64,
    current_epoch: u64,
    last_sequence: Option<u64>,
    failed_epoch: Option<u64>,
}

impl ExtendedWebSocketSession {
    pub async fn connect(
        config: &ExtendedDataClientConfig,
        instruments: Arc<AtomicMap<Ustr, InstrumentAny>>,
        data_sender: EventSender<DataEvent>,
        clock: &'static AtomicTime,
        cancellation: CancellationToken,
        socket_control: SocketControl,
    ) -> anyhow::Result<(mpsc::Sender<ExtendedCommand>, Self)> {
        let (raw_tx, raw_rx) = mpsc::channel(RAW_QUEUE_CAPACITY);
        let overflowed = Arc::new(AtomicBool::new(false));
        let overflowed_handler = Arc::clone(&overflowed);
        let epoch_handler: EpochMessageHandler =
            Arc::new(
                move |epoch, message| match raw_tx.try_send((epoch, message)) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        overflowed_handler.store(true, Ordering::Release);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {}
                },
            );

        let ws_config = WebSocketConfig {
            url: config.ws_url(),
            headers: create_standard_nautilus_headers(),
            heartbeat_interval_secs: None,
            heartbeat_payload: None,
            connect_timeout_ms: Some(config.ws_timeout_secs.saturating_mul(1_000).max(1)),
            reconnect_delay_initial_ms: Some(500),
            reconnect_delay_max_ms: Some(10_000),
            reconnect_backoff_factor: Some(2.0),
            reconnect_jitter_ms: Some(250),
            reconnect_max_attempts: None,
            heartbeat_timeout_secs: Some(45),
            idle_timeout_ms: None,
            writer_capacity: Some(COMMAND_QUEUE_CAPACITY),
            backend: config.transport_backend,
            proxy_url: config.proxy_url.clone(),
            max_message_size_bytes: Some(MAX_MESSAGE_SIZE_BYTES),
            max_frame_size_bytes: Some(MAX_MESSAGE_SIZE_BYTES),
        };
        let max_attempts = NonZeroU32::new(5).context("connect attempts must be non-zero")?;
        let client = WebSocketClient::epoch_builder()
            .config(ws_config)
            .epoch_handler(epoch_handler)
            .state_sink(socket_control.sink())
            .initial_connect_retry_policy(InitialConnectRetryPolicy {
                max_attempts,
                delay_initial: Duration::from_millis(500),
                delay_max: Duration::from_secs(5),
                backoff_factor: 2.0,
                jitter_ms: 250,
            })
            .cancellation_token(cancellation.clone())
            .connect()
            .await
            .context("failed to connect Extended RPC v2 WebSocket")?;

        let reconnect = client.reconnect_handle();
        let registered = reconnect.clone();
        socket_control.register(move || registered.request_reconnect());
        let current_epoch = client.connection_epoch();
        let (command_tx, command_rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);

        Ok((
            command_tx,
            Self {
                client,
                reconnect,
                raw_rx,
                command_rx,
                overflowed,
                subscriptions: SubscriptionState::new('.'),
                topics: AHashMap::new(),
                pending: AHashMap::new(),
                books: AHashMap::new(),
                book_sequences: AHashMap::new(),
                trade_replay: AHashMap::new(),
                instruments,
                data_sender,
                clock,
                cancellation,
                next_request_id: 1,
                current_epoch,
                last_sequence: None,
                failed_epoch: None,
            },
        ))
    }

    pub async fn run(mut self) {
        let mut health_tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                biased;
                () = self.cancellation.cancelled() => break,
                command = self.command_rx.recv() => {
                    let Some(command) = command else { break };
                    if let Err(error) = self.handle_command(command).await {
                        self.fail_generation(format!("subscription command failed: {error:#}"));
                    }
                }
                _ = health_tick.tick() => self.check_health(),
                raw = self.raw_rx.recv() => {
                    let Some((epoch, message)) = raw else { break };
                    if let Err(error) = self.handle_message(epoch, message).await {
                        self.fail_generation(format!("stream handling failed: {error:#}"));
                    }
                }
            }
        }

        self.subscriptions.clear();
        self.client.disconnect().await;
    }

    async fn handle_command(&mut self, command: ExtendedCommand) -> anyhow::Result<()> {
        match command {
            ExtendedCommand::Subscribe(topic) => {
                let key = topic.key();
                if !self.subscriptions.add_reference(&key) {
                    return Ok(());
                }
                self.subscriptions.mark_subscribe(&key);
                self.topics.insert(key.clone(), topic.clone());
                if topic.kind == ExtendedDataKind::OrderBook {
                    self.books.entry(key).or_default().begin_subscribe();
                }
                self.send_topic_request(RpcOperation::Subscribe, topic)
                    .await
            }
            ExtendedCommand::Unsubscribe(topic) => {
                let key = topic.key();
                if !self.subscriptions.remove_reference(&key) {
                    return Ok(());
                }
                self.subscriptions.mark_unsubscribe(&key);
                if let Some(book) = self.books.get_mut(&key) {
                    book.stop_monitoring();
                }
                self.send_topic_request(RpcOperation::Unsubscribe, topic)
                    .await
            }
        }
    }

    async fn send_topic_request(
        &mut self,
        operation: RpcOperation,
        topic: ExtendedTopic,
    ) -> anyhow::Result<()> {
        let epoch = self.current_epoch;
        let request_id = self.take_request_id();
        let method = match operation {
            RpcOperation::Subscribe => "subscribe",
            RpcOperation::Unsubscribe => "unsubscribe",
        };
        let payload = json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": topic.subscribe_params(),
        })
        .to_string();
        self.pending.insert(
            request_id.clone(),
            PendingRequest {
                epoch,
                operation,
                topic: Some(topic.clone()),
                deadline: tokio::time::Instant::now() + RPC_TIMEOUT,
            },
        );

        let result = self
            .client
            .send_text_on_connection(payload, None, epoch)
            .await;
        if let Some(book) = self.books.get(&topic.key()) {
            book.write_completed();
        }
        if let Err(error) = result {
            self.pending.remove(&request_id);
            self.subscriptions.mark_failure(&topic.key());
            anyhow::bail!(
                "failed to write Extended {method} for {}: {error}",
                topic.key()
            );
        }
        Ok(())
    }

    async fn handle_message(&mut self, epoch: u64, message: Message) -> anyhow::Result<()> {
        let Some(text) = message.as_text() else {
            anyhow::bail!("Extended RPC v2 sent a non-text application frame");
        };
        if text == RECONNECTED {
            return self.handle_reconnected(epoch).await;
        }
        if epoch != self.current_epoch || self.failed_epoch == Some(epoch) {
            return Ok(());
        }

        let value: Value = serde_json::from_str(text).context("invalid Extended JSON frame")?;
        if value.get("id").is_some() {
            return self.handle_rpc_response(epoch, value).await;
        }

        let envelope: StreamEnvelope =
            serde_json::from_value(value).context("unrecognized Extended RPC v2 frame")?;
        self.validate_sequence(envelope.seq)?;
        self.handle_stream(envelope)
    }

    async fn handle_rpc_response(&mut self, epoch: u64, value: Value) -> anyhow::Result<()> {
        let request_id = rpc_id(&value)?;
        let Some(pending) = self.pending.remove(&request_id) else {
            log::debug!("Ignoring stale Extended RPC response id={request_id}");
            return Ok(());
        };
        if pending.epoch != epoch {
            return Ok(());
        }
        if let Some(error) = value.get("error") {
            let error: RpcError = serde_json::from_value(error.clone())?;
            if let Some(topic) = pending.topic {
                self.subscriptions.mark_failure(&topic.key());
            }
            anyhow::bail!("Extended RPC error {}: {}", error.code, error.message);
        }

        let result: RpcResult = serde_json::from_value(
            value
                .get("result")
                .cloned()
                .context("Extended RPC response is missing result")?,
        )?;
        anyhow::ensure!(
            result.status == "OK",
            "Extended RPC status is {}",
            result.status
        );

        match pending.operation {
            RpcOperation::Subscribe => {
                let topic = pending
                    .topic
                    .context("subscribe response missing local topic")?;
                let key = topic.key();
                validate_rpc_ack(&result, "subscribe", &key, true)?;
                self.subscriptions.confirm_subscribe(&key);
                if let Some(book) = self.books.get_mut(&key) {
                    book.confirm();
                }
            }
            RpcOperation::Unsubscribe => {
                let topic = pending
                    .topic
                    .context("unsubscribe response missing local topic")?;
                let key = topic.key();
                validate_rpc_ack(&result, "unsubscribe", &key, false)?;
                self.subscriptions.confirm_unsubscribe(&key);
                if self.subscriptions.get_reference_count(&key) == 0 {
                    self.retire_topic(&topic)?;
                }
            }
        }
        Ok(())
    }

    fn handle_stream(&mut self, envelope: StreamEnvelope) -> anyhow::Result<()> {
        let key = envelope.subscription.clone();
        let topic = self
            .topics
            .get(&key)
            .cloned()
            .with_context(|| format!("unknown Extended subscription `{key}`"))?;
        if self.subscriptions.get_reference_count(&key) == 0 {
            return Ok(());
        }
        anyhow::ensure!(
            !self
                .subscriptions
                .pending_subscribe_topics()
                .iter()
                .any(|pending| pending == &key),
            "Extended data arrived before subscribe ACK for {key}",
        );
        let instrument = self
            .instruments
            .get_cloned(&topic.market)
            .with_context(|| format!("missing Extended instrument for {}", topic.market))?;
        let ts_init = self.clock.get_time_ns();
        let book_sequence = if topic.kind == ExtendedDataKind::OrderBook {
            self.book_sequences
                .get(&instrument.id())
                .copied()
                .unwrap_or_default()
                .checked_add(1)
                .context("Extended canonical book sequence overflow")?
        } else {
            envelope.seq
        };
        let parsed = parse_stream_message(&envelope, &instrument, book_sequence, ts_init)?;
        let Some(parsed) = parsed else {
            return Ok(());
        };
        self.validate_parsed_kind(&topic, &parsed)?;

        match parsed {
            ParsedMessage::Book(deltas) => {
                let state = self
                    .books
                    .get_mut(&key)
                    .context("Extended book state is missing")?;
                if envelope.message_type == "ORDERBOOKS.SNAPSHOT" {
                    state.accept_snapshot()?;
                } else {
                    state.accept_delta()?;
                }
                self.data_sender
                    .send(DataEvent::Data(Data::BookDeltas(Box::new(deltas))))?;
                self.book_sequences.insert(instrument.id(), book_sequence);
            }
            ParsedMessage::Trades(trades) => {
                let state = self.trade_replay.entry(instrument.id()).or_default();
                for trade in state.filter(trades) {
                    self.data_sender.send(DataEvent::Data(Data::Trade(trade)))?;
                }
            }
            ParsedMessage::MarkPrice(value) => {
                self.data_sender
                    .send(DataEvent::Data(Data::MarkPrice(value)))?;
            }
            ParsedMessage::IndexPrice(value) => {
                self.data_sender
                    .send(DataEvent::Data(Data::IndexPrice(value)))?;
            }
            ParsedMessage::FundingRate(value) => {
                self.data_sender.send(DataEvent::FundingRate(value))?;
            }
        }
        Ok(())
    }

    fn validate_parsed_kind(
        &self,
        topic: &ExtendedTopic,
        parsed: &ParsedMessage,
    ) -> anyhow::Result<()> {
        let matches = matches!(
            (topic.kind, parsed),
            (ExtendedDataKind::OrderBook, ParsedMessage::Book(_))
                | (ExtendedDataKind::Trades, ParsedMessage::Trades(_))
                | (ExtendedDataKind::MarkPrice, ParsedMessage::MarkPrice(_))
                | (ExtendedDataKind::IndexPrice, ParsedMessage::IndexPrice(_))
                | (ExtendedDataKind::FundingRate, ParsedMessage::FundingRate(_))
        );
        anyhow::ensure!(matches, "Extended topic and payload family mismatch");
        Ok(())
    }

    async fn handle_reconnected(&mut self, epoch: u64) -> anyhow::Result<()> {
        self.current_epoch = epoch;
        self.failed_epoch = None;
        self.last_sequence = None;
        self.pending.clear();

        let mut topics = self.subscriptions.reset_after_reconnect();
        topics.sort();
        let desired = topics.iter().cloned().collect::<AHashSet<_>>();
        let retired = self
            .topics
            .iter()
            .filter(|(key, _)| !desired.contains(*key))
            .map(|(_, topic)| topic.clone())
            .collect::<Vec<_>>();
        for topic in retired {
            self.retire_topic(&topic)?;
        }
        self.books.retain(|key, _| desired.contains(key));
        for state in self.books.values_mut() {
            state.begin_subscribe();
        }

        for key in topics {
            let topic = self
                .topics
                .get(&key)
                .cloned()
                .with_context(|| format!("missing retained Extended topic `{key}`"))?;
            self.send_topic_request(RpcOperation::Subscribe, topic)
                .await?;
        }
        Ok(())
    }

    fn retire_topic(&mut self, topic: &ExtendedTopic) -> anyhow::Result<()> {
        let key = topic.key();
        self.topics.remove(&key);
        self.books.remove(&key);
        if topic.kind == ExtendedDataKind::Trades {
            self.trade_replay.remove(&topic.instrument_id()?);
        }
        Ok(())
    }

    fn validate_sequence(&mut self, sequence: u64) -> anyhow::Result<()> {
        validate_next_sequence(&mut self.last_sequence, sequence)
    }

    fn check_health(&mut self) {
        if self.overflowed.swap(false, Ordering::AcqRel) {
            self.fail_generation("Extended raw frame queue overflow".to_string());
            return;
        }
        let now = tokio::time::Instant::now();
        if let Some((request_id, request)) = self
            .pending
            .iter()
            .find(|(_, request)| request.deadline <= now)
        {
            let target = request
                .topic
                .as_ref()
                .map_or_else(|| "session".to_string(), ExtendedTopic::key);
            self.fail_generation(format!(
                "Extended RPC {:?} request {request_id} timed out for {target}",
                request.operation,
            ));
            return;
        }
        if let Some((topic, _)) = self
            .books
            .iter()
            .find(|(_, state)| state.deadline.is_some_and(|deadline| deadline <= now))
        {
            self.fail_generation(format!("Extended book stream timed out for {topic}"));
        }
    }

    fn fail_generation(&mut self, reason: String) {
        if self.failed_epoch == Some(self.current_epoch) {
            return;
        }
        self.failed_epoch = Some(self.current_epoch);
        self.last_sequence = None;
        self.pending.clear();
        for state in self.books.values_mut() {
            state.begin_subscribe();
        }
        let outcome = self.reconnect.request_reconnect();
        log::error!(
            "Extended RPC generation failed: {reason}; reconnect={outcome:?}, epoch={}",
            self.current_epoch,
        );
        if matches!(
            outcome,
            ReconnectRequestOutcome::Closed | ReconnectRequestOutcome::Unsupported
        ) {
            self.cancellation.cancel();
        }
    }

    fn take_request_id(&mut self) -> String {
        let request_id = self.next_request_id.to_string();
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        request_id
    }
}

fn validate_next_sequence(last_sequence: &mut Option<u64>, sequence: u64) -> anyhow::Result<()> {
    match *last_sequence {
        None => anyhow::ensure!(
            sequence == 0,
            "first Extended sequence is {sequence}, not 0"
        ),
        Some(last) => anyhow::ensure!(
            sequence == last.saturating_add(1),
            "Extended sequence break: last={last}, current={sequence}",
        ),
    }
    *last_sequence = Some(sequence);
    Ok(())
}

fn validate_rpc_ack(
    result: &RpcResult,
    expected_method: &str,
    expected_topic: &str,
    topic_required: bool,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        result.method == expected_method,
        "unexpected Extended RPC method `{}` for {expected_method}",
        result.method,
    );
    match result.subscription.as_deref() {
        Some(actual) => anyhow::ensure!(
            actual == expected_topic,
            "Extended {expected_method} ACK topic mismatch: expected `{expected_topic}`, received `{actual}`",
        ),
        None => anyhow::ensure!(
            !topic_required,
            "Extended {expected_method} ACK is missing subscription `{expected_topic}`",
        ),
    }
    Ok(())
}

fn rpc_id(value: &Value) -> anyhow::Result<String> {
    match value.get("id") {
        Some(Value::String(value)) => Ok(value.clone()),
        Some(Value::Number(value)) => Ok(value.to_string()),
        _ => anyhow::bail!("Extended RPC response has invalid request id"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trade_replay_seeds_then_emits_only_unseen_ids() {
        use nautilus_core::UnixNanos;
        use nautilus_model::{
            enums::AggressorSide,
            identifiers::{InstrumentId, Symbol, TradeId, Venue},
            types::{Price, Quantity},
        };

        fn trade(id: &str) -> TradeTick {
            TradeTick::new(
                InstrumentId::new(Symbol::new("BTC-USD-PERP"), Venue::new("EXTENDED")),
                Price::from("100.0"),
                Quantity::from("1.0"),
                AggressorSide::Buy,
                TradeId::new(id),
                UnixNanos::from(1),
                UnixNanos::from(2),
            )
        }

        let mut state = TradeReplayState::default();
        assert!(state.filter(vec![trade("1"), trade("2")]).is_empty());
        let emitted = state.filter(vec![trade("2"), trade("3")]);
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].trade_id.as_str(), "3");
    }

    #[test]
    fn global_sequence_allows_interleaved_topics_but_not_gaps() {
        let mut last = None;
        for sequence in [0, 1, 2] {
            validate_next_sequence(&mut last, sequence).unwrap();
        }
        assert!(validate_next_sequence(&mut last, 4).is_err());
        assert_eq!(last, Some(2));

        let mut fresh = None;
        assert!(validate_next_sequence(&mut fresh, 1).is_err());
        assert_eq!(fresh, None);
    }

    #[test]
    fn rpc_ack_topic_is_required_for_subscribe_and_optional_for_unsubscribe() {
        let subscribe = RpcResult {
            method: "subscribe".to_string(),
            status: "OK".to_string(),
            subscription: None,
            subscriptions: None,
        };
        assert!(validate_rpc_ack(&subscribe, "subscribe", "trades.BTC-USD", true).is_err());

        let unsubscribe = RpcResult {
            method: "unsubscribe".to_string(),
            status: "OK".to_string(),
            subscription: None,
            subscriptions: None,
        };
        validate_rpc_ack(&unsubscribe, "unsubscribe", "trades.BTC-USD", false).unwrap();

        let mismatched = RpcResult {
            method: "unsubscribe".to_string(),
            status: "OK".to_string(),
            subscription: Some("prices.mark.BTC-USD".to_string()),
            subscriptions: None,
        };
        assert!(validate_rpc_ack(&mismatched, "unsubscribe", "trades.BTC-USD", false).is_err());
    }
}
