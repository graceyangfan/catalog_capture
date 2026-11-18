// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Single-owner Native transport session. All control and publication is epoch-bound.
use super::{
    messages::{
        BulletCommand, BulletDataKind, BulletMessageType, BulletTopic, BulletWsMessage,
        decode_message,
    },
    parse::{parse_book, parse_prices, parse_trade},
};
use crate::config::BulletDataClientConfig;
use ahash::{AHashMap, AHashSet};
use anyhow::Context;
use nautilus_common::{live::sender::EventSender, messages::DataEvent};
use nautilus_core::{AtomicMap, UnixNanos, time::AtomicTime};
use nautilus_live::SocketControl;
use nautilus_model::{
    data::{Data, OrderBookDelta, OrderBookDeltas},
    enums::RecordFlag,
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_network::{
    RECONNECTED, SocketState,
    http::create_standard_nautilus_headers,
    mode::ReconnectRequestOutcome,
    transport::Message,
    websocket::{
        EpochMessageHandler, InitialConnectRetryPolicy, SubscriptionState, WebSocketClient,
        WebSocketConfig, WebSocketReconnectHandle,
    },
};
use std::{
    collections::VecDeque,
    future::Future,
    num::NonZeroU32,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use ustr::Ustr;

const RAW_CAPACITY: usize = 256;
const RAW_BYTES: usize = 8 * 1024 * 1024;
const FRAME_BYTES: usize = 2 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const TRADE_IDS: usize = 10_000;

type SubscriptionWrite = Pin<Box<dyn Future<Output = (u64, anyhow::Result<()>)> + Send>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operation {
    Subscribe,
    Unsubscribe,
}
#[derive(Debug)]
enum Phase {
    Absent,
    Pending {
        operation: Operation,
        id: u64,
        deadline: tokio::time::Instant,
    },
    Live,
}
#[derive(Debug)]
struct TopicState {
    topic: BulletTopic,
    phase: Phase,
    last_u: Option<u64>,
    book_ready: bool,
    snapshot_deadline: Option<tokio::time::Instant>,
    staged: VecDeque<(BulletWsMessage, UnixNanos)>,
    staged_bytes: usize,
}
impl TopicState {
    fn new(topic: BulletTopic) -> Self {
        Self {
            topic,
            phase: Phase::Absent,
            last_u: None,
            book_ready: false,
            snapshot_deadline: None,
            staged: VecDeque::new(),
            staged_bytes: 0,
        }
    }
    fn reset(&mut self) {
        self.phase = Phase::Absent;
        self.last_u = None;
        self.snapshot_deadline = None;
        self.staged.clear();
        self.staged_bytes = 0;
    }
}
#[derive(Debug, Default)]
struct TradeDedupe {
    ids: AHashSet<u64>,
    order: VecDeque<u64>,
}
impl TradeDedupe {
    fn contains(&self, id: u64) -> bool {
        self.ids.contains(&id)
    }
    fn remember(&mut self, id: u64) {
        if !self.ids.insert(id) {
            return;
        }
        self.order.push_back(id);
        if self.order.len() > TRADE_IDS
            && let Some(id) = self.order.pop_front()
        {
            self.ids.remove(&id);
        }
    }
}

pub struct BulletWebSocketSession {
    client: Arc<WebSocketClient>,
    // The registration belongs to this session; dropping the control deregisters the endpoint.
    _socket_control: SocketControl,
    reconnect: WebSocketReconnectHandle,
    raw_rx: mpsc::Receiver<(u64, Message, usize)>,
    command_rx: mpsc::Receiver<BulletCommand>,
    loss_rx: watch::Receiver<u64>,
    observed_loss: u64,
    queued_bytes: Arc<AtomicUsize>,
    overflow_epoch: Arc<AtomicU64>,
    logical: SubscriptionState,
    subscriptions: SubscriptionState,
    topics: AHashMap<String, TopicState>,
    trades: AHashMap<InstrumentId, TradeDedupe>,
    book_timestamps: AHashMap<InstrumentId, UnixNanos>,
    instruments: Arc<AtomicMap<Ustr, InstrumentAny>>,
    sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
    cancellation: CancellationToken,
    epoch: u64,
    failed: bool,
    next_id: u64,
    subscription_write: Option<SubscriptionWrite>,
}
impl std::fmt::Debug for BulletWebSocketSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BulletWebSocketSession")
            .field("epoch", &self.epoch)
            .field("topics", &self.topics)
            .finish_non_exhaustive()
    }
}
impl BulletWebSocketSession {
    pub async fn connect(
        config: &BulletDataClientConfig,
        instruments: Arc<AtomicMap<Ustr, InstrumentAny>>,
        sender: EventSender<DataEvent>,
        clock: &'static AtomicTime,
        cancellation: CancellationToken,
        socket_control: SocketControl,
    ) -> anyhow::Result<(mpsc::Sender<BulletCommand>, Self)> {
        let (raw_tx, raw_rx) = mpsc::channel(RAW_CAPACITY);
        // Remember loss edges even if the transport reconnects before the owner is polled.
        let (loss_tx, loss_rx) = watch::channel(0_u64);
        let state_sink = socket_control.sink_with(move |state| {
            if state == SocketState::Disconnected {
                loss_tx.send_modify(|revision| *revision = revision.wrapping_add(1));
            }
        });
        let queued_bytes = Arc::new(AtomicUsize::new(0));
        let overflow_epoch = Arc::new(AtomicU64::new(0));
        let bytes = queued_bytes.clone();
        let overflow = overflow_epoch.clone();
        let handler: EpochMessageHandler = Arc::new(move |epoch, message| {
            let size = message.as_text().map_or(FRAME_BYTES, str::len);
            let previous = bytes.fetch_add(size, Ordering::AcqRel);
            if previous.saturating_add(size) > RAW_BYTES {
                bytes.fetch_sub(size, Ordering::AcqRel);
                overflow.fetch_max(epoch.saturating_add(1), Ordering::AcqRel);
                return;
            }
            if raw_tx.try_send((epoch, message, size)).is_err() {
                bytes.fetch_sub(size, Ordering::AcqRel);
                overflow.fetch_max(epoch.saturating_add(1), Ordering::AcqRel);
            }
        });
        let ws_config = WebSocketConfig {
            url: config.ws_url(),
            headers: create_standard_nautilus_headers(),
            heartbeat_interval_secs: Some(20),
            heartbeat_payload: Some(r#"{"method":"PING"}"#.to_string()),
            connect_timeout_ms: Some(config.ws_timeout_secs.saturating_mul(1000).max(1)),
            reconnect_delay_initial_ms: Some(1000),
            reconnect_delay_max_ms: Some(30_000),
            reconnect_backoff_factor: Some(2.0),
            reconnect_jitter_ms: Some(250),
            reconnect_max_attempts: None,
            heartbeat_timeout_secs: Some(65),
            idle_timeout_ms: None,
            writer_capacity: Some(256),
            backend: config.transport_backend,
            proxy_url: config.proxy_url.clone(),
            max_message_size_bytes: Some(FRAME_BYTES),
            max_frame_size_bytes: Some(FRAME_BYTES),
        };
        let client = WebSocketClient::epoch_builder()
            .config(ws_config)
            .epoch_handler(handler)
            .state_sink(state_sink)
            .initial_connect_retry_policy(InitialConnectRetryPolicy {
                max_attempts: NonZeroU32::new(5).context("invalid connect attempts")?,
                delay_initial: Duration::from_secs(1),
                delay_max: Duration::from_secs(5),
                backoff_factor: 2.0,
                jitter_ms: 250,
            })
            .cancellation_token(cancellation.clone())
            .connect()
            .await
            .context("failed to connect Bullet WebSocket")?;
        let reconnect = client.reconnect_handle();
        let control = reconnect.clone();
        socket_control.register(move || control.request_reconnect());
        let epoch = client.connection_epoch();
        let (tx, command_rx) = mpsc::channel(256);
        Ok((
            tx,
            Self {
                client: Arc::new(client),
                _socket_control: socket_control,
                reconnect,
                raw_rx,
                command_rx,
                loss_rx,
                observed_loss: 0,
                queued_bytes,
                overflow_epoch,
                logical: SubscriptionState::new('@'),
                subscriptions: SubscriptionState::new('@'),
                topics: AHashMap::new(),
                trades: AHashMap::new(),
                book_timestamps: AHashMap::new(),
                instruments,
                sender,
                clock,
                cancellation,
                epoch,
                failed: false,
                next_id: 1,
                subscription_write: None,
            },
        ))
    }
    pub async fn run(mut self) {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        loop {
            if !self.failed
                && self.epoch == self.client.connection_epoch()
                && let Err(error) = self.reconcile()
            {
                self.fail_generation(format!("{error:#}"));
            }
            tokio::select! {
                biased;
                () = self.cancellation.cancelled() => break,
                _ = self.loss_rx.changed() => {
                    if let Err(error) = self.observe_transport_loss() {
                        self.fail_generation(format!("{error:#}"));
                    }
                }
                (epoch, result) = async {
                    match &mut self.subscription_write {
                        Some(write) => write.await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.subscription_write = None;
                    if epoch == self.epoch && epoch == self.client.connection_epoch()
                        && !self.failed && let Err(error) = result
                    {
                        self.fail_generation(format!("{error:#}"));
                    }
                }
                _ = tick.tick() => self.check_health(),
                command = self.command_rx.recv(), if self.subscription_write.is_none() => {
                    let Some(command) = command else { break };
                    self.handle_command(command);
                }
                raw = self.raw_rx.recv() => {
                    let Some((epoch, message, size)) = raw else { break };
                    self.queued_bytes.fetch_sub(size, Ordering::AcqRel);
                    if let Err(error) = self.handle_message(epoch, message, size) {
                        self.fail_generation(format!("{error:#}"));
                    }
                }
            }
        }
        self.subscription_write = None;
        if let Err(error) = self.invalidate_books() {
            log::error!("Bullet teardown book invalidation failed: {error:#}");
        }
        self.logical.clear();
        self.subscriptions.clear();
        self.topics.clear();
        self.client.disconnect().await;
    }
    fn handle_command(&mut self, command: BulletCommand) {
        let (topic, subscribe) = match command {
            BulletCommand::Subscribe(topic) => (topic, true),
            BulletCommand::Unsubscribe(topic) => (topic, false),
        };
        let logical = topic.logical_key();
        let key = topic.key();
        if subscribe {
            self.logical.add_reference(&logical);
            self.subscriptions.add_reference(&key);
            self.topics
                .entry(key)
                .or_insert_with(|| TopicState::new(topic));
        } else if self.logical.get_reference_count(&logical) > 0 {
            self.logical.remove_reference(&logical);
            self.subscriptions.remove_reference(&key);
            if self.subscriptions.get_reference_count(&key) == 0 {
                if let Err(error) = self.invalidate_book(&key) {
                    self.fail_generation(format!("{error:#}"));
                    return;
                }
                if let Some(state) = self.topics.get_mut(&key) {
                    state.last_u = None;
                    state.snapshot_deadline = None;
                    state.staged.clear();
                    state.staged_bytes = 0;
                }
            }
        }
    }
    fn reconcile(&mut self) -> anyhow::Result<()> {
        if self.subscription_write.is_some() || self.cancellation.is_cancelled() {
            return Ok(());
        }
        let mut actions = self
            .topics
            .iter()
            .filter_map(|(key, state)| {
                let desired = self.subscriptions.get_reference_count(key) > 0;
                match (&state.phase, desired) {
                    (Phase::Absent, true) => Some((key.clone(), Operation::Subscribe)),
                    (Phase::Live, false) => Some((key.clone(), Operation::Unsubscribe)),
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        actions.sort_by(|a, b| a.0.cmp(&b.0));
        if let Some((key, operation)) = actions.into_iter().next() {
            let id = self.next_id;
            self.next_id = id.checked_add(1).context("Bullet request ID overflow")?;
            let state = self.topics.get_mut(&key).context("missing Bullet topic")?;
            state.phase = Phase::Pending {
                operation,
                id,
                deadline: tokio::time::Instant::now() + REQUEST_TIMEOUT,
            };
            match operation {
                Operation::Subscribe => {
                    self.subscriptions.mark_subscribe(&key);
                    state.last_u = None;
                }
                Operation::Unsubscribe => self.subscriptions.mark_unsubscribe(&key),
            }
            let method = match operation {
                Operation::Subscribe => "SUBSCRIBE",
                Operation::Unsubscribe => "UNSUBSCRIBE",
            };
            let text = serde_json::json!({"id":id,"method":method,"params":[key]}).to_string();
            let client = Arc::clone(&self.client);
            let epoch = self.epoch;
            self.subscription_write = Some(Box::pin(async move {
                let result = async {
                    tokio::time::timeout(
                        REQUEST_TIMEOUT,
                        client.send_text_on_connection(text, None, epoch),
                    )
                    .await
                    .context("Bullet subscription write timed out")??;
                    Ok(())
                }
                .await;
                (epoch, result)
            }));
        }
        // An absent, ownerless topic no longer needs protocol or replay state.
        let retired = self
            .topics
            .iter()
            .filter(|(key, state)| {
                self.subscriptions.get_reference_count(key) == 0
                    && matches!(state.phase, Phase::Absent)
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in retired {
            if let Some(state) = self.topics.remove(&key)
                && state.topic.kind == BulletDataKind::Trades
                && let Ok(id) =
                    crate::common::instrument_id_from_market(state.topic.market.as_str())
            {
                self.trades.remove(&id);
            }
        }
        Ok(())
    }
    fn handle_message(&mut self, epoch: u64, message: Message, bytes: usize) -> anyhow::Result<()> {
        self.observe_transport_loss()?;
        // Check provenance before interpreting even a control marker or malformed frame.
        if epoch != self.client.connection_epoch() || epoch < self.epoch {
            return Ok(());
        }
        let text = message
            .as_text()
            .context("Bullet sent non-text application frame")?;
        if text == RECONNECTED {
            self.reset_epoch(epoch)?;
            return Ok(());
        }
        if epoch != self.epoch || self.failed {
            return Ok(());
        }
        anyhow::ensure!(
            self.overflow_epoch.load(Ordering::Acquire) != epoch.saturating_add(1),
            "Bullet raw queue overflow"
        );
        let message = decode_message(text.as_bytes())?;
        match message {
            BulletWsMessage::Subscribe(ack) => {
                self.acknowledge(ack.id, ack.result, Operation::Subscribe)
            }
            BulletWsMessage::Unsubscribe(ack) => {
                self.acknowledge(ack.id, ack.result, Operation::Unsubscribe)
            }
            BulletWsMessage::Error(error) => anyhow::bail!(
                "Bullet WS error id={:?} {}: {}",
                error.id,
                error.error.code,
                error.error.msg
            ),
            BulletWsMessage::Status(status) => {
                if status.status == "disconnecting" {
                    anyhow::bail!("Bullet disconnecting: {:?}", status.reason);
                }
                Ok(())
            }
            BulletWsMessage::Pong(_) => Ok(()),
            data => {
                let key = match &data {
                    BulletWsMessage::Depth(m) => {
                        format!("{}@depth{}", m.symbol, crate::common::BULLET_BOOK_DEPTH)
                    }
                    BulletWsMessage::AggTrade(m) => format!("{}@aggTrade", m.symbol),
                    BulletWsMessage::MarkPrice(m) => format!("{}@markPrice", m.symbol),
                    _ => unreachable!(),
                };
                if self.subscriptions.get_reference_count(&key) == 0 {
                    return Ok(());
                }
                let (staged_items, staged_bytes) = if matches!(
                    self.topics.get(&key).map(|s| &s.phase),
                    Some(Phase::Pending {
                        operation: Operation::Subscribe,
                        ..
                    })
                ) {
                    (
                        self.topics.values().map(|s| s.staged.len()).sum::<usize>(),
                        self.topics.values().map(|s| s.staged_bytes).sum::<usize>(),
                    )
                } else {
                    (0, 0)
                };
                let state = self.topics.get_mut(&key).context("unowned Bullet stream")?;
                let ts_init = self.clock.get_time_ns();
                match state.phase {
                    Phase::Live => self.publish(&key, data, ts_init),
                    Phase::Pending {
                        operation: Operation::Subscribe,
                        ..
                    } => {
                        anyhow::ensure!(
                            staged_items < RAW_CAPACITY
                                && staged_bytes.saturating_add(bytes) <= RAW_BYTES,
                            "Bullet pre-ACK buffer overflow"
                        );
                        state.staged.push_back((data, ts_init));
                        state.staged_bytes += bytes;
                        Ok(())
                    }
                    _ => Ok(()),
                }
            }
        }
    }
    fn acknowledge(
        &mut self,
        id: Option<u64>,
        result: String,
        operation: Operation,
    ) -> anyhow::Result<()> {
        let id = id.context("Bullet ACK missing request ID")?;
        let key = self.topics.iter().find_map(|(key, state)| {
            matches!(state.phase,Phase::Pending {id:pending,..} if pending==id).then(|| key.clone())
        });
        let Some(key) = key else {
            return Ok(());
        };
        let state = self
            .topics
            .get_mut(&key)
            .context("missing Bullet ACK topic")?;
        anyhow::ensure!(
            matches!(state.phase,Phase::Pending {operation:expected,..} if expected==operation),
            "Bullet ACK operation mismatch"
        );
        anyhow::ensure!(
            result == "success",
            "Bullet subscription rejected: {result}"
        );
        log::info!(
            "Bullet {:?} confirmed: topic={}, epoch={}",
            operation,
            key,
            self.epoch
        );
        match operation {
            Operation::Subscribe => {
                self.subscriptions.confirm_subscribe(&key);
                state.phase = Phase::Live;
                if state.topic.kind == BulletDataKind::OrderBook
                    && self.subscriptions.get_reference_count(&key) > 0
                {
                    state.snapshot_deadline = Some(tokio::time::Instant::now() + REQUEST_TIMEOUT);
                }
                let staged = std::mem::take(&mut state.staged);
                state.staged_bytes = 0;
                for (message, ts_init) in staged {
                    self.publish(&key, message, ts_init)?;
                }
            }
            Operation::Unsubscribe => {
                self.subscriptions.confirm_unsubscribe(&key);
                state.reset();
            }
        }
        Ok(())
    }
    fn has_owner(&self, market: Ustr, kind: BulletDataKind) -> bool {
        self.logical
            .get_reference_count(&BulletTopic { kind, market }.logical_key())
            > 0
    }
    fn publish(
        &mut self,
        key: &str,
        message: BulletWsMessage,
        ts_init: UnixNanos,
    ) -> anyhow::Result<()> {
        if self.subscriptions.get_reference_count(key) == 0 {
            return Ok(());
        }
        let market = self
            .topics
            .get(key)
            .context("missing Bullet publication topic")?
            .topic
            .market;
        let instrument = self
            .instruments
            .get_cloned(&market)
            .context("missing Bullet instrument")?;
        match message {
            BulletWsMessage::Depth(message) => {
                let state = self.topics.get(key).context("missing Bullet book state")?;
                let snapshot = message.message_type == BulletMessageType::Snapshot;
                if !snapshot {
                    anyhow::ensure!(
                        state.last_u == Some(message.prev_update_id),
                        "Bullet book gap or update before snapshot"
                    );
                    anyhow::ensure!(
                        message.last_update_id > message.prev_update_id,
                        "non-increasing Bullet update ID"
                    );
                }
                let batch = parse_book(&message, &instrument, ts_init)?;
                let source_time = super::parse::micros_to_nanos(message.transaction_time)?;
                anyhow::ensure!(
                    self.book_timestamps
                        .get(&instrument.id())
                        .is_none_or(|previous| source_time >= *previous),
                    "Bullet book state time regressed; waiting for a current snapshot"
                );
                if snapshot
                    && state.last_u.is_some_and(|last| {
                        message.last_update_id < last
                            || (message.last_update_id > last && message.prev_update_id != last)
                    })
                {
                    self.invalidate_book(key)?;
                }
                if let Some(batch) = batch {
                    self.sender
                        .send(DataEvent::Data(Data::BookDeltas(Box::new(batch))))?;
                    self.book_timestamps.insert(instrument.id(), source_time);
                    self.topics
                        .get_mut(key)
                        .context("missing book topic")?
                        .book_ready = true;
                }
                let state = self.topics.get_mut(key).context("missing book topic")?;
                state.last_u = Some(message.last_update_id);
                state.snapshot_deadline = None;
            }
            BulletWsMessage::AggTrade(message) => {
                let trade = parse_trade(&message, &instrument, ts_init)?;
                let seen = self.trades.entry(instrument.id()).or_default();
                if !seen.contains(message.agg_trade_id) {
                    self.sender.send(DataEvent::Data(Data::Trade(trade)))?;
                    seen.remember(message.agg_trade_id);
                }
            }
            BulletWsMessage::MarkPrice(message) => {
                let prices = parse_prices(&message, &instrument, ts_init)?;
                if self.has_owner(market, BulletDataKind::MarkPrice) {
                    self.sender
                        .send(DataEvent::Data(Data::MarkPrice(prices.mark)))?;
                }
                if self.has_owner(market, BulletDataKind::IndexPrice) {
                    self.sender
                        .send(DataEvent::Data(Data::IndexPrice(prices.index)))?;
                }
            }
            _ => anyhow::bail!("Bullet control message reached data parser"),
        }
        Ok(())
    }
    fn invalidate_book(&mut self, key: &str) -> anyhow::Result<()> {
        let Some(state) = self.topics.get_mut(key) else {
            return Ok(());
        };
        if !state.book_ready {
            return Ok(());
        }
        let id = state.topic.instrument_id()?;
        let source_time = *self
            .book_timestamps
            .get(&id)
            .context("missing published book timestamp")?;
        let ts_init = self.clock.get_time_ns();
        let mut clear = OrderBookDelta::clear(id, 0, source_time, ts_init);
        clear.flags = RecordFlag::F_LAST as u8;
        if let Err(error) = self.sender.send(DataEvent::Data(Data::BookDeltas(Box::new(
            OrderBookDeltas::new(id, vec![clear]),
        )))) {
            self.cancellation.cancel();
            return Err(error.into());
        }
        state.book_ready = false;
        state.last_u = None;
        log::warn!(
            "Bullet book invalidated: instrument_id={id}, epoch={}, ts_init_ns={}; awaiting source snapshot",
            self.epoch,
            ts_init.as_u64()
        );
        Ok(())
    }
    fn invalidate_books(&mut self) -> anyhow::Result<()> {
        let keys = self
            .topics
            .iter()
            .filter(|(_, state)| state.book_ready)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in keys {
            self.invalidate_book(&key)?;
        }
        Ok(())
    }
    fn observe_transport_loss(&mut self) -> anyhow::Result<()> {
        let revision = *self.loss_rx.borrow_and_update();
        if revision == self.observed_loss {
            return Ok(());
        }
        self.observed_loss = revision;
        self.failed = true;
        self.subscription_write = None;
        self.invalidate_books()?;
        for state in self.topics.values_mut() {
            state.reset();
        }
        Ok(())
    }
    fn reset_epoch(&mut self, epoch: u64) -> anyhow::Result<()> {
        if epoch <= self.epoch {
            return Ok(());
        }
        self.invalidate_books()?;
        self.subscription_write = None;
        self.epoch = epoch;
        self.failed = false;
        self.subscriptions.reset_after_reconnect();
        for state in self.topics.values_mut() {
            state.reset();
        }
        log::warn!(
            "Bullet epoch {} reconnected; resubscribing desired topics; missed observations remain a data gap",
            epoch
        );
        Ok(())
    }
    fn check_health(&mut self) {
        if let Err(error) = self.observe_transport_loss() {
            self.fail_generation(format!("{error:#}"));
        }
        if self.failed || self.client.connection_epoch() != self.epoch {
            return;
        }
        let now = tokio::time::Instant::now();
        if self.overflow_epoch.load(Ordering::Acquire) == self.epoch.saturating_add(1) {
            self.fail_generation("raw ingress overflow".to_string());
            return;
        }
        if self.topics.values().any(|state| {
            matches!(state.phase,Phase::Pending {deadline,..} if deadline<=now)
                || state
                    .snapshot_deadline
                    .is_some_and(|deadline| deadline <= now)
        }) {
            self.fail_generation("subscription ACK or snapshot timed out".to_string());
        }
    }
    fn fail_generation(&mut self, reason: String) {
        if self.failed {
            return;
        }
        self.failed = true;
        self.subscription_write = None;
        if let Err(error) = self.invalidate_books() {
            log::error!("Bullet book invalidation delivery failed: {error:#}");
            self.cancellation.cancel();
        }
        for state in self.topics.values_mut() {
            state.reset();
        }
        let outcome = self.reconnect.request_reconnect();
        log::error!(
            "Bullet generation failed: {reason}; epoch={}, reconnect={outcome:?}; data continuity lost",
            self.epoch
        );
        if matches!(
            outcome,
            ReconnectRequestOutcome::Closed | ReconnectRequestOutcome::Unsupported
        ) {
            self.cancellation.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        extract::{
            State,
            ws::{Message as AxumMessage, WebSocketUpgrade},
        },
        response::IntoResponse,
        routing::get,
    };
    use nautilus_core::time::get_atomic_clock_realtime;
    use nautilus_live::SocketControlFactory;
    use nautilus_model::identifiers::ClientId;
    use serde_json::{Value, json};

    async fn mock_socket(
        State(connections): State<Arc<AtomicU64>>,
        upgrade: WebSocketUpgrade,
    ) -> impl IntoResponse {
        let connection = connections.fetch_add(1, Ordering::AcqRel);
        upgrade.on_upgrade(move |mut socket| async move {
            while let Some(Ok(AxumMessage::Text(text)))=socket.recv().await {
                let request:Value=serde_json::from_str(&text).unwrap();
                let subscribe=request["method"]=="SUBSCRIBE";
                let topic=request["params"][0].as_str().unwrap();
                let data=if subscribe && topic.ends_with("depth20") {
                    let mut v:Value=serde_json::from_slice(include_bytes!("../../test_data/ws_depth_snapshot.json")).unwrap();
                    let u=if connection==0 {42_550_033_u64}else{2_744_357};
                    v["U"]=json!(u);v["u"]=json!(u);Some(v)
                } else if subscribe && topic.ends_with("aggTrade") {
                    let rows:Vec<Value>=serde_json::from_slice(include_bytes!("../../test_data/ws_mainnet_btc_probe.json")).unwrap();
                    Some(rows.into_iter().find(|v|v["e"]=="aggTrade").unwrap())
                } else { None };
                // Deliberately send the snapshot before ACK to exercise the bounded admission gate.
                if let Some(ref data)=data { socket.send(AxumMessage::Text(data.to_string().into())).await.unwrap(); }
                let ack=json!({"e":if subscribe {"subscribe"}else{"unsubscribe"},"id":request["id"],"E":1,"result":"success"});
                if socket.send(AxumMessage::Text(ack.to_string().into())).await.is_err(){break;}
                if let Some(ref data)=data && data["e"]=="aggTrade" { let _=socket.send(AxumMessage::Text(data.to_string().into())).await; }
            }
        })
    }
    async fn harness() -> (
        BulletWebSocketSession,
        mpsc::UnboundedReceiver<DataEvent>,
        tokio::task::JoinHandle<()>,
    ) {
        harness_with_control(
            SocketControlFactory::new(ClientId::from("BULLET"), Some(*crate::common::BULLET_VENUE))
                .control("test"),
        )
        .await
    }
    async fn harness_with_control(
        control: SocketControl,
    ) -> (
        BulletWebSocketSession,
        mpsc::UnboundedReceiver<DataEvent>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/ws", get(mock_socket))
                    .with_state(Arc::new(AtomicU64::new(0))),
            )
            .await
            .unwrap();
        });
        let instruments = Arc::new(AtomicMap::new());
        let btc = crate::http::tests::btc();
        instruments.store([(btc.raw_symbol().inner(), btc)].into_iter().collect());
        let (tx, rx) = mpsc::unbounded_channel();
        let config = BulletDataClientConfig {
            base_url_ws: Some(format!("ws://{address}/ws")),
            ..Default::default()
        };
        let (_, session) = BulletWebSocketSession::connect(
            &config,
            instruments,
            tx.into(),
            get_atomic_clock_realtime(),
            CancellationToken::new(),
            control,
        )
        .await
        .unwrap();
        (session, rx, server)
    }
    impl BulletWebSocketSession {
        async fn reconcile_and_write(&mut self) {
            loop {
                self.reconcile().unwrap();
                let Some(write) = self.subscription_write.take() else {
                    break;
                };
                let (epoch, result) = write.await;
                assert_eq!(epoch, self.epoch);
                result.unwrap();
            }
        }
    }

    fn topic(kind: BulletDataKind) -> BulletTopic {
        BulletTopic::new(
            kind,
            crate::common::instrument_id_from_market("BTC-USD").unwrap(),
        )
        .unwrap()
    }
    fn fixture(bytes: &[u8]) -> BulletWsMessage {
        decode_message(bytes).unwrap()
    }
    #[tokio::test]
    async fn session_teardown_invalidates_before_a_replacement_can_publish_lower_u() {
        for cancelled in [true, false] {
            let (mut session, mut events, server) = harness().await;
            let key = topic(BulletDataKind::OrderBook).key();
            session.handle_command(BulletCommand::Subscribe(topic(BulletDataKind::OrderBook)));
            // The ready source state is synthetic; exercise the real run teardown below.
            session.topics.get_mut(&key).unwrap().phase = Phase::Live;
            session
                .publish(
                    &key,
                    fixture(include_bytes!("../../test_data/ws_depth_snapshot.json")),
                    1.into(),
                )
                .unwrap();
            let DataEvent::Data(Data::BookDeltas(first)) = events.try_recv().unwrap() else {
                panic!("source snapshot")
            };
            let mut book = nautilus_model::orderbook::OrderBook::new(
                first.instrument_id,
                nautilus_model::enums::BookType::L2_MBP,
            );
            book.apply_deltas(&first).unwrap();
            assert_eq!(book.sequence, 1000);
            if cancelled {
                session.cancellation.cancel();
            }
            // The harness command sender is closed, covering owner-channel termination as well.
            session.run().await;
            let DataEvent::Data(Data::BookDeltas(cut)) = events.try_recv().unwrap() else {
                panic!("teardown cut")
            };
            assert_eq!(cut.deltas.len(), 1);
            assert_eq!(cut.flags, RecordFlag::F_LAST as u8);
            book.apply_deltas(&cut).unwrap();
            assert_eq!(book.sequence, 0);
            assert_eq!(book.bids(None).count(), 0);
            assert!(
                events.try_recv().is_err(),
                "teardown emits one invalidation"
            );
            let mut replacement = first.as_ref().clone();
            for row in &mut replacement.deltas {
                row.sequence = 1;
            }
            // Rebuild the frame's derived metadata after changing the source sequence.
            let replacement = OrderBookDeltas::new(first.instrument_id, replacement.deltas);
            book.apply_deltas(&replacement).unwrap();
            assert_eq!(book.sequence, 1);
            assert_eq!(book.bids(None).count(), 2);
            server.abort();
        }
    }

    #[tokio::test]
    async fn pending_subscription_write_does_not_block_transport_invalidation() {
        let (mut session, mut events, server) = harness().await;
        let key = topic(BulletDataKind::OrderBook).key();
        session.handle_command(BulletCommand::Subscribe(topic(BulletDataKind::OrderBook)));
        session.topics.get_mut(&key).unwrap().phase = Phase::Live;
        session
            .publish(
                &key,
                fixture(include_bytes!("../../test_data/ws_depth_snapshot.json")),
                1.into(),
            )
            .unwrap();
        let _first = events.try_recv().unwrap();
        let (_command_tx, command_rx) = mpsc::channel(1);
        session.command_rx = command_rx;
        let (loss_tx, loss_rx) = watch::channel(0);
        session.loss_rx = loss_rx;
        let started = Arc::new(tokio::sync::Notify::new());
        let write_started = started.clone();
        session.subscription_write = Some(Box::pin(async move {
            write_started.notify_one();
            std::future::pending().await
        }));
        let cancellation = session.cancellation.clone();
        let task = tokio::spawn(session.run());
        started.notified().await;
        loss_tx.send(1).unwrap();
        let Some(DataEvent::Data(Data::BookDeltas(cut))) =
            tokio::time::timeout(Duration::from_secs(1), events.recv())
                .await
                .unwrap()
        else {
            panic!("loss cut")
        };
        assert_eq!(cut.sequence, 0);
        assert_eq!(cut.flags, RecordFlag::F_LAST as u8);
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert!(
            events.try_recv().is_err(),
            "cancellation must not repeat the loss cut"
        );
        server.abort();
    }
    #[tokio::test]
    async fn shared_price_topic_keeps_survivor_and_serializes_pending_release() {
        let (mut session, mut events, server) = harness().await;
        let mark = topic(BulletDataKind::MarkPrice);
        let index = topic(BulletDataKind::IndexPrice);
        let key = mark.key();
        session.handle_command(BulletCommand::Subscribe(mark.clone()));
        session.handle_command(BulletCommand::Subscribe(index.clone()));
        session.reconcile_and_write().await;
        let id = match session.topics[&key].phase {
            Phase::Pending { id, .. } => id,
            _ => panic!(),
        };
        session
            .acknowledge(Some(id), "success".into(), Operation::Subscribe)
            .unwrap();
        session.handle_command(BulletCommand::Unsubscribe(mark));
        session.reconcile_and_write().await;
        assert!(matches!(session.topics[&key].phase, Phase::Live));
        session
            .publish(
                &key,
                fixture(include_bytes!("../../test_data/ws_mark_price.json")),
                UnixNanos::from(2),
            )
            .unwrap();
        assert!(matches!(
            events.try_recv().unwrap(),
            DataEvent::Data(Data::IndexPrice(_))
        ));
        assert!(events.try_recv().is_err());
        session.handle_command(BulletCommand::Unsubscribe(index.clone()));
        session.reconcile_and_write().await;
        let id = match session.topics[&key].phase {
            Phase::Pending { id, .. } => id,
            _ => panic!(),
        };
        session.handle_command(BulletCommand::Subscribe(index));
        session
            .acknowledge(Some(id), "success".into(), Operation::Unsubscribe)
            .unwrap();
        session.reconcile_and_write().await;
        assert!(matches!(
            session.topics[&key].phase,
            Phase::Pending {
                operation: Operation::Subscribe,
                ..
            }
        ));
        session.client.disconnect().await;
        server.abort();
    }
    #[tokio::test]
    async fn raw_stale_epoch_and_wrong_ack_cannot_publish_or_confirm() {
        let (mut session, mut events, server) = harness().await;
        let book = topic(BulletDataKind::OrderBook);
        let key = book.key();
        session.handle_command(BulletCommand::Subscribe(book));
        session.reconcile_and_write().await;
        let id = match session.topics[&key].phase {
            Phase::Pending { id, .. } => id,
            _ => panic!(),
        };
        assert!(
            session
                .acknowledge(Some(id), "success".into(), Operation::Unsubscribe)
                .is_err()
        );
        assert!(
            session
                .acknowledge(Some(id), "rejected".into(), Operation::Subscribe)
                .is_err()
        );
        let old_epoch = session.epoch;
        session.epoch += 1;
        session
            .handle_message(old_epoch, Message::text("not even JSON"), 0)
            .unwrap();
        session.epoch = old_epoch;
        let mut value: Value =
            serde_json::from_slice(include_bytes!("../../test_data/ws_depth_snapshot.json"))
                .unwrap();
        value["mt"] = json!("u");
        session
            .acknowledge(Some(id), "success".into(), Operation::Subscribe)
            .unwrap();
        assert!(
            session
                .publish(
                    &key,
                    fixture(value.to_string().as_bytes()),
                    UnixNanos::from(1)
                )
                .is_err()
        );
        assert!(events.try_recv().is_err());
        session.client.disconnect().await;
        server.abort();
    }
    #[tokio::test]
    async fn periodic_snapshots_preserve_continuity_but_reinitializations_emit_a_cut() {
        let (mut session, mut events, server) = harness().await;
        let key = topic(BulletDataKind::OrderBook).key();
        session.handle_command(BulletCommand::Subscribe(topic(BulletDataKind::OrderBook)));
        let mut wire: Value =
            serde_json::from_slice(include_bytes!("../../test_data/ws_depth_snapshot.json"))
                .unwrap();
        for (u, pu) in [(1000, 0), (1001, 1000), (1001, 1000), (1003, 999), (1, 0)] {
            wire["U"] = json!(u);
            wire["u"] = json!(u);
            wire["pu"] = json!(pu);
            session
                .publish(&key, fixture(wire.to_string().as_bytes()), (u + 10).into())
                .unwrap();
        }
        let mut sequences = Vec::new();
        while let Ok(DataEvent::Data(Data::BookDeltas(frame))) = events.try_recv() {
            sequences.push(frame.sequence);
        }
        assert_eq!(sequences, [1000, 1001, 1001, 0, 1003, 0, 1]);
        session.client.disconnect().await;
        server.abort();
    }

    #[tokio::test]
    async fn snapshot_replaces_sequence_and_empty_updates_advance_wire_continuity() {
        let (mut session, mut events, server) = harness().await;
        let book = topic(BulletDataKind::OrderBook);
        let key = book.key();
        session.handle_command(BulletCommand::Subscribe(book));
        session.reconcile_and_write().await;
        let id = match session.topics[&key].phase {
            Phase::Pending { id, .. } => id,
            _ => panic!(),
        };
        session
            .acknowledge(Some(id), "success".into(), Operation::Subscribe)
            .unwrap();
        let mut v: Value =
            serde_json::from_slice(include_bytes!("../../test_data/ws_depth_snapshot.json"))
                .unwrap();
        session
            .publish(&key, fixture(v.to_string().as_bytes()), UnixNanos::from(1))
            .unwrap();
        v["mt"] = json!("u");
        v["pu"] = json!(1000);
        v["U"] = json!(1001);
        v["u"] = json!(1001);
        v["b"] = json!([]);
        v["a"] = json!([]);
        session
            .publish(&key, fixture(v.to_string().as_bytes()), UnixNanos::from(2))
            .unwrap();
        assert_eq!(session.topics[&key].last_u, Some(1001));
        v["pu"] = json!(999);
        v["U"] = json!(1002);
        v["u"] = json!(1002);
        assert!(
            session
                .publish(&key, fixture(v.to_string().as_bytes()), UnixNanos::from(3))
                .is_err()
        );
        assert_eq!(session.topics[&key].last_u, Some(1001));
        v["mt"] = json!("s");
        v["U"] = json!(1);
        v["u"] = json!(1);
        session
            .publish(&key, fixture(v.to_string().as_bytes()), UnixNanos::from(4))
            .unwrap();
        let mut seq = Vec::new();
        while let Ok(DataEvent::Data(Data::BookDeltas(book))) = events.try_recv() {
            seq.push(book.deltas[0].sequence);
        }
        assert_eq!(seq, vec![1000, 0, 1]);
        session.client.disconnect().await;
        server.abort();
    }
    #[tokio::test]
    async fn native_transport_forced_reconnect_restores_snapshot_and_dedupes_replayed_trade() {
        let registry = nautilus_live::SocketReconnectRegistry::default();
        let client_id = ClientId::from("BULLET");
        let endpoint = Ustr::from("test");
        let (mut session, mut events, server) = harness_with_control(
            SocketControlFactory::with_registry(
                client_id,
                Some(*crate::common::BULLET_VENUE),
                &registry,
            )
            .control(endpoint.as_str()),
        )
        .await;
        let (tx, rx) = mpsc::channel(16);
        session.command_rx = rx;
        let cancellation = session.cancellation.clone();
        let reconnect = registry
            .handle(client_id, endpoint)
            .expect("session must retain Native controller registration after connect");
        let task = tokio::spawn(session.run());
        tx.send(BulletCommand::Subscribe(topic(BulletDataKind::Trades)))
            .await
            .unwrap();
        tx.send(BulletCommand::Subscribe(topic(BulletDataKind::OrderBook)))
            .await
            .unwrap();
        let mut trades = 0;
        let mut next_book = async || {
            loop {
                match tokio::time::timeout(Duration::from_secs(8), events.recv())
                    .await
                    .unwrap()
                    .unwrap()
                {
                    DataEvent::Data(Data::BookDeltas(book)) => return book,
                    DataEvent::Data(Data::Trade(_)) => trades += 1,
                    _ => {}
                }
            }
        };
        let first = next_book().await;
        assert_eq!(first.deltas[0].sequence, 42_550_033);
        let mut managed = nautilus_model::orderbook::OrderBook::new(
            first.instrument_id,
            nautilus_model::enums::BookType::L2_MBP,
        );
        managed.apply_deltas(&first).unwrap();
        let outcome = reconnect.request_reconnect();
        assert!(!matches!(
            outcome,
            ReconnectRequestOutcome::Closed | ReconnectRequestOutcome::Unsupported
        ));
        let invalidation = next_book().await;
        assert_eq!(invalidation.deltas.len(), 1);
        assert_eq!(invalidation.deltas[0].flags, RecordFlag::F_LAST as u8);
        managed.apply_deltas(&invalidation).unwrap();
        assert_eq!(managed.sequence, 0);
        assert_eq!(managed.bids(None).count(), 0);
        let second = next_book().await;
        assert_eq!(second.deltas[0].sequence, 2_744_357);
        managed.apply_deltas(&second).unwrap();
        assert_eq!(managed.sequence, 2_744_357);
        assert_eq!(trades, 1);
        assert_eq!(first.deltas[1].order, second.deltas[1].order);
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
        assert!(
            registry.handle(client_id, endpoint).is_none(),
            "session teardown must deregister the endpoint"
        );
        server.abort();
    }
    #[tokio::test]
    async fn transport_loss_invalidates_before_reconnect_and_rejects_queued_old_frames() {
        let (mut session, mut events, server) = harness().await;
        let topic = topic(BulletDataKind::OrderBook);
        let key = topic.key();
        session.handle_command(BulletCommand::Subscribe(topic));
        session.reconcile_and_write().await;
        let Phase::Pending { id, .. } = session.topics[&key].phase else {
            panic!()
        };
        session
            .acknowledge(Some(id), "success".into(), Operation::Subscribe)
            .unwrap();
        let frame = include_str!("../../test_data/ws_depth_snapshot.json");
        session
            .publish(&key, fixture(frame.as_bytes()), 1.into())
            .unwrap();
        assert!(matches!(
            events.try_recv().unwrap(),
            DataEvent::Data(Data::BookDeltas(_))
        ));
        assert_eq!(
            session.reconnect.request_reconnect(),
            ReconnectRequestOutcome::Accepted
        );
        session
            .handle_message(session.epoch, Message::text(frame), frame.len())
            .unwrap();
        let DataEvent::Data(Data::BookDeltas(cut)) = events.try_recv().unwrap() else {
            panic!()
        };
        assert_eq!(cut.deltas[0].sequence, 0);
        assert_eq!(cut.deltas[0].flags, RecordFlag::F_LAST as u8);
        assert!(session.failed);
        session
            .handle_message(session.epoch, Message::text(frame), frame.len())
            .unwrap();
        session.observe_transport_loss().unwrap();
        assert!(
            events.try_recv().is_err(),
            "loss must emit one cut and no stale source publication"
        );
        session.client.disconnect().await;
        server.abort();
    }
    #[tokio::test]
    async fn older_chain_state_is_not_promoted_after_invalidation() {
        let (mut session, mut events, server) = harness().await;
        let topic = topic(BulletDataKind::OrderBook);
        let key = topic.key();
        session.handle_command(BulletCommand::Subscribe(topic));
        session.reconcile_and_write().await;
        let Phase::Pending { id, .. } = session.topics[&key].phase else {
            panic!()
        };
        session
            .acknowledge(Some(id), "success".into(), Operation::Subscribe)
            .unwrap();
        let mut v: Value =
            serde_json::from_slice(include_bytes!("../../test_data/ws_depth_snapshot.json"))
                .unwrap();
        session
            .publish(&key, fixture(v.to_string().as_bytes()), 1.into())
            .unwrap();
        session.invalidate_books().unwrap();
        while events.try_recv().is_ok() {}
        v["T"] = json!(v["T"].as_u64().unwrap() - 1);
        assert!(
            session
                .publish(&key, fixture(v.to_string().as_bytes()), 2.into())
                .is_err()
        );
        assert!(events.try_recv().is_err());
        v["T"] = json!(v["T"].as_u64().unwrap() + 2);
        v["U"] = json!(1);
        v["u"] = json!(1);
        session
            .publish(&key, fixture(v.to_string().as_bytes()), 3.into())
            .unwrap();
        assert!(session.topics[&key].book_ready);
        session.client.disconnect().await;
        server.abort();
    }
    #[tokio::test]
    async fn overflow_marker_is_empty_at_epoch_zero_and_timeout_closes_generation() {
        let (mut session, mut events, server) = harness().await;
        assert_eq!(session.overflow_epoch.load(Ordering::Acquire), 0);
        session.check_health();
        assert!(!session.failed);
        let book = topic(BulletDataKind::OrderBook);
        let key = book.key();
        session.handle_command(BulletCommand::Subscribe(book));
        session.reconcile_and_write().await;
        if let Phase::Pending {
            ref mut deadline, ..
        } = session.topics.get_mut(&key).unwrap().phase
        {
            *deadline = tokio::time::Instant::now();
        }
        session.check_health();
        assert!(session.failed);
        assert!(session.topics[&key].last_u.is_none());
        assert!(events.try_recv().is_err());
        session.client.disconnect().await;
        server.abort();
    }

    #[tokio::test]
    async fn pre_ack_data_is_gated_and_global_buffer_overflow_fails_closed() {
        let (mut session, mut events, server) = harness().await;
        let book = topic(BulletDataKind::OrderBook);
        let key = book.key();
        session.handle_command(BulletCommand::Subscribe(book));
        session.reconcile_and_write().await;
        let id = match session.topics[&key].phase {
            Phase::Pending { id, .. } => id,
            _ => panic!(),
        };
        let frame = include_str!("../../test_data/ws_depth_snapshot.json");
        session
            .handle_message(session.epoch, Message::text(frame), frame.len())
            .unwrap();
        assert!(events.try_recv().is_err());
        assert_eq!(session.topics[&key].staged.len(), 1);
        assert!(
            session
                .handle_message(session.epoch, Message::text(frame), RAW_BYTES)
                .is_err()
        );
        assert!(events.try_recv().is_err());
        session
            .acknowledge(Some(id), "success".to_string(), Operation::Subscribe)
            .unwrap();
        assert!(matches!(
            events.try_recv().unwrap(),
            DataEvent::Data(Data::BookDeltas(_))
        ));
        assert!(session.topics[&key].staged.is_empty());
        session
            .overflow_epoch
            .store(session.epoch + 1, Ordering::Release);
        session.check_health();
        assert!(session.failed);
        assert!(session.topics[&key].last_u.is_none());
        session.client.disconnect().await;
        server.abort();
    }

    #[tokio::test]
    #[ignore = "Requires Bullet mainnet public HTTP and WebSocket access"]
    async fn live_btc_forced_reconnect_smoke() {
        let config = BulletDataClientConfig::default();
        let clock = get_atomic_clock_realtime();
        let provider = crate::config::BulletInstrumentProviderConfig {
            load_all: false,
            load_ids: Some(vec![
                crate::common::instrument_id_from_market("BTC-USD").unwrap(),
            ]),
        };
        let instruments = crate::http::BulletHttpClient::new(&config)
            .unwrap()
            .request_instruments(&provider, clock.get_time_ns())
            .await
            .unwrap();
        let cache = Arc::new(AtomicMap::new());
        cache.store(
            instruments
                .into_iter()
                .map(|i| (i.raw_symbol().inner(), i))
                .collect(),
        );
        let (sender, mut events) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let (commands, session) = BulletWebSocketSession::connect(
            &config,
            cache,
            sender.into(),
            clock,
            cancel.clone(),
            SocketControlFactory::new(ClientId::from("BULLET"), Some(*crate::common::BULLET_VENUE))
                .control("live-probe"),
        )
        .await
        .unwrap();
        let reconnect = session.reconnect.clone();
        let probe_client = session.client.clone();
        let initial_epoch = session.epoch;
        let task = tokio::spawn(session.run());
        for kind in [
            BulletDataKind::OrderBook,
            BulletDataKind::Trades,
            BulletDataKind::MarkPrice,
            BulletDataKind::IndexPrice,
        ] {
            commands
                .send(BulletCommand::Subscribe(topic(kind)))
                .await
                .unwrap();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
        let mut snapshots = 0;
        let mut previous = 0;
        let mut book_batches = 0;
        let mut mark = 0;
        let mut index = 0;
        let mut trade_ids = AHashSet::new();
        let mut requested = false;
        let mut snapshots_before_request = 0;
        loop {
            let event = tokio::time::timeout_at(deadline, events.recv())
                .await
                .expect("live probe timed out")
                .unwrap();
            match event {
                DataEvent::Data(Data::BookDeltas(batch)) => {
                    if !RecordFlag::F_SNAPSHOT.matches(batch.flags)
                        && batch.deltas.len() == 1
                        && batch.deltas[0].action == nautilus_model::enums::BookAction::Clear
                    {
                        continue;
                    }
                    book_batches += 1;
                    let sequence = batch.deltas[0].sequence;
                    if batch.deltas[0].action != nautilus_model::enums::BookAction::Clear {
                        assert!(sequence > previous);
                    }
                    previous = sequence;
                    if batch.deltas[0].action == nautilus_model::enums::BookAction::Clear {
                        snapshots += 1;
                    }
                }
                DataEvent::Data(Data::Trade(trade)) => {
                    assert!(
                        trade_ids.insert(trade.trade_id.to_string()),
                        "duplicate canonical trade ID"
                    );
                }
                DataEvent::Data(Data::MarkPrice(_)) => mark += 1,
                DataEvent::Data(Data::IndexPrice(_)) => index += 1,
                _ => {}
            }
            if !requested && snapshots > 0 && mark > 0 && index > 0 && !trade_ids.is_empty() {
                assert!(!matches!(
                    reconnect.request_reconnect(),
                    ReconnectRequestOutcome::Closed | ReconnectRequestOutcome::Unsupported
                ));
                snapshots_before_request = snapshots;
                requested = true;
                // No book can publish while the physical epoch is being replaced.
                // A fresh CLEAR batch is required by the session before any new-epoch delta.
            }
            if requested
                && snapshots > snapshots_before_request
                && probe_client.connection_epoch() > initial_epoch
                && mark >= 2
                && index >= 2
            {
                break;
            }
        }
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
        println!(
            "{}",
            json!({"forced_reconnect":requested,"snapshots":snapshots,"book_batches":book_batches,"unique_trades":trade_ids.len(),"mark_updates":mark,"index_updates":index,"last_venue_book_sequence":previous})
        );
    }
}
