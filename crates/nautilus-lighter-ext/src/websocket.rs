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

//! Binary-only session using Nautilus transport epochs and upstream Lighter parsers.
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ahash::{AHashMap, AHashSet};
use anyhow::Context;
use nautilus_common::{live::sender::EventSender, messages::DataEvent};
use nautilus_core::{AtomicMap, UnixNanos, time::AtomicTime};
use nautilus_lighter::{
    common::{
        consts::{HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT},
        enums::LighterTradeType,
    },
    websocket::{
        messages::{LighterWsFrame, LighterWsOrderBook},
        parse::{parse_ws_order_book_deltas, parse_ws_trade_tick},
    },
};
use nautilus_live::SocketControl;
use nautilus_model::{
    data::{Data, IndexPriceUpdate},
    instruments::Instrument,
};
use nautilus_network::{
    RECONNECTED,
    error::SendError,
    http::create_standard_nautilus_headers,
    mode::ReconnectRequestOutcome,
    transport::Message,
    websocket::{EpochMessageHandler, WebSocketClient, WebSocketConfig, WebSocketReconnectHandle},
};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    common::oracle_price,
    config::LighterBinaryDataClientConfig,
    data_types::LighterOpenInterest,
    models::{BinaryStats, MarketContext, OraclePoint},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Kind {
    Book,
    Trades,
    OpenInterest,
    Oracle,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Topic {
    pub market: u64,
    pub kind: Kind,
}
#[derive(Debug)]
pub(crate) enum Command {
    Subscribe(Topic),
    Unsubscribe(Topic),
    Definition { market: u64, subscribe: bool },
}

#[derive(Debug, Default)]
struct TradeIds {
    ids: AHashSet<String>,
    order: VecDeque<String>,
}
impl TradeIds {
    fn accept(&mut self, id: String) -> bool {
        if !self.ids.insert(id.clone()) {
            return false;
        }
        self.order.push_back(id);
        if self.order.len() > 10_000
            && let Some(old) = self.order.pop_front()
        {
            self.ids.remove(&old);
        }
        true
    }
}
#[derive(Debug)]
struct Book {
    nonce: i64,
    ready: bool,
}

enum ControlMessage {
    Pong(Vec<u8>),
    Heartbeat,
}

type SubscriptionSend =
    Pin<Box<dyn Future<Output = (u64, &'static str, String, Result<(), SendError>)> + Send>>;
type ControlSend = Pin<Box<dyn Future<Output = (u64, Result<(), SendError>)> + Send>>;

pub(crate) struct BinarySession {
    client: Arc<WebSocketClient>,
    pending_writes: VecDeque<(&'static str, String)>,
    subscription_send: Option<SubscriptionSend>,
    control_send: Option<ControlSend>,
    reconnect: WebSocketReconnectHandle,
    raw: mpsc::Receiver<(u64, Message)>,
    commands: mpsc::Receiver<Command>,
    overflow: Arc<AtomicBool>,
    contexts: Arc<AtomicMap<u64, MarketContext>>,
    sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
    cancel: CancellationToken,
    epoch: u64,
    failed: Option<u64>,
    owners: AHashMap<Topic, usize>,
    definition_owners: AHashMap<u64, usize>,
    deadlines: AHashMap<String, tokio::time::Instant>,
    books: AHashMap<u64, Book>,
    trades: AHashMap<u64, TradeIds>,
    oracle_seen: AHashMap<u64, (u64, nautilus_model::types::Price)>,
}
impl std::fmt::Debug for BinarySession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinarySession")
            .field("epoch", &self.epoch)
            .field("owners", &self.owners)
            .finish_non_exhaustive()
    }
}

impl BinarySession {
    pub async fn connect(
        config: &LighterBinaryDataClientConfig,
        contexts: Arc<AtomicMap<u64, MarketContext>>,
        sender: EventSender<DataEvent>,
        clock: &'static AtomicTime,
        cancel: CancellationToken,
        control: SocketControl,
    ) -> anyhow::Result<(mpsc::Sender<Command>, Self)> {
        let (tx, raw) = mpsc::channel(1024);
        let overflow = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&overflow);
        let handler: EpochMessageHandler = Arc::new(move |epoch, message| {
            if matches!(
                tx.try_send((epoch, message)),
                Err(mpsc::error::TrySendError::Full(_))
            ) {
                flag.store(true, Ordering::Release);
            }
        });
        let client = WebSocketClient::epoch_builder()
            .config(WebSocketConfig {
                url: config.ws_url(),
                headers: create_standard_nautilus_headers(),
                heartbeat_interval_secs: Some(HEARTBEAT_INTERVAL.as_secs()),
                heartbeat_payload: None,
                connect_timeout_ms: Some(config.ws_timeout_secs.saturating_mul(1000).max(1)),
                reconnect_delay_initial_ms: Some(500),
                reconnect_delay_max_ms: Some(10_000),
                reconnect_backoff_factor: Some(2.0),
                reconnect_jitter_ms: Some(250),
                reconnect_max_attempts: None,
                heartbeat_timeout_secs: Some(HEARTBEAT_TIMEOUT.as_secs()),
                idle_timeout_ms: None,
                writer_capacity: Some(256),
                backend: config.transport_backend,
                proxy_url: config.proxy_url.clone(),
                max_message_size_bytes: Some(4 * 1024 * 1024),
                max_frame_size_bytes: Some(4 * 1024 * 1024),
            })
            .epoch_handler(handler)
            .rate_limiter(
                nautilus_lighter::common::rate_limit::ws_message_rate_limiter(&config.ws_url()),
            )
            .state_sink(control.sink())
            .cancellation_token(cancel.clone())
            .connect()
            .await?;
        let reconnect = client.reconnect_handle();
        let registered = reconnect.clone();
        control.register(move || registered.request_reconnect());
        let epoch = client.connection_epoch();
        let (tx, commands) = mpsc::channel(256);
        Ok((
            tx,
            Self {
                client: Arc::new(client),
                pending_writes: VecDeque::new(),
                subscription_send: None,
                control_send: None,
                reconnect,
                raw,
                commands,
                overflow,
                contexts,
                sender,
                clock,
                cancel,
                epoch,
                failed: None,
                owners: AHashMap::new(),
                definition_owners: AHashMap::new(),
                deadlines: AHashMap::new(),
                books: AHashMap::new(),
                trades: AHashMap::new(),
                oracle_seen: AHashMap::new(),
            },
        ))
    }
    pub async fn run(mut self) {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            self.start_subscription_send();
            let send_idle = self.subscription_send.is_none() && self.pending_writes.is_empty();
            tokio::select! {
                biased;
                () = self.cancel.cancelled() => break,
                (epoch, operation, channel, result) = async {
                    match &mut self.subscription_send {
                        Some(send) => send.await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.subscription_send = None;
                    if epoch == self.epoch && epoch == self.client.connection_epoch() && self.failed.is_none() {
                        if let Err(error) = result {
                            self.fail(&error.into());
                        } else if operation == "subscribe" && self.wire_owned(&channel) {
                            self.deadlines.insert(channel, tokio::time::Instant::now() + Duration::from_secs(10));
                        }
                    }
                }
                (epoch, result) = async {
                    match &mut self.control_send {
                        Some(send) => send.await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.control_send = None;
                    if epoch == self.epoch && epoch == self.client.connection_epoch() && let Err(error) = result { self.fail(&error.into()); }
                }
                _ = tick.tick() => {
                    if self.overflow.swap(false, Ordering::AcqRel) {
                        self.fail(&anyhow::anyhow!("raw input queue overflow"));
                    }
                    if self.deadlines.values().any(|deadline| *deadline <= tokio::time::Instant::now()) {
                        self.fail(&anyhow::anyhow!("subscription snapshot timeout"));
                    }
                }
                message = self.raw.recv() => {
                    let Some((epoch, message)) = message else { break };
                    if self.overflow.swap(false, Ordering::AcqRel) {
                        self.fail(&anyhow::anyhow!("raw input queue overflow"));
                    }
                    if let Err(error) = self.message(epoch, message).await {
                        self.fail(&error);
                    }
                }
                command = self.commands.recv(), if send_idle => {
                    let Some(command) = command else { break };
                    if let Err(error) = self.command(command).await {
                        self.fail(&error);
                    }
                }
            }
        }
        self.clear_writes();
        self.client.disconnect().await;
    }
    fn fail(&mut self, error: &anyhow::Error) {
        if self.failed == Some(self.epoch) {
            return;
        }
        log::error!("Lighter binary stream invalidated: {error:#}");
        self.failed = Some(self.epoch);
        self.clear_writes();
        self.books.clear();
        self.deadlines.clear();
        let outcome = self.reconnect.request_reconnect();
        if matches!(
            outcome,
            ReconnectRequestOutcome::Closed | ReconnectRequestOutcome::Unsupported
        ) {
            self.cancel.cancel();
        }
    }
    fn channel(&self, topic: Topic) -> anyhow::Result<String> {
        let context = self
            .contexts
            .get_cloned(&topic.market)
            .context("unloaded Lighter binary market")?;
        Ok(match topic.kind {
            Kind::Book => format!("order_book/{}", topic.market),
            Kind::Trades => format!("trade/{}", topic.market),
            Kind::OpenInterest => format!("bo_market_stats/{}", topic.market),
            Kind::Oracle => context.source.channel(),
        })
    }
    fn wire_owned(&self, channel: &str) -> bool {
        self.owners
            .keys()
            .any(|topic| self.channel(*topic).is_ok_and(|key| key == channel))
    }
    fn queue_subscription(&mut self, operation: &'static str, channel: &str) {
        if self.failed.is_none() {
            self.pending_writes
                .push_back((operation, channel.to_owned()));
        }
    }

    fn start_subscription_send(&mut self) {
        if self.subscription_send.is_some() || self.client.connection_epoch() != self.epoch {
            return;
        }
        let Some((operation, channel)) = self.pending_writes.pop_front() else {
            return;
        };
        let client = Arc::clone(&self.client);
        let epoch = self.epoch;
        self.subscription_send = Some(Box::pin(async move {
            let result = client
                .send_text_on_connection(
                    json!({"type":operation,"channel":channel}).to_string(),
                    Some(
                        nautilus_lighter::common::rate_limit::LIGHTER_WS_MESSAGE_RATE_LIMIT_KEY
                            .as_slice(),
                    ),
                    epoch,
                )
                .await;
            (epoch, operation, channel, result)
        }));
    }

    fn queue_control(&mut self, message: ControlMessage) {
        if self.control_send.is_some() {
            self.fail(&anyhow::anyhow!("heartbeat send is still pending"));
            return;
        }
        let client = Arc::clone(&self.client);
        let epoch = self.epoch;
        self.control_send = Some(Box::pin(async move {
            let result = match message {
                ControlMessage::Pong(data) => client.send_pong_on_connection(data, epoch).await,
                ControlMessage::Heartbeat => {
                    client
                        .send_text_on_connection(json!({"type":"pong"}).to_string(), None, epoch)
                        .await
                }
            };
            (epoch, result)
        }));
    }

    fn clear_writes(&mut self) {
        self.subscription_send = None;
        self.control_send = None;
        self.pending_writes.clear();
    }

    async fn command(&mut self, command: Command) -> anyhow::Result<()> {
        if let Command::Definition { market, subscribe } = command {
            if subscribe {
                *self.definition_owners.entry(market).or_default() += 1;
            } else if let Some(count) = self.definition_owners.get_mut(&market) {
                *count -= 1;
                if *count == 0 {
                    self.definition_owners.remove(&market);
                }
            }
            self.prune_market(market);
            return Ok(());
        }
        let topic = match command {
            Command::Subscribe(t) | Command::Unsubscribe(t) => t,
            Command::Definition { .. } => unreachable!("handled above"),
        };
        if matches!(command, Command::Unsubscribe(_))
            && !self.contexts.contains_key(&topic.market)
            && !self.owners.contains_key(&topic)
        {
            return Ok(());
        }
        let channel = self.channel(topic)?;
        let was_owned = self.wire_owned(&channel);
        match command {
            Command::Subscribe(_) => {
                *self.owners.entry(topic).or_default() += 1;
                if !was_owned && self.failed.is_none() {
                    self.queue_subscription("subscribe", &channel);
                }
            }
            Command::Unsubscribe(_) => {
                if let Some(count) = self.owners.get_mut(&topic) {
                    *count -= 1;
                    if *count == 0 {
                        self.owners.remove(&topic);
                        if topic.kind == Kind::Book {
                            self.books.remove(&topic.market);
                        }
                        if topic.kind == Kind::Oracle {
                            self.oracle_seen.remove(&topic.market);
                        }
                    }
                }
                if was_owned && !self.wire_owned(&channel) {
                    self.deadlines.remove(&channel);
                    if self.failed.is_none() {
                        self.queue_subscription("unsubscribe", &channel);
                    }
                }
            }
            Command::Definition { .. } => unreachable!("handled above"),
        }
        self.prune_market(topic.market);
        Ok(())
    }
    fn prune_market(&mut self, market: u64) {
        if self.owners.keys().any(|topic| topic.market == market)
            || self.definition_owners.contains_key(&market)
        {
            return;
        }
        if self.contexts.get_cloned(&market).is_some_and(|c| {
            c.instrument
                .expiration_ns()
                .is_some_and(|end| end <= self.clock.get_time_ns())
        }) {
            self.contexts.remove(&market);
            self.books.remove(&market);
            self.trades.remove(&market);
            self.oracle_seen.remove(&market);
            log::info!(
                "Lighter binary context retired: market_id={market}, contexts={}, books={}, trade_maps={}, oracle_maps={}",
                self.contexts.len(),
                self.books.len(),
                self.trades.len(),
                self.oracle_seen.len()
            );
        }
    }
    async fn message(&mut self, epoch: u64, message: Message) -> anyhow::Result<()> {
        if let Message::Ping(data) = &message {
            if epoch == self.epoch
                && epoch == self.client.connection_epoch()
                && self.failed != Some(epoch)
            {
                self.queue_control(ControlMessage::Pong(data.to_vec()));
            }
            return Ok(());
        }
        let Some(text) = message.as_text() else {
            return Ok(());
        };
        if text == RECONNECTED {
            if epoch <= self.epoch {
                return Ok(());
            }
            self.clear_writes();
            self.epoch = epoch;
            self.failed = None;
            self.books.clear();
            self.deadlines.clear();
            let channels = self
                .owners
                .keys()
                .map(|t| self.channel(*t))
                .collect::<anyhow::Result<AHashSet<_>>>()?;
            for channel in channels {
                self.queue_subscription("subscribe", &channel);
            }
            return Ok(());
        }
        if epoch != self.epoch
            || epoch != self.client.connection_epoch()
            || self.failed == Some(epoch)
        {
            return Ok(());
        }
        let value: Value = serde_json::from_str(text)?;
        let ty = value["type"]
            .as_str()
            .context("Lighter frame missing type")?;
        if ty == "ping" {
            self.queue_control(ControlMessage::Heartbeat);
            return Ok(());
        }
        if ty == "error" || value.get("error").is_some() {
            anyhow::bail!("Lighter stream error {value}");
        }
        if ty == "connected" || ty.starts_with("unsubscribed/") || ty == "pong" {
            return Ok(());
        }
        let channel = value["channel"]
            .as_str()
            .context("Lighter frame missing channel")?
            .replace(':', "/");
        if !self.wire_owned(&channel) {
            return Ok(());
        }
        let snapshot = ty.starts_with("subscribed/");
        anyhow::ensure!(
            snapshot || ty.starts_with("update/"),
            "unexpected Lighter frame type {ty}"
        );
        let expected = ty
            .split_once('/')
            .context("invalid Lighter message type")?
            .1;
        anyhow::ensure!(
            channel.split('/').next() == Some(expected),
            "Lighter type/channel mismatch"
        );
        let ts_init = self.clock.get_time_ns();
        if channel.starts_with("up_down_candle/") {
            let points: Vec<OraclePoint> = serde_json::from_value(value["candles"].clone())?;
            let markets: Vec<_> = self
                .owners
                .keys()
                .filter(|t| t.kind == Kind::Oracle && self.channel(**t).is_ok_and(|c| c == channel))
                .map(|t| t.market)
                .collect();
            for point in points {
                for market in &markets {
                    let context = self
                        .contexts
                        .get_cloned(market)
                        .context("oracle context removed")?;
                    anyhow::ensure!(
                        context.source.validates(&point),
                        "oracle provenance mismatch"
                    );
                    let price = oracle_price(point.close)?;
                    if let Some((timestamp, old)) = self.oracle_seen.get(market)
                        && (point.timestamp < *timestamp
                            || (point.timestamp == *timestamp && price == *old))
                    {
                        continue;
                    }
                    let update = IndexPriceUpdate::new(
                        context.instrument.id(),
                        price,
                        UnixNanos::from(
                            point
                                .timestamp
                                .checked_mul(1_000_000_000)
                                .context("oracle timestamp overflow")?,
                        ),
                        ts_init,
                    );
                    self.sender
                        .send(DataEvent::Data(Data::IndexPrice(update)))?;
                    self.oracle_seen.insert(*market, (point.timestamp, price));
                }
            }
        } else {
            let market: u64 = channel
                .split('/')
                .nth(1)
                .context("missing market ID")?
                .parse()?;
            let context = self
                .contexts
                .get_cloned(&market)
                .context("unknown market ID")?;
            match expected {
                "order_book" => {
                    let book: LighterWsOrderBook =
                        serde_json::from_value(value["order_book"].clone())?;
                    anyhow::ensure!(book.code == 0, "Lighter book error {}", book.code);
                    if !snapshot {
                        let state = self
                            .books
                            .get(&market)
                            .context("book delta before snapshot")?;
                        anyhow::ensure!(
                            state.ready
                                && book.begin_nonce == state.nonce
                                && book.nonce >= book.begin_nonce,
                            "Lighter book nonce gap"
                        );
                    }
                    let timestamp = value["timestamp"]
                        .as_u64()
                        .context("book timestamp missing")?;
                    // Empty incrementals are keepalives, not fabricated CLEAR operations.
                    if snapshot || !book.bids.is_empty() || !book.asks.is_empty() {
                        let deltas = parse_ws_order_book_deltas(
                            &book,
                            &context.instrument,
                            timestamp,
                            snapshot,
                            ts_init,
                        )?;
                        self.sender
                            .send(DataEvent::Data(Data::BookDeltas(Box::new(deltas))))?;
                    }
                    self.books.insert(
                        market,
                        Book {
                            nonce: book.nonce,
                            ready: true,
                        },
                    );
                }
                "trade" => {
                    let frame: LighterWsFrame = serde_json::from_value(value.clone())?;
                    let (trades, liquidations) = match frame {
                        LighterWsFrame::TradeSnapshot {
                            trades,
                            liquidation_trades,
                            ..
                        }
                        | LighterWsFrame::Trade {
                            trades,
                            liquidation_trades,
                            ..
                        } => (trades, liquidation_trades),
                        _ => anyhow::bail!("unexpected upstream trade frame"),
                    };
                    for trade in trades.into_iter().chain(liquidations) {
                        anyhow::ensure!(
                            u64::try_from(trade.market_id)? == market,
                            "trade market mismatch"
                        );
                        if trade.trade_type == LighterTradeType::MarketSettlement {
                            continue;
                        }
                        let tick = parse_ws_trade_tick(&trade, &context.instrument, ts_init)?;
                        if self
                            .trades
                            .entry(market)
                            .or_default()
                            .accept(tick.trade_id.to_string())
                        {
                            self.sender.send(DataEvent::Data(Data::Trade(tick)))?;
                        }
                    }
                }
                "bo_market_stats" => {
                    let stats: BinaryStats =
                        serde_json::from_value(value["bo_market_stats"].clone())?;
                    anyhow::ensure!(stats.market_id == market, "OI market mismatch");
                    anyhow::ensure!(
                        [
                            "inactive",
                            "active",
                            "waiting-for-settlement",
                            "in-settlement",
                            "settled"
                        ]
                        .contains(&stats.status.as_str()),
                        "unknown binary status {}",
                        stats.status
                    );
                    if let Some(oi) = stats.open_interest {
                        anyhow::ensure!(oi >= rust_decimal::Decimal::ZERO, "negative OI");
                        let timestamp = value["timestamp"]
                            .as_u64()
                            .context("OI timestamp missing")?;
                        let data = LighterOpenInterest {
                            instrument_id: context.instrument.id(),
                            market_id: market,
                            open_interest: oi,
                            ts_event: UnixNanos::from(
                                timestamp
                                    .checked_mul(1_000_000)
                                    .context("OI timestamp overflow")?,
                            ),
                            ts_init,
                        };
                        self.sender
                            .send(DataEvent::Data(Data::Custom(data.into_custom_data())))?;
                    }
                }
                _ => anyhow::bail!("unexpected binary channel {channel}"),
            }
        }
        self.deadlines.remove(&channel);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::OracleSource;
    use axum::{
        Router,
        extract::{
            State,
            ws::{Message as AxumMessage, WebSocket, WebSocketUpgrade},
        },
        routing::get,
    };
    use futures_util::{SinkExt, StreamExt};
    use nautilus_live::SocketControlFactory;
    use nautilus_model::{
        enums::AssetClass,
        identifiers::{ClientId, Symbol, Venue},
        instruments::BinaryOption,
        types::{Currency, Price, Quantity},
    };
    use std::sync::atomic::AtomicUsize;

    #[derive(Clone)]
    struct Server {
        connections: Arc<AtomicUsize>,
        journal: mpsc::UnboundedSender<(usize, Value)>,
    }
    async fn socket(
        ws: WebSocketUpgrade,
        State(server): State<Server>,
    ) -> axum::response::Response {
        ws.on_upgrade(move |socket| serve(socket, server))
    }
    fn book_frame(snapshot: bool, nonce: i64, begin: i64, price: &str) -> Value {
        json!({"type":if snapshot{"subscribed/order_book"}else{"update/order_book"},"channel":"order_book:4255","timestamp":1792480927776_u64,
            "order_book":{"code":0,"asks":[],"bids":[{"price":price,"size":"10.0000"}],"offset":1,"nonce":nonce,"last_updated_at":1792480927728496_u64,"begin_nonce":begin}})
    }
    async fn serve(socket: WebSocket, server: Server) {
        let connection = server.connections.fetch_add(1, Ordering::AcqRel) + 1;
        let (mut sink, mut stream) = socket.split();
        while let Some(Ok(message)) = stream.next().await {
            let text = match message {
                AxumMessage::Text(text) => text,
                AxumMessage::Pong(data) => {
                    assert_eq!(data.as_ref(), b"lighter-heartbeat");
                    server
                        .journal
                        .send((connection, json!({"type":"control_pong"})))
                        .unwrap();
                    continue;
                }
                _ => continue,
            };
            let command: Value = serde_json::from_str(&text).unwrap();
            server.journal.send((connection, command.clone())).unwrap();
            if command["type"] != "subscribe" {
                continue;
            }
            let frames = match command["channel"].as_str().unwrap() {
                "order_book/4255" if connection == 1 => vec![
                    book_frame(true, 10, 0, "0.39"),
                    book_frame(false, 20, 10, "0.38"),
                    book_frame(false, 999, 777, "0.99"),
                    book_frame(false, 1000, 999, "0.98"),
                ],
                "order_book/4255" => vec![book_frame(true, 100, 0, "0.35")],
                "up_down_candle/eth/15m/1s" => vec![
                    json!({"type":"subscribed/up_down_candle","channel":"up_down_candle:eth:15m:1s","candles":[{"symbol":"eth","duration":"15m","resolution":"1s","timestamp":1792480925_u64,"close":"2392.641853126338412544","provider":"chainlink","feed_id":"feed"}]}),
                ],
                _ => vec![],
            };
            if command["channel"] == "up_down_candle/eth/15m/1s" {
                sink.send(AxumMessage::Ping(b"lighter-heartbeat".to_vec().into()))
                    .await
                    .unwrap();
            }
            for frame in frames {
                if sink
                    .send(AxumMessage::Text(frame.to_string().into()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    }
    fn context(market: u64) -> MarketContext {
        let id = crate::common::instrument_id(market).unwrap();
        let instrument = BinaryOption::builder()
            .instrument_id(id)
            .raw_symbol(Symbol::new(format!("ETH-{market}")))
            .asset_class(AssetClass::Alternative)
            .currency(Currency::USDC())
            .activation_ns(UnixNanos::from(1))
            .expiration_ns(UnixNanos::from(u64::MAX))
            .price_precision(2)
            .size_precision(4)
            .price_increment(Price::from("0.01"))
            .size_increment(Quantity::from("0.0001"))
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap();
        MarketContext {
            market_id: market,
            instrument: nautilus_model::instruments::InstrumentAny::BinaryOption(instrument),
            source: OracleSource {
                symbol: "eth".into(),
                duration: "15m".into(),
                provider: "chainlink".into(),
                feed_id: "feed".into(),
            },
            start_secs: 1,
        }
    }
    #[tokio::test]
    async fn shared_oracle_owners_gap_reconnect_and_epoch_fence() {
        let (journal, mut observed) = mpsc::unbounded_channel();
        let state = Server {
            connections: Arc::new(AtomicUsize::new(0)),
            journal,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = Router::new()
            .route("/stream", get(socket))
            .with_state(state.clone());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let config = LighterBinaryDataClientConfig {
            market_ids: vec![4255, 4256],
            base_url_ws: Some(format!("ws://{addr}/stream")),
            ..Default::default()
        };
        let contexts = Arc::new(AtomicMap::from(
            [4255, 4256]
                .into_iter()
                .map(|id| (id, context(id)))
                .collect::<AHashMap<_, _>>(),
        ));
        let (sender, mut received) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let control =
            SocketControlFactory::new(ClientId::new("LIGHTER"), Some(Venue::new("LIGHTER")))
                .control("test");
        let (tx, mut session) = BinarySession::connect(
            &config,
            contexts,
            sender.into(),
            nautilus_core::time::get_atomic_clock_realtime(),
            cancel.clone(),
            control,
        )
        .await
        .unwrap();
        let first = Topic {
            market: 4255,
            kind: Kind::Oracle,
        };
        let second = Topic {
            market: 4256,
            kind: Kind::Oracle,
        };
        session.command(Command::Subscribe(first)).await.unwrap();
        session.command(Command::Subscribe(first)).await.unwrap();
        session.command(Command::Subscribe(second)).await.unwrap();
        session.start_subscription_send();
        let (_, _, _, result) = session.subscription_send.take().unwrap().await;
        result.unwrap();
        // One physical subscription serves two instruments and repeated owners.
        let (_, request) = observed.recv().await.unwrap();
        assert_eq!(request["channel"], "up_down_candle/eth/15m/1s");
        assert!(observed.try_recv().is_err());
        session.command(Command::Unsubscribe(first)).await.unwrap();
        assert_eq!(session.owners[&first], 1);
        let task = tokio::spawn(session.run());
        let mut oracle_ids = AHashSet::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            while oracle_ids.len() < 2 {
                if let Some(DataEvent::Data(Data::IndexPrice(point))) = received.recv().await {
                    oracle_ids.insert(point.instrument_id);
                }
            }
        })
        .await
        .unwrap();
        tx.send(Command::Subscribe(Topic {
            market: 4255,
            kind: Kind::Book,
        }))
        .await
        .unwrap();
        let mut nonces = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(DataEvent::Data(Data::BookDeltas(book))) = received.recv().await {
                    nonces.push(book.sequence);
                    if book.sequence == 100 {
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            nonces,
            vec![10, 20, 100],
            "gap and old epoch updates must never reach consumers"
        );
        // Partial removal keeps the physical oracle; only the last owner sends unsubscribe.
        tx.send(Command::Unsubscribe(first)).await.unwrap();
        tx.send(Command::Unsubscribe(second)).await.unwrap();
        let mut requests = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let item = observed.recv().await.unwrap();
                let last = item.1["type"] == "unsubscribe"
                    && item.1["channel"] == "up_down_candle/eth/15m/1s";
                requests.push(item);
                if last {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|(_, v)| v["type"] == "unsubscribe"
                    && v["channel"] == "up_down_candle/eth/15m/1s")
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|(_, v)| v["type"] == "subscribe"
                    && v["channel"] == "up_down_candle/eth/15m/1s")
                .count(),
            1,
            "one oracle resubscription after reconnect"
        );
        assert!(
            requests
                .iter()
                .any(|(connection, value)| *connection == 1 && value["type"] == "control_pong"),
            "reply to server control Ping before reconnect"
        );
        cancel.cancel();
        task.await.unwrap();
        server.abort();
    }

    #[derive(Clone)]
    struct ResponsiveServer {
        frames: mpsc::UnboundedSender<mpsc::UnboundedSender<Value>>,
        observed: mpsc::UnboundedSender<Value>,
    }

    async fn responsive_socket(
        ws: WebSocketUpgrade,
        State(state): State<ResponsiveServer>,
    ) -> axum::response::Response {
        ws.on_upgrade(move |socket| async move {
            let (mut sink, mut stream) = socket.split();
            let (tx, mut frames) = mpsc::unbounded_channel::<Value>();
            state.frames.send(tx).unwrap();
            loop {
                tokio::select! {
                    frame = frames.recv() => {
                        let Some(frame) = frame else { break };
                        if sink.send(AxumMessage::Text(frame.to_string().into())).await.is_err() { break; }
                    }
                    message = stream.next() => {
                        let Some(Ok(message)) = message else { break };
                        if let AxumMessage::Text(text) = message {
                            state.observed.send(serde_json::from_str(&text).unwrap()).unwrap();
                        }
                    }
                }
            }
        })
    }

    #[tokio::test]
    async fn quota_wait_keeps_data_heartbeat_and_shutdown_responsive() {
        for backend in [
            nautilus_network::websocket::TransportBackend::default(),
            nautilus_network::websocket::TransportBackend::Tungstenite,
        ] {
            let (frame_tx, mut frames) = mpsc::unbounded_channel();
            let (observed_tx, mut observed) = mpsc::unbounded_channel();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let config = LighterBinaryDataClientConfig {
                base_url_ws: Some(format!(
                    "ws://{}/stream/{}",
                    listener.local_addr().unwrap(),
                    nautilus_core::UUID4::new()
                )),
                transport_backend: backend,
                ..Default::default()
            };
            let server = tokio::spawn(async move {
                axum::serve(
                    listener,
                    Router::new()
                        .route("/stream/{session}", get(responsive_socket))
                        .with_state(ResponsiveServer {
                            frames: frame_tx,
                            observed: observed_tx,
                        }),
                )
                .await
                .unwrap();
            });
            // Use the real Native shared limiter with an isolated URL and a slow test quota.
            let quota =
                nautilus_lighter::common::rate_limit::ws_message_rate_limiter(&config.ws_url());
            let key = &nautilus_lighter::common::rate_limit::LIGHTER_WS_MESSAGE_RATE_LIMIT_KEY[0];
            quota.add_quota_for_key(
                *key,
                nautilus_network::ratelimiter::quota::Quota::per_minute(
                    std::num::NonZeroU32::new(1).unwrap(),
                ),
            );
            let contexts = Arc::new(AtomicMap::from(AHashMap::from([(4255, context(4255))])));
            let (sender, mut events) = mpsc::unbounded_channel();
            let cancel = CancellationToken::new();
            let control =
                SocketControlFactory::new(ClientId::new("LIGHTER"), Some(Venue::new("LIGHTER")))
                    .control("responsive-test");
            let (tx, session) = BinarySession::connect(
                &config,
                contexts,
                sender.into(),
                nautilus_core::time::get_atomic_clock_realtime(),
                cancel.clone(),
                control,
            )
            .await
            .unwrap();
            let task = tokio::spawn(session.run());
            let frames = frames.recv().await.unwrap();
            tx.send(Command::Subscribe(Topic {
                market: 4255,
                kind: Kind::Oracle,
            }))
            .await
            .unwrap();
            let first = tokio::time::timeout(Duration::from_secs(2), observed.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(first["channel"], "up_down_candle/eth/15m/1s");
            assert!(
                quota.check_key(key).is_err(),
                "the first physical subscription consumes the quota"
            );
            tx.send(Command::Subscribe(Topic {
                market: 4255,
                kind: Kind::Book,
            }))
            .await
            .unwrap();
            // Causal barrier: the session has taken the command whose send must wait for quota.
            tokio::time::timeout(Duration::from_secs(2), async {
                while tx.capacity() != tx.max_capacity() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            // The bounded ingress remains full while the send lane is occupied.
            for _ in 0..tx.max_capacity() {
                tx.try_send(Command::Definition {
                    market: 4255,
                    subscribe: true,
                })
                .unwrap();
            }
            assert!(matches!(
                tx.try_send(Command::Definition {
                    market: 4255,
                    subscribe: true
                }),
                Err(mpsc::error::TrySendError::Full(_))
            ));
            frames.send(json!({"type":"ping"})).unwrap();
            frames.send(json!({"type":"update/up_down_candle","channel":"up_down_candle:eth:15m:1s","candles":[{"symbol":"eth","duration":"15m","resolution":"1s","timestamp":1792480925_u64,"close":"2392.64","provider":"chainlink","feed_id":"feed"}]})).unwrap();
            let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(
                matches!(event, DataEvent::Data(Data::IndexPrice(_))),
                "market data progresses before the throttled subscription write"
            );
            let reply = tokio::time::timeout(Duration::from_secs(2), observed.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                reply["type"], "pong",
                "heartbeat bypasses subscription quota wait"
            );
            assert_eq!(
                tx.capacity(),
                0,
                "pending writes cannot drain ingress into an unbounded queue"
            );
            cancel.cancel();
            tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
            server.abort();
        }
    }
}
