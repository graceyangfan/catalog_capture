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

//! Single-writer data session: commands, frames, discovery completions, and shutdown.

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ahash::AHashMap;
use anyhow::Context;
use nautilus_common::{live::sender::EventSender, messages::DataEvent};
use nautilus_core::time::AtomicTime;
use nautilus_live::task::TaskSpawner;
use nautilus_model::{
    data::{DataType, QuoteTick},
    identifiers::InstrumentId,
};
use nautilus_network::{
    error::SendError,
    mode::ReconnectRequestOutcome,
    transport::Message,
    websocket::{WebSocketClient, WebSocketReconnectHandle},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    discovery::{
        DiscoveryResult, DiscoveryScheduler, DynamicSubscription, MAX_DISCOVERY_REQUESTS,
        dynamic_selector_key,
    },
    instruments::PredictDynamicMarketRegistry,
};
use crate::{
    PredictCryptoUpDownSelector, PredictDataClientConfig, PredictHttpClient, PredictMarketContext,
    common::CryptoUpDownSelectorKey,
    websocket::{
        client::{COMMAND_QUEUE_CAPACITY, PredictWebSocketConnection},
        parse::subscription_message,
    },
};

#[derive(Debug)]
pub(super) enum PredictCommand {
    Subscribe(u64),
    Unsubscribe(u64),
    SubscribeQuotes(InstrumentId, PredictMarketContext),
    UnsubscribeQuotes(InstrumentId),
    SubscribeDepth(
        InstrumentId,
        PredictMarketContext,
        Option<std::num::NonZeroUsize>,
    ),
    UnsubscribeDepth(InstrumentId),
    SubscribeTrades(InstrumentId, PredictMarketContext),
    UnsubscribeTrades(InstrumentId),
    StartDynamic(PredictCryptoUpDownSelector, String),
    StopDynamic(CryptoUpDownSelectorKey, String),
}

#[derive(Debug)]
pub(super) struct QuoteSubscription {
    pub(super) context: PredictMarketContext,
    pub(super) last_quote: Option<QuoteTick>,
}

#[derive(Debug)]
pub(super) struct DepthSubscription {
    pub(super) context: PredictMarketContext,
    pub(super) depth: Option<std::num::NonZeroUsize>,
}

#[derive(Debug, Default)]
pub(super) struct BookSourceState {
    pub(super) last_timestamp: Option<nautilus_core::UnixNanos>,
    pub(super) snapshot: Option<crate::PredictOrderbookSnapshot>,
}

#[derive(Debug)]
pub(super) struct MarketSubscription {
    pub(super) owners: usize,
    // The identity is active only while custom owners remain
    pub(super) data_type: DataType,
}

#[derive(Debug)]
struct CustomDataConflict(u64);

impl std::fmt::Display for CustomDataConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "conflicting Predict custom data identities for market {}",
            self.0
        )
    }
}

impl std::error::Error for CustomDataConflict {}

type SubscriptionSend = Pin<Box<dyn Future<Output = (u64, Result<(), SendError>)> + Send>>;

pub(super) struct PredictDataSession {
    pub(super) client: Arc<WebSocketClient>,
    pub(super) pending_writes: VecDeque<String>,
    pub(super) subscription_send: Option<SubscriptionSend>,
    pub(super) control_send: Option<SubscriptionSend>,
    pub(super) http_client: PredictHttpClient,
    pub(super) reconnect: WebSocketReconnectHandle,
    pub(super) raw_rx: mpsc::Receiver<(u64, Message)>,
    pub(super) command_rx: mpsc::Receiver<PredictCommand>,
    pub(super) overflowed: std::sync::Arc<AtomicBool>,
    pub(super) subscriptions: AHashMap<u64, MarketSubscription>,
    pub(super) quotes: AHashMap<InstrumentId, QuoteSubscription>,
    pub(super) depths: AHashMap<InstrumentId, DepthSubscription>,
    pub(super) trades: AHashMap<InstrumentId, PredictMarketContext>,
    pub(super) books: AHashMap<u64, BookSourceState>,
    pub(super) contexts: AHashMap<u64, PredictMarketContext>,
    pub(super) dynamic_registry: Arc<RwLock<PredictDynamicMarketRegistry>>,
    pub(super) dynamic_generation: u64,
    pub(super) dynamic_subscriptions: AHashMap<CryptoUpDownSelectorKey, DynamicSubscription>,
    pub(super) data_sender: EventSender<DataEvent>,
    pub(super) clock: &'static AtomicTime,
    pub(super) cancellation: CancellationToken,
    pub(super) connected: Arc<AtomicBool>,
    pub(super) current_epoch: u64,
    pub(super) failed_epoch: Option<u64>,
    pub(super) next_request_id: u64,
    pub(super) dynamic_refresh_secs: u64,
    pub(super) discovery: DiscoveryScheduler,
    pub(super) discovery_tx: mpsc::Sender<DiscoveryResult>,
    pub(super) discovery_rx: mpsc::Receiver<DiscoveryResult>,
    pub(super) task_spawner: TaskSpawner,
    pub(super) discovery_timeout: Duration,
}

impl std::fmt::Debug for PredictDataSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PredictDataSession")
            .field("current_epoch", &self.current_epoch)
            .field("subscriptions", &self.subscriptions)
            .finish_non_exhaustive()
    }
}

pub(super) struct PredictSessionInputs {
    pub(super) http_client: PredictHttpClient,
    pub(super) contexts: AHashMap<u64, PredictMarketContext>,
    pub(super) dynamic_registry: Arc<RwLock<PredictDynamicMarketRegistry>>,
    pub(super) dynamic_generation: u64,
    pub(super) data_sender: EventSender<DataEvent>,
    pub(super) clock: &'static AtomicTime,
    pub(super) cancellation: CancellationToken,
    pub(super) connected: Arc<AtomicBool>,
    pub(super) task_spawner: TaskSpawner,
}

impl PredictDataSession {
    pub(super) async fn connect(
        config: &PredictDataClientConfig,
        inputs: PredictSessionInputs,
    ) -> anyhow::Result<(mpsc::Sender<PredictCommand>, Self)> {
        let PredictSessionInputs {
            http_client,
            contexts,
            dynamic_registry,
            dynamic_generation,
            data_sender,
            clock,
            cancellation,
            connected,
            task_spawner,
        } = inputs;
        let PredictWebSocketConnection {
            client,
            raw_rx,
            overflowed,
        } = PredictWebSocketConnection::connect(config, cancellation.clone()).await?;
        let (command_tx, command_rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
        let (discovery_tx, discovery_rx) = mpsc::channel(MAX_DISCOVERY_REQUESTS);
        let session = Self {
            current_epoch: client.connection_epoch(),
            reconnect: client.reconnect_handle(),
            client: Arc::new(client),
            pending_writes: VecDeque::new(),
            subscription_send: None,
            control_send: None,
            http_client,
            raw_rx,
            command_rx,
            overflowed,
            subscriptions: AHashMap::new(),
            quotes: AHashMap::new(),
            depths: AHashMap::new(),
            trades: AHashMap::new(),
            books: AHashMap::new(),
            contexts,
            dynamic_registry,
            dynamic_generation,
            dynamic_subscriptions: AHashMap::new(),
            data_sender,
            clock,
            cancellation,
            connected,
            failed_epoch: None,
            next_request_id: 1,
            dynamic_refresh_secs: config.dynamic_refresh_secs.max(1),
            discovery: DiscoveryScheduler::default(),
            discovery_tx,
            discovery_rx,
            task_spawner,
            discovery_timeout: Duration::from_secs(config.http_timeout_secs.max(1)),
        };
        Ok((command_tx, session))
    }

    pub(super) async fn run(mut self) {
        let mut health_tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            self.start_subscription_send();
            let send_idle = self.subscription_send.is_none() && self.pending_writes.is_empty();
            let deadline = self
                .dynamic_subscriptions
                .iter()
                .filter(|(key, _)| !self.discovery.inflight.contains_key(*key))
                .map(|(_, state)| state.next_refresh_ns)
                .min();
            let can_discover =
                deadline.is_some() && self.discovery.inflight.len() < MAX_DISCOVERY_REQUESTS;
            let delay = Duration::from_nanos(
                deadline
                    .unwrap_or(u64::MAX)
                    .saturating_sub(self.clock.get_time_ns().as_u64()),
            );
            tokio::select! {
                biased;
                () = self.cancellation.cancelled() => break,
                (epoch, result) = async {
                    match &mut self.subscription_send {
                        Some(send) => send.await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.subscription_send = None;
                    self.finish_write(epoch, result, "subscription");
                }
                (epoch, result) = async {
                    match &mut self.control_send {
                        Some(send) => send.await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.control_send = None;
                    self.finish_write(epoch, result, "heartbeat");
                }
                _ = health_tick.tick() => self.check_health(),
                _ = tokio::time::sleep(delay), if can_discover => {
                    if let Err(error) = self.schedule_discovery() {
                        log::warn!("Predict discovery admission failed: {error:#}");
                        break;
                    }
                }
                Some(result) = self.discovery_rx.recv(), if send_idle => {
                    if let Err(error) = self.apply_discovery_result(result).await {
                        log::warn!("Predict discovery apply failed: {error:#}");
                    }
                }
                raw = self.raw_rx.recv() => {
                    let Some((epoch, message)) = raw else { break };
                    if let Err(error) = self.handle_message(epoch, message).await {
                        log::error!("Predict WebSocket message discarded: {error:#}");
                    }
                }
                command = self.command_rx.recv(), if send_idle => {
                    let Some(command) = command else { break };
                    if let Err(e) = self.handle_command(command).await {
                        if e.is::<CustomDataConflict>() {
                            log::warn!("Predict subscription rejected: {e:#}");
                        } else {
                            self.fail_generation(format!("subscription command failed: {e:#}"));
                        }
                    }
                }
            }
        }
        self.clear_writes();
        for (_, cancellation) in self.discovery.inflight.values() {
            cancellation.cancel();
        }
        self.client.disconnect().await;
        self.connected.store(false, Ordering::Release);
    }

    pub(super) async fn handle_command(&mut self, command: PredictCommand) -> anyhow::Result<()> {
        match command {
            PredictCommand::Subscribe(market_id) => {
                self.subscribe_custom_market(
                    market_id,
                    crate::predict_orderbook_data_type(market_id),
                )
                .await?;
            }
            PredictCommand::Unsubscribe(market_id) => {
                if self
                    .custom_data_type(market_id)
                    .is_some_and(|active| *active == crate::predict_orderbook_data_type(market_id))
                {
                    self.unsubscribe_market(market_id).await?;
                }
            }
            PredictCommand::SubscribeQuotes(id, context) => {
                if !self.quotes.contains_key(&id) {
                    let market_id = context.market_id;
                    self.quotes.insert(
                        id,
                        QuoteSubscription {
                            context,
                            last_quote: None,
                        },
                    );
                    self.subscribe_market(market_id).await?;
                }
            }
            PredictCommand::UnsubscribeQuotes(id) => {
                if let Some(subscription) = self.quotes.remove(&id) {
                    self.unsubscribe_market(subscription.context.market_id)
                        .await?;
                }
            }
            PredictCommand::SubscribeDepth(id, context, depth) => {
                if let Some(active) = self.depths.get(&id) {
                    anyhow::ensure!(
                        active.depth == depth,
                        "conflicting Predict snapshot depths for {id}"
                    );
                    return Ok(());
                }
                let market_id = context.market_id;
                self.depths.insert(id, DepthSubscription { context, depth });
                self.subscribe_market(market_id).await?;
                if !self.client.is_active() || self.client.connection_epoch() != self.current_epoch
                {
                    self.invalidate_book_snapshots();
                }
                if let Some(snapshot) = self
                    .books
                    .get(&market_id)
                    .and_then(|source| source.snapshot.as_ref())
                {
                    let subscription = self.depths.get(&id).expect("inserted depth owner");
                    let depth = super::depth::depth_from_snapshot(
                        snapshot,
                        &subscription.context,
                        id,
                        subscription.depth,
                    )?;
                    self.data_sender.send(DataEvent::Data(
                        nautilus_model::data::Data::BookDepth(Box::new(depth)),
                    ))?;
                }
            }
            PredictCommand::UnsubscribeDepth(id) => {
                if let Some(subscription) = self.depths.remove(&id) {
                    self.unsubscribe_market(subscription.context.market_id)
                        .await?;
                }
            }
            PredictCommand::SubscribeTrades(id, context) => {
                if !self.trades.contains_key(&id) {
                    let first = !self
                        .trades
                        .values()
                        .any(|c| c.market_id == context.market_id);
                    let market_id = context.market_id;
                    // Keep desired interest if send fails; reconnect reconciles it.
                    self.trades.insert(id, context);
                    if first {
                        self.queue_trade_subscription("subscribe", market_id)?;
                    }
                }
            }
            PredictCommand::UnsubscribeTrades(id) => {
                if let Some(context) = self.trades.remove(&id)
                    && !self
                        .trades
                        .values()
                        .any(|c| c.market_id == context.market_id)
                {
                    self.remove_dynamic_market_if_unused(context.market_id);
                    self.queue_trade_subscription("unsubscribe", context.market_id)?;
                }
            }
            PredictCommand::StartDynamic(selector, owner) => {
                let key = dynamic_selector_key(&selector)?;
                if let Some(active) = self.dynamic_subscriptions.get_mut(&key) {
                    active.owners.insert(owner);
                    return Ok(());
                }
                self.dynamic_subscriptions.insert(
                    key.clone(),
                    DynamicSubscription {
                        owners: [owner].into(),
                        selector,
                        version: self.discovery.version()?,
                        current_market_id: None,
                        pending_market_id: None,
                        next_refresh_ns: 0,
                    },
                );
                self.schedule_discovery()?;
            }
            PredictCommand::StopDynamic(key, owner) => {
                if let Some(active) = self.dynamic_subscriptions.get_mut(&key)
                    && active.owners.remove(&owner)
                    && active.owners.is_empty()
                {
                    self.stop_dynamic_subscription(&key).await?;
                }
            }
        }
        Ok(())
    }

    fn retain_market(&mut self, market_id: u64) {
        self.subscriptions
            .entry(market_id)
            .or_insert_with(|| MarketSubscription {
                owners: 0,
                data_type: crate::predict_orderbook_data_type(market_id),
            })
            .owners += 1;
    }

    pub(super) fn native_owners(&self, market_id: u64) -> usize {
        self.quotes
            .values()
            .filter(|q| q.context.market_id == market_id)
            .count()
            + self
                .depths
                .values()
                .filter(|d| d.context.market_id == market_id)
                .count()
    }

    pub(super) fn custom_data_type(&self, market_id: u64) -> Option<&DataType> {
        self.subscriptions
            .get(&market_id)
            .filter(|subscription| subscription.owners > self.native_owners(market_id))
            .map(|subscription| &subscription.data_type)
    }

    pub(super) fn ensure_custom_data_type(
        &self,
        market_id: u64,
        data_type: &DataType,
    ) -> anyhow::Result<()> {
        if self
            .custom_data_type(market_id)
            .is_some_and(|active| active != data_type)
        {
            return Err(CustomDataConflict(market_id).into());
        }
        Ok(())
    }

    pub(super) async fn subscribe_custom_market(
        &mut self,
        market_id: u64,
        data_type: DataType,
    ) -> anyhow::Result<()> {
        self.ensure_custom_data_type(market_id, &data_type)?;
        let result = self.subscribe_market(market_id).await;
        self.subscriptions
            .get_mut(&market_id)
            .expect("subscribed market")
            .data_type = data_type;
        result
    }

    pub(super) async fn subscribe_market(&mut self, market_id: u64) -> anyhow::Result<()> {
        let first = !self.subscriptions.contains_key(&market_id);
        self.retain_market(market_id);
        if first {
            self.queue_book_subscription("subscribe", market_id)?;
        }
        Ok(())
    }

    pub(super) async fn unsubscribe_market(&mut self, market_id: u64) -> anyhow::Result<()> {
        let Some(count) = self.subscriptions.get_mut(&market_id) else {
            return Ok(());
        };
        count.owners = count.owners.saturating_sub(1);
        if count.owners == 0 {
            self.subscriptions.remove(&market_id);
            self.books.remove(&market_id);
            self.remove_dynamic_market_if_unused(market_id);
            self.queue_book_subscription("unsubscribe", market_id)?;
        }
        Ok(())
    }

    pub(super) fn queue_book_subscription(
        &mut self,
        method: &'static str,
        market_id: u64,
    ) -> anyhow::Result<()> {
        let request_id = self.next_request_id;
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .context("Predict request ID overflow")?;
        let payload = subscription_message(method, request_id, market_id);
        self.queue_subscription(payload);
        Ok(())
    }

    pub(super) fn queue_subscription(&mut self, payload: String) {
        if self.failed_epoch.is_none() {
            self.pending_writes.push_back(payload);
        }
    }

    pub(super) fn start_subscription_send(&mut self) {
        if self.client.connection_epoch() == self.current_epoch
            && self.subscription_send.is_none()
            && let Some(payload) = self.pending_writes.pop_front()
        {
            let client = Arc::clone(&self.client);
            let epoch = self.current_epoch;
            self.subscription_send = Some(Box::pin(async move {
                let result = client.send_text_on_connection(payload, None, epoch).await;
                (epoch, result)
            }));
        }
    }

    pub(super) fn finish_write(&mut self, epoch: u64, result: Result<(), SendError>, kind: &str) {
        if epoch == self.current_epoch
            && epoch == self.client.connection_epoch()
            && let Err(error) = result
        {
            self.fail_generation(format!("{kind} write failed: {error}"));
        }
    }

    pub(super) fn queue_heartbeat(&mut self, payload: String) {
        if self.control_send.is_some() {
            self.fail_generation("heartbeat send is still pending".into());
            return;
        }
        let client = Arc::clone(&self.client);
        let epoch = self.current_epoch;
        self.control_send = Some(Box::pin(async move {
            let result = client.send_text_on_connection(payload, None, epoch).await;
            (epoch, result)
        }));
    }

    pub(super) fn clear_writes(&mut self) {
        self.subscription_send = None;
        self.control_send = None;
        self.pending_writes.clear();
    }

    pub(super) fn invalidate_book_snapshots(&mut self) {
        for source in self.books.values_mut() {
            source.snapshot = None;
        }
    }

    pub(super) fn check_health(&mut self) {
        if self.overflowed.swap(false, Ordering::AcqRel) {
            self.fail_generation("raw frame queue overflow".to_string());
        }
    }

    pub(super) fn fail_generation(&mut self, reason: String) {
        if self.failed_epoch == Some(self.current_epoch) {
            return;
        }
        self.failed_epoch = Some(self.current_epoch);
        self.clear_writes();
        self.invalidate_book_snapshots();
        let outcome = self.reconnect.request_reconnect();
        log::error!(
            "Predict WebSocket generation failed: {reason}; reconnect={outcome:?}, epoch={}",
            self.current_epoch,
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
#[path = "../../tests/support/quote_session.rs"]
mod quote_session_tests;

#[cfg(test)]
#[path = "../../tests/support/custom_subscriptions.rs"]
mod custom_subscription_tests;

#[cfg(test)]
#[path = "../../tests/support/subscription_contracts.rs"]
mod subscription_contract_tests;
