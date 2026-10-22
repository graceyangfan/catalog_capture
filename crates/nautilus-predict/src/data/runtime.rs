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
    StartDynamic(PredictCryptoUpDownSelector),
    StopDynamic(CryptoUpDownSelectorKey),
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
    pub(super) last_timestamp: Option<nautilus_core::UnixNanos>,
}

#[derive(Debug)]
pub(super) struct MarketSubscription {
    pub(super) owners: usize,
    pub(super) data_type: DataType,
}

#[derive(Debug)]
pub(super) struct PredictDataSession {
    pub(super) client: WebSocketClient,
    pub(super) http_client: PredictHttpClient,
    pub(super) reconnect: WebSocketReconnectHandle,
    pub(super) raw_rx: mpsc::Receiver<(u64, Message)>,
    pub(super) command_rx: mpsc::Receiver<PredictCommand>,
    pub(super) overflowed: std::sync::Arc<AtomicBool>,
    pub(super) subscriptions: AHashMap<u64, MarketSubscription>,
    pub(super) quotes: AHashMap<InstrumentId, QuoteSubscription>,
    pub(super) depths: AHashMap<InstrumentId, DepthSubscription>,
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

pub(super) struct PredictSessionInputs {
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
        let http_client = PredictHttpClient::new(
            config.api_key.expose_secret(),
            config.base_url_http.as_deref(),
            config.http_timeout_secs,
        )?;
        let (command_tx, command_rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
        let (discovery_tx, discovery_rx) = mpsc::channel(MAX_DISCOVERY_REQUESTS);
        let session = Self {
            current_epoch: client.connection_epoch(),
            reconnect: client.reconnect_handle(),
            client,
            http_client,
            raw_rx,
            command_rx,
            overflowed,
            subscriptions: AHashMap::new(),
            quotes: AHashMap::new(),
            depths: AHashMap::new(),
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
                () = self.cancellation.cancelled() => break,
                _ = health_tick.tick() => self.check_health(),
                _ = tokio::time::sleep(delay), if can_discover => {
                    if let Err(error) = self.schedule_discovery() {
                        log::warn!("Predict discovery admission failed: {error:#}");
                        break;
                    }
                }
                Some(result) = self.discovery_rx.recv() => {
                    if let Err(error) = self.apply_discovery_result(result).await {
                        log::warn!("Predict discovery apply failed: {error:#}");
                    }
                }
                command = self.command_rx.recv() => {
                    let Some(command) = command else { break };
                    if let Err(error) = self.handle_command(command).await {
                        self.fail_generation(format!("subscription command failed: {error:#}"));
                    }
                }
                raw = self.raw_rx.recv() => {
                    let Some((epoch, message)) = raw else { break };
                    if let Err(error) = self.handle_message(epoch, message).await {
                        log::error!("Predict WebSocket message discarded: {error:#}");
                    }
                }
            }
        }
        for (_, cancellation) in self.discovery.inflight.values() {
            cancellation.cancel();
        }
        self.client.disconnect().await;
        self.connected.store(false, Ordering::Release);
    }

    pub(super) async fn handle_command(&mut self, command: PredictCommand) -> anyhow::Result<()> {
        match command {
            PredictCommand::Subscribe(market_id) => {
                self.subscribe_market(market_id).await?;
                self.refresh_custom_type(market_id);
            }
            PredictCommand::Unsubscribe(market_id) => {
                let native_owners = self.native_owners(market_id);
                if self
                    .subscriptions
                    .get(&market_id)
                    .map_or(0, |subscription| subscription.owners)
                    > native_owners
                {
                    self.unsubscribe_market(market_id).await?;
                }
            }
            PredictCommand::SubscribeQuotes(id, context) => {
                if !self.quotes.contains_key(&id) {
                    let result = self.subscribe_market(context.market_id).await;
                    if result.is_err() {
                        // Retain desired quote interest for the reconnect generation.
                        self.retain_market(context.market_id);
                    }
                    self.quotes.insert(
                        id,
                        QuoteSubscription {
                            context,
                            last_quote: None,
                        },
                    );
                    result?;
                }
            }
            PredictCommand::UnsubscribeQuotes(id) => {
                if let Some(subscription) = self.quotes.get(&id) {
                    let market_id = subscription.context.market_id;
                    if let Err(error) = self.unsubscribe_market(market_id).await {
                        // Keep the quote owner and topic count consistent until reconnect.
                        self.retain_market(market_id);
                        return Err(error);
                    }
                    self.quotes.remove(&id);
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
                let result = self.subscribe_market(context.market_id).await;
                if result.is_err() {
                    self.retain_market(context.market_id);
                }
                self.depths.insert(
                    id,
                    DepthSubscription {
                        context,
                        depth,
                        last_timestamp: None,
                    },
                );
                result?;
            }
            PredictCommand::UnsubscribeDepth(id) => {
                if let Some(subscription) = self.depths.get(&id) {
                    let market_id = subscription.context.market_id;
                    if let Err(error) = self.unsubscribe_market(market_id).await {
                        self.retain_market(market_id);
                        return Err(error);
                    }
                    self.depths.remove(&id);
                }
            }
            PredictCommand::StartDynamic(selector) => {
                let key = dynamic_selector_key(&selector)?;
                if self.dynamic_subscriptions.contains_key(&key) {
                    return Ok(());
                }
                self.dynamic_subscriptions.insert(
                    key.clone(),
                    DynamicSubscription {
                        selector,
                        version: self.discovery.version()?,
                        current_market_id: None,
                        pending_market_id: None,
                        next_refresh_ns: 0,
                    },
                );
                self.schedule_discovery()?;
            }
            PredictCommand::StopDynamic(key) => self.stop_dynamic_subscription(&key).await?,
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

    pub(super) fn refresh_custom_type(&mut self, market_id: u64) {
        let data_type = self
            .dynamic_subscriptions
            .iter()
            .find(|(_, state)| {
                state.current_market_id == Some(market_id)
                    || state.pending_market_id == Some(market_id)
            })
            .map(|(key, _)| crate::predict_crypto_up_down_orderbook_data_type(market_id, key))
            .unwrap_or_else(|| crate::predict_orderbook_data_type(market_id));
        if let Some(subscription) = self.subscriptions.get_mut(&market_id) {
            subscription.data_type = data_type;
        }
    }

    pub(super) async fn subscribe_market(&mut self, market_id: u64) -> anyhow::Result<()> {
        if !self.subscriptions.contains_key(&market_id) {
            self.send_subscription("subscribe", market_id).await?;
        }
        self.retain_market(market_id);
        Ok(())
    }

    pub(super) async fn unsubscribe_market(&mut self, market_id: u64) -> anyhow::Result<()> {
        let Some(count) = self.subscriptions.get_mut(&market_id) else {
            return Ok(());
        };
        count.owners = count.owners.saturating_sub(1);
        if count.owners == 0 {
            self.subscriptions.remove(&market_id);
            self.send_subscription("unsubscribe", market_id).await?;
        }
        Ok(())
    }

    pub(super) async fn send_subscription(
        &mut self,
        method: &str,
        market_id: u64,
    ) -> anyhow::Result<()> {
        let request_id = self.next_request_id;
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .context("Predict request ID overflow")?;
        let payload = subscription_message(method, request_id, market_id);
        self.client
            .send_text(payload, None)
            .await
            .with_context(|| format!("failed to {method} Predict orderbook {market_id}"))
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
