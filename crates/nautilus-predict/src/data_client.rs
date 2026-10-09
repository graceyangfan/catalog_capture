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

//! Live public orderbook client for Predict.
//!
//! Predict publishes one complete orderbook snapshot per binary market. The client loads both
//! outcome definitions before it subscribes, but emits only the native `indexSet = 1` book as a
//! market-scoped [`PredictOrderbookSnapshot`](crate::PredictOrderbookSnapshot).

use std::{
    num::NonZeroU32,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ahash::AHashMap;
use anyhow::Context;
use catalog_capture_core::CryptoUpDownSelectorKey;
use nautilus_common::{
    clients::DataClient,
    live::{runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent,
        data::{
            RequestInstrument, SubscribeCustomData, SubscribeInstrument, SubscribeInstruments,
            UnsubscribeCustomData, UnsubscribeInstrument, UnsubscribeInstruments,
        },
    },
};
use nautilus_core::time::{AtomicTime, get_atomic_clock_realtime};
use nautilus_live::task::TaskGroup;
use nautilus_model::{
    data::{Data, DataType},
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};
use nautilus_network::{
    RECONNECTED,
    http::create_standard_nautilus_headers,
    mode::ReconnectRequestOutcome,
    transport::Message,
    websocket::{
        EpochMessageHandler, InitialConnectRetryPolicy, WebSocketClient, WebSocketConfig,
        WebSocketReconnectHandle,
    },
};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    PREDICT_VENUE, PredictCryptoUpDownSelector, PredictDataClientConfig, PredictHttpClient,
    PredictInstrumentProvider, PredictMarketContext, PredictOrderbookSnapshot,
    PredictOrderbookWire, discover_crypto_up_down_market,
    predict_crypto_up_down_orderbook_data_type, predict_orderbook_data_type,
    providers::instruments_from_market,
};

const RAW_QUEUE_CAPACITY: usize = 1_024;
const COMMAND_QUEUE_CAPACITY: usize = 128;
const MAX_MESSAGE_SIZE_BYTES: usize = 2 * 1024 * 1024;
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug)]
enum PredictCommand {
    Subscribe(u64),
    Unsubscribe(u64),
    StartDynamic(PredictCryptoUpDownSelector),
    StopDynamic(CryptoUpDownSelectorKey),
}

/// Definitions discovered by a rolling selector during one live-client generation.
///
/// `PredictInstrumentProvider` remains the synchronous bootstrap provider required by the
/// Nautilus provider interface. The transport task owns no durable definitions of its own:
/// it registers a discovered market here before publishing it, so synchronous instrument
/// requests from the engine can resolve the same definitions while the selector is live.
#[derive(Debug, Default)]
struct PredictDynamicMarketRegistry {
    generation: u64,
    markets: AHashMap<u64, PredictDynamicMarket>,
}

#[derive(Debug)]
struct PredictDynamicMarket {
    context: PredictMarketContext,
    instruments: AHashMap<InstrumentId, InstrumentAny>,
}

impl PredictDynamicMarketRegistry {
    fn begin_generation(&mut self) -> u64 {
        self.generation = self.generation.wrapping_add(1);
        self.markets.clear();
        self.generation
    }

    fn invalidate_generation(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.markets.clear();
    }

    fn register(
        &mut self,
        generation: u64,
        context: PredictMarketContext,
        instruments: &[InstrumentAny],
    ) -> bool {
        if self.generation != generation {
            return false;
        }
        self.markets.insert(
            context.market_id,
            PredictDynamicMarket {
                context,
                instruments: instruments
                    .iter()
                    .cloned()
                    .map(|instrument| (instrument.id(), instrument))
                    .collect(),
            },
        );
        true
    }

    fn context(&self, market_id: u64) -> Option<PredictMarketContext> {
        self.markets
            .get(&market_id)
            .map(|market| market.context.clone())
    }

    fn instrument(&self, instrument_id: InstrumentId) -> Option<InstrumentAny> {
        self.markets
            .values()
            .find_map(|market| market.instruments.get(&instrument_id).cloned())
    }

    fn remove(&mut self, generation: u64, market_id: u64) {
        if self.generation == generation {
            self.markets.remove(&market_id);
        }
    }
}

/// Nautilus data client which publishes Predict instruments and complete orderbook snapshots.
#[derive(Debug)]
pub struct PredictDataClient {
    clock: &'static AtomicTime,
    client_id: ClientId,
    config: PredictDataClientConfig,
    provider: PredictInstrumentProvider,
    data_sender: EventSender<DataEvent>,
    instruments: AHashMap<InstrumentId, InstrumentAny>,
    contexts: AHashMap<u64, PredictMarketContext>,
    dynamic_registry: Arc<RwLock<PredictDynamicMarketRegistry>>,
    command_tx: Option<mpsc::Sender<PredictCommand>>,
    tasks: TaskGroup,
    cancellation: CancellationToken,
    connected: Arc<AtomicBool>,
}

impl PredictDataClient {
    pub fn new(client_id: ClientId, config: PredictDataClientConfig) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !config.api_key.expose_secret().trim().is_empty(),
            "Predict API key is required for REST market metadata"
        );
        anyhow::ensure!(
            config.market_ids.iter().all(|market_id| *market_id != 0),
            "Predict market IDs must be non-zero"
        );
        let mut unique_market_ids = config.market_ids.clone();
        unique_market_ids.sort_unstable();
        unique_market_ids.dedup();
        anyhow::ensure!(
            unique_market_ids.len() == config.market_ids.len(),
            "Predict market IDs must be unique"
        );

        let http_client = PredictHttpClient::new(
            config.api_key.expose_secret(),
            config.base_url_http.as_deref(),
            config.http_timeout_secs,
        )?;
        let tasks = TaskGroup::new();
        Ok(Self {
            clock: get_atomic_clock_realtime(),
            client_id,
            provider: PredictInstrumentProvider::new(http_client),
            data_sender: get_data_event_sender(),
            instruments: AHashMap::new(),
            contexts: AHashMap::new(),
            dynamic_registry: Arc::new(RwLock::new(PredictDynamicMarketRegistry::default())),
            command_tx: None,
            cancellation: tasks.cancellation_token(),
            tasks,
            connected: Arc::new(AtomicBool::new(false)),
            config,
        })
    }

    async fn bootstrap_markets(&mut self) -> anyhow::Result<()> {
        let mut contexts = AHashMap::new();
        for market_id in &self.config.market_ids {
            let (context, definitions) = self
                .provider
                .load_market_with_definitions(*market_id, self.clock.get_time_ns())
                .await
                .with_context(|| format!("failed to load Predict market {market_id}"))?;
            for instrument in definitions {
                self.instruments.insert(instrument.id(), instrument.clone());
                self.data_sender.send(DataEvent::Instrument(instrument))?;
            }
            contexts.insert(*market_id, context);
        }
        self.contexts = contexts;
        Ok(())
    }

    fn send_command(&self, command: PredictCommand) -> anyhow::Result<()> {
        let sender = self
            .command_tx
            .as_ref()
            .context("Predict WebSocket session is not connected")?;
        sender.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => {
                anyhow::anyhow!("Predict subscription command queue is full")
            }
            mpsc::error::TrySendError::Closed(_) => {
                anyhow::anyhow!("Predict subscription command queue is closed")
            }
        })
    }

    fn validate_snapshot_data_type(&self, data_type: &DataType) -> anyhow::Result<u64> {
        anyhow::ensure!(
            data_type.type_name() == "PredictOrderbookSnapshot",
            "Predict only supports PredictOrderbookSnapshot custom data"
        );
        anyhow::ensure!(
            data_type.metadata().is_none(),
            "PredictOrderbookSnapshot does not accept custom metadata"
        );
        let identifier = data_type.identifier().context(
            "PredictOrderbookSnapshot requires its decimal market ID as the data type identifier",
        )?;
        let market_id = identifier.parse::<u64>().with_context(|| {
            format!("invalid PredictOrderbookSnapshot market identifier `{identifier}`")
        })?;
        anyhow::ensure!(
            predict_orderbook_data_type(market_id).identifier() == Some(identifier),
            "PredictOrderbookSnapshot identifier must be the canonical decimal market ID"
        );
        anyhow::ensure!(
            self.contexts.contains_key(&market_id)
                || self
                    .dynamic_registry
                    .read()
                    .expect("Predict dynamic registry lock poisoned")
                    .context(market_id)
                    .is_some(),
            "Predict market {market_id} was not bootstrapped; configure it before subscribing"
        );
        Ok(market_id)
    }

    fn dynamic_selector(data_type: &DataType) -> anyhow::Result<PredictCryptoUpDownSelector> {
        anyhow::ensure!(
            data_type.type_name() == "PredictCryptoUpDown",
            "Predict dynamic selector must be PredictCryptoUpDown"
        );
        anyhow::ensure!(
            data_type.identifier().is_none(),
            "PredictCryptoUpDown does not accept identifier; market identity is discovered at runtime"
        );
        let metadata = data_type.metadata().context(
            "PredictCryptoUpDown requires metadata.price_feed_symbol, metadata.title_asset, and metadata.interval_secs",
        )?;
        let string = |key: &str| -> anyhow::Result<String> {
            metadata
                .get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .with_context(|| format!("PredictCryptoUpDown requires non-empty metadata.{key}"))
        };
        let interval_secs = string("interval_secs")?.parse::<u64>().with_context(
            || "PredictCryptoUpDown metadata.interval_secs must be a positive integer",
        )?;
        anyhow::ensure!(
            interval_secs > 0,
            "PredictCryptoUpDown metadata.interval_secs must be positive"
        );
        Ok(PredictCryptoUpDownSelector {
            price_feed_symbol: string("price_feed_symbol")?,
            title_asset: string("title_asset")?,
            interval_secs,
        })
    }

    fn publish_instrument(&self, instrument_id: InstrumentId) -> anyhow::Result<()> {
        let instrument = self
            .instruments
            .get(&instrument_id)
            .cloned()
            .or_else(|| {
                self.dynamic_registry
                    .read()
                    .expect("Predict dynamic registry lock poisoned")
                    .instrument(instrument_id)
            })
            .with_context(|| format!("Predict instrument {instrument_id} is not loaded"))?;
        self.data_sender.send(DataEvent::Instrument(instrument))?;
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl DataClient for PredictDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*PREDICT_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!("Starting Predict data client: client_id={}", self.client_id);
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.tasks.begin_shutdown();
        self.command_tx = None;
        self.dynamic_registry
            .write()
            .expect("Predict dynamic registry lock poisoned")
            .invalidate_generation();
        self.connected.store(false, Ordering::Release);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        if ensure_open_task_generation(&self.tasks).await? {
            self.cancellation = self.tasks.cancellation_token();
        }

        // This precedes WebSocket subscription and guarantees definitions reach the engine first.
        self.bootstrap_markets().await?;
        let dynamic_generation = self
            .dynamic_registry
            .write()
            .expect("Predict dynamic registry lock poisoned")
            .begin_generation();
        let (command_tx, session) = PredictWebSocketSession::connect(
            &self.config,
            self.contexts.clone(),
            Arc::clone(&self.dynamic_registry),
            dynamic_generation,
            self.data_sender.clone(),
            self.clock,
            self.cancellation.clone(),
            Arc::clone(&self.connected),
        )
        .await?;
        self.connected.store(true, Ordering::Release);
        self.tasks
            .spawn(session.run())
            .map_err(|error| {
                self.connected.store(false, Ordering::Release);
                error
            })
            .context("failed to spawn Predict WebSocket session")?;
        self.command_tx = Some(command_tx);
        log::info!("Connected: client_id={}", self.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.tasks.begin_shutdown();
        self.command_tx = None;
        self.tasks
            .finish_shutdown(Duration::from_secs(1), DISCONNECT_TIMEOUT)
            .await
            .context("failed to stop Predict tasks")?;
        self.instruments.clear();
        self.contexts.clear();
        self.dynamic_registry
            .write()
            .expect("Predict dynamic registry lock poisoned")
            .invalidate_generation();
        self.connected.store(false, Ordering::Release);
        log::info!("Disconnected: client_id={}", self.client_id);
        Ok(())
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        self.publish_instrument(request.instrument_id)
    }

    fn subscribe_instrument(&mut self, subscription: SubscribeInstrument) -> anyhow::Result<()> {
        self.publish_instrument(subscription.instrument_id)
    }

    fn subscribe_instruments(&mut self, _subscription: SubscribeInstruments) -> anyhow::Result<()> {
        // Definitions for rolling selectors are emitted immediately after discovery. There is no
        // Predict venue-wide instrument stream to open; this command only records the Nautilus
        // subscription so those locally published definitions reach venue-level consumers.
        Ok(())
    }

    fn unsubscribe_instrument(
        &mut self,
        _unsubscription: &UnsubscribeInstrument,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn unsubscribe_instruments(
        &mut self,
        _unsubscription: &UnsubscribeInstruments,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn subscribe(&mut self, command: SubscribeCustomData) -> anyhow::Result<()> {
        match command.data_type.type_name() {
            "PredictOrderbookSnapshot" => self.send_command(PredictCommand::Subscribe(
                self.validate_snapshot_data_type(&command.data_type)?,
            )),
            "PredictCryptoUpDown" => self.send_command(PredictCommand::StartDynamic(
                Self::dynamic_selector(&command.data_type)?,
            )),
            _ => anyhow::bail!("unsupported Predict custom data type {}", command.data_type),
        }
    }

    fn unsubscribe(&mut self, command: &UnsubscribeCustomData) -> anyhow::Result<()> {
        match command.data_type.type_name() {
            "PredictOrderbookSnapshot" => self.send_command(PredictCommand::Unsubscribe(
                self.validate_snapshot_data_type(&command.data_type)?,
            )),
            "PredictCryptoUpDown" => self.send_command(PredictCommand::StopDynamic(
                dynamic_selector_key(&Self::dynamic_selector(&command.data_type)?)?,
            )),
            _ => anyhow::bail!("unsupported Predict custom data type {}", command.data_type),
        }
    }
}

/// Reopens a task group only after its prior generation has fully drained.
///
/// `DataClient::stop` and `reset` are synchronous, whereas `TaskGroup` deliberately requires
/// the owner to await the previous generation before opening the next one.
async fn ensure_open_task_generation(tasks: &TaskGroup) -> anyhow::Result<bool> {
    if tasks.is_open() {
        return Ok(false);
    }
    tasks
        .finish_shutdown(Duration::from_secs(1), DISCONNECT_TIMEOUT)
        .await
        .context("failed to finish previous Predict task generation")?;
    tasks
        .start_generation()
        .context("failed to start Predict task generation")?;
    Ok(true)
}

#[derive(Debug)]
struct PredictWebSocketSession {
    client: WebSocketClient,
    http_client: PredictHttpClient,
    reconnect: WebSocketReconnectHandle,
    raw_rx: mpsc::Receiver<(u64, Message)>,
    command_rx: mpsc::Receiver<PredictCommand>,
    overflowed: std::sync::Arc<AtomicBool>,
    subscriptions: AHashMap<u64, usize>,
    contexts: AHashMap<u64, PredictMarketContext>,
    dynamic_registry: Arc<RwLock<PredictDynamicMarketRegistry>>,
    dynamic_generation: u64,
    dynamic_subscriptions: AHashMap<CryptoUpDownSelectorKey, DynamicSubscription>,
    data_sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
    cancellation: CancellationToken,
    connected: Arc<AtomicBool>,
    current_epoch: u64,
    failed_epoch: Option<u64>,
    next_request_id: u64,
    dynamic_refresh_secs: u64,
}

#[derive(Debug, Clone)]
struct DynamicSubscription {
    selector: PredictCryptoUpDownSelector,
    current_market_id: Option<u64>,
    pending_market_id: Option<u64>,
    /// Earliest UTC instant at which discovery must run again.
    ///
    /// A selected market window is authoritative for the normal rollover
    /// deadline. While a successor is awaiting its first snapshot we retry at
    /// the short configured cadence instead, retaining the last confirmed
    /// market until the successor has proved live.
    next_refresh_ns: u64,
}

impl PredictWebSocketSession {
    async fn connect(
        config: &PredictDataClientConfig,
        contexts: AHashMap<u64, PredictMarketContext>,
        dynamic_registry: Arc<RwLock<PredictDynamicMarketRegistry>>,
        dynamic_generation: u64,
        data_sender: EventSender<DataEvent>,
        clock: &'static AtomicTime,
        cancellation: CancellationToken,
        connected: Arc<AtomicBool>,
    ) -> anyhow::Result<(mpsc::Sender<PredictCommand>, Self)> {
        let (raw_tx, raw_rx) = mpsc::channel(RAW_QUEUE_CAPACITY);
        let overflowed = std::sync::Arc::new(AtomicBool::new(false));
        let overflowed_handler = std::sync::Arc::clone(&overflowed);
        let epoch_handler: EpochMessageHandler = std::sync::Arc::new(move |epoch, message| {
            if let Err(error) = raw_tx.try_send((epoch, message)) {
                if matches!(error, mpsc::error::TrySendError::Full(_)) {
                    overflowed_handler.store(true, Ordering::Release);
                }
            }
        });
        let mut headers = create_standard_nautilus_headers();
        headers.push((
            "x-api-key".to_string(),
            config.api_key.expose_secret().to_string(),
        ));
        let ws_config = WebSocketConfig {
            url: config.ws_url(),
            headers,
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
            proxy_url: None,
            max_message_size_bytes: Some(MAX_MESSAGE_SIZE_BYTES),
            max_frame_size_bytes: Some(MAX_MESSAGE_SIZE_BYTES),
        };
        let max_attempts = NonZeroU32::new(5).context("connect attempts must be non-zero")?;
        let client = WebSocketClient::epoch_builder()
            .config(ws_config)
            .epoch_handler(epoch_handler)
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
            .context("failed to connect Predict WebSocket")?;
        let http_client = PredictHttpClient::new(
            config.api_key.expose_secret(),
            config.base_url_http.as_deref(),
            config.http_timeout_secs,
        )?;
        let (command_tx, command_rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
        let session = Self {
            current_epoch: client.connection_epoch(),
            reconnect: client.reconnect_handle(),
            client,
            http_client,
            raw_rx,
            command_rx,
            overflowed,
            subscriptions: AHashMap::new(),
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
        };
        Ok((command_tx, session))
    }

    async fn run(mut self) {
        let mut health_tick = tokio::time::interval(Duration::from_secs(1));
        let mut dynamic_tick =
            tokio::time::interval(Duration::from_secs(self.dynamic_refresh_secs));
        loop {
            tokio::select! {
                biased;
                () = self.cancellation.cancelled() => break,
                _ = health_tick.tick() => self.check_health(),
                _ = dynamic_tick.tick(), if !self.dynamic_subscriptions.is_empty() => {
                    if let Err(error) = self.refresh_dynamic_subscriptions().await {
                        log::warn!("Predict crypto up/down discovery refresh failed: {error:#}");
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
        self.client.disconnect().await;
        self.connected.store(false, Ordering::Release);
    }

    async fn handle_command(&mut self, command: PredictCommand) -> anyhow::Result<()> {
        match command {
            PredictCommand::Subscribe(market_id) => self.subscribe_market(market_id).await?,
            PredictCommand::Unsubscribe(market_id) => self.unsubscribe_market(market_id).await?,
            PredictCommand::StartDynamic(selector) => {
                let key = dynamic_selector_key(&selector)?;
                if self.dynamic_subscriptions.contains_key(&key) {
                    return Ok(());
                }
                self.dynamic_subscriptions.insert(
                    key.clone(),
                    DynamicSubscription {
                        selector,
                        current_market_id: None,
                        pending_market_id: None,
                        next_refresh_ns: 0,
                    },
                );
                self.refresh_dynamic_subscription(&key, self.clock.get_time_ns().as_u64())
                    .await?;
            }
            PredictCommand::StopDynamic(key) => self.stop_dynamic_subscription(&key).await?,
        }
        Ok(())
    }

    async fn subscribe_market(&mut self, market_id: u64) -> anyhow::Result<()> {
        let count = self.subscriptions.entry(market_id).or_default();
        *count += 1;
        if *count == 1 {
            self.send_subscription("subscribe", market_id).await?;
        }
        Ok(())
    }

    async fn unsubscribe_market(&mut self, market_id: u64) -> anyhow::Result<()> {
        let Some(count) = self.subscriptions.get_mut(&market_id) else {
            return Ok(());
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.subscriptions.remove(&market_id);
            self.send_subscription("unsubscribe", market_id).await?;
        }
        Ok(())
    }

    async fn refresh_dynamic_subscriptions(&mut self) -> anyhow::Result<()> {
        let now_ns = self.clock.get_time_ns().as_u64();
        let keys = self
            .dynamic_subscriptions
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            let due = self
                .dynamic_subscriptions
                .get(&key)
                .is_some_and(|state| state.next_refresh_ns <= now_ns);
            if due {
                self.refresh_dynamic_subscription(&key, now_ns).await?;
            }
        }
        Ok(())
    }

    async fn refresh_dynamic_subscription(
        &mut self,
        key: &CryptoUpDownSelectorKey,
        now_ns: u64,
    ) -> anyhow::Result<()> {
        let selector = self
            .dynamic_subscriptions
            .get(key)
            .map(|state| state.selector.clone())
            .with_context(|| format!("missing Predict dynamic selector {key}"))?;
        let selected = discover_crypto_up_down_market(&self.http_client, &selector, now_ns).await?;
        let (current_market_id, pending_market_id) = self
            .dynamic_subscriptions
            .get(key)
            .map(|state| (state.current_market_id, state.pending_market_id))
            .context("Predict dynamic selector disappeared during discovery")?;
        if current_market_id == Some(selected.market_id) {
            // A transient discovery regression can select the confirmed market while a previous
            // successor is still pending. The confirmed market remains valid, but the stale
            // successor must not keep a socket subscription or registry entry alive.
            if let Some(stale_pending) = pending_market_id
                && Some(stale_pending) != current_market_id
            {
                self.unsubscribe_market(stale_pending).await?;
                if let Some(state) = self.dynamic_subscriptions.get_mut(key)
                    && state.pending_market_id == Some(stale_pending)
                {
                    state.pending_market_id = None;
                }
                self.remove_dynamic_market_if_unused(stale_pending);
            }
            self.set_dynamic_refresh_deadline(key, selected.window.end_ns, now_ns, false)?;
            return Ok(());
        }
        if pending_market_id == Some(selected.market_id) {
            // Keep discovery cheap in the steady state, but retry promptly while
            // the successor has not emitted a valid snapshot. A failed or lost
            // subscription must not strand the selector until the next window.
            self.set_dynamic_refresh_deadline(key, selected.window.end_ns, now_ns, true)?;
            return Ok(());
        }
        if let Some(stale_pending) = pending_market_id {
            self.unsubscribe_market(stale_pending).await?;
        }
        let market = self.http_client.request_market(selected.market_id).await?;
        let (context, instruments) = instruments_from_market(&market, self.clock.get_time_ns())?;
        let registered = self
            .dynamic_registry
            .write()
            .expect("Predict dynamic registry lock poisoned")
            .register(self.dynamic_generation, context.clone(), &instruments);
        if !registered {
            // The client was stopped or reset while the REST request was in flight. Do not
            // publish a stale definition into the next Nautilus client generation.
            return Ok(());
        }
        for instrument in instruments {
            self.data_sender.send(DataEvent::Instrument(instrument))?;
        }
        self.subscribe_market(selected.market_id).await?;
        let state = self
            .dynamic_subscriptions
            .get_mut(key)
            .context("Predict dynamic selector disappeared before activation")?;
        state.pending_market_id = Some(selected.market_id);
        state.next_refresh_ns =
            now_ns.saturating_add(self.dynamic_refresh_secs.saturating_mul(1_000_000_000));
        if let Some(stale_pending) = pending_market_id {
            self.remove_dynamic_market_if_unused(stale_pending);
        }
        Ok(())
    }

    fn set_dynamic_refresh_deadline(
        &mut self,
        key: &CryptoUpDownSelectorKey,
        window_end_ns: u64,
        now_ns: u64,
        retry_pending: bool,
    ) -> anyhow::Result<()> {
        let state = self
            .dynamic_subscriptions
            .get_mut(key)
            .with_context(|| format!("missing Predict dynamic selector {key}"))?;
        state.next_refresh_ns = next_dynamic_refresh_ns(
            window_end_ns,
            now_ns,
            self.dynamic_refresh_secs,
            retry_pending,
        );
        Ok(())
    }

    async fn stop_dynamic_subscription(
        &mut self,
        key: &CryptoUpDownSelectorKey,
    ) -> anyhow::Result<()> {
        let Some(state) = self.dynamic_subscriptions.remove(key) else {
            return Ok(());
        };
        if let Some(market_id) = state.pending_market_id {
            self.unsubscribe_market(market_id).await?;
        }
        if let Some(market_id) = state.current_market_id
            && Some(market_id) != state.pending_market_id
        {
            self.unsubscribe_market(market_id).await?;
        }
        for market_id in [state.pending_market_id, state.current_market_id]
            .into_iter()
            .flatten()
        {
            self.remove_dynamic_market_if_unused(market_id);
        }
        Ok(())
    }

    fn remove_dynamic_market_if_unused(&self, market_id: u64) {
        let still_referenced = self.dynamic_subscriptions.values().any(|state| {
            state.current_market_id == Some(market_id) || state.pending_market_id == Some(market_id)
        });
        if !still_referenced {
            self.dynamic_registry
                .write()
                .expect("Predict dynamic registry lock poisoned")
                .remove(self.dynamic_generation, market_id);
        }
    }

    async fn send_subscription(&mut self, method: &str, market_id: u64) -> anyhow::Result<()> {
        let request_id = self.next_request_id;
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .context("Predict request ID overflow")?;
        let payload = json!({
            "method": method,
            "requestId": request_id,
            "params": [format!("predictOrderbook/{market_id}")],
        })
        .to_string();
        self.client
            .send_text(payload, None)
            .await
            .with_context(|| format!("failed to {method} Predict orderbook {market_id}"))
    }

    async fn handle_message(&mut self, epoch: u64, message: Message) -> anyhow::Result<()> {
        let text = message
            .as_text()
            .context("Predict sent a non-text application frame")?;
        if text == RECONNECTED {
            self.current_epoch = epoch;
            self.failed_epoch = None;
            let market_ids = self.subscriptions.keys().copied().collect::<Vec<_>>();
            for market_id in market_ids {
                self.send_subscription("subscribe", market_id).await?;
            }
            return Ok(());
        }
        if epoch != self.current_epoch || self.failed_epoch == Some(epoch) {
            return Ok(());
        }
        let value: Value = serde_json::from_str(text).context("invalid Predict WebSocket JSON")?;
        // Predict sends heartbeat probes as market frames (`type=M`, `topic=heartbeat`),
        // not as a client-style `method=heartbeat` request. Echo the exact timestamp before
        // applying normal market-topic routing; otherwise the server closes the socket.
        if value.get("topic").and_then(Value::as_str) == Some("heartbeat") {
            let data = value
                .get("data")
                .cloned()
                .context("Predict heartbeat is missing data")?;
            self.client
                .send_text(
                    json!({"method": "heartbeat", "data": data}).to_string(),
                    None,
                )
                .await
                .context("failed to echo Predict heartbeat")?;
            return Ok(());
        }
        if value.get("type").and_then(Value::as_str) != Some("M") {
            return Ok(()); // response/ack frames are informational; subscription state is local.
        }
        let topic = value
            .get("topic")
            .and_then(Value::as_str)
            .context("Predict market frame is missing topic")?;
        // The public socket can multiplex informational market frames. Only the canonical
        // orderbook topic belongs to this client; ignore unrelated market topics instead of
        // tearing down a healthy generation.
        if !topic.starts_with("predictOrderbook/") {
            log::debug!("Predict market frame ignored unrelated topic={topic:?}");
            return Ok(());
        }
        let market_id = parse_topic_market_id(topic)?;
        if !self.subscriptions.contains_key(&market_id) {
            return Ok(());
        }
        let context = self
            .contexts
            .get(&market_id)
            .cloned()
            .or_else(|| {
                self.dynamic_registry
                    .read()
                    .expect("Predict dynamic registry lock poisoned")
                    .context(market_id)
            })
            .with_context(|| format!("Predict frame for unconfigured market {market_id}"))?;
        let wire: PredictOrderbookWire = serde_json::from_value(
            value
                .get("data")
                .cloned()
                .context("Predict market frame is missing data")?,
        )
        .context("failed to decode Predict orderbook snapshot")?;
        let snapshot =
            PredictOrderbookSnapshot::from_wire(&context, wire, self.clock.get_time_ns())?;
        let data_type = self
            .dynamic_subscriptions
            .iter()
            .find(|(_, state)| {
                state.current_market_id == Some(market_id)
                    || state.pending_market_id == Some(market_id)
            })
            .map(|(key, _)| predict_crypto_up_down_orderbook_data_type(market_id, key))
            .unwrap_or_else(|| predict_orderbook_data_type(market_id));
        self.data_sender.send(DataEvent::Data(Data::Custom(
            snapshot.into_custom_data_with_type(data_type),
        )))?;
        self.confirm_dynamic_snapshot(market_id).await?;
        Ok(())
    }

    async fn confirm_dynamic_snapshot(&mut self, market_id: u64) -> anyhow::Result<()> {
        let ready = self
            .dynamic_subscriptions
            .iter()
            .filter_map(|(key, state)| {
                (state.pending_market_id == Some(market_id))
                    .then_some((key.clone(), state.current_market_id))
            })
            .collect::<Vec<_>>();
        for (key, previous_market_id) in ready {
            if let Some(previous_market_id) = previous_market_id {
                self.unsubscribe_market(previous_market_id).await?;
            }
            if let Some(state) = self.dynamic_subscriptions.get_mut(&key) {
                state.current_market_id = Some(market_id);
                state.pending_market_id = None;
            }
            if let Some(previous_market_id) = previous_market_id {
                self.remove_dynamic_market_if_unused(previous_market_id);
            }
        }
        Ok(())
    }

    fn check_health(&mut self) {
        if self.overflowed.swap(false, Ordering::AcqRel) {
            self.fail_generation("raw frame queue overflow".to_string());
        }
    }

    fn fail_generation(&mut self, reason: String) {
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

fn dynamic_selector_key(
    selector: &PredictCryptoUpDownSelector,
) -> anyhow::Result<CryptoUpDownSelectorKey> {
    CryptoUpDownSelectorKey::new(
        &selector.price_feed_symbol,
        &selector.title_asset,
        selector.interval_secs,
    )
}

fn next_dynamic_refresh_ns(
    window_end_ns: u64,
    now_ns: u64,
    retry_secs: u64,
    retry_pending: bool,
) -> u64 {
    if retry_pending {
        now_ns.saturating_add(retry_secs.saturating_mul(1_000_000_000))
    } else {
        window_end_ns.max(now_ns)
    }
}

fn parse_topic_market_id(topic: &str) -> anyhow::Result<u64> {
    let market_id = topic
        .strip_prefix("predictOrderbook/")
        .context("unexpected Predict topic; expected predictOrderbook/{marketId}")?;
    anyhow::ensure!(
        !market_id.is_empty() && !market_id.contains('/'),
        "invalid Predict orderbook topic `{topic}`"
    );
    market_id
        .parse()
        .with_context(|| format!("invalid Predict orderbook topic `{topic}`"))
}

#[cfg(test)]
mod tests {
    use super::{
        PredictDynamicMarketRegistry, dynamic_selector_key, ensure_open_task_generation,
        next_dynamic_refresh_ns, parse_topic_market_id,
    };
    use crate::{PredictCryptoUpDownSelector, PredictMarketContext, instrument_id_from_outcome};

    fn context(market_id: u64) -> PredictMarketContext {
        PredictMarketContext {
            market_id,
            yes_instrument_id: instrument_id_from_outcome(market_id, 1).unwrap(),
            no_instrument_id: instrument_id_from_outcome(market_id, 2).unwrap(),
            price_precision: 2,
            size_precision: 16,
        }
    }

    #[test]
    fn refreshes_at_window_boundary_and_retries_pending_successor() {
        let now_ns = 100 * 1_000_000_000;
        let end_ns = 300 * 1_000_000_000;
        assert_eq!(next_dynamic_refresh_ns(end_ns, now_ns, 1, false), end_ns);
        assert_eq!(
            next_dynamic_refresh_ns(end_ns, now_ns, 1, true),
            now_ns + 1_000_000_000
        );
    }

    #[test]
    fn selector_key_deduplicates_title_casing_and_whitespace() {
        let canonical = PredictCryptoUpDownSelector {
            price_feed_symbol: "BTC/USDT".to_string(),
            title_asset: "Bitcoin".to_string(),
            interval_secs: 300,
        };
        let equivalent = PredictCryptoUpDownSelector {
            price_feed_symbol: " BTC/USDT ".to_string(),
            title_asset: " bitcoin ".to_string(),
            interval_secs: 300,
        };
        assert_eq!(
            dynamic_selector_key(&canonical).unwrap(),
            dynamic_selector_key(&equivalent).unwrap()
        );
    }

    #[test]
    fn selector_key_deduplicates_feed_separator_and_case_aliases() {
        let canonical = PredictCryptoUpDownSelector {
            price_feed_symbol: "BTC/USDT".to_string(),
            title_asset: "Bitcoin".to_string(),
            interval_secs: 300,
        };
        let equivalent = PredictCryptoUpDownSelector {
            price_feed_symbol: "btcusdt".to_string(),
            title_asset: "BITCOIN".to_string(),
            interval_secs: 300,
        };
        assert_eq!(
            dynamic_selector_key(&canonical).unwrap(),
            dynamic_selector_key(&equivalent).unwrap()
        );
    }

    #[test]
    fn only_accepts_canonical_orderbook_topics() {
        assert_eq!(
            parse_topic_market_id("predictOrderbook/2859346").unwrap(),
            2_859_346
        );
        for topic in ["predictOrderbook/", "predictOrderbook/1/extra", "trades/1"] {
            assert!(
                parse_topic_market_id(topic).is_err(),
                "expected {topic} to fail"
            );
        }
    }

    #[test]
    fn dynamic_registry_rejects_a_stale_client_generation() {
        let mut registry = PredictDynamicMarketRegistry::default();
        let first_generation = registry.begin_generation();
        assert!(registry.register(first_generation, context(42), &[]));
        assert_eq!(registry.context(42).unwrap().market_id, 42);

        let second_generation = registry.begin_generation();
        assert_ne!(first_generation, second_generation);
        assert!(registry.context(42).is_none());
        assert!(!registry.register(first_generation, context(43), &[]));
        assert!(registry.register(second_generation, context(43), &[]));
        assert_eq!(registry.context(43).unwrap().market_id, 43);
    }

    #[test]
    fn dynamic_registry_removes_only_its_current_generation_market() {
        let mut registry = PredictDynamicMarketRegistry::default();
        let generation = registry.begin_generation();
        assert!(registry.register(generation, context(42), &[]));
        registry.remove(generation.wrapping_add(1), 42);
        assert!(registry.context(42).is_some());
        registry.remove(generation, 42);
        assert!(registry.context(42).is_none());
    }

    #[tokio::test]
    async fn reset_generation_is_drained_before_it_is_reopened() {
        let tasks = nautilus_live::task::TaskGroup::new();
        tasks.begin_shutdown();
        assert!(!tasks.is_open());
        assert!(ensure_open_task_generation(&tasks).await.unwrap());
        assert!(tasks.is_open());
        assert!(!ensure_open_task_generation(&tasks).await.unwrap());
    }
}
