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

//! Single-owner wallet stream driver. Private close reasons and raw errors are never logged.
//! Native handler mode owns transport replacement; this driver owns only the Predict protocol.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use alloy::primitives::Address;
use nautilus_core::string::secret::SecretString;
use nautilus_network::{
    RECONNECTED, SocketState, SocketStateSink,
    transport::{Message, TransportError},
    websocket::{AuthTracker, SubscriptionState, WebSocketClient, WebSocketConfig},
};
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use super::{
    error::{PredictWalletWsError, Result},
    private_client::PredictWalletSubscriptionState,
    private_messages::{PredictWalletEvent, PredictWalletStreamMessage},
    private_parse::{PredictWalletFrame, parse_frame},
};
use crate::{common::credential::PredictAuthToken, config::PredictWalletWebSocketConfig};

pub(super) async fn connect_transport(
    config: WebSocketConfig,
    timeout_secs: u64,
    connected: Arc<AtomicBool>,
    epoch: Arc<AtomicU64>,
    auth_tracker: AuthTracker,
    overflow: Arc<AtomicBool>,
    capacity: usize,
) -> Result<(mpsc::Receiver<(u64, Message)>, WebSocketClient)> {
    let tracker = auth_tracker.clone();
    let sink = SocketStateSink::new(move |state| {
        connected.store(state == SocketState::Connected, Ordering::Release);
        if state == SocketState::Disconnected {
            tracker.invalidate();
        }
    });
    let (tx, reader) = mpsc::channel(capacity);
    let ping_tx = tx.clone();
    let ping_epoch = epoch.clone();
    let ping_overflow = overflow.clone();
    let client = tokio::time::timeout(
        Duration::from_secs(timeout_secs),
        WebSocketClient::epoch_builder()
            .config(config)
            .state_sink(sink)
            .epoch_handler(Arc::new(move |generation, message| {
                epoch.fetch_max(generation, Ordering::AcqRel);
                if tx.try_send((generation, message)).is_err() {
                    overflow.store(true, Ordering::Release);
                }
            }))
            .epoch_ping_handler(Arc::new(move |generation, data| {
                ping_epoch.fetch_max(generation, Ordering::AcqRel);
                if ping_tx
                    .try_send((generation, Message::Ping(data.into())))
                    .is_err()
                {
                    ping_overflow.store(true, Ordering::Release);
                }
            }))
            .connect(),
    )
    .await
    .map_err(|_| PredictWalletWsError::ConnectTimeout)?
    .map_err(|error| match error {
        TransportError::UpgradeRejected(status) => PredictWalletWsError::HandshakeRejected(status),
        _ => PredictWalletWsError::Transport,
    })?;
    client.set_auth_tracker(auth_tracker, false);
    Ok((reader, client))
}

pub(super) struct PredictWalletHandler {
    client: WebSocketClient,
    reader: mpsc::Receiver<(u64, Message)>,
    output: mpsc::Sender<PredictWalletStreamMessage>,
    state: watch::Sender<PredictWalletSubscriptionState>,
    topic: SecretString,
    account: Address,
    config: PredictWalletWebSocketConfig,
    cancellation: CancellationToken,
    connected: Arc<AtomicBool>,
    epoch: Arc<AtomicU64>,
    next_request_id: i64,
    pending_request: Option<i64>,
    deadline: Option<Instant>,
    before_ack: VecDeque<Box<PredictWalletEvent>>,
    active_epoch: u64,
    rotating: bool,
    overflow: Arc<AtomicBool>,
    token_updates: watch::Receiver<PredictAuthToken>,
    auth_tracker: AuthTracker,
    subscriptions: SubscriptionState,
}

impl PredictWalletHandler {
    #[expect(clippy::too_many_arguments)]
    pub(super) fn new(
        client: WebSocketClient,
        reader: mpsc::Receiver<(u64, Message)>,
        output: mpsc::Sender<PredictWalletStreamMessage>,
        state: watch::Sender<PredictWalletSubscriptionState>,
        token_updates: watch::Receiver<PredictAuthToken>,
        overflow: Arc<AtomicBool>,
        account: Address,
        config: PredictWalletWebSocketConfig,
        cancellation: CancellationToken,
        connected: Arc<AtomicBool>,
        epoch: Arc<AtomicU64>,
        auth_tracker: AuthTracker,
        subscriptions: SubscriptionState,
    ) -> Self {
        let topic = format!(
            "predictWalletEvents/{}",
            token_updates.borrow().expose_secret()
        )
        .into();
        Self {
            client,
            reader,
            output,
            state,
            topic,
            account,
            config,
            cancellation,
            connected,
            epoch,
            next_request_id: 1,
            pending_request: None,
            deadline: None,
            before_ack: VecDeque::new(),
            active_epoch: 0,
            rotating: false,
            overflow,
            token_updates,
            auth_tracker,
            subscriptions,
        }
    }

    pub(super) async fn run(mut self) {
        let cancellation = self.cancellation.clone();
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Ok(()),
            result = self.drive() => result,
        };
        self.connected.store(false, Ordering::Release);
        self.auth_tracker.fail("Predict wallet session stopped");
        self.subscriptions.clear();
        self.state.send_replace(match result {
            Ok(()) => PredictWalletSubscriptionState::Closed,
            Err(error) => PredictWalletSubscriptionState::Failed(error),
        });
        self.client.disconnect().await;
    }

    async fn drive(&mut self) -> Result<()> {
        self.active_epoch = self.client.connection_epoch();
        self.epoch.store(self.active_epoch, Ordering::Release);
        self.subscribe().await?;
        let mut token_watch_open = true;
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if self.overflow.load(Ordering::Acquire) {
                return Err(PredictWalletWsError::QueueOverflow);
            }
            if self.client.is_closed() {
                return Err(PredictWalletWsError::Transport);
            }
            tokio::select! {
                biased;
                _ = self.output.closed() => return Ok(()),
                changed = self.token_updates.changed(), if token_watch_open => {
                    if changed.is_err() { token_watch_open = false; continue; }
                    self.topic = format!("predictWalletEvents/{}", self.token_updates.borrow_and_update().expose_secret()).into();
                    self.reset_subscription();
                    self.rotating = true;
                    self.active_epoch = self.client.connection_epoch();
                    let _ = self.client.reconnect_handle().request_reconnect();
                },
                _ = tick.tick() => {},
                raw = self.reader.recv() => {
                    let (epoch, message) = raw.ok_or(PredictWalletWsError::Closed)?;
                    if epoch < self.epoch.load(Ordering::Acquire) { continue; }
                    if self.rotating && epoch == self.active_epoch { continue; }
                    if epoch != self.active_epoch {
                        self.rotating = false;
                        self.active_epoch = epoch;
                        self.reset_subscription();
                        self.subscribe().await?;
                    }
                    match message {
                        Message::Text(ref text) if text.as_ref() == RECONNECTED.as_bytes() => {},
                        Message::Ping(data) => {
                            let result = self.client.send_pong_on_connection(data.to_vec(), epoch).await;
                            self.check_protocol_send(result, epoch)?;
                        },
                        Message::Pong(_) => {},
                        message => self.handle(epoch, message).await?,
                    }
                },
            }
            if self.client.is_reconnecting() && self.pending_request.is_some() {
                self.reset_subscription();
            }
            if self
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                return Err(PredictWalletWsError::SubscriptionTimeout);
            }
        }
    }

    // A late protocol reply belongs to its original socket; Native replaces the transport.
    fn check_protocol_send(
        &self,
        result: std::result::Result<(), nautilus_network::error::SendError>,
        epoch: u64,
    ) -> Result<()> {
        if result.is_err()
            && !self.client.is_reconnecting()
            && self.client.connection_epoch() == epoch
        {
            return Err(PredictWalletWsError::Transport);
        }
        Ok(())
    }

    fn reset_subscription(&mut self) {
        self.auth_tracker.invalidate();
        self.subscriptions.reset_after_reconnect();
        self.state
            .send_replace(PredictWalletSubscriptionState::Reconnecting);
        self.pending_request = None;
        self.deadline = None;
        self.before_ack.clear();
    }

    async fn subscribe(&mut self) -> Result<()> {
        drop(self.auth_tracker.begin());
        let epoch = self.active_epoch;
        let request_id = self.next_request_id;
        self.next_request_id = request_id
            .checked_add(1)
            .ok_or(PredictWalletWsError::Protocol)?;
        self.pending_request = Some(request_id);
        self.deadline =
            Some(Instant::now() + Duration::from_secs(self.config.subscription_timeout_secs));
        self.state
            .send_replace(PredictWalletSubscriptionState::Subscribing {
                connection_epoch: epoch,
            });
        let request = serde_json::json!({"method": "subscribe", "requestId": request_id,
            "params": [self.topic.expose_secret()]})
        .to_string();
        if self
            .client
            .send_text_on_connection(request, None, self.active_epoch)
            .await
            .is_err()
        {
            if self.client.is_reconnecting() || self.client.connection_epoch() != self.active_epoch
            {
                self.reset_subscription();
                return Ok(());
            }
            return Err(PredictWalletWsError::Transport);
        }
        // Track a non-secret logical channel: JWT-bearing wire topics must never be interned.
        self.subscriptions.mark_subscribe("predictWalletEvents");
        Ok(())
    }

    async fn handle(&mut self, epoch: u64, message: Message) -> Result<()> {
        let text = message.as_text().ok_or(PredictWalletWsError::Decode)?;
        match parse_frame(text, self.topic.expose_secret(), self.account)? {
            PredictWalletFrame::Heartbeat(data) => {
                let reply = format!(r#"{{"method":"heartbeat","data":{}}}"#, data.get());
                let result = self
                    .client
                    .send_text_on_connection(reply, None, epoch)
                    .await;
                self.check_protocol_send(result, epoch)?;
            }
            PredictWalletFrame::Response {
                request_id,
                success,
                code,
            } => {
                if self.pending_request != Some(request_id) {
                    return Ok(());
                }
                if !success {
                    self.subscriptions.mark_failure("predictWalletEvents");
                    self.auth_tracker
                        .fail("Predict wallet subscription rejected");
                    return Err(PredictWalletWsError::SubscriptionRejected(code));
                }
                self.pending_request = None;
                self.deadline = None;
                self.emit(PredictWalletStreamMessage::Subscribed {
                    connection_epoch: epoch,
                    reconciliation_required: true,
                })?;
                while let Some(event) = self.before_ack.pop_front() {
                    self.emit(PredictWalletStreamMessage::Wallet {
                        connection_epoch: epoch,
                        event,
                    })?;
                }
                self.subscriptions.confirm_subscribe("predictWalletEvents");
                self.auth_tracker.succeed();
                self.state
                    .send_replace(PredictWalletSubscriptionState::Subscribed {
                        connection_epoch: epoch,
                    });
            }
            PredictWalletFrame::Wallet(event) => {
                if self.pending_request.is_some() {
                    if self.before_ack.len() == self.config.queue_capacity {
                        return Err(PredictWalletWsError::QueueOverflow);
                    }
                    self.before_ack.push_back(event);
                } else {
                    self.emit(PredictWalletStreamMessage::Wallet {
                        connection_epoch: epoch,
                        event,
                    })?;
                }
            }
            PredictWalletFrame::Ignored => {}
        }
        Ok(())
    }

    fn emit(&self, message: PredictWalletStreamMessage) -> Result<()> {
        self.output.try_send(message).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => PredictWalletWsError::QueueOverflow,
            mpsc::error::TrySendError::Closed(_) => PredictWalletWsError::Closed,
        })
    }
}
