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

//! Account-scoped wallet stream with Native transport lifecycle management.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use nautilus_live::task::{TaskJoinOutcome, TaskSlot, finish_task};
use nautilus_network::{
    SocketState, SocketStateSink,
    http::create_standard_nautilus_headers,
    transport::{Message, TransportError},
    websocket::{AuthTracker, WebSocketClient, WebSocketConfig},
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::{
    error::{PredictWalletWsError, Result},
    private_handler::{PredictWalletHandler, TimedWalletMessage},
    private_messages::PredictWalletStreamMessage,
};
use crate::{config::PredictWalletWebSocketConfig, http::PredictAuthenticatedHttpClient};

/// Wallet subscription state for the current transport generation.
/// A confirmed subscription does not establish account or signing readiness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredictWalletSubscriptionState {
    Subscribing { connection_epoch: u64 },
    Subscribed { connection_epoch: u64 },
    Reconnecting,
    Failed(PredictWalletWsError),
    Closed,
}

/// Wallet stream backed by a Native transport task and generation cancellation.
pub struct PredictWalletWebSocketClient {
    cancellation: CancellationToken,
    task: TaskSlot<()>,
    auth_tracker: AuthTracker,
    state: watch::Receiver<PredictWalletSubscriptionState>,
    output: mpsc::Receiver<TimedWalletMessage>,
    connected: Arc<AtomicBool>,
    epoch: Arc<AtomicU64>,
}

impl fmt::Debug for PredictWalletWebSocketClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PredictWalletWebSocketClient")
            .field("state", &self.subscription_state())
            .finish_non_exhaustive()
    }
}

impl Drop for PredictWalletWebSocketClient {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

impl PredictWalletWebSocketClient {
    // Execution tasks share these observations; the transport/ACK driver owns writes.
    pub(crate) fn authentication_tracker(&self) -> AuthTracker {
        self.auth_tracker.clone()
    }
    pub(crate) fn connection_epoch_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.epoch)
    }

    /// Connects a dedicated wallet socket. Call `wait_until_subscribed` to await its ACK.
    /// The caller's cancellation token controls initial connection and the session lifetime.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration, mismatched environment or handshake failure.
    pub async fn connect(
        config: PredictWalletWebSocketConfig,
        session: &PredictAuthenticatedHttpClient,
        cancellation: CancellationToken,
    ) -> Result<Self> {
        Self::connect_observed(config, session, cancellation, false).await
    }

    pub(crate) async fn connect_observed(
        config: PredictWalletWebSocketConfig,
        session: &PredictAuthenticatedHttpClient,
        cancellation: CancellationToken,
        observe: bool,
    ) -> Result<Self> {
        let url = config.validated_ws_url()?;
        if config.environment != session.environment() {
            return Err(PredictWalletWsError::Configuration(
                "wallet socket and HTTP session environments differ",
            ));
        }
        let cancellation = cancellation.child_token();
        let mut headers = create_standard_nautilus_headers();
        let key = config
            .api_key
            .as_ref()
            .ok_or(PredictWalletWsError::Configuration(
                "missing WebSocket API key",
            ))?;
        headers.push(("x-api-key".into(), key.expose_secret().to_string()));
        let ws_config = WebSocketConfig {
            url,
            headers,
            heartbeat_interval_secs: None,
            heartbeat_payload: None,
            connect_timeout_ms: Some(config.connect_timeout_secs * 1000),
            reconnect_delay_initial_ms: Some(500),
            reconnect_delay_max_ms: Some(10_000),
            reconnect_backoff_factor: Some(2.0),
            reconnect_jitter_ms: Some(250),
            reconnect_max_attempts: None,
            heartbeat_timeout_secs: Some(45),
            idle_timeout_ms: None,
            writer_capacity: Some(128),
            backend: config.transport_backend,
            proxy_url: None,
            max_message_size_bytes: Some(2 * 1024 * 1024),
            max_frame_size_bytes: Some(2 * 1024 * 1024),
        };
        let connected = Arc::new(AtomicBool::new(false));
        let epoch = Arc::new(AtomicU64::new(0));
        let auth_tracker = AuthTracker::new();
        let overflow = Arc::new(AtomicBool::new(false));
        let state_connected = Arc::clone(&connected);
        let tracker = auth_tracker.clone();
        let sink = SocketStateSink::new(move |state| {
            state_connected.store(state == SocketState::Connected, Ordering::Release);
            if state == SocketState::Disconnected {
                tracker.invalidate();
            }
        });
        let (tx, reader) = mpsc::channel(config.queue_capacity);
        let ping_tx = tx.clone();
        let ping_epoch = Arc::clone(&epoch);
        let ping_overflow = Arc::clone(&overflow);
        let message_epoch = Arc::clone(&epoch);
        let message_overflow = Arc::clone(&overflow);
        let builder = WebSocketClient::epoch_builder()
            .config(ws_config)
            .state_sink(sink)
            .epoch_handler(Arc::new(move |generation, message| {
                let received = observe.then(std::time::Instant::now);
                message_epoch.fetch_max(generation, Ordering::AcqRel);
                if tx.try_send((generation, message, received)).is_err() {
                    message_overflow.store(true, Ordering::Release);
                }
            }))
            .epoch_ping_handler(Arc::new(move |generation, data| {
                ping_epoch.fetch_max(generation, Ordering::AcqRel);
                if ping_tx
                    .try_send((generation, Message::Ping(data.into()), None))
                    .is_err()
                {
                    ping_overflow.store(true, Ordering::Release);
                }
            }))
            .cancellation_token(cancellation.clone());
        let client = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(PredictWalletWsError::Closed),
            result = tokio::time::timeout(Duration::from_secs(config.connect_timeout_secs), builder.connect()) => {
                result.map_err(|_| PredictWalletWsError::ConnectTimeout)?
                    .map_err(|error| match error {
                        TransportError::UpgradeRejected(status) => PredictWalletWsError::HandshakeRejected(status),
                        _ => PredictWalletWsError::Transport,
                    })?
            }
        };
        client.set_auth_tracker(auth_tracker.clone(), false);
        let (state_tx, state) = watch::channel(PredictWalletSubscriptionState::Subscribing {
            connection_epoch: 0,
        });
        let (out_tx, output) = mpsc::channel(config.queue_capacity);
        let handler = PredictWalletHandler::new(
            client,
            reader,
            out_tx,
            state_tx,
            session.token_updates(),
            overflow,
            session.account_address(),
            config,
            cancellation.clone(),
            Arc::clone(&connected),
            Arc::clone(&epoch),
            auth_tracker.clone(),
        );
        let mut task = TaskSlot::new();
        task.spawn(handler.run())
            .map_err(|_| PredictWalletWsError::Task)?;
        Ok(Self {
            cancellation,
            task,
            auth_tracker,
            state,
            output,
            connected,
            epoch,
        })
    }

    /// Returns wallet ACK state for the current transport generation.
    #[must_use]
    pub fn subscription_state(&self) -> PredictWalletSubscriptionState {
        let state = *self.state.borrow();
        if matches!(
            state,
            PredictWalletSubscriptionState::Closed | PredictWalletSubscriptionState::Failed(_)
        ) {
            return state;
        }
        if self.cancellation.is_cancelled() {
            return PredictWalletSubscriptionState::Closed;
        }
        if !self.connected.load(Ordering::Acquire) {
            return PredictWalletSubscriptionState::Reconnecting;
        }
        if let PredictWalletSubscriptionState::Subscribed { connection_epoch } = state
            && (connection_epoch != self.epoch.load(Ordering::Acquire)
                || !self.auth_tracker.is_authenticated())
        {
            return PredictWalletSubscriptionState::Subscribing {
                connection_epoch: self.epoch.load(Ordering::Acquire),
            };
        }
        state
    }

    /// Awaits confirmation for the current connection. This does not establish trading readiness.
    ///
    /// # Errors
    ///
    /// Returns an error if the session fails or closes before ACK confirmation.
    pub async fn wait_until_subscribed(&mut self) -> Result<u64> {
        loop {
            match self.subscription_state() {
                PredictWalletSubscriptionState::Subscribed { connection_epoch } => {
                    return Ok(connection_epoch);
                }
                PredictWalletSubscriptionState::Failed(error) => return Err(error),
                PredictWalletSubscriptionState::Closed => return Err(PredictWalletWsError::Closed),
                _ => {}
            }
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => return Err(PredictWalletWsError::Closed),
                result = self.state.changed() => result.map_err(|_| PredictWalletWsError::Task)?,
            }
        }
    }

    /// Receives control/events for the current connection; obsolete queued epochs are retired.
    /// Re-subscription always reports a reconciliation requirement before fresh wallet events.
    ///
    /// # Errors
    ///
    /// Returns an error on data loss, invalid frames or terminal subscription failure.
    pub async fn next_message(&mut self) -> Result<Option<PredictWalletStreamMessage>> {
        Ok(self.next_timed_message().await?.map(|(message, _)| message))
    }

    pub(crate) async fn next_timed_message(&mut self) -> Result<Option<TimedWalletMessage>> {
        loop {
            match self.subscription_state() {
                PredictWalletSubscriptionState::Failed(error) => return Err(error),
                PredictWalletSubscriptionState::Closed => return Ok(None),
                _ => {}
            }
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => return Ok(None),
                changed = self.state.changed() => { if changed.is_err() { return Err(PredictWalletWsError::Task); } },
                message = self.output.recv() => {
                    let Some(message) = message else { continue };
                    if matches!(self.subscription_state(), PredictWalletSubscriptionState::Failed(_)) { continue; }
                    if self.connected.load(Ordering::Acquire)
                        && message.0.connection_epoch() == self.epoch.load(Ordering::Acquire) {
                        return Ok(Some(message));
                    }
                }
            }
        }
    }

    /// Cancels the handler and joins transport cleanup. Drop also signals cancellation.
    ///
    /// # Errors
    ///
    /// Returns an error if task cleanup fails or remains incomplete.
    pub async fn close(&mut self) -> Result<()> {
        self.cancellation.cancel();
        let outcome = finish_task(
            &mut self.task,
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .await;
        self.auth_tracker.invalidate();
        match outcome {
            None | Some(TaskJoinOutcome::Completed(()) | TaskJoinOutcome::Aborted) => {}
            Some(TaskJoinOutcome::Failed(_) | TaskJoinOutcome::Incomplete) => {
                return Err(PredictWalletWsError::Task);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_shutdown_retains_the_owned_task_for_a_later_drain() {
        let mut task = TaskSlot::new();
        task.spawn(std::future::pending::<()>()).unwrap();
        let (_state_tx, state) = watch::channel(PredictWalletSubscriptionState::Subscribing {
            connection_epoch: 0,
        });
        let (_out_tx, output) = mpsc::channel(1);
        let mut client = PredictWalletWebSocketClient {
            cancellation: CancellationToken::new(),
            task,
            state,
            output,
            auth_tracker: AuthTracker::new(),
            connected: Arc::new(AtomicBool::new(false)),
            epoch: Arc::new(AtomicU64::new(0)),
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(10), client.close())
                .await
                .is_err()
        );
        assert!(
            client.task.is_some(),
            "cancelled close must not detach the task"
        );
        client.close().await.unwrap();
        assert!(client.task.is_none());
    }
}
