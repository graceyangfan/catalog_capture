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

//! Connection policy and bounded epoch-aware ingress; no recording or market state.

use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use nautilus_network::{
    http::create_standard_nautilus_headers,
    transport::Message,
    websocket::{EpochMessageHandler, InitialConnectRetryPolicy, WebSocketClient, WebSocketConfig},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::PredictDataClientConfig;

const RAW_QUEUE_CAPACITY: usize = 1_024;
pub(crate) const COMMAND_QUEUE_CAPACITY: usize = 128;
const MAX_MESSAGE_SIZE_BYTES: usize = 2 * 1024 * 1024;

pub(crate) struct PredictWebSocketConnection {
    pub(crate) client: WebSocketClient,
    pub(crate) raw_rx: mpsc::Receiver<(u64, Message)>,
    pub(crate) overflowed: Arc<AtomicBool>,
}

impl PredictWebSocketConnection {
    pub(crate) async fn connect(
        config: &PredictDataClientConfig,
        cancellation: CancellationToken,
    ) -> anyhow::Result<Self> {
        let (raw_tx, raw_rx) = mpsc::channel(RAW_QUEUE_CAPACITY);
        let overflowed = std::sync::Arc::new(AtomicBool::new(false));
        let overflowed_handler = std::sync::Arc::clone(&overflowed);
        let epoch_handler: EpochMessageHandler = std::sync::Arc::new(move |epoch, message| {
            if let Err(error) = raw_tx.try_send((epoch, message))
                && matches!(error, mpsc::error::TrySendError::Full(_))
            {
                overflowed_handler.store(true, Ordering::Release);
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
        Ok(Self {
            client,
            raw_rx,
            overflowed,
        })
    }
}
