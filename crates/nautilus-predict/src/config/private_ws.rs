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

//! Private wallet WebSocket configuration, independent of public capture.

use std::fmt;

use nautilus_core::string::secret::SecretString;
use nautilus_network::{http::Url, websocket::TransportBackend};

use crate::{
    common::{credential::validate_header_secret, environment::PredictEnvironment},
    websocket::error::{PredictWalletWsError, Result},
};

#[derive(Clone)]
pub struct PredictWalletWebSocketConfig {
    pub environment: PredictEnvironment,
    /// The handshake requires an API key independently of the wallet JWT.
    pub api_key: Option<SecretString>,
    /// Only the mainnet endpoint is documented; testnet requires an explicit override.
    pub base_url_ws: Option<String>,
    pub connect_timeout_secs: u64,
    pub subscription_timeout_secs: u64,
    /// Both pre-ACK events and consumer queues are bounded. Overflow terminates the session.
    pub queue_capacity: usize,
    pub transport_backend: TransportBackend,
}

impl Default for PredictWalletWebSocketConfig {
    fn default() -> Self {
        Self {
            environment: PredictEnvironment::BnbMainnet,
            api_key: None,
            base_url_ws: None,
            connect_timeout_secs: 10,
            subscription_timeout_secs: 10,
            queue_capacity: 1024,
            transport_backend: TransportBackend::default(),
        }
    }
}

impl fmt::Debug for PredictWalletWebSocketConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PredictWalletWebSocketConfig")
            .field("environment", &self.environment)
            .field("api_key", &self.api_key)
            .field(
                "base_url_ws",
                &self.base_url_ws.as_ref().map(|_| "<custom endpoint>"),
            )
            .field("connect_timeout_secs", &self.connect_timeout_secs)
            .field("subscription_timeout_secs", &self.subscription_timeout_secs)
            .field("queue_capacity", &self.queue_capacity)
            .field("transport_backend", &self.transport_backend)
            .finish()
    }
}

impl PredictWalletWebSocketConfig {
    pub(crate) fn validated_ws_url(&self) -> Result<String> {
        let key = self
            .api_key
            .as_ref()
            .ok_or(PredictWalletWsError::Configuration(
                "wallet WebSocket handshake requires an API key",
            ))?;
        validate_header_secret(key)
            .map_err(|_| PredictWalletWsError::Configuration("invalid WebSocket API key"))?;
        if self.connect_timeout_secs == 0
            || self.subscription_timeout_secs == 0
            || self.connect_timeout_secs > 300
            || self.subscription_timeout_secs > 300
            || self.queue_capacity == 0
            || self.queue_capacity > 65536
        {
            return Err(PredictWalletWsError::Configuration(
                "timeouts or queue capacity exceed supported bounds",
            ));
        }
        let default = match self.environment {
            PredictEnvironment::BnbMainnet => Some("wss://ws.predict.fun/ws"),
            PredictEnvironment::BnbTestnet => None,
        };
        let value =
            self.base_url_ws
                .as_deref()
                .or(default)
                .ok_or(PredictWalletWsError::Configuration(
                    "testnet WebSocket requires an explicit endpoint",
                ))?;
        let url = Url::parse(value)
            .map_err(|_| PredictWalletWsError::Configuration("invalid wallet WebSocket URL"))?;
        let host = url.host_str().ok_or(PredictWalletWsError::Configuration(
            "WebSocket URL requires a host",
        ))?;
        let loopback = host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback());
        if !(url.scheme() == "wss" || (url.scheme() == "ws" && loopback))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/ws"
        {
            return Err(PredictWalletWsError::Configuration(
                "wallet URL must use WSS or loopback WS at /ws without credentials, query or fragment",
            ));
        }
        if host == "ws.predict.fun" && self.environment != PredictEnvironment::BnbMainnet {
            return Err(PredictWalletWsError::Configuration(
                "WebSocket origin differs from configured environment",
            ));
        }
        Ok(url.to_string())
    }
}
