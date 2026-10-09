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

//! Configuration for the public Predict orderbook data client.

use nautilus_core::string::secret::SecretString;
use nautilus_network::websocket::TransportBackend;

/// Runtime-only client configuration. The API key is deliberately supplied by the CLI environment,
/// not by capture TOML.
#[derive(Debug, Clone)]
pub struct PredictDataClientConfig {
    pub api_key: SecretString,
    /// Explicit market IDs that must be resolved into instruments before subscriptions begin.
    pub market_ids: Vec<u64>,
    pub base_url_http: Option<String>,
    pub base_url_ws: Option<String>,
    pub http_timeout_secs: u64,
    pub ws_timeout_secs: u64,
    /// Discovery cadence for configured rolling crypto up/down product selectors.
    pub dynamic_refresh_secs: u64,
    pub transport_backend: TransportBackend,
}

impl PredictDataClientConfig {
    #[must_use]
    pub fn ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| "wss://ws.predict.fun/ws".to_string())
    }
}
