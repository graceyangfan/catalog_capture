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

use nautilus_network::websocket::TransportBackend;
use serde::{Deserialize, Serialize};

/// Public mainnet binary transport and initial instrument configuration.
/// Empty initial IDs allow lazy discovery through Native instrument requests.
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
pub struct LighterBinaryDataClientConfig {
    #[builder(default)]
    pub market_ids: Vec<u64>,
    pub base_url_http: Option<String>,
    pub base_url_ws: Option<String>,
    pub proxy_url: Option<String>,
    #[builder(default = 10)]
    pub http_timeout_secs: u64,
    #[builder(default = 10)]
    pub ws_timeout_secs: u64,
    #[builder(default)]
    pub transport_backend: TransportBackend,
}
impl Default for LighterBinaryDataClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}
impl LighterBinaryDataClientConfig {
    /// Keep the worst-case four physical subscriptions per market below the upstream
    /// subscribe inflight bound. Rolling request limits are also enforced by discovery.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.http_timeout_secs > 0 && self.ws_timeout_secs > 0,
            "Lighter binary transport timeouts must be positive"
        );
        let max = nautilus_lighter::common::consts::SUBSCRIBE_INFLIGHT_MAX / 4;
        anyhow::ensure!(
            self.market_ids.len() <= max,
            "binary capture supports at most {max} initial market IDs"
        );
        let unique = self
            .market_ids
            .iter()
            .collect::<std::collections::BTreeSet<_>>();
        anyhow::ensure!(
            unique.len() == self.market_ids.len(),
            "duplicate fixed binary market IDs"
        );
        Ok(())
    }

    pub fn http_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| "https://mainnet.zklighter.elliot.ai/api/v1".into())
    }
    pub fn ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| "wss://mainnet.zklighter.elliot.ai/stream".into())
    }
}
