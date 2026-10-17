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

//! Explicit, read-only RPC configuration. Provider URLs may contain secret path/query tokens.

use nautilus_core::string::secret::SecretString;
use nautilus_network::http::Url;

use crate::common::environment::PredictEnvironment;

#[derive(Clone, Debug)]
pub struct PredictChainClientConfig {
    pub environment: PredictEnvironment,
    pub rpc_url: SecretString,
    pub timeout_secs: u64,
}

impl PredictChainClientConfig {
    /// Creates explicit RPC settings without reading credentials or selecting a public provider.
    #[must_use]
    pub fn new(environment: PredictEnvironment, rpc_url: SecretString) -> Self {
        Self {
            environment,
            rpc_url,
            timeout_secs: 30,
        }
    }

    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=300).contains(&self.timeout_secs),
            "invalid Predict RPC timeout"
        );
        let url = Url::parse(self.rpc_url.expose_secret())
            .map_err(|_| anyhow::anyhow!("invalid Predict RPC URL"))?;
        let host = url
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("Predict RPC URL requires a host"))?;
        let loopback = host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback());
        anyhow::ensure!(
            (url.scheme() == "https" || (url.scheme() == "http" && loopback))
                && url.username().is_empty()
                && url.password().is_none()
                && url.fragment().is_none(),
            "Predict RPC requires HTTPS or loopback HTTP, without user info or fragment"
        );
        Ok(())
    }
}
