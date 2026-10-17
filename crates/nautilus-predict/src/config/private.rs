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

//! Private HTTP configuration, separate from capture configuration.

use nautilus_core::string::secret::SecretString;
use nautilus_network::http::Url;

use crate::{
    common::{credential::validate_header_secret, environment::PredictEnvironment},
    http::error::{Error, Result},
};

/// Explicit network identity for private authentication. URL overrides preserve this identity.
#[derive(Clone, Debug)]
pub struct PredictPrivateClientConfig {
    /// Environment used by both transport and signer.
    pub environment: PredictEnvironment,
    /// Mainnet API key, supplied separately from public capture TOML.
    pub api_key: Option<SecretString>,
    /// Optional HTTPS origin or loopback HTTP origin for local fixtures.
    pub base_url_http: Option<String>,
    /// Positive HTTP timeout in seconds.
    pub http_timeout_secs: u64,
}

impl Default for PredictPrivateClientConfig {
    fn default() -> Self {
        Self {
            environment: PredictEnvironment::BnbMainnet,
            api_key: None,
            base_url_http: None,
            http_timeout_secs: 30,
        }
    }
}

impl PredictPrivateClientConfig {
    pub(crate) fn validated_http_url(&self) -> Result<String> {
        if self.environment == PredictEnvironment::BnbMainnet && self.api_key.is_none() {
            return Err(Error::Configuration("Predict mainnet requires an API key"));
        }
        if let Some(key) = &self.api_key {
            validate_header_secret(key)?;
        }
        if self.http_timeout_secs == 0 {
            return Err(Error::Configuration("HTTP timeout must be positive"));
        }
        let url = Url::parse(
            self.base_url_http
                .as_deref()
                .unwrap_or(self.environment.http_url()),
        )
        .map_err(|_| Error::Configuration("invalid private HTTP base URL"))?;
        let host = url
            .host_str()
            .ok_or(Error::Configuration("HTTP base URL requires a host"))?;
        let loopback = host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback());
        if !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
        {
            return Err(Error::Configuration(
                "private HTTP URL must be an HTTPS origin without credentials, query or fragment",
            ));
        }
        let official_environment = match host {
            "api.predict.fun" => Some(PredictEnvironment::BnbMainnet),
            "api-testnet.predict.fun" => Some(PredictEnvironment::BnbTestnet),
            _ => None,
        };
        if official_environment.is_some_and(|environment| environment != self.environment) {
            return Err(Error::Configuration(
                "REST origin differs from configured Predict environment",
            ));
        }
        Ok(url.as_str().trim_end_matches('/').to_string())
    }
}
