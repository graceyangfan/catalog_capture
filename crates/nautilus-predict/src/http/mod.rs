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

//! REST transport, authentication, and venue response models.

#[cfg(feature = "execution")]
pub mod account_models;
#[cfg(feature = "live")]
pub mod client;
#[cfg(feature = "execution")]
mod collect;
#[cfg(feature = "execution")]
pub mod command_models;
#[cfg(feature = "execution")]
pub mod error;
#[cfg(feature = "execution")]
pub mod execution_models;
#[cfg(feature = "execution")]
pub mod market_models;
pub mod models;
#[cfg(feature = "execution")]
pub mod order_models;
#[cfg(feature = "execution")]
pub mod parse;
#[cfg(feature = "execution")]
pub mod private_client;
#[cfg(feature = "execution")]
pub mod query;
#[cfg(feature = "live")]
pub use client::PredictHttpClient;
#[cfg(feature = "execution")]
pub use private_client::{PredictAuthenticatedHttpClient, PredictPrivateHttpClient};

/// Validates credential-bearing REST origins before any request is constructed.
#[cfg(feature = "live")]
pub(crate) fn validated_http_origin(
    raw: &str,
) -> Result<nautilus_network::http::Url, &'static str> {
    use nautilus_network::http::Url;

    let url = Url::parse(raw).map_err(|_| "invalid HTTP base URL")?;
    let host = url.host_str().ok_or("HTTP base URL requires a host")?;
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
        return Err(
            "HTTP URL must be an HTTPS origin without credentials, path, query or fragment",
        );
    }
    Ok(url)
}
