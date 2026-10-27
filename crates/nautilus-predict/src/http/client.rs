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

use std::{
    collections::{HashMap, HashSet},
    fmt,
};

use anyhow::Context;
use nautilus_network::http::{
    HttpClient, HttpRedirectPolicy, Method, Url, create_standard_nautilus_headers,
};

use serde::{Serialize, de::DeserializeOwned};

use super::models::{PredictListResponse, PredictMarket, PredictMarketsResponse, PredictResponse};

const DEFAULT_HTTP_URL: &str = "https://api.predict.fun";

/// Public market-discovery client using Nautilus HTTP transport.
#[derive(Clone)]
pub struct PredictHttpClient {
    base_url: String,
    client: HttpClient,
}

impl fmt::Debug for PredictHttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Native transport Debug includes its default headers.
        f.debug_struct("PredictHttpClient")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl PredictHttpClient {
    /// Creates a client with an API key, optional origin and per-request timeout.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid credentials, origin, timeout or transport setup.
    pub fn new(api_key: &str, base_url: Option<&str>, timeout_secs: u64) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !api_key.is_empty() && api_key.bytes().all(|byte| byte.is_ascii_graphic()),
            "Predict API key must be a nonempty printable ASCII header value"
        );
        anyhow::ensure!(timeout_secs > 0, "HTTP timeout must be positive");
        let url = super::validated_http_origin(base_url.unwrap_or(DEFAULT_HTTP_URL))
            .map_err(anyhow::Error::msg)?;
        let mut headers: HashMap<String, String> =
            create_standard_nautilus_headers().into_iter().collect();
        headers.insert("x-api-key".to_string(), api_key.to_string());
        let client = HttpClient::builder()
            .headers(headers)
            .redirect_policy(HttpRedirectPolicy::Reject)
            .timeout_secs(timeout_secs)
            .build()
            .context("failed to build Predict HTTP client")?;
        Ok(Self {
            base_url: url.as_str().trim_end_matches('/').to_string(),
            client,
        })
    }

    async fn get<P: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        params: Option<&P>,
    ) -> anyhow::Result<T> {
        let response = self
            .client
            .request_with_params(
                Method::GET,
                format!("{}{path}", self.base_url),
                params,
                None,
                None,
                None,
                None,
            )
            .await
            .with_context(|| format!("Predict GET {path} failed"))?;
        anyhow::ensure!(
            response.status.is_success(),
            "Predict GET {path} returned HTTP {}",
            response.status.as_u16()
        );
        serde_json::from_slice(&response.body)
            .with_context(|| format!("failed to decode Predict GET {path}"))
    }

    /// Fetches a market and validates its returned identity.
    ///
    /// # Errors
    ///
    /// Returns an error for request, decoding or market-identity failures.
    pub async fn request_market(&self, market_id: u64) -> anyhow::Result<PredictMarket> {
        let payload: PredictResponse<PredictMarket> = self
            .get(&format!("/v1/markets/{market_id}"), None::<&()>)
            .await?;
        anyhow::ensure!(
            payload.success,
            "Predict market response reported success=false"
        );
        anyhow::ensure!(
            payload.data.id == market_id,
            "Predict response market ID {} did not match requested {market_id}",
            payload.data.id
        );
        Ok(payload.data)
    }

    /// Fetches the open-market catalogue in at most 100 cursor pages.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid responses, repeated cursors or the page limit.
    pub async fn request_open_markets(&self) -> anyhow::Result<Vec<PredictMarket>> {
        #[derive(Serialize)]
        struct Query<'a> {
            first: u32,
            status: &'static str,
            after: &'a str,
        }
        const MAX_PAGES: usize = 100;
        let mut cursor: Option<String> = None;
        let mut seen = HashSet::new();
        let mut markets = Vec::new();
        for _ in 0..MAX_PAGES {
            let query = Query {
                first: 100,
                status: "OPEN",
                after: cursor.as_deref().unwrap_or_default(),
            };
            let payload: PredictListResponse<PredictMarket> =
                self.get("/v1/markets", Some(&query)).await?;
            anyhow::ensure!(
                payload.success,
                "Predict market discovery reported success=false"
            );
            markets.extend(payload.data);
            let Some(next) = payload.cursor.filter(|value| !value.is_empty()) else {
                return Ok(markets);
            };
            anyhow::ensure!(
                seen.insert(next.clone()),
                "Predict market discovery repeated a cursor"
            );
            cursor = Some(next);
        }
        anyhow::bail!("Predict market discovery exceeded {MAX_PAGES} pages")
    }

    /// Searches markets through Predict's relevance-ranked search endpoint.
    /// The result limit is clamped to 1..=100.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty query, request failure or invalid response.
    pub async fn request_search_markets(
        &self,
        query: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<PredictMarket>> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Query<'a> {
            query: &'a str,
            include_resolved: bool,
            limit: usize,
        }
        let query = query.trim();
        anyhow::ensure!(!query.is_empty(), "Predict market search query is empty");
        let query = Query {
            query,
            include_resolved: false,
            limit: limit.clamp(1, 100),
        };
        let payload: PredictMarketsResponse = self.get("/v1/search", Some(&query)).await?;
        anyhow::ensure!(
            payload.success,
            "Predict market search reported success=false"
        );
        Ok(payload.data.markets)
    }

    /// Fetches category markets, encoding the slug as one path segment.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty slug, request failure or invalid response.
    pub async fn request_category_markets(&self, slug: &str) -> anyhow::Result<Vec<PredictMarket>> {
        let slug = slug.trim();
        anyhow::ensure!(!slug.is_empty(), "Predict category slug is empty");
        let mut url = Url::parse(&format!("{}/v1/categories/", self.base_url))?;
        url.path_segments_mut()
            .expect("validated hierarchical HTTP origin")
            .pop_if_empty()
            .push(slug);
        let payload: PredictMarketsResponse = self.get(url.path(), None::<&()>).await?;
        anyhow::ensure!(
            payload.success,
            "Predict category response reported success=false"
        );
        Ok(payload.data.markets)
    }
}
