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

use std::collections::HashMap;

use anyhow::Context;
use nautilus_network::http::{
    HttpClient, HttpRedirectPolicy, Method, create_standard_nautilus_headers,
};

use crate::models::{
    PredictCategoryResponse, PredictListResponse, PredictMarket, PredictResponse,
    PredictSearchResponse,
};

const DEFAULT_HTTP_URL: &str = "https://api.predict.fun";

#[derive(Debug, Clone)]
pub struct PredictHttpClient {
    base_url: String,
    client: HttpClient,
}

impl PredictHttpClient {
    pub fn new(api_key: &str, base_url: Option<&str>, timeout_secs: u64) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !api_key.trim().is_empty(),
            "Predict API key is required for REST market metadata"
        );
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
            base_url: base_url
                .unwrap_or(DEFAULT_HTTP_URL)
                .trim_end_matches('/')
                .to_string(),
            client,
        })
    }

    pub async fn request_market(&self, market_id: u64) -> anyhow::Result<PredictMarket> {
        let response = self
            .client
            .request(
                Method::GET,
                format!("{}/v1/markets/{market_id}", self.base_url),
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .context("Predict market request failed")?;
        anyhow::ensure!(
            response.status.is_success(),
            "Predict market request returned HTTP {}",
            response.status.as_u16()
        );
        let payload: PredictResponse<PredictMarket> = serde_json::from_slice(&response.body)
            .context("failed to decode Predict market response")?;
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

    /// Fetches open markets through Predict's documented paginated discovery endpoint.
    pub async fn request_open_markets(&self) -> anyhow::Result<Vec<PredictMarket>> {
        const PAGE_SIZE: usize = 100;
        // Predict lists all open markets, including sports and other products. Five
        // pages silently became insufficient as the catalogue grew and prevented
        // the typed crypto selector from ever reaching its BTC market. Keep a
        // generous guard against a broken cursor while allowing the documented
        // pagination to complete in normal operation.
        const MAX_PAGES: usize = 100;
        let mut cursor: Option<String> = None;
        let mut markets = Vec::new();
        for _ in 0..MAX_PAGES {
            let after = cursor.as_deref().unwrap_or_default();
            let response = self
                .client
                .request(
                    Method::GET,
                    format!(
                        "{}/v1/markets?first={PAGE_SIZE}&status=OPEN&after={after}",
                        self.base_url
                    ),
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .context("Predict open-market discovery request failed")?;
            anyhow::ensure!(
                response.status.is_success(),
                "Predict open-market discovery returned HTTP {}",
                response.status.as_u16()
            );
            let payload: PredictListResponse<PredictMarket> =
                serde_json::from_slice(&response.body)
                    .context("failed to decode Predict open-market discovery response")?;
            anyhow::ensure!(payload.success, "Predict market discovery reported success=false");
            markets.extend(payload.data);
            let next = payload.cursor.filter(|value| !value.is_empty());
            if next.is_none() {
                return Ok(markets);
            }
            cursor = next;
        }
        anyhow::bail!("Predict market discovery exceeded {MAX_PAGES} pages")
    }

    /// Searches markets through Predict's relevance-ranked search endpoint.
    ///
    /// Dynamic crypto selectors use this endpoint because `/v1/markets` is a
    /// global catalogue containing many unrelated open sports markets. Search
    /// keeps discovery bounded and avoids scanning that catalogue at every
    /// rolling-market boundary.
    pub async fn request_search_markets(
        &self,
        query: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<PredictMarket>> {
        let query = query.trim();
        anyhow::ensure!(!query.is_empty(), "Predict market search query is empty");
        let limit = limit.clamp(1, 100);
        let response = self
            .client
            .request(
                Method::GET,
                format!(
                    "{}/v1/search?query={}&includeResolved=false&limit={limit}",
                    self.base_url,
                    encode_query_component(query),
                ),
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .context("Predict market search request failed")?;
        anyhow::ensure!(
            response.status.is_success(),
            "Predict market search returned HTTP {}",
            response.status.as_u16()
        );
        let payload: PredictSearchResponse = serde_json::from_slice(&response.body)
            .context("failed to decode Predict market search response")?;
        anyhow::ensure!(payload.success, "Predict market search reported success=false");
        Ok(payload.data.markets)
    }

    /// Fetches the markets belonging to one exact Predict category slug.
    /// Rolling crypto products expose one category per time window, so this is
    /// the preferred path when the selector can derive the current slug.
    pub async fn request_category_markets(&self, slug: &str) -> anyhow::Result<Vec<PredictMarket>> {
        let slug = slug.trim();
        anyhow::ensure!(!slug.is_empty(), "Predict category slug is empty");
        let response = self
            .client
            .request(
                Method::GET,
                format!("{}/v1/categories/{}", self.base_url, encode_query_component(slug)),
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .context("Predict category request failed")?;
        anyhow::ensure!(
            response.status.is_success(),
            "Predict category request returned HTTP {}",
            response.status.as_u16()
        );
        let payload: PredictCategoryResponse = serde_json::from_slice(&response.body)
            .context("failed to decode Predict category response")?;
        anyhow::ensure!(payload.success, "Predict category response reported success=false");
        Ok(payload.data.markets)
    }
}

fn encode_query_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(char::from_digit((byte >> 4) as u32, 16).unwrap());
            encoded.push(char::from_digit((byte & 0x0f) as u32, 16).unwrap());
        }
    }
    encoded
}
