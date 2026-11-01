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

//! Bounded public sports catalogue discovery using the existing credential-bearing client.
use super::{client::PredictHttpClient, models::PredictListResponse};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictSportsCategory {
    pub id: u64,
    pub slug: String,
    pub title: String,
    pub status: String,
    pub starts_at: Option<String>,
    #[serde(default)]
    pub tags: Vec<PredictSportsTag>,
    #[serde(default)]
    pub teams: Option<Vec<PredictSportsTeam>>,
    #[serde(default)]
    pub markets: Vec<PredictSportsMarket>,
    pub stats: Option<serde_json::Value>,
    pub variant_details: Option<serde_json::Value>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PredictSportsTag {
    pub id: String,
    pub name: String,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PredictSportsTeam {
    pub name: String,
    pub abbreviation: Option<String>,
    pub alias: Option<String>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictSportsOutcome {
    pub name: String,
    pub index_set: u8,
    pub on_chain_id: String,
    pub team: Option<PredictSportsTeam>,
    pub variant_details: Option<serde_json::Value>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictSportsMarket {
    pub id: u64,
    pub question: Option<String>,
    pub description: String,
    pub trading_status: String,
    pub market_type: Option<String>,
    #[serde(default)]
    pub polymarket_condition_ids: Vec<String>,
    pub outcomes: Vec<PredictSportsOutcome>,
    pub variant_details: Option<serde_json::Value>,
    pub rewards: Option<serde_json::Value>,
    pub created_at: Option<String>,
}
impl PredictHttpClient {
    /// A bounded admission shortlist, never an authoritative catalogue of open markets.
    pub async fn request_sports_activity_ids(
        &self,
        tag_id: u64,
        eligible_ids: &BTreeSet<u64>,
        max_pages: usize,
        max_markets: usize,
    ) -> Result<SportsActivityIds> {
        anyhow::ensure!(
            tag_id > 0 && (1..=20).contains(&max_pages) && (1..=1024).contains(&max_markets),
            "invalid sports activity shortlist budget"
        );
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Query<'a> {
            first: usize,
            status: &'static str,
            sort: &'static str,
            tag_ids: u64,
            #[serde(skip_serializing_if = "Option::is_none")]
            after: Option<&'a str>,
        }
        #[derive(Deserialize)]
        struct MarketId {
            id: u64,
        }
        let mut result = SportsActivityIds::default();
        if eligible_ids.is_empty() {
            return Ok(result);
        }
        let mut cursor = None;
        let mut cursors = HashSet::new();
        let mut markets = HashSet::new();
        for _ in 0..max_pages {
            let page: PredictListResponse<MarketId> = self
                .get(
                    "/v1/markets",
                    Some(&Query {
                        first: 50,
                        status: "OPEN",
                        sort: "VOLUME_24H_DESC",
                        tag_ids: tag_id,
                        after: cursor.as_deref(),
                    }),
                )
                .await?;
            anyhow::ensure!(
                page.success && page.data.len() <= 50,
                "invalid Predict activity shortlist page"
            );
            result.pages += 1;
            result.has_more = false;
            for row in page.data {
                anyhow::ensure!(markets.insert(row.id), "duplicate Predict activity market");
                if eligible_ids.contains(&row.id) && result.ids.len() < max_markets {
                    result.ids.push(row.id);
                } else if eligible_ids.contains(&row.id) {
                    result.has_more = true;
                }
            }
            cursor = page.cursor.filter(|v| !v.is_empty());
            result.has_more |= cursor.is_some();
            let Some(next) = cursor.as_ref() else {
                return Ok(result);
            };
            anyhow::ensure!(
                cursors.insert(next.clone()),
                "Predict activity shortlist repeated cursor"
            );
            if result.ids.len() == max_markets {
                result.has_more = true;
                return Ok(result);
            }
        }
        Ok(result)
    }

    /// Returns a complete OPEN tag catalogue, or an error. Partial pages never look complete.
    pub async fn request_sports_categories(
        &self,
        tag_id: u64,
        max_pages: usize,
    ) -> Result<Vec<PredictSportsCategory>> {
        anyhow::ensure!(
            (1..=100).contains(&max_pages),
            "invalid sports catalogue page budget"
        );
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Query<'a> {
            first: u32,
            status: &'static str,
            tag_ids: u64,
            #[serde(skip_serializing_if = "Option::is_none")]
            after: Option<&'a str>,
        }
        let mut cursor = String::new();
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        for _ in 0..max_pages {
            let page: PredictListResponse<PredictSportsCategory> = self
                .get(
                    "/v1/categories",
                    Some(&Query {
                        first: 100,
                        status: "OPEN",
                        tag_ids: tag_id,
                        after: (!cursor.is_empty()).then_some(cursor.as_str()),
                    }),
                )
                .await?;
            anyhow::ensure!(page.success, "Predict sports catalogue success=false");
            result.extend(page.data);
            match page.cursor.filter(|v| !v.is_empty()) {
                None => return Ok(result),
                Some(next) => {
                    anyhow::ensure!(
                        seen.insert(next.clone()),
                        "Predict sports catalogue repeated cursor"
                    );
                    cursor = next;
                }
            }
        }
        anyhow::bail!("Predict sports catalogue exceeded page budget; refresh is incomplete")
    }
}

#[derive(Debug, Default)]
pub struct SportsActivityIds {
    pub ids: Vec<u64>,
    pub pages: usize,
    pub has_more: bool,
}

/// Reported dollar statistics used to prioritize recording, not participant counts.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictSportsMarketStats {
    pub volume24h_usd: rust_decimal::Decimal,
    pub volume_total_usd: Option<rust_decimal::Decimal>,
    pub total_liquidity_usd: rust_decimal::Decimal,
}
impl PredictHttpClient {
    pub async fn request_sports_stats(&self, market_id: u64) -> Result<PredictSportsMarketStats> {
        let payload: super::models::PredictResponse<PredictSportsMarketStats> = self
            .get(&format!("/v1/markets/{market_id}/stats"), None::<&()>)
            .await?;
        anyhow::ensure!(
            payload.success
                && payload.data.volume24h_usd >= rust_decimal::Decimal::ZERO
                && payload.data.total_liquidity_usd >= rust_decimal::Decimal::ZERO
                && payload
                    .data
                    .volume_total_usd
                    .is_none_or(|v| v >= rust_decimal::Decimal::ZERO),
            "invalid Predict activity statistics"
        );
        Ok(payload.data)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictSportsBook {
    pub market_id: u64,
    pub bids: Vec<[rust_decimal::Decimal; 2]>,
    pub asks: Vec<[rust_decimal::Decimal; 2]>,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SportsMatchTime {
    market: SportsMatchMarket,
    executed_at: String,
}
#[derive(Debug, Deserialize)]
struct SportsMatchMarket {
    id: u64,
}
#[derive(Debug)]
pub struct SportsRecentTrades {
    pub timestamps: Vec<u64>,
    pub has_more: bool,
}
impl PredictHttpClient {
    pub async fn request_sports_book(&self, market_id: u64) -> Result<PredictSportsBook> {
        let response: super::models::PredictResponse<PredictSportsBook> = self
            .get(&format!("/v1/markets/{market_id}/orderbook"), None::<&()>)
            .await?;
        anyhow::ensure!(
            response.success && response.data.market_id == market_id,
            "invalid Predict sports orderbook envelope or identity"
        );
        Ok(response.data)
    }
    /// One bounded public market page; no private signer filter or execution feature.
    pub async fn request_sports_recent_trades(&self, market_id: u64) -> Result<SportsRecentTrades> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Query {
            market_id: u64,
            first: usize,
        }
        let response: PredictListResponse<SportsMatchTime> = self
            .get(
                "/v1/orders/matches",
                Some(&Query {
                    market_id,
                    first: 50,
                }),
            )
            .await?;
        anyhow::ensure!(
            response.success && response.data.len() <= 50,
            "invalid Predict match page"
        );
        let mut timestamps = Vec::new();
        for row in response.data {
            anyhow::ensure!(
                row.market.id == market_id,
                "Predict ignored marketId on match page"
            );
            let time = chrono::DateTime::parse_from_rfc3339(&row.executed_at)?;
            timestamps.push(u64::try_from(time.timestamp())?);
        }
        anyhow::ensure!(
            timestamps.windows(2).all(|w| w[0] >= w[1]),
            "unsorted Predict matches"
        );
        Ok(SportsRecentTrades {
            timestamps,
            has_more: response.cursor.is_some_and(|v| !v.is_empty()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, http::Uri, routing::get};
    use serde_json::json;
    #[tokio::test]
    async fn activity_shortlist_filters_types_by_identity_and_reports_truncation() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().route(
            "/v1/markets",
            get(|uri: Uri| async move {
                let url =
                    nautilus_network::http::Url::parse(&format!("http://localhost{uri}")).unwrap();
                let q: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
                assert_eq!(q["first"], "50");
                assert_eq!(q["status"], "OPEN");
                assert_eq!(q["sort"], "VOLUME_24H_DESC");
                if q["tagIds"] == "96" {
                    json!({"success":true,"data":[],"cursor":"loop"}).to_string()
                } else if q["tagIds"] == "132" {
                    json!({"success":true,"data":[{"id":42},{"id":42}],"cursor":null}).to_string()
                } else if q.contains_key("after") {
                    assert_eq!(q["after"], "next");
                    json!({"success":true,"data":[{"id":44}],"cursor":null}).to_string()
                } else {
                    json!({"success":true,"data":[{"id":99},{"id":42},{"id":43}],"cursor":"next"})
                        .to_string()
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let http = PredictHttpClient::new("test-key", Some(&origin), 2).unwrap();
        let eligible = BTreeSet::from([42, 43, 44]);
        let capped = http
            .request_sports_activity_ids(93, &eligible, 2, 2)
            .await
            .unwrap();
        assert_eq!(capped.ids, vec![42, 43]);
        assert_eq!(capped.pages, 1);
        assert!(capped.has_more);
        let paged = http
            .request_sports_activity_ids(93, &eligible, 1, 4)
            .await
            .unwrap();
        assert_eq!(paged.ids, vec![42, 43]);
        assert!(paged.has_more);
        let complete = http
            .request_sports_activity_ids(93, &eligible, 2, 4)
            .await
            .unwrap();
        assert_eq!(complete.ids, vec![42, 43, 44]);
        assert_eq!(complete.pages, 2);
        assert!(!complete.has_more);
        assert!(
            http.request_sports_activity_ids(96, &eligible, 3, 4)
                .await
                .is_err()
        );
        assert!(
            http.request_sports_activity_ids(132, &eligible, 2, 4)
                .await
                .is_err()
        );
        server.abort();
    }
    async fn catalogue(uri: Uri) -> String {
        let url = nautilus_network::http::Url::parse(&format!("http://localhost{uri}")).unwrap();
        let q: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(q["status"], "OPEN");
        let cursor = if q["tagIds"] == "84" && q.get("after").is_some_and(|v| !v.is_empty()) {
            None
        } else {
            Some("next-page")
        };
        json!({"success":true,"data":[],"cursor":cursor}).to_string()
    }
    #[tokio::test]
    async fn bounded_pagination_rejects_incomplete_and_looping_refreshes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().route("/v1/categories", get(catalogue)).route(
            "/v1/markets/42/stats",
            get(|| async {
                json!({"success":true,"data":{"volume24hUsd":1234.56,"volumeTotalUsd":2000.01,"totalLiquidityUsd":7890.12}})
                    .to_string()
            }),
        );
        let app = app.route(
            "/v1/markets/43/stats",
            get(|| async {
                json!({"success":true,"data":{"volume24hUsd":1,"totalLiquidityUsd":-1}}).to_string()
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = PredictHttpClient::new("test-key", Some(&url), 2).unwrap();
        assert!(
            client
                .request_sports_categories(84, 2)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(client.request_sports_categories(84, 1).await.is_err());
        assert!(
            client
                .request_sports_categories(132, 4)
                .await
                .unwrap_err()
                .to_string()
                .contains("repeated cursor")
        );
        assert_eq!(
            client
                .request_sports_stats(42)
                .await
                .unwrap()
                .total_liquidity_usd
                .to_string(),
            "7890.12"
        );
        assert_eq!(
            client
                .request_sports_stats(42)
                .await
                .unwrap()
                .volume_total_usd,
            Some("2000.01".parse().unwrap())
        );
        assert!(
            client.request_sports_stats(43).await.is_err(),
            "negative liquidity must not enter ranking"
        );
        server.abort();
    }
}
