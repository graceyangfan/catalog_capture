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

//! Live test selection only. Market identity and terms never come from operator configuration.
use std::collections::{HashMap, HashSet};

use nautilus_network::http::{HttpClient, HttpRedirectPolicy, Method};
use nautilus_predict::http::market_models::PredictExecutionMarket;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};

use super::settings::PredictExecutionSettings;

#[derive(Clone, Copy, Debug)]
pub enum Phase {
    Resting,
    Fill,
}
#[derive(Clone, Debug)]
pub struct Candidate {
    pub market: PredictExecutionMarket,
    pub outcome: u8,
    pub price: Decimal,
    pub quantity: Decimal,
    pub best_bid: Decimal,
    pub best_ask: Decimal,
}

#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct Exact(#[serde(deserialize_with = "exact_decimal")] Decimal);
fn exact_decimal<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Decimal, D::Error> {
    let raw = Box::<serde_json::value::RawValue>::deserialize(d)?;
    let text = raw.get();
    let value = if text.starts_with('"') {
        serde_json::from_str::<String>(text).map_err(serde::de::Error::custom)?
    } else {
        text.to_owned()
    };
    Decimal::from_str_exact(&value).map_err(|_| serde::de::Error::custom("invalid exact quote"))
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Book {
    market_id: Option<u64>,
    asks: Vec<(Exact, Exact)>,
    bids: Vec<(Exact, Exact)>,
}
#[derive(Deserialize)]
struct Markets {
    success: bool,
    cursor: Option<String>,
    data: Vec<PredictExecutionMarket>,
}
#[derive(Deserialize)]
struct BookEnvelope {
    success: bool,
    data: Book,
}
#[derive(Serialize)]
struct Query<'a> {
    first: u32,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    after: Option<&'a str>,
}

pub struct PublicApi {
    client: HttpClient,
    origin: &'static str,
}
impl PublicApi {
    pub fn new(settings: &PredictExecutionSettings) -> anyhow::Result<Self> {
        let config = settings.private_config()?;
        let mut headers = HashMap::new();
        if let Some(key) = config.api_key {
            headers.insert("x-api-key".to_string(), key.expose_secret().to_string());
        }
        let client = HttpClient::builder()
            .headers(headers)
            .redirect_policy(HttpRedirectPolicy::Reject)
            .timeout_secs(settings.network.timeout_secs)
            .build()
            .map_err(|_| anyhow::anyhow!("public discovery HTTP setup failed"))?;
        Ok(Self {
            client,
            origin: config.environment.http_url(),
        })
    }
    async fn read<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        params: Option<HashMap<String, String>>,
    ) -> anyhow::Result<T> {
        let response = self
            .client
            .request_with_params(
                Method::GET,
                format!("{}{path}", self.origin),
                params.as_ref(),
                None,
                None,
                None,
                None,
            )
            .await
            .map_err(|_| anyhow::anyhow!("public market discovery request failed"))?;
        anyhow::ensure!(
            response.status.is_success(),
            "public market discovery HTTP {}",
            response.status.as_u16()
        );
        serde_json::from_slice(&response.body)
            .map_err(|_| anyhow::anyhow!("public market discovery schema mismatch"))
    }
    pub async fn discover(
        &self,
        settings: &PredictExecutionSettings,
        phase: Phase,
    ) -> anyhow::Result<Candidate> {
        tokio::time::timeout(
            std::time::Duration::from_secs(settings.test.wait_secs),
            self.discover_bounded(settings, phase),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "market discovery reached its configured deadline; no order was selected"
            )
        })?
    }

    pub async fn confirm_book(&self, candidate: &Candidate) -> anyhow::Result<(Decimal, Decimal)> {
        let reply: BookEnvelope = self
            .read(
                &format!("/v1/markets/{}/orderbook", candidate.market.id),
                None,
            )
            .await?;
        anyhow::ensure!(
            reply.success
                && reply
                    .data
                    .market_id
                    .is_none_or(|id| id == candidate.market.id),
            "REST confirmation identity mismatch"
        );
        let bid = reply
            .data
            .bids
            .iter()
            .filter(|(_, q)| q.0 > Decimal::ZERO)
            .map(|(p, _)| p.0)
            .max();
        let ask = reply
            .data
            .asks
            .iter()
            .filter(|(_, q)| q.0 > Decimal::ZERO)
            .map(|(p, _)| p.0)
            .min();
        let (bid, ask) = match (bid, ask) {
            (Some(b), Some(a)) if b > Decimal::ZERO && b < a && a <= Decimal::ONE => (b, a),
            _ => anyhow::bail!("invalid confirmation book"),
        };
        Ok(if candidate.outcome == 1 {
            (bid, ask)
        } else {
            (Decimal::ONE - ask, Decimal::ONE - bid)
        })
    }

    /// Separate execution-test selector; public recorder discovery is never changed.
    pub async fn discover_quiet_resting(
        &self,
        settings: &PredictExecutionSettings,
    ) -> anyhow::Result<Candidate> {
        self.discover_quiet_resting_outcome(settings, None).await
    }

    pub async fn discover_quiet_resting_outcome(
        &self,
        settings: &PredictExecutionSettings,
        required_outcome: Option<u8>,
    ) -> anyhow::Result<Candidate> {
        anyhow::ensure!(
            required_outcome.is_none_or(|v| v == 1 || v == 2),
            "invalid resting outcome"
        );
        tokio::time::timeout(
            std::time::Duration::from_secs(settings.test.wait_secs),
            async {
                let page: serde_json::Value = self
                    .read(
                        "/v1/markets",
                        Some(HashMap::from([
                            ("first".into(), "100".into()),
                            ("status".into(), "OPEN".into()),
                        ])),
                    )
                    .await?;
                anyhow::ensure!(page["success"] == true, "quiet market query failed");
                let rows = page["data"]
                    .as_array()
                    .ok_or_else(|| anyhow::anyhow!("invalid market list"))?;
                let mut candidates = Vec::new();
                for row in rows
                    .iter()
                    .filter(|row| {
                        row["marketVariant"] != "CRYPTO_UP_DOWN" && row["tradingStatus"] == "OPEN"
                    })
                    .take(12)
                {
                    let market: PredictExecutionMarket = serde_json::from_value(row.clone())?;
                    if !(2..=3).contains(&market.decimal_precision) || market.outcomes.len() != 2 {
                        continue;
                    }
                    let stats: serde_json::Value = self
                        .read(&format!("/v1/markets/{}/stats", market.id), None)
                        .await?;
                    anyhow::ensure!(stats["success"] == true, "quiet market statistics failed");
                    let volume: Exact =
                        serde_json::from_value(stats["data"]["volume24hUsd"].clone())?;
                    if volume.0 < Decimal::ZERO || volume.0 > Decimal::from(1000) {
                        continue;
                    }
                    let reply: BookEnvelope = self
                        .read(&format!("/v1/markets/{}/orderbook", market.id), None)
                        .await?;
                    anyhow::ensure!(
                        reply.success && reply.data.market_id.is_none_or(|id| id == market.id),
                        "quiet book identity mismatch"
                    );
                    for outcome in [1, 2] {
                        if required_outcome.is_some_and(|required| required != outcome) {
                            continue;
                        }
                        let bids: Vec<_> = if outcome == 1 {
                            reply.data.bids.iter().map(|(p, q)| (p.0, q.0)).collect()
                        } else {
                            reply
                                .data
                                .asks
                                .iter()
                                .map(|(p, q)| (Decimal::ONE - p.0, q.0))
                                .collect()
                        };
                        let asks: Vec<_> = if outcome == 1 {
                            reply.data.asks.iter().map(|(p, q)| (p.0, q.0)).collect()
                        } else {
                            reply
                                .data
                                .bids
                                .iter()
                                .map(|(p, q)| (Decimal::ONE - p.0, q.0))
                                .collect()
                        };
                        let bid = bids
                            .iter()
                            .filter(|(_, q)| *q > Decimal::ZERO)
                            .map(|(p, _)| *p)
                            .max();
                        let ask = asks
                            .iter()
                            .filter(|(_, q)| *q > Decimal::ZERO)
                            .map(|(p, _)| *p)
                            .min();
                        if let (Some(bid), Some(ask)) = (bid, ask)
                            && bid >= Decimal::new(2, 1)
                            && ask > bid
                            && ask <= Decimal::ONE
                        {
                            candidates.push((
                                volume.0,
                                Candidate {
                                    market: market.clone(),
                                    outcome,
                                    price: Decimal::new(1, 2),
                                    quantity: Decimal::from(100),
                                    best_bid: bid,
                                    best_ask: ask,
                                },
                            ));
                        }
                    }
                }
                candidates
                    .into_iter()
                    .min_by_key(|(volume, _)| *volume)
                    .map(|(_, c)| c)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "no quiet non-UpDown market with a distant 0.01 post-only limit"
                        )
                    })
            },
        )
        .await
        .map_err(|_| anyhow::anyhow!("quiet discovery deadline; no order selected"))?
    }

    async fn discover_bounded(
        &self,
        settings: &PredictExecutionSettings,
        phase: Phase,
    ) -> anyhow::Result<Candidate> {
        let mut cursor = None;
        let mut seen = HashSet::new();
        let mut candidates = vec![];
        for _ in 0..settings.test.max_discovery_pages {
            let query = Query {
                first: 100,
                status: "OPEN",
                after: cursor.as_deref(),
            };
            let mut params = HashMap::from([
                ("first".to_string(), query.first.to_string()),
                ("status".to_string(), query.status.to_string()),
            ]);
            if let Some(after) = query.after {
                params.insert("after".to_string(), after.to_string());
            }
            let page: Markets = self.read("/v1/markets", Some(params)).await?;
            anyhow::ensure!(page.success, "market discovery returned success=false");
            for market in page.data {
                if market.trading_status != "OPEN"
                    || market.outcomes.len() != 2
                    || market.decimal_precision > 3
                {
                    continue;
                }
                let indices: HashSet<_> = market.outcomes.iter().map(|o| o.index_set).collect();
                if indices != HashSet::from([1, 2]) {
                    continue;
                }
                let reply: BookEnvelope = self
                    .read(&format!("/v1/markets/{}/orderbook", market.id), None)
                    .await?;
                anyhow::ensure!(
                    reply.success && reply.data.market_id.is_none_or(|id| id == market.id),
                    "market book identity mismatch"
                );
                let tick = Decimal::new(1, u32::from(market.decimal_precision));
                for outcome in [1, 2] {
                    let (bids, asks) = if outcome == 1 {
                        (
                            reply
                                .data
                                .bids
                                .iter()
                                .map(|(p, q)| (p.0, q.0))
                                .collect::<Vec<_>>(),
                            reply
                                .data
                                .asks
                                .iter()
                                .map(|(p, q)| (p.0, q.0))
                                .collect::<Vec<_>>(),
                        )
                    } else {
                        (
                            reply
                                .data
                                .asks
                                .iter()
                                .map(|(p, q)| (Decimal::ONE - p.0, q.0))
                                .collect(),
                            reply
                                .data
                                .bids
                                .iter()
                                .map(|(p, q)| (Decimal::ONE - p.0, q.0))
                                .collect(),
                        )
                    };
                    if let Some(candidate) =
                        choose(market.clone(), outcome, &bids, &asks, tick, settings, phase)
                    {
                        candidates.push(candidate);
                    }
                }
                if !candidates.is_empty() {
                    break;
                }
            }
            if !candidates.is_empty() {
                break;
            }
            cursor = page.cursor;
            if let Some(next) = &cursor {
                anyhow::ensure!(seen.insert(next.clone()), "market discovery cursor cycle");
            } else {
                break;
            }
        }
        candidates.into_iter().min_by_key(|c|c.price*c.quantity)
            .ok_or_else(||anyhow::anyhow!("no open market with executable depth within the test bounds; no order was selected"))
    }
}

fn choose(
    market: PredictExecutionMarket,
    outcome: u8,
    bids: &[(Decimal, Decimal)],
    asks: &[(Decimal, Decimal)],
    tick: Decimal,
    settings: &PredictExecutionSettings,
    phase: Phase,
) -> Option<Candidate> {
    let quantity = Decimal::new(1, 2); // Official SDK LIMIT helper lower bound: 0.01 shares.
    let best_bid = bids
        .iter()
        .filter(|(p, q)| *p > Decimal::ZERO && *q >= quantity)
        .map(|(p, _)| *p)
        .max()?;
    let best_ask = asks
        .iter()
        .filter(|(p, q)| *p > Decimal::ZERO && *p <= Decimal::ONE && *q >= quantity)
        .map(|(p, _)| *p)
        .min()?;
    if best_bid >= best_ask {
        return None;
    }
    let price = match phase {
        Phase::Resting => (best_bid / Decimal::from(2))
            .min(Decimal::new(1, 2))
            .round_dp_with_strategy(
                u32::from(market.decimal_precision),
                RoundingStrategy::ToZero,
            ),
        Phase::Fill => best_ask,
    };
    if price < tick
        || price % tick != Decimal::ZERO
        || price * quantity > settings.test.max_order_usdt
        || matches!(phase, Phase::Fill) && price > settings.test.max_fill_price
    {
        return None;
    }
    Some(Candidate {
        market,
        outcome,
        price,
        quantity,
        best_bid,
        best_ask,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selector_requires_depth_and_budget_without_configured_market() {
        let settings = PredictExecutionSettings::parse(include_str!(
            "../../../../examples/predict-execution.toml.example"
        ))
        .unwrap();
        let market=serde_json::from_value(serde_json::json!({"id":42,"tradingStatus":"OPEN","isNegRisk":false,"isYieldBearing":false,
            "feeRateBps":0,"decimalPrecision":3,"outcomes":[{"indexSet":1,"onChainId":"11"},{"indexSet":2,"onChainId":"22"}]})).unwrap();
        let bid = [(Decimal::new(9, 3), Decimal::ONE)];
        let ask = [(Decimal::new(1, 2), Decimal::ONE)];
        let fill = choose(
            market,
            1,
            &bid,
            &ask,
            Decimal::new(1, 3),
            &settings,
            Phase::Fill,
        )
        .unwrap();
        assert_eq!(fill.price * fill.quantity, Decimal::new(1, 4));
        assert!(
            choose(
                fill.market.clone(),
                1,
                &bid,
                &[(Decimal::new(2, 2), Decimal::ONE)],
                Decimal::new(1, 3),
                &settings,
                Phase::Fill
            )
            .is_none()
        );
        assert!(
            choose(
                fill.market,
                1,
                &[],
                &ask,
                Decimal::new(1, 3),
                &settings,
                Phase::Resting
            )
            .is_none()
        );
    }
    #[test]
    fn wire_quotes_keep_all_decimal_digits() {
        let x: Exact = serde_json::from_str("0.123456789012345678").unwrap();
        assert_eq!(x.0.to_string(), "0.123456789012345678");
    }
}
