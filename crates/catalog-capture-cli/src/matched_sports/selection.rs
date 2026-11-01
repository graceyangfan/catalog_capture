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

//! Bounded REST checks for new recording admissions; no accumulated startup history.
use anyhow::{Context, Result};
use catalog_capture_core::paired_markets::{MatchedMarket, MatchedSportsCaptureConfig};
use nautilus_network::http::{HttpClient, HttpRedirectPolicy, Method};
use nautilus_predict::{PredictHttpClient, http::sports::SportsRecentTrades};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{collections::BTreeSet, str::FromStr};

#[derive(Clone)]
pub(super) struct SelectionHttp {
    client: HttpClient,
    clob_origin: String,
    data_origin: String,
}
impl SelectionHttp {
    pub fn new() -> Result<Self> {
        Ok(Self {
            client: HttpClient::builder()
                .timeout_secs(5)
                .redirect_policy(HttpRedirectPolicy::Reject)
                .build()?,
            clob_origin: "https://clob.polymarket.com".into(),
            data_origin: "https://data-api.polymarket.com".into(),
        })
    }
    #[cfg(test)]
    pub fn for_test(origin: &str) -> Self {
        let mut client = Self::new().unwrap();
        client.clob_origin = origin.into();
        client.data_origin = origin.into();
        client
    }
    async fn get<T: DeserializeOwned>(&self, url: String, query: &[(&str, &str)]) -> Result<T> {
        let response = self
            .client
            .request_with_params(Method::GET, url, Some(&query), None, None, None, None)
            .await?;
        anyhow::ensure!(
            response.status.is_success(),
            "selection API returned HTTP {}",
            response.status.as_u16()
        );
        Ok(serde_json::from_slice(&response.body)?)
    }
    async fn book(&self, token: &str, condition: &str, band: Decimal) -> Result<BookQuality> {
        let response: PmBook = self
            .get(format!("{}/book", self.clob_origin), &[("token_id", token)])
            .await?;
        anyhow::ensure!(
            response.asset_id == token && response.market == condition,
            "Polymarket orderbook identity mismatch"
        );
        let levels = |rows: Vec<PmLevel>| -> Result<Vec<[Decimal; 2]>> {
            rows.into_iter()
                .map(|r| Ok([Decimal::from_str(&r.price)?, Decimal::from_str(&r.size)?]))
                .collect()
        };
        book_quality(&levels(response.bids)?, &levels(response.asks)?, band)
    }
    async fn trades(&self, condition: &str) -> Result<SportsRecentTrades> {
        let page: PmTrades = self
            .get(
                format!("{}/v2/trades", self.data_origin),
                &[
                    ("condition", condition),
                    ("taker_only", "true"),
                    ("limit", "50"),
                ],
            )
            .await?;
        anyhow::ensure!(page.data.len() <= 50, "oversized PM trade page");
        let mut timestamps = Vec::new();
        for row in page.data {
            anyhow::ensure!(
                row.condition_id == condition,
                "Polymarket ignored condition filter"
            );
            timestamps.push(row.timestamp);
        }
        anyhow::ensure!(
            timestamps.windows(2).all(|w| w[0] >= w[1]),
            "unsorted PM trades"
        );
        Ok(SportsRecentTrades {
            timestamps,
            has_more: page.pagination.has_more,
        })
    }
    pub async fn check(
        &self,
        predict: &PredictHttpClient,
        pair: &MatchedMarket,
        config: &MatchedSportsCaptureConfig,
    ) -> Result<SelectionCheck> {
        let token = |index: usize| -> Result<&str> {
            pair.outcomes[index]
                .polymarket_instrument
                .symbol
                .as_str()
                .rsplit_once('-')
                .map(|(_, token)| token)
                .context("missing PM token identity")
        };
        let (a, b) = (token(0)?, token(1)?);
        let condition = &pair.polymarket_condition_id;
        let (predict_book, book_a, book_b, predict_trades, pm_trades) = tokio::try_join!(
            predict.request_sports_book(pair.predict_market_id),
            self.book(a, condition, config.depth_band),
            self.book(b, condition, config.depth_band),
            predict.request_sports_recent_trades(pair.predict_market_id),
            self.trades(condition)
        )?;
        let books = [
            book_quality(&predict_book.bids, &predict_book.asks, config.depth_band)?,
            book_a,
            book_b,
        ];
        let now = nautilus_core::time::get_atomic_clock_realtime()
            .get_time_ns()
            .as_u64()
            / 1_000_000_000;
        let recent = [
            recent_activity(&predict_trades, now, config)?,
            recent_activity(&pm_trades, now, config)?,
        ];
        let qualified =
            books.iter().all(|book| book.qualifies(config)) && recent.iter().all(|r| r.qualified);
        Ok(SelectionCheck {
            qualified,
            books,
            recent,
        })
    }
}
#[derive(Deserialize)]
struct PmBook {
    market: String,
    asset_id: String,
    bids: Vec<PmLevel>,
    asks: Vec<PmLevel>,
}
#[derive(Deserialize)]
struct PmLevel {
    price: String,
    size: String,
}
#[derive(Deserialize)]
struct PmTrades {
    data: Vec<PmTrade>,
    pagination: PmPagination,
}
#[derive(Deserialize)]
struct PmTrade {
    condition_id: String,
    timestamp: u64,
}
#[derive(Deserialize)]
struct PmPagination {
    has_more: bool,
}

#[derive(Debug, Serialize)]
pub(super) struct SelectionCheck {
    pub qualified: bool,
    /// Predict YES, then PM outcome 0 and outcome 1; never added together.
    books: [BookQuality; 3],
    recent: [RecentActivity; 2],
}
#[derive(Debug, Serialize)]
struct BookQuality {
    spread: Decimal,
    bid_depth_shares: Decimal,
    ask_depth_shares: Decimal,
}
fn book_quality(
    bids: &[[Decimal; 2]],
    asks: &[[Decimal; 2]],
    band: Decimal,
) -> Result<BookQuality> {
    fn levels(rows: &[[Decimal; 2]], reverse: bool) -> Result<Vec<[Decimal; 2]>> {
        anyhow::ensure!(!rows.is_empty(), "one-sided or empty book");
        let mut unique = BTreeSet::new();
        for [price, quantity] in rows {
            anyhow::ensure!(
                *price >= Decimal::ZERO
                    && *price <= Decimal::ONE
                    && *quantity > Decimal::ZERO
                    && unique.insert(*price),
                "invalid or duplicate book level"
            );
        }
        let mut result = rows.to_vec();
        result.sort_by_key(|row| row[0]);
        if reverse {
            result.reverse();
        }
        Ok(result)
    }
    let bids = levels(bids, true)?;
    let asks = levels(asks, false)?;
    anyhow::ensure!(bids[0][0] < asks[0][0], "crossed or locked book");
    let sum = |rows: &[[Decimal; 2]]| -> Result<Decimal> {
        rows.iter()
            .filter(|r| (r[0] - rows[0][0]).abs() <= band)
            .try_fold(Decimal::ZERO, |total, row| {
                total.checked_add(row[1]).context("depth overflow")
            })
    };
    Ok(BookQuality {
        spread: asks[0][0] - bids[0][0],
        bid_depth_shares: sum(&bids)?,
        ask_depth_shares: sum(&asks)?,
    })
}
impl BookQuality {
    fn qualifies(&self, config: &MatchedSportsCaptureConfig) -> bool {
        self.spread <= config.max_spread
            && self.bid_depth_shares >= config.min_depth_shares
            && self.ask_depth_shares >= config.min_depth_shares
    }
}
#[derive(Debug, Serialize)]
struct RecentActivity {
    qualified: bool,
    distinct_trade_seconds: usize,
    last_trade_age_secs: Option<u64>,
    has_more: bool,
}
fn recent_activity(
    page: &SportsRecentTrades,
    now: u64,
    config: &MatchedSportsCaptureConfig,
) -> Result<RecentActivity> {
    anyhow::ensure!(
        page.timestamps.iter().all(|t| *t <= now.saturating_add(10)),
        "future execution timestamp"
    );
    let times: BTreeSet<_> = page
        .timestamps
        .iter()
        .copied()
        .filter(|t| *t >= now.saturating_sub(config.recent_trade_window_secs))
        .collect();
    let age = times.last().map(|last| now.saturating_sub(*last));
    Ok(RecentActivity {
        qualified: times.len() >= config.min_recent_trade_times
            && age.is_some_and(|a| a <= config.max_last_trade_age_secs),
        distinct_trade_seconds: times.len(),
        last_trade_age_secs: age,
        has_more: page.has_more,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn levels(rows: &[(i64, i64)]) -> Vec<[Decimal; 2]> {
        rows.iter()
            .map(|(p, q)| [Decimal::new(*p, 2), (*q).into()])
            .collect()
    }
    #[test]
    fn book_gate_uses_near_best_depth_on_each_side_and_sorts_levels() {
        let config = MatchedSportsCaptureConfig::default();
        let good = book_quality(
            &levels(&[(48, 100), (49, 100)]),
            &levels(&[(52, 100), (51, 100)]),
            config.depth_band,
        )
        .unwrap();
        assert_eq!(good.spread, Decimal::new(2, 2));
        assert_eq!(good.bid_depth_shares, 200.into());
        assert!(good.qualifies(&config));
        let distant_size = book_quality(
            &levels(&[(49, 1), (10, 100_000)]),
            &levels(&[(51, 100)]),
            config.depth_band,
        )
        .unwrap();
        assert!(!distant_size.qualifies(&config));
        let wide = book_quality(
            &levels(&[(40, 1000)]),
            &levels(&[(60, 1000)]),
            config.depth_band,
        )
        .unwrap();
        assert!(!wide.qualifies(&config));
        assert!(book_quality(&[], &levels(&[(51, 100)]), config.depth_band).is_err());
        assert!(
            book_quality(
                &levels(&[(51, 100)]),
                &levels(&[(51, 100)]),
                config.depth_band
            )
            .is_err()
        );
        assert!(
            book_quality(
                &levels(&[(49, 100), (49, 100)]),
                &levels(&[(51, 100)]),
                config.depth_band
            )
            .is_err()
        );
    }
    #[test]
    fn newest_page_confirms_activity_without_waiting_or_claiming_full_volume() {
        let config = MatchedSportsCaptureConfig::default();
        let check = |timestamps| {
            recent_activity(
                &SportsRecentTrades {
                    timestamps,
                    has_more: true,
                },
                1000,
                &config,
            )
            .unwrap()
        };
        assert!(check(vec![995, 980]).qualified);
        assert!(check(vec![995, 980]).has_more);
        assert!(!check(vec![995, 995]).qualified);
        assert!(!check(vec![850, 800]).qualified);
        assert!(!check(vec![]).qualified);
        assert!(
            recent_activity(
                &SportsRecentTrades {
                    timestamps: vec![2000],
                    has_more: false
                },
                1000,
                &config
            )
            .is_err()
        );
    }
}
