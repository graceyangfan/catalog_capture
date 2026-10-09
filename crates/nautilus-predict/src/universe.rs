// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/LICENSE-3.0.html
// -------------------------------------------------------------------------------------------------

//! Typed discovery for rolling Predict crypto up/down markets.

use anyhow::Context;
use catalog_capture_core::{
    CryptoUpDownMarketCandidate, CryptoUpDownMarketSelector, CryptoUpDownMarketWindow,
    resolve_crypto_up_down_market,
};
use chrono::{Datelike, Utc};

use crate::{PredictHttpClient, models::PredictMarket};

/// Product selector configured by the operator; market ID is intentionally absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictCryptoUpDownSelector {
    pub price_feed_symbol: String,
    pub title_asset: String,
    pub interval_secs: u64,
}

/// One selected market and its title-derived UTC window.
#[derive(Debug, Clone)]
pub struct SelectedCryptoUpDownMarket {
    pub market_id: u64,
    pub window: CryptoUpDownMarketWindow,
}

/// Selects the current interval; if it is advertised early, selects the nearest future interval.
pub async fn discover_crypto_up_down_market(
    http: &PredictHttpClient,
    selector: &PredictCryptoUpDownSelector,
    now_ns: u64,
) -> anyhow::Result<SelectedCryptoUpDownMarket> {
    let now_year = chrono::DateTime::<Utc>::from_timestamp_nanos(now_ns as i64).year();
    let expected = CryptoUpDownMarketSelector {
        price_feed_symbol: selector.price_feed_symbol.clone(),
        title_asset: selector.title_asset.clone(),
        interval_secs: selector.interval_secs,
    };
    // Predict's rolling crypto products use a deterministic category slug,
    // e.g. `btc-updown-5m-<window_start_ts>`. Resolve the current and next
    // windows directly first; this is one bounded request per window and is
    // more precise than searching the entire market catalogue. Search remains
    // the fallback for products whose slug convention is not derivable.
    let mut markets = Vec::new();
    for slug in crypto_category_slugs(selector, now_ns) {
        match http.request_category_markets(&slug).await {
            Ok(category_markets) => {
                log::debug!(
                    "Predict category discovery slug={slug} markets={}",
                    category_markets.len()
                );
                markets.extend(category_markets);
            }
            Err(error) => {
                log::debug!("Predict category discovery slug={slug} failed: {error:#}");
            }
        }
    }
    let mut candidates = matching_candidates(&markets, &expected, now_year);
    if candidates.is_empty() {
        let query = format!("{} up or down", selector.title_asset.trim());
        let searched_markets = http.request_search_markets(&query, 100).await?;
        log::debug!(
            "Predict market search fallback query={query:?} markets={}",
            searched_markets.len()
        );
        candidates = matching_candidates(&searched_markets, &expected, now_year);
    }
    anyhow::ensure!(
        !candidates.is_empty(),
        "Predict discovery returned no market matching feed {:?}, title asset {:?}, and interval {}s",
        selector.price_feed_symbol,
        selector.title_asset,
        selector.interval_secs
    );
    candidates.sort_by_key(|(_, window)| window.start_ns);
    let (_, window) = candidates
        .iter()
        .find(|(_, window)| window.start_ns <= now_ns && now_ns < window.end_ns)
        .or_else(|| candidates.iter().find(|(_, window)| window.start_ns > now_ns))
        .context("no open Predict crypto up/down market matched the configured feed, title asset, and interval")?;
    let market_id = candidates
        .iter()
        .find(|(_, candidate_window)| candidate_window == window)
        .expect("selected candidate must exist")
        .0
        .id;
    log::info!(
        "Selected Predict crypto up/down market id={} window={}..{}",
        market_id,
        window.start_ns,
        window.end_ns
    );
    Ok(SelectedCryptoUpDownMarket {
        market_id,
        window: window.clone(),
    })
}

fn crypto_category_slugs(selector: &PredictCryptoUpDownSelector, now_ns: u64) -> Vec<String> {
    let Some(symbol) = selector
        .price_feed_symbol
        .split_once('/')
        .map(|(base, _)| base.trim().to_ascii_lowercase())
        .filter(|base| !base.is_empty() && base.chars().all(|c| c.is_ascii_alphanumeric()))
    else {
        return Vec::new();
    };
    let Some(interval) = interval_slug(selector.interval_secs) else {
        return Vec::new();
    };
    let interval_secs = selector.interval_secs;
    let now_secs = now_ns / 1_000_000_000;
    let current_start = now_secs / interval_secs * interval_secs;
    vec![
        format!("{symbol}-updown-{interval}-{current_start}"),
        format!("{symbol}-updown-{interval}-{}", current_start + interval_secs),
    ]
}

fn interval_slug(interval_secs: u64) -> Option<String> {
    if interval_secs == 0 {
        return None;
    }
    if interval_secs % 3600 == 0 {
        Some(format!("{}h", interval_secs / 3600))
    } else if interval_secs % 60 == 0 {
        Some(format!("{}m", interval_secs / 60))
    } else {
        Some(format!("{interval_secs}s"))
    }
}

fn resolve_market_window(
    market: &PredictMarket,
    selector: &CryptoUpDownMarketSelector,
    fallback_year: i32,
) -> Option<CryptoUpDownMarketWindow> {
    let variant = market.variant_data.as_ref()?;
    let price_feed_symbol = variant.price_feed_symbol.as_deref()?;
    // Predict titles omit the year. The runtime year is the authoritative first choice;
    // created_at may be from the previous day and must not shift a current-window title
    // into the previous year. The neighbouring years cover the New Year boundary.
    [fallback_year, fallback_year + 1, fallback_year - 1]
        .into_iter()
        .find_map(|title_year| {
            resolve_crypto_up_down_market(
                CryptoUpDownMarketCandidate {
                    variant_type: &variant.variant_type,
                    price_feed_symbol,
                    title: &market.title,
                    title_year,
                },
                selector,
            )
            .ok()
        })
}

fn matching_candidates(
    markets: &[PredictMarket],
    selector: &CryptoUpDownMarketSelector,
    fallback_year: i32,
) -> Vec<(PredictMarket, CryptoUpDownMarketWindow)> {
    markets
        .iter()
        .filter_map(|market| {
            match resolve_market_window(market, selector, fallback_year) {
                Some(window) => Some((market.clone(), window)),
                None => {
                    log::debug!(
                        "Predict market rejected id={} title={:?} variant={:?} feed={:?}",
                        market.id,
                        market.title,
                        market.variant_data.as_ref().map(|value| value.variant_type.as_str()),
                        market
                            .variant_data
                            .as_ref()
                            .and_then(|value| value.price_feed_symbol.as_deref()),
                    );
                    None
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{crypto_category_slugs, interval_slug, resolve_market_window};
    use crate::models::{PredictMarket, PredictVariantData};
    use catalog_capture_core::CryptoUpDownMarketSelector;

    #[test]
    fn requires_typed_variant_feed_and_title_interval() {
        let market = PredictMarket {
            id: 9,
            title: "Bitcoin Up or Down - October 3, 6PM-6:05PM ET".to_string(),
            decimal_precision: 2,
            outcomes: Vec::new(),
            created_at: Some("2026-10-03T20:00:00Z".to_string()),
            variant_data: Some(PredictVariantData {
                variant_type: "CRYPTO_UP_DOWN".to_string(),
                price_feed_symbol: Some("BTC/USD".to_string()),
            }),
        };
        let selector = CryptoUpDownMarketSelector {
            price_feed_symbol: "BTC/USD".to_string(),
            title_asset: "Bitcoin".to_string(),
            interval_secs: 300,
        };
        assert_eq!(
            resolve_market_window(&market, &selector, 2026)
                .expect("matching market")
                .duration_secs(),
            300
        );
    }

    #[test]
    fn accepts_current_predict_btc_category_shape() {
        let market = PredictMarket {
            id: 3_043_876,
            title: "Bitcoin Up or Down - October 8, 12PM-12:05PM ET".to_string(),
            decimal_precision: 2,
            outcomes: Vec::new(),
            created_at: Some("2026-10-07T16:00:02.000Z".to_string()),
            variant_data: Some(PredictVariantData {
                variant_type: "CRYPTO_UP_DOWN".to_string(),
                price_feed_symbol: Some("BTCUSDT".to_string()),
            }),
        };
        let selector = CryptoUpDownMarketSelector {
            price_feed_symbol: "BTC/USDT".to_string(),
            title_asset: "Bitcoin".to_string(),
            interval_secs: 300,
        };
        let window = resolve_market_window(&market, &selector, 2026)
            .expect("current official category market should match");
        assert_eq!(window.duration_secs(), 300);
        assert_eq!(window.start_ns / 1_000_000_000, 1_791_475_200);
    }

    #[test]
    fn derives_btc_rolling_category_slugs_for_current_and_next_window() {
        let selector = super::PredictCryptoUpDownSelector {
            price_feed_symbol: "BTC/USDT".to_string(),
            title_asset: "Bitcoin".to_string(),
            interval_secs: 300,
        };
        assert_eq!(
            crypto_category_slugs(&selector, 1_800_000_149_000_000_000),
            vec![
                "btc-updown-5m-1800000000".to_string(),
                "btc-updown-5m-1800000300".to_string(),
            ]
        );
    }

    #[test]
    fn interval_slug_supports_seconds_minutes_and_hours() {
        assert_eq!(interval_slug(5), Some("5s".to_string()));
        assert_eq!(interval_slug(300), Some("5m".to_string()));
        assert_eq!(interval_slug(3600), Some("1h".to_string()));
        assert_eq!(interval_slug(0), None);
    }
}
