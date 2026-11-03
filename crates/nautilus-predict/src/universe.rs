// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/LICENSE-3.0.html
// -------------------------------------------------------------------------------------------------

//! Typed discovery for rolling Predict crypto up/down markets.

use anyhow::Context;
use chrono::{Datelike, Duration, Timelike, Utc};
use chrono_tz::America::New_York;

use crate::{
    common::{
        CryptoUpDownMarketCandidate, CryptoUpDownMarketSelector, CryptoUpDownMarketWindow,
        normalize_price_feed_symbol, resolve_crypto_up_down_market_near, title_year_near,
    },
    http::{PredictHttpClient, models::PredictMarket},
};

pub use crate::common::CryptoUpDownMarketSelector as PredictCryptoUpDownSelector;

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
    i64::try_from(now_ns).context("Predict discovery timestamp exceeds supported range")?;
    // Predict's rolling crypto products use a deterministic category slug,
    // e.g. `btc-updown-5m-<window_start_ts>`. Resolve the current and next
    // windows directly first; this is one bounded request per window and is
    // more precise than searching the entire market catalogue. Search remains
    // the fallback for products whose slug convention is not derivable.
    let mut candidates = Vec::new();
    for slug in crypto_category_slugs(selector, now_ns) {
        match http.request_category_markets(&slug).await {
            Ok(category_markets) => {
                log::debug!(
                    "Predict category discovery slug={slug} markets={}",
                    category_markets.len()
                );
                let expected_start = slug
                    .rsplit_once('-')
                    .and_then(|(_, epoch)| epoch.parse::<u64>().ok())
                    .and_then(|epoch| epoch.checked_mul(1_000_000_000))
                    .filter(|_| !matches!(selector.interval_secs, 3600 | 86_400));
                let reference = expected_start.unwrap_or(now_ns);
                candidates.extend(
                    matching_candidates(&category_markets, selector, reference)
                        .into_iter()
                        .filter(|(_, window)| {
                            expected_start.is_none_or(|start| window.start_ns == start)
                        }),
                );
            }
            Err(error) => {
                log::debug!("Predict category discovery slug={slug} failed: {error:#}");
            }
        }
    }
    if candidates.is_empty() {
        let query = format!("{} up or down", selector.title_asset.trim());
        let searched_markets = http.request_search_markets(&query, 100).await?;
        log::debug!(
            "Predict market search fallback query={query:?} markets={}",
            searched_markets.len()
        );
        // Search has no UTC category identity. A repeated title alone cannot
        // identify which market owns which fold, so retry rather than relabel it.
        candidates = matching_candidates(&searched_markets, selector, now_ns)
            .into_iter()
            .filter(|(market, _)| {
                title_year_near(&market.title, now_ns).is_ok_and(|year| {
                    crate::common::parse_crypto_up_down_title(&market.title, year).is_ok()
                })
            })
            .collect();
    }
    anyhow::ensure!(
        !candidates.is_empty(),
        "Predict discovery returned no market matching feed {:?}, title asset {:?}, and interval {}s",
        selector.price_feed_symbol,
        selector.title_asset,
        selector.interval_secs
    );
    candidates.sort_by_key(|(_, window)| window.start_ns);
    let (market, window) = candidates
        .iter()
        .find(|(_, window)| window.start_ns <= now_ns && now_ns < window.end_ns)
        .or_else(|| candidates.iter().find(|(_, window)| window.start_ns > now_ns))
        .context("no open Predict crypto up/down market matched the configured feed, title asset, and interval")?;
    let market_id = market.id;
    log::info!(
        "Selected Predict crypto up/down market id={} title={:?} window={}..{}",
        market_id,
        market.display_title(),
        window.start_ns,
        window.end_ns
    );
    Ok(SelectedCryptoUpDownMarket {
        market_id,
        window: window.clone(),
    })
}

fn crypto_category_slugs(selector: &PredictCryptoUpDownSelector, now_ns: u64) -> Vec<String> {
    if matches!(selector.interval_secs, 3600 | 86_400) {
        let Some(now) = i64::try_from(now_ns)
            .ok()
            .map(chrono::DateTime::<Utc>::from_timestamp_nanos)
        else {
            return Vec::new();
        };
        let now = now.with_timezone(&New_York);
        let asset = selector
            .title_asset
            .trim()
            .to_ascii_lowercase()
            .replace(' ', "-");
        return if selector.interval_secs == 86_400 {
            let end_date = now.date_naive() + Duration::days(i64::from(now.hour() >= 12));
            [end_date, end_date + Duration::days(1)]
                .into_iter()
                .map(|date| {
                    format!(
                        "{asset}-up-or-down-on-{}-{}-{}",
                        date.format("%B").to_string().to_ascii_lowercase(),
                        date.day(),
                        date.year()
                    )
                })
                .collect()
        } else {
            [now, now + Duration::hours(1)]
                .into_iter()
                .map(|time| {
                    format!(
                        "{asset}-up-or-down-{}-{}-{}-{}-et",
                        time.format("%B").to_string().to_ascii_lowercase(),
                        time.day(),
                        time.year(),
                        time.format("%-I%p").to_string().to_ascii_lowercase()
                    )
                })
                .collect()
        };
    }
    // Discovery and routing must agree for canonical and separator-alias feeds.
    // Only recognized quote suffixes support category derivation; other feeds
    // retain the bounded search fallback instead of guessing a base symbol.
    let feed = normalize_price_feed_symbol(&selector.price_feed_symbol);
    let Some(symbol) = ["USDT", "USDC", "USD"]
        .into_iter()
        .find_map(|quote| feed.strip_suffix(quote))
        .filter(|base| !base.is_empty())
        .map(str::to_ascii_lowercase)
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
        format!(
            "{symbol}-updown-{interval}-{}",
            current_start + interval_secs
        ),
    ]
}

fn interval_slug(interval_secs: u64) -> Option<String> {
    // The date-named daily product does not use the rolling interval slug.
    if interval_secs == 0 || interval_secs >= 86_400 {
        return None;
    }
    if interval_secs.is_multiple_of(3600) {
        Some(format!("{}h", interval_secs / 3600))
    } else if interval_secs.is_multiple_of(60) {
        Some(format!("{}m", interval_secs / 60))
    } else {
        Some(format!("{interval_secs}s"))
    }
}

fn resolve_market_window(
    market: &PredictMarket,
    selector: &CryptoUpDownMarketSelector,
    now_ns: u64,
) -> Option<CryptoUpDownMarketWindow> {
    let variant = market.variant_data.as_ref()?;
    let price_feed_symbol = variant.price_feed_symbol.as_deref()?;
    // Infer the date's year independently of timezone conversion. Never accept
    // a neighbouring year because this year's local clock time is ambiguous.
    let title_year = title_year_near(&market.title, now_ns).ok()?;
    resolve_crypto_up_down_market_near(
        CryptoUpDownMarketCandidate {
            variant_type: &variant.variant_type,
            price_feed_symbol,
            title: &market.title,
            title_year,
        },
        selector,
        now_ns,
    )
    .ok()
}

fn matching_candidates(
    markets: &[PredictMarket],
    selector: &CryptoUpDownMarketSelector,
    now_ns: u64,
) -> Vec<(PredictMarket, CryptoUpDownMarketWindow)> {
    markets
        .iter()
        .filter_map(
            |market| match resolve_market_window(market, selector, now_ns) {
                Some(window) => Some((market.clone(), window)),
                None => {
                    log::debug!(
                        "Predict market rejected id={} title={:?} variant={:?} feed={:?}",
                        market.id,
                        market.title,
                        market
                            .variant_data
                            .as_ref()
                            .map(|value| value.variant_type.as_str()),
                        market
                            .variant_data
                            .as_ref()
                            .and_then(|value| value.price_feed_symbol.as_deref()),
                    );
                    None
                }
            },
        )
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{crypto_category_slugs, interval_slug, resolve_market_window};
    use crate::{
        common::CryptoUpDownMarketSelector,
        http::models::{PredictMarket, PredictVariantData},
    };

    fn nanos(value: &str) -> u64 {
        chrono::DateTime::parse_from_rfc3339(value)
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap() as u64
    }

    #[test]
    fn six_selectors_resolve_both_folds_and_roll_back_without_changing_year() {
        use chrono::Timelike;
        use chrono_tz::America::New_York;
        for (feed, asset) in [
            ("BTCUSDT", "Bitcoin"),
            ("ETHUSDT", "Ethereum"),
            ("BNBUSDT", "BNB"),
        ] {
            for interval_secs in [300, 900] {
                let selector = CryptoUpDownMarketSelector {
                    price_feed_symbol: feed.into(),
                    title_asset: asset.into(),
                    interval_secs,
                };
                // Walk both complete 1AM hours, including 1:55/1:45 EDT -> 1AM EST.
                let first = nanos("2026-11-01T04:45:00Z");
                let last = nanos("2026-11-01T07:15:00Z");
                let step = interval_secs * 1_000_000_000;
                for start in (first..last).step_by(step as usize) {
                    let end = start + step;
                    let local = |ns| {
                        chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(ns as i64)
                            .with_timezone(&New_York)
                    };
                    let begin = local(start);
                    let finish = local(end);
                    let title = format!(
                        "{asset} Up or Down - November 1, {}-{} ET",
                        begin.format("%-I:%M%p"),
                        finish.format("%-I:%M%p")
                    );
                    let market = PredictMarket {
                        id: start / 1_000_000_000,
                        title,
                        decimal_precision: 2,
                        outcomes: vec![],
                        created_at: Some("2026-10-31T16:00:00Z".into()),
                        variant_data: Some(PredictVariantData {
                            variant_type: "CRYPTO_UP_DOWN".into(),
                            start_price: None,
                            price_feed_symbol: Some(feed.into()),
                        }),
                    };
                    for reference in [start, start + step / 2, end - 1] {
                        let window = resolve_market_window(&market, &selector, reference)
                            .expect("DST window remains resolvable");
                        assert_eq!(
                            (window.start_ns, window.end_ns),
                            (start, end),
                            "{feed} {interval_secs} {} at {reference}",
                            market.title
                        );
                    }
                    let slugs = crypto_category_slugs(&selector, start);
                    assert!(slugs[0].ends_with(&format!("-{}", start / 1_000_000_000)));
                    assert!(slugs[1].ends_with(&format!("-{}", end / 1_000_000_000)));
                    assert!(begin.hour() <= 2);
                }
                // Reported incident: 04:55 UTC must discover 05:00 in 2026.
                let market = PredictMarket {
                    id: 42,
                    title: format!(
                        "{asset} Up or Down - November 1, 1AM-1:{}AM ET",
                        if interval_secs == 300 { "05" } else { "15" }
                    ),
                    decimal_precision: 2,
                    outcomes: vec![],
                    created_at: None,
                    variant_data: Some(PredictVariantData {
                        variant_type: "CRYPTO_UP_DOWN".into(),
                        start_price: None,
                        price_feed_symbol: Some(feed.into()),
                    }),
                };
                let window =
                    resolve_market_window(&market, &selector, nanos("2026-11-01T04:55:00Z"))
                        .unwrap();
                assert_eq!(window.start_ns, nanos("2026-11-01T05:00:00Z"));
            }
        }
    }

    #[test]
    fn nonexistent_spring_time_cannot_be_reinterpreted_as_another_year() {
        let selector = CryptoUpDownMarketSelector {
            price_feed_symbol: "BTCUSDT".into(),
            title_asset: "Bitcoin".into(),
            interval_secs: 300,
        };
        let market = PredictMarket {
            id: 42,
            title: "Bitcoin Up or Down - March 8, 2AM-2:05AM ET".into(),
            decimal_precision: 2,
            outcomes: vec![],
            created_at: None,
            variant_data: Some(PredictVariantData {
                variant_type: "CRYPTO_UP_DOWN".into(),
                start_price: None,
                price_feed_symbol: Some("BTCUSDT".into()),
            }),
        };
        assert!(resolve_market_window(&market, &selector, nanos("2026-03-08T06:55:00Z")).is_none());
        let valid = PredictMarket {
            title: "Bitcoin Up or Down - March 8, 1:55AM-3AM ET".into(),
            ..market
        };
        let window =
            resolve_market_window(&valid, &selector, nanos("2026-03-08T06:55:00Z")).unwrap();
        assert_eq!(
            (window.start_ns, window.end_ns),
            (nanos("2026-03-08T06:55:00Z"), nanos("2026-03-08T07:00:00Z"))
        );
    }

    #[tokio::test]
    async fn category_discovery_disambiguates_early_successor_by_utc_slug() {
        use axum::{
            Router,
            extract::{Path, State},
            routing::get,
        };
        use chrono_tz::America::New_York;
        use serde_json::{Value, json};

        async fn category(Path(slug): Path<String>, State(earliest): State<u64>) -> String {
            let parts = slug.split('-').collect::<Vec<_>>();
            let start = parts.last().unwrap().parse::<u64>().unwrap();
            if start < earliest {
                return json!({"success":true,"data":{"markets":[]}}).to_string();
            }
            let interval = if parts[2] == "5m" { 300 } else { 900 };
            let (feed, asset) = match parts[0] {
                "btc" => ("BTCUSDT", "Bitcoin"),
                "eth" => ("ETHUSDT", "Ethereum"),
                "bnb" => ("BNBUSDT", "BNB"),
                _ => panic!("unexpected feed"),
            };
            let local = |secs| {
                chrono::DateTime::from_timestamp(secs as i64, 0)
                    .unwrap()
                    .with_timezone(&New_York)
            };
            let mut market: Value =
                serde_json::from_str(include_str!("../test_data/market_2859346.json")).unwrap();
            market["data"]["id"] = json!(start);
            market["data"]["title"] = json!(format!(
                "{asset} Up or Down - November 1, {}-{} ET",
                local(start).format("%-I:%M%p"),
                local(start + interval).format("%-I:%M%p")
            ));
            market["data"]["variantData"]["priceFeedSymbol"] = json!(feed);
            json!({"success":true,"data":{"markets":[market["data"].clone()]}}).to_string()
        }
        // Only the successor is advertised. At 05:55, the next 1AM is the
        // second fold, not the already expired first 1AM market.
        for (now, expected) in [
            ("2026-11-01T04:55:00Z", "2026-11-01T05:00:00Z"),
            ("2026-11-01T05:55:00Z", "2026-11-01T06:00:00Z"),
        ] {
            let expected = nanos(expected);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                axum::serve(
                    listener,
                    Router::new()
                        .route("/v1/categories/{slug}", get(category))
                        .with_state(expected / 1_000_000_000),
                )
                .await
                .unwrap();
            });
            let client =
                crate::http::PredictHttpClient::new("loopback-only", Some(&origin), 2).unwrap();
            for (feed, asset) in [
                ("BTCUSDT", "Bitcoin"),
                ("ETHUSDT", "Ethereum"),
                ("BNBUSDT", "BNB"),
            ] {
                for interval_secs in [300, 900] {
                    let selector = CryptoUpDownMarketSelector {
                        price_feed_symbol: feed.into(),
                        title_asset: asset.into(),
                        interval_secs,
                    };
                    let selected =
                        super::discover_crypto_up_down_market(&client, &selector, nanos(now))
                            .await
                            .unwrap();
                    assert_eq!(selected.market_id, expected / 1_000_000_000);
                    assert_eq!(selected.window.start_ns, expected);
                    assert_eq!(selected.window.duration_secs(), interval_secs);
                }
            }
            server.abort();
        }
    }

    #[tokio::test]
    async fn search_without_utc_identity_retries_ambiguous_titles() {
        use axum::{Router, routing::get};
        use serde_json::{Value, json};
        async fn empty() -> String {
            json!({"success":true,"data":{"markets":[]}}).to_string()
        }
        async fn search() -> String {
            let mut market: Value =
                serde_json::from_str(include_str!("../test_data/market_2859346.json")).unwrap();
            market["data"]["title"] = json!("Bitcoin Up or Down - November 1, 1AM-1:05AM ET");
            market["data"]["variantData"]["priceFeedSymbol"] = json!("BTCUSDT");
            json!({"success":true,"data":{"markets":[market["data"].clone()]}}).to_string()
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/v1/categories/{slug}", get(empty))
                    .route("/v1/search", get(search)),
            )
            .await
            .unwrap();
        });
        let client =
            crate::http::PredictHttpClient::new("loopback-only", Some(&origin), 2).unwrap();
        let selector = CryptoUpDownMarketSelector {
            price_feed_symbol: "BTCUSDT".into(),
            title_asset: "Bitcoin".into(),
            interval_secs: 300,
        };
        for now in ["2026-11-01T04:55:00Z", "2026-11-01T05:55:00Z"] {
            let error = super::discover_crypto_up_down_market(&client, &selector, nanos(now))
                .await
                .unwrap_err();
            assert!(error.to_string().contains("returned no market matching"));
        }
        server.abort();
    }

    #[test]
    fn minute_windows_infer_year_from_date_across_new_year() {
        let selector = CryptoUpDownMarketSelector {
            price_feed_symbol: "BTCUSDT".into(),
            title_asset: "Bitcoin".into(),
            interval_secs: 300,
        };
        for (title, now, expected) in [
            (
                "Bitcoin Up or Down - January 1, 12AM-12:05AM ET",
                "2026-12-31T23:55:00Z",
                "2027-01-01T05:00:00Z",
            ),
            (
                "Bitcoin Up or Down - December 31, 11:55PM-12AM ET",
                "2027-01-01T04:55:00Z",
                "2027-01-01T04:55:00Z",
            ),
        ] {
            let market = PredictMarket {
                id: 42,
                title: title.into(),
                decimal_precision: 2,
                outcomes: vec![],
                created_at: None,
                variant_data: Some(PredictVariantData {
                    variant_type: "CRYPTO_UP_DOWN".into(),
                    start_price: None,
                    price_feed_symbol: Some("BTCUSDT".into()),
                }),
            };
            let window = resolve_market_window(&market, &selector, nanos(now)).unwrap();
            assert_eq!(window.start_ns, nanos(expected));
            assert_eq!(window.duration_secs(), 300);
        }
    }

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
                start_price: None,
                price_feed_symbol: Some("BTC/USD".to_string()),
            }),
        };
        let selector = CryptoUpDownMarketSelector {
            price_feed_symbol: "BTC/USD".to_string(),
            title_asset: "Bitcoin".to_string(),
            interval_secs: 300,
        };
        assert_eq!(
            resolve_market_window(&market, &selector, 1_791_475_200_000_000_000)
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
                start_price: None,
                price_feed_symbol: Some("BTCUSDT".to_string()),
            }),
        };
        let selector = CryptoUpDownMarketSelector {
            price_feed_symbol: "BTC/USDT".to_string(),
            title_asset: "Bitcoin".to_string(),
            interval_secs: 300,
        };
        let window = resolve_market_window(&market, &selector, 1_791_475_200_000_000_000)
            .expect("current official category market should match");
        assert_eq!(window.duration_secs(), 300);
        assert_eq!(window.start_ns / 1_000_000_000, 1_791_475_200);
    }

    #[test]
    fn daily_and_hourly_slugs_follow_local_boundaries_and_year_rollover() {
        let mut selector = super::PredictCryptoUpDownSelector {
            price_feed_symbol: "BTCUSDT".into(),
            title_asset: "Bitcoin".into(),
            interval_secs: 86_400,
        };
        let nanos = |value| {
            chrono::DateTime::parse_from_rfc3339(value)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap() as u64
        };
        assert_eq!(
            crypto_category_slugs(&selector, nanos("2026-10-26T15:59:59Z")),
            [
                "bitcoin-up-or-down-on-october-26-2026",
                "bitcoin-up-or-down-on-october-27-2026"
            ]
        );
        assert_eq!(
            crypto_category_slugs(&selector, nanos("2026-10-26T16:00:00Z")),
            [
                "bitcoin-up-or-down-on-october-27-2026",
                "bitcoin-up-or-down-on-october-28-2026"
            ]
        );
        assert_eq!(
            crypto_category_slugs(&selector, nanos("2026-12-31T18:00:00Z")),
            [
                "bitcoin-up-or-down-on-january-1-2027",
                "bitcoin-up-or-down-on-january-2-2027"
            ]
        );
        let mut market: PredictMarket =
            serde_json::from_str::<crate::http::models::PredictResponse<PredictMarket>>(
                include_str!("../test_data/market_3759734_daily.json"),
            )
            .unwrap()
            .data;
        market.title = "Bitcoin Up or Down on January 1?".into();
        let window =
            resolve_market_window(&market, &selector, nanos("2026-12-31T18:00:00Z")).unwrap();
        assert_eq!(window.start_ns, nanos("2026-12-31T17:00:00Z"));
        assert_eq!(window.end_ns, nanos("2027-01-01T17:00:00Z"));
        selector.interval_secs = 3600;
        assert_eq!(
            crypto_category_slugs(&selector, nanos("2027-01-01T04:30:00Z")),
            [
                "bitcoin-up-or-down-december-31-2026-11pm-et",
                "bitcoin-up-or-down-january-1-2027-12am-et"
            ]
        );
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
    fn canonical_and_legacy_feeds_use_identical_category_discovery() {
        for (base, quote, interval, slug_interval) in [
            ("BTC", "USDT", 300, "5m"),
            ("ETH", "USDT", 900, "15m"),
            ("BNB", "USDT", 300, "5m"),
            ("BTC", "USD", 300, "5m"),
            ("BTC", "USDC", 300, "5m"),
        ] {
            let now = 1_800_000_000_000_000_000;
            let expected = vec![
                format!(
                    "{}-updown-{slug_interval}-1800000000",
                    base.to_ascii_lowercase()
                ),
                format!(
                    "{}-updown-{slug_interval}-{}",
                    base.to_ascii_lowercase(),
                    1_800_000_000 + interval
                ),
            ];
            for feed in [
                format!("{base}{quote}"),
                format!("{base}/{quote}"),
                format!(
                    " {}/{} ",
                    base.to_ascii_lowercase(),
                    quote.to_ascii_lowercase()
                ),
            ] {
                let selector = super::PredictCryptoUpDownSelector {
                    price_feed_symbol: feed.clone(),
                    title_asset: base.into(),
                    interval_secs: interval,
                };
                assert_eq!(
                    crypto_category_slugs(&selector, now),
                    expected,
                    "feed={feed}"
                );
            }
        }
    }

    #[test]
    fn unknown_feed_quote_uses_search_without_guessing_category() {
        for feed in ["USDT", "BTCXYZ", "BTC/XYZ", "BTCUSDT.BINANCE"] {
            let selector = super::PredictCryptoUpDownSelector {
                price_feed_symbol: feed.into(),
                title_asset: "Bitcoin".into(),
                interval_secs: 300,
            };
            assert!(
                crypto_category_slugs(&selector, 1_800_000_000_000_000_000).is_empty(),
                "feed={feed}"
            );
        }
    }

    #[test]
    fn interval_slug_supports_seconds_minutes_and_hours() {
        assert_eq!(interval_slug(5), Some("5s".to_string()));
        assert_eq!(interval_slug(300), Some("5m".to_string()));
        assert_eq!(interval_slug(3600), Some("1h".to_string()));
        assert_eq!(interval_slug(86_400), None);
        assert_eq!(interval_slug(0), None);
    }
}
