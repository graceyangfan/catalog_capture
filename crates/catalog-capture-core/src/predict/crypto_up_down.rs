// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Parsing for Predict crypto up/down market titles.
//!
//! Predict's typed variant data identifies a `CRYPTO_UP_DOWN` market and its
//! `priceFeedSymbol`, while the displayed title expresses the market window.
//! The public schema does not publish a supported-coin or interval enum, so the
//! selected feed, display asset, and interval are caller configuration—not a
//! hard-coded venue list.

use std::str::FromStr;

use anyhow::{Context, Result, bail};
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use chrono_tz::America::New_York;
use nautilus_model::identifiers::InstrumentId;

/// Custom-data metadata key carrying the canonical Predict feed identity for a rolling selector.
pub const PREDICT_SELECTOR_PRICE_FEED_SYMBOL: &str = "predict_price_feed_symbol";
/// Custom-data metadata key carrying the canonical Predict title-asset identity for a selector.
pub const PREDICT_SELECTOR_TITLE_ASSET: &str = "predict_title_asset";
/// Custom-data metadata key carrying the rolling selector interval in seconds.
pub const PREDICT_SELECTOR_INTERVAL_SECS: &str = "predict_interval_secs";

/// A normalized crypto up/down market time window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CryptoUpDownMarketWindow {
    /// Asset label from the market title, for example `Bitcoin`.
    pub title_asset: String,
    /// Window start in UTC nanoseconds.
    pub start_ns: u64,
    /// Window end in UTC nanoseconds.
    pub end_ns: u64,
}

impl CryptoUpDownMarketWindow {
    #[must_use]
    pub fn duration_secs(&self) -> u64 {
        (self.end_ns - self.start_ns) / 1_000_000_000
    }
}

/// The typed fields from one Predict market required for crypto up/down
/// selection. The adapter constructs this from the REST market response.
#[derive(Debug, Clone, Copy)]
pub struct CryptoUpDownMarketCandidate<'a> {
    pub variant_type: &'a str,
    pub price_feed_symbol: &'a str,
    pub title: &'a str,
    /// The year supplied by discovery metadata; Predict titles omit it.
    pub title_year: i32,
}

/// One configured crypto up/down product.
///
/// `price_feed_symbol` is matched against Predict's typed
/// `variantData.priceFeedSymbol`, allowing only case and separator aliases.
/// `title_asset` is separately matched against the display-title prefix because
/// Predict's public schema does not specify a universal transformation between
/// the two strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CryptoUpDownMarketSelector {
    pub price_feed_symbol: String,
    pub title_asset: String,
    pub interval_secs: u64,
}

/// Complete canonical owner identity for one rolling crypto Up/Down selector.
///
/// Several assets can share an interval (for example BTC, ETH, and SOL 5m). The interval
/// alone is therefore not a writer, subscription, or rollover identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CryptoUpDownSelectorKey {
    price_feed_symbol: String,
    title_asset: String,
    interval_secs: u64,
}

impl CryptoUpDownSelectorKey {
    /// Normalizes the configured selector fields into their stable routing identity.
    pub fn new(price_feed_symbol: &str, title_asset: &str, interval_secs: u64) -> Result<Self> {
        let price_feed_symbol = normalize_price_feed_symbol(price_feed_symbol);
        let title_asset = title_asset.trim().to_ascii_lowercase();
        anyhow::ensure!(
            !price_feed_symbol.is_empty(),
            "crypto up/down price feed symbol must contain an ASCII alphanumeric character"
        );
        anyhow::ensure!(
            !title_asset.is_empty(),
            "crypto up/down title asset must be non-empty"
        );
        anyhow::ensure!(
            interval_secs > 0,
            "crypto up/down interval must be positive"
        );
        Ok(Self {
            price_feed_symbol,
            title_asset,
            interval_secs,
        })
    }

    #[must_use]
    pub fn price_feed_symbol(&self) -> &str {
        &self.price_feed_symbol
    }

    #[must_use]
    pub fn title_asset(&self) -> &str {
        &self.title_asset
    }

    #[must_use]
    pub const fn interval_secs(&self) -> u64 {
        self.interval_secs
    }
}

impl std::fmt::Display for CryptoUpDownSelectorKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}s",
            self.price_feed_symbol, self.title_asset, self.interval_secs
        )
    }
}

/// Returns the canonical Nautilus instrument identity for one Predict binary outcome.
///
/// Predict assigns outcome indices `1` (YES) and `2` (NO). Keeping this construction in
/// core ensures discovery, recording, and retirement use the same identity.
pub fn predict_outcome_instrument_id(market_id: u64, outcome_index: u8) -> Result<InstrumentId> {
    anyhow::ensure!(market_id != 0, "Predict market ID must be non-zero");
    anyhow::ensure!(
        matches!(outcome_index, 1 | 2),
        "Predict outcome index must be 1 (YES) or 2 (NO)"
    );
    InstrumentId::from_str(&format!("{market_id}-{outcome_index}.PREDICT")).with_context(|| {
        format!("invalid Predict market/outcome identity {market_id}/{outcome_index}")
    })
}

/// Parses a Predict title such as
/// `Bitcoin Up or Down - October 3, 6PM-6:05PM ET`.
///
/// The displayed title does not contain a year, so the caller must supply the
/// year obtained from the market metadata/discovery context. Time is explicitly
/// Eastern time, including daylight-saving-time rules. An ambiguous or
/// nonexistent local timestamp is rejected rather than guessed.
pub fn parse_crypto_up_down_title(title: &str, year: i32) -> Result<CryptoUpDownMarketWindow> {
    let (title_asset, schedule) = title
        .trim()
        .split_once(" Up or Down - ")
        .ok_or_else(|| anyhow::anyhow!("invalid crypto up/down title {title:?}"))?;
    let title_asset = title_asset.trim();
    if title_asset.is_empty() {
        bail!("crypto up/down title has an empty asset label: {title:?}");
    }

    let schedule = schedule
        .strip_suffix(" ET")
        .ok_or_else(|| anyhow::anyhow!("crypto up/down title must end with ` ET`: {title:?}"))?;
    let (date, time_range) = schedule
        .split_once(", ")
        .ok_or_else(|| anyhow::anyhow!("missing date/time separator in title {title:?}"))?;
    let (start_time, end_time) = time_range
        .split_once('-')
        .ok_or_else(|| anyhow::anyhow!("missing time range in title {title:?}"))?;

    let date = NaiveDate::parse_from_str(&format!("{year} {date}"), "%Y %B %d")
        .with_context(|| format!("invalid crypto up/down date in title {title:?}"))?;
    let start_time = parse_title_time(start_time)
        .with_context(|| format!("invalid crypto up/down start time in title {title:?}"))?;
    let end_time = parse_title_time(end_time)
        .with_context(|| format!("invalid crypto up/down end time in title {title:?}"))?;

    let start = eastern_to_utc(NaiveDateTime::new(date, start_time), title)?;
    let mut end_date = date;
    if end_time <= start_time {
        end_date = end_date
            .checked_add_signed(Duration::days(1))
            .ok_or_else(|| anyhow::anyhow!("crypto up/down title date overflows: {title:?}"))?;
    }
    let end = eastern_to_utc(NaiveDateTime::new(end_date, end_time), title)?;

    let start_ns =
        u64::try_from(start.timestamp_nanos_opt().ok_or_else(|| {
            anyhow::anyhow!("crypto up/down start timestamp overflows: {title:?}")
        })?)
        .context("crypto up/down start timestamp predates Unix epoch")?;
    let end_ns = u64::try_from(
        end.timestamp_nanos_opt()
            .ok_or_else(|| anyhow::anyhow!("crypto up/down end timestamp overflows: {title:?}"))?,
    )
    .context("crypto up/down end timestamp predates Unix epoch")?;
    if end_ns <= start_ns {
        bail!("crypto up/down title has a non-positive window: {title:?}");
    }

    Ok(CryptoUpDownMarketWindow {
        title_asset: title_asset.to_string(),
        start_ns,
        end_ns,
    })
}

/// Validates metadata and resolves a configured crypto up/down product.
pub fn resolve_crypto_up_down_market(
    candidate: CryptoUpDownMarketCandidate<'_>,
    selector: &CryptoUpDownMarketSelector,
) -> Result<CryptoUpDownMarketWindow> {
    if candidate.variant_type != "CRYPTO_UP_DOWN" {
        bail!(
            "expected CRYPTO_UP_DOWN variant, received {:?} for title {:?}",
            candidate.variant_type,
            candidate.title
        );
    }
    if normalize_price_feed_symbol(candidate.price_feed_symbol)
        != normalize_price_feed_symbol(&selector.price_feed_symbol)
    {
        bail!(
            "expected Predict priceFeedSymbol {:?}, received {:?} for title {:?}",
            selector.price_feed_symbol,
            candidate.price_feed_symbol,
            candidate.title
        );
    }

    let window = parse_crypto_up_down_title(candidate.title, candidate.title_year)?;
    if !window
        .title_asset
        .eq_ignore_ascii_case(selector.title_asset.trim())
    {
        bail!(
            "expected crypto up/down title asset {:?}, received {:?} from title {:?}",
            selector.title_asset,
            window.title_asset,
            candidate.title
        );
    }
    if window.duration_secs() != selector.interval_secs {
        bail!(
            "expected a {}-second crypto up/down market, received {} seconds from title {:?}",
            selector.interval_secs,
            window.duration_secs(),
            candidate.title
        );
    }
    Ok(window)
}

/// Predict has returned equivalent feed symbols both with and without a
/// separator (for example `BTC/USDT` in operator config and `BTCUSDT` in the
/// live `variantData` response). Preserve the quote asset distinction while
/// normalizing only harmless separators and case.
fn normalize_price_feed_symbol(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_uppercase)
        .collect()
}

fn parse_title_time(raw: &str) -> Result<NaiveTime> {
    let normalized = raw.trim().replace(' ', "").to_ascii_uppercase();
    let with_minutes = if normalized.contains(':') {
        normalized
    } else if let Some(hour) = normalized.strip_suffix("AM") {
        format!("{hour}:00AM")
    } else if let Some(hour) = normalized.strip_suffix("PM") {
        format!("{hour}:00PM")
    } else {
        normalized
    };
    NaiveTime::parse_from_str(&with_minutes, "%I:%M%p")
        .with_context(|| format!("invalid Eastern title time {raw:?}"))
}

fn eastern_to_utc(local: NaiveDateTime, title: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    New_York
        .from_local_datetime(&local)
        .single()
        .map(|value| value.with_timezone(&chrono::Utc))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "ambiguous or nonexistent Eastern timestamp {local} in crypto up/down title {title:?}"
            )
        })
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::{
        CryptoUpDownMarketCandidate, CryptoUpDownMarketSelector, parse_crypto_up_down_title,
        resolve_crypto_up_down_market,
    };

    fn five_minute_selector() -> CryptoUpDownMarketSelector {
        CryptoUpDownMarketSelector {
            price_feed_symbol: "fixture-feed-symbol".to_string(),
            title_asset: "Bitcoin".to_string(),
            interval_secs: 300,
        }
    }

    #[test]
    fn parses_title_window_in_daylight_time() {
        let window =
            parse_crypto_up_down_title("Bitcoin Up or Down - October 3, 6PM-6:05PM ET", 2026)
                .expect("valid title");

        assert_eq!(window.title_asset, "Bitcoin");
        assert_eq!(window.duration_secs(), 300);
        assert_eq!(
            window.start_ns,
            Utc.with_ymd_and_hms(2026, 10, 3, 22, 0, 0)
                .single()
                .expect("UTC date")
                .timestamp_nanos_opt()
                .expect("timestamp") as u64
        );
    }

    #[test]
    fn accepts_any_title_asset_without_inventing_a_coin_list() {
        let window =
            parse_crypto_up_down_title("Aptos Up or Down - June 12, 8:50AM-8:55AM ET", 2026)
                .expect("valid title");

        assert_eq!(window.title_asset, "Aptos");
        assert_eq!(window.duration_secs(), 300);
    }

    #[test]
    fn selector_requires_actual_variant_feed_asset_and_interval() {
        let selector = five_minute_selector();
        let candidate = CryptoUpDownMarketCandidate {
            variant_type: "CRYPTO_UP_DOWN",
            price_feed_symbol: "fixture-feed-symbol",
            title: "Bitcoin Up or Down - October 3, 6PM-6:05PM ET",
            title_year: 2026,
        };
        resolve_crypto_up_down_market(candidate, &selector).expect("matching market");

        let aliased_feed = CryptoUpDownMarketCandidate {
            price_feed_symbol: "FIXTURE/FEED-SYMBOL",
            ..candidate
        };
        resolve_crypto_up_down_market(aliased_feed, &selector)
            .expect("separator and case aliases should match");

        let wrong_feed = CryptoUpDownMarketCandidate {
            price_feed_symbol: "ETH/USDT",
            ..candidate
        };
        assert!(
            resolve_crypto_up_down_market(wrong_feed, &selector)
                .unwrap_err()
                .to_string()
                .contains("priceFeedSymbol")
        );

        let wrong_interval = CryptoUpDownMarketCandidate {
            title: "Bitcoin Up or Down - October 3, 6PM-6:10PM ET",
            ..candidate
        };
        assert!(
            resolve_crypto_up_down_market(wrong_interval, &selector)
                .unwrap_err()
                .to_string()
                .contains("300-second")
        );

        let wrong_variant = CryptoUpDownMarketCandidate {
            variant_type: "DEFAULT",
            ..candidate
        };
        assert!(
            resolve_crypto_up_down_market(wrong_variant, &selector)
                .unwrap_err()
                .to_string()
                .contains("CRYPTO_UP_DOWN")
        );
    }

    #[test]
    fn accepts_predict_live_btc_symbol_alias() {
        let selector = CryptoUpDownMarketSelector {
            price_feed_symbol: "BTC/USDT".to_string(),
            title_asset: "Bitcoin".to_string(),
            interval_secs: 300,
        };
        let candidate = CryptoUpDownMarketCandidate {
            variant_type: "CRYPTO_UP_DOWN",
            price_feed_symbol: "BTCUSDT",
            title: "Bitcoin Up or Down - October 8, 12PM-12:05PM ET",
            title_year: 2026,
        };
        resolve_crypto_up_down_market(candidate, &selector).expect("live Predict shape matches");
    }

    #[test]
    fn rejects_ambiguous_eastern_time() {
        let result =
            parse_crypto_up_down_title("Bitcoin Up or Down - November 1, 1:55AM-2AM ET", 2026);
        assert!(result.unwrap_err().to_string().contains("ambiguous"));
    }
}
