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

//! Single registry for known custom-data type names (Track C2).
//!
//! **Subscribe** types (`[[capture.custom_data]]`) and **request** types
//! (`[[capture.custom_data_requests]]`) are declared once here. Parse, validate,
//! register, and error messages all consult this module — adding a type should
//! not require hunting through `runner` and `config` for string literals.

mod build;
mod register;
mod validate;

use anyhow::{bail, Result};
use nautilus_model::data::DataType;

use crate::config::VenueRuntimeConfig;

pub use build::build_request_data_type;
pub use register::{register_request_types, register_subscribe_types};
pub use validate::{validate_request_data_type, validate_subscribe_data_type};

/// Capture path: stream subscribe vs HTTP/request poll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CustomDataChannel {
    /// `subscribe_data` → live `on_data`.
    Subscribe,
    /// `request_data` → response handler (poll jobs).
    Request,
}

/// Venue class required in `[[venues]]` for a known custom type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CustomDataVenue {
    #[cfg(feature = "venue-binance")]
    BinanceFutures,
    #[cfg(feature = "venue-deribit")]
    Deribit,
    #[cfg(feature = "venue-hyperliquid")]
    Hyperliquid,
}

/// One known custom-data type compiled into this binary (feature-gated).
///
/// This is the **registry**: every supported `type_name` maps to exactly one
/// variant, with a fixed channel and venue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnownCustomDataType {
    #[cfg(feature = "venue-binance")]
    BinanceFuturesLiquidation,
    #[cfg(feature = "venue-binance")]
    BinanceFuturesTicker,
    #[cfg(feature = "venue-deribit")]
    DeribitVolatilityIndex,
    #[cfg(feature = "venue-hyperliquid")]
    HyperliquidOpenInterest,
    #[cfg(feature = "venue-deribit")]
    DeribitBookSummary,
}

impl KnownCustomDataType {
    pub const fn type_name(self) -> &'static str {
        match self {
            #[cfg(feature = "venue-binance")]
            Self::BinanceFuturesLiquidation => "BinanceFuturesLiquidation",
            #[cfg(feature = "venue-binance")]
            Self::BinanceFuturesTicker => "BinanceFuturesTicker",
            #[cfg(feature = "venue-deribit")]
            Self::DeribitVolatilityIndex => "DeribitVolatilityIndex",
            #[cfg(feature = "venue-hyperliquid")]
            Self::HyperliquidOpenInterest => "HyperliquidOpenInterest",
            #[cfg(feature = "venue-deribit")]
            Self::DeribitBookSummary => "DeribitBookSummary",
        }
    }

    pub const fn channel(self) -> CustomDataChannel {
        match self {
            #[cfg(feature = "venue-binance")]
            Self::BinanceFuturesLiquidation | Self::BinanceFuturesTicker => {
                CustomDataChannel::Subscribe
            }
            #[cfg(feature = "venue-deribit")]
            Self::DeribitVolatilityIndex => CustomDataChannel::Subscribe,
            #[cfg(feature = "venue-hyperliquid")]
            Self::HyperliquidOpenInterest => CustomDataChannel::Subscribe,
            #[cfg(feature = "venue-deribit")]
            Self::DeribitBookSummary => CustomDataChannel::Request,
        }
    }

    pub const fn venue(self) -> CustomDataVenue {
        match self {
            #[cfg(feature = "venue-binance")]
            Self::BinanceFuturesLiquidation | Self::BinanceFuturesTicker => {
                CustomDataVenue::BinanceFutures
            }
            #[cfg(feature = "venue-deribit")]
            Self::DeribitVolatilityIndex | Self::DeribitBookSummary => CustomDataVenue::Deribit,
            #[cfg(feature = "venue-hyperliquid")]
            Self::HyperliquidOpenInterest => CustomDataVenue::Hyperliquid,
        }
    }

    pub fn from_type_name(type_name: &str) -> Option<Self> {
        ALL.iter()
            .copied()
            .find(|entry| entry.type_name() == type_name)
    }

    pub fn is_subscribe(self) -> bool {
        matches!(self.channel(), CustomDataChannel::Subscribe)
    }

    pub fn is_request(self) -> bool {
        matches!(self.channel(), CustomDataChannel::Request)
    }
}

/// All known types enabled in this build (order is stable for error messages).
pub const ALL: &[KnownCustomDataType] = &[
    #[cfg(feature = "venue-binance")]
    KnownCustomDataType::BinanceFuturesLiquidation,
    #[cfg(feature = "venue-binance")]
    KnownCustomDataType::BinanceFuturesTicker,
    #[cfg(feature = "venue-deribit")]
    KnownCustomDataType::DeribitVolatilityIndex,
    #[cfg(feature = "venue-hyperliquid")]
    KnownCustomDataType::HyperliquidOpenInterest,
    #[cfg(feature = "venue-deribit")]
    KnownCustomDataType::DeribitBookSummary,
];

pub fn subscribe_types() -> impl Iterator<Item = KnownCustomDataType> {
    ALL.iter().copied().filter(|entry| entry.is_subscribe())
}

pub fn request_types() -> impl Iterator<Item = KnownCustomDataType> {
    ALL.iter().copied().filter(|entry| entry.is_request())
}

pub fn supported_subscribe_csv() -> String {
    join_type_names(subscribe_types())
}

pub fn supported_request_csv() -> String {
    join_type_names(request_types())
}

fn join_type_names(iter: impl Iterator<Item = KnownCustomDataType>) -> String {
    let names: Vec<&'static str> = iter.map(KnownCustomDataType::type_name).collect();
    if names.is_empty() {
        "(none; enable relevant venue-* features)".to_string()
    } else {
        names.join(", ")
    }
}

pub(crate) fn require_venue(
    venues: &[VenueRuntimeConfig],
    requirement: CustomDataVenue,
    error: &str,
) -> Result<()> {
    let ok = venues.iter().any(|venue| venue_matches(venue, requirement));
    if ok {
        Ok(())
    } else {
        bail!("{error}")
    }
}

fn venue_matches(venue: &VenueRuntimeConfig, requirement: CustomDataVenue) -> bool {
    match (requirement, venue) {
        #[cfg(feature = "venue-binance")]
        (CustomDataVenue::BinanceFutures, VenueRuntimeConfig::BinanceFutures { .. }) => true,
        #[cfg(feature = "venue-deribit")]
        (CustomDataVenue::Deribit, VenueRuntimeConfig::Deribit { .. }) => true,
        #[cfg(feature = "venue-hyperliquid")]
        (CustomDataVenue::Hyperliquid, VenueRuntimeConfig::Hyperliquid { .. }) => true,
        #[allow(unreachable_patterns)]
        _ => false,
    }
}

pub(crate) fn string_metadata<'a>(data_type: &'a DataType, key: &str) -> Option<&'a str> {
    data_type
        .metadata()
        .as_ref()
        .and_then(|metadata| metadata.get(key))
        .and_then(|value| value.as_str())
}

pub(crate) fn ensure_non_empty(value: &str, error: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{error}");
    }
    Ok(())
}

#[cfg_attr(
    not(any(feature = "venue-binance", feature = "venue-hyperliquid")),
    allow(dead_code)
)]
pub(crate) fn ensure_identifier_matches(
    identifier: Option<&str>,
    expected: &str,
    type_name: &str,
) -> Result<()> {
    if let Some(identifier) = identifier {
        if identifier != expected {
            bail!(
                "custom_data {type_name} identifier `{identifier}` must match metadata.instrument_id `{expected}`"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_partitions_subscribe_and_request() {
        for entry in ALL {
            match entry.channel() {
                CustomDataChannel::Subscribe => assert!(entry.is_subscribe()),
                CustomDataChannel::Request => assert!(entry.is_request()),
            }
            assert_eq!(
                KnownCustomDataType::from_type_name(entry.type_name()),
                Some(*entry)
            );
        }
    }

    #[cfg(feature = "venue-deribit")]
    #[test]
    fn deribit_book_summary_is_request_only() {
        let entry = KnownCustomDataType::from_type_name("DeribitBookSummary").expect("registered");
        assert!(entry.is_request());
        assert!(!entry.is_subscribe());
        assert!(supported_request_csv().contains("DeribitBookSummary"));
        assert!(!supported_subscribe_csv().contains("DeribitBookSummary"));
    }

    #[cfg(feature = "venue-binance")]
    #[test]
    fn binance_liquidation_is_subscribe_only() {
        let entry =
            KnownCustomDataType::from_type_name("BinanceFuturesLiquidation").expect("registered");
        assert!(entry.is_subscribe());
        assert!(!supported_request_csv().contains("BinanceFuturesLiquidation"));
    }

    /// C3: request-only type must not be accepted on the subscribe channel.
    #[cfg(feature = "venue-deribit")]
    #[test]
    fn rejects_request_type_on_subscribe_channel() {
        use nautilus_model::data::DataType;

        let data_type = DataType::new("DeribitBookSummary", None, None);
        let err = validate_subscribe_data_type(&data_type, &[])
            .expect_err("request-only type must fail on subscribe channel");
        let msg = err.to_string();
        assert!(
            msg.contains("request-only") && msg.contains("custom_data_requests"),
            "unexpected error: {msg}"
        );
    }

    /// C3: subscribe-only type must not be accepted on the request channel.
    #[cfg(feature = "venue-deribit")]
    #[test]
    fn rejects_subscribe_type_on_request_channel() {
        use nautilus_model::data::DataType;

        let data_type = DataType::new("DeribitVolatilityIndex", None, None);
        let err = validate_request_data_type(&data_type, &[])
            .expect_err("subscribe-only type must fail on request channel");
        let msg = err.to_string();
        assert!(
            msg.contains("subscribe-only") && msg.contains("custom_data"),
            "unexpected error: {msg}"
        );
    }

    /// C3: parse path also rejects subscribe-only names in custom_data_requests.
    #[cfg(feature = "venue-deribit")]
    #[test]
    fn build_request_rejects_subscribe_only_type_name() {
        let err = build_request_data_type(
            "DeribitVolatilityIndex",
            &std::collections::BTreeMap::new(),
            None,
        )
        .expect_err("subscribe-only type must fail request builder");
        let msg = err.to_string();
        assert!(
            msg.contains("subscribe-only") || msg.contains("custom_data"),
            "unexpected error: {msg}"
        );
    }
}
