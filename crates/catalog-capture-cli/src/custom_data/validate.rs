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

//! Runtime validation of custom-data `DataType` values against the registry.

use anyhow::{bail, Result};
use nautilus_model::data::DataType;

#[cfg(any(feature = "venue-binance", feature = "venue-hyperliquid"))]
use super::ensure_identifier_matches;
use super::{
    ensure_non_empty, require_venue, string_metadata, supported_request_csv,
    supported_subscribe_csv, KnownCustomDataType,
};
use crate::config::VenueRuntimeConfig;

pub fn validate_subscribe_data_type(
    data_type: &DataType,
    venues: &[VenueRuntimeConfig],
) -> Result<()> {
    let name = data_type.type_name();
    match KnownCustomDataType::from_type_name(name) {
        Some(entry) if entry.is_request() => {
            bail!(
                "custom_data type_name `{name}` is request-only; use [[capture.custom_data_requests]] \
                 (Nautilus request_data), not [[capture.custom_data]] (subscribe_data)"
            );
        }
        Some(entry) if entry.is_subscribe() => entry.validate_payload(data_type, venues),
        Some(_) => unreachable!("channel is only subscribe or request"),
        None => bail!(
            "unknown custom_data type_name `{name}`; supported subscribe types: {}. \
             For request-only types use [[capture.custom_data_requests]]",
            supported_subscribe_csv()
        ),
    }
}

pub fn validate_request_data_type(
    data_type: &DataType,
    venues: &[VenueRuntimeConfig],
) -> Result<()> {
    let name = data_type.type_name();
    match KnownCustomDataType::from_type_name(name) {
        Some(entry) if entry.is_subscribe() => {
            bail!(
                "custom_data_requests type_name `{name}` is subscribe-only; use [[capture.custom_data]] \
                 (Nautilus subscribe_data), not [[capture.custom_data_requests]] (request_data)"
            );
        }
        Some(entry) if entry.is_request() => entry.validate_payload(data_type, venues),
        Some(_) => unreachable!("channel is only subscribe or request"),
        None => bail!(
            "unknown custom_data_requests type_name `{name}`; supported request types: {}",
            supported_request_csv()
        ),
    }
}

impl KnownCustomDataType {
    fn validate_payload(self, data_type: &DataType, venues: &[VenueRuntimeConfig]) -> Result<()> {
        match self {
            #[cfg(feature = "venue-binance")]
            Self::BinanceFuturesLiquidation => {
                require_venue(
                    venues,
                    self.venue(),
                    "custom_data BinanceFuturesLiquidation requires at least one [[venues]] entry with kind = \"binance_futures\"",
                )?;
                let identifier = data_type.identifier();
                let instrument_id = string_metadata(data_type, "instrument_id");
                if identifier.is_some() && instrument_id.is_none() {
                    bail!(
                        "custom_data BinanceFuturesLiquidation identifier requires metadata.instrument_id; \
                         omit both fields for all-market capture"
                    );
                }
                if let Some(instrument_id) = instrument_id {
                    ensure_non_empty(
                        instrument_id,
                        "custom_data BinanceFuturesLiquidation metadata.instrument_id must be non-empty when provided",
                    )?;
                    ensure_identifier_matches(
                        identifier,
                        instrument_id,
                        "BinanceFuturesLiquidation",
                    )?;
                }
                Ok(())
            }
            #[cfg(feature = "venue-binance")]
            Self::BinanceFuturesTicker => {
                require_venue(
                    venues,
                    self.venue(),
                    "custom_data BinanceFuturesTicker requires at least one [[venues]] entry with kind = \"binance_futures\"",
                )?;
                let Some(instrument_id) = string_metadata(data_type, "instrument_id") else {
                    bail!(
                        "custom_data BinanceFuturesTicker requires metadata.instrument_id \
                         (for example `ETHUSDT-PERP.BINANCE`)"
                    );
                };
                ensure_non_empty(
                    instrument_id,
                    "custom_data BinanceFuturesTicker metadata.instrument_id must be non-empty",
                )?;
                ensure_identifier_matches(
                    data_type.identifier(),
                    instrument_id,
                    "BinanceFuturesTicker",
                )
            }
            #[cfg(feature = "venue-deribit")]
            Self::DeribitVolatilityIndex => {
                require_venue(
                    venues,
                    self.venue(),
                    "custom_data DeribitVolatilityIndex requires at least one [[venues]] entry with kind = \"deribit\"",
                )?;
                let Some(index_name) = string_metadata(data_type, "index_name") else {
                    bail!(
                        "custom_data DeribitVolatilityIndex requires metadata.index_name \
                         (for example `btc_usd`)"
                    );
                };
                ensure_non_empty(
                    index_name,
                    "custom_data DeribitVolatilityIndex metadata.index_name must be non-empty",
                )
            }
            #[cfg(feature = "venue-hyperliquid")]
            Self::HyperliquidOpenInterest => {
                require_venue(
                    venues,
                    self.venue(),
                    "custom_data HyperliquidOpenInterest requires at least one [[venues]] entry with kind = \"hyperliquid\"",
                )?;
                let Some(instrument_id) = string_metadata(data_type, "instrument_id") else {
                    bail!(
                        "custom_data HyperliquidOpenInterest requires metadata.instrument_id \
                         (for example `ETH-USD-PERP.HYPERLIQUID`)"
                    );
                };
                ensure_non_empty(
                    instrument_id,
                    "custom_data HyperliquidOpenInterest metadata.instrument_id must be non-empty",
                )?;
                ensure_identifier_matches(
                    data_type.identifier(),
                    instrument_id,
                    "HyperliquidOpenInterest",
                )
            }
            #[cfg(feature = "venue-deribit")]
            Self::DeribitBookSummary => {
                require_venue(
                    venues,
                    self.venue(),
                    "custom_data_requests DeribitBookSummary requires at least one [[venues]] entry with kind = \"deribit\"",
                )?;
                let Some(currency) = string_metadata(data_type, "currency") else {
                    bail!(
                        "custom_data_requests DeribitBookSummary requires metadata.currency \
                         (for example `BTC`)"
                    );
                };
                ensure_non_empty(
                    currency,
                    "custom_data_requests DeribitBookSummary metadata.currency must be non-empty",
                )
            }
        }
    }
}
