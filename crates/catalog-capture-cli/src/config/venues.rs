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

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

#[cfg(feature = "venue-binance")]
use nautilus_binance::common::enums::{BinanceEnvironment, BinanceProductType};
#[cfg(feature = "venue-bybit")]
use nautilus_bybit::common::enums::{BybitEnvironment, BybitProductType};
#[cfg(feature = "venue-deribit")]
use nautilus_deribit::{common::enums::DeribitEnvironment, http::models::DeribitProductType};
#[cfg(feature = "venue-hyperliquid")]
use nautilus_hyperliquid::common::enums::HyperliquidEnvironment;
#[cfg(feature = "venue-okx")]
use nautilus_okx::common::enums::{OKXEnvironment, OKXInstrumentType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueConfig {
    pub id: String,
    pub kind: String,
    #[serde(default = "default_binance_environment")]
    pub environment: String,
    #[serde(default = "default_binance_product_type")]
    pub product_type: String,
    /// Deribit / Bybit: product types to load (e.g. `future`, `option`, `linear`).
    #[serde(default)]
    pub product_types: Vec<String>,
    /// OKX-only: instrument types to load (e.g. `swap`, `option`).
    #[serde(default)]
    pub instrument_types: Vec<String>,
    /// OKX-only: instrument families for options (e.g. `BTC-USD`).
    #[serde(default)]
    pub instrument_families: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum VenueRuntimeConfig {
    #[cfg(feature = "venue-binance")]
    BinanceFutures {
        id: String,
        environment: BinanceEnvironment,
        product_type: BinanceProductType,
    },
    #[cfg(feature = "venue-deribit")]
    Deribit {
        id: String,
        environment: DeribitEnvironment,
        product_types: Vec<DeribitProductType>,
    },
    #[cfg(feature = "venue-bybit")]
    Bybit {
        id: String,
        environment: BybitEnvironment,
        product_types: Vec<BybitProductType>,
    },
    #[cfg(feature = "venue-hyperliquid")]
    Hyperliquid {
        id: String,
        environment: HyperliquidEnvironment,
    },
    #[cfg(feature = "venue-okx")]
    Okx {
        id: String,
        environment: OKXEnvironment,
        instrument_types: Vec<OKXInstrumentType>,
        instrument_families: Option<Vec<String>>,
    },
}

impl VenueRuntimeConfig {
    pub fn id(&self) -> &str {
        match self {
            #[cfg(feature = "venue-binance")]
            Self::BinanceFutures { id, .. } => id,
            #[cfg(feature = "venue-deribit")]
            Self::Deribit { id, .. } => id,
            #[cfg(feature = "venue-bybit")]
            Self::Bybit { id, .. } => id,
            #[cfg(feature = "venue-hyperliquid")]
            Self::Hyperliquid { id, .. } => id,
            #[cfg(feature = "venue-okx")]
            Self::Okx { id, .. } => id,
        }
    }
}

// Only referenced from `#[cfg(not(feature = "venue-*"))]` arms; unused when all-venues is on.
#[allow(dead_code)]
fn venue_feature_required(kind: &str, feature: &str) -> Result<VenueRuntimeConfig> {
    bail!(
        "venue kind `{kind}` requires cargo feature `{feature}` \
         (rebuild with `--features {feature}` or `--features all-venues`)"
    )
}

fn default_binance_environment() -> String {
    "testnet".to_string()
}

pub(crate) fn default_binance_product_type() -> String {
    "usd_m".to_string()
}

pub(crate) fn parse_venue(venue: VenueConfig) -> Result<VenueRuntimeConfig> {
    match venue.kind.to_ascii_lowercase().as_str() {
        "binance_futures" => {
            #[cfg(feature = "venue-binance")]
            {
                Ok(VenueRuntimeConfig::BinanceFutures {
                    id: venue.id,
                    environment: parse_binance_environment(&venue.environment)?,
                    product_type: parse_binance_product_type(&venue.product_type)?,
                })
            }
            #[cfg(not(feature = "venue-binance"))]
            {
                venue_feature_required("binance_futures", "venue-binance")
            }
        }
        "deribit" => {
            #[cfg(feature = "venue-deribit")]
            {
                Ok(VenueRuntimeConfig::Deribit {
                    id: venue.id,
                    environment: parse_deribit_environment(&venue.environment)?,
                    product_types: parse_deribit_product_types(&venue.product_types)?,
                })
            }
            #[cfg(not(feature = "venue-deribit"))]
            {
                venue_feature_required("deribit", "venue-deribit")
            }
        }
        "bybit" => {
            #[cfg(feature = "venue-bybit")]
            {
                Ok(VenueRuntimeConfig::Bybit {
                    id: venue.id,
                    environment: parse_bybit_environment(&venue.environment)?,
                    product_types: parse_bybit_product_types(&venue.product_types)?,
                })
            }
            #[cfg(not(feature = "venue-bybit"))]
            {
                venue_feature_required("bybit", "venue-bybit")
            }
        }
        "hyperliquid" => {
            #[cfg(feature = "venue-hyperliquid")]
            {
                Ok(VenueRuntimeConfig::Hyperliquid {
                    id: venue.id,
                    environment: parse_hyperliquid_environment(&venue.environment)?,
                })
            }
            #[cfg(not(feature = "venue-hyperliquid"))]
            {
                venue_feature_required("hyperliquid", "venue-hyperliquid")
            }
        }
        "okx" => {
            #[cfg(feature = "venue-okx")]
            {
                let instrument_types = parse_okx_instrument_types(&venue.instrument_types)?;
                let instrument_families = parse_okx_instrument_families(&venue.instrument_families)?;
                if instrument_types.contains(&OKXInstrumentType::Option)
                    && instrument_families.as_ref().is_none_or(std::vec::Vec::is_empty)
                {
                    bail!(
                        "okx venue requires non-empty instrument_families when instrument_types includes option"
                    );
                }
                Ok(VenueRuntimeConfig::Okx {
                    id: venue.id,
                    environment: parse_okx_environment(&venue.environment)?,
                    instrument_types,
                    instrument_families,
                })
            }
            #[cfg(not(feature = "venue-okx"))]
            {
                venue_feature_required("okx", "venue-okx")
            }
        }
        other => bail!(
            "unsupported venue kind {other}; known kinds: binance_futures, deribit, bybit, hyperliquid, okx \
             (enabled at build time via cargo features venue-* / all-venues)"
        ),
    }
}

pub(crate) fn validate_unique_venue_ids(venues: &[VenueRuntimeConfig]) -> Result<()> {
    let mut ids = std::collections::BTreeSet::new();
    for venue in venues {
        let inserted = ids.insert(venue.id());
        if !inserted {
            bail!(
                "duplicate [[venues]] id {}; venue ids must be unique",
                venue.id()
            );
        }
    }
    Ok(())
}

#[cfg(feature = "venue-binance")]
fn parse_binance_environment(value: &str) -> Result<BinanceEnvironment> {
    match value.to_ascii_lowercase().as_str() {
        "live" => Ok(BinanceEnvironment::Live),
        "testnet" => Ok(BinanceEnvironment::Testnet),
        "demo" => Ok(BinanceEnvironment::Demo),
        other => bail!("unsupported Binance environment {other}; expected live|testnet|demo"),
    }
}

#[cfg(feature = "venue-deribit")]
fn parse_deribit_environment(value: &str) -> Result<DeribitEnvironment> {
    match value.to_ascii_lowercase().as_str() {
        "mainnet" | "live" => Ok(DeribitEnvironment::Mainnet),
        "testnet" => Ok(DeribitEnvironment::Testnet),
        other => bail!("unsupported Deribit environment {other}; expected mainnet|testnet"),
    }
}

#[cfg(feature = "venue-bybit")]
fn parse_bybit_environment(value: &str) -> Result<BybitEnvironment> {
    match value.to_ascii_lowercase().as_str() {
        "mainnet" | "live" => Ok(BybitEnvironment::Mainnet),
        "testnet" => Ok(BybitEnvironment::Testnet),
        "demo" => Ok(BybitEnvironment::Demo),
        other => bail!("unsupported Bybit environment {other}; expected mainnet|testnet|demo"),
    }
}

#[cfg(feature = "venue-hyperliquid")]
pub(crate) fn parse_hyperliquid_environment(value: &str) -> Result<HyperliquidEnvironment> {
    match value.to_ascii_lowercase().as_str() {
        "mainnet" | "live" => Ok(HyperliquidEnvironment::Mainnet),
        "testnet" => Ok(HyperliquidEnvironment::Testnet),
        other => bail!("unsupported Hyperliquid environment {other}; expected mainnet|testnet"),
    }
}

#[cfg(feature = "venue-bybit")]
fn parse_bybit_product_types(values: &[String]) -> Result<Vec<BybitProductType>> {
    if values.is_empty() {
        return Ok(vec![BybitProductType::Linear, BybitProductType::Option]);
    }

    values
        .iter()
        .map(|value| match value.to_ascii_lowercase().as_str() {
            "linear" => Ok(BybitProductType::Linear),
            "inverse" => Ok(BybitProductType::Inverse),
            "spot" => Ok(BybitProductType::Spot),
            "option" => Ok(BybitProductType::Option),
            other => {
                bail!("unsupported Bybit product_type {other}; expected linear|inverse|spot|option")
            }
        })
        .collect()
}

#[cfg(feature = "venue-okx")]
fn parse_okx_environment(value: &str) -> Result<OKXEnvironment> {
    match value.to_ascii_lowercase().as_str() {
        "live" | "mainnet" => Ok(OKXEnvironment::Live),
        "demo" => Ok(OKXEnvironment::Demo),
        other => bail!("unsupported OKX environment {other}; expected live|demo"),
    }
}

#[cfg(feature = "venue-okx")]
pub(crate) fn parse_okx_instrument_types(values: &[String]) -> Result<Vec<OKXInstrumentType>> {
    if values.is_empty() {
        return Ok(vec![OKXInstrumentType::Swap]);
    }

    values
        .iter()
        .map(|value| match value.to_ascii_lowercase().as_str() {
            "swap" => Ok(OKXInstrumentType::Swap),
            "option" => Ok(OKXInstrumentType::Option),
            "spot" => Ok(OKXInstrumentType::Spot),
            "margin" => Ok(OKXInstrumentType::Margin),
            "futures" => Ok(OKXInstrumentType::Futures),
            "any" => Ok(OKXInstrumentType::Any),
            "events" => Ok(OKXInstrumentType::Events),
            other => bail!(
                "unsupported OKX instrument_type {other}; expected swap|option|spot|margin|futures|any|events"
            ),
        })
        .collect()
}

#[cfg(feature = "venue-okx")]
fn parse_okx_instrument_families(values: &[String]) -> Result<Option<Vec<String>>> {
    if values.is_empty() {
        Ok(None)
    } else {
        Ok(Some(values.to_vec()))
    }
}

#[cfg(feature = "venue-deribit")]
fn parse_deribit_product_types(values: &[String]) -> Result<Vec<DeribitProductType>> {
    if values.is_empty() {
        return Ok(vec![DeribitProductType::Future, DeribitProductType::Option]);
    }

    values
        .iter()
        .map(|value| match value.to_ascii_lowercase().as_str() {
            "future" => Ok(DeribitProductType::Future),
            "option" => Ok(DeribitProductType::Option),
            "spot" => Ok(DeribitProductType::Spot),
            "future_combo" => Ok(DeribitProductType::FutureCombo),
            "option_combo" => Ok(DeribitProductType::OptionCombo),
            other => bail!(
                "unsupported Deribit product_type {other}; expected future|option|spot|future_combo|option_combo"
            ),
        })
        .collect()
}

#[cfg(feature = "venue-binance")]
fn parse_binance_product_type(value: &str) -> Result<BinanceProductType> {
    match value.to_ascii_lowercase().as_str() {
        "usd_m" => Ok(BinanceProductType::UsdM),
        "coin_m" => Ok(BinanceProductType::CoinM),
        "spot" => Ok(BinanceProductType::Spot),
        "margin" => Ok(BinanceProductType::Margin),
        "options" => Ok(BinanceProductType::Options),
        other => bail!(
            "unsupported Binance product_type {other}; expected usd_m|coin_m|spot|margin|options"
        ),
    }
}
