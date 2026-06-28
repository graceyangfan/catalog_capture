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

use std::{collections::BTreeMap, str::FromStr};

use nautilus_core::UnixNanos;
use nautilus_model::{identifiers::InstrumentId, types::Price};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionUniverseFamily {
    Instruments,
    Quotes,
    Trades,
    MarkPrices,
    IndexPrices,
    FundingRates,
    InstrumentStatuses,
    InstrumentCloses,
    OptionGreeks,
    ForwardPrices,
    BookDeltas,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpiryPolicy {
    Nearest { days_max: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrikePolicy {
    AtmRelative {
        strikes_above: usize,
        strikes_below: usize,
    },
    OiRanked {
        top_n: usize,
    },
    AllStrikes,
}

impl StrikePolicy {
    #[must_use]
    pub const fn selection_mode(&self) -> &'static str {
        match self {
            Self::AtmRelative { .. } => "atm_relative",
            Self::OiRanked { .. } => "oi_ranked",
            Self::AllStrikes => "all",
        }
    }

    #[must_use]
    pub const fn oi_ranked_top_n(&self) -> Option<usize> {
        match self {
            Self::OiRanked { top_n } => Some(*top_n),
            Self::AtmRelative { .. } | Self::AllStrikes => None,
        }
    }

    #[must_use]
    pub const fn requires_open_interest(&self) -> bool {
        matches!(self, Self::OiRanked { .. })
    }
}

pub type StrikeOpenInterestByStrike = BTreeMap<Price, f64>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionUniverseVenueKind {
    Deribit,
    Bybit,
    Okx,
}

impl OptionUniverseVenueKind {
    #[must_use]
    pub const fn supports_runtime_refresh(self) -> bool {
        matches!(self, Self::Deribit | Self::Bybit | Self::Okx)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionUniverseSpec {
    pub venue_id: String,
    pub underlying: String,
    pub settlement_currency: Option<String>,
    pub include_perp: bool,
    pub families: Vec<OptionUniverseFamily>,
    pub expiry_policy: ExpiryPolicy,
    pub strike_policy: StrikePolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedOptionUniverse {
    pub resolved_at_ns: UnixNanos,
    pub selected_expiry_ns: UnixNanos,
    pub atm_reference: Price,
    pub atm_reference_source: Option<String>,
    pub selected_strikes: Vec<Price>,
    pub perp_instrument_id: Option<InstrumentId>,
    pub option_instrument_ids: Vec<InstrumentId>,
    pub all_instrument_ids: Vec<InstrumentId>,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum OptionUniverseResolveError {
    #[error(
        "no matching option instruments found for venue_id={venue_id} underlying={underlying}"
    )]
    NoMatchingOptions {
        venue_id: String,
        underlying: String,
    },
    #[error("no expiry matched the configured expiry policy for venue_id={venue_id} underlying={underlying}")]
    NoMatchingExpiry {
        venue_id: String,
        underlying: String,
    },
    #[error("no call/put pairs remained after strike filtering for venue_id={venue_id} underlying={underlying}")]
    NoStrikePairs {
        venue_id: String,
        underlying: String,
    },
    #[error("include_perp=true requires a resolved perpetual instrument for venue_id={venue_id} underlying={underlying}")]
    MissingPerpetual {
        venue_id: String,
        underlying: String,
    },
    #[error("capture.option_universe venue_id={venue_id} requires settlement_currency to derive the hedge instrument")]
    MissingSettlementCurrency { venue_id: String },
    #[error("no option instrument matched the configured expiry policy for venue_id={venue_id} underlying={underlying}")]
    NoReferenceInstrument {
        venue_id: String,
        underlying: String,
    },
    #[error(
        "oi_ranked strike policy requires open interest data for venue_id={venue_id} underlying={underlying}"
    )]
    MissingOpenInterest {
        venue_id: String,
        underlying: String,
    },
}

pub fn okx_instrument_family(
    spec: &OptionUniverseSpec,
) -> Result<String, OptionUniverseResolveError> {
    let Some(settlement_currency) = spec.settlement_currency.as_deref() else {
        return Err(OptionUniverseResolveError::MissingSettlementCurrency {
            venue_id: spec.venue_id.clone(),
        });
    };
    Ok(format!("{}-{settlement_currency}", spec.underlying))
}

pub fn derive_perp_instrument_id(
    spec: &OptionUniverseSpec,
    venue: OptionUniverseVenueKind,
) -> Result<InstrumentId, OptionUniverseResolveError> {
    let instrument_id = match venue {
        OptionUniverseVenueKind::Deribit => format!("{}-PERPETUAL.DERIBIT", spec.underlying),
        OptionUniverseVenueKind::Bybit => {
            let settlement_currency = spec.settlement_currency.as_deref().ok_or_else(|| {
                OptionUniverseResolveError::MissingSettlementCurrency {
                    venue_id: spec.venue_id.clone(),
                }
            })?;
            format!("{}{}-LINEAR.BYBIT", spec.underlying, settlement_currency)
        }
        OptionUniverseVenueKind::Okx => format!("{}-SWAP.OKX", okx_instrument_family(spec)?),
    };

    InstrumentId::from_str(instrument_id.as_str()).map_err(|_| {
        OptionUniverseResolveError::MissingPerpetual {
            venue_id: spec.venue_id.clone(),
            underlying: spec.underlying.clone(),
        }
    })
}
