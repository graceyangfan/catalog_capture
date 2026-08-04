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
use catalog_capture_core::{ExpiryPolicy, OptionUniverseFamily, OptionUniverseSpec, StrikePolicy};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionUniverseSelector {
    pub venue_id: String,
    pub underlying: String,
    #[serde(default)]
    pub settlement_currency: Option<String>,
    #[serde(default)]
    pub include_perp: bool,
    #[serde(default)]
    pub families: Vec<String>,
    pub expiry_policy: ExpiryPolicySelector,
    pub strike_policy: StrikePolicySelector,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpiryPolicySelector {
    pub mode: String,
    pub days_max: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrikePolicySelector {
    pub mode: String,
    #[serde(default)]
    pub strikes_above: usize,
    #[serde(default)]
    pub strikes_below: usize,
    #[serde(default)]
    pub top_n: Option<usize>,
}

pub(crate) fn parse_option_universe_specs(
    items: &[OptionUniverseSelector],
) -> Result<Vec<OptionUniverseSpec>> {
    items.iter().map(parse_option_universe_spec).collect()
}

pub(crate) fn parse_option_universe_spec(
    item: &OptionUniverseSelector,
) -> Result<OptionUniverseSpec> {
    if item.venue_id.trim().is_empty() {
        bail!("capture.option_universe.venue_id must be non-empty");
    }
    if item.underlying.trim().is_empty() {
        bail!("capture.option_universe.underlying must be non-empty");
    }
    if item.families.is_empty() {
        bail!("capture.option_universe.families must be non-empty");
    }

    let families = item
        .families
        .iter()
        .map(|family| parse_option_universe_family(family))
        .collect::<Result<Vec<_>>>()?;

    let spec = OptionUniverseSpec {
        venue_id: item.venue_id.trim().to_string(),
        underlying: item.underlying.trim().to_ascii_uppercase(),
        settlement_currency: item
            .settlement_currency
            .as_ref()
            .map(|value| value.trim().to_ascii_uppercase()),
        include_perp: item.include_perp,
        families,
        expiry_policy: parse_expiry_policy(&item.expiry_policy)?,
        strike_policy: parse_strike_policy(&item.strike_policy)?,
    };

    validate_option_universe_family_shape(&spec)?;
    Ok(spec)
}

pub(crate) fn parse_option_universe_family(value: &str) -> Result<OptionUniverseFamily> {
    match value.to_ascii_lowercase().as_str() {
        "instruments" => Ok(OptionUniverseFamily::Instruments),
        "quotes" => Ok(OptionUniverseFamily::Quotes),
        "trades" => Ok(OptionUniverseFamily::Trades),
        "mark_prices" => Ok(OptionUniverseFamily::MarkPrices),
        "index_prices" => Ok(OptionUniverseFamily::IndexPrices),
        "funding_rates" => Ok(OptionUniverseFamily::FundingRates),
        "instrument_statuses" => Ok(OptionUniverseFamily::InstrumentStatuses),
        "instrument_closes" => Ok(OptionUniverseFamily::InstrumentCloses),
        "option_greeks" => Ok(OptionUniverseFamily::OptionGreeks),
        "forward_prices" => Ok(OptionUniverseFamily::ForwardPrices),
        "book_deltas" => Ok(OptionUniverseFamily::BookDeltas),
        other => bail!(
            "unsupported capture.option_universe family {other}; expected instruments|quotes|trades|mark_prices|index_prices|funding_rates|instrument_statuses|instrument_closes|option_greeks|forward_prices|book_deltas"
        ),
    }
}

pub(crate) fn parse_expiry_policy(policy: &ExpiryPolicySelector) -> Result<ExpiryPolicy> {
    match policy.mode.to_ascii_lowercase().as_str() {
        "nearest" => {
            if policy.days_max == 0 {
                bail!("capture.option_universe.expiry_policy.days_max must be > 0");
            }
            Ok(ExpiryPolicy::Nearest {
                days_max: policy.days_max,
            })
        }
        other => bail!(
            "unsupported capture.option_universe.expiry_policy.mode {other}; expected nearest"
        ),
    }
}

pub(crate) fn parse_strike_policy(policy: &StrikePolicySelector) -> Result<StrikePolicy> {
    match policy.mode.to_ascii_lowercase().as_str() {
        "atm_relative" => Ok(StrikePolicy::AtmRelative {
            strikes_above: policy.strikes_above,
            strikes_below: policy.strikes_below,
        }),
        "oi_ranked" => {
            let top_n = policy.top_n.filter(|value| *value > 0).ok_or_else(|| {
                anyhow::anyhow!(
                    "capture.option_universe.strike_policy.mode oi_ranked requires top_n > 0"
                )
            })?;
            Ok(StrikePolicy::OiRanked { top_n })
        }
        "all" => Ok(StrikePolicy::AllStrikes),
        other => bail!(
            "unsupported capture.option_universe.strike_policy.mode {other}; expected atm_relative, oi_ranked, or all"
        ),
    }
}

pub(crate) fn validate_option_universe_family_shape(spec: &OptionUniverseSpec) -> Result<()> {
    let needs_perp = spec.families.iter().any(|family| {
        matches!(
            family,
            OptionUniverseFamily::IndexPrices | OptionUniverseFamily::FundingRates
        )
    });
    if needs_perp && !spec.include_perp {
        bail!(
            "capture.option_universe families index_prices/funding_rates require include_perp = true"
        );
    }
    Ok(())
}
