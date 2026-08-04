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
use catalog_capture_core::{Hip4UniverseFamily, Hip4UniverseSpec};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hip4UniverseSelector {
    pub venue_id: String,
    pub underlying: String,
    pub period: String,
    pub market_class: String,
    #[serde(default)]
    pub include_fallback: bool,
    #[serde(default = "default_hip4_include_perp_mark")]
    pub include_perp_mark: bool,
    #[serde(default)]
    pub families: Vec<String>,
}

const fn default_hip4_include_perp_mark() -> bool {
    true
}

pub(crate) fn parse_hip4_universe_specs(
    items: &[Hip4UniverseSelector],
) -> Result<Vec<Hip4UniverseSpec>> {
    items.iter().map(parse_hip4_universe_spec).collect()
}

pub(crate) fn parse_hip4_universe_spec(item: &Hip4UniverseSelector) -> Result<Hip4UniverseSpec> {
    if item.venue_id.trim().is_empty() {
        bail!("capture.hip4_universe.venue_id must be non-empty");
    }
    if item.underlying.trim().is_empty() {
        bail!("capture.hip4_universe.underlying must be non-empty");
    }
    if item.period.trim().is_empty() {
        bail!("capture.hip4_universe.period must be non-empty");
    }
    if item.market_class.trim().is_empty() {
        bail!("capture.hip4_universe.market_class must be non-empty");
    }
    if item.families.is_empty() {
        bail!("capture.hip4_universe.families must be non-empty");
    }

    let families = item
        .families
        .iter()
        .map(|family| parse_hip4_universe_family(family))
        .collect::<Result<Vec<_>>>()?;

    let spec = Hip4UniverseSpec {
        venue_id: item.venue_id.trim().to_string(),
        underlying: item.underlying.trim().to_ascii_uppercase(),
        period: item.period.trim().to_string(),
        market_class: item.market_class.trim().to_string(),
        include_fallback: item.include_fallback,
        include_perp_mark: item.include_perp_mark,
        families,
    };
    validate_hip4_universe_family_shape(&spec)?;
    Ok(spec)
}

pub(crate) fn parse_hip4_universe_family(value: &str) -> Result<Hip4UniverseFamily> {
    match value.to_ascii_lowercase().as_str() {
        "instruments" => Ok(Hip4UniverseFamily::Instruments),
        "quotes" => Ok(Hip4UniverseFamily::Quotes),
        "trades" => Ok(Hip4UniverseFamily::Trades),
        "mark_prices" => Ok(Hip4UniverseFamily::MarkPrices),
        other => bail!(
            "unsupported capture.hip4_universe family {other}; \
             expected instruments|quotes|trades|mark_prices"
        ),
    }
}

pub(crate) fn validate_hip4_universe_family_shape(spec: &Hip4UniverseSpec) -> Result<()> {
    if spec.include_perp_mark
        && !spec
            .families
            .iter()
            .any(|family| matches!(family, Hip4UniverseFamily::MarkPrices))
    {
        bail!("capture.hip4_universe include_perp_mark = true requires mark_prices in families");
    }
    Ok(())
}
