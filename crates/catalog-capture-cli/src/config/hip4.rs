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

use anyhow::{Result, bail};
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

/// Dynamic selectors share the ordinary capture family tables.
pub(crate) fn resolve_rolling_capture(
    capture: &super::capture::CaptureConfigFile,
) -> Result<Option<catalog_capture_core::hip4::rolling::Hip4RollingConfig>> {
    use catalog_capture_core::hip4::rolling::Hip4RollingFamily;
    let mut families = Vec::new();
    for (items, family) in [
        (&capture.quotes, Hip4RollingFamily::Quotes),
        (&capture.trades, Hip4RollingFamily::Trades),
    ] {
        for item in items.iter().filter(|item| item.hip4_rolling) {
            anyhow::ensure!(
                item.instrument_id.is_empty(),
                "hip4_rolling and instrument_id are mutually exclusive"
            );
            families.push(family);
        }
    }
    for item in capture.book_deltas.iter().filter(|item| item.hip4_rolling) {
        anyhow::ensure!(
            item.instrument_id.is_empty(),
            "hip4_rolling and instrument_id are mutually exclusive"
        );
        anyhow::ensure!(
            item.book_type == "L2_MBP" && item.depth.is_none(),
            "HIP-4 rolling book_deltas requires L2_MBP with default depth"
        );
        families.push(Hip4RollingFamily::BookDeltas);
    }
    for items in [
        &capture.instruments,
        &capture.mark_prices,
        &capture.index_prices,
        &capture.funding_rates,
        &capture.instrument_statuses,
        &capture.instrument_closes,
        &capture.option_greeks,
        &capture.forward_prices,
    ] {
        anyhow::ensure!(
            !items.iter().any(|item| item.hip4_rolling),
            "hip4_rolling selectors support quotes, trades and book_deltas"
        );
    }
    anyhow::ensure!(
        !capture.book_depth.iter().any(|item| item.hip4_rolling),
        "HIP-4 rolling uses book_deltas for L2 snapshots"
    );
    let Some(mut config) = capture.hip4_rolling.clone() else {
        anyhow::ensure!(
            families.is_empty(),
            "hip4_rolling selector requires [capture.hip4_rolling]"
        );
        return Ok(None);
    };
    config.families = families;
    config.validate()?;
    Ok(Some(config))
}
