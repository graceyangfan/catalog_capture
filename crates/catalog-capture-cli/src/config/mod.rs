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

//! CLI TOML configuration: load, resolve, and typed runtime views.
//!
//! Split by concern (Track C1): runtime/output, capture plan, custom data,
//! option/hip4 universe, and venues.

mod capture;
mod custom;
mod hip4;
mod option_universe;
mod output;
mod plan;
mod runtime;
mod venues;

#[cfg(test)]
mod tests;

use std::{fs, path::Path};

use anyhow::{bail, Context, Result};
use catalog_capture_core::{
    validate_capture_config, CaptureConfig, CapturePlan, Hip4UniverseSpec, OptionUniverseSpec,
};

// Field type for CliConfigFile; tests re-export selectors via super::*.
#[cfg(not(test))]
use capture::CaptureConfigFile;
#[cfg(test)]
pub use capture::{CaptureConfigFile, InstrumentSelector};
#[cfg(test)]
pub use custom::{CustomDataRequestSelector, CustomDataSelector};
#[cfg(test)]
pub use hip4::Hip4UniverseSelector;
#[cfg(test)]
pub use option_universe::{ExpiryPolicySelector, OptionUniverseSelector, StrikePolicySelector};

pub use output::OutputConfig;
pub use runtime::{MetricsExportRuntimeConfig, RuntimeConfig};
pub use venues::{VenueConfig, VenueRuntimeConfig};

use custom::{parse_custom_data_request_specs, parse_custom_data_specs};
use hip4::parse_hip4_universe_specs;
use option_universe::parse_option_universe_specs;
use output::{parse_compression, parse_layout_compatibility, parse_overflow_policy};
use plan::{
    parse_bar_specs, parse_book_delta_specs, parse_forward_price_specs, parse_funding_rate_specs,
    parse_index_price_specs, parse_instrument_close_specs, parse_instrument_specs,
    parse_instrument_status_specs, parse_mark_price_specs, parse_option_greeks_specs,
    parse_quote_specs, parse_trade_specs,
};
use venues::{parse_venue, validate_unique_venue_ids};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CliConfigFile {
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub output: OutputConfig,
    #[serde(default)]
    pub capture: CaptureConfigFile,
    #[serde(default)]
    pub venues: Vec<VenueConfig>,
}

#[derive(Debug, Clone)]
pub struct EffectiveConfig {
    pub runtime: RuntimeConfig,
    pub capture: CaptureConfig,
    pub plan: CapturePlan,
    pub option_universes: Vec<OptionUniverseSpec>,
    pub hip4_universes: Vec<Hip4UniverseSpec>,
    pub venues: Vec<VenueRuntimeConfig>,
}

pub fn load_config(path: &Path) -> Result<CliConfigFile> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("failed to parse TOML {}", path.display()))
}

pub fn resolve_config(config: CliConfigFile) -> Result<EffectiveConfig> {
    if config.venues.is_empty() {
        bail!("at least one [[venues]] entry is required");
    }

    let venues = config
        .venues
        .into_iter()
        .map(parse_venue)
        .collect::<Result<Vec<_>>>()?;
    validate_unique_venue_ids(&venues)?;

    let capture = CaptureConfig {
        enabled: true,
        catalog_uri: config.output.catalog_uri,
        lifecycle: config.output.lifecycle,
        queue_capacity: config.output.queue_capacity,
        flush_rows: config.output.flush_rows,
        flush_interval_ms: config.output.flush_interval_ms,
        max_buffer_bytes: config.output.max_buffer_bytes,
        max_total_buffer_bytes: config.output.max_total_buffer_bytes,
        max_active_partitions: config.output.max_active_partitions,
        compression: parse_compression(&config.output.compression)?,
        overflow_policy: parse_overflow_policy(&config.output.overflow_policy)?,
        layout_compatibility: parse_layout_compatibility(&config.output.layout_compatibility)?,
    };
    validate_capture_config(&capture)?;

    let plan = CapturePlan {
        instruments: parse_instrument_specs(&config.capture.instruments)?,
        quotes: parse_quote_specs(&config.capture.quotes)?,
        trades: parse_trade_specs(&config.capture.trades)?,
        bars: parse_bar_specs(&config.capture.bars)?,
        book_deltas: parse_book_delta_specs(&config.capture.book_deltas)?,
        mark_prices: parse_mark_price_specs(&config.capture.mark_prices)?,
        index_prices: parse_index_price_specs(&config.capture.index_prices)?,
        funding_rates: parse_funding_rate_specs(&config.capture.funding_rates)?,
        instrument_statuses: parse_instrument_status_specs(&config.capture.instrument_statuses)?,
        instrument_closes: parse_instrument_close_specs(&config.capture.instrument_closes)?,
        option_greeks: parse_option_greeks_specs(&config.capture.option_greeks)?,
        forward_prices: parse_forward_price_specs(&config.capture.forward_prices)?,
        custom_data: parse_custom_data_specs(&config.capture.custom_data)?,
        custom_data_requests: parse_custom_data_request_specs(
            &config.capture.custom_data_requests,
        )?,
    };
    let option_universes = parse_option_universe_specs(&config.capture.option_universe)?;
    let hip4_universes = parse_hip4_universe_specs(&config.capture.hip4_universe)?;

    if plan.is_empty() && option_universes.is_empty() && hip4_universes.is_empty() {
        bail!("capture plan is empty; enable at least one capture family");
    }

    Ok(EffectiveConfig {
        runtime: config.runtime,
        capture,
        plan,
        option_universes,
        hip4_universes,
        venues,
    })
}

pub fn render_effective_config(config: &CliConfigFile) -> Result<String> {
    toml::to_string_pretty(config)
        .map_err(|err| anyhow::anyhow!("failed to render effective TOML: {err}"))
}
