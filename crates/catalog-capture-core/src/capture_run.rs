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

//! Startup capture-run metadata (`metadata/capture_run.json`) — Track L7.

use std::{fs, path::Path};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::{metrics_export::unix_time_ms, CapturePlan};

pub const CAPTURE_RUN_FILE: &str = "metadata/capture_run.json";
pub const CAPTURE_RUN_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureRunVenueRecord {
    pub id: String,
    pub kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CaptureRunPlanSummary {
    pub instruments: usize,
    pub quotes: usize,
    pub trades: usize,
    pub bars: usize,
    pub book_deltas: usize,
    pub mark_prices: usize,
    pub index_prices: usize,
    pub funding_rates: usize,
    pub option_greeks: usize,
    pub custom_data: usize,
    pub custom_data_requests: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureRunRecord {
    pub schema_version: u32,
    pub written_at_unix_ms: u64,
    pub node_name: String,
    pub catalog_uri: String,
    pub layout_compatibility: String,
    pub capture_seconds: u64,
    pub venues: Vec<CaptureRunVenueRecord>,
    pub plan: CaptureRunPlanSummary,
    pub option_universe_count: usize,
    pub hip4_universe_count: usize,
    /// Optional pin from `NAUTILUS_TRADER_REF` (or similar) for reproducibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nautilus_trader_ref: Option<String>,
    /// Venue cargo features compiled into this CLI binary.
    pub cli_venue_features: Vec<String>,
}

impl CaptureRunPlanSummary {
    #[must_use]
    pub fn from_plan(plan: &CapturePlan) -> Self {
        Self {
            instruments: plan.instruments.len(),
            quotes: plan.quotes.len(),
            trades: plan.trades.len(),
            bars: plan.bars.len(),
            book_deltas: plan.book_deltas.len(),
            mark_prices: plan.mark_prices.len(),
            index_prices: plan.index_prices.len(),
            funding_rates: plan.funding_rates.len(),
            option_greeks: plan.option_greeks.len(),
            custom_data: plan.custom_data.len(),
            custom_data_requests: plan.custom_data_requests.len(),
        }
    }
}

#[must_use]
pub fn capture_run_path(catalog_root: &Path) -> std::path::PathBuf {
    catalog_root.join(CAPTURE_RUN_FILE)
}

/// Write (overwrite) `metadata/capture_run.json` for this capture process start.
pub fn write_capture_run_record(catalog_root: &Path, record: &CaptureRunRecord) -> Result<()> {
    let path = capture_run_path(catalog_root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create capture_run metadata dir {}",
                parent.display()
            )
        })?;
    }
    let body =
        serde_json::to_vec_pretty(record).context("failed to serialize capture_run metadata")?;
    fs::write(&path, body)
        .with_context(|| format!("failed to write capture_run metadata {}", path.display()))?;
    Ok(())
}

/// Inputs for building a [`CaptureRunRecord`] (avoids a long positional arg list).
#[derive(Debug, Clone)]
pub struct CaptureRunInput<'a> {
    pub node_name: String,
    pub catalog_uri: String,
    pub layout_compatibility: String,
    pub capture_seconds: u64,
    pub venues: Vec<CaptureRunVenueRecord>,
    pub plan: &'a CapturePlan,
    pub option_universe_count: usize,
    pub hip4_universe_count: usize,
    pub nautilus_trader_ref: Option<String>,
    pub cli_venue_features: Vec<String>,
}

#[must_use]
pub fn new_capture_run_record(input: CaptureRunInput<'_>) -> CaptureRunRecord {
    CaptureRunRecord {
        schema_version: CAPTURE_RUN_SCHEMA_VERSION,
        written_at_unix_ms: unix_time_ms(),
        node_name: input.node_name,
        catalog_uri: input.catalog_uri,
        layout_compatibility: input.layout_compatibility,
        capture_seconds: input.capture_seconds,
        venues: input.venues,
        plan: CaptureRunPlanSummary::from_plan(input.plan),
        option_universe_count: input.option_universe_count,
        hip4_universe_count: input.hip4_universe_count,
        nautilus_trader_ref: input.nautilus_trader_ref,
        cli_venue_features: input.cli_venue_features,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::CapturePlan;
    use std::fs;

    #[test]
    fn write_capture_run_roundtrip() {
        let temp = std::env::temp_dir().join(format!(
            "capture-run-meta-{}-{}",
            std::process::id(),
            unix_time_ms()
        ));
        let _ = fs::remove_dir_all(&temp);
        fs::create_dir_all(&temp).unwrap();

        let plan = CapturePlan::default();
        let record = new_capture_run_record(CaptureRunInput {
            node_name: "NODE-1".to_string(),
            catalog_uri: "file:///tmp/catalog".to_string(),
            layout_compatibility: "rust_canonical_only".to_string(),
            capture_seconds: 30,
            venues: vec![CaptureRunVenueRecord {
                id: "deribit_main".to_string(),
                kind: "deribit".to_string(),
            }],
            plan: &plan,
            option_universe_count: 1,
            hip4_universe_count: 0,
            nautilus_trader_ref: Some("abc123".to_string()),
            cli_venue_features: vec!["venue-deribit".to_string()],
        });
        write_capture_run_record(&temp, &record).expect("write");
        let path = capture_run_path(&temp);
        let loaded: CaptureRunRecord =
            serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(loaded.schema_version, CAPTURE_RUN_SCHEMA_VERSION);
        assert_eq!(loaded.node_name, "NODE-1");
        assert_eq!(loaded.venues[0].kind, "deribit");
        assert_eq!(loaded.nautilus_trader_ref.as_deref(), Some("abc123"));
        let _ = fs::remove_dir_all(&temp);
    }
}
