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

use std::collections::BTreeSet;

use crate::plan_overlap::plan_overlap;
use anyhow::Result;
use catalog_capture_core::{
    read_option_universe_resolution_records, summarize_option_universe_resolution_records,
    OptionUniverseResolutionEventKind, OptionUniverseResolutionRecord,
    OptionUniverseResolutionSummary, OptionUniverseSpec, ResolvedOptionUniverse,
};
use nautilus_model::identifiers::InstrumentId;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OptionUniverseResolutionReport {
    pub venue_id: String,
    pub underlying: String,
    pub resolved_at_ns: u64,
    pub selected_expiry_ns: u64,
    pub selected_expiry_iso8601: String,
    pub atm_reference: String,
    pub atm_reference_source: String,
    pub strike_selection_mode: String,
    pub oi_ranked_top_n: Option<usize>,
    pub selected_strikes: Vec<String>,
    pub perp_instrument_id: Option<String>,
    pub option_instrument_ids: Vec<String>,
    pub all_instrument_ids: Vec<String>,
    pub overlapping_instrument_ids: Vec<String>,
    pub new_instrument_ids: Vec<String>,
}

pub fn build_option_universe_resolution_report(
    spec: &OptionUniverseSpec,
    resolved: &ResolvedOptionUniverse,
    explicit_plan_instrument_ids: &BTreeSet<InstrumentId>,
    universe_plan_instrument_ids: &BTreeSet<InstrumentId>,
) -> OptionUniverseResolutionReport {
    let overlap = plan_overlap(explicit_plan_instrument_ids, universe_plan_instrument_ids);

    OptionUniverseResolutionReport {
        venue_id: spec.venue_id.clone(),
        underlying: spec.underlying.clone(),
        resolved_at_ns: resolved.resolved_at_ns.as_u64(),
        selected_expiry_ns: resolved.selected_expiry_ns.as_u64(),
        selected_expiry_iso8601: nautilus_core::datetime::unix_nanos_to_iso8601(
            resolved.selected_expiry_ns,
        ),
        atm_reference: resolved.atm_reference.to_string(),
        atm_reference_source: resolved
            .atm_reference_source
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        strike_selection_mode: spec.strike_policy.selection_mode().to_string(),
        oi_ranked_top_n: spec.strike_policy.oi_ranked_top_n(),
        selected_strikes: resolved
            .selected_strikes
            .iter()
            .map(ToString::to_string)
            .collect(),
        perp_instrument_id: resolved.perp_instrument_id.map(|id| id.to_string()),
        option_instrument_ids: resolved
            .option_instrument_ids
            .iter()
            .map(ToString::to_string)
            .collect(),
        all_instrument_ids: resolved
            .all_instrument_ids
            .iter()
            .map(ToString::to_string)
            .collect(),
        overlapping_instrument_ids: overlap.overlapping_instrument_ids,
        new_instrument_ids: overlap.new_instrument_ids,
    }
}

pub fn startup_resolution_record_from_report(
    report: &OptionUniverseResolutionReport,
) -> OptionUniverseResolutionRecord {
    OptionUniverseResolutionRecord {
        event_kind: OptionUniverseResolutionEventKind::Startup,
        venue_id: report.venue_id.clone(),
        underlying: report.underlying.clone(),
        resolved_at_ns: report.resolved_at_ns,
        resolved_at_iso8601: nautilus_core::datetime::unix_nanos_to_iso8601(
            nautilus_core::UnixNanos::from(report.resolved_at_ns),
        ),
        selected_expiry_ns: report.selected_expiry_ns,
        selected_expiry_iso8601: report.selected_expiry_iso8601.clone(),
        atm_reference: report.atm_reference.clone(),
        atm_reference_source: report.atm_reference_source.clone(),
        strike_selection_mode: report.strike_selection_mode.clone(),
        oi_ranked_top_n: report.oi_ranked_top_n,
        selected_strikes: report.selected_strikes.clone(),
        perp_instrument_id: report.perp_instrument_id.clone(),
        option_instrument_ids: report.option_instrument_ids.clone(),
        all_instrument_ids: report.all_instrument_ids.clone(),
        added_instrument_ids: report.new_instrument_ids.clone(),
        removed_instrument_ids: Vec::new(),
        rollover_reason: None,
    }
}

pub fn render_option_universe_reports_json(
    reports: &[OptionUniverseResolutionReport],
) -> Result<String> {
    serde_json::to_string_pretty(reports)
        .map_err(|err| anyhow::anyhow!("failed to render option universe resolution report: {err}"))
}

pub fn render_option_universe_reports_text(reports: &[OptionUniverseResolutionReport]) -> String {
    if reports.is_empty() {
        return "No option universes configured.".to_string();
    }

    let mut sections = Vec::with_capacity(reports.len());
    for report in reports {
        let strikes = report.selected_strikes.join(", ");
        let options = report.option_instrument_ids.join(", ");
        let perp = report.perp_instrument_id.as_deref().unwrap_or("-");

        sections.push(format!(
            "venue={} underlying={} expiry={} expiry_ns={}\n\
             atm_reference={}\n\
             strikes=[{}]\n\
             perp={}\n\
             options=[{}]\n\
             overlap=[{}]\n\
             new=[{}]",
            report.venue_id,
            report.underlying,
            report.selected_expiry_iso8601,
            report.selected_expiry_ns,
            report.atm_reference,
            strikes,
            perp,
            options,
            report.overlapping_instrument_ids.join(", "),
            report.new_instrument_ids.join(", "),
        ));
    }

    sections.join("\n\n")
}

pub fn load_option_universe_summaries(
    catalog_root: &std::path::Path,
) -> Result<Vec<OptionUniverseResolutionSummary>> {
    let records = read_option_universe_resolution_records(catalog_root)?;
    Ok(summarize_option_universe_resolution_records(&records))
}

pub fn render_option_universe_summaries_json(
    summaries: &[OptionUniverseResolutionSummary],
) -> Result<String> {
    serde_json::to_string_pretty(summaries)
        .map_err(|err| anyhow::anyhow!("failed to render option universe summaries: {err}"))
}

pub fn render_option_universe_summaries_text(
    summaries: &[OptionUniverseResolutionSummary],
) -> String {
    if summaries.is_empty() {
        return "No option universe resolution metadata found.".to_string();
    }

    summaries
        .iter()
        .map(|summary| {
            format!(
                "venue={} underlying={}\n\
                 startup_at={}\n\
                 latest_event={:?}\n\
                 latest_resolved_at={}\n\
                 expiry={}\n\
                 strike_selection_mode={}\n\
                 refresh_count={}\n\
                 latest_rollover_reason={}\n\
                 perp={}\n\
                 option_count={}\n\
                 options=[{}]",
                summary.venue_id,
                summary.underlying,
                summary.startup_resolved_at_iso8601,
                summary.latest_event_kind,
                summary.latest_resolved_at_iso8601,
                summary.latest_selected_expiry_iso8601,
                summary.strike_selection_mode,
                summary.refresh_count,
                summary.latest_rollover_reason.as_deref().unwrap_or("-"),
                summary.perp_instrument_id.as_deref().unwrap_or("-"),
                summary.option_count,
                summary.option_instrument_ids.join(", "),
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}
