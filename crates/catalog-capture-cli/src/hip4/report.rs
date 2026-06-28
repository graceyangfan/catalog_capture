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

use anyhow::Result;
use catalog_capture_core::{
    hip4_startup_resolution_record, Hip4UniverseResolutionRecord, Hip4UniverseSpec,
    ResolvedHip4Universe,
};
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::InstrumentId;
use serde::Serialize;

use crate::plan_overlap::plan_overlap;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Hip4UniverseResolutionReport {
    pub venue_id: String,
    pub underlying: String,
    pub period: String,
    pub market_class: String,
    pub resolved_at_ns: u64,
    pub question_id: u32,
    pub expiration_ns: u64,
    pub expiration_iso8601: String,
    pub outcome_ids: Vec<u32>,
    pub perp_instrument_id: Option<String>,
    pub outcome_instrument_ids: Vec<String>,
    pub all_instrument_ids: Vec<String>,
    pub overlapping_instrument_ids: Vec<String>,
    pub new_instrument_ids: Vec<String>,
}

pub fn build_hip4_universe_resolution_report(
    spec: &Hip4UniverseSpec,
    resolved: &ResolvedHip4Universe,
    explicit_plan_instrument_ids: &BTreeSet<InstrumentId>,
    universe_plan_instrument_ids: &BTreeSet<InstrumentId>,
) -> Hip4UniverseResolutionReport {
    let overlap = plan_overlap(explicit_plan_instrument_ids, universe_plan_instrument_ids);

    Hip4UniverseResolutionReport {
        venue_id: spec.venue_id.clone(),
        underlying: spec.underlying.clone(),
        period: spec.period.clone(),
        market_class: spec.market_class.clone(),
        resolved_at_ns: resolved.resolved_at_ns.as_u64(),
        question_id: resolved.market.question_id,
        expiration_ns: resolved.market.expiration_ns,
        expiration_iso8601: nautilus_core::datetime::unix_nanos_to_iso8601(UnixNanos::from(
            resolved.market.expiration_ns,
        )),
        outcome_ids: resolved.market.outcome_ids.clone(),
        perp_instrument_id: resolved.perp_instrument_id.map(|id| id.to_string()),
        outcome_instrument_ids: resolved
            .outcome_instrument_ids
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
    report: &Hip4UniverseResolutionReport,
    spec: &Hip4UniverseSpec,
    resolved: &ResolvedHip4Universe,
) -> Hip4UniverseResolutionRecord {
    hip4_startup_resolution_record(
        spec,
        resolved,
        report.new_instrument_ids.clone(),
        Vec::new(),
    )
}

pub fn render_hip4_universe_reports_json(
    reports: &[Hip4UniverseResolutionReport],
) -> Result<String> {
    serde_json::to_string_pretty(reports)
        .map_err(|err| anyhow::anyhow!("failed to render HIP-4 universe resolution report: {err}"))
}

pub fn render_hip4_universe_reports_text(reports: &[Hip4UniverseResolutionReport]) -> String {
    if reports.is_empty() {
        return "No HIP-4 universes configured.".to_string();
    }

    let mut sections = Vec::with_capacity(reports.len());
    for report in reports {
        let outcomes = report.outcome_instrument_ids.join(", ");
        let perp = report.perp_instrument_id.as_deref().unwrap_or("-");
        sections.push(format!(
            "venue={} underlying={} period={} market_class={}\n\
             question_id={} expiration={} expiration_ns={}\n\
             outcome_ids=[{}]\n\
             perp={}\n\
             outcomes=[{}]\n\
             overlap=[{}]\n\
             new=[{}]",
            report.venue_id,
            report.underlying,
            report.period,
            report.market_class,
            report.question_id,
            report.expiration_iso8601,
            report.expiration_ns,
            report
                .outcome_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", "),
            perp,
            outcomes,
            report.overlapping_instrument_ids.join(", "),
            report.new_instrument_ids.join(", "),
        ));
    }

    sections.join("\n\n")
}
