use std::collections::BTreeSet;

use anyhow::Result;
use catalog_capture_core::{
    OptionUniverseResolutionEventKind, OptionUniverseResolutionRecord, OptionUniverseSpec,
    ResolvedOptionUniverse,
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
    pub volume_ranked_top_n: Option<usize>,
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
    let overlapping_instrument_ids = universe_plan_instrument_ids
        .iter()
        .filter(|instrument_id| explicit_plan_instrument_ids.contains(instrument_id))
        .map(ToString::to_string)
        .collect();
    let new_instrument_ids = universe_plan_instrument_ids
        .iter()
        .filter(|instrument_id| !explicit_plan_instrument_ids.contains(instrument_id))
        .map(ToString::to_string)
        .collect();

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
        volume_ranked_top_n: spec.strike_policy.volume_ranked_top_n(),
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
        overlapping_instrument_ids,
        new_instrument_ids,
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
        volume_ranked_top_n: report.volume_ranked_top_n,
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