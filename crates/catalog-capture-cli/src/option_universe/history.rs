use anyhow::Result;
use catalog_capture_core::{
    read_option_universe_resolution_records, summarize_option_universe_resolution_records,
    OptionUniverseResolutionSummary,
};

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
