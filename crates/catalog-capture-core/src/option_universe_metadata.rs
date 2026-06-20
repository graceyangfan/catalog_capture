use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::{OptionUniverseSpec, ResolvedOptionUniverse};

pub const OPTION_UNIVERSE_RESOLUTIONS_FILE: &str = "metadata/option_universe_resolutions.jsonl";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionUniverseResolutionEventKind {
    Startup,
    Refresh,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptionUniverseResolutionRecord {
    pub event_kind: OptionUniverseResolutionEventKind,
    pub venue_id: String,
    pub underlying: String,
    pub resolved_at_ns: u64,
    pub resolved_at_iso8601: String,
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
    pub added_instrument_ids: Vec<String>,
    pub removed_instrument_ids: Vec<String>,
    pub rollover_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptionUniverseResolutionSummary {
    pub venue_id: String,
    pub underlying: String,
    pub startup_resolved_at_iso8601: String,
    pub latest_event_kind: OptionUniverseResolutionEventKind,
    pub latest_resolved_at_iso8601: String,
    pub latest_selected_expiry_iso8601: String,
    pub strike_selection_mode: String,
    pub refresh_count: usize,
    pub latest_rollover_reason: Option<String>,
    pub perp_instrument_id: Option<String>,
    pub option_count: usize,
    pub option_instrument_ids: Vec<String>,
}

pub fn catalog_root_from_uri(catalog_uri: &str) -> Result<PathBuf> {
    let path = catalog_uri.strip_prefix("file://").unwrap_or(catalog_uri);
    if path.is_empty() {
        anyhow::bail!("catalog_uri cannot be empty");
    }
    Ok(PathBuf::from(path))
}

pub fn option_universe_resolution_log_path(catalog_root: &Path) -> PathBuf {
    catalog_root.join(OPTION_UNIVERSE_RESOLUTIONS_FILE)
}

pub fn read_option_universe_resolution_records(
    catalog_root: &Path,
) -> Result<Vec<OptionUniverseResolutionRecord>> {
    let path = option_universe_resolution_log_path(catalog_root);
    let content = fs::read_to_string(&path)
        .with_context(|| format!("failed to read option universe metadata {}", path.display()))?;

    let mut records = Vec::new();
    for (idx, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let record =
            serde_json::from_str::<OptionUniverseResolutionRecord>(line).with_context(|| {
                format!(
                    "failed to parse option universe metadata line {} in {}",
                    idx + 1,
                    path.display()
                )
            })?;
        records.push(record);
    }

    Ok(records)
}

#[must_use]
pub fn summarize_option_universe_resolution_records(
    records: &[OptionUniverseResolutionRecord],
) -> Vec<OptionUniverseResolutionSummary> {
    use std::collections::BTreeMap;

    #[derive(Debug)]
    struct State<'a> {
        startup: &'a OptionUniverseResolutionRecord,
        latest: &'a OptionUniverseResolutionRecord,
        refresh_count: usize,
    }

    let mut states = BTreeMap::<(String, String), State<'_>>::new();
    for record in records {
        let key = (record.venue_id.clone(), record.underlying.clone());
        match states.get_mut(&key) {
            Some(state) => {
                state.latest = record;
                if record.event_kind == OptionUniverseResolutionEventKind::Refresh {
                    state.refresh_count += 1;
                }
            }
            None => {
                states.insert(
                    key,
                    State {
                        startup: record,
                        latest: record,
                        refresh_count: usize::from(
                            record.event_kind == OptionUniverseResolutionEventKind::Refresh,
                        ),
                    },
                );
            }
        }
    }

    states
        .into_values()
        .map(|state| OptionUniverseResolutionSummary {
            venue_id: state.latest.venue_id.clone(),
            underlying: state.latest.underlying.clone(),
            startup_resolved_at_iso8601: state.startup.resolved_at_iso8601.clone(),
            latest_event_kind: state.latest.event_kind,
            latest_resolved_at_iso8601: state.latest.resolved_at_iso8601.clone(),
            latest_selected_expiry_iso8601: state.latest.selected_expiry_iso8601.clone(),
            strike_selection_mode: state.latest.strike_selection_mode.clone(),
            refresh_count: state.refresh_count,
            latest_rollover_reason: state.latest.rollover_reason.clone(),
            perp_instrument_id: state.latest.perp_instrument_id.clone(),
            option_count: state.latest.option_instrument_ids.len(),
            option_instrument_ids: state.latest.option_instrument_ids.clone(),
        })
        .collect()
}

pub fn append_option_universe_resolution_records(
    catalog_root: &Path,
    records: &[OptionUniverseResolutionRecord],
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }

    let path = option_universe_resolution_log_path(catalog_root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create metadata dir {}", parent.display()))?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("failed to open {}", path.display()))?;

    for record in records {
        let line = serde_json::to_string(record)
            .with_context(|| "failed to serialize option universe resolution record")?;
        writeln!(file, "{line}").with_context(|| format!("failed to append {}", path.display()))?;
    }

    Ok(())
}

pub fn startup_resolution_record(
    spec: &OptionUniverseSpec,
    resolved: &ResolvedOptionUniverse,
    added_instrument_ids: Vec<String>,
    removed_instrument_ids: Vec<String>,
) -> OptionUniverseResolutionRecord {
    resolution_record(
        OptionUniverseResolutionEventKind::Startup,
        spec,
        resolved,
        added_instrument_ids,
        removed_instrument_ids,
        None,
    )
}

pub fn refresh_resolution_record(
    spec: &OptionUniverseSpec,
    resolved: &ResolvedOptionUniverse,
    added_instrument_ids: Vec<String>,
    removed_instrument_ids: Vec<String>,
    rollover_reason: Option<String>,
) -> OptionUniverseResolutionRecord {
    resolution_record(
        OptionUniverseResolutionEventKind::Refresh,
        spec,
        resolved,
        added_instrument_ids,
        removed_instrument_ids,
        rollover_reason,
    )
}

#[must_use]
pub fn compute_refresh_rollover_reason(
    previous_expiry_ns: Option<u64>,
    resolved: &ResolvedOptionUniverse,
    previous_atm_reference: Option<&str>,
    instruments_changed: bool,
    strike_selection_mode: &str,
) -> Option<String> {
    if !instruments_changed {
        return None;
    }

    if let Some(previous_expiry_ns) = previous_expiry_ns {
        if previous_expiry_ns != resolved.selected_expiry_ns.as_u64() {
            return Some("expiry_roll".to_string());
        }
    }

    let current_atm = resolved.atm_reference.to_string();
    if let Some(previous_atm_reference) = previous_atm_reference {
        if previous_atm_reference != current_atm.as_str() {
            return Some("atm_drift".to_string());
        }
    }

    if strike_selection_mode == "oi_ranked" {
        return Some("oi_rank_shift".to_string());
    }

    Some("strike_window_shift".to_string())
}

fn resolution_record(
    event_kind: OptionUniverseResolutionEventKind,
    spec: &OptionUniverseSpec,
    resolved: &ResolvedOptionUniverse,
    added_instrument_ids: Vec<String>,
    removed_instrument_ids: Vec<String>,
    rollover_reason: Option<String>,
) -> OptionUniverseResolutionRecord {
    OptionUniverseResolutionRecord {
        event_kind,
        venue_id: spec.venue_id.clone(),
        underlying: spec.underlying.clone(),
        resolved_at_ns: resolved.resolved_at_ns.as_u64(),
        resolved_at_iso8601: nautilus_core::datetime::unix_nanos_to_iso8601(
            resolved.resolved_at_ns,
        ),
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
        added_instrument_ids,
        removed_instrument_ids,
        rollover_reason,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use nautilus_core::UnixNanos;
    use nautilus_model::{identifiers::InstrumentId, types::Price};

    use super::*;

    fn sample_resolved() -> ResolvedOptionUniverse {
        ResolvedOptionUniverse {
            resolved_at_ns: UnixNanos::from(1_781_740_800_000_000_000u64),
            selected_expiry_ns: UnixNanos::from(1_782_432_000_000_000_000u64),
            atm_reference: Price::from("65000"),
            atm_reference_source: Some("http_perp_ticker_mark".to_string()),
            selected_strikes: vec![Price::from("64000"), Price::from("65000")],
            perp_instrument_id: Some(InstrumentId::from("BTC-PERPETUAL.DERIBIT")),
            option_instrument_ids: vec![
                InstrumentId::from("BTC-26JUN26-65000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-65000-P.DERIBIT"),
            ],
            all_instrument_ids: vec![
                InstrumentId::from("BTC-26JUN26-65000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-65000-P.DERIBIT"),
                InstrumentId::from("BTC-PERPETUAL.DERIBIT"),
            ],
        }
    }

    #[test]
    fn compute_refresh_rollover_reason_prefers_expiry_roll() {
        let resolved = sample_resolved();
        assert_eq!(
            compute_refresh_rollover_reason(
                Some(resolved.selected_expiry_ns.as_u64().saturating_sub(1)),
                &resolved,
                Some("65000"),
                true,
                "atm_relative",
            ),
            Some("expiry_roll".to_string())
        );
        assert_eq!(
            compute_refresh_rollover_reason(
                Some(resolved.selected_expiry_ns.as_u64()),
                &resolved,
                Some("64900"),
                true,
                "atm_relative",
            ),
            Some("atm_drift".to_string())
        );
        assert_eq!(
            compute_refresh_rollover_reason(
                Some(resolved.selected_expiry_ns.as_u64()),
                &resolved,
                Some("65000"),
                true,
                "atm_relative",
            ),
            Some("strike_window_shift".to_string())
        );
        assert_eq!(
            compute_refresh_rollover_reason(
                Some(resolved.selected_expiry_ns.as_u64()),
                &resolved,
                Some("65000"),
                true,
                "oi_ranked",
            ),
            Some("oi_rank_shift".to_string())
        );
    }

    #[test]
    fn append_resolution_records_writes_jsonl() {
        let temp =
            std::env::temp_dir().join(format!("option-universe-metadata-{}", std::process::id()));
        fs::create_dir_all(&temp).unwrap();

        let record = startup_resolution_record(
            &OptionUniverseSpec {
                venue_id: "deribit_main".to_string(),
                underlying: "BTC".to_string(),
                settlement_currency: Some("BTC".to_string()),
                include_perp: true,
                families: vec![],
                expiry_policy: crate::ExpiryPolicy::Nearest { days_max: 45 },
                strike_policy: crate::StrikePolicy::AtmRelative {
                    strikes_above: 1,
                    strikes_below: 1,
                },
            },
            &sample_resolved(),
            vec!["BTC-26JUN26-65000-C.DERIBIT".to_string()],
            vec![],
        );
        append_option_universe_resolution_records(&temp, &[record]).unwrap();

        let contents =
            fs::read_to_string(option_universe_resolution_log_path(&temp)).expect("metadata file");
        assert!(contents.contains("\"event_kind\":\"startup\""));
        assert!(contents.contains("\"atm_reference_source\":\"http_perp_ticker_mark\""));

        fs::remove_dir_all(temp).ok();
    }

    #[test]
    fn summarize_option_universe_resolution_records_aggregates_latest_state() {
        let spec = OptionUniverseSpec {
            venue_id: "deribit_main".to_string(),
            underlying: "BTC".to_string(),
            settlement_currency: Some("BTC".to_string()),
            include_perp: true,
            families: vec![],
            expiry_policy: crate::ExpiryPolicy::Nearest { days_max: 45 },
            strike_policy: crate::StrikePolicy::AtmRelative {
                strikes_above: 1,
                strikes_below: 1,
            },
        };

        let mut startup = startup_resolution_record(
            &spec,
            &sample_resolved(),
            vec!["BTC-26JUN26-65000-C.DERIBIT".to_string()],
            vec![],
        );
        startup.resolved_at_iso8601 = "2026-06-20T00:00:00Z".to_string();

        let mut refreshed = refresh_resolution_record(
            &spec,
            &sample_resolved(),
            vec!["BTC-26JUN26-66000-C.DERIBIT".to_string()],
            vec!["BTC-26JUN26-64000-C.DERIBIT".to_string()],
            Some("atm_drift".to_string()),
        );
        refreshed.resolved_at_iso8601 = "2026-06-20T00:15:00Z".to_string();
        refreshed.selected_expiry_iso8601 = "2026-06-26T08:00:00Z".to_string();

        let summaries = summarize_option_universe_resolution_records(&[startup, refreshed]);
        assert_eq!(summaries.len(), 1);
        let summary = &summaries[0];
        assert_eq!(summary.venue_id, "deribit_main");
        assert_eq!(summary.underlying, "BTC");
        assert_eq!(summary.refresh_count, 1);
        assert_eq!(summary.latest_rollover_reason.as_deref(), Some("atm_drift"));
        assert_eq!(summary.latest_resolved_at_iso8601, "2026-06-20T00:15:00Z");
    }
}
