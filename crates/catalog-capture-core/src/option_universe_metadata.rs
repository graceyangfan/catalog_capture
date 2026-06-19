use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::ResolvedOptionUniverse;

pub const OPTION_UNIVERSE_RESOLUTIONS_FILE: &str =
    "metadata/option_universe_resolutions.jsonl";

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
    pub selected_strikes: Vec<String>,
    pub perp_instrument_id: Option<String>,
    pub option_instrument_ids: Vec<String>,
    pub all_instrument_ids: Vec<String>,
    pub added_instrument_ids: Vec<String>,
    pub removed_instrument_ids: Vec<String>,
    pub rollover_reason: Option<String>,
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
        writeln!(file, "{line}")
            .with_context(|| format!("failed to append {}", path.display()))?;
    }

    Ok(())
}

pub fn startup_resolution_record(
    venue_id: &str,
    underlying: &str,
    resolved: &ResolvedOptionUniverse,
    added_instrument_ids: Vec<String>,
    removed_instrument_ids: Vec<String>,
) -> OptionUniverseResolutionRecord {
    resolution_record(
        OptionUniverseResolutionEventKind::Startup,
        venue_id,
        underlying,
        resolved,
        added_instrument_ids,
        removed_instrument_ids,
        None,
    )
}

pub fn refresh_resolution_record(
    venue_id: &str,
    underlying: &str,
    resolved: &ResolvedOptionUniverse,
    added_instrument_ids: Vec<String>,
    removed_instrument_ids: Vec<String>,
    rollover_reason: Option<String>,
) -> OptionUniverseResolutionRecord {
    resolution_record(
        OptionUniverseResolutionEventKind::Refresh,
        venue_id,
        underlying,
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

    Some("strike_window_shift".to_string())
}

fn resolution_record(
    event_kind: OptionUniverseResolutionEventKind,
    venue_id: &str,
    underlying: &str,
    resolved: &ResolvedOptionUniverse,
    added_instrument_ids: Vec<String>,
    removed_instrument_ids: Vec<String>,
    rollover_reason: Option<String>,
) -> OptionUniverseResolutionRecord {
    OptionUniverseResolutionRecord {
        event_kind,
        venue_id: venue_id.to_string(),
        underlying: underlying.to_string(),
        resolved_at_ns: resolved.resolved_at_ns.as_u64(),
        resolved_at_iso8601: nautilus_core::datetime::unix_nanos_to_iso8601(resolved.resolved_at_ns),
        selected_expiry_ns: resolved.selected_expiry_ns.as_u64(),
        selected_expiry_iso8601: nautilus_core::datetime::unix_nanos_to_iso8601(
            resolved.selected_expiry_ns,
        ),
        atm_reference: resolved.atm_reference.to_string(),
        atm_reference_source: resolved
            .atm_reference_source
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
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
                true
            ),
            Some("expiry_roll".to_string())
        );
        assert_eq!(
            compute_refresh_rollover_reason(
                Some(resolved.selected_expiry_ns.as_u64()),
                &resolved,
                Some("64900"),
                true
            ),
            Some("atm_drift".to_string())
        );
        assert_eq!(
            compute_refresh_rollover_reason(
                Some(resolved.selected_expiry_ns.as_u64()),
                &resolved,
                Some("65000"),
                true
            ),
            Some("strike_window_shift".to_string())
        );
    }

    #[test]
    fn append_resolution_records_writes_jsonl() {
        let temp = std::env::temp_dir().join(format!(
            "option-universe-metadata-{}",
            std::process::id()
        ));
        fs::create_dir_all(&temp).unwrap();

        let record = startup_resolution_record(
            "deribit_main",
            "BTC",
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
}