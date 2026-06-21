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

use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use nautilus_core::UnixNanos;
use serde::{Deserialize, Serialize};

use crate::{
    hip4::spec::{Hip4UniverseSpec, ResolvedHip4Universe},
    jsonl::append_jsonl_records,
};

pub const HIP4_UNIVERSE_RESOLUTIONS_FILE: &str = "metadata/hip4_universe_resolutions.jsonl";

pub const REFRESH_ROLLOVER_REASONS: [&str; 2] = ["question_roll", "expiry_roll"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Hip4UniverseResolutionEventKind {
    Startup,
    Refresh,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hip4UniverseResolutionRecord {
    pub event_kind: Hip4UniverseResolutionEventKind,
    pub venue_id: String,
    pub underlying: String,
    pub period: String,
    pub market_class: String,
    pub resolved_at_ns: u64,
    pub resolved_at_iso8601: String,
    pub question_id: u32,
    pub expiration_ns: u64,
    pub expiration_iso8601: String,
    pub outcome_ids: Vec<u32>,
    pub perp_instrument_id: Option<String>,
    pub outcome_instrument_ids: Vec<String>,
    pub all_instrument_ids: Vec<String>,
    pub added_instrument_ids: Vec<String>,
    pub removed_instrument_ids: Vec<String>,
    pub rollover_reason: Option<String>,
}

pub fn hip4_universe_resolution_log_path(catalog_root: &Path) -> PathBuf {
    catalog_root.join(HIP4_UNIVERSE_RESOLUTIONS_FILE)
}

pub fn append_hip4_universe_resolution_records(
    catalog_root: &Path,
    records: &[Hip4UniverseResolutionRecord],
) -> Result<()> {
    append_jsonl_records(
        &hip4_universe_resolution_log_path(catalog_root),
        records,
        "HIP-4 universe resolution",
    )
}

#[must_use]
pub fn startup_resolution_record(
    spec: &Hip4UniverseSpec,
    resolved: &ResolvedHip4Universe,
    added_instrument_ids: Vec<String>,
    removed_instrument_ids: Vec<String>,
) -> Hip4UniverseResolutionRecord {
    resolution_record(
        Hip4UniverseResolutionEventKind::Startup,
        spec,
        resolved,
        added_instrument_ids,
        removed_instrument_ids,
        None,
    )
}

pub fn validate_hip4_refresh_rollover_reason(reason: &str) -> Result<()> {
    if !REFRESH_ROLLOVER_REASONS.contains(&reason) {
        bail!(
            "HIP-4 refresh rollover_reason unexpected: got {reason:?}, expected one of {:?}",
            REFRESH_ROLLOVER_REASONS
        );
    }
    Ok(())
}

pub fn validate_hip4_refresh_resolution_record(
    record: &Hip4UniverseResolutionRecord,
) -> Result<()> {
    if record.event_kind != Hip4UniverseResolutionEventKind::Refresh {
        bail!("HIP-4 refresh validation expected refresh event");
    }

    if let Some(reason) = record.rollover_reason.as_deref() {
        validate_hip4_refresh_rollover_reason(reason)?;
    }

    if record.added_instrument_ids.is_empty() && record.removed_instrument_ids.is_empty() {
        bail!(
            "HIP-4 refresh metadata for {}:{} should include added or removed instruments",
            record.venue_id,
            record.underlying
        );
    }

    Ok(())
}

pub fn refresh_resolution_record(
    spec: &Hip4UniverseSpec,
    resolved: &ResolvedHip4Universe,
    added_instrument_ids: Vec<String>,
    removed_instrument_ids: Vec<String>,
    rollover_reason: Option<String>,
) -> Hip4UniverseResolutionRecord {
    resolution_record(
        Hip4UniverseResolutionEventKind::Refresh,
        spec,
        resolved,
        added_instrument_ids,
        removed_instrument_ids,
        rollover_reason,
    )
}

#[must_use]
pub fn compute_hip4_refresh_rollover_reason(
    previous_question_id: Option<u32>,
    previous_expiration_ns: Option<u64>,
    resolved: &ResolvedHip4Universe,
    instruments_changed: bool,
) -> Option<String> {
    if !instruments_changed {
        return None;
    }

    if let Some(previous_question_id) = previous_question_id {
        if previous_question_id != resolved.market.question_id {
            return Some("question_roll".to_string());
        }
    }

    if let Some(previous_expiration_ns) = previous_expiration_ns {
        if previous_expiration_ns != resolved.market.expiration_ns {
            return Some("expiry_roll".to_string());
        }
    }

    None
}

fn resolution_record(
    event_kind: Hip4UniverseResolutionEventKind,
    spec: &Hip4UniverseSpec,
    resolved: &ResolvedHip4Universe,
    added_instrument_ids: Vec<String>,
    removed_instrument_ids: Vec<String>,
    rollover_reason: Option<String>,
) -> Hip4UniverseResolutionRecord {
    Hip4UniverseResolutionRecord {
        event_kind,
        venue_id: spec.venue_id.clone(),
        underlying: spec.underlying.clone(),
        period: spec.period.clone(),
        market_class: spec.market_class.clone(),
        resolved_at_ns: resolved.resolved_at_ns.as_u64(),
        resolved_at_iso8601: nautilus_core::datetime::unix_nanos_to_iso8601(
            resolved.resolved_at_ns,
        ),
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
        added_instrument_ids,
        removed_instrument_ids,
        rollover_reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_hip4_refresh_rollover_reason_accepts_known_values() {
        for reason in REFRESH_ROLLOVER_REASONS {
            validate_hip4_refresh_rollover_reason(reason).expect(reason);
        }
    }

    #[test]
    fn validate_hip4_refresh_rollover_reason_rejects_unknown_values() {
        let err = validate_hip4_refresh_rollover_reason("atm_drift").expect_err("unknown reason");
        assert!(err.to_string().contains("rollover_reason unexpected"));
    }
}
