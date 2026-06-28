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

mod discovery;
mod report;
mod validate;

use anyhow::Result;
use catalog_capture_core::{expand_hip4_universe, CapturePlan};

use crate::config::EffectiveConfig;
use crate::universe_materialize::UniverseMaterialization;

pub use discovery::resolve_hip4_universe_spec;
pub use report::{
    build_hip4_universe_resolution_report, render_hip4_universe_reports_json,
    render_hip4_universe_reports_text, startup_resolution_record_from_report,
    Hip4UniverseResolutionReport,
};
pub use validate::validate_hip4_universes;

#[derive(Debug, Clone)]
pub struct MaterializedHip4UniversePlan {
    pub plan: CapturePlan,
    pub reports: Vec<Hip4UniverseResolutionReport>,
    pub resolved: Vec<catalog_capture_core::ResolvedHip4Universe>,
}

pub async fn materialize_hip4_capture_plan(
    config: &EffectiveConfig,
) -> Result<MaterializedHip4UniversePlan> {
    if config.hip4_universes.is_empty() {
        return Ok(MaterializedHip4UniversePlan {
            plan: CapturePlan::default(),
            reports: Vec::new(),
            resolved: Vec::new(),
        });
    }

    let http_timeout_secs = config
        .runtime
        .hip4_universe_refresh
        .http_timeout_secs
        .max(1);
    let mut materialization = UniverseMaterialization::new(CapturePlan::default());
    materialization
        .planned_instrument_ids
        .extend(config.plan.planned_instrument_ids());
    let mut reports = Vec::with_capacity(config.hip4_universes.len());
    let mut resolved_entries = Vec::with_capacity(config.hip4_universes.len());

    for spec in &config.hip4_universes {
        let resolved = resolve_hip4_universe_spec(spec, &config.venues, http_timeout_secs).await?;
        let expanded = expand_hip4_universe(spec, &resolved);
        let baseline_ids = materialization.planned_instrument_ids.clone();
        let universe_plan_instrument_ids = materialization.append_expanded_plan(&expanded);
        reports.push(build_hip4_universe_resolution_report(
            spec,
            &resolved,
            &baseline_ids,
            &universe_plan_instrument_ids,
        ));
        resolved_entries.push(resolved);
    }

    Ok(MaterializedHip4UniversePlan {
        plan: materialization.plan,
        reports,
        resolved: resolved_entries,
    })
}
