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

mod catalog;
mod catalog_presets;
mod discovery;
mod history;
mod metadata;
mod post_run_report;
mod readback;
mod report;
#[cfg(test)]
mod tests;
mod validate;
mod validate_suite;

use std::collections::BTreeSet;

use anyhow::Result;
use catalog_capture_core::{expand_option_universe, merge_capture_plans, CapturePlan};

use crate::config::EffectiveConfig;

pub use catalog::{
    render_option_universe_catalog_validation_json, render_option_universe_catalog_validation_text,
    validate_option_universe_catalog, OptionUniverseCatalogValidationReport,
};
pub use catalog_presets::{
    merge_validation_options, validation_options_for_preset,
    OptionUniverseCatalogValidationOverrides, OptionUniverseCatalogValidationPreset,
};
pub use discovery::resolve_option_universe_spec;
pub use history::{
    load_option_universe_summaries, render_option_universe_summaries_json,
    render_option_universe_summaries_text,
};
pub use metadata::{
    render_option_universe_metadata_validation_json,
    render_option_universe_metadata_validation_text, validate_option_universe_metadata,
    validation_options_from_cli, StrikeModeArg,
};
pub use post_run_report::{
    run_option_universe_post_run_report, OptionUniverseOutputFormat, PostRunReportOptions,
};
pub use readback::{
    readback_options_for_config, readback_options_from_cli, render_option_universe_readback_json,
    render_option_universe_readback_text, run_option_universe_readback_validation,
};
pub use report::{
    build_option_universe_resolution_report, render_option_universe_reports_json,
    render_option_universe_reports_text, startup_resolution_record_from_report,
    OptionUniverseResolutionReport,
};
pub use validate::validate_option_universes;
pub use validate_suite::{
    run_option_universe_validation_suite, OptionUniverseValidationSuiteOptions,
};

#[derive(Debug, Clone)]
pub struct MaterializedOptionUniversePlan {
    pub plan: CapturePlan,
    pub reports: Vec<OptionUniverseResolutionReport>,
}

pub async fn materialize_capture_plan_with_reports(
    config: &EffectiveConfig,
) -> Result<MaterializedOptionUniversePlan> {
    let mut plan = config.plan.clone();
    let mut planned_instrument_ids = plan
        .planned_instrument_ids()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let mut reports = Vec::with_capacity(config.option_universes.len());
    for spec in &config.option_universes {
        let resolved = resolve_option_universe_spec(spec, &config.venues).await?;
        let expanded = expand_option_universe(spec, &resolved);
        let universe_plan_instrument_ids = expanded
            .planned_instrument_ids()
            .into_iter()
            .collect::<BTreeSet<_>>();
        reports.push(build_option_universe_resolution_report(
            spec,
            &resolved,
            &planned_instrument_ids,
            &universe_plan_instrument_ids,
        ));
        planned_instrument_ids.extend(universe_plan_instrument_ids.iter().copied());
        plan = merge_capture_plans(&plan, &expanded);
    }

    Ok(MaterializedOptionUniversePlan { plan, reports })
}

pub async fn resolve_option_universe_reports(
    config: &EffectiveConfig,
) -> Result<Vec<OptionUniverseResolutionReport>> {
    Ok(materialize_capture_plan_with_reports(config).await?.reports)
}
