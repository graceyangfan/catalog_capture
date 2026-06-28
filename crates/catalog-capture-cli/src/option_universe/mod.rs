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
mod metadata;
mod readback;
mod report;
#[cfg(test)]
mod tests;
mod validate;
mod validate_suite;

use anyhow::Result;
use catalog_capture_core::{expand_option_universe, CapturePlan};

use crate::config::EffectiveConfig;
use crate::universe_materialize::UniverseMaterialization;

pub use catalog::{
    render_option_universe_catalog_validation_json, render_option_universe_catalog_validation_text,
    validate_option_universe_catalog, OptionUniverseCatalogValidationReport,
};
pub use catalog_presets::{
    merge_validation_options, validation_options_for_preset,
    OptionUniverseCatalogValidationOverrides, OptionUniverseCatalogValidationPreset,
};
pub use discovery::resolve_option_universe_spec;
pub use metadata::{
    render_option_universe_metadata_validation_json,
    render_option_universe_metadata_validation_text, validate_option_universe_metadata,
    validation_options_from_cli, StrikeModeArg,
};
pub use readback::{
    readback_options_for_config, readback_options_from_cli, render_option_universe_readback_json,
    render_option_universe_readback_text, run_option_universe_readback_validation,
};
pub use report::{
    build_option_universe_resolution_report, load_option_universe_summaries,
    render_option_universe_reports_json, render_option_universe_reports_text,
    render_option_universe_summaries_json, render_option_universe_summaries_text,
    startup_resolution_record_from_report, OptionUniverseResolutionReport,
};
pub use validate::validate_option_universes;
pub use validate_suite::{
    run_option_universe_post_run_report, run_option_universe_validation_suite,
    OptionUniverseOutputFormat, OptionUniverseValidationSuiteOptions, PostRunReportOptions,
};

#[derive(Debug, Clone)]
pub struct MaterializedOptionUniversePlan {
    pub plan: CapturePlan,
    pub reports: Vec<OptionUniverseResolutionReport>,
}

pub async fn materialize_capture_plan_with_reports(
    config: &EffectiveConfig,
) -> Result<MaterializedOptionUniversePlan> {
    let mut materialization = UniverseMaterialization::new(config.plan.clone());
    let mut reports = Vec::with_capacity(config.option_universes.len());
    for spec in &config.option_universes {
        let resolved = resolve_option_universe_spec(spec, &config.venues).await?;
        let expanded = expand_option_universe(spec, &resolved);
        let baseline_ids = materialization.planned_instrument_ids.clone();
        let universe_plan_instrument_ids = materialization.append_expanded_plan(&expanded);
        reports.push(build_option_universe_resolution_report(
            spec,
            &resolved,
            &baseline_ids,
            &universe_plan_instrument_ids,
        ));
    }

    Ok(MaterializedOptionUniversePlan {
        plan: materialization.plan,
        reports,
    })
}

pub async fn resolve_option_universe_reports(
    config: &EffectiveConfig,
) -> Result<Vec<OptionUniverseResolutionReport>> {
    Ok(materialize_capture_plan_with_reports(config).await?.reports)
}
