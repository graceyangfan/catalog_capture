use std::path::Path;

use anyhow::Result;
use catalog_capture_core::{
    validate_option_universe_resolution_metadata, ALL_STRIKES_MIN_SELECTED_STRIKES,
    OptionUniverseResolutionValidationOptions, OptionUniverseResolutionValidationReport,
    StrikePolicy, StrikeSelectionProfile,
};

use crate::config::EffectiveConfig;

pub fn metadata_validation_options_for_config(
    config: &EffectiveConfig,
) -> OptionUniverseResolutionValidationOptions {
    OptionUniverseResolutionValidationOptions {
        require_refresh_change: config.runtime.option_universe_refresh.enabled,
        strike_profile: config
            .option_universes
            .iter()
            .find_map(strike_profile_from_spec),
    }
}

pub fn validation_options_from_cli(
    require_refresh_change: bool,
    strike_mode: Option<StrikeModeArg>,
    oi_ranked_top_n: Option<usize>,
    all_min_strikes: Option<usize>,
) -> Result<OptionUniverseResolutionValidationOptions> {
    Ok(OptionUniverseResolutionValidationOptions {
        require_refresh_change,
        strike_profile: strike_mode
            .map(|mode| strike_profile_from_arg(mode, oi_ranked_top_n, all_min_strikes))
            .transpose()?,
    })
}

pub fn validate_option_universe_metadata(
    catalog_root: &Path,
    options: &OptionUniverseResolutionValidationOptions,
) -> Result<Vec<OptionUniverseResolutionValidationReport>> {
    validate_option_universe_resolution_metadata(catalog_root, options)
}

pub fn render_option_universe_metadata_validation_json(
    reports: &[OptionUniverseResolutionValidationReport],
) -> Result<String> {
    serde_json::to_string_pretty(reports).map_err(|err| {
        anyhow::anyhow!("failed to render option universe metadata validation: {err}")
    })
}

pub fn render_option_universe_metadata_validation_text(
    reports: &[OptionUniverseResolutionValidationReport],
) -> String {
    if reports.is_empty() {
        return "No option universe metadata validation results.".to_string();
    }

    reports
        .iter()
        .map(|report| {
            format!(
                "venue={} underlying={}\n\
                 records={}\n\
                 refresh_count={}\n\
                 strike_selection_mode={}\n\
                 selected_strikes={}\n\
                 option_count={}\n\
                 atm_reference_source={}\n\
                 oi_ranked_top_n={}",
                report.venue_id,
                report.underlying,
                report.record_count,
                report.refresh_count,
                report.strike_selection_mode,
                report.selected_strike_count,
                report.option_count,
                report.atm_reference_source,
                report
                    .oi_ranked_top_n
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum StrikeModeArg {
    OiRanked,
    All,
}

pub(crate) fn strike_profile_from_spec(
    spec: &catalog_capture_core::OptionUniverseSpec,
) -> Option<StrikeSelectionProfile> {
    match spec.strike_policy {
        StrikePolicy::OiRanked { top_n } => Some(StrikeSelectionProfile::OiRanked { top_n }),
        StrikePolicy::AllStrikes => Some(StrikeSelectionProfile::AllStrikes {
            min_strikes: ALL_STRIKES_MIN_SELECTED_STRIKES,
        }),
        StrikePolicy::AtmRelative { .. } => None,
    }
}

fn strike_profile_from_arg(
    mode: StrikeModeArg,
    oi_ranked_top_n: Option<usize>,
    all_min_strikes: Option<usize>,
) -> Result<StrikeSelectionProfile> {
    match mode {
        StrikeModeArg::OiRanked => {
            let top_n = oi_ranked_top_n.ok_or_else(|| {
                anyhow::anyhow!("--oi-ranked-top-n is required when --strike-mode oi-ranked")
            })?;
            if top_n == 0 {
                anyhow::bail!("--oi-ranked-top-n must be positive");
            }
            Ok(StrikeSelectionProfile::OiRanked { top_n })
        }
        StrikeModeArg::All => Ok(StrikeSelectionProfile::AllStrikes {
            min_strikes: all_min_strikes.unwrap_or(ALL_STRIKES_MIN_SELECTED_STRIKES),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validation_options_for_config_detects_oi_ranked_profile() {
        let repo_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = crate::config::load_config(
            &repo_root.join("examples/capture.bybit-btc-universe-oi-ranked.toml"),
        )
        .expect("example should load");
        let effective = crate::config::resolve_config(config).expect("example should resolve");

        let options = metadata_validation_options_for_config(&effective);
        assert_eq!(
            options.strike_profile,
            Some(StrikeSelectionProfile::OiRanked { top_n: 3 })
        );
    }

    #[test]
    fn validation_options_from_cli_requires_oi_ranked_top_n() {
        let err = validation_options_from_cli(false, Some(StrikeModeArg::OiRanked), None, None)
            .expect_err("missing top_n should fail");
        assert!(err.to_string().contains("--oi-ranked-top-n"));
    }
}