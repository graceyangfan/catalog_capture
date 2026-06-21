use std::path::Path;

use anyhow::Result;
use catalog_capture_core::{
    OptionUniverseReadbackOptions, OptionUniverseResolutionValidationOptions,
    StrikeSelectionProfile,
};

use crate::config::EffectiveConfig;

use super::catalog::{
    render_option_universe_catalog_validation_json, render_option_universe_catalog_validation_text,
    validate_option_universe_catalog,
};
use super::catalog_presets::{
    merge_validation_options, validation_options_for_config, validation_options_for_preset,
    OptionUniverseCatalogValidationOverrides, OptionUniverseCatalogValidationPreset,
};
use super::history::{
    load_option_universe_summaries, render_option_universe_summaries_json,
    render_option_universe_summaries_text,
};
use super::metadata::{
    render_option_universe_metadata_validation_json, render_option_universe_metadata_validation_text,
    validate_option_universe_metadata,
};
use super::post_run_report::OptionUniverseOutputFormat;
use super::readback::{
    readback_options_for_config, render_option_universe_readback_json,
    render_option_universe_readback_text, run_option_universe_readback_validation,
};
use super::metadata::{metadata_validation_options_for_config, strike_profile_from_spec};

#[derive(Debug, Clone)]
pub struct OptionUniverseValidationSuiteOptions {
    pub format: OptionUniverseOutputFormat,
    pub include_inspect: bool,
    pub include_readback: bool,
    pub require_refresh_change: bool,
    pub require_contract_state: bool,
    pub catalog_preset_override: Option<OptionUniverseCatalogValidationPreset>,
    pub strike_profile_override: Option<StrikeSelectionProfile>,
    pub readback_perp_id: Option<String>,
    pub readback_option_ids: Option<Vec<String>>,
    pub catalog_overrides: OptionUniverseCatalogValidationOverrides,
}

impl Default for OptionUniverseValidationSuiteOptions {
    fn default() -> Self {
        Self {
            format: OptionUniverseOutputFormat::Text,
            include_inspect: true,
            include_readback: true,
            require_refresh_change: false,
            require_contract_state: false,
            catalog_preset_override: None,
            strike_profile_override: None,
            readback_perp_id: None,
            readback_option_ids: None,
            catalog_overrides: OptionUniverseCatalogValidationOverrides::default(),
        }
    }
}

pub fn run_option_universe_validation_suite(
    catalog_root: &Path,
    config: &EffectiveConfig,
    options: &OptionUniverseValidationSuiteOptions,
) -> Result<()> {
    if config.option_universes.is_empty() {
        return Ok(());
    }

    if options.include_inspect {
        println!("\n--- Lineage inspect ---");
        let summaries = load_option_universe_summaries(catalog_root)?;
        match options.format {
            OptionUniverseOutputFormat::Json => {
                println!("{}", render_option_universe_summaries_json(&summaries)?);
            }
            OptionUniverseOutputFormat::Text => {
                println!("{}", render_option_universe_summaries_text(&summaries));
            }
        }
    }

    println!("\n--- Metadata validation ---");
    let metadata_options = metadata_validation_options_for_suite(config, options);
    let metadata_reports = validate_option_universe_metadata(catalog_root, &metadata_options)?;
    match options.format {
        OptionUniverseOutputFormat::Json => {
            println!(
                "{}",
                render_option_universe_metadata_validation_json(&metadata_reports)?
            );
        }
        OptionUniverseOutputFormat::Text => {
            println!(
                "{}",
                render_option_universe_metadata_validation_text(&metadata_reports)
            );
        }
    }

    if options.include_readback {
        println!("\n--- Catalog readback ---");
        let readback_options = readback_options_for_suite(config, catalog_root, options)?;
        let readback_report =
            run_option_universe_readback_validation(catalog_root, &readback_options)?;
        match options.format {
            OptionUniverseOutputFormat::Json => {
                println!("{}", render_option_universe_readback_json(&readback_report)?);
            }
            OptionUniverseOutputFormat::Text => {
                println!("{}", render_option_universe_readback_text(&readback_report));
            }
        }
    }

    println!("\n--- Catalog validation ---");
    let base = options
        .catalog_preset_override
        .map(validation_options_for_preset)
        .unwrap_or_else(|| validation_options_for_config(config));
    let mut catalog_overrides = options.catalog_overrides.clone();
    if options.require_contract_state {
        catalog_overrides.require_contract_state = true;
    }
    if options.require_refresh_change {
        catalog_overrides.require_refresh_change = true;
    }
    let catalog_options = merge_validation_options(base, &catalog_overrides);
    let catalog_reports = validate_option_universe_catalog(catalog_root, &catalog_options)?;
    match options.format {
        OptionUniverseOutputFormat::Json => {
            println!(
                "{}",
                render_option_universe_catalog_validation_json(&catalog_reports)?
            );
        }
        OptionUniverseOutputFormat::Text => {
            println!("{}", render_option_universe_catalog_validation_text(&catalog_reports));
        }
    }

    Ok(())
}

fn metadata_validation_options_for_suite(
    config: &EffectiveConfig,
    options: &OptionUniverseValidationSuiteOptions,
) -> OptionUniverseResolutionValidationOptions {
    let mut metadata_options = metadata_validation_options_for_config(config);
    metadata_options.require_refresh_change = options.require_refresh_change;
    if let Some(profile) = options.strike_profile_override {
        metadata_options.strike_profile = Some(profile);
    } else if metadata_options.strike_profile.is_none() {
        metadata_options.strike_profile = config
            .option_universes
            .iter()
            .find_map(strike_profile_from_spec);
    }
    metadata_options
}

fn readback_options_for_suite(
    config: &EffectiveConfig,
    catalog_root: &Path,
    options: &OptionUniverseValidationSuiteOptions,
) -> Result<OptionUniverseReadbackOptions> {
    let mut readback_options = readback_options_for_config(config, catalog_root)?;
    if let Some(perp_id) = &options.readback_perp_id {
        readback_options.perp_instrument_id = perp_id.clone();
    }
    if let Some(option_ids) = &options.readback_option_ids {
        if !option_ids.is_empty() {
            readback_options.option_instrument_ids = option_ids.clone();
        }
    }
    Ok(readback_options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog_capture_core::StrikeSelectionProfile;

    #[test]
    fn metadata_validation_options_for_suite_honors_refresh_override() {
        let repo_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = crate::config::load_config(
            &repo_root.join("examples/capture.deribit-btc-universe-autorefresh.toml"),
        )
        .expect("example should load");
        let effective = crate::config::resolve_config(config).expect("example should resolve");

        let options = OptionUniverseValidationSuiteOptions {
            require_refresh_change: false,
            ..OptionUniverseValidationSuiteOptions::default()
        };
        let metadata_options = metadata_validation_options_for_suite(&effective, &options);
        assert!(!metadata_options.require_refresh_change);
    }

    #[test]
    fn metadata_validation_options_for_suite_applies_strike_profile_override() {
        let repo_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = crate::config::load_config(
            &repo_root.join("examples/capture.deribit-btc-universe.toml"),
        )
        .expect("example should load");
        let effective = crate::config::resolve_config(config).expect("example should resolve");

        let options = OptionUniverseValidationSuiteOptions {
            strike_profile_override: Some(StrikeSelectionProfile::OiRanked { top_n: 3 }),
            ..OptionUniverseValidationSuiteOptions::default()
        };
        let metadata_options = metadata_validation_options_for_suite(&effective, &options);
        assert_eq!(
            metadata_options.strike_profile,
            Some(StrikeSelectionProfile::OiRanked { top_n: 3 })
        );
    }
}