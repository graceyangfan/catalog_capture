use std::path::Path;

use anyhow::Result;

use crate::config::EffectiveConfig;

use super::validate_suite::{
    run_option_universe_validation_suite, OptionUniverseValidationSuiteOptions,
};
use super::catalog_presets::{
    OptionUniverseCatalogValidationOverrides, OptionUniverseCatalogValidationPreset,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionUniverseOutputFormat {
    Json,
    Text,
}

#[derive(Debug, Clone)]
pub struct PostRunReportOptions {
    pub enabled: bool,
    pub format: OptionUniverseOutputFormat,
    pub validation_preset_override: Option<OptionUniverseCatalogValidationPreset>,
    pub validation_overrides: OptionUniverseCatalogValidationOverrides,
}

impl Default for PostRunReportOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            format: OptionUniverseOutputFormat::Text,
            validation_preset_override: None,
            validation_overrides: OptionUniverseCatalogValidationOverrides::default(),
        }
    }
}

pub fn run_option_universe_post_run_report(
    catalog_root: &Path,
    config: &EffectiveConfig,
    options: &PostRunReportOptions,
) -> Result<()> {
    if !options.enabled || config.option_universes.is_empty() {
        return Ok(());
    }

    println!("\n=== Option universe post-run report ===");
    println!("Catalog dir: {}", catalog_root.display());

    run_option_universe_validation_suite(
        catalog_root,
        config,
        &OptionUniverseValidationSuiteOptions {
            format: options.format,
            include_inspect: true,
            include_readback: true,
            require_refresh_change: config.runtime.option_universe_refresh.enabled
                || options.validation_overrides.require_refresh_change,
            require_contract_state: options.validation_overrides.require_contract_state,
            catalog_preset_override: options.validation_preset_override,
            catalog_overrides: options.validation_overrides.clone(),
            ..OptionUniverseValidationSuiteOptions::default()
        },
    )
}