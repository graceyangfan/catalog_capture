use std::path::Path;

use anyhow::Result;

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

    println!("\n--- Catalog validation ---");
    let base = options
        .validation_preset_override
        .map(validation_options_for_preset)
        .unwrap_or_else(|| validation_options_for_config(config));
    let validation_options =
        merge_validation_options(base, &options.validation_overrides);
    let reports = validate_option_universe_catalog(catalog_root, &validation_options)?;
    match options.format {
        OptionUniverseOutputFormat::Json => {
            println!(
                "{}",
                render_option_universe_catalog_validation_json(&reports)?
            );
        }
        OptionUniverseOutputFormat::Text => {
            println!("{}", render_option_universe_catalog_validation_text(&reports));
        }
    }

    Ok(())
}