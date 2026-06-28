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

mod config;
mod hip4;
mod metrics_server;
mod option_universe;
mod plan_overlap;
mod runner;
mod universe_materialize;

use std::path::{Path, PathBuf};

use anyhow::Result;
use catalog_capture_core::catalog_root_from_uri;
use catalog_capture_core::{
    OptionUniverseReadbackReport, OptionUniverseResolutionValidationReport,
};
use clap::{Parser, Subcommand, ValueEnum};
use config::{load_config, render_effective_config, resolve_config, EffectiveConfig};
use hip4::{
    render_hip4_universe_reports_json, render_hip4_universe_reports_text,
    Hip4UniverseResolutionReport,
};
use nautilus_common::logging::ensure_logging_initialized;
use option_universe::{
    load_option_universe_summaries, merge_validation_options, readback_options_for_config,
    readback_options_from_cli, render_option_universe_catalog_validation_json,
    render_option_universe_catalog_validation_text,
    render_option_universe_metadata_validation_json,
    render_option_universe_metadata_validation_text, render_option_universe_readback_json,
    render_option_universe_readback_text, render_option_universe_reports_json,
    render_option_universe_reports_text, render_option_universe_summaries_json,
    render_option_universe_summaries_text, resolve_option_universe_reports,
    run_option_universe_readback_validation, run_option_universe_validation_suite,
    validate_option_universe_catalog, validate_option_universe_metadata,
    validation_options_for_preset, validation_options_from_cli,
    OptionUniverseCatalogValidationOverrides, OptionUniverseCatalogValidationPreset,
    OptionUniverseCatalogValidationReport, OptionUniverseOutputFormat,
    OptionUniverseResolutionReport, OptionUniverseValidationSuiteOptions, PostRunReportOptions,
    StrikeModeArg,
};
use runner::{
    materialize_full_capture_plan, run_capture, run_capture_with_plan_and_reports, validate_runtime,
};

#[derive(Debug, Parser)]
#[command(name = "nautilus-capture")]
#[command(about = "Run direct Nautilus catalog capture from a TOML config")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OptionUniverseOutputFormatArg {
    Json,
    Text,
}

impl From<OptionUniverseOutputFormatArg> for OptionUniverseOutputFormat {
    fn from(value: OptionUniverseOutputFormatArg) -> Self {
        match value {
            OptionUniverseOutputFormatArg::Json => OptionUniverseOutputFormat::Json,
            OptionUniverseOutputFormatArg::Text => OptionUniverseOutputFormat::Text,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OptionUniverseCatalogValidationPresetArg {
    PostCapture,
    RollingAutorefresh,
    VenueTrades,
    Research,
    TradesSmoke,
    BarsSmoke,
    BookDeltasSmoke,
}

impl From<OptionUniverseCatalogValidationPresetArg> for OptionUniverseCatalogValidationPreset {
    fn from(value: OptionUniverseCatalogValidationPresetArg) -> Self {
        match value {
            OptionUniverseCatalogValidationPresetArg::PostCapture => {
                OptionUniverseCatalogValidationPreset::PostCapture
            }
            OptionUniverseCatalogValidationPresetArg::RollingAutorefresh => {
                OptionUniverseCatalogValidationPreset::RollingAutorefresh
            }
            OptionUniverseCatalogValidationPresetArg::VenueTrades => {
                OptionUniverseCatalogValidationPreset::VenueTrades
            }
            OptionUniverseCatalogValidationPresetArg::Research => {
                OptionUniverseCatalogValidationPreset::Research
            }
            OptionUniverseCatalogValidationPresetArg::TradesSmoke => {
                OptionUniverseCatalogValidationPreset::TradesSmoke
            }
            OptionUniverseCatalogValidationPresetArg::BarsSmoke => {
                OptionUniverseCatalogValidationPreset::BarsSmoke
            }
            OptionUniverseCatalogValidationPresetArg::BookDeltasSmoke => {
                OptionUniverseCatalogValidationPreset::BookDeltasSmoke
            }
        }
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    Run {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        print_option_universe: bool,
        #[arg(long)]
        dry_run_resolve: bool,
        #[arg(long, value_enum, default_value_t = OptionUniverseOutputFormatArg::Json)]
        option_universe_format: OptionUniverseOutputFormatArg,
        #[arg(
            long,
            help = "Skip inspect + validate report after capture when option_universe is configured"
        )]
        skip_post_run_report: bool,
        #[arg(
            long,
            value_enum,
            help = "Override inferred post-run validation preset"
        )]
        post_run_validation_preset: Option<OptionUniverseCatalogValidationPresetArg>,
    },
    Validate {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        print_option_universe: bool,
        #[arg(long, value_enum, default_value_t = OptionUniverseOutputFormatArg::Json)]
        option_universe_format: OptionUniverseOutputFormatArg,
    },
    PrintEffectiveConfig {
        #[arg(long)]
        config: PathBuf,
    },
    ResolveOptionUniverse {
        #[arg(long)]
        config: PathBuf,
        #[arg(long, value_enum, default_value_t = OptionUniverseOutputFormatArg::Json)]
        option_universe_format: OptionUniverseOutputFormatArg,
    },
    InspectOptionUniverse {
        #[arg(long)]
        catalog_uri: String,
        #[arg(long, value_enum, default_value_t = OptionUniverseOutputFormatArg::Json)]
        option_universe_format: OptionUniverseOutputFormatArg,
    },
    ValidateOptionUniverseCatalog {
        #[arg(long)]
        catalog_uri: String,
        #[arg(long, value_enum, default_value_t = OptionUniverseOutputFormatArg::Json)]
        option_universe_format: OptionUniverseOutputFormatArg,
        #[arg(
            long,
            value_enum,
            help = "Built-in validation baseline; explicit flags override preset defaults"
        )]
        preset: Option<OptionUniverseCatalogValidationPresetArg>,
        #[arg(long, help = "Override preset/default minimum parquet rows per family")]
        min_rows: Option<i64>,
        #[arg(long, help = "Override preset/default minimum perp trade rows")]
        min_perp_trade_rows: Option<i64>,
        #[arg(long, help = "Override preset/default minimum option trade rows")]
        min_option_trade_rows: Option<i64>,
        #[arg(
            long,
            help = "Require instrument_status and instrument_closes parquet rows"
        )]
        require_contract_state: bool,
        #[arg(long, help = "Require at least one applied runtime refresh delta")]
        require_refresh_change: bool,
        #[arg(long, help = "Require bar parquet rows for each bar_type identifier")]
        bar_type: Vec<String>,
    },
    ValidateOptionUniverseMetadata {
        #[arg(long)]
        catalog_uri: String,
        #[arg(long, value_enum, default_value_t = OptionUniverseOutputFormatArg::Json)]
        option_universe_format: OptionUniverseOutputFormatArg,
        #[arg(long, help = "Require at least one applied runtime refresh delta")]
        require_refresh_change: bool,
        #[arg(
            long,
            value_enum,
            help = "Assert strike_selection_mode-specific metadata shape"
        )]
        strike_mode: Option<StrikeModeArg>,
        #[arg(long, help = "Expected oi_ranked_top_n when --strike-mode oi-ranked")]
        oi_ranked_top_n: Option<usize>,
        #[arg(
            long,
            help = "Minimum selected strikes when --strike-mode all (default: 5)"
        )]
        all_min_strikes: Option<usize>,
    },
    ValidateOptionUniverseReadback {
        #[arg(long)]
        catalog_uri: String,
        #[arg(long, value_enum, default_value_t = OptionUniverseOutputFormatArg::Json)]
        option_universe_format: OptionUniverseOutputFormatArg,
        #[arg(long, help = "Hedge/reference perp instrument id")]
        perp_id: Option<String>,
        #[arg(
            long,
            help = "Option instrument id to validate; repeat for multiple options"
        )]
        option_id: Vec<String>,
        #[arg(long, help = "Minimum rows per readback family (default: 1)")]
        min_rows: Option<i64>,
        #[arg(long, help = "Minimum perp trade ticks (0 skips trade readback)")]
        min_perp_trade_rows: Option<i64>,
        #[arg(
            long,
            help = "Minimum option trade ticks (0 skips option trade readback)"
        )]
        min_option_trade_rows: Option<i64>,
        #[arg(long, help = "Require instrument_status and instrument_closes rows")]
        require_contract_state: bool,
        #[arg(long, help = "Bar type identifier to validate via ParquetDataCatalog")]
        bar_type: Vec<String>,
        #[arg(
            long,
            help = "Infer perp/option ids and validation thresholds from catalog metadata + TOML config"
        )]
        config: Option<PathBuf>,
    },
    ValidateOptionUniverse {
        #[arg(long)]
        catalog_uri: String,
        #[arg(long, help = "Capture profile used to infer validation thresholds")]
        config: PathBuf,
        #[arg(long, value_enum, default_value_t = OptionUniverseOutputFormatArg::Json)]
        option_universe_format: OptionUniverseOutputFormatArg,
        #[arg(long, value_enum, help = "Override inferred catalog validation preset")]
        preset: Option<OptionUniverseCatalogValidationPresetArg>,
        #[arg(long, help = "Override preset/default minimum perp trade rows")]
        min_perp_trade_rows: Option<i64>,
        #[arg(long, help = "Override preset/default minimum option trade rows")]
        min_option_trade_rows: Option<i64>,
        #[arg(
            long,
            help = "Require instrument_status and instrument_closes parquet rows"
        )]
        require_contract_state: bool,
        #[arg(long, help = "Require at least one applied runtime refresh delta")]
        require_refresh_change: bool,
        #[arg(
            long,
            value_enum,
            help = "Override strike_selection_mode metadata assertions"
        )]
        strike_mode: Option<StrikeModeArg>,
        #[arg(long, help = "Expected oi_ranked_top_n when --strike-mode oi-ranked")]
        oi_ranked_top_n: Option<usize>,
        #[arg(
            long,
            help = "Minimum selected strikes when --strike-mode all (default: 5)"
        )]
        all_min_strikes: Option<usize>,
        #[arg(long, help = "Skip lineage inspect section")]
        skip_inspect: bool,
        #[arg(long, help = "Skip ParquetDataCatalog readback section")]
        skip_readback: bool,
        #[arg(long, help = "Override hedge/reference perp id for readback")]
        perp_id: Option<String>,
        #[arg(
            long,
            help = "Override option ids for readback; repeat for multiple options"
        )]
        option_id: Vec<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    ensure_logging_initialized();
    let cli = Cli::parse();

    match cli.command {
        Command::Run {
            config,
            print_option_universe,
            dry_run_resolve,
            option_universe_format,
            skip_post_run_report,
            post_run_validation_preset,
        } => {
            let effective = load_validated_config(&config)?;
            let post_run = build_post_run_report_options(
                skip_post_run_report,
                post_run_validation_preset,
                option_universe_format.into(),
            );
            if print_option_universe || dry_run_resolve {
                let materialized = materialize_full_capture_plan(&effective).await?;
                print_option_universe_report_values(
                    &materialized.option_universe_reports,
                    option_universe_format.into(),
                )?;
                print_hip4_universe_report_values(
                    &materialized.hip4_reports,
                    option_universe_format.into(),
                )?;
                if dry_run_resolve {
                    return Ok(());
                }
                run_capture_with_plan_and_reports(
                    effective,
                    materialized.plan,
                    &materialized.option_universe_reports,
                    &materialized.hip4_reports,
                    &materialized.hip4_resolved,
                    post_run,
                )
                .await?;
                return Ok(());
            }
            run_capture(effective, post_run).await?;
        }
        Command::Validate {
            config,
            print_option_universe,
            option_universe_format: _option_universe_format,
        } => {
            let _effective = load_validated_config(&config)?;
            println!("Configuration is valid: {}", config.display());
            if print_option_universe {
                println!(
                    "Option universe preflight requires live venue metadata; use `resolve-option-universe` or `run --dry-run-resolve`."
                );
            }
        }
        Command::PrintEffectiveConfig { config } => {
            let loaded = load_config(&config)?;
            println!("{}", render_effective_config(&loaded)?);
        }
        Command::ResolveOptionUniverse {
            config,
            option_universe_format,
        } => {
            let effective = load_validated_config(&config)?;
            print_option_universe_reports(&effective, option_universe_format.into()).await?;
        }
        Command::InspectOptionUniverse {
            catalog_uri,
            option_universe_format,
        } => {
            let catalog_root = catalog_root_from_uri(&catalog_uri)?;
            let summaries = load_option_universe_summaries(&catalog_root)?;
            print_option_universe_summary_values(&summaries, option_universe_format.into())?;
        }
        Command::ValidateOptionUniverseCatalog {
            catalog_uri,
            option_universe_format,
            preset,
            min_rows,
            min_perp_trade_rows,
            min_option_trade_rows,
            require_contract_state,
            require_refresh_change,
            bar_type,
        } => {
            let catalog_root = catalog_root_from_uri(&catalog_uri)?;
            let base = preset
                .map(Into::into)
                .map(validation_options_for_preset)
                .unwrap_or_else(|| {
                    validation_options_for_preset(
                        OptionUniverseCatalogValidationPreset::PostCapture,
                    )
                });
            let options = merge_validation_options(
                base,
                &OptionUniverseCatalogValidationOverrides {
                    min_rows,
                    min_perp_trade_rows,
                    min_option_trade_rows,
                    require_contract_state,
                    require_refresh_change,
                    bar_types: bar_type,
                },
            );
            let reports = validate_option_universe_catalog(&catalog_root, &options)?;
            print_option_universe_catalog_validation_values(
                &reports,
                option_universe_format.into(),
            )?;
        }
        Command::ValidateOptionUniverseMetadata {
            catalog_uri,
            option_universe_format,
            require_refresh_change,
            strike_mode,
            oi_ranked_top_n,
            all_min_strikes,
        } => {
            let catalog_root = catalog_root_from_uri(&catalog_uri)?;
            let options = validation_options_from_cli(
                require_refresh_change,
                strike_mode,
                oi_ranked_top_n,
                all_min_strikes,
            )?;
            let reports = validate_option_universe_metadata(&catalog_root, &options)?;
            print_option_universe_metadata_validation_values(
                &reports,
                option_universe_format.into(),
            )?;
        }
        Command::ValidateOptionUniverseReadback {
            catalog_uri,
            option_universe_format,
            perp_id,
            option_id,
            min_rows,
            min_perp_trade_rows,
            min_option_trade_rows,
            require_contract_state,
            bar_type,
            config,
        } => {
            let catalog_root = catalog_root_from_uri(&catalog_uri)?;
            let mut options = if let Some(config_path) = config {
                let effective = load_validated_config(&config_path)?;
                readback_options_for_config(&effective, &catalog_root, require_contract_state)?
            } else {
                readback_options_from_cli(
                    perp_id,
                    option_id,
                    min_rows,
                    min_perp_trade_rows,
                    require_contract_state,
                    bar_type,
                )?
            };
            if let Some(min_option_trade_rows) = min_option_trade_rows {
                options.min_option_trade_rows = min_option_trade_rows;
            }
            let report = run_option_universe_readback_validation(&catalog_root, &options)?;
            print_option_universe_readback_values(&report, option_universe_format.into())?;
        }
        Command::ValidateOptionUniverse {
            catalog_uri,
            config,
            option_universe_format,
            preset,
            min_perp_trade_rows,
            min_option_trade_rows,
            require_contract_state,
            require_refresh_change,
            strike_mode,
            oi_ranked_top_n,
            all_min_strikes,
            skip_inspect,
            skip_readback,
            perp_id,
            option_id,
        } => {
            let catalog_root = catalog_root_from_uri(&catalog_uri)?;
            let effective = load_validated_config(&config)?;
            let metadata_options =
                validation_options_from_cli(false, strike_mode, oi_ranked_top_n, all_min_strikes)?;
            println!("=== Option universe validation suite ===");
            println!("Catalog dir: {}", catalog_root.display());
            println!("Config: {}", config.display());
            run_option_universe_validation_suite(
                &catalog_root,
                &effective,
                &OptionUniverseValidationSuiteOptions {
                    format: option_universe_format.into(),
                    include_inspect: !skip_inspect,
                    include_readback: !skip_readback,
                    require_refresh_change,
                    require_contract_state,
                    catalog_preset_override: preset.map(Into::into),
                    catalog_overrides: OptionUniverseCatalogValidationOverrides {
                        min_perp_trade_rows,
                        min_option_trade_rows,
                        require_contract_state,
                        require_refresh_change,
                        ..OptionUniverseCatalogValidationOverrides::default()
                    },
                    strike_profile_override: metadata_options.strike_profile,
                    readback_perp_id: perp_id,
                    readback_option_ids: if option_id.is_empty() {
                        None
                    } else {
                        Some(option_id)
                    },
                },
            )?;
        }
    }

    Ok(())
}

fn build_post_run_report_options(
    skip_post_run_report: bool,
    post_run_validation_preset: Option<OptionUniverseCatalogValidationPresetArg>,
    format: OptionUniverseOutputFormat,
) -> PostRunReportOptions {
    PostRunReportOptions {
        enabled: !skip_post_run_report,
        format,
        validation_preset_override: post_run_validation_preset.map(Into::into),
        validation_overrides: OptionUniverseCatalogValidationOverrides::default(),
    }
}

fn load_validated_config(path: &Path) -> Result<EffectiveConfig> {
    let loaded = load_config(path)?;
    let effective = resolve_config(loaded)?;
    validate_runtime(&effective)?;
    Ok(effective)
}

async fn print_option_universe_reports(
    config: &EffectiveConfig,
    format: OptionUniverseOutputFormat,
) -> Result<()> {
    let reports = resolve_option_universe_reports(config).await?;
    print_option_universe_report_values(&reports, format)
}

fn print_option_universe_report_values(
    reports: &[OptionUniverseResolutionReport],
    format: OptionUniverseOutputFormat,
) -> Result<()> {
    match format {
        OptionUniverseOutputFormat::Json => {
            println!("{}", render_option_universe_reports_json(reports)?);
        }
        OptionUniverseOutputFormat::Text => {
            println!("{}", render_option_universe_reports_text(reports));
        }
    }
    Ok(())
}

fn print_hip4_universe_report_values(
    reports: &[Hip4UniverseResolutionReport],
    format: OptionUniverseOutputFormat,
) -> Result<()> {
    match format {
        OptionUniverseOutputFormat::Json => {
            println!("{}", render_hip4_universe_reports_json(reports)?);
        }
        OptionUniverseOutputFormat::Text => {
            println!("{}", render_hip4_universe_reports_text(reports));
        }
    }
    Ok(())
}

fn print_option_universe_summary_values(
    summaries: &[catalog_capture_core::OptionUniverseResolutionSummary],
    format: OptionUniverseOutputFormat,
) -> Result<()> {
    match format {
        OptionUniverseOutputFormat::Json => {
            println!("{}", render_option_universe_summaries_json(summaries)?);
        }
        OptionUniverseOutputFormat::Text => {
            println!("{}", render_option_universe_summaries_text(summaries));
        }
    }
    Ok(())
}

fn print_option_universe_readback_values(
    report: &OptionUniverseReadbackReport,
    format: OptionUniverseOutputFormat,
) -> Result<()> {
    match format {
        OptionUniverseOutputFormat::Json => {
            println!("{}", render_option_universe_readback_json(report)?);
        }
        OptionUniverseOutputFormat::Text => {
            println!("{}", render_option_universe_readback_text(report));
        }
    }
    Ok(())
}

fn print_option_universe_metadata_validation_values(
    reports: &[OptionUniverseResolutionValidationReport],
    format: OptionUniverseOutputFormat,
) -> Result<()> {
    match format {
        OptionUniverseOutputFormat::Json => {
            println!(
                "{}",
                render_option_universe_metadata_validation_json(reports)?
            );
        }
        OptionUniverseOutputFormat::Text => {
            println!(
                "{}",
                render_option_universe_metadata_validation_text(reports)
            );
        }
    }
    Ok(())
}

fn print_option_universe_catalog_validation_values(
    reports: &[OptionUniverseCatalogValidationReport],
    format: OptionUniverseOutputFormat,
) -> Result<()> {
    match format {
        OptionUniverseOutputFormat::Json => {
            println!(
                "{}",
                render_option_universe_catalog_validation_json(reports)?
            );
        }
        OptionUniverseOutputFormat::Text => {
            println!(
                "{}",
                render_option_universe_catalog_validation_text(reports)
            );
        }
    }
    Ok(())
}
