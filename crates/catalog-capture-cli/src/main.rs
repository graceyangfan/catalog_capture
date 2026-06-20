mod config;
mod option_universe;
mod runner;

use std::path::PathBuf;

use anyhow::Result;
use catalog_capture_core::catalog_root_from_uri;
use clap::{Parser, Subcommand, ValueEnum};
use config::{load_config, render_effective_config, resolve_config, EffectiveConfig};
use option_universe::{
    load_option_universe_summaries, materialize_capture_plan_with_reports,
    merge_validation_options, render_option_universe_catalog_validation_json,
    render_option_universe_catalog_validation_text, render_option_universe_reports_json,
    render_option_universe_reports_text, render_option_universe_summaries_json,
    render_option_universe_summaries_text, resolve_option_universe_reports,
    validate_option_universe_catalog, validation_options_for_preset,
    OptionUniverseCatalogValidationOverrides, OptionUniverseCatalogValidationPreset,
    OptionUniverseCatalogValidationReport, OptionUniverseOutputFormat,
    OptionUniverseResolutionReport, PostRunReportOptions,
};
use runner::{run_capture, run_capture_with_plan_and_reports, validate_runtime};

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
        #[arg(long, help = "Require instrument_status and instrument_closes parquet rows")]
        require_contract_state: bool,
        #[arg(long, help = "Require at least one applied runtime refresh delta")]
        require_refresh_change: bool,
        #[arg(long, help = "Require bar parquet rows for each bar_type identifier")]
        bar_type: Vec<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
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
                let materialized = materialize_capture_plan_with_reports(&effective).await?;
                print_option_universe_report_values(
                    &materialized.reports,
                    option_universe_format.into(),
                )?;
                if dry_run_resolve {
                    return Ok(());
                }
                run_capture_with_plan_and_reports(
                    effective,
                    materialized.plan,
                    &materialized.reports,
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
            require_contract_state,
            require_refresh_change,
            bar_type,
        } => {
            let catalog_root = catalog_root_from_uri(&catalog_uri)?;
            let base = preset
                .map(Into::into)
                .map(validation_options_for_preset)
                .unwrap_or_else(|| {
                    validation_options_for_preset(OptionUniverseCatalogValidationPreset::PostCapture)
                });
            let options = merge_validation_options(
                base,
                &OptionUniverseCatalogValidationOverrides {
                    min_rows,
                    min_perp_trade_rows,
                    require_contract_state,
                    require_refresh_change,
                    bar_types: bar_type,
                },
            );
            let reports = validate_option_universe_catalog(&catalog_root, &options)?;
            print_option_universe_catalog_validation_values(&reports, option_universe_format.into())?;
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

fn load_validated_config(path: &PathBuf) -> Result<EffectiveConfig> {
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
            println!("{}", render_option_universe_reports_json(&reports)?);
        }
        OptionUniverseOutputFormat::Text => {
            println!("{}", render_option_universe_reports_text(&reports));
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
