mod config;
mod option_universe;
mod runner;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use config::{load_config, render_effective_config, resolve_config, EffectiveConfig};
use option_universe::{render_option_universe_reports_json, resolve_option_universe_reports};
use runner::{run_capture, validate_runtime};

#[derive(Debug, Parser)]
#[command(name = "nautilus-capture")]
#[command(about = "Run direct Nautilus catalog capture from a TOML config")]
struct Cli {
    #[command(subcommand)]
    command: Command,
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
    },
    Validate {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        print_option_universe: bool,
    },
    PrintEffectiveConfig {
        #[arg(long)]
        config: PathBuf,
    },
    ResolveOptionUniverse {
        #[arg(long)]
        config: PathBuf,
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
        } => {
            let effective = load_validated_config(&config)?;
            if print_option_universe || dry_run_resolve {
                print_option_universe_reports(&effective).await?;
            }
            if dry_run_resolve {
                return Ok(());
            }
            run_capture(effective).await?;
        }
        Command::Validate {
            config,
            print_option_universe,
        } => {
            let effective = load_validated_config(&config)?;
            println!("Configuration is valid: {}", config.display());
            if print_option_universe {
                print_option_universe_reports(&effective).await?;
            }
        }
        Command::PrintEffectiveConfig { config } => {
            let loaded = load_config(&config)?;
            println!("{}", render_effective_config(&loaded)?);
        }
        Command::ResolveOptionUniverse { config } => {
            let effective = load_validated_config(&config)?;
            print_option_universe_reports(&effective).await?;
        }
    }

    Ok(())
}

fn load_validated_config(path: &PathBuf) -> Result<EffectiveConfig> {
    let loaded = load_config(path)?;
    let effective = resolve_config(loaded)?;
    validate_runtime(&effective)?;
    Ok(effective)
}

async fn print_option_universe_reports(config: &EffectiveConfig) -> Result<()> {
    let reports = resolve_option_universe_reports(config).await?;
    println!("{}", render_option_universe_reports_json(&reports)?);
    Ok(())
}
