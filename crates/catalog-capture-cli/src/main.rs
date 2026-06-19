mod config;
mod option_universe;
mod runner;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use config::{load_config, render_effective_config, resolve_config};
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
    },
    Validate {
        #[arg(long)]
        config: PathBuf,
    },
    PrintEffectiveConfig {
        #[arg(long)]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();

    match cli.command {
        Command::Run { config } => {
            let loaded = load_config(&config)?;
            let effective = resolve_config(loaded)?;
            validate_runtime(&effective)?;
            run_capture(effective).await?;
        }
        Command::Validate { config } => {
            let loaded = load_config(&config)?;
            let effective = resolve_config(loaded)?;
            validate_runtime(&effective)?;
            println!("Configuration is valid: {}", config.display());
        }
        Command::PrintEffectiveConfig { config } => {
            let loaded = load_config(&config)?;
            println!("{}", render_effective_config(&loaded)?);
        }
    }

    Ok(())
}
