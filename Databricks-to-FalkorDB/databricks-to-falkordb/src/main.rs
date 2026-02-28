mod config;
mod cypher;
mod mapping;
mod orchestrator;
mod sink;
mod sink_async;
mod source;
mod state;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::config::Config;

#[derive(Parser, Debug)]
#[command(name = "databricks-to-falkordb")]
#[command(about = "Load data from Databricks into FalkorDB using declarative mappings.")]
struct Cli {
    /// Path to JSON/YAML config file.
    #[arg(long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cfg = Config::from_file(&cli.config)?;

    orchestrator::run_once(&cfg).await?;

    Ok(())
}
