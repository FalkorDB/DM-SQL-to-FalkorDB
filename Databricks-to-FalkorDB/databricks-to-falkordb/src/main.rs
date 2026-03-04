mod config;
mod cypher;
mod mapping;
mod metrics;
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
use crate::metrics::{serve_metrics, METRICS};

#[derive(Parser, Debug)]
#[command(name = "databricks-to-falkordb")]
#[command(about = "Load data from Databricks into FalkorDB using declarative mappings.")]
struct Cli {
    /// Path to JSON/YAML config file.
    #[arg(long)]
    config: PathBuf,

    /// Port to expose Prometheus-style metrics on.
    #[arg(long, env = "DATABRICKS_TO_FALKORDB_METRICS_PORT", default_value_t = 9994)]
    metrics_port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cfg = Config::from_file(&cli.config)?;
    let metrics_port = cli.metrics_port;
    tokio::spawn(async move {
        let addr = ([0, 0, 0, 0], metrics_port).into();
        serve_metrics(addr).await;
    });

    if let Err(e) = orchestrator::run_once(&cfg).await {
        METRICS.inc_failed_runs();
        return Err(e);
    }

    Ok(())
}
