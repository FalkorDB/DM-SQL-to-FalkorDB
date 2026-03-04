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
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::metrics::serve_metrics;
use crate::orchestrator::{run_daemon, run_once};

#[derive(Parser, Debug)]
#[command(name = "postgres-to-falkordb")]
#[command(about = "Load data from PostgreSQL into FalkorDB using declarative mappings.")]
struct Cli {
    /// Path to JSON/YAML config file.
    #[arg(long)]
    config: PathBuf,

    /// Run continuously, performing syncs at a fixed interval.
    #[arg(long)]
    daemon: bool,

    /// Interval in seconds between sync runs in daemon mode.
    #[arg(long, value_name = "SECS", default_value_t = 60)]
    interval_secs: u64,

    /// Port to expose Prometheus-style metrics on.
    #[arg(long, env = "POSTGRES_TO_FALKORDB_METRICS_PORT", default_value_t = 9993)]
    metrics_port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();
    let cfg = Config::from_file(&cli.config)?;
    let metrics_port = cli.metrics_port;
    tokio::spawn(async move {
        let addr = ([0, 0, 0, 0], metrics_port).into();
        serve_metrics(addr).await;
    });

    if cli.daemon {
        run_daemon(&cfg, cli.interval_secs).await?;
    } else {
        run_once(&cfg).await?;
    }

    println!("Load completed successfully.");
    Ok(())
}

fn init_tracing() {
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .init();
}
