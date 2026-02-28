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
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::metrics::serve_metrics;
use crate::orchestrator::{run_daemon, run_once};

/// CLI for phase 2+: supports multi-mapping, async writes, purge, and daemon mode.
#[derive(Debug, Parser)]
#[command(name = "snowflake-to-falkordb")]
#[command(about = "Load tabular/Snowflake data into FalkorDB via UNWIND+MERGE", long_about = None)]
struct Cli {
    /// Path to JSON or YAML config file.
    #[arg(long, value_name = "PATH")]
    config: PathBuf,

    /// Purge the entire graph before loading.
    #[arg(long)]
    purge_graph: bool,

    /// Purge only specific mappings before loading (can be repeated).
    #[arg(long, value_name = "MAPPING_NAME")]
    purge_mapping: Vec<String>,

    /// Run continuously, performing syncs at a fixed interval.
    #[arg(long)]
    daemon: bool,

    /// Interval in seconds between sync runs in daemon mode.
    #[arg(long, value_name = "SECS", default_value_t = 60)]
    interval_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();
    let cfg = Config::from_file(&cli.config)?;

    // Start metrics server on 0.0.0.0:9898
    tokio::spawn(async {
        let addr = ([0, 0, 0, 0], 9898).into();
        serve_metrics(addr).await;
    });

    if cli.daemon {
        run_daemon(&cfg, cli.purge_graph, &cli.purge_mapping, cli.interval_secs).await?;
    } else {
        run_once(&cfg, cli.purge_graph, &cli.purge_mapping).await?;
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
