mod config;
mod cypher;
mod mapping;
mod metrics;
mod orchestrator;
mod scaffold;
mod sink;
mod sink_async;
mod source;
mod state;
use std::fs;

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::metrics::{serve_metrics, METRICS};
use crate::orchestrator::{run_daemon, run_once};
const SINGLE_RUN_METRICS_GRACE_PERIOD_SECS: u64 = 10;

#[derive(Parser, Debug)]
#[command(name = "bigquery-to-falkordb")]
#[command(about = "Load data from BigQuery into FalkorDB using declarative mappings.")]
struct Cli {
    /// Path to JSON/YAML config file.
    #[arg(long)]
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

    /// Port to expose Prometheus-style metrics on.
    #[arg(
        long,
        env = "BIGQUERY_TO_FALKORDB_METRICS_PORT",
        default_value_t = 9995
    )]
    metrics_port: u16,

    /// Introspect source BigQuery schema and print a normalized summary.
    #[arg(long)]
    introspect_schema: bool,

    /// Generate a YAML template mapping from source schema.
    #[arg(long)]
    generate_template: bool,

    /// Output path for generated template YAML (stdout if omitted).
    #[arg(long, value_name = "PATH")]
    output: Option<PathBuf>,
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

    if cli.introspect_schema || cli.generate_template {
        if cli.daemon || cli.purge_graph || !cli.purge_mapping.is_empty() {
            anyhow::bail!(
                "Scaffold mode flags (--introspect-schema/--generate-template) cannot be combined with daemon/purge flags"
            );
        }
        if cli.output.is_some() && !cli.generate_template {
            anyhow::bail!("--output requires --generate-template");
        }

        let result = scaffold::introspect_bigquery_schema(&cfg).await?;
        if cli.introspect_schema {
            let schema_yaml = serde_yaml::to_string(&result.schema)?;
            println!("{schema_yaml}");
        }
        if cli.generate_template {
            let template_yaml = scaffold::generate_template_yaml(&cfg, &result.schema)?;
            if let Some(path) = &cli.output {
                fs::write(path, template_yaml)?;
                println!("Template written to {}", path.display());
            } else {
                println!("{template_yaml}");
            }
        }
        return Ok(());
    }

    if cli.daemon {
        run_daemon(&cfg, cli.purge_graph, &cli.purge_mapping, cli.interval_secs).await?;
    } else {
        let run_result = run_once(&cfg, cli.purge_graph, &cli.purge_mapping).await;
        if run_result.is_err() {
            METRICS.inc_failed_runs();
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(
            SINGLE_RUN_METRICS_GRACE_PERIOD_SECS,
        ))
        .await;
        run_result?;
    }

    Ok(())
}
