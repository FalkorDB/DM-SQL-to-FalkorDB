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
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::metrics::serve_metrics;
use crate::orchestrator::{run_daemon, run_once};
const SINGLE_RUN_METRICS_GRACE_PERIOD_SECS: u64 = 10;

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

    /// Introspect source PostgreSQL schema and print a normalized summary.
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
    init_tracing();

    let cli = Cli::parse();
    let cfg = Config::from_file(&cli.config)?;
    let metrics_port = cli.metrics_port;
    tokio::spawn(async move {
        let addr = ([0, 0, 0, 0], metrics_port).into();
        serve_metrics(addr).await;
    });
    if cli.introspect_schema || cli.generate_template {
        if cli.daemon {
            anyhow::bail!(
                "Scaffold mode flags (--introspect-schema/--generate-template) cannot be combined with --daemon"
            );
        }
        if cli.output.is_some() && !cli.generate_template {
            anyhow::bail!("--output requires --generate-template");
        }

        let result = scaffold::introspect_postgres_schema(&cfg).await?;
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
        run_daemon(&cfg, cli.interval_secs).await?;
    } else {
        let run_result = run_once(&cfg).await;
        tokio::time::sleep(tokio::time::Duration::from_secs(
            SINGLE_RUN_METRICS_GRACE_PERIOD_SECS,
        ))
        .await;
        run_result?;
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
