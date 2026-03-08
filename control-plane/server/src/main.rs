mod auth;
mod metrics;
mod models;
mod runs;
mod scaffold;
mod store;
mod tools;

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use axum::routing::{get, post};
use axum::Router;
use clap::Parser;
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use crate::runs::RunManager;
use crate::store::Store;
use crate::tools::ToolRegistry;

#[derive(Parser, Debug, Clone)]
struct Cli {
    #[arg(long, env = "CONTROL_PLANE_BIND", default_value = "0.0.0.0:3003")]
    bind: SocketAddr,

    /// Path to the migration repo root (used to resolve tool manifests and working dirs).
    #[arg(long, env = "CONTROL_PLANE_REPO_ROOT")]
    repo_root: Option<PathBuf>,

    /// Directory for runtime data (sqlite db, run logs, materialized configs).
    #[arg(long, env = "CONTROL_PLANE_DATA_DIR")]
    data_dir: Option<PathBuf>,

    /// Directory containing the built UI (Vite dist). If not present, API still works.
    #[arg(long, env = "CONTROL_PLANE_UI_DIST")]
    ui_dist: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = Cli::parse();

    let default_repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .context("failed to resolve default repo root")?;

    let repo_root = cli.repo_root.unwrap_or(default_repo_root);

    let data_dir = cli
        .data_dir
        .unwrap_or_else(|| repo_root.join("control-plane/data"));

    tokio::fs::create_dir_all(&data_dir).await?;
    tokio::fs::create_dir_all(data_dir.join("runs")).await?;
    tokio::fs::create_dir_all(data_dir.join("configs")).await?;

    let db_path = data_dir.join("control-plane.sqlite");
    let store = Store::connect(&db_path).await?;

    let tools = ToolRegistry::load_from_repo(&repo_root)
        .await
        .context("failed to load tool manifests")?;

    store.upsert_tools(&tools).await?;

    let runs = RunManager::new(data_dir.join("runs"));

    let app_state = models::AppState {
        repo_root: repo_root.clone(),
        data_dir,
        tools,
        store,
        runs,
        api_key: std::env::var("CONTROL_PLANE_API_KEY").ok().filter(|v| !v.is_empty()),
    };

    let api_routes = Router::new()
        .route("/health", get(models::health))
        .route("/tools", get(tools::list_tools))
        .route("/tools/:tool_id", get(tools::get_tool))
        .route(
            "/tools/:tool_id/scaffold-template",
            post(scaffold::generate_scaffold_template),
        )
        .route("/metrics", get(metrics::list_tool_metrics))
        .route("/metrics/:tool_id", get(metrics::get_tool_metrics))
        .route("/configs", get(store::list_configs).post(store::create_config))
        .route(
            "/configs/:config_id",
            get(store::get_config).put(store::update_config),
        )
        .route("/configs/:config_id/state", get(store::get_config_state))
        .route(
            "/configs/:config_id/state/clear",
            post(store::clear_config_state),
        )
        .route("/runs", get(runs::list_runs).post(runs::start_run))
        .route("/runs/:run_id", get(runs::get_run))
        .route("/runs/:run_id/stop", post(runs::stop_run))
        .route("/runs/:run_id/events", get(runs::run_events_sse))
        .route("/runs/:run_id/logs", get(runs::run_logs));

    let api = Router::new()
        .nest("/api", api_routes)
        .layer(axum::middleware::from_fn_with_state(
            app_state.clone(),
            auth::require_api_key,
        ))
        .layer(CorsLayer::new().allow_origin(Any).allow_headers(Any).allow_methods(Any))
        .layer(TraceLayer::new_for_http())
        .with_state(app_state.clone());

    let mut app = Router::new().merge(api);

    // Serve UI if built assets exist.
    let ui_dist = cli
        .ui_dist
        .unwrap_or_else(|| repo_root.join("control-plane/ui/dist"));

    if ui_dist.is_dir() {
        tracing::info!(ui_dist = %ui_dist.display(), "serving UI assets");

        // SPA fallback: serve index.html for non-file routes like /runs/<id> so deep links work.
        let index_html = ui_dist.join("index.html");
        app = app.fallback_service(
            ServeDir::new(ui_dist)
                .append_index_html_on_directories(true)
                .fallback(ServeFile::new(index_html)),
        );
    } else {
        tracing::warn!(ui_dist = %ui_dist.display(), "UI dist dir not found; serving API only");
    }

    tracing::info!(bind = %cli.bind, "control-plane listening");
    let listener = tokio::net::TcpListener::bind(cli.bind).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
