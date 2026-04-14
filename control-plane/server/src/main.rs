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

    /// Execution backend for tool runs: local | kubernetes.
    #[arg(long, env = "CONTROL_PLANE_EXECUTION_BACKEND", default_value = "local")]
    execution_backend: String,

    /// Namespace used by the kubernetes execution backend.
    #[arg(long, env = "CONTROL_PLANE_K8S_NAMESPACE", default_value = "default")]
    k8s_namespace: String,

    /// Multi-tool runner image used by the kubernetes execution backend.
    #[arg(
        long,
        env = "CONTROL_PLANE_K8S_RUNNER_IMAGE",
        default_value = "ghcr.io/falkordb/dm-sql-to-falkordb-runner:latest"
    )]
    k8s_runner_image: String,

    /// Image pull policy used by the kubernetes execution backend.
    #[arg(
        long,
        env = "CONTROL_PLANE_K8S_IMAGE_PULL_POLICY",
        default_value = "IfNotPresent"
    )]
    k8s_image_pull_policy: String,

    /// Optional service account used for runner workloads.
    #[arg(long, env = "CONTROL_PLANE_K8S_SERVICE_ACCOUNT")]
    k8s_service_account: Option<String>,

    /// Optional shared PVC name mounted into runner workloads for persistent state.
    #[arg(long, env = "CONTROL_PLANE_K8S_SHARED_PVC")]
    k8s_shared_pvc: Option<String>,

    /// Optional Secret name to project tool environment variables into runner pods.
    #[arg(long, env = "CONTROL_PLANE_K8S_ENV_SECRET")]
    k8s_env_secret: Option<String>,

    /// Optional ConfigMap name to project tool environment variables into runner pods.
    #[arg(long, env = "CONTROL_PLANE_K8S_ENV_CONFIGMAP")]
    k8s_env_configmap: Option<String>,

    /// kubectl binary path used for kubernetes operations.
    #[arg(long, env = "CONTROL_PLANE_K8S_KUBECTL_BIN", default_value = "kubectl")]
    k8s_kubectl_bin: String,

    /// Directory inside runner images where migration binaries are located.
    #[arg(
        long,
        env = "CONTROL_PLANE_K8S_BINARY_DIR",
        default_value = "/opt/falkordb/bin"
    )]
    k8s_binary_dir: String,
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

    let execution_backend = models::ExecutionBackend::parse(&cli.execution_backend)
        .context("failed to parse CONTROL_PLANE_EXECUTION_BACKEND")?;

    let app_state = models::AppState {
        repo_root: repo_root.clone(),
        data_dir,
        tools,
        store,
        runs,
        execution: models::ExecutionConfig {
            backend: execution_backend,
            kubernetes: models::KubernetesExecutionConfig {
                namespace: cli.k8s_namespace,
                runner_image: cli.k8s_runner_image,
                image_pull_policy: cli.k8s_image_pull_policy,
                service_account: cli.k8s_service_account,
                shared_pvc_name: cli.k8s_shared_pvc,
                env_secret_name: cli.k8s_env_secret,
                env_configmap_name: cli.k8s_env_configmap,
                kubectl_bin: cli.k8s_kubectl_bin,
                binary_dir: cli.k8s_binary_dir,
            },
        },
        api_key: std::env::var("CONTROL_PLANE_API_KEY")
            .ok()
            .filter(|v| !v.is_empty()),
    };

    let api_routes = Router::new()
        .route("/health", get(models::health))
        .route("/tools", get(tools::list_tools))
        .route("/tools/:tool_id", get(tools::get_tool))
        .route(
            "/tools/:tool_id/scaffold-template",
            post(scaffold::generate_scaffold_template),
        )
        .route(
            "/tools/:tool_id/schema-graph-preview",
            post(scaffold::generate_schema_graph_preview),
        )
        .route("/metrics", get(metrics::list_tool_metrics))
        .route("/metrics/:tool_id", get(metrics::get_tool_metrics))
        .route(
            "/configs",
            get(store::list_configs).post(store::create_config),
        )
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
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_headers(Any)
                .allow_methods(Any),
        )
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
