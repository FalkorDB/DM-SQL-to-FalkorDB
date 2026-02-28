use std::collections::HashMap;
use std::path::PathBuf;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::runs::RunManager;
use crate::store::Store;
use crate::tools::ToolRegistry;

#[derive(Clone)]
pub struct AppState {
    pub repo_root: PathBuf,
    pub data_dir: PathBuf,
    pub tools: ToolRegistry,
    pub store: Store,
    pub runs: RunManager,
    pub api_key: Option<String>,
}

#[derive(Serialize)]
pub struct Health {
    pub ok: bool,
}

pub async fn health() -> Json<Health> {
    Json(Health { ok: true })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunMode {
    OneShot,
    Daemon,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Stopped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSummary {
    pub id: String,
    pub display_name: String,
    pub description: Option<String>,
    pub capabilities: ToolCapabilities,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ToolCapabilities {
    pub supports_daemon: bool,
    pub supports_purge_graph: bool,
    pub supports_purge_mapping: bool,
    pub supports_metrics: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigRecord {
    pub id: Uuid,
    pub tool_id: String,
    pub name: String,
    pub content: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub id: Uuid,
    pub tool_id: String,
    pub config_id: Uuid,
    pub mode: RunMode,
    pub status: RunStatus,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub exit_code: Option<i64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateConfigRequest {
    pub tool_id: String,
    pub name: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateConfigRequest {
    pub name: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigStateInfo {
    pub backend: Option<String>,
    pub file_path: Option<String>,
    pub resolved_path: Option<String>,
    pub exists: bool,
    pub last_watermark: Option<String>,
    pub watermarks: Option<HashMap<String, String>>,
    pub warning: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartRunRequest {
    pub tool_id: String,
    pub config_id: Uuid,
    pub mode: RunMode,
    pub daemon_interval_secs: Option<u64>,
    pub purge_graph: Option<bool>,
    pub purge_mappings: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunEvent {
    State { status: RunStatus },
    Log { stream: String, line: String },
    Exit { status: RunStatus, exit_code: Option<i64>, error: Option<String> },
}

pub fn not_found<T>() -> Result<T, (StatusCode, String)> {
    Err((StatusCode::NOT_FOUND, "not found".to_string()))
}

pub fn bad_request<T>(msg: impl Into<String>) -> Result<T, (StatusCode, String)> {
    Err((StatusCode::BAD_REQUEST, msg.into()))
}

pub fn internal_error<T>(msg: impl Into<String>) -> Result<T, (StatusCode, String)> {
    Err((StatusCode::INTERNAL_SERVER_ERROR, msg.into()))
}

pub type ApiResult<T> = Result<T, (StatusCode, String)>;

pub fn ok_json<T: Serialize>(val: T) -> Json<T> {
    Json(val)
}

pub fn state<S>(State(s): State<S>) -> S {
    s
}
