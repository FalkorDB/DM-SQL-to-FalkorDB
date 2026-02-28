use std::collections::HashMap;
use std::path::{Path, PathBuf};

use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::{sqlite::SqliteConnectOptions, SqlitePool};
use uuid::Uuid;

use crate::models::{
    ApiResult, ConfigRecord, ConfigStateInfo, CreateConfigRequest, RunMode, RunRecord, RunStatus,
    UpdateConfigRequest,
};
use crate::models::{bad_request, not_found};
use crate::models::AppState;
use crate::tools::ToolRegistry;

#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
}

impl Store {
    pub async fn connect(db_path: &Path) -> anyhow::Result<Self> {
        let opts = SqliteConnectOptions::new()
            .filename(db_path)
            .create_if_missing(true);

        let pool = SqlitePool::connect_with(opts).await?;

        sqlx::migrate!().run(&pool).await?;

        Ok(Self { pool })
    }

    pub async fn upsert_tools(&self, tools: &ToolRegistry) -> anyhow::Result<()> {
        let now = Utc::now().to_rfc3339();
        for (id, manifest_json) in tools.as_json_by_id() {
            let manifest_json = serde_json::to_string(&manifest_json).unwrap_or_else(|_| "{}".to_string());
            sqlx::query(
                "INSERT INTO tools (id, manifest_json, loaded_at) VALUES (?1, ?2, ?3)\
                 ON CONFLICT(id) DO UPDATE SET manifest_json=excluded.manifest_json, loaded_at=excluded.loaded_at",
            )
            .bind(id)
            .bind(manifest_json)
            .bind(&now)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    pub async fn list_configs(&self, tool_id: Option<&str>) -> anyhow::Result<Vec<ConfigRecord>> {
        let rows = if let Some(tool_id) = tool_id {
            sqlx::query_as::<_, ConfigRow>(
                "SELECT id, tool_id, name, content, created_at, updated_at FROM configs WHERE tool_id = ?1 ORDER BY updated_at DESC",
            )
            .bind(tool_id)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, ConfigRow>(
                "SELECT id, tool_id, name, content, created_at, updated_at FROM configs ORDER BY updated_at DESC",
            )
            .fetch_all(&self.pool)
            .await?
        };

        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    pub async fn get_config(&self, config_id: Uuid) -> anyhow::Result<Option<ConfigRecord>> {
        let row = sqlx::query_as::<_, ConfigRow>(
            "SELECT id, tool_id, name, content, created_at, updated_at FROM configs WHERE id = ?1",
        )
        .bind(config_id.to_string())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Into::into))
    }

    pub async fn create_config(&self, req: CreateConfigRequest) -> anyhow::Result<ConfigRecord> {
        let id = Uuid::new_v4();
        let now = Utc::now();

        sqlx::query(
            "INSERT INTO configs (id, tool_id, name, content, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(id.to_string())
        .bind(&req.tool_id)
        .bind(&req.name)
        .bind(&req.content)
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .execute(&self.pool)
        .await?;

        Ok(ConfigRecord {
            id,
            tool_id: req.tool_id,
            name: req.name,
            content: req.content,
            created_at: now,
            updated_at: now,
        })
    }

    pub async fn update_config(
        &self,
        config_id: Uuid,
        req: UpdateConfigRequest,
    ) -> anyhow::Result<Option<ConfigRecord>> {
        let existing = self.get_config(config_id).await?;
        let Some(existing) = existing else {
            return Ok(None);
        };

        let now = Utc::now();

        sqlx::query(
            "UPDATE configs SET name=?1, content=?2, updated_at=?3 WHERE id=?4",
        )
        .bind(&req.name)
        .bind(&req.content)
        .bind(now.to_rfc3339())
        .bind(config_id.to_string())
        .execute(&self.pool)
        .await?;

        Ok(Some(ConfigRecord {
            id: existing.id,
            tool_id: existing.tool_id,
            name: req.name,
            content: req.content,
            created_at: existing.created_at,
            updated_at: now,
        }))
    }

    pub async fn insert_run(&self, run: &RunRecord) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO runs (id, tool_id, config_id, mode, status, started_at, ended_at, exit_code, error) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )
        .bind(run.id.to_string())
        .bind(&run.tool_id)
        .bind(run.config_id.to_string())
        .bind(run_mode_to_str(&run.mode))
        .bind(run_status_to_str(&run.status))
        .bind(run.started_at.to_rfc3339())
        .bind(run.ended_at.map(|t| t.to_rfc3339()))
        .bind(run.exit_code)
        .bind(run.error.clone())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn update_run_completion(
        &self,
        run_id: Uuid,
        status: RunStatus,
        ended_at: DateTime<Utc>,
        exit_code: Option<i64>,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE runs SET status=?1, ended_at=?2, exit_code=?3, error=?4 WHERE id=?5",
        )
        .bind(run_status_to_str(&status))
        .bind(ended_at.to_rfc3339())
        .bind(exit_code)
        .bind(error)
        .bind(run_id.to_string())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn list_runs(&self, tool_id: Option<&str>) -> anyhow::Result<Vec<RunRecord>> {
        let rows = if let Some(tool_id) = tool_id {
            sqlx::query_as::<_, RunRow>(
                "SELECT id, tool_id, config_id, mode, status, started_at, ended_at, exit_code, error FROM runs WHERE tool_id=?1 ORDER BY started_at DESC",
            )
            .bind(tool_id)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, RunRow>(
                "SELECT id, tool_id, config_id, mode, status, started_at, ended_at, exit_code, error FROM runs ORDER BY started_at DESC",
            )
            .fetch_all(&self.pool)
            .await?
        };

        Ok(rows.into_iter().filter_map(|r| r.try_into().ok()).collect())
    }

    pub async fn get_run(&self, run_id: Uuid) -> anyhow::Result<Option<RunRecord>> {
        let row = sqlx::query_as::<_, RunRow>(
            "SELECT id, tool_id, config_id, mode, status, started_at, ended_at, exit_code, error FROM runs WHERE id=?1",
        )
        .bind(run_id.to_string())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.and_then(|r| r.try_into().ok()))
    }
}

#[derive(sqlx::FromRow)]
struct ConfigRow {
    id: String,
    tool_id: String,
    name: String,
    content: String,
    created_at: String,
    updated_at: String,
}

impl From<ConfigRow> for ConfigRecord {
    fn from(r: ConfigRow) -> Self {
        Self {
            id: Uuid::parse_str(&r.id).unwrap(),
            tool_id: r.tool_id,
            name: r.name,
            content: r.content,
            created_at: DateTime::parse_from_rfc3339(&r.created_at)
                .unwrap()
                .with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339(&r.updated_at)
                .unwrap()
                .with_timezone(&Utc),
        }
    }
}

#[derive(sqlx::FromRow)]
struct RunRow {
    id: String,
    tool_id: String,
    config_id: String,
    mode: String,
    status: String,
    started_at: String,
    ended_at: Option<String>,
    exit_code: Option<i64>,
    error: Option<String>,
}

impl TryFrom<RunRow> for RunRecord {
    type Error = anyhow::Error;

    fn try_from(r: RunRow) -> Result<Self, Self::Error> {
        let mode: RunMode = run_mode_from_str(&r.mode);
        let status: RunStatus = run_status_from_str(&r.status);

        Ok(Self {
            id: Uuid::parse_str(&r.id)?,
            tool_id: r.tool_id,
            config_id: Uuid::parse_str(&r.config_id)?,
            mode,
            status,
            started_at: DateTime::parse_from_rfc3339(&r.started_at)?.with_timezone(&Utc),
            ended_at: r
                .ended_at
                .map(|s| DateTime::parse_from_rfc3339(&s).map(|d| d.with_timezone(&Utc)))
                .transpose()?,
            exit_code: r.exit_code,
            error: r.error,
        })
    }
}

fn run_mode_to_str(m: &RunMode) -> &'static str {
    match m {
        RunMode::OneShot => "one_shot",
        RunMode::Daemon => "daemon",
    }
}

fn run_status_to_str(s: &RunStatus) -> &'static str {
    match s {
        RunStatus::Queued => "queued",
        RunStatus::Running => "running",
        RunStatus::Succeeded => "succeeded",
        RunStatus::Failed => "failed",
        RunStatus::Stopped => "stopped",
    }
}

fn run_mode_from_str(s: &str) -> RunMode {
    match s {
        "daemon" => RunMode::Daemon,
        _ => RunMode::OneShot,
    }
}

fn run_status_from_str(s: &str) -> RunStatus {
    match s {
        "queued" => RunStatus::Queued,
        "running" => RunStatus::Running,
        "succeeded" => RunStatus::Succeeded,
        "stopped" => RunStatus::Stopped,
        _ => RunStatus::Failed,
    }
}

#[derive(Deserialize)]
pub struct ConfigListQuery {
    pub tool_id: Option<String>,
}

pub async fn list_configs(
    State(state): State<AppState>,
    Query(q): Query<ConfigListQuery>,
) -> ApiResult<Json<Vec<ConfigRecord>>> {
    let cfgs = state
        .store
        .list_configs(q.tool_id.as_deref())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(cfgs))
}

pub async fn create_config(
    State(state): State<AppState>,
    Json(req): Json<CreateConfigRequest>,
) -> ApiResult<Json<ConfigRecord>> {
    if state.tools.get(&req.tool_id).is_none() {
        return bad_request("unknown tool_id".to_string());
    }

    let rec = state
        .store
        .create_config(req)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(rec))
}

pub async fn get_config(
    State(state): State<AppState>,
    AxumPath(config_id): AxumPath<Uuid>,
) -> ApiResult<Json<ConfigRecord>> {
    let Some(cfg) = state
        .store
        .get_config(config_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    else {
        return not_found();
    };

    Ok(Json(cfg))
}

pub async fn update_config(
    State(state): State<AppState>,
    AxumPath(config_id): AxumPath<Uuid>,
    Json(req): Json<UpdateConfigRequest>,
) -> ApiResult<Json<ConfigRecord>> {
    let Some(cfg) = state
        .store
        .update_config(config_id, req)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    else {
        return not_found();
    };

    Ok(Json(cfg))
}

#[derive(Deserialize)]
struct StateFile {
    mappings: HashMap<String, String>,
}

pub async fn get_config_state(
    State(state): State<AppState>,
    AxumPath(config_id): AxumPath<Uuid>,
) -> ApiResult<Json<ConfigStateInfo>> {
    let Some(cfg) = state
        .store
        .get_config(config_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    else {
        return not_found();
    };

    let Some(tool) = state.tools.get(&cfg.tool_id) else {
        return bad_request("unknown tool_id".to_string());
    };

    let mut out = ConfigStateInfo {
        backend: None,
        file_path: None,
        resolved_path: None,
        exists: false,
        last_watermark: None,
        watermarks: None,
        warning: None,
    };

    let cfg_value: serde_json::Value = match serde_json::from_str(&cfg.content) {
        Ok(v) => v,
        Err(_) => serde_yaml::from_str(&cfg.content).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("config is not valid JSON or YAML: {e}"),
            )
        })?,
    };

    let state_section = cfg_value.get("state");
    let backend = state_section
        .and_then(|s| s.get("backend"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    out.backend = backend.clone();

    // Only file-backed state is currently introspected.
    if backend.as_deref() != Some("file") {
        return Ok(Json(out));
    }

    let file_path = state_section
        .and_then(|s| s.get("file_path"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "state.json".to_string());

    out.file_path = Some(file_path.clone());

    let cwd = tool.working_dir_abs(&state.repo_root);
    let resolved = resolve_path(&cwd, &file_path);
    out.resolved_path = Some(resolved.to_string_lossy().to_string());

    let meta = tokio::fs::metadata(&resolved).await;
    if meta.is_err() {
        out.exists = false;
        return Ok(Json(out));
    }

    out.exists = true;

    // Safety: Only read state files within repo_root or data_dir.
    let resolved_canon = tokio::fs::canonicalize(&resolved).await.ok();
    let repo_root_canon = tokio::fs::canonicalize(&state.repo_root)
        .await
        .unwrap_or_else(|_| state.repo_root.clone());
    let data_dir_canon = tokio::fs::canonicalize(&state.data_dir)
        .await
        .unwrap_or_else(|_| state.data_dir.clone());

    if let Some(canon) = &resolved_canon {
        if !canon.starts_with(&repo_root_canon) && !canon.starts_with(&data_dir_canon) {
            out.warning = Some(
                "state file is outside control-plane repo_root/data_dir; skipping watermark read"
                    .to_string(),
            );
            return Ok(Json(out));
        }
    }

    let raw = tokio::fs::read_to_string(&resolved)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let state_file: StateFile = serde_json::from_str(&raw)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("invalid state json: {e}")))?;

    if state_file.mappings.is_empty() {
        out.watermarks = Some(HashMap::new());
        return Ok(Json(out));
    }

    // Compute max watermark.
    let mut max_dt: Option<chrono::DateTime<Utc>> = None;
    for (_, ts) in &state_file.mappings {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts) {
            let dt = dt.with_timezone(&Utc);
            max_dt = Some(max_dt.map(|m| m.max(dt)).unwrap_or(dt));
        }
    }

    out.last_watermark = max_dt.map(|d| d.to_rfc3339());
    out.watermarks = Some(state_file.mappings);

    Ok(Json(out))
}

pub async fn clear_config_state(
    State(state): State<AppState>,
    AxumPath(config_id): AxumPath<Uuid>,
) -> ApiResult<Json<serde_json::Value>> {
    let Some(cfg) = state
        .store
        .get_config(config_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    else {
        return not_found();
    };

    let Some(tool) = state.tools.get(&cfg.tool_id) else {
        return bad_request("unknown tool_id".to_string());
    };

    let cfg_value: serde_json::Value = match serde_json::from_str(&cfg.content) {
        Ok(v) => v,
        Err(_) => serde_yaml::from_str(&cfg.content).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("config is not valid JSON or YAML: {e}"),
            )
        })?,
    };

    let state_section = cfg_value.get("state");
    let backend = state_section
        .and_then(|s| s.get("backend"))
        .and_then(|v| v.as_str());

    if backend != Some("file") {
        return bad_request("config state backend is not 'file'".to_string());
    }

    let file_path = state_section
        .and_then(|s| s.get("file_path"))
        .and_then(|v| v.as_str())
        .unwrap_or("state.json");

    let cwd = tool.working_dir_abs(&state.repo_root);
    let resolved = resolve_path(&cwd, file_path);

    // Safety: Only delete state files within repo_root or data_dir.
    let resolved_canon = tokio::fs::canonicalize(&resolved).await.ok();
    let repo_root_canon = tokio::fs::canonicalize(&state.repo_root)
        .await
        .unwrap_or_else(|_| state.repo_root.clone());
    let data_dir_canon = tokio::fs::canonicalize(&state.data_dir)
        .await
        .unwrap_or_else(|_| state.data_dir.clone());

    if let Some(canon) = &resolved_canon {
        if !canon.starts_with(&repo_root_canon) && !canon.starts_with(&data_dir_canon) {
            return bad_request(
                "refusing to delete state file outside control-plane repo_root/data_dir".to_string(),
            );
        }
    }

    let deleted = match tokio::fs::remove_file(&resolved).await {
        Ok(()) => true,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to delete state file: {e}"),
            ))
        }
    };

    Ok(Json(serde_json::json!({
        "ok": true,
        "deleted": deleted,
        "resolved_path": resolved.to_string_lossy().to_string()
    })))
}

fn resolve_path(base: &Path, raw: &str) -> PathBuf {
    let p = Path::new(raw);
    if p.is_absolute() {
        return p.to_path_buf();
    }
    base.join(p)
}
