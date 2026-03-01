use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, Sse};
use axum::Json;
use chrono::Utc;
use futures::{Stream, StreamExt};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

use crate::models::{
    ApiResult, RunEvent, RunMode, RunRecord, RunStatus, StartRunRequest,
};
use crate::models::{bad_request, not_found};
use crate::models::AppState;

#[derive(Clone)]
pub struct RunManager {
    runs_dir: PathBuf,
    inner: Arc<Mutex<HashMap<Uuid, RunHandle>>>,
}

struct RunHandle {
    tool_id: String,
    config_id: Uuid,
    mode: RunMode,
    status: RunStatus,
    tx: broadcast::Sender<RunEvent>,
    child: Option<tokio::process::Child>,
    log_path: PathBuf,
}

impl RunManager {
    pub fn new(runs_dir: PathBuf) -> Self {
        Self {
            runs_dir,
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn subscribe(&self, run_id: Uuid) -> Option<broadcast::Receiver<RunEvent>> {
        let guard = self.inner.lock().await;
        guard.get(&run_id).map(|h| h.tx.subscribe())
    }

    pub async fn stop(&self, run_id: Uuid) -> anyhow::Result<()> {
        let mut guard = self.inner.lock().await;
        let Some(handle) = guard.get_mut(&run_id) else {
            anyhow::bail!("unknown run");
        };

        if handle.status != RunStatus::Running {
            return Ok(());
        }

        if let Some(child) = handle.child.as_mut() {
            // Best-effort graceful kill.
            let _ = child.start_kill();
        }

        handle.status = RunStatus::Stopped;
        let _ = handle.tx.send(RunEvent::State {
            status: RunStatus::Stopped,
        });

        Ok(())
    }

    async fn create_run_dir(&self, run_id: Uuid) -> anyhow::Result<PathBuf> {
        let dir = self.runs_dir.join(run_id.to_string());
        tokio::fs::create_dir_all(&dir).await?;
        Ok(dir)
    }
}

#[derive(Deserialize)]
pub struct RunListQuery {
    pub tool_id: Option<String>,
}

#[derive(Deserialize)]
pub struct RunLogsQuery {
    /// Return at most the last N log lines. Defaults to 2000.
    pub limit: Option<usize>,
}

pub async fn list_runs(
    State(state): State<AppState>,
    Query(q): Query<RunListQuery>,
) -> ApiResult<Json<Vec<RunRecord>>> {
    let runs = state
        .store
        .list_runs(q.tool_id.as_deref())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(runs))
}

pub async fn get_run(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<Uuid>,
) -> ApiResult<Json<RunRecord>> {
    let Some(run) = state
        .store
        .get_run(run_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    else {
        return not_found();
    };

    Ok(Json(run))
}

pub async fn start_run(
    State(state): State<AppState>,
    Json(req): Json<StartRunRequest>,
) -> ApiResult<Json<RunRecord>> {
    let Some(tool) = state.tools.get(&req.tool_id) else {
        return bad_request("unknown tool_id".to_string());
    };

    let Some(config) = state
        .store
        .get_config(req.config_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    else {
        return bad_request("unknown config_id".to_string());
    };

    let purge_graph = req.purge_graph.unwrap_or(false);
    let purge_mappings = req.purge_mappings.clone().unwrap_or_default();
    tool.validate_run_request(&req.mode, purge_graph, &purge_mappings)?;

    let run_id = Uuid::new_v4();
    let started_at = Utc::now();

    let run = RunRecord {
        id: run_id,
        tool_id: req.tool_id.clone(),
        config_id: req.config_id,
        mode: req.mode.clone(),
        status: RunStatus::Running,
        started_at,
        ended_at: None,
        exit_code: None,
        error: None,
    };

    state
        .store
        .insert_run(&run)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Materialize config to run directory.
    let run_dir = state
        .runs
        .create_run_dir(run_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let config_ext = if serde_json::from_str::<serde_json::Value>(&config.content).is_ok() {
        "json"
    } else {
        "yaml"
    };

    let config_path = run_dir.join(format!("config.{config_ext}"));
    tokio::fs::write(&config_path, &config.content).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed writing run config: {e}"),
        )
    })?;

    let log_path = run_dir.join("run.log");

    // Spawn the tool.
    let (program, mut tool_args, cwd) = tool
        .executable_command(&state.repo_root)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Tool-specific CLI args (capability-driven, but flag names are currently per tool).
    // For now we assume:
    // - all tools: --config <path>
    // - snowflake: --purge-graph, --purge-mapping, --daemon, --interval-secs
    // - postgres:  --daemon, --interval-secs
    tool_args.push("--".to_string());
    tool_args.push("--config".to_string());
    tool_args.push(config_path.to_string_lossy().to_string());

    if purge_graph {
        tool_args.push("--purge-graph".to_string());
    }
    for m in &purge_mappings {
        tool_args.push("--purge-mapping".to_string());
        tool_args.push(m.clone());
    }

    if matches!(req.mode, RunMode::Daemon) {
        tool_args.push("--daemon".to_string());
        let interval = req.daemon_interval_secs.unwrap_or(60);
        tool_args.push("--interval-secs".to_string());
        tool_args.push(interval.to_string());
    }

    let (tx, _rx) = broadcast::channel::<RunEvent>(1024);

    {
        let mut guard = state.runs.inner.lock().await;
        guard.insert(
            run_id,
            RunHandle {
                tool_id: req.tool_id.clone(),
                config_id: req.config_id,
                mode: req.mode.clone(),
                status: RunStatus::Running,
                tx: tx.clone(),
                child: None,
                log_path: log_path.clone(),
            },
        );
    }

    let _ = tx.send(RunEvent::State {
        status: RunStatus::Running,
    });

    let mut cmd = Command::new(program);
    cmd.args(&tool_args).current_dir(cwd);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = cmd
        .spawn()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("spawn failed: {e}")))?;

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    {
        let mut guard = state.runs.inner.lock().await;
        if let Some(h) = guard.get_mut(&run_id) {
            h.child = Some(child);
        }
    }

    // Start log tasks.
    if let Some(stdout) = stdout {
        spawn_log_reader(run_id, "stdout", stdout, tx.clone(), log_path.clone());
    }
    if let Some(stderr) = stderr {
        spawn_log_reader(run_id, "stderr", stderr, tx.clone(), log_path.clone());
    }

    // Completion task: monitor the process with try_wait so we don't hold mutex guards across .await.
    let runs = state.runs.clone();
    let store = state.store.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            // If we can't find the run, stop monitoring.
            let maybe_status = {
                let mut guard = runs.inner.lock().await;
                let Some(h) = guard.get_mut(&run_id) else {
                    return;
                };

                if let Some(child) = h.child.as_mut() {
                    match child.try_wait() {
                        Ok(Some(status)) => Some(Ok(status)),
                        Ok(None) => None,
                        Err(e) => Some(Err(e)),
                    }
                } else {
                    return;
                }
            };

            let Some(result) = maybe_status else {
                continue;
            };

            let ended_at = Utc::now();

            let (final_status, exit_code, error) = match result {
                Ok(status) => {
                    let code = status.code().map(|c| c as i64);
                    if status.success() {
                        (RunStatus::Succeeded, code, None)
                    } else {
                        (
                            RunStatus::Failed,
                            code,
                            Some(format!("process exited with status: {status}")),
                        )
                    }
                }
                Err(e) => (RunStatus::Failed, None, Some(e.to_string())),
            };

            // If the run was explicitly stopped, keep Stopped as terminal state.
            let (status_to_send, err_to_send, tx) = {
                let mut guard = runs.inner.lock().await;
                let Some(h) = guard.get_mut(&run_id) else {
                    return;
                };

                let (status_to_send, err_to_send) = if h.status == RunStatus::Stopped {
                    (RunStatus::Stopped, None)
                } else {
                    h.status = final_status.clone();
                    (final_status, error.clone())
                };

                (status_to_send, err_to_send, h.tx.clone())
            };

            // Send exit event.

            let _ = tx.send(RunEvent::Exit {
                status: status_to_send.clone(),
                exit_code,
                error: err_to_send.clone(),
            });

            let _ = store
                .update_run_completion(run_id, status_to_send, ended_at, exit_code, err_to_send)
                .await;

            return;
        }
    });

    Ok(Json(run))
}

fn spawn_log_reader<R>(
    run_id: Uuid,
    stream_name: &'static str,
    reader: R,
    tx: broadcast::Sender<RunEvent>,
    log_path: PathBuf,
) where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let evt = RunEvent::Log {
                stream: stream_name.to_string(),
                line: line.clone(),
            };
            let _ = tx.send(evt);

            // Best-effort append to file.
            let _ = append_log_line(&log_path, stream_name, &line).await;
        }

        tracing::info!(run_id = %run_id, stream = %stream_name, "log stream ended");
    });
}

async fn append_log_line(path: &Path, stream: &str, line: &str) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut f = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;

    f.write_all(format!("[{stream}] {line}\n").as_bytes()).await?;
    Ok(())
}

pub async fn stop_run(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<Uuid>,
) -> ApiResult<Json<serde_json::Value>> {
    state
        .runs
        .stop(run_id)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;

    // Persist stop.
    let _ = state
        .store
        .update_run_completion(run_id, RunStatus::Stopped, Utc::now(), None, None)
        .await;

    Ok(Json(serde_json::json!({"ok": true})))
}

pub async fn run_logs(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<Uuid>,
    Query(q): Query<RunLogsQuery>,
) -> ApiResult<Json<Vec<RunEvent>>> {
    // Default to something sane and cap to avoid huge payloads.
    let limit = q.limit.unwrap_or(2000).clamp(1, 10_000);

    // Log file path is stable across process restarts.
    let log_path = state
        .data_dir
        .join("runs")
        .join(run_id.to_string())
        .join("run.log");

    let f = match tokio::fs::File::open(&log_path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Json(vec![]));
        }
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed reading logs: {e}"),
            ));
        }
    };

    let mut buf: VecDeque<RunEvent> = VecDeque::with_capacity(limit.min(2048));
    let mut lines = BufReader::new(f).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let (stream, msg) = parse_persisted_log_line(&line);
        let evt = RunEvent::Log {
            stream: stream.to_string(),
            line: msg.to_string(),
        };

        if buf.len() == limit {
            buf.pop_front();
        }
        buf.push_back(evt);
    }

    Ok(Json(buf.into_iter().collect()))
}

fn parse_persisted_log_line(line: &str) -> (&str, &str) {
    // Persisted format from append_log_line(): "[stdout] hello".
    if let Some(rest) = line.strip_prefix('[') {
        if let Some((stream, msg)) = rest.split_once("] ") {
            return (stream, msg);
        }
    }
    ("unknown", line)
}

pub async fn run_events_sse(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<Uuid>,
) -> ApiResult<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>> {
    let rx = state
        .runs
        .subscribe(run_id)
        .await
        .ok_or_else(|| (StatusCode::NOT_FOUND, "unknown run".to_string()))?;

    let stream = BroadcastStream::new(rx).filter_map(|msg| async move {
        match msg {
            Ok(evt) => {
                let json = serde_json::to_string(&evt).unwrap_or_else(|_| "{}".to_string());
                Some(Ok(Event::default().event("message").data(json)))
            }
            Err(_) => None,
        }
    });

    Ok(Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default()))
}
