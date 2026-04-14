use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::sync::Arc;

use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, Sse};
use axum::Json;
use chrono::Utc;
use futures::{Stream, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

use crate::metrics::{collect_tool_metrics, collect_tool_metrics_with_endpoint};
use crate::models::{bad_request, not_found};
use crate::models::{
    ApiResult, AppState, ExecutionBackend, RunBackend, RunEvent, RunExecutionRef, RunMode,
    RunRecord, RunStatus, StartRunRequest,
};
use crate::store::Store;
use crate::tools::Tool;

#[derive(Clone)]
pub struct RunManager {
    runs_dir: PathBuf,
    inner: Arc<Mutex<HashMap<Uuid, RunHandle>>>,
}

struct RunHandle {
    config_id: Uuid,
    status: RunStatus,
    tx: broadcast::Sender<RunEvent>,
    runtime: RunRuntime,
}

enum RunRuntime {
    Local {
        child: Option<tokio::process::Child>,
    },
    Kubernetes(KubernetesRuntime),
}

#[derive(Debug, Clone)]
struct KubernetesRuntime {
    kubectl_bin: String,
    namespace: String,
    workload_kind: String,
    workload_name: String,
    config_map_name: String,
    pod_label_selector: String,
    image: String,
    image_pull_policy: String,
    service_account: Option<String>,
    shared_pvc_name: Option<String>,
    env_secret_name: Option<String>,
    env_configmap_name: Option<String>,
    binary_path: String,
}

#[derive(Debug, Clone)]
enum MetricsCollectionTarget {
    Local,
    Kubernetes(KubernetesRuntime),
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
        let (k8s_runtime, tx) = {
            let mut k8s_runtime: Option<KubernetesRuntime> = None;
            let mut guard = self.inner.lock().await;
            let Some(handle) = guard.get_mut(&run_id) else {
                anyhow::bail!("unknown run");
            };

            if handle.status != RunStatus::Running {
                return Ok(());
            }

            match &mut handle.runtime {
                RunRuntime::Local { child } => {
                    if let Some(child) = child.as_mut() {
                        let _ = child.start_kill();
                    }
                }
                RunRuntime::Kubernetes(runtime) => {
                    k8s_runtime = Some(runtime.clone());
                }
            }

            handle.status = RunStatus::Stopped;
            (k8s_runtime, handle.tx.clone())
        };

        if let Some(runtime) = k8s_runtime {
            cleanup_kubernetes_runtime(&runtime).await?;
        }
        let _ = tx.send(RunEvent::State {
            status: RunStatus::Stopped,
        });

        Ok(())
    }

    async fn create_run_dir(&self, run_id: Uuid) -> anyhow::Result<PathBuf> {
        let dir = self.runs_dir.join(run_id.to_string());
        tokio::fs::create_dir_all(&dir).await?;
        Ok(dir)
    }

    async fn is_running(&self, run_id: Uuid) -> bool {
        let guard = self.inner.lock().await;
        matches!(
            guard.get(&run_id).map(|h| &h.status),
            Some(RunStatus::Running)
        )
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

    let run_config_content = match &state.execution.backend {
        ExecutionBackend::Local => config.content.clone(),
        ExecutionBackend::Kubernetes => maybe_rewrite_state_file_path_for_kubernetes(
            &config.content,
            req.config_id,
            state.execution.kubernetes.shared_pvc_name.is_some(),
            config_ext,
        )
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?,
    };

    let config_path = run_dir.join(format!("config.{config_ext}"));
    tokio::fs::write(&config_path, &run_config_content)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed writing run config: {e}"),
            )
        })?;

    let mut kubernetes_runtime_for_start: Option<KubernetesRuntime> = None;
    let (run_backend, run_runtime, execution_ref, runtime_config_path) =
        match &state.execution.backend {
            ExecutionBackend::Local => (
                RunBackend::Local,
                RunRuntime::Local { child: None },
                None,
                config_path.to_string_lossy().to_string(),
            ),
            ExecutionBackend::Kubernetes => {
                let runtime = build_kubernetes_runtime(&state, run_id, &req, tool)
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                let execution_ref = Some(run_execution_ref_from_kubernetes_runtime(&runtime));
                kubernetes_runtime_for_start = Some(runtime.clone());
                (
                    RunBackend::Kubernetes,
                    RunRuntime::Kubernetes(runtime),
                    execution_ref,
                    format!("/run-config/config.{config_ext}"),
                )
            }
        };

    let tool_args = build_tool_cli_args(
        tool,
        &req,
        purge_graph,
        &purge_mappings,
        &runtime_config_path,
    );
    let log_path = run_dir.join("run.log");

    let run = RunRecord {
        id: run_id,
        tool_id: req.tool_id.clone(),
        config_id: req.config_id,
        mode: req.mode.clone(),
        status: RunStatus::Running,
        backend: run_backend,
        execution_ref,
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

    let (tx, _rx) = broadcast::channel::<RunEvent>(1024);

    {
        let mut guard = state.runs.inner.lock().await;
        guard.insert(
            run_id,
            RunHandle {
                config_id: req.config_id,
                status: RunStatus::Running,
                tx: tx.clone(),
                runtime: run_runtime,
            },
        );
    }

    let _ = tx.send(RunEvent::State {
        status: RunStatus::Running,
    });

    let start_result = match &state.execution.backend {
        ExecutionBackend::Local => {
            start_local_backend(
                &state,
                run_id,
                tool,
                tool_args,
                tx.clone(),
                log_path.clone(),
            )
            .await
        }
        ExecutionBackend::Kubernetes => {
            let Some(runtime) = kubernetes_runtime_for_start else {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "missing kubernetes runtime state".to_string(),
                ));
            };
            start_kubernetes_backend(
                &state,
                run_id,
                &req,
                tool,
                &runtime,
                &run_config_content,
                config_ext,
                tool_args,
                tx.clone(),
                log_path.clone(),
            )
            .await
        }
    };

    if let Err(err) = start_result {
        mark_run_start_failed(&state, run_id, err.clone()).await;
        return Err((StatusCode::INTERNAL_SERVER_ERROR, err));
    }

    Ok(Json(run))
}

async fn start_local_backend(
    state: &AppState,
    run_id: Uuid,
    tool: &Tool,
    tool_args: Vec<String>,
    tx: broadcast::Sender<RunEvent>,
    log_path: PathBuf,
) -> Result<(), String> {
    let (program, mut launcher_args, cwd) = tool
        .executable_command(&state.repo_root)
        .map_err(|e| e.to_string())?;

    launcher_args.push("--".to_string());
    launcher_args.extend(tool_args);

    let mut cmd = Command::new(program);
    cmd.args(&launcher_args).current_dir(cwd);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed spawning local run process: {e}"))?;

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    {
        let mut guard = state.runs.inner.lock().await;
        let Some(handle) = guard.get_mut(&run_id) else {
            return Err("run handle missing after local spawn".to_string());
        };
        match &mut handle.runtime {
            RunRuntime::Local { child: slot } => {
                *slot = Some(child);
            }
            RunRuntime::Kubernetes(_) => {
                return Err("run backend mismatch while setting local child handle".to_string());
            }
        }
    }

    if let Some(stdout) = stdout {
        spawn_log_reader(run_id, "stdout", stdout, tx.clone(), log_path.clone());
    }
    if let Some(stderr) = stderr {
        spawn_log_reader(run_id, "stderr", stderr, tx.clone(), log_path.clone());
    }

    if tool.manifest.capabilities.supports_metrics {
        spawn_metrics_poller(
            run_id,
            {
                let guard = state.runs.inner.lock().await;
                guard
                    .get(&run_id)
                    .map(|h| h.config_id)
                    .unwrap_or(Uuid::nil())
            },
            tool.clone(),
            state.runs.clone(),
            state.store.clone(),
            MetricsCollectionTarget::Local,
        );
    }

    spawn_local_completion_monitor(run_id, state.runs.clone(), state.store.clone());
    Ok(())
}

async fn start_kubernetes_backend(
    state: &AppState,
    run_id: Uuid,
    req: &StartRunRequest,
    tool: &Tool,
    runtime: &KubernetesRuntime,
    config_content: &str,
    config_ext: &str,
    tool_args: Vec<String>,
    tx: broadcast::Sender<RunEvent>,
    log_path: PathBuf,
) -> Result<(), String> {
    let config_manifest = build_kubernetes_config_map_manifest(runtime, config_content, config_ext);
    kubectl_apply_manifest_json(&runtime.kubectl_bin, &config_manifest)
        .await
        .map_err(|e| format!("failed to create kubernetes run config map: {e}"))?;

    let workload_manifest =
        build_kubernetes_workload_manifest(runtime, tool_args, req.mode.clone());
    if let Err(e) = kubectl_apply_manifest_json(&runtime.kubectl_bin, &workload_manifest).await {
        let _ = kubectl_delete_resource(
            &runtime.kubectl_bin,
            &runtime.namespace,
            "configmap",
            &runtime.config_map_name,
        )
        .await;
        return Err(format!("failed to create kubernetes run workload: {e}"));
    }

    spawn_kubernetes_log_reader(
        run_id,
        runtime.clone(),
        tx.clone(),
        log_path.clone(),
        state.runs.clone(),
    );

    if tool.manifest.capabilities.supports_metrics {
        spawn_metrics_poller(
            run_id,
            req.config_id,
            tool.clone(),
            state.runs.clone(),
            state.store.clone(),
            MetricsCollectionTarget::Kubernetes(runtime.clone()),
        );
    }

    if matches!(req.mode, RunMode::OneShot) {
        spawn_kubernetes_job_completion_monitor(
            run_id,
            runtime.clone(),
            state.runs.clone(),
            state.store.clone(),
        );
    }

    Ok(())
}

async fn mark_run_start_failed(state: &AppState, run_id: Uuid, err: String) {
    let tx = {
        let mut guard = state.runs.inner.lock().await;
        guard.remove(&run_id).map(|h| h.tx)
    };

    if let Some(tx) = tx {
        let _ = tx.send(RunEvent::Exit {
            status: RunStatus::Failed,
            exit_code: None,
            error: Some(err.clone()),
        });
    }

    let _ = state
        .store
        .update_run_completion(run_id, RunStatus::Failed, Utc::now(), None, Some(err))
        .await;
}

fn build_tool_cli_args(
    tool: &Tool,
    req: &StartRunRequest,
    purge_graph: bool,
    purge_mappings: &[String],
    config_path: &str,
) -> Vec<String> {
    let mut args = vec!["--config".to_string(), config_path.to_string()];

    if purge_graph {
        args.push("--purge-graph".to_string());
    }

    for mapping in purge_mappings {
        args.push("--purge-mapping".to_string());
        args.push(mapping.clone());
    }

    if matches!(req.mode, RunMode::Daemon) {
        args.push("--daemon".to_string());
        args.push("--interval-secs".to_string());
        args.push(req.daemon_interval_secs.unwrap_or(60).to_string());
    }

    if tool.manifest.capabilities.supports_metrics {
        if let Some(metrics_port) = metrics_port_for_tool(tool) {
            args.push("--metrics-port".to_string());
            args.push(metrics_port.to_string());
        }
    }

    args
}

fn build_kubernetes_runtime(
    state: &AppState,
    run_id: Uuid,
    req: &StartRunRequest,
    tool: &Tool,
) -> anyhow::Result<KubernetesRuntime> {
    let run_token = run_id.simple().to_string();
    let workload_name = format!("dm-run-{run_token}");
    let config_map_name = format!("{workload_name}-cfg");
    let pod_label_selector = format!("dm-falkordb-run-id={run_token}");
    let binary_name = tool.runner_binary_name(&state.repo_root)?;
    let binary_path = format!(
        "{}/{}",
        state.execution.kubernetes.binary_dir.trim_end_matches('/'),
        binary_name
    );

    Ok(KubernetesRuntime {
        kubectl_bin: state.execution.kubernetes.kubectl_bin.clone(),
        namespace: state.execution.kubernetes.namespace.clone(),
        workload_kind: if matches!(req.mode, RunMode::Daemon) {
            "deployment".to_string()
        } else {
            "job".to_string()
        },
        workload_name,
        config_map_name,
        pod_label_selector,
        image: state.execution.kubernetes.runner_image.clone(),
        image_pull_policy: state.execution.kubernetes.image_pull_policy.clone(),
        service_account: state.execution.kubernetes.service_account.clone(),
        shared_pvc_name: state.execution.kubernetes.shared_pvc_name.clone(),
        env_secret_name: state.execution.kubernetes.env_secret_name.clone(),
        env_configmap_name: state.execution.kubernetes.env_configmap_name.clone(),
        binary_path,
    })
}

fn run_execution_ref_from_kubernetes_runtime(runtime: &KubernetesRuntime) -> RunExecutionRef {
    RunExecutionRef {
        kind: Some(runtime.workload_kind.clone()),
        name: Some(runtime.workload_name.clone()),
        namespace: Some(runtime.namespace.clone()),
        pod_label_selector: Some(runtime.pod_label_selector.clone()),
        config_map_name: Some(runtime.config_map_name.clone()),
        image: Some(runtime.image.clone()),
    }
}

fn build_kubernetes_config_map_manifest(
    runtime: &KubernetesRuntime,
    config_content: &str,
    config_ext: &str,
) -> serde_json::Value {
    let data_key = format!("config.{config_ext}");
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": runtime.config_map_name.clone(),
            "namespace": runtime.namespace.clone(),
            "labels": {
                "app.kubernetes.io/name": "falkordb-migrate-runner",
                "dm.falkordb/run-id": runtime.workload_name.trim_start_matches("dm-run-"),
            }
        },
        "data": {
            data_key: config_content
        }
    })
}

fn build_kubernetes_workload_manifest(
    runtime: &KubernetesRuntime,
    tool_args: Vec<String>,
    mode: RunMode,
) -> serde_json::Value {
    let run_id_label = runtime.workload_name.trim_start_matches("dm-run-");
    let labels = json!({
        "app.kubernetes.io/name": "falkordb-migrate-runner",
        "dm.falkordb/run-id": run_id_label,
    });

    let mut volumes = vec![json!({
        "name": "run-config",
        "configMap": {
            "name": runtime.config_map_name
        }
    })];

    let mut volume_mounts = vec![json!({
        "name": "run-config",
        "mountPath": "/run-config",
        "readOnly": true
    })];

    if let Some(shared_pvc_name) = &runtime.shared_pvc_name {
        volumes.push(json!({
            "name": "workspace",
            "persistentVolumeClaim": {
                "claimName": shared_pvc_name
            }
        }));
        volume_mounts.push(json!({
            "name": "workspace",
            "mountPath": "/workspace"
        }));
    }

    let mut env_from = Vec::new();
    if let Some(secret_name) = &runtime.env_secret_name {
        env_from.push(json!({
            "secretRef": {
                "name": secret_name
            }
        }));
    }
    if let Some(configmap_name) = &runtime.env_configmap_name {
        env_from.push(json!({
            "configMapRef": {
                "name": configmap_name
            }
        }));
    }

    let mut pod_spec = json!({
        "restartPolicy": if matches!(mode, RunMode::Daemon) { "Always" } else { "Never" },
        "containers": [{
            "name": "runner",
            "image": runtime.image.clone(),
            "imagePullPolicy": runtime.image_pull_policy.clone(),
            "command": [runtime.binary_path.clone()],
            "args": tool_args,
            "volumeMounts": volume_mounts,
        }],
        "volumes": volumes,
    });

    if let Some(service_account) = &runtime.service_account {
        if let Some(spec) = pod_spec.as_object_mut() {
            spec.insert(
                "serviceAccountName".to_string(),
                serde_json::Value::String(service_account.clone()),
            );
        }
    }

    if !env_from.is_empty() {
        if let Some(containers) = pod_spec
            .get_mut("containers")
            .and_then(|v| v.as_array_mut())
        {
            if let Some(container) = containers.first_mut().and_then(|v| v.as_object_mut()) {
                container.insert("envFrom".to_string(), serde_json::Value::Array(env_from));
            }
        }
    }

    match mode {
        RunMode::OneShot => json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": runtime.workload_name.clone(),
                "namespace": runtime.namespace.clone(),
                "labels": labels.clone(),
            },
            "spec": {
                "backoffLimit": 0,
                "template": {
                    "metadata": {
                        "labels": labels,
                    },
                    "spec": pod_spec,
                }
            }
        }),
        RunMode::Daemon => json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": runtime.workload_name.clone(),
                "namespace": runtime.namespace.clone(),
                "labels": labels.clone(),
            },
            "spec": {
                "replicas": 1,
                "selector": {
                    "matchLabels": labels.clone()
                },
                "template": {
                    "metadata": {
                        "labels": labels,
                    },
                    "spec": pod_spec,
                }
            }
        }),
    }
}

async fn cleanup_kubernetes_runtime(runtime: &KubernetesRuntime) -> anyhow::Result<()> {
    kubectl_delete_resource(
        &runtime.kubectl_bin,
        &runtime.namespace,
        &runtime.workload_kind,
        &runtime.workload_name,
    )
    .await?;
    kubectl_delete_resource(
        &runtime.kubectl_bin,
        &runtime.namespace,
        "configmap",
        &runtime.config_map_name,
    )
    .await?;
    Ok(())
}

async fn kubectl_apply_manifest_json(
    kubectl_bin: &str,
    manifest: &serde_json::Value,
) -> anyhow::Result<()> {
    let args = vec!["apply".to_string(), "-f".to_string(), "-".to_string()];
    let payload = serde_json::to_string_pretty(manifest)?;
    let output = run_kubectl_raw(kubectl_bin, &args, Some(&payload)).await?;
    if !output.status.success() {
        anyhow::bail!(
            "kubectl apply failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

async fn kubectl_delete_resource(
    kubectl_bin: &str,
    namespace: &str,
    kind: &str,
    name: &str,
) -> anyhow::Result<()> {
    let args = vec![
        "-n".to_string(),
        namespace.to_string(),
        "delete".to_string(),
        format!("{kind}/{name}"),
        "--ignore-not-found=true".to_string(),
    ];
    let output = run_kubectl_raw(kubectl_bin, &args, None).await?;
    if !output.status.success() {
        anyhow::bail!(
            "kubectl delete failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

async fn run_kubectl_raw(
    kubectl_bin: &str,
    args: &[String],
    stdin_payload: Option<&str>,
) -> anyhow::Result<Output> {
    let mut cmd = Command::new(kubectl_bin);
    cmd.args(args);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    if stdin_payload.is_some() {
        cmd.stdin(Stdio::piped());
    }

    let mut child = cmd.spawn()?;
    if let Some(payload) = stdin_payload {
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(payload.as_bytes()).await?;
        }
    }

    let output = child.wait_with_output().await?;
    Ok(output)
}

async fn kubectl_get_workload_json(
    runtime: &KubernetesRuntime,
) -> anyhow::Result<Option<serde_json::Value>> {
    let args = vec![
        "-n".to_string(),
        runtime.namespace.clone(),
        "get".to_string(),
        runtime.workload_kind.clone(),
        runtime.workload_name.clone(),
        "-o".to_string(),
        "json".to_string(),
    ];
    let output = run_kubectl_raw(&runtime.kubectl_bin, &args, None).await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        if is_kubectl_not_found(&stderr) {
            return Ok(None);
        }
        anyhow::bail!("kubectl get failed: {}", stderr.trim());
    }

    let value = serde_json::from_slice::<serde_json::Value>(&output.stdout)?;
    Ok(Some(value))
}

async fn kubectl_get_first_pod_ip(runtime: &KubernetesRuntime) -> anyhow::Result<Option<String>> {
    let args = vec![
        "-n".to_string(),
        runtime.namespace.clone(),
        "get".to_string(),
        "pods".to_string(),
        "-l".to_string(),
        runtime.pod_label_selector.clone(),
        "-o".to_string(),
        "jsonpath={.items[0].status.podIP}".to_string(),
    ];
    let output = run_kubectl_raw(&runtime.kubectl_bin, &args, None).await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        if is_kubectl_not_found(&stderr) {
            return Ok(None);
        }
        anyhow::bail!("kubectl get pods failed: {}", stderr.trim());
    }

    let pod_ip = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if pod_ip.is_empty() {
        return Ok(None);
    }
    Ok(Some(pod_ip))
}

fn is_kubectl_not_found(stderr: &str) -> bool {
    stderr.contains("(NotFound)") || stderr.to_ascii_lowercase().contains("not found")
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

            let _ = append_log_line(&log_path, stream_name, &line).await;
        }

        tracing::info!(run_id = %run_id, stream = %stream_name, "log stream ended");
    });
}

fn spawn_kubernetes_log_reader(
    run_id: Uuid,
    runtime: KubernetesRuntime,
    tx: broadcast::Sender<RunEvent>,
    log_path: PathBuf,
    runs: RunManager,
) {
    tokio::spawn(async move {
        loop {
            if !runs.is_running(run_id).await {
                return;
            }

            let mut cmd = Command::new(&runtime.kubectl_bin);
            cmd.args([
                "-n",
                &runtime.namespace,
                "logs",
                "-l",
                &runtime.pod_label_selector,
                "--all-containers=true",
                "--prefix=true",
                "--pod-running-timeout=120s",
                "-f",
            ]);
            cmd.stdout(Stdio::piped());
            cmd.stderr(Stdio::piped());

            let mut child = match cmd.spawn() {
                Ok(c) => c,
                Err(e) => {
                    let msg = format!("failed to start kubectl log stream: {e}");
                    let _ = tx.send(RunEvent::Log {
                        stream: "stderr".to_string(),
                        line: msg.clone(),
                    });
                    let _ = append_log_line(&log_path, "stderr", &msg).await;
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    continue;
                }
            };

            if let Some(stdout) = child.stdout.take() {
                spawn_log_reader(run_id, "stdout", stdout, tx.clone(), log_path.clone());
            }
            if let Some(stderr) = child.stderr.take() {
                spawn_log_reader(run_id, "stderr", stderr, tx.clone(), log_path.clone());
            }

            let _ = child.wait().await;
            if !runs.is_running(run_id).await {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });
}

async fn append_log_line(path: &Path, stream: &str, line: &str) -> anyhow::Result<()> {
    let mut f = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;

    f.write_all(format!("[{stream}] {line}\n").as_bytes())
        .await?;
    Ok(())
}

fn spawn_local_completion_monitor(run_id: Uuid, runs: RunManager, store: Store) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            let maybe_status = {
                let mut guard = runs.inner.lock().await;
                let Some(h) = guard.get_mut(&run_id) else {
                    return;
                };

                match &mut h.runtime {
                    RunRuntime::Local { child } => {
                        if let Some(child) = child.as_mut() {
                            match child.try_wait() {
                                Ok(Some(status)) => Some(Ok(status)),
                                Ok(None) => None,
                                Err(e) => Some(Err(e)),
                            }
                        } else {
                            return;
                        }
                    }
                    RunRuntime::Kubernetes(_) => return,
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
}

fn spawn_kubernetes_job_completion_monitor(
    run_id: Uuid,
    runtime: KubernetesRuntime,
    runs: RunManager,
    store: Store,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            if !runs.is_running(run_id).await {
                return;
            }

            let workload = match kubectl_get_workload_json(&runtime).await {
                Ok(Some(v)) => v,
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(
                        run_id = %run_id,
                        workload = %runtime.workload_name,
                        error = %e,
                        "kubernetes workload status polling failed",
                    );
                    continue;
                }
            };

            if job_succeeded(&workload) {
                finalize_kubernetes_run(
                    run_id,
                    RunStatus::Succeeded,
                    Some(0),
                    None,
                    runs.clone(),
                    store.clone(),
                )
                .await;
                return;
            }

            if let Some(reason) = job_failure_reason(&workload) {
                finalize_kubernetes_run(
                    run_id,
                    RunStatus::Failed,
                    None,
                    Some(reason),
                    runs.clone(),
                    store.clone(),
                )
                .await;
                return;
            }
        }
    });
}

async fn finalize_kubernetes_run(
    run_id: Uuid,
    final_status: RunStatus,
    exit_code: Option<i64>,
    error: Option<String>,
    runs: RunManager,
    store: Store,
) {
    let ended_at = Utc::now();
    let (status_to_send, err_to_send, tx) = {
        let mut guard = runs.inner.lock().await;
        let Some(h) = guard.get_mut(&run_id) else {
            return;
        };

        let (status_to_send, err_to_send) = if h.status == RunStatus::Stopped {
            (RunStatus::Stopped, None)
        } else {
            h.status = final_status.clone();
            (final_status, error)
        };

        (status_to_send, err_to_send, h.tx.clone())
    };

    let _ = tx.send(RunEvent::Exit {
        status: status_to_send.clone(),
        exit_code,
        error: err_to_send.clone(),
    });

    let _ = store
        .update_run_completion(run_id, status_to_send, ended_at, exit_code, err_to_send)
        .await;
}

fn job_succeeded(workload: &serde_json::Value) -> bool {
    workload
        .get("status")
        .and_then(|s| s.get("succeeded"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
        > 0
}

fn job_failure_reason(workload: &serde_json::Value) -> Option<String> {
    if workload
        .get("status")
        .and_then(|s| s.get("failed"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
        > 0
    {
        let condition_reason = workload
            .get("status")
            .and_then(|s| s.get("conditions"))
            .and_then(|v| v.as_array())
            .and_then(|conditions| {
                conditions.iter().find_map(|c| {
                    let cond_type = c.get("type").and_then(|v| v.as_str())?;
                    let cond_status = c.get("status").and_then(|v| v.as_str())?;
                    if cond_type.eq_ignore_ascii_case("failed")
                        && cond_status.eq_ignore_ascii_case("true")
                    {
                        c.get("message")
                            .and_then(|v| v.as_str())
                            .map(str::to_string)
                            .or_else(|| {
                                c.get("reason").and_then(|v| v.as_str()).map(str::to_string)
                            })
                    } else {
                        None
                    }
                })
            });

        return Some(condition_reason.unwrap_or_else(|| "kubernetes job failed".to_string()));
    }

    None
}

fn spawn_metrics_poller(
    run_id: Uuid,
    config_id: Uuid,
    tool: Tool,
    runs: RunManager,
    store: Store,
    target: MetricsCollectionTarget,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));

        loop {
            ticker.tick().await;
            if !runs.is_running(run_id).await {
                return;
            }

            let view = match &target {
                MetricsCollectionTarget::Local => collect_tool_metrics(&tool).await,
                MetricsCollectionTarget::Kubernetes(runtime) => {
                    let endpoint_override =
                        match resolve_kubernetes_metrics_endpoint(runtime, &tool).await {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!(
                                    run_id = %run_id,
                                    tool_id = %tool.manifest.id,
                                    error = %e,
                                    "failed resolving kubernetes metrics endpoint",
                                );
                                continue;
                            }
                        };
                    collect_tool_metrics_with_endpoint(&tool, endpoint_override.as_deref()).await
                }
            };

            if view.error.is_some() {
                continue;
            }

            let view_json = match serde_json::to_string(&view) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        run_id = %run_id,
                        tool_id = %tool.manifest.id,
                        error = %e,
                        "Failed to serialize metrics snapshot",
                    );
                    continue;
                }
            };

            if let Err(e) = store
                .insert_tool_metrics_snapshot(
                    &tool.manifest.id,
                    Some(config_id),
                    Some(run_id),
                    &view.fetched_at,
                    &view_json,
                )
                .await
            {
                tracing::warn!(
                    run_id = %run_id,
                    tool_id = %tool.manifest.id,
                    error = %e,
                    "Failed to persist metrics snapshot",
                );
            }
        }
    });
}

async fn resolve_kubernetes_metrics_endpoint(
    runtime: &KubernetesRuntime,
    tool: &Tool,
) -> anyhow::Result<Option<String>> {
    let Some(metrics_port) = metrics_port_for_tool(tool) else {
        return Ok(None);
    };
    let Some(pod_ip) = kubectl_get_first_pod_ip(runtime).await? else {
        return Ok(None);
    };
    Ok(Some(format!("http://{pod_ip}:{metrics_port}/")))
}

fn metrics_port_for_tool(tool: &Tool) -> Option<u16> {
    let endpoint = tool.manifest.metrics.as_ref()?.endpoint.trim();
    if endpoint.is_empty() {
        return None;
    }
    let parsed = reqwest::Url::parse(endpoint).ok()?;
    parsed.port_or_known_default()
}

fn maybe_rewrite_state_file_path_for_kubernetes(
    config_content: &str,
    config_id: Uuid,
    should_rewrite_state_path: bool,
    config_ext: &str,
) -> anyhow::Result<String> {
    if !should_rewrite_state_path {
        return Ok(config_content.to_string());
    }

    let mut value: serde_json::Value = if config_ext == "json" {
        serde_json::from_str(config_content)?
    } else {
        serde_yaml::from_str(config_content)?
    };

    if let Some(state) = value.get_mut("state").and_then(|v| v.as_object_mut()) {
        let backend = state
            .get("backend")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_ascii_lowercase();
        if backend == "file" {
            state.insert(
                "file_path".to_string(),
                serde_json::Value::String(format!("/workspace/state/{config_id}.json")),
            );
        }
    }

    if config_ext == "json" {
        Ok(serde_json::to_string_pretty(&value)?)
    } else {
        Ok(serde_yaml::to_string(&value)?)
    }
}

pub async fn stop_run(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<Uuid>,
) -> ApiResult<Json<serde_json::Value>> {
    let stop_result = state.runs.stop(run_id).await;
    if stop_result.is_err() {
        let Some(run) = state
            .store
            .get_run(run_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        else {
            return not_found();
        };

        if run.backend != RunBackend::Kubernetes {
            return Err((StatusCode::NOT_FOUND, "unknown run".to_string()));
        }

        let Some(execution_ref) = run.execution_ref else {
            return bad_request("kubernetes run has no execution reference metadata".to_string());
        };

        stop_kubernetes_run_by_execution_ref(&state, &execution_ref)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    let _ = state
        .store
        .update_run_completion(run_id, RunStatus::Stopped, Utc::now(), None, None)
        .await;

    Ok(Json(json!({"ok": true})))
}

async fn stop_kubernetes_run_by_execution_ref(
    state: &AppState,
    execution_ref: &RunExecutionRef,
) -> anyhow::Result<()> {
    let namespace = execution_ref
        .namespace
        .as_deref()
        .unwrap_or(&state.execution.kubernetes.namespace);

    if let (Some(kind), Some(name)) = (execution_ref.kind.as_deref(), execution_ref.name.as_deref())
    {
        kubectl_delete_resource(
            &state.execution.kubernetes.kubectl_bin,
            namespace,
            kind,
            name,
        )
        .await?;
    }

    if let Some(config_map_name) = execution_ref.config_map_name.as_deref() {
        kubectl_delete_resource(
            &state.execution.kubernetes.kubectl_bin,
            namespace,
            "configmap",
            config_map_name,
        )
        .await?;
    }

    Ok(())
}

pub async fn run_logs(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<Uuid>,
    Query(q): Query<RunLogsQuery>,
) -> ApiResult<Json<Vec<RunEvent>>> {
    let limit = q.limit.unwrap_or(2000).clamp(1, 10_000);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{CreateConfigRequest, ExecutionConfig, KubernetesExecutionConfig};
    use crate::tools::ToolRegistry;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    struct TestContext {
        _tmp: TempDir,
        state: AppState,
        config_id: Uuid,
        kubectl_log: PathBuf,
    }

    enum MockKubectlBehavior {
        OneShotJobSucceeds,
        DaemonNeverCompletes,
    }

    #[tokio::test]
    async fn kubernetes_one_shot_run_persists_execution_ref_and_completes() {
        let ctx = setup_test_context(MockKubectlBehavior::OneShotJobSucceeds)
            .await
            .expect("failed preparing test context");

        let req = StartRunRequest {
            tool_id: "mocktool".to_string(),
            config_id: ctx.config_id,
            mode: RunMode::OneShot,
            daemon_interval_secs: None,
            purge_graph: Some(false),
            purge_mappings: Some(vec![]),
        };

        let Json(run) = start_run(State(ctx.state.clone()), Json(req))
            .await
            .expect("start_run should succeed");

        assert_eq!(run.backend, RunBackend::Kubernetes);
        let execution_ref = run
            .execution_ref
            .clone()
            .expect("execution ref should exist");
        assert_eq!(execution_ref.kind.as_deref(), Some("job"));
        assert_eq!(execution_ref.namespace.as_deref(), Some("dm-sql-tests"));
        assert!(
            execution_ref
                .name
                .as_deref()
                .unwrap_or_default()
                .starts_with("dm-run-"),
            "expected generated run workload name",
        );

        let materialized_cfg = ctx
            .state
            .data_dir
            .join("runs")
            .join(run.id.to_string())
            .join("config.yaml");
        let cfg_raw = tokio::fs::read_to_string(&materialized_cfg)
            .await
            .expect("materialized config should exist");
        assert!(
            cfg_raw.contains("/workspace/state/"),
            "expected state.file_path rewrite for shared pvc"
        );

        let final_run = wait_for_run_terminal(&ctx.state.store, run.id, Duration::from_secs(8))
            .await
            .expect("run should reach terminal state");
        assert_eq!(final_run.status, RunStatus::Succeeded);
        assert_eq!(final_run.backend, RunBackend::Kubernetes);

        let kubectl_log = tokio::fs::read_to_string(&ctx.kubectl_log)
            .await
            .expect("kubectl log should exist");
        assert!(
            kubectl_log.matches("apply -f -").count() >= 2,
            "expected configmap+workload apply commands in kubectl log; got: {kubectl_log}",
        );
        assert!(
            kubectl_log.contains(" get job "),
            "expected kubectl get job polling command; got: {kubectl_log}",
        );
    }

    #[tokio::test]
    async fn kubernetes_daemon_stop_deletes_deployment_and_configmap() {
        let ctx = setup_test_context(MockKubectlBehavior::DaemonNeverCompletes)
            .await
            .expect("failed preparing test context");

        let req = StartRunRequest {
            tool_id: "mocktool".to_string(),
            config_id: ctx.config_id,
            mode: RunMode::Daemon,
            daemon_interval_secs: Some(30),
            purge_graph: Some(false),
            purge_mappings: Some(vec![]),
        };

        let Json(run) = start_run(State(ctx.state.clone()), Json(req))
            .await
            .expect("start_run should succeed");
        assert_eq!(run.backend, RunBackend::Kubernetes);
        let execution_ref = run
            .execution_ref
            .clone()
            .expect("execution ref should exist");
        assert_eq!(execution_ref.kind.as_deref(), Some("deployment"));

        let Json(stop_resp) = stop_run(State(ctx.state.clone()), AxumPath(run.id))
            .await
            .expect("stop_run should succeed");
        assert_eq!(stop_resp.get("ok").and_then(|v| v.as_bool()), Some(true));

        let persisted = ctx
            .state
            .store
            .get_run(run.id)
            .await
            .expect("store query should succeed")
            .expect("run should exist");
        assert_eq!(persisted.status, RunStatus::Stopped);

        let kubectl_log = tokio::fs::read_to_string(&ctx.kubectl_log)
            .await
            .expect("kubectl log should exist");
        assert!(
            kubectl_log.contains("delete deployment/"),
            "expected deployment deletion command in kubectl log; got: {kubectl_log}",
        );
        assert!(
            kubectl_log.contains("delete configmap/"),
            "expected configmap deletion command in kubectl log; got: {kubectl_log}",
        );
    }

    async fn setup_test_context(behavior: MockKubectlBehavior) -> anyhow::Result<TestContext> {
        let tmp = TempDir::new()?;
        let repo_root = tmp.path().join("repo");
        let data_dir = tmp.path().join("data");
        let kubectl_log = tmp.path().join("kubectl.log");
        let kubectl_path = tmp.path().join("mock-kubectl.sh");

        tokio::fs::create_dir_all(&repo_root).await?;
        tokio::fs::create_dir_all(&data_dir).await?;
        tokio::fs::write(&kubectl_log, "").await?;

        let manifest_dir = repo_root.join("mock-tool");
        tokio::fs::create_dir_all(&manifest_dir).await?;
        let manifest_json = serde_json::json!({
            "id": "mocktool",
            "displayName": "Mock Tool",
            "description": "mock tool manifest for kubernetes run tests",
            "workingDir": ".",
            "executable": {
                "type": "path",
                "path": "bin/mock-tool"
            },
            "capabilities": {
                "supports_daemon": true,
                "supports_purge_graph": true,
                "supports_purge_mapping": true,
                "supports_metrics": false
            },
            "config": {
                "fileExtensions": [".yaml"],
                "examples": []
            }
        });
        tokio::fs::write(
            manifest_dir.join("tool.manifest.json"),
            serde_json::to_vec_pretty(&manifest_json)?,
        )
        .await?;

        write_mock_kubectl_script(&kubectl_path, &kubectl_log, behavior)?;

        let tools = ToolRegistry::load_from_repo(&repo_root).await?;
        let store = Store::connect(&data_dir.join("control-plane.sqlite")).await?;
        store.upsert_tools(&tools).await?;
        let runs = RunManager::new(data_dir.join("runs"));

        let app_state = AppState {
            repo_root: repo_root.clone(),
            data_dir: data_dir.clone(),
            tools,
            store,
            runs,
            execution: ExecutionConfig {
                backend: ExecutionBackend::Kubernetes,
                kubernetes: KubernetesExecutionConfig {
                    namespace: "dm-sql-tests".to_string(),
                    runner_image: "ghcr.io/falkordb/dm-sql-to-falkordb-runner:test".to_string(),
                    image_pull_policy: "IfNotPresent".to_string(),
                    service_account: Some("dm-sql-tests-sa".to_string()),
                    shared_pvc_name: Some("runner-shared-pvc".to_string()),
                    env_secret_name: Some("tool-env-secret".to_string()),
                    env_configmap_name: Some("tool-env-config".to_string()),
                    kubectl_bin: kubectl_path.to_string_lossy().to_string(),
                    binary_dir: "/opt/falkordb/bin".to_string(),
                },
            },
            api_key: None,
        };

        let cfg = app_state
            .store
            .create_config(CreateConfigRequest {
                tool_id: "mocktool".to_string(),
                name: "mock-config".to_string(),
                content: r#"
state:
  backend: file
  file_path: state.json
mappings: []
"#
                .trim()
                .to_string(),
            })
            .await?;

        Ok(TestContext {
            _tmp: tmp,
            state: app_state,
            config_id: cfg.id,
            kubectl_log,
        })
    }

    async fn wait_for_run_terminal(
        store: &Store,
        run_id: Uuid,
        timeout: Duration,
    ) -> anyhow::Result<RunRecord> {
        let deadline = Instant::now() + timeout;
        loop {
            let run = store
                .get_run(run_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("run not found"))?;
            if run.status != RunStatus::Running {
                return Ok(run);
            }
            if Instant::now() >= deadline {
                anyhow::bail!("run did not reach terminal state before timeout");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    fn write_mock_kubectl_script(
        path: &Path,
        log_path: &Path,
        behavior: MockKubectlBehavior,
    ) -> anyhow::Result<()> {
        let mut script = format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
echo "$*" >> "{log_path}"

if [[ "$*" == *" get pods "* ]]; then
  printf "10.42.0.9"
  exit 0
fi

if [[ "$*" == *" logs "* ]]; then
  echo "mock log line from kubectl logs"
  sleep 0.1
  exit 0
fi

if [[ "$*" == *" get job "* && "$*" == *"-o json"* ]]; then
"#,
            log_path = log_path.to_string_lossy()
        );

        match behavior {
            MockKubectlBehavior::OneShotJobSucceeds => {
                script.push_str(
                    r#"  cat <<'JSON'
{"status":{"succeeded":1}}
JSON
  exit 0
"#,
                );
            }
            MockKubectlBehavior::DaemonNeverCompletes => {
                script.push_str(
                    r#"  cat <<'JSON'
{"status":{"active":1}}
JSON
  exit 0
"#,
                );
            }
        }

        script.push_str(
            r#"
fi
exit 0
"#,
        );

        fs::write(path, script)?;
        let mut perms = fs::metadata(path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms)?;
        Ok(())
    }
}
