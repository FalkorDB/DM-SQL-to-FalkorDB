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
    pub execution: ExecutionConfig,
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ExecutionBackend {
    #[default]
    Local,
    Kubernetes,
}

impl ExecutionBackend {
    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "local" => Ok(Self::Local),
            "kubernetes" | "k8s" => Ok(Self::Kubernetes),
            other => anyhow::bail!("unsupported execution backend '{other}'"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct KubernetesExecutionConfig {
    pub namespace: String,
    pub runner_image: String,
    pub image_pull_policy: String,
    pub service_account: Option<String>,
    pub shared_pvc_name: Option<String>,
    pub env_secret_name: Option<String>,
    pub env_configmap_name: Option<String>,
    pub kubectl_bin: String,
    pub binary_dir: String,
    /// Security/resource settings applied to every runner pod (run and scaffold
    /// workloads) created by the kubernetes execution backend.
    pub runner_pod: RunnerPodHardening,
}

/// Pod- and container-level settings applied to runner pods created by the
/// kubernetes execution backend. All fields are optional so that the control plane
/// keeps working when it is deployed without the Helm chart.
#[derive(Debug, Clone, Default)]
pub struct RunnerPodHardening {
    /// Optional pod-level `securityContext.fsGroup` applied to runner workloads so that
    /// non-root runner containers can write to mounted PersistentVolumeClaims (e.g. the
    /// shared workspace PVC). Ignored when `pod_security_context` already sets `fsGroup`.
    pub pod_fs_group: Option<i64>,
    /// Full pod-level `securityContext` (JSON object).
    pub pod_security_context: Option<serde_json::Value>,
    /// Container-level `securityContext` (JSON object) for the runner container.
    pub container_security_context: Option<serde_json::Value>,
    /// Container `resources` (JSON object) for the runner container.
    pub resources: Option<serde_json::Value>,
    /// Pod-level `automountServiceAccountToken`.
    pub automount_service_account_token: Option<bool>,
    /// Extra labels (JSON object of string values) added to runner workloads and their
    /// pod templates, e.g. a release-specific label used by NetworkPolicy selectors.
    pub extra_labels: Option<serde_json::Value>,
}

impl RunnerPodHardening {
    /// Merges `extra_labels` into a labels object. Built-in labels are never overridden.
    pub fn apply_labels(&self, labels: &mut serde_json::Value) {
        let (Some(extra), Some(target)) = (
            self.extra_labels.as_ref().and_then(|v| v.as_object()),
            labels.as_object_mut(),
        ) else {
            return;
        };
        for (k, v) in extra {
            if v.is_string() {
                target.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
    }

    /// Applies the configured settings to a pod spec that has a single runner
    /// container. When no shared workspace volume is mounted at `/workspace`, an
    /// `emptyDir` is mounted there so tools can write file-backed state regardless of
    /// the UID the pod runs as (e.g. OpenShift arbitrary UIDs). When the container uses
    /// a read-only root filesystem, an `emptyDir` is also mounted at `/tmp`.
    pub fn apply(&self, pod_spec: &mut serde_json::Value) {
        let Some(spec) = pod_spec.as_object_mut() else {
            return;
        };

        let mut pod_sc = self
            .pod_security_context
            .as_ref()
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_default();
        if let Some(fs_group) = self.pod_fs_group {
            pod_sc
                .entry("fsGroup".to_string())
                .or_insert_with(|| serde_json::Value::from(fs_group));
        }
        if !pod_sc.is_empty() {
            spec.insert(
                "securityContext".to_string(),
                serde_json::Value::Object(pod_sc),
            );
        }

        if let Some(automount) = self.automount_service_account_token {
            spec.insert(
                "automountServiceAccountToken".to_string(),
                serde_json::Value::Bool(automount),
            );
        }

        let read_only_root = self
            .container_security_context
            .as_ref()
            .and_then(|v| v.get("readOnlyRootFilesystem"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let mut extra_volumes = Vec::new();
        if let Some(container) = spec
            .get_mut("containers")
            .and_then(|v| v.as_array_mut())
            .and_then(|c| c.first_mut())
            .and_then(|c| c.as_object_mut())
        {
            if let Some(sc) = self
                .container_security_context
                .as_ref()
                .filter(|v| v.as_object().is_some_and(|o| !o.is_empty()))
            {
                container.insert("securityContext".to_string(), sc.clone());
            }
            if let Some(resources) = self
                .resources
                .as_ref()
                .filter(|v| v.as_object().is_some_and(|o| !o.is_empty()))
            {
                container.insert("resources".to_string(), resources.clone());
            }

            let mut scratch = vec![("workspace-scratch", "/workspace")];
            if read_only_root {
                scratch.insert(0, ("tmp", "/tmp"));
            }
            {
                let mounts = container
                    .entry("volumeMounts".to_string())
                    .or_insert_with(|| serde_json::Value::Array(Vec::new()));
                if let Some(mounts) = mounts.as_array_mut() {
                    let has_mount = |mounts: &Vec<serde_json::Value>, path: &str| {
                        mounts
                            .iter()
                            .any(|m| m.get("mountPath").and_then(|p| p.as_str()) == Some(path))
                    };
                    for (name, path) in scratch {
                        if !has_mount(mounts, path) {
                            mounts.push(serde_json::json!({ "name": name, "mountPath": path }));
                            extra_volumes.push(serde_json::json!({ "name": name, "emptyDir": {} }));
                        }
                    }
                }
            }
        }

        if !extra_volumes.is_empty() {
            let volumes = spec
                .entry("volumes".to_string())
                .or_insert_with(|| serde_json::Value::Array(Vec::new()));
            if let Some(volumes) = volumes.as_array_mut() {
                volumes.extend(extra_volumes);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    pub backend: ExecutionBackend,
    pub kubernetes: KubernetesExecutionConfig,
}

#[derive(Debug, Serialize)]
pub struct Health {
    pub ok: bool,
    pub version: String,
}

pub async fn health() -> Json<Health> {
    Json(Health {
        ok: true,
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RunBackend {
    #[default]
    Local,
    Kubernetes,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RunExecutionRef {
    pub kind: Option<String>,
    pub name: Option<String>,
    pub namespace: Option<String>,
    pub pod_label_selector: Option<String>,
    pub config_map_name: Option<String>,
    pub image: Option<String>,
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
    #[serde(default)]
    pub backend: RunBackend,
    #[serde(default)]
    pub execution_ref: Option<RunExecutionRef>,
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
pub struct GenerateScaffoldTemplateRequest {
    pub config_content: String,
    #[serde(default)]
    pub include_schema_summary: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateScaffoldTemplateResponse {
    pub template_yaml: String,
    pub schema_summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateSchemaGraphPreviewRequest {
    pub config_content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaGraphPreviewSource {
    Config,
    Template,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanvasData {
    pub nodes: Vec<CanvasNode>,
    pub links: Vec<CanvasLink>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanvasNode {
    pub id: u64,
    pub labels: Vec<String>,
    pub color: String,
    pub visible: bool,
    pub caption: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u32>,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanvasLink {
    pub id: u64,
    pub relationship: String,
    pub color: String,
    pub source: u64,
    pub target: u64,
    pub visible: bool,
    pub data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateSchemaGraphPreviewResponse {
    pub canvas_data: CanvasData,
    pub warnings: Vec<String>,
    pub source: SchemaGraphPreviewSource,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunEvent {
    State {
        status: RunStatus,
    },
    Log {
        stream: String,
        line: String,
    },
    Exit {
        status: RunStatus,
        exit_code: Option<i64>,
        error: Option<String>,
    },
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

#[cfg(test)]
mod runner_pod_hardening_tests {
    use super::RunnerPodHardening;
    use serde_json::json;

    fn base_pod_spec() -> serde_json::Value {
        json!({
            "containers": [{
                "name": "runner",
                "volumeMounts": [{ "name": "run-config", "mountPath": "/run-config" }]
            }],
            "volumes": [{ "name": "run-config", "configMap": { "name": "cfg" } }]
        })
    }

    #[test]
    fn default_only_adds_workspace_scratch() {
        let mut spec = base_pod_spec();
        RunnerPodHardening::default().apply(&mut spec);
        assert!(spec.get("securityContext").is_none());
        let mounts = spec["containers"][0]["volumeMounts"].as_array().unwrap();
        assert_eq!(mounts.len(), 2);
        assert_eq!(mounts[1]["mountPath"], json!("/workspace"));
        assert_eq!(spec["volumes"][1]["emptyDir"], json!({}));
    }

    #[test]
    fn extra_labels_do_not_override_builtin() {
        let hardening = RunnerPodHardening {
            extra_labels: Some(json!({
                "app.kubernetes.io/instance": "rel-a",
                "dm.falkordb/release-namespace": "ns-a",
                "app.kubernetes.io/name": "evil",
                "ignored": 1
            })),
            ..Default::default()
        };
        let mut labels = json!({ "app.kubernetes.io/name": "falkordb-migrate-runner" });
        hardening.apply_labels(&mut labels);
        assert_eq!(
            labels,
            json!({
                "app.kubernetes.io/name": "falkordb-migrate-runner",
                "app.kubernetes.io/instance": "rel-a",
                "dm.falkordb/release-namespace": "ns-a"
            })
        );
    }

    #[test]
    fn fs_group_only_keeps_legacy_behavior() {
        let mut spec = base_pod_spec();
        RunnerPodHardening {
            pod_fs_group: Some(10002),
            ..Default::default()
        }
        .apply(&mut spec);
        assert_eq!(spec["securityContext"], json!({ "fsGroup": 10002 }));
        assert!(spec["containers"][0].get("securityContext").is_none());
    }

    #[test]
    fn applies_restricted_settings() {
        let mut spec = base_pod_spec();
        RunnerPodHardening {
            pod_fs_group: Some(1),
            pod_security_context: Some(json!({
                "runAsNonRoot": true,
                "fsGroup": 10002,
                "seccompProfile": { "type": "RuntimeDefault" }
            })),
            container_security_context: Some(json!({
                "allowPrivilegeEscalation": false,
                "readOnlyRootFilesystem": true,
                "capabilities": { "drop": ["ALL"] }
            })),
            resources: Some(json!({ "limits": { "memory": "1Gi" } })),
            automount_service_account_token: Some(false),
            extra_labels: None,
        }
        .apply(&mut spec);

        // An explicit fsGroup in the pod security context wins over pod_fs_group.
        assert_eq!(spec["securityContext"]["fsGroup"], json!(10002));
        assert_eq!(spec["securityContext"]["runAsNonRoot"], json!(true));
        assert_eq!(spec["automountServiceAccountToken"], json!(false));
        let container = &spec["containers"][0];
        assert_eq!(
            container["securityContext"]["capabilities"]["drop"],
            json!(["ALL"])
        );
        assert_eq!(container["resources"]["limits"]["memory"], json!("1Gi"));
        let mounts: Vec<&str> = container["volumeMounts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["mountPath"].as_str().unwrap())
            .collect();
        assert_eq!(mounts, vec!["/run-config", "/tmp", "/workspace"]);
        assert_eq!(spec["volumes"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn read_only_root_does_not_shadow_shared_workspace() {
        let mut spec = json!({
            "containers": [{
                "name": "runner",
                "volumeMounts": [{ "name": "workspace", "mountPath": "/workspace" }]
            }],
            "volumes": [{ "name": "workspace", "persistentVolumeClaim": { "claimName": "pvc" } }]
        });
        RunnerPodHardening {
            container_security_context: Some(json!({ "readOnlyRootFilesystem": true })),
            ..Default::default()
        }
        .apply(&mut spec);
        let mounts = spec["containers"][0]["volumeMounts"].as_array().unwrap();
        assert_eq!(mounts.len(), 2);
        assert_eq!(mounts[1]["mountPath"], json!("/tmp"));
    }
}
