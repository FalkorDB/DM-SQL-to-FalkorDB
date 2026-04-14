use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Context;
use axum::extract::{Path as AxumPath, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

use crate::models::AppState;
use crate::models::{bad_request, not_found};
use crate::models::{ApiResult, ToolCapabilities, ToolSummary};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolManifest {
    pub id: String,
    #[serde(rename = "displayName")]
    pub display_name: String,
    pub description: Option<String>,
    #[serde(rename = "workingDir")]
    pub working_dir: String,
    pub executable: ExecutableSpec,
    pub capabilities: ToolCapabilities,
    pub config: ToolConfigSpec,
    #[serde(default)]
    pub metrics: Option<ToolMetricsSpec>,
}

fn parse_enabled_tools_env() -> Option<HashSet<String>> {
    let raw = std::env::var("CONTROL_PLANE_ENABLED_TOOLS").ok()?;
    let parsed = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<HashSet<_>>();
    if parsed.is_empty() {
        None
    } else {
        Some(parsed)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutableSpec {
    Cargo {
        #[serde(rename = "manifestPath")]
        manifest_path: String,
        #[serde(rename = "bin")]
        bin: Option<String>,
        #[serde(default = "default_true")]
        release: bool,
    },
    Path {
        path: String,
    },
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolConfigSpec {
    #[serde(rename = "fileExtensions")]
    pub file_extensions: Vec<String>,
    pub examples: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolMetricsSpec {
    pub endpoint: String,
    #[serde(default = "default_metrics_format")]
    pub format: String,
    #[serde(rename = "metricPrefix")]
    pub metric_prefix: Option<String>,
    #[serde(rename = "mappingLabel", default = "default_mapping_label")]
    pub mapping_label: String,
}

fn default_metrics_format() -> String {
    "prometheus_text".to_string()
}

fn default_mapping_label() -> String {
    "mapping".to_string()
}

#[derive(Debug, Clone)]
pub struct Tool {
    pub manifest_path: PathBuf,
    pub manifest: ToolManifest,
}

#[derive(Debug, Clone, Default)]
pub struct ToolRegistry {
    by_id: HashMap<String, Tool>,
}

impl ToolRegistry {
    pub async fn load_from_repo(repo_root: &Path) -> anyhow::Result<Self> {
        let mut by_id = HashMap::new();
        let enabled_tools = parse_enabled_tools_env();

        for entry in WalkDir::new(repo_root)
            .follow_links(false)
            .max_depth(4)
            .into_iter()
            .filter_map(Result::ok)
        {
            if !entry.file_type().is_file() {
                continue;
            }
            if entry.file_name() != "tool.manifest.json" {
                continue;
            }

            let path = entry.path().to_path_buf();
            let raw = tokio::fs::read_to_string(&path).await?;
            let manifest: ToolManifest = serde_json::from_str(&raw)
                .with_context(|| format!("invalid manifest json: {}", path.display()))?;

            if let Some(enabled) = &enabled_tools {
                if !enabled.contains(&manifest.id) {
                    continue;
                }
            }

            if by_id.contains_key(&manifest.id) {
                anyhow::bail!("duplicate tool id '{}' in {}", manifest.id, path.display());
            }

            by_id.insert(
                manifest.id.clone(),
                Tool {
                    manifest_path: path,
                    manifest,
                },
            );
        }

        if by_id.is_empty() {
            tracing::warn!("no tool.manifest.json files found; UI will be empty");
        }

        Ok(Self { by_id })
    }

    pub fn list(&self) -> Vec<ToolSummary> {
        let mut out: Vec<ToolSummary> = self
            .by_id
            .values()
            .map(|t| ToolSummary {
                id: t.manifest.id.clone(),
                display_name: t.manifest.display_name.clone(),
                description: t.manifest.description.clone(),
                capabilities: t.manifest.capabilities.clone(),
            })
            .collect();

        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    pub fn get(&self, id: &str) -> Option<&Tool> {
        self.by_id.get(id)
    }

    pub fn all(&self) -> Vec<&Tool> {
        self.by_id.values().collect()
    }

    pub fn as_json_by_id(&self) -> HashMap<String, serde_json::Value> {
        self.by_id
            .iter()
            .map(|(id, tool)| {
                let v = serde_json::to_value(&tool.manifest).unwrap_or(serde_json::Value::Null);
                (id.clone(), v)
            })
            .collect()
    }
}

pub async fn list_tools(State(state): State<AppState>) -> Json<Vec<ToolSummary>> {
    Json(state.tools.list())
}

pub async fn get_tool(
    State(state): State<AppState>,
    AxumPath(tool_id): AxumPath<String>,
) -> ApiResult<Json<ToolManifest>> {
    let Some(tool) = state.tools.get(&tool_id) else {
        return not_found();
    };

    Ok(Json(tool.manifest.clone()))
}

impl Tool {
    pub fn working_dir_abs(&self, repo_root: &Path) -> PathBuf {
        repo_root.join(&self.manifest.working_dir)
    }

    pub fn executable_command(
        &self,
        repo_root: &Path,
    ) -> anyhow::Result<(String, Vec<String>, PathBuf)> {
        let cwd = self.working_dir_abs(repo_root);

        match &self.manifest.executable {
            ExecutableSpec::Cargo {
                manifest_path,
                bin,
                release,
            } => {
                let mut args = vec![
                    "run".to_string(),
                    if *release { "--release" } else { "" }.to_string(),
                    "--manifest-path".to_string(),
                    repo_root.join(manifest_path).to_string_lossy().to_string(),
                ];

                args.retain(|a| !a.is_empty());

                if let Some(bin) = bin {
                    args.push("--bin".to_string());
                    args.push(bin.clone());
                }

                Ok(("cargo".to_string(), args, cwd))
            }
            ExecutableSpec::Path { path } => Ok((
                repo_root.join(path).to_string_lossy().to_string(),
                vec![],
                cwd,
            )),
        }
    }

    pub fn runner_binary_name(&self, repo_root: &Path) -> anyhow::Result<String> {
        match &self.manifest.executable {
            ExecutableSpec::Path { path } => {
                let file_name = Path::new(path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| {
                        anyhow::anyhow!("unable to determine binary filename for '{}'", path)
                    })?;
                Ok(file_name.to_string())
            }
            ExecutableSpec::Cargo {
                manifest_path, bin, ..
            } => {
                if let Some(bin) = bin {
                    return Ok(bin.clone());
                }

                let manifest_abs = repo_root.join(manifest_path);
                infer_cargo_package_name(&manifest_abs).with_context(|| {
                    format!(
                        "failed to infer runner binary from Cargo manifest '{}'",
                        manifest_abs.display()
                    )
                })
            }
        }
    }

    pub fn validate_run_request(
        &self,
        mode: &crate::models::RunMode,
        purge_graph: bool,
        purge_mappings: &[String],
    ) -> ApiResult<()> {
        if matches!(mode, crate::models::RunMode::Daemon)
            && !self.manifest.capabilities.supports_daemon
        {
            return bad_request("tool does not support daemon mode".to_string());
        }

        if purge_graph && !self.manifest.capabilities.supports_purge_graph {
            return bad_request("tool does not support purge_graph".to_string());
        }

        if !purge_mappings.is_empty() && !self.manifest.capabilities.supports_purge_mapping {
            return bad_request("tool does not support purge_mappings".to_string());
        }

        Ok(())
    }
}

fn infer_cargo_package_name(manifest_path: &Path) -> anyhow::Result<String> {
    let raw = fs::read_to_string(manifest_path)?;
    let mut in_package_section = false;

    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            in_package_section = trimmed == "[package]";
            continue;
        }
        if !in_package_section {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("name") {
            let Some((_, value)) = rest.split_once('=') else {
                continue;
            };
            let candidate = value.trim().trim_matches('"');
            if !candidate.is_empty() {
                return Ok(candidate.to_string());
            }
        }
    }

    anyhow::bail!("package.name not found in {}", manifest_path.display())
}
