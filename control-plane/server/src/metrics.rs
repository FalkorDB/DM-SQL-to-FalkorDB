use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use axum::extract::{Path as AxumPath, Query, State};
use axum::Json;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::models::{bad_request, not_found, ApiResult};
use crate::models::AppState;
use crate::tools::Tool;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolMetricsView {
    pub tool_id: String,
    pub display_name: String,
    pub supports_metrics: bool,
    pub endpoint: Option<String>,
    pub format: Option<String>,
    pub metric_prefix: Option<String>,
    pub mapping_label: Option<String>,
    pub fetched_at: String,
    pub snapshot_timestamp: Option<String>,
    pub snapshot_run_id: Option<String>,
    pub snapshot_config_id: Option<String>,
    pub snapshot_source: Option<String>,
    pub overall: BTreeMap<String, f64>,
    pub per_mapping: BTreeMap<String, BTreeMap<String, f64>>,
    pub warnings: Vec<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
struct MetricSample {
    name: String,
    labels: HashMap<String, String>,
    value: f64,
}

#[derive(Debug, Clone)]
struct ResolvedMetricsSpec {
    endpoint: String,
    format: String,
    metric_prefix: String,
    mapping_label: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MetricsQuery {
    pub config_id: Option<Uuid>,
}

pub async fn list_tool_metrics(
    State(state): State<AppState>,
    Query(query): Query<MetricsQuery>,
) -> ApiResult<Json<Vec<ToolMetricsView>>> {
    let mut out = Vec::new();

    if let Some(config_id) = query.config_id {
        let Some(config) = state
            .store
            .get_config(config_id)
            .await
            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        else {
            return bad_request("unknown config_id".to_string());
        };

        let Some(tool) = state.tools.get(&config.tool_id) else {
            return bad_request("config references unknown tool_id".to_string());
        };

        out.push(load_persisted_tool_metrics_or_default(&state, tool, Some(config_id)).await);
    } else {
        for summary in state.tools.list() {
            if let Some(tool) = state.tools.get(&summary.id) {
                out.push(load_persisted_tool_metrics_or_default(&state, tool, None).await);
            }
        }
    }

    Ok(Json(out))
}

pub async fn get_tool_metrics(
    State(state): State<AppState>,
    AxumPath(tool_id): AxumPath<String>,
    Query(query): Query<MetricsQuery>,
) -> ApiResult<Json<ToolMetricsView>> {
    let Some(tool) = state.tools.get(&tool_id) else {
        return not_found();
    };
    if let Some(config_id) = query.config_id {
        let Some(config) = state
            .store
            .get_config(config_id)
            .await
            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        else {
            return bad_request("unknown config_id".to_string());
        };

        if config.tool_id != tool_id {
            return bad_request("config_id does not belong to requested tool_id".to_string());
        }
    }

    Ok(Json(
        load_persisted_tool_metrics_or_default(&state, tool, query.config_id).await,
    ))
}

async fn load_persisted_tool_metrics_or_default(
    state: &AppState,
    tool: &Tool,
    config_id: Option<Uuid>,
) -> ToolMetricsView {
    let mut fallback = empty_tool_metrics_view(tool);

    if !fallback.supports_metrics {
        fallback
            .warnings
            .push("Tool does not advertise metrics support.".to_string());
        return fallback;
    }

    let stored = match state
        .store
        .get_latest_tool_metrics_snapshot(&tool.manifest.id, config_id)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            fallback.error = Some(format!("Failed reading persisted metrics snapshot: {e}"));
            return fallback;
        }
    };

    let Some(snapshot_row) = stored else {
        if let Some(config_id) = config_id {
            fallback.warnings.push(format!(
                "No persisted metrics snapshot yet for config_id '{config_id}'. Start a run with this config to collect metrics."
            ));
        } else {
            fallback
                .warnings
                .push("No persisted metrics snapshot yet. Start a run to collect metrics.".to_string());
        }
        return fallback;
    };

    let raw_json = snapshot_row.view_json;

    match serde_json::from_str::<ToolMetricsView>(&raw_json) {
        Ok(mut view) => {
            let spec = resolve_metrics_spec(tool);
            view.tool_id = tool.manifest.id.clone();
            view.display_name = tool.manifest.display_name.clone();
            view.supports_metrics = tool.manifest.capabilities.supports_metrics;
            if view.endpoint.is_none() {
                view.endpoint = Some(spec.endpoint);
            }
            if view.format.is_none() {
                view.format = Some(spec.format);
            }
            if view.metric_prefix.is_none() {
                view.metric_prefix = Some(spec.metric_prefix);
            }
            if view.mapping_label.is_none() {
                view.mapping_label = Some(spec.mapping_label);
            }
            view.snapshot_timestamp = Some(snapshot_row.fetched_at.clone());
            view.snapshot_run_id = snapshot_row.run_id;
            view.snapshot_config_id = snapshot_row.config_id;
            view.snapshot_source = Some("persisted".to_string());
            view.fetched_at = snapshot_row.fetched_at;
            view.warnings
                .push("Serving persisted metrics snapshot from control-plane store.".to_string());
            view
        }
        Err(e) => {
            fallback.error = Some(format!("Failed to decode persisted metrics snapshot: {e}"));
            fallback
        }
    }
}

fn empty_tool_metrics_view(tool: &Tool) -> ToolMetricsView {
    let spec = resolve_metrics_spec(tool);
    ToolMetricsView {
        tool_id: tool.manifest.id.clone(),
        display_name: tool.manifest.display_name.clone(),
        supports_metrics: tool.manifest.capabilities.supports_metrics,
        endpoint: Some(spec.endpoint),
        format: Some(spec.format),
        metric_prefix: Some(spec.metric_prefix),
        mapping_label: Some(spec.mapping_label),
        fetched_at: Utc::now().to_rfc3339(),
        snapshot_timestamp: None,
        snapshot_run_id: None,
        snapshot_config_id: None,
        snapshot_source: None,
        overall: BTreeMap::new(),
        per_mapping: BTreeMap::new(),
        warnings: Vec::new(),
        error: None,
    }
}

pub async fn collect_tool_metrics(tool: &Tool) -> ToolMetricsView {
    let mut view = empty_tool_metrics_view(tool);
    let spec = resolve_metrics_spec(tool);
    view.snapshot_source = Some("live".to_string());

    if spec.format != "prometheus_text" {
        view.error = Some(format!(
            "Unsupported metrics format '{}'; only 'prometheus_text' is currently supported.",
            spec.format
        ));
        return view;
    }

    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            view.error = Some(format!("Failed to create HTTP client: {e}"));
            return view;
        }
    };

    let resp = match client.get(&spec.endpoint).send().await {
        Ok(r) => r,
        Err(e) => {
            view.error = Some(format!("Failed to fetch metrics from {}: {e}", spec.endpoint));
            return view;
        }
    };

    if !resp.status().is_success() {
        view.error = Some(format!(
            "Metrics endpoint {} returned HTTP {}",
            spec.endpoint,
            resp.status()
        ));
        return view;
    }

    let body = match resp.text().await {
        Ok(t) => t,
        Err(e) => {
            view.error = Some(format!("Failed reading metrics body: {e}"));
            return view;
        }
    };

    let samples = parse_prometheus_text(&body);
    let mut matched = 0usize;

    for sample in samples {
        if !sample.name.starts_with(&spec.metric_prefix) {
            continue;
        }

        matched += 1;
        let metric_name = sample
            .name
            .strip_prefix(&spec.metric_prefix)
            .unwrap_or(sample.name.as_str())
            .to_string();

        if let Some(mapping) = sample.labels.get(&spec.mapping_label) {
            view.per_mapping
                .entry(mapping.clone())
                .or_default()
                .insert(metric_name, sample.value);
        } else {
            view.overall.insert(metric_name, sample.value);
        }
    }

    if matched == 0 {
        let non_comment_lines = body
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .count();
        if non_comment_lines == 0 {
            view.warnings
                .push("Metrics endpoint is reachable but returned no samples.".to_string());
        } else {
            view.warnings.push(format!(
                "Fetched {non_comment_lines} samples but none matched expected prefix '{}'.",
                spec.metric_prefix
            ));
        }
    }

    view
}

fn resolve_metrics_spec(tool: &Tool) -> ResolvedMetricsSpec {
    let default_port = default_metrics_port(&tool.manifest.id);
    let default_prefix = format!("{}_to_falkordb_", tool.manifest.id);
    let default = ResolvedMetricsSpec {
        endpoint: format!("http://127.0.0.1:{default_port}/"),
        format: "prometheus_text".to_string(),
        metric_prefix: default_prefix,
        mapping_label: "mapping".to_string(),
    };

    let Some(spec) = tool.manifest.metrics.as_ref() else {
        return default;
    };

    ResolvedMetricsSpec {
        endpoint: if spec.endpoint.trim().is_empty() {
            default.endpoint
        } else {
            spec.endpoint.clone()
        },
        format: spec.format.clone(),
        metric_prefix: spec
            .metric_prefix
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or(default.metric_prefix),
        mapping_label: if spec.mapping_label.trim().is_empty() {
            default.mapping_label
        } else {
            spec.mapping_label.clone()
        },
    }
}

fn default_metrics_port(tool_id: &str) -> u16 {
    match tool_id {
        "clickhouse" => 9991,
        "snowflake" => 9992,
        "postgres" => 9993,
        "databricks" => 9994,
        _ => 9999,
    }
}

fn parse_prometheus_text(raw: &str) -> Vec<MetricSample> {
    raw.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(parse_prometheus_sample_line)
        .collect()
}

fn parse_prometheus_sample_line(line: &str) -> Option<MetricSample> {
    let mut parts = line.split_whitespace();
    let metric = parts.next()?;
    let value = parts.next()?.parse::<f64>().ok()?;

    let (name, labels) = parse_metric_and_labels(metric);
    if name.is_empty() {
        return None;
    }

    Some(MetricSample {
        name,
        labels,
        value,
    })
}

fn parse_metric_and_labels(metric: &str) -> (String, HashMap<String, String>) {
    let mut labels = HashMap::new();

    let Some(open_idx) = metric.find('{') else {
        return (metric.to_string(), labels);
    };

    if !metric.ends_with('}') || open_idx + 1 >= metric.len() {
        return (metric.to_string(), labels);
    }

    let name = metric[..open_idx].to_string();
    let labels_raw = &metric[open_idx + 1..metric.len() - 1];

    for pair in labels_raw.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }

        let Some((k, v)) = pair.split_once('=') else {
            continue;
        };

        let key = k.trim().to_string();
        let value = v.trim().trim_matches('"').replace("\\\"", "\"");
        labels.insert(key, value);
    }

    (name, labels)
}
