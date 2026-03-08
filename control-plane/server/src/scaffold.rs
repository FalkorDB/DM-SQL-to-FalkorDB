use std::collections::HashMap;

use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::Json;
use serde_yaml::Value as YamlValue;
use tokio::process::Command;
use uuid::Uuid;

use crate::models::{bad_request, not_found};
use crate::models::{
    ApiResult, AppState, CanvasData, CanvasLink, CanvasNode, GenerateScaffoldTemplateRequest,
    GenerateScaffoldTemplateResponse, GenerateSchemaGraphPreviewRequest,
    GenerateSchemaGraphPreviewResponse, SchemaGraphPreviewSource,
};
use crate::tools::Tool;

const TEMPLATE_MARKER: &str = "# Auto-generated template from source schema introspection.";

pub async fn generate_scaffold_template(
    State(state): State<AppState>,
    AxumPath(tool_id): AxumPath<String>,
    Json(req): Json<GenerateScaffoldTemplateRequest>,
) -> ApiResult<Json<GenerateScaffoldTemplateResponse>> {
    let Some(tool) = state.tools.get(&tool_id) else {
        return not_found();
    };
    if !supports_scaffold(&tool.manifest.id) {
        return bad_request(format!(
            "tool '{}' does not support scaffold template generation",
            tool.manifest.id
        ));
    }
    if req.config_content.trim().is_empty() {
        return bad_request("config_content must not be empty".to_string());
    }
    let include_schema = req.include_schema_summary.unwrap_or(false);
    let response =
        run_scaffold_generation(&state, tool, &req.config_content, include_schema).await?;
    Ok(Json(response))
}

pub async fn generate_schema_graph_preview(
    State(state): State<AppState>,
    AxumPath(tool_id): AxumPath<String>,
    Json(req): Json<GenerateSchemaGraphPreviewRequest>,
) -> ApiResult<Json<GenerateSchemaGraphPreviewResponse>> {
    let Some(tool) = state.tools.get(&tool_id) else {
        return not_found();
    };
    if req.config_content.trim().is_empty() {
        return bad_request("config_content must not be empty".to_string());
    }

    let mut warnings = Vec::new();
    match build_canvas_data_from_config_content(&req.config_content) {
        Ok((canvas_data, mut parse_warnings)) => {
            warnings.append(&mut parse_warnings);
            if !canvas_data.nodes.is_empty() || !canvas_data.links.is_empty() {
                return Ok(Json(GenerateSchemaGraphPreviewResponse {
                    canvas_data,
                    warnings,
                    source: SchemaGraphPreviewSource::Config,
                }));
            }
            warnings.push(
                "config mappings are present but no graph entities were inferred".to_string(),
            );
        }
        Err(parse_err) => {
            warnings.push(format!("failed to parse config mappings: {parse_err}"));
        }
    }

    if supports_scaffold(&tool.manifest.id) {
        match run_scaffold_generation(&state, tool, &req.config_content, false).await {
            Ok(scaffold) => {
                match build_canvas_data_from_config_content(&scaffold.template_yaml) {
                    Ok((canvas_data, mut parse_warnings)) => {
                        warnings
                        .push("using scaffold-template fallback because config mappings were unavailable".to_string());
                        warnings.append(&mut parse_warnings);
                        if !canvas_data.nodes.is_empty() || !canvas_data.links.is_empty() {
                            return Ok(Json(GenerateSchemaGraphPreviewResponse {
                                canvas_data,
                                warnings,
                                source: SchemaGraphPreviewSource::Template,
                            }));
                        }
                    }
                    Err(err) => {
                        warnings.push(format!("failed to parse scaffold template mappings: {err}"));
                    }
                }
            }
            Err((_, err)) => warnings.push(format!("scaffold fallback failed: {err}")),
        }
    }

    Ok(Json(GenerateSchemaGraphPreviewResponse {
        canvas_data: CanvasData {
            nodes: vec![],
            links: vec![],
        },
        warnings,
        source: SchemaGraphPreviewSource::Config,
    }))
}

async fn run_scaffold_generation(
    state: &AppState,
    tool: &Tool,
    config_content: &str,
    include_schema: bool,
) -> ApiResult<GenerateScaffoldTemplateResponse> {
    let ext = if serde_json::from_str::<serde_json::Value>(config_content).is_ok() {
        "json"
    } else {
        "yaml"
    };
    let temp_path =
        state
            .data_dir
            .join("configs")
            .join(format!("scaffold-{}.{}", Uuid::new_v4(), ext));
    tokio::fs::write(&temp_path, config_content)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to write temp config: {e}"),
            )
        })?;

    let (program, mut args, cwd) = tool
        .executable_command(&state.repo_root)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    args.push("--".to_string());
    args.push("--config".to_string());
    args.push(temp_path.to_string_lossy().to_string());
    if include_schema {
        args.push("--introspect-schema".to_string());
    }
    args.push("--generate-template".to_string());

    let out = Command::new(&program)
        .args(&args)
        .current_dir(cwd)
        .output()
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to run scaffold command: {e}"),
            )
        })?;

    let _ = tokio::fs::remove_file(&temp_path).await;

    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    if !out.status.success() {
        let msg = if stderr.trim().is_empty() {
            stdout.trim().to_string()
        } else {
            stderr.trim().to_string()
        };
        return bad_request(format!("scaffold generation failed: {}", msg));
    }

    let (schema_summary, template_yaml) = split_scaffold_output(&stdout, include_schema);
    if template_yaml.trim().is_empty() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "scaffold command did not return template output".to_string(),
        ));
    }

    Ok(GenerateScaffoldTemplateResponse {
        template_yaml,
        schema_summary,
    })
}

fn split_scaffold_output(stdout: &str, include_schema: bool) -> (Option<String>, String) {
    if let Some(marker_index) = stdout.find(TEMPLATE_MARKER) {
        let schema_raw = stdout[..marker_index].trim().to_string();
        let template = stdout[marker_index..].trim().to_string();
        let schema_summary = if include_schema && !schema_raw.is_empty() {
            Some(schema_raw)
        } else {
            None
        };
        return (schema_summary, template);
    }

    (None, stdout.trim().to_string())
}
#[derive(Debug)]
struct PendingEdge {
    name: String,
    relationship: String,
    from_mapping: String,
    to_mapping: String,
}

fn build_canvas_data_from_config_content(
    config_content: &str,
) -> Result<(CanvasData, Vec<String>), String> {
    let parsed: YamlValue = serde_yaml::from_str(config_content)
        .map_err(|e| format!("invalid YAML/JSON config content: {e}"))?;
    let root = parsed
        .as_mapping()
        .ok_or_else(|| "config content must be a mapping/object".to_string())?;
    let mappings = root
        .get(YamlValue::String("mappings".to_string()))
        .and_then(YamlValue::as_sequence)
        .ok_or_else(|| "config does not contain a valid 'mappings' sequence".to_string())?;

    let palette = [
        "#4E79A7", "#59A14F", "#F28E2B", "#E15759", "#B07AA1", "#76B7B2",
    ];

    let mut warnings = Vec::new();
    let mut nodes = Vec::new();
    let mut links = Vec::new();
    let mut node_ids_by_mapping: HashMap<String, u64> = HashMap::new();
    let mut pending_edges = Vec::new();
    let mut next_node_id: u64 = 1;
    let mut next_edge_id: u64 = 1;

    for (index, mapping_value) in mappings.iter().enumerate() {
        let Some(mapping) = mapping_value.as_mapping() else {
            warnings.push(format!("mapping #{index} is not an object and was skipped"));
            continue;
        };

        let mapping_type = get_string_field(mapping, "type")
            .map(str::to_ascii_lowercase)
            .unwrap_or_default();
        let Some(name) = get_string_field(mapping, "name") else {
            warnings.push(format!(
                "mapping #{index} is missing 'name' and was skipped"
            ));
            continue;
        };

        match mapping_type.as_str() {
            "node" => {
                if node_ids_by_mapping.contains_key(name) {
                    warnings.push(format!("duplicate node mapping '{name}' was skipped"));
                    continue;
                }

                let labels = get_labels(mapping, name);
                let source_table = get_nested_string_field(mapping, "source", "table")
                    .map(std::string::ToString::to_string);
                let key_column = get_nested_string_field(mapping, "key", "column")
                    .map(std::string::ToString::to_string);
                let node_id = next_node_id;
                next_node_id += 1;
                node_ids_by_mapping.insert(name.to_string(), node_id);
                let primary_label = labels.first().cloned();

                nodes.push(CanvasNode {
                    id: node_id,
                    labels,
                    color: palette[(node_id as usize - 1) % palette.len()].to_string(),
                    visible: true,
                    caption: Some("mapping_name".to_string()),
                    data: serde_json::json!({
                        "kind": "node",
                        "node_label": primary_label,
                        "mapping_name": name,
                        "source_table": source_table,
                        "key_column": key_column,
                    }),
                });
            }
            "edge" => {
                let relationship = get_string_field(mapping, "relationship")
                    .map(std::string::ToString::to_string)
                    .unwrap_or_else(|| infer_relationship_from_name(name));
                let Some(from_mapping) = get_nested_string_field(mapping, "from", "node_mapping")
                else {
                    warnings.push(format!(
                        "edge mapping '{name}' is missing 'from.node_mapping' and was skipped"
                    ));
                    continue;
                };
                let Some(to_mapping) = get_nested_string_field(mapping, "to", "node_mapping")
                else {
                    warnings.push(format!(
                        "edge mapping '{name}' is missing 'to.node_mapping' and was skipped"
                    ));
                    continue;
                };

                pending_edges.push(PendingEdge {
                    name: name.to_string(),
                    relationship,
                    from_mapping: from_mapping.to_string(),
                    to_mapping: to_mapping.to_string(),
                });
            }
            _ => {
                warnings.push(format!(
                    "mapping '{name}' has unsupported type '{}' and was skipped",
                    if mapping_type.is_empty() {
                        "<missing>"
                    } else {
                        &mapping_type
                    }
                ));
            }
        }
    }

    for edge in pending_edges {
        let Some(source_node_id) = node_ids_by_mapping.get(&edge.from_mapping).copied() else {
            warnings.push(format!(
                "edge '{}' references missing from node mapping '{}'",
                edge.name, edge.from_mapping
            ));
            continue;
        };
        let Some(target_node_id) = node_ids_by_mapping.get(&edge.to_mapping).copied() else {
            warnings.push(format!(
                "edge '{}' references missing to node mapping '{}'",
                edge.name, edge.to_mapping
            ));
            continue;
        };

        links.push(CanvasLink {
            id: next_edge_id,
            relationship: edge.relationship,
            color: "#9CA3AF".to_string(),
            source: source_node_id,
            target: target_node_id,
            visible: true,
            data: serde_json::json!({
                "kind": "edge",
                "mapping_name": edge.name,
                "from_mapping": edge.from_mapping,
                "to_mapping": edge.to_mapping,
            }),
        });
        next_edge_id += 1;
    }

    Ok((CanvasData { nodes, links }, warnings))
}

fn get_string_field<'a>(mapping: &'a serde_yaml::Mapping, key: &str) -> Option<&'a str> {
    mapping
        .get(YamlValue::String(key.to_string()))
        .and_then(YamlValue::as_str)
}

fn get_nested_string_field<'a>(
    mapping: &'a serde_yaml::Mapping,
    parent_key: &str,
    child_key: &str,
) -> Option<&'a str> {
    mapping
        .get(YamlValue::String(parent_key.to_string()))
        .and_then(YamlValue::as_mapping)
        .and_then(|child| child.get(YamlValue::String(child_key.to_string())))
        .and_then(YamlValue::as_str)
}

fn get_labels(mapping: &serde_yaml::Mapping, fallback_name: &str) -> Vec<String> {
    if let Some(labels) = mapping
        .get(YamlValue::String("labels".to_string()))
        .and_then(YamlValue::as_sequence)
    {
        let parsed = labels
            .iter()
            .filter_map(YamlValue::as_str)
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>();
        if !parsed.is_empty() {
            return parsed;
        }
    }

    vec![fallback_name.to_string()]
}

fn infer_relationship_from_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
}

fn supports_scaffold(tool_id: &str) -> bool {
    matches!(
        tool_id,
        "mysql" | "mariadb" | "sqlserver" | "postgres" | "snowflake" | "clickhouse" | "databricks"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_canvas_from_config_mappings() {
        let config = r#"
mappings:
  - type: node
    name: customers
    labels: [Customer]
    source:
      table: public.customers
    key:
      column: customer_id
      property: id
  - type: node
    name: orders
    labels: [Order]
    source:
      table: public.orders
    key:
      column: order_id
      property: id
  - type: edge
    name: orders_customer
    relationship: PLACED_BY
    from:
      node_mapping: orders
    to:
      node_mapping: customers
"#;

        let (canvas, warnings) =
            build_canvas_data_from_config_content(config).expect("should parse");
        assert!(warnings.is_empty());
        assert_eq!(canvas.nodes.len(), 2);
        assert_eq!(canvas.links.len(), 1);
        assert_eq!(canvas.links[0].relationship, "PLACED_BY");
    }

    #[test]
    fn warns_on_dangling_edge_endpoints() {
        let config = r#"
mappings:
  - type: node
    name: customers
    labels: [Customer]
    key:
      column: customer_id
      property: id
  - type: edge
    name: missing_ref
    from:
      node_mapping: customers
    to:
      node_mapping: orders
"#;

        let (canvas, warnings) =
            build_canvas_data_from_config_content(config).expect("should parse");
        assert_eq!(canvas.nodes.len(), 1);
        assert_eq!(canvas.links.len(), 0);
        assert!(warnings
            .iter()
            .any(|w| w.contains("references missing to node mapping")));
    }
}
