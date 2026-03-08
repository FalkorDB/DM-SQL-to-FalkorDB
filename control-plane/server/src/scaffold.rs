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

fn extract_mapping_properties(mapping: &serde_yaml::Mapping) -> Vec<serde_json::Value> {
    let Some(properties) = mapping
        .get(YamlValue::String("properties".to_string()))
        .and_then(YamlValue::as_mapping)
    else {
        return vec![];
    };

    let mut out: Vec<(String, Option<String>)> = Vec::new();
    for (prop_key, prop_value) in properties {
        let Some(name) = prop_key.as_str() else {
            continue;
        };
        let column = prop_value
            .as_mapping()
            .and_then(|m| m.get(YamlValue::String("column".to_string())))
            .and_then(YamlValue::as_str)
            .map(std::string::ToString::to_string);

        out.push((name.to_string(), column));
    }

    out.sort_by(|a, b| a.0.cmp(&b.0));
    out.into_iter()
        .map(|(name, column)| serde_json::json!({ "name": name, "column": column }))
        .collect()
}

fn extract_match_on_fields(mapping: &serde_yaml::Mapping, side: &str) -> Vec<serde_json::Value> {
    let Some(match_on) = mapping
        .get(YamlValue::String(side.to_string()))
        .and_then(YamlValue::as_mapping)
        .and_then(|side_cfg| side_cfg.get(YamlValue::String("match_on".to_string())))
        .and_then(YamlValue::as_sequence)
    else {
        return vec![];
    };

    let mut out = Vec::new();
    for entry in match_on {
        let Some(entry_map) = entry.as_mapping() else {
            continue;
        };
        let column = entry_map
            .get(YamlValue::String("column".to_string()))
            .and_then(YamlValue::as_str)
            .map(std::string::ToString::to_string);
        let property = entry_map
            .get(YamlValue::String("property".to_string()))
            .and_then(YamlValue::as_str)
            .map(std::string::ToString::to_string);

        out.push(serde_json::json!({
            "column": column,
            "property": property,
        }));
    }

    out
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
        Ok((mut canvas_data, mut parse_warnings)) => {
            warnings.append(&mut parse_warnings);
            if !canvas_data.nodes.is_empty() || !canvas_data.links.is_empty() {
                if supports_scaffold(&tool.manifest.id) {
                    match run_scaffold_generation(&state, tool, &req.config_content, true).await {
                        Ok(scaffold) => maybe_enrich_canvas_data_from_schema_summary(
                            &mut canvas_data,
                            scaffold.schema_summary.as_deref(),
                            &mut warnings,
                        ),
                        Err((_, err)) => warnings
                            .push(format!("type enrichment skipped: failed schema introspection: {err}")),
                    }
                }
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
        match run_scaffold_generation(&state, tool, &req.config_content, true).await {
            Ok(scaffold) => {
                match build_canvas_data_from_config_content(&scaffold.template_yaml) {
                    Ok((mut canvas_data, mut parse_warnings)) => {
                        warnings
                        .push("using scaffold-template fallback because config mappings were unavailable".to_string());
                        warnings.append(&mut parse_warnings);
                        maybe_enrich_canvas_data_from_schema_summary(
                            &mut canvas_data,
                            scaffold.schema_summary.as_deref(),
                            &mut warnings,
                        );
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

fn maybe_enrich_canvas_data_from_schema_summary(
    canvas_data: &mut CanvasData,
    schema_summary: Option<&str>,
    warnings: &mut Vec<String>,
) {
    let Some(schema_summary) = schema_summary else {
        warnings.push(
            "schema summary was unavailable; SQL property types could not be resolved".to_string(),
        );
        return;
    };

    match build_table_column_type_index(schema_summary) {
        Ok(type_index) => enrich_canvas_data_with_graph_types(canvas_data, &type_index),
        Err(err) => {
            warnings.push(format!("failed to parse schema summary for graph types: {err}"));
        }
    }
}

fn build_table_column_type_index(
    schema_summary: &str,
) -> Result<HashMap<String, HashMap<String, String>>, String> {
    let parsed: YamlValue =
        serde_yaml::from_str(schema_summary).map_err(|e| format!("invalid schema summary: {e}"))?;
    let root = parsed
        .as_mapping()
        .ok_or_else(|| "schema summary root is not a mapping".to_string())?;
    let tables = root
        .get(YamlValue::String("tables".to_string()))
        .and_then(YamlValue::as_sequence)
        .ok_or_else(|| "schema summary missing 'tables' sequence".to_string())?;

    let mut table_index: HashMap<String, HashMap<String, String>> = HashMap::new();
    for table in tables {
        let Some(table_map) = table.as_mapping() else {
            continue;
        };

        let Some(table_name) = table_map
            .get(YamlValue::String("name".to_string()))
            .and_then(YamlValue::as_str)
            .map(normalize_identifier)
        else {
            continue;
        };

        let schema_name = table_map
            .get(YamlValue::String("schema".to_string()))
            .and_then(YamlValue::as_str)
            .map(normalize_identifier);

        let Some(columns) = table_map
            .get(YamlValue::String("columns".to_string()))
            .and_then(YamlValue::as_sequence)
        else {
            continue;
        };

        let mut column_index = HashMap::new();
        for column in columns {
            let Some(column_map) = column.as_mapping() else {
                continue;
            };
            let Some(column_name) = column_map
                .get(YamlValue::String("name".to_string()))
                .and_then(YamlValue::as_str)
            else {
                continue;
            };
            let Some(data_type) = column_map
                .get(YamlValue::String("data_type".to_string()))
                .and_then(YamlValue::as_str)
            else {
                continue;
            };
            column_index.insert(normalize_identifier(column_name), data_type.to_string());
        }

        if column_index.is_empty() {
            continue;
        }

        table_index.insert(table_name.clone(), column_index.clone());
        if let Some(schema_name) = schema_name {
            table_index.insert(format!("{schema_name}.{table_name}"), column_index);
        }
    }

    if table_index.is_empty() {
        return Err("schema summary did not contain typed table columns".to_string());
    }

    Ok(table_index)
}

fn enrich_canvas_data_with_graph_types(
    canvas_data: &mut CanvasData,
    table_index: &HashMap<String, HashMap<String, String>>,
) {
    let mut node_table_by_mapping = HashMap::new();
    for node in &canvas_data.nodes {
        let Some(node_data) = node.data.as_object() else {
            continue;
        };
        let Some(mapping_name) = node_data
            .get("mapping_name")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
        else {
            continue;
        };
        let Some(source_table) = node_data
            .get("source_table")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
        else {
            continue;
        };
        node_table_by_mapping.insert(mapping_name, source_table);
    }

    for node in &mut canvas_data.nodes {
        let Some(node_data) = node.data.as_object_mut() else {
            continue;
        };
        let source_table = node_data
            .get("source_table")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        let key_column = node_data
            .get("key_column")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        let Some(source_table) = source_table else {
            continue;
        };
        let Some(column_types) = lookup_table_columns(table_index, &source_table) else {
            continue;
        };

        if let Some(properties) = node_data.get_mut("properties") {
            enrich_properties_value(properties, column_types);
        }

        if let Some(key_column) = key_column {
            if let Some(source_type) = lookup_column_type(column_types, &key_column) {
                node_data.insert(
                    "key_type".to_string(),
                    serde_json::Value::String(map_source_type_to_graph_type(source_type).to_string()),
                );
            }
        }
    }

    for edge in &mut canvas_data.links {
        let Some(edge_data) = edge.data.as_object_mut() else {
            continue;
        };

        let edge_source_table = edge_data
            .get("source_table")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        if let Some(edge_source_table) = edge_source_table {
            if let Some(column_types) = lookup_table_columns(table_index, &edge_source_table) {
                if let Some(properties) = edge_data.get_mut("properties") {
                    enrich_properties_value(properties, column_types);
                }
            }
        }

        let from_mapping = edge_data
            .get("from_mapping")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        let to_mapping = edge_data
            .get("to_mapping")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);

        if let Some(from_mapping) = from_mapping {
            if let Some(source_table) = node_table_by_mapping.get(&from_mapping) {
                if let Some(column_types) = lookup_table_columns(table_index, source_table) {
                    if let Some(match_on) = edge_data.get_mut("from_match_on") {
                        enrich_match_on_value(match_on, column_types);
                    }
                }
            }
        }

        if let Some(to_mapping) = to_mapping {
            if let Some(source_table) = node_table_by_mapping.get(&to_mapping) {
                if let Some(column_types) = lookup_table_columns(table_index, source_table) {
                    if let Some(match_on) = edge_data.get_mut("to_match_on") {
                        enrich_match_on_value(match_on, column_types);
                    }
                }
            }
        }
    }
}

fn enrich_properties_value(value: &mut serde_json::Value, column_types: &HashMap<String, String>) {
    let Some(properties) = value.as_array_mut() else {
        return;
    };

    for property in properties {
        let Some(property_map) = property.as_object_mut() else {
            continue;
        };
        let column = property_map
            .get("column")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        let Some(column) = column else {
            continue;
        };

        if let Some(source_type) = lookup_column_type(column_types, &column) {
            property_map.insert(
                "graph_type".to_string(),
                serde_json::Value::String(map_source_type_to_graph_type(source_type).to_string()),
            );
        }
    }
}

fn enrich_match_on_value(value: &mut serde_json::Value, column_types: &HashMap<String, String>) {
    let Some(match_fields) = value.as_array_mut() else {
        return;
    };

    for entry in match_fields {
        let Some(entry_map) = entry.as_object_mut() else {
            continue;
        };
        let column = entry_map
            .get("column")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        let Some(column) = column else {
            continue;
        };

        if let Some(source_type) = lookup_column_type(column_types, &column) {
            entry_map.insert(
                "graph_type".to_string(),
                serde_json::Value::String(map_source_type_to_graph_type(source_type).to_string()),
            );
        }
    }
}

fn lookup_table_columns<'a>(
    table_index: &'a HashMap<String, HashMap<String, String>>,
    source_table: &str,
) -> Option<&'a HashMap<String, String>> {
    let normalized = normalize_table_name(source_table);
    if let Some(columns) = table_index.get(&normalized) {
        return Some(columns);
    }

    let parts: Vec<&str> = normalized.split('.').collect();
    if parts.len() >= 2 {
        let schema_and_table = format!("{}.{}", parts[parts.len() - 2], parts[parts.len() - 1]);
        if let Some(columns) = table_index.get(&schema_and_table) {
            return Some(columns);
        }
    }

    normalized
        .rsplit_once('.')
        .and_then(|(_, table_name)| table_index.get(table_name))
}

fn lookup_column_type<'a>(
    column_types: &'a HashMap<String, String>,
    column_name: &str,
) -> Option<&'a str> {
    column_types
        .get(&normalize_identifier(column_name))
        .map(String::as_str)
}

fn map_source_type_to_graph_type(source_type: &str) -> &'static str {
    let tokens = normalize_identifier(source_type)
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();

    if tokens
        .iter()
        .any(|token| token == "vector" || token == "array" || token.starts_with("vector"))
    {
        return "vector";
    }

    if tokens.iter().any(|token| {
        token == "bool" || token == "boolean" || token == "bit"
    }) {
        return "boolean";
    }

    if tokens.iter().any(|token| {
        token == "money"
            || token == "real"
            || token == "decimal"
            || token == "numeric"
            || token == "number"
            || token.starts_with("int")
            || token.ends_with("int")
            || token.starts_with("uint")
            || token.starts_with("float")
            || token.starts_with("double")
            || token.starts_with("serial")
    }) {
        return "number";
    }

    "string"
}

fn normalize_table_name(value: &str) -> String {
    normalize_identifier(value.split_whitespace().next().unwrap_or(value))
}

fn normalize_identifier(value: &str) -> String {
    value
        .trim()
        .replace('\"', "")
        .replace('`', "")
        .replace('[', "")
        .replace(']', "")
        .to_ascii_lowercase()
}
#[derive(Debug)]
struct PendingEdge {
    name: String,
    relationship: String,
    from_mapping: String,
    to_mapping: String,
    source_table: Option<String>,
    properties: Vec<serde_json::Value>,
    from_match_on: Vec<serde_json::Value>,
    to_match_on: Vec<serde_json::Value>,
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
                let key_property = get_nested_string_field(mapping, "key", "property")
                    .map(std::string::ToString::to_string);
                let properties = extract_mapping_properties(mapping);
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
                        "key_property": key_property,
                        "properties": properties,
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
                let source_table = get_nested_string_field(mapping, "source", "table")
                    .map(std::string::ToString::to_string);
                let properties = extract_mapping_properties(mapping);
                let from_match_on = extract_match_on_fields(mapping, "from");
                let to_match_on = extract_match_on_fields(mapping, "to");

                pending_edges.push(PendingEdge {
                    name: name.to_string(),
                    relationship,
                    from_mapping: from_mapping.to_string(),
                    to_mapping: to_mapping.to_string(),
                    source_table,
                    properties,
                    from_match_on,
                    to_match_on,
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
                "source_table": edge.source_table,
                "properties": edge.properties,
                "from_match_on": edge.from_match_on,
                "to_match_on": edge.to_match_on,
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
    properties:
      ordered_at:
        column: ordered_at
    from:
      node_mapping: orders
      match_on:
        - column: order_id
          property: id
    to:
      node_mapping: customers
      match_on:
        - column: customer_id
          property: id
"#;

        let (canvas, warnings) =
            build_canvas_data_from_config_content(config).expect("should parse");
        assert!(warnings.is_empty());
        assert_eq!(canvas.nodes.len(), 2);
        assert_eq!(canvas.links.len(), 1);
        assert_eq!(canvas.links[0].relationship, "PLACED_BY");
        assert_eq!(
            canvas.links[0]
                .data
                .get("properties")
                .and_then(serde_json::Value::as_array)
                .map(|v| v.len()),
            Some(1)
        );
        assert_eq!(
            canvas.links[0]
                .data
                .get("from_match_on")
                .and_then(serde_json::Value::as_array)
                .map(|v| v.len()),
            Some(1)
        );
        assert_eq!(
            canvas.links[0]
                .data
                .get("to_match_on")
                .and_then(serde_json::Value::as_array)
                .map(|v| v.len()),
            Some(1)
        );
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

    #[test]
    fn enriches_canvas_with_graph_types_from_schema_summary() {
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
    properties:
      name:
        column: customer_name
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
    source:
      table: public.orders
    properties:
      ordered_at:
        column: ordered_at
    from:
      node_mapping: orders
      match_on:
        - column: order_id
          property: id
    to:
      node_mapping: customers
      match_on:
        - column: customer_id
          property: id
"#;

        let schema_summary = r#"
database: app
tables:
  - schema: public
    name: customers
    columns:
      - name: customer_id
        data_type: integer
      - name: customer_name
        data_type: text
  - schema: public
    name: orders
    columns:
      - name: order_id
        data_type: bigint
      - name: customer_id
        data_type: integer
      - name: ordered_at
        data_type: timestamp without time zone
"#;

        let (mut canvas, warnings) =
            build_canvas_data_from_config_content(config).expect("should parse");
        assert!(warnings.is_empty());

        let type_index = build_table_column_type_index(schema_summary).expect("index should parse");
        enrich_canvas_data_with_graph_types(&mut canvas, &type_index);

        let customers_node = canvas
            .nodes
            .iter()
            .find(|n| n.data.get("mapping_name").and_then(serde_json::Value::as_str) == Some("customers"))
            .expect("customers node should exist");
        assert_eq!(
            customers_node.data.get("key_type").and_then(serde_json::Value::as_str),
            Some("number")
        );
        assert_eq!(
            customers_node
                .data
                .get("properties")
                .and_then(serde_json::Value::as_array)
                .and_then(|props| props.first())
                .and_then(|p| p.get("graph_type"))
                .and_then(serde_json::Value::as_str),
            Some("string")
        );

        let edge = canvas.links.first().expect("edge should exist");
        assert_eq!(
            edge.data
                .get("properties")
                .and_then(serde_json::Value::as_array)
                .and_then(|props| props.first())
                .and_then(|p| p.get("graph_type"))
                .and_then(serde_json::Value::as_str),
            Some("string")
        );
        assert_eq!(
            edge.data
                .get("from_match_on")
                .and_then(serde_json::Value::as_array)
                .and_then(|fields| fields.first())
                .and_then(|f| f.get("graph_type"))
                .and_then(serde_json::Value::as_str),
            Some("number")
        );
        assert_eq!(
            edge.data
                .get("to_match_on")
                .and_then(serde_json::Value::as_array)
                .and_then(|fields| fields.first())
                .and_then(|f| f.get("graph_type"))
                .and_then(serde_json::Value::as_str),
            Some("number")
        );
    }
}
