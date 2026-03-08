use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::Json;
use tokio::process::Command;
use uuid::Uuid;

use crate::models::{
    ApiResult, GenerateScaffoldTemplateRequest, GenerateScaffoldTemplateResponse,
};
use crate::models::{bad_request, not_found};
use crate::models::AppState;

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

    let ext = if serde_json::from_str::<serde_json::Value>(&req.config_content).is_ok() {
        "json"
    } else {
        "yaml"
    };
    let temp_path = state
        .data_dir
        .join("configs")
        .join(format!("scaffold-{}.{}", Uuid::new_v4(), ext));

    tokio::fs::write(&temp_path, &req.config_content)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to write temp config: {e}"),
            )
        })?;

    let include_schema = req.include_schema_summary.unwrap_or(false);
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

    Ok(Json(GenerateScaffoldTemplateResponse {
        template_yaml,
        schema_summary,
    }))
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

fn supports_scaffold(tool_id: &str) -> bool {
    matches!(
        tool_id,
        "mysql"
            | "mariadb"
            | "sqlserver"
            | "postgres"
            | "snowflake"
            | "clickhouse"
            | "databricks"
    )
}
