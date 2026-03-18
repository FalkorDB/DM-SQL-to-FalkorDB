use std::fs;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use tokio::time::sleep;
use yup_oauth2::{read_service_account_key, ServiceAccountAuthenticator};

use crate::config::{BigQueryConfig, CommonMappingFields, Config, Mode};

const BIGQUERY_SCOPE: &str = "https://www.googleapis.com/auth/bigquery";
const BIGQUERY_API_BASE: &str = "https://bigquery.googleapis.com/bigquery/v2";
const POLL_INTERVAL_MS: u64 = 500;
const MAX_POLL_ATTEMPTS: usize = 240;
const DEFAULT_TIMEOUT_MS: u64 = 60_000;

/// Logical representation of a single row coming from BigQuery or a file source.
pub type LogicalRow = JsonMap<String, JsonValue>;

#[derive(Debug, Deserialize, Clone)]
struct JobReference {
    #[serde(rename = "projectId")]
    project_id: Option<String>,
    #[serde(rename = "jobId")]
    job_id: String,
    location: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct BigQuerySchema {
    #[serde(default)]
    fields: Vec<BigQueryField>,
}

#[derive(Debug, Deserialize, Clone)]
struct BigQueryField {
    name: String,
    #[serde(rename = "type")]
    field_type: String,
    mode: Option<String>,
    #[serde(default)]
    fields: Vec<BigQueryField>,
}

#[derive(Debug, Deserialize, Clone)]
struct BigQueryCell {
    #[serde(rename = "v")]
    value: JsonValue,
}

#[derive(Debug, Deserialize, Clone)]
struct BigQueryRow {
    #[serde(rename = "f", default)]
    cells: Vec<BigQueryCell>,
}

#[derive(Debug, Deserialize, Clone)]
struct BigQueryError {
    message: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct QueryResponse {
    #[serde(rename = "jobComplete", default)]
    job_complete: bool,
    #[serde(rename = "jobReference")]
    job_reference: Option<JobReference>,
    #[serde(default)]
    schema: Option<BigQuerySchema>,
    #[serde(default)]
    rows: Vec<BigQueryRow>,
    #[serde(rename = "pageToken")]
    page_token: Option<String>,
    #[serde(rename = "errorResult")]
    error_result: Option<BigQueryError>,
    #[serde(default)]
    errors: Vec<BigQueryError>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct JobsQueryRequest<'a> {
    query: &'a str,
    use_legacy_sql: bool,
    max_results: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<&'a str>,
}

struct BigQueryClient {
    http: Client,
    cfg: BigQueryConfig,
}

impl BigQueryClient {
    fn new(cfg: &BigQueryConfig) -> Result<Self> {
        let timeout = Duration::from_millis(cfg.query_timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS));
        let http = Client::builder().timeout(timeout).build()?;
        Ok(Self {
            http,
            cfg: cfg.clone(),
        })
    }

    async fn execute_query(&self, statement: &str) -> Result<Vec<LogicalRow>> {
        let token = self.resolve_access_token().await?;
        let mut resp = self.jobs_query(statement, &token).await?;
        self.raise_if_response_failed(&resp)?;

        let mut job_ref = resp.job_reference.clone().ok_or_else(|| {
            anyhow!("BigQuery response is missing jobReference (cannot continue polling)")
        })?;

        if !resp.job_complete {
            let mut attempts = 0usize;
            while !resp.job_complete {
                if attempts >= MAX_POLL_ATTEMPTS {
                    return Err(anyhow!(
                        "BigQuery query did not complete after {} polling attempts",
                        MAX_POLL_ATTEMPTS
                    ));
                }
                sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
                resp = self
                    .get_query_results(&job_ref, &token, None)
                    .await
                    .with_context(|| "Polling BigQuery query results failed")?;
                self.raise_if_response_failed(&resp)?;
                if let Some(r) = &resp.job_reference {
                    job_ref = r.clone();
                }
                attempts += 1;
            }
        }

        let mut schema = resp.schema.clone();
        let mut rows = decode_rows(&resp.rows, schema.as_ref())?;
        let mut page_token = resp.page_token.clone();

        while let Some(next_page_token) = page_token {
            let page = self
                .get_query_results(&job_ref, &token, Some(next_page_token.as_str()))
                .await?;
            self.raise_if_response_failed(&page)?;
            if schema.is_none() {
                schema = page.schema.clone();
            }
            rows.extend(decode_rows(&page.rows, schema.as_ref())?);
            page_token = page.page_token;
        }

        Ok(rows)
    }

    async fn resolve_access_token(&self) -> Result<String> {
        if let Some(token) = &self.cfg.access_token {
            if !token.trim().is_empty() {
                return Ok(token.trim().to_string());
            }
        }

        if let Some(path) = &self.cfg.service_account_key_path {
            let key = read_service_account_key(path)
                .await
                .with_context(|| format!("Failed to read service account key from {}", path))?;
            let auth = ServiceAccountAuthenticator::builder(key)
                .build()
                .await
                .context("Failed to initialize service account authenticator")?;
            let token = auth
                .token(&[BIGQUERY_SCOPE])
                .await
                .context("Failed to fetch OAuth token for BigQuery scope")?;
            let access_token = token.token().ok_or_else(|| {
                anyhow!("OAuth token response did not contain an access token string")
            })?;
            return Ok(access_token.to_string());
        }

        Err(anyhow!(
            "BigQuery authentication not configured: set bigquery.access_token or bigquery.service_account_key_path"
        ))
    }

    async fn jobs_query(&self, statement: &str, token: &str) -> Result<QueryResponse> {
        let url = format!(
            "{}/projects/{}/queries",
            BIGQUERY_API_BASE, self.cfg.project_id
        );
        let body = JobsQueryRequest {
            query: statement,
            use_legacy_sql: false,
            max_results: 10_000,
            timeout_ms: self.cfg.query_timeout_ms,
            location: self.cfg.location.as_deref(),
        };

        let resp = self
            .http
            .post(url)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .context("Failed to call BigQuery jobs.query")?;

        deserialize_response(resp, "BigQuery jobs.query")
            .await
            .context("Failed to decode BigQuery jobs.query response")
    }

    async fn get_query_results(
        &self,
        job_ref: &JobReference,
        token: &str,
        page_token: Option<&str>,
    ) -> Result<QueryResponse> {
        let project_id = job_ref
            .project_id
            .as_deref()
            .unwrap_or(self.cfg.project_id.as_str());
        let url = format!(
            "{}/projects/{}/queries/{}",
            BIGQUERY_API_BASE, project_id, job_ref.job_id
        );

        let mut req = self.http.get(url).bearer_auth(token);
        let location = job_ref
            .location
            .as_deref()
            .or(self.cfg.location.as_deref())
            .unwrap_or("US");
        req = req.query(&[("location", location), ("maxResults", "10000")]);
        if let Some(page) = page_token {
            req = req.query(&[("pageToken", page)]);
        }

        let resp = req
            .send()
            .await
            .context("Failed to call BigQuery jobs.getQueryResults")?;

        deserialize_response(resp, "BigQuery jobs.getQueryResults")
            .await
            .context("Failed to decode BigQuery jobs.getQueryResults response")
    }

    fn raise_if_response_failed(&self, resp: &QueryResponse) -> Result<()> {
        if let Some(err) = &resp.error_result {
            return Err(anyhow!(
                "BigQuery query failed: {}",
                err.message
                    .clone()
                    .unwrap_or_else(|| "unknown error".to_string())
            ));
        }
        if !resp.errors.is_empty() {
            let message = resp
                .errors
                .iter()
                .filter_map(|e| e.message.clone())
                .collect::<Vec<_>>()
                .join("; ");
            if !message.is_empty() {
                return Err(anyhow!("BigQuery query returned errors: {}", message));
            }
        }
        Ok(())
    }
}

async fn deserialize_response(resp: reqwest::Response, label: &str) -> Result<QueryResponse> {
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("{} failed with status {}: {}", label, status, body));
    }

    let parsed: QueryResponse = resp.json().await?;
    Ok(parsed)
}

fn decode_rows(rows: &[BigQueryRow], schema: Option<&BigQuerySchema>) -> Result<Vec<LogicalRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let schema = schema.ok_or_else(|| anyhow!("BigQuery response contains rows but no schema"))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(decode_row(row, &schema.fields)?);
    }
    Ok(out)
}

fn decode_row(row: &BigQueryRow, fields: &[BigQueryField]) -> Result<LogicalRow> {
    let mut obj = JsonMap::new();
    for (idx, field) in fields.iter().enumerate() {
        let raw = row
            .cells
            .get(idx)
            .map(|c| c.value.clone())
            .unwrap_or(JsonValue::Null);
        let decoded = decode_cell_value(&raw, field)?;
        obj.insert(field.name.clone(), decoded);
    }
    Ok(obj)
}

fn decode_cell_value(raw: &JsonValue, field: &BigQueryField) -> Result<JsonValue> {
    if raw.is_null() {
        return Ok(JsonValue::Null);
    }

    if field
        .mode
        .as_deref()
        .unwrap_or("NULLABLE")
        .eq_ignore_ascii_case("REPEATED")
    {
        let arr = raw
            .as_array()
            .ok_or_else(|| anyhow!("Expected repeated value array for '{}'", field.name))?;
        let mut elem_field = field.clone();
        elem_field.mode = Some("NULLABLE".to_string());
        let mut out = Vec::with_capacity(arr.len());
        for item in arr {
            let inner = item.get("v").unwrap_or(item);
            out.push(decode_cell_value(inner, &elem_field)?);
        }
        return Ok(JsonValue::Array(out));
    }

    if field.field_type.eq_ignore_ascii_case("RECORD")
        || field.field_type.eq_ignore_ascii_case("STRUCT")
    {
        let nested = raw
            .get("f")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("Expected RECORD object with 'f' for '{}'", field.name))?;
        let mut obj = JsonMap::new();
        for (idx, nested_field) in field.fields.iter().enumerate() {
            let inner = nested
                .get(idx)
                .and_then(|v| v.get("v"))
                .cloned()
                .unwrap_or(JsonValue::Null);
            obj.insert(
                nested_field.name.clone(),
                decode_cell_value(&inner, nested_field)?,
            );
        }
        return Ok(JsonValue::Object(obj));
    }

    decode_scalar_value(raw, field)
}

fn decode_scalar_value(raw: &JsonValue, field: &BigQueryField) -> Result<JsonValue> {
    let Some(s) = raw.as_str() else {
        return Ok(raw.clone());
    };

    let out = match field.field_type.to_ascii_uppercase().as_str() {
        "BOOL" | "BOOLEAN" => match s {
            "true" | "TRUE" | "1" => JsonValue::Bool(true),
            "false" | "FALSE" | "0" => JsonValue::Bool(false),
            _ => JsonValue::String(s.to_string()),
        },
        "INT64" | "INTEGER" => s
            .parse::<i64>()
            .map(JsonValue::from)
            .unwrap_or_else(|_| JsonValue::String(s.to_string())),
        "FLOAT64" | "FLOAT" => s
            .parse::<f64>()
            .ok()
            .and_then(JsonNumber::from_f64)
            .map(JsonValue::Number)
            .unwrap_or_else(|| JsonValue::String(s.to_string())),
        "JSON" => serde_json::from_str::<JsonValue>(s).unwrap_or(JsonValue::String(s.to_string())),
        // Keep arbitrary precision numeric types as strings to avoid precision loss.
        "NUMERIC" | "BIGNUMERIC" | "DECIMAL" | "BIGDECIMAL" => JsonValue::String(s.to_string()),
        _ => JsonValue::String(s.to_string()),
    };

    Ok(out)
}

pub async fn query_bigquery_sql(cfg: &BigQueryConfig, statement: &str) -> Result<Vec<LogicalRow>> {
    let client = BigQueryClient::new(cfg)?;
    client.execute_query(statement).await
}

/// Fetch rows for a given mapping, using either a local JSON file or BigQuery SQL.
///
/// Watermark (if provided) is used only when the mapping is in incremental mode and
/// has a delta.updated_at_column configured.
pub async fn fetch_rows_for_mapping(
    cfg: &Config,
    mapping: &CommonMappingFields,
    watermark: Option<&str>,
) -> Result<Vec<LogicalRow>> {
    if let Some(path) = &mapping.source.file {
        let data = fs::read_to_string(path)
            .with_context(|| format!("Failed to read input file {}", path))?;
        let value: JsonValue = serde_json::from_str(&data)
            .with_context(|| format!("Failed to parse JSON array from {}", path))?;
        let arr = value.as_array().ok_or_else(|| {
            anyhow!(
                "Expected JSON array of objects in file {} for mapping {}",
                path,
                mapping.name
            )
        })?;

        let mut rows = Vec::with_capacity(arr.len());
        for (idx, v) in arr.iter().enumerate() {
            let obj = v.as_object().ok_or_else(|| {
                anyhow!(
                    "Element {} in file {} for mapping {} is not a JSON object",
                    idx,
                    path,
                    mapping.name
                )
            })?;
            rows.push(obj.clone());
        }
        return Ok(rows);
    }

    if let Some(bq_cfg) = cfg.bigquery.as_ref() {
        if mapping.source.table.is_some() || mapping.source.select.is_some() {
            let sql = build_sql_for_mapping(bq_cfg, mapping, watermark);
            return query_bigquery_sql(bq_cfg, &sql).await;
        }
    }

    Ok(Vec::new())
}

fn build_sql_for_mapping(
    bq_cfg: &BigQueryConfig,
    mapping: &CommonMappingFields,
    watermark: Option<&str>,
) -> String {
    let mut predicates: Vec<String> = Vec::new();

    if let Some(w) = &mapping.source.r#where {
        predicates.push(w.clone());
    }

    if let (Mode::Incremental, Some(delta), Some(wm)) =
        (&mapping.mode, mapping.delta.as_ref(), watermark)
    {
        let escaped = wm.replace('\'', "''");
        predicates.push(format!("{} > '{}'", delta.updated_at_column, escaped));
    }

    let combined = if predicates.is_empty() {
        None
    } else {
        Some(predicates.join(" AND "))
    };

    if let Some(select) = &mapping.source.select {
        if let Some(cond) = combined {
            format!("SELECT * FROM ({}) AS s WHERE {}", select, cond)
        } else {
            select.clone()
        }
    } else if let Some(table) = &mapping.source.table {
        let qualified = qualify_source_table(bq_cfg, table);
        if let Some(cond) = combined {
            format!("SELECT * FROM {} WHERE {}", qualified, cond)
        } else {
            format!("SELECT * FROM {}", qualified)
        }
    } else {
        "SELECT 1".to_string()
    }
}

fn qualify_source_table(cfg: &BigQueryConfig, table: &str) -> String {
    let trimmed = table.trim();
    if trimmed.contains('`') {
        return trimmed.to_string();
    }

    let parts = trimmed.split('.').collect::<Vec<_>>();
    let fully_qualified = match parts.as_slice() {
        [table] => format!("{}.{}.{}", cfg.project_id, cfg.dataset, table),
        [dataset, table] => format!("{}.{}.{}", cfg.project_id, dataset, table),
        [project, dataset, table] => format!("{}.{}.{}", project, dataset, table),
        _ => trimmed.to_string(),
    };

    format!("`{}`", fully_qualified.replace('`', ""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualify_source_table_expands_unqualified_names() {
        let cfg = BigQueryConfig {
            project_id: "my-project".to_string(),
            dataset: "analytics".to_string(),
            location: Some("US".to_string()),
            access_token: None,
            service_account_key_path: None,
            query_timeout_ms: None,
        };
        assert_eq!(
            qualify_source_table(&cfg, "orders"),
            "`my-project.analytics.orders`"
        );
        assert_eq!(
            qualify_source_table(&cfg, "sales.orders"),
            "`my-project.sales.orders`"
        );
        assert_eq!(
            qualify_source_table(&cfg, "p1.sales.orders"),
            "`p1.sales.orders`"
        );
    }

    /// Optional BigQuery connectivity smoke test.
    ///
    /// Uses environment variables:
    /// - BIGQUERY_PROJECT_ID
    /// - BIGQUERY_DATASET
    /// and one of:
    /// - BIGQUERY_ACCESS_TOKEN
    /// - BIGQUERY_SERVICE_ACCOUNT_KEY_PATH
    ///
    /// If these are missing, the test is a no-op and returns Ok(()).
    #[tokio::test]
    async fn bigquery_connectivity_smoke_test() -> Result<()> {
        let project_id = match std::env::var("BIGQUERY_PROJECT_ID") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        let dataset = match std::env::var("BIGQUERY_DATASET") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        let access_token = std::env::var("BIGQUERY_ACCESS_TOKEN").ok();
        let service_account_key_path = std::env::var("BIGQUERY_SERVICE_ACCOUNT_KEY_PATH").ok();
        if access_token.is_none() && service_account_key_path.is_none() {
            return Ok(());
        }

        let cfg = BigQueryConfig {
            project_id,
            dataset,
            location: std::env::var("BIGQUERY_LOCATION").ok(),
            access_token,
            service_account_key_path,
            query_timeout_ms: Some(30_000),
        };

        let rows = query_bigquery_sql(&cfg, "SELECT 1 AS one").await?;
        if let Some(row) = rows.first() {
            if let Some(v) = row.get("one").or_else(|| row.get("ONE")) {
                assert!(v.is_number() || v.is_string());
            }
        }
        Ok(())
    }
}
