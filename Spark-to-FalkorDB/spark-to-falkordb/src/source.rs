use std::fs;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::{Client, StatusCode};
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use tokio::time::sleep;

use crate::config::{
    CommonMappingFields, Config, Mode, SchemaStrategy, SourceConfig, SourcePartitionConfig,
    SparkConfig,
};

const DEFAULT_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_POLL_INTERVAL_MS: u64 = 300;
const DEFAULT_MAX_POLL_ATTEMPTS: u32 = 400;
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_BACKOFF_MS: u64 = 250;
const DEFAULT_RETRY_BACKOFF_MAX_MS: u64 = 5_000;
const DEFAULT_FLATTEN_MAX_DEPTH: u8 = 2;
const MAX_ERROR_BODY_CHARS: usize = 512;

/// Logical representation of a single row coming from Spark or a file source.
pub type LogicalRow = JsonMap<String, JsonValue>;

/// Minimal Livy client for submitting and polling Spark SQL statements.
struct SparkClient {
    client: Client,
    base_url: String,
    session_id: u64,
    statement_kind: String,
    auth_token: Option<String>,
    poll_interval: Duration,
    max_poll_attempts: u32,
    max_retries: u32,
    retry_backoff: Duration,
    retry_backoff_max: Duration,
}

impl SparkClient {
    fn new(cfg: &SparkConfig) -> Result<Self> {
        let base_url = cfg.livy_url.trim_end_matches('/').to_string();
        if base_url.is_empty() {
            return Err(anyhow!("spark.livy_url must be set"));
        }

        let timeout = Duration::from_millis(cfg.query_timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS));
        let client = Client::builder().timeout(timeout).build()?;

        Ok(Self {
            client,
            base_url,
            session_id: cfg.session_id,
            statement_kind: cfg.statement_kind.clone(),
            auth_token: cfg.auth_token.clone(),
            poll_interval: Duration::from_millis(
                cfg.poll_interval_ms.unwrap_or(DEFAULT_POLL_INTERVAL_MS),
            ),
            max_poll_attempts: cfg.max_poll_attempts.unwrap_or(DEFAULT_MAX_POLL_ATTEMPTS),
            max_retries: cfg.max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
            retry_backoff: Duration::from_millis(
                cfg.retry_backoff_ms.unwrap_or(DEFAULT_RETRY_BACKOFF_MS),
            ),
            retry_backoff_max: Duration::from_millis(
                cfg.retry_backoff_max_ms
                    .unwrap_or(DEFAULT_RETRY_BACKOFF_MAX_MS),
            ),
        })
    }

    fn with_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(token) = &self.auth_token {
            request.bearer_auth(token)
        } else {
            request
        }
    }

    async fn send_json_with_retry<F>(&self, operation: &str, mut build_request: F) -> Result<JsonValue>
    where
        F: FnMut() -> reqwest::RequestBuilder,
    {
        let mut retries_used = 0u32;

        loop {
            let response_result = self.with_auth(build_request()).send().await;
            match response_result {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    if status.is_success() {
                        return serde_json::from_str::<JsonValue>(&body).with_context(|| {
                            format!(
                                "Spark Livy {} returned non-JSON response: {}",
                                operation,
                                truncate_for_error(&body)
                            )
                        });
                    }

                    let classification = classify_http_status(status);
                    if should_retry_http_status(status) && retries_used < self.max_retries {
                        retries_used += 1;
                        let delay = retry_delay_for_attempt(
                            retries_used,
                            self.retry_backoff,
                            self.retry_backoff_max,
                        );
                        tracing::warn!(
                            operation = %operation,
                            status = %status,
                            classification = %classification,
                            retry = retries_used,
                            max_retries = self.max_retries,
                            delay_ms = delay.as_millis(),
                            "Spark Livy request failed with transient HTTP status, retrying"
                        );
                        sleep(delay).await;
                        continue;
                    }

                    return Err(anyhow!(
                        "Spark Livy {} failed (classification={}, status={}): {}",
                        operation,
                        classification,
                        status,
                        truncate_for_error(&body)
                    ));
                }
                Err(err) => {
                    let classification = classify_reqwest_error(&err);
                    if should_retry_reqwest_error(&err) && retries_used < self.max_retries {
                        retries_used += 1;
                        let delay = retry_delay_for_attempt(
                            retries_used,
                            self.retry_backoff,
                            self.retry_backoff_max,
                        );
                        tracing::warn!(
                            operation = %operation,
                            classification = %classification,
                            retry = retries_used,
                            max_retries = self.max_retries,
                            delay_ms = delay.as_millis(),
                            error = %err,
                            "Spark Livy request failed with transient transport error, retrying"
                        );
                        sleep(delay).await;
                        continue;
                    }

                    return Err(anyhow!(
                        "Spark Livy {} failed (classification={}): {}",
                        operation,
                        classification,
                        err
                    ));
                }
            }
        }
    }

    async fn execute_query(&self, statement: &str) -> Result<Vec<LogicalRow>> {
        let submit_url = format!("{}/sessions/{}/statements", self.base_url, self.session_id);
        let submit_body = json!({
            "code": statement,
            "kind": self.statement_kind,
        });
        let submit_json = self
            .send_json_with_retry("statement submission", || {
                self.client.post(&submit_url).json(&submit_body)
            })
            .await?;

        let statement_id = submit_json
            .get("id")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow!("Missing statement id in Livy response: {}", submit_json))?;

        let poll_url = format!(
            "{}/sessions/{}/statements/{}",
            self.base_url, self.session_id, statement_id
        );
        for attempt in 0..self.max_poll_attempts {
            if attempt > 0 {
                sleep(self.poll_interval).await;
            }

            let poll_json = self
                .send_json_with_retry("statement polling", || self.client.get(&poll_url))
                .await?;
            let state = poll_json
                .get("state")
                .and_then(|s| s.as_str())
                .unwrap_or("unknown");

            match state {
                "available" => {
                    return parse_statement_rows(&poll_json).with_context(|| {
                        format!(
                            "Failed to parse Spark statement output for statement {}",
                            statement_id
                        )
                    })
                }
                "error" | "cancelled" | "cancelling" => {
                    return Err(anyhow!(
                        "Spark statement {} failed (state={}, classification=statement_failed): {}",
                        statement_id,
                        state,
                        extract_statement_error(&poll_json)
                    ));
                }
                "waiting" | "running" => {}
                other => {
                    tracing::debug!(
                        statement_id,
                        state = %other,
                        "Spark statement is still in non-terminal state"
                    );
                }
            }
        }

        Err(anyhow!(
            "Spark statement {} did not finish after {} poll attempts (classification=statement_timeout)",
            statement_id,
            self.max_poll_attempts
        ))
    }
}

fn should_retry_http_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

fn classify_http_status(status: StatusCode) -> &'static str {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => "authentication",
        StatusCode::TOO_MANY_REQUESTS => "throttled",
        StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => "timeout",
        s if s.is_server_error() => "server_error",
        s if s.is_client_error() => "client_error",
        _ => "unknown_http_error",
    }
}

fn should_retry_reqwest_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request() || err.is_body()
}

fn classify_reqwest_error(err: &reqwest::Error) -> &'static str {
    if err.is_timeout() {
        "timeout"
    } else if err.is_connect() {
        "connectivity"
    } else if err.is_request() {
        "request_build"
    } else if err.is_body() {
        "body"
    } else if err.is_decode() {
        "decode"
    } else {
        "transport_error"
    }
}

fn retry_delay_for_attempt(retry_number: u32, base: Duration, max: Duration) -> Duration {
    let exponent = retry_number.saturating_sub(1).min(10);
    let multiplier = 1u128 << exponent;
    let raw_ms = base.as_millis().saturating_mul(multiplier);
    let capped_ms = raw_ms.min(max.as_millis()).max(1);
    Duration::from_millis(capped_ms as u64)
}

fn truncate_for_error(body: &str) -> String {
    if body.chars().count() <= MAX_ERROR_BODY_CHARS {
        body.to_string()
    } else {
        let truncated: String = body.chars().take(MAX_ERROR_BODY_CHARS).collect();
        format!("{truncated}…")
    }
}

fn extract_statement_error(statement_json: &JsonValue) -> String {
    let Some(output) = statement_json.get("output") else {
        return truncate_for_error(&statement_json.to_string());
    };

    let mut parts = Vec::new();
    if let Some(ename) = output.get("ename").and_then(|v| v.as_str()) {
        parts.push(format!("ename={}", ename));
    }
    if let Some(evalue) = output.get("evalue").and_then(|v| v.as_str()) {
        parts.push(format!("evalue={}", evalue));
    }
    if let Some(traceback) = output.get("traceback").and_then(|v| v.as_array()) {
        let snippet = traceback
            .iter()
            .filter_map(|v| v.as_str())
            .take(3)
            .collect::<Vec<_>>()
            .join(" | ");
        if !snippet.is_empty() {
            parts.push(format!("traceback={}", snippet));
        }
    }
    if parts.is_empty() {
        truncate_for_error(&output.to_string())
    } else {
        truncate_for_error(&parts.join("; "))
    }
}

fn parse_statement_rows(statement_json: &JsonValue) -> Result<Vec<LogicalRow>> {
    let output = statement_json
        .get("output")
        .ok_or_else(|| anyhow!("Missing output in Spark statement response"))?;
    let status = output
        .get("status")
        .and_then(|s| s.as_str())
        .unwrap_or("unknown");
    if status != "ok" {
        return Err(anyhow!(
            "Spark statement output status is '{}' instead of 'ok': {}",
            status,
            truncate_for_error(&output.to_string())
        ));
    }

    let data = output
        .get("data")
        .ok_or_else(|| anyhow!("Missing output.data in Spark statement response"))?;

    if let Some(v) = data.get("application/json") {
        return parse_application_json(v);
    }

    if let Some(v) = data.get("application/vnd.livy.table.v1+json") {
        return parse_livy_table_json(v);
    }

    if let Some(text) = data.get("text/plain").and_then(|v| v.as_str()) {
        if let Ok(parsed_json) = serde_json::from_str::<JsonValue>(text) {
            return parse_application_json(&parsed_json);
        }
        return Err(anyhow!(
            "Spark statement returned only text/plain output; use SQL sessions or %json output. text={}",
            truncate_for_error(text)
        ));
    }

    Ok(Vec::new())
}

fn parse_application_json(v: &JsonValue) -> Result<Vec<LogicalRow>> {
    match v {
        JsonValue::Null => Ok(Vec::new()),
        JsonValue::Array(rows) => parse_array_payload(rows),
        JsonValue::Object(obj) => {
            if let Some(data_rows) = obj.get("data").and_then(|d| d.as_array()) {
                let col_names = obj
                    .get("schema")
                    .and_then(|s| s.get("fields"))
                    .and_then(|f| f.as_array())
                    .map(|fields| {
                        fields
                            .iter()
                            .map(|f| {
                                f.get("name")
                                    .and_then(|n| n.as_str())
                                    .unwrap_or("")
                                    .to_string()
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                return parse_rows_with_columns(data_rows, &col_names);
            }

            // Some payloads return a single row object directly.
            let mut row = JsonMap::new();
            for (k, val) in obj {
                row.insert(k.clone(), val.clone());
            }
            Ok(vec![row])
        }
        _ => Err(anyhow!("Unsupported application/json payload for Spark statement")),
    }
}

fn parse_livy_table_json(v: &JsonValue) -> Result<Vec<LogicalRow>> {
    let obj = v
        .as_object()
        .ok_or_else(|| anyhow!("Invalid application/vnd.livy.table.v1+json payload"))?;
    let headers = obj
        .get("headers")
        .and_then(|h| h.as_array())
        .map(|arr| {
            arr.iter()
                .map(|h| {
                    h.get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("")
                        .to_string()
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let data_rows = obj
        .get("data")
        .and_then(|d| d.as_array())
        .ok_or_else(|| anyhow!("Missing data array in Livy table payload"))?;

    let mut out = Vec::with_capacity(data_rows.len());
    for row in data_rows {
        match row {
            JsonValue::Array(arr) => {
                let mut mapped = JsonMap::new();
                for (idx, value) in arr.iter().enumerate() {
                    let key = headers
                        .get(idx)
                        .filter(|s| !s.is_empty())
                        .cloned()
                        .unwrap_or_else(|| format!("col_{}", idx + 1));
                    mapped.insert(key, unwrap_livy_typed_value(value));
                }
                out.push(mapped);
            }
            JsonValue::Object(obj_row) => {
                let mut mapped = JsonMap::new();
                for (k, value) in obj_row {
                    mapped.insert(k.clone(), unwrap_livy_typed_value(value));
                }
                out.push(mapped);
            }
            _ => {}
        }
    }

    Ok(out)
}

fn parse_array_payload(rows: &[JsonValue]) -> Result<Vec<LogicalRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    match &rows[0] {
        JsonValue::Object(_) => {
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let obj = row
                    .as_object()
                    .ok_or_else(|| anyhow!("Expected object rows in application/json array"))?;
                out.push(obj.clone());
            }
            Ok(out)
        }
        JsonValue::Array(_) => parse_rows_with_columns(rows, &[]),
        _ => Err(anyhow!(
            "Unsupported application/json row format (expected object or array)"
        )),
    }
}

fn parse_rows_with_columns(rows: &[JsonValue], col_names: &[String]) -> Result<Vec<LogicalRow>> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        match row {
            JsonValue::Object(obj) => out.push(obj.clone()),
            JsonValue::Array(arr) => {
                let mut mapped = JsonMap::new();
                for (idx, value) in arr.iter().enumerate() {
                    let key = col_names
                        .get(idx)
                        .filter(|s| !s.is_empty())
                        .cloned()
                        .unwrap_or_else(|| format!("col_{}", idx + 1));
                    mapped.insert(key, unwrap_livy_typed_value(value));
                }
                out.push(mapped);
            }
            _ => {}
        }
    }
    Ok(out)
}

fn unwrap_livy_typed_value(value: &JsonValue) -> JsonValue {
    let JsonValue::Object(obj) = value else {
        return value.clone();
    };

    for key in [
        "stringVal",
        "booleanVal",
        "doubleVal",
        "longVal",
        "intVal",
        "i32",
        "i64",
        "f64",
        "decimalVal",
        "timestampVal",
        "dateVal",
    ] {
        if let Some(v) = obj.get(key) {
            return v.clone();
        }
    }

    if obj.len() == 1 {
        if let Some((_, v)) = obj.iter().next() {
            return unwrap_livy_typed_value(v);
        }
    }

    value.clone()
}

pub async fn query_spark_sql(cfg: &SparkConfig, statement: &str) -> Result<Vec<LogicalRow>> {
    SparkClient::new(cfg)?.execute_query(statement).await
}

/// Fetch rows for a given mapping, using either a local JSON file or Spark SQL.
///
/// Watermark (if provided) is used only when the mapping is in incremental mode and
/// has a delta.updated_at_column configured.
pub async fn fetch_rows_for_mapping(
    cfg: &Config,
    mapping: &CommonMappingFields,
    watermark: Option<&str>,
) -> Result<Vec<LogicalRow>> {
    // File-based source for local testing.
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

    if let Some(spark_cfg) = cfg.spark.as_ref() {
        if mapping.source.table.is_some() || mapping.source.select.is_some() {
            let sql = build_sql_for_mapping(mapping, watermark)?;
            let rows = query_spark_sql(spark_cfg, &sql).await?;
            return Ok(apply_schema_strategy(rows, spark_cfg, &mapping.source));
        }
    }

    Ok(Vec::new())
}

fn build_sql_for_mapping(mapping: &CommonMappingFields, watermark: Option<&str>) -> Result<String> {
    let predicates = build_source_predicates(mapping, watermark)?;
    let partition_hint = build_partition_hint(mapping.source.partition.as_ref());

    let mut sql = if let Some(select) = &mapping.source.select {
        let hint = partition_hint.as_deref().unwrap_or("");
        format!("SELECT{hint} * FROM ({select}) AS s")
    } else if let Some(table) = &mapping.source.table {
        let hint = partition_hint.as_deref().unwrap_or("");
        format!("SELECT{hint} * FROM {table}")
    } else {
        return Err(anyhow!(
            "Spark source for mapping '{}' must specify source.table or source.select/query/sql/statement",
            mapping.name
        ));
    };

    if !predicates.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&predicates.join(" AND "));
    }
    if let Some(query_count) = mapping.source.query_count {
        sql.push_str(&format!(" LIMIT {}", query_count));
    }

    Ok(sql)
}

fn build_source_predicates(
    mapping: &CommonMappingFields,
    watermark: Option<&str>,
) -> Result<Vec<String>> {
    let mut predicates: Vec<String> = Vec::new();

    if let Some(w) = &mapping.source.r#where {
        predicates.push(w.clone());
    }

    if let (Mode::Incremental, Some(delta), Some(wm)) = (mapping.mode, mapping.delta.as_ref(), watermark) {
        let escaped = wm.replace('\'', "''");
        predicates.push(format!("{} > '{}'", delta.updated_at_column, escaped));
    }

    if let Some(partition) = mapping.source.partition.as_ref() {
        if let (Some(lower), Some(upper)) = (&partition.lower_bound, &partition.upper_bound) {
            predicates.push(format!(
                "({column} >= {lower} AND {column} < {upper})",
                column = partition.column,
                lower = sql_literal(lower),
                upper = sql_literal(upper),
            ));
        }
    }

    Ok(predicates)
}

fn build_partition_hint(partition: Option<&SourcePartitionConfig>) -> Option<String> {
    let partition = partition?;
    let num_partitions = partition.num_partitions?;
    if num_partitions <= 1 {
        return None;
    }
    Some(format!(
        " /*+ REPARTITION({}, {}) */",
        num_partitions, partition.column
    ))
}

fn sql_literal(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return "''".to_string();
    }
    if trimmed.eq_ignore_ascii_case("null")
        || trimmed.eq_ignore_ascii_case("true")
        || trimmed.eq_ignore_ascii_case("false")
        || trimmed.parse::<i64>().is_ok()
        || trimmed.parse::<f64>().is_ok()
        || (trimmed.starts_with('\'') && trimmed.ends_with('\''))
    {
        return trimmed.to_string();
    }
    format!("'{}'", trimmed.replace('\'', "''"))
}

fn apply_schema_strategy(
    rows: Vec<LogicalRow>,
    spark_cfg: &SparkConfig,
    source_cfg: &SourceConfig,
) -> Vec<LogicalRow> {
    let strategy = source_cfg
        .schema_strategy
        .unwrap_or(spark_cfg.schema_strategy);
    let flatten_max_depth = source_cfg
        .flatten_max_depth
        .or(spark_cfg.flatten_max_depth)
        .unwrap_or(DEFAULT_FLATTEN_MAX_DEPTH);

    rows.into_iter()
        .map(|row| apply_schema_strategy_to_row(row, strategy, flatten_max_depth))
        .collect()
}

fn apply_schema_strategy_to_row(
    row: LogicalRow,
    strategy: SchemaStrategy,
    flatten_max_depth: u8,
) -> LogicalRow {
    match strategy {
        SchemaStrategy::Preserve => row,
        SchemaStrategy::JsonStringify => row
            .into_iter()
            .map(|(k, v)| (k, stringify_complex_value(v)))
            .collect(),
        SchemaStrategy::DropComplex => row
            .into_iter()
            .filter(|(_, v)| !is_complex_value(v))
            .collect(),
        SchemaStrategy::Flatten => flatten_row(row, flatten_max_depth),
    }
}

fn is_complex_value(value: &JsonValue) -> bool {
    matches!(value, JsonValue::Array(_) | JsonValue::Object(_))
}

fn stringify_complex_value(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(_) | JsonValue::Object(_) => JsonValue::String(
            serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string()),
        ),
        other => other,
    }
}

fn flatten_row(row: LogicalRow, max_depth: u8) -> LogicalRow {
    let mut out = JsonMap::new();
    for (key, value) in row {
        flatten_value_into(&mut out, &key, &value, 0, max_depth.max(1));
    }
    out
}

fn flatten_value_into(
    out: &mut LogicalRow,
    key: &str,
    value: &JsonValue,
    depth: u8,
    max_depth: u8,
) {
    match value {
        JsonValue::Object(obj) if depth < max_depth => {
            for (nested_key, nested_value) in obj {
                let composite = format!("{}_{}", key, nested_key);
                flatten_value_into(out, &composite, nested_value, depth + 1, max_depth);
            }
        }
        JsonValue::Array(_) | JsonValue::Object(_) => {
            out.insert(
                key.to_string(),
                JsonValue::String(
                    serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
                ),
            );
        }
        _ => {
            out.insert(key.to_string(), value.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DeltaSpec, SchemaStrategy};

    fn spark_cfg(strategy: SchemaStrategy) -> SparkConfig {
        SparkConfig {
            livy_url: "http://localhost:8998".to_string(),
            session_id: 1,
            statement_kind: "sql".to_string(),
            auth_token: None,
            catalog: None,
            schema: None,
            query_timeout_ms: Some(30_000),
            poll_interval_ms: Some(300),
            max_poll_attempts: Some(100),
            max_retries: Some(3),
            retry_backoff_ms: Some(100),
            retry_backoff_max_ms: Some(1_000),
            schema_strategy: strategy,
            flatten_max_depth: Some(2),
        }
    }

    fn table_mapping() -> CommonMappingFields {
        CommonMappingFields {
            name: "orders".to_string(),
            source: SourceConfig {
                file: None,
                table: Some("sales.orders".to_string()),
                select: None,
                r#where: Some("is_active = true".to_string()),
                query_count: None,
                partition: None,
                schema_strategy: None,
                flatten_max_depth: None,
            },
            mode: Mode::Incremental,
            delta: Some(DeltaSpec {
                updated_at_column: "updated_at".to_string(),
                deleted_flag_column: None,
                deleted_flag_value: None,
                initial_full_load: None,
            }),
        }
    }

    #[test]
    fn parse_application_json_schema_and_data() -> Result<()> {
        let payload = serde_json::json!({
            "schema": { "fields": [ { "name": "id" }, { "name": "name" } ] },
            "data": [
                [1, "Alice"],
                [2, "Bob"]
            ]
        });
        let rows = parse_application_json(&payload)?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("id"), Some(&serde_json::json!(1)));
        assert_eq!(rows[1].get("name"), Some(&serde_json::json!("Bob")));
        Ok(())
    }

    #[test]
    fn build_sql_for_mapping_with_incremental_predicate() -> Result<()> {
        let mapping = table_mapping();
        let sql = build_sql_for_mapping(&mapping, Some("2026-01-01T00:00:00Z"))?;
        assert!(sql.contains("SELECT * FROM sales.orders"));
        assert!(sql.contains("is_active = true"));
        assert!(sql.contains("updated_at > '2026-01-01T00:00:00Z'"));
        Ok(())
    }

    #[test]
    fn build_sql_for_mapping_with_query_count_and_partition_hints() -> Result<()> {
        let mut mapping = table_mapping();
        mapping.source.query_count = Some(500);
        mapping.source.partition = Some(SourcePartitionConfig {
            column: "customer_id".to_string(),
            num_partitions: Some(16),
            lower_bound: Some("1".to_string()),
            upper_bound: Some("10000".to_string()),
        });

        let sql = build_sql_for_mapping(&mapping, None)?;
        assert!(sql.contains("/*+ REPARTITION(16, customer_id) */"));
        assert!(sql.contains("customer_id >= 1"));
        assert!(sql.contains("customer_id < 10000"));
        assert!(sql.ends_with("LIMIT 500"));
        Ok(())
    }

    #[test]
    fn apply_schema_strategy_json_stringify_complex_values() {
        let rows = vec![serde_json::json!({
            "id": 1,
            "meta": { "tier": "gold" },
            "tags": ["a", "b"]
        })
        .as_object()
        .expect("object")
        .clone()];

        let cfg = spark_cfg(SchemaStrategy::JsonStringify);
        let source = SourceConfig {
            file: None,
            table: Some("sales.customers".to_string()),
            select: None,
            r#where: None,
            query_count: None,
            partition: None,
            schema_strategy: None,
            flatten_max_depth: None,
        };
        let transformed = apply_schema_strategy(rows, &cfg, &source);
        let row = &transformed[0];
        assert_eq!(row.get("id"), Some(&serde_json::json!(1)));
        assert_eq!(
            row.get("meta"),
            Some(&serde_json::json!("{\"tier\":\"gold\"}"))
        );
        assert_eq!(row.get("tags"), Some(&serde_json::json!("[\"a\",\"b\"]")));
    }

    #[test]
    fn apply_schema_strategy_drop_complex_values() {
        let rows = vec![serde_json::json!({
            "id": 1,
            "meta": { "tier": "gold" },
            "active": true
        })
        .as_object()
        .expect("object")
        .clone()];

        let cfg = spark_cfg(SchemaStrategy::DropComplex);
        let source = SourceConfig {
            file: None,
            table: Some("sales.customers".to_string()),
            select: None,
            r#where: None,
            query_count: None,
            partition: None,
            schema_strategy: None,
            flatten_max_depth: None,
        };
        let transformed = apply_schema_strategy(rows, &cfg, &source);
        let row = &transformed[0];
        assert!(row.get("meta").is_none());
        assert_eq!(row.get("id"), Some(&serde_json::json!(1)));
        assert_eq!(row.get("active"), Some(&serde_json::json!(true)));
    }

    #[test]
    fn apply_schema_strategy_flatten_nested_objects() {
        let rows = vec![serde_json::json!({
            "id": 1,
            "profile": { "name": { "first": "Ada" } },
            "tags": ["vip"]
        })
        .as_object()
        .expect("object")
        .clone()];

        let cfg = spark_cfg(SchemaStrategy::Preserve);
        let source = SourceConfig {
            file: None,
            table: Some("sales.customers".to_string()),
            select: None,
            r#where: None,
            query_count: None,
            partition: None,
            schema_strategy: Some(SchemaStrategy::Flatten),
            flatten_max_depth: Some(3),
        };
        let transformed = apply_schema_strategy(rows, &cfg, &source);
        let row = &transformed[0];
        assert_eq!(row.get("profile_name_first"), Some(&serde_json::json!("Ada")));
        assert_eq!(row.get("tags"), Some(&serde_json::json!("[\"vip\"]")));
    }

    #[test]
    fn retry_backoff_is_exponential_and_capped() {
        let base = Duration::from_millis(100);
        let max = Duration::from_millis(1_000);
        assert_eq!(retry_delay_for_attempt(1, base, max), Duration::from_millis(100));
        assert_eq!(retry_delay_for_attempt(2, base, max), Duration::from_millis(200));
        assert_eq!(retry_delay_for_attempt(4, base, max), Duration::from_millis(800));
        assert_eq!(retry_delay_for_attempt(5, base, max), Duration::from_millis(1_000));
        assert_eq!(
            retry_delay_for_attempt(10, base, max),
            Duration::from_millis(1_000)
        );
    }

    #[test]
    fn retryable_http_statuses_match_expected() {
        assert!(should_retry_http_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(should_retry_http_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!should_retry_http_status(StatusCode::BAD_REQUEST));
        assert_eq!(classify_http_status(StatusCode::UNAUTHORIZED), "authentication");
    }

    /// Optional Spark connectivity smoke test.
    ///
    /// Uses environment variables:
    /// - SPARK_LIVY_URL
    /// - SPARK_LIVY_SESSION_ID
    /// - SPARK_LIVY_AUTH_TOKEN (optional)
    ///
    /// If required vars are missing, the test is a no-op and returns Ok(()).
    #[tokio::test]
    async fn spark_connectivity_smoke_test() -> Result<()> {
        let livy_url = match std::env::var("SPARK_LIVY_URL") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        let session_id = match std::env::var("SPARK_LIVY_SESSION_ID") {
            Ok(v) => match v.parse::<u64>() {
                Ok(id) => id,
                Err(_) => return Ok(()),
            },
            Err(_) => return Ok(()),
        };
        let auth_token = std::env::var("SPARK_LIVY_AUTH_TOKEN").ok();

        let cfg = SparkConfig {
            livy_url,
            session_id,
            statement_kind: "sql".to_string(),
            auth_token,
            catalog: None,
            schema: None,
            query_timeout_ms: Some(30_000),
            poll_interval_ms: Some(300),
            max_poll_attempts: Some(100),
            max_retries: Some(3),
            retry_backoff_ms: Some(250),
            retry_backoff_max_ms: Some(5_000),
            schema_strategy: SchemaStrategy::Preserve,
            flatten_max_depth: Some(2),
        };

        let rows = query_spark_sql(&cfg, "SELECT 1 AS one").await?;
        if let Some(row) = rows.first() {
            if let Some(v) = row.get("one").or_else(|| row.get("ONE")) {
                assert!(v.is_number() || v.is_string());
            }
        }

        Ok(())
    }
}
