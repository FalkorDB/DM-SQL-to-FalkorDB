use std::{fs, time::Duration};

use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::config::{ClickHouseConfig, CommonMappingFields, Config, Mode};

/// Logical row abstraction used by the mapping layer.
#[derive(Debug, Clone)]
pub struct LogicalRow {
    pub values: JsonMap<String, JsonValue>,
}

impl LogicalRow {
    pub fn get(&self, key: &str) -> Option<&JsonValue> {
        self.values.get(key)
    }
}

/// Fetch all rows for a given mapping, from either a file or ClickHouse.
pub async fn fetch_rows_for_mapping(
    cfg: &Config,
    common: &CommonMappingFields,
    watermark: Option<&str>,
) -> Result<Vec<LogicalRow>> {
    if let Some(file) = &common.source.file {
        return load_rows_from_file(file);
    }

    let ch_cfg = cfg.clickhouse.as_ref().ok_or_else(|| {
        anyhow!(
            "No supported source configured for mapping {} (need `file` or ClickHouse)",
            common.name
        )
    })?;

    let sql = build_sql(common, watermark)?;
    fetch_rows_from_clickhouse(ch_cfg, &sql).await
}

/// Fetch a single incremental page for a table-based mapping.
pub async fn fetch_rows_page_for_incremental_table(
    cfg: &Config,
    common: &CommonMappingFields,
    watermark: Option<&str>,
    page_size: usize,
) -> Result<Vec<LogicalRow>> {
    if common.source.file.is_some() {
        return Err(anyhow!(
            "Paged incremental fetch is not supported for file source on mapping '{}'",
            common.name
        ));
    }

    let ch_cfg = cfg.clickhouse.as_ref().ok_or_else(|| {
        anyhow!(
            "No ClickHouse config provided for paged mapping '{}'",
            common.name
        )
    })?;
    let sql = build_paged_sql(common, watermark, page_size)?;
    fetch_rows_from_clickhouse(ch_cfg, &sql).await
}

pub fn incremental_page_size(cfg: &Config) -> Option<usize> {
    cfg.clickhouse
        .as_ref()
        .and_then(|c| c.fetch_batch_size)
        .filter(|v| *v > 0)
}

pub fn should_use_incremental_paging(cfg: &Config, common: &CommonMappingFields) -> bool {
    incremental_page_size(cfg).is_some()
        && matches!(common.mode, Mode::Incremental)
        && common.delta.is_some()
        && common.source.table.is_some()
        && common.source.select.is_none()
        && common.source.file.is_none()
}

/// Build SQL for mapping fetch:
/// - `source.select` is treated as user-owned SQL and returned as-is.
/// - `source.table` gets optional WHERE and optional watermark predicate.
pub fn build_sql(common: &CommonMappingFields, watermark: Option<&str>) -> Result<String> {
    if let Some(sel) = &common.source.select {
        return Ok(sel.clone());
    }

    if let Some(table) = &common.source.table {
        let mut predicates: Vec<String> = Vec::new();
        if let Some(w) = &common.source.r#where {
            predicates.push(w.clone());
        }

        if let (Mode::Incremental, Some(delta), Some(wm)) =
            (common.mode, common.delta.as_ref(), watermark)
        {
            let escaped = escape_sql_literal(wm);
            predicates.push(format!("{} > '{}'", delta.updated_at_column, escaped));
        }

        if predicates.is_empty() {
            Ok(format!("SELECT * FROM {}", table))
        } else {
            Ok(format!(
                "SELECT * FROM {} WHERE {}",
                table,
                predicates.join(" AND ")
            ))
        }
    } else {
        Err(anyhow!(
            "ClickHouse source for mapping '{}' must specify `source.table`, `source.select`, or `source.file`",
            common.name
        ))
    }
}

fn build_paged_sql(
    common: &CommonMappingFields,
    watermark: Option<&str>,
    page_size: usize,
) -> Result<String> {
    if page_size == 0 {
        return Err(anyhow!("Paged fetch requested with page_size=0"));
    }
    if common.source.select.is_some() || common.source.table.is_none() {
        return Err(anyhow!(
            "Paged fetch for mapping '{}' requires `source.table` and does not support `source.select`",
            common.name
        ));
    }

    let delta = common
        .delta
        .as_ref()
        .ok_or_else(|| anyhow!("Paged fetch for mapping '{}' requires `delta`", common.name))?;

    let mut sql = build_sql(common, watermark)?;
    sql.push_str(&format!(
        " ORDER BY {} ASC LIMIT {}",
        delta.updated_at_column, page_size
    ));
    Ok(sql)
}

async fn fetch_rows_from_clickhouse(
    ch_cfg: &ClickHouseConfig,
    sql_without_format: &str,
) -> Result<Vec<LogicalRow>> {
    let mut client_builder = Client::builder();
    if let Some(timeout_ms) = ch_cfg.query_timeout_ms {
        client_builder = client_builder.timeout(Duration::from_millis(timeout_ms));
    }
    let client = client_builder
        .build()
        .with_context(|| "Failed to build HTTP client for ClickHouse")?;

    let endpoint = ch_cfg.endpoint();
    let sql = ensure_format_json_each_row(sql_without_format);

    let mut request = client.post(endpoint).body(sql);
    if let Some(db) = &ch_cfg.database {
        request = request.query(&[("database", db)]);
    }
    if let Some(user) = &ch_cfg.user {
        request = request.basic_auth(user, ch_cfg.password.clone());
    } else if let Some(password) = &ch_cfg.password {
        request = request.query(&[("password", password)]);
    }

    let response = request
        .send()
        .await
        .with_context(|| "Failed to send ClickHouse HTTP request")?
        .error_for_status()
        .with_context(|| "ClickHouse returned an error status")?;

    let body = response
        .text()
        .await
        .with_context(|| "Failed to read ClickHouse HTTP response body")?;

    parse_json_each_row_lines(&body)
}

fn parse_json_each_row_lines(body: &str) -> Result<Vec<LogicalRow>> {
    let mut rows = Vec::new();
    for (idx, line) in body.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let value: JsonValue = serde_json::from_str(trimmed).with_context(|| {
            format!(
                "Failed to parse JSONEachRow line {} as JSON object",
                idx + 1
            )
        })?;
        match value {
            JsonValue::Object(map) => rows.push(LogicalRow { values: map }),
            _ => {
                return Err(anyhow!(
                    "Expected JSON object on JSONEachRow line {}, got {}",
                    idx + 1,
                    trimmed
                ))
            }
        }
    }
    Ok(rows)
}

fn ensure_format_json_each_row(query: &str) -> String {
    if query.to_ascii_uppercase().contains("FORMAT JSONEACHROW") {
        query.to_string()
    } else {
        format!("{} FORMAT JSONEachRow", query)
    }
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn load_rows_from_file(path: &str) -> Result<Vec<LogicalRow>> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("Failed to read input file {}", path))?;

    let value: JsonValue = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse JSON input from {}", path))?;

    let arr = value
        .as_array()
        .cloned()
        .ok_or_else(|| anyhow!("Expected top-level JSON array in input file {}", path))?;

    let mut rows = Vec::with_capacity(arr.len());
    for (idx, v) in arr.into_iter().enumerate() {
        match v {
            JsonValue::Object(map) => rows.push(LogicalRow { values: map }),
            _ => {
                return Err(anyhow!(
                    "Row at index {} in {} is not a JSON object",
                    idx,
                    path
                ))
            }
        }
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CommonMappingFields, DeltaSpec, Mode, SourceConfig};

    #[test]
    fn build_sql_with_table_where_and_watermark() -> Result<()> {
        let mapping = CommonMappingFields {
            name: "customers".to_string(),
            source: SourceConfig {
                file: None,
                table: Some("analytics.customers".to_string()),
                select: None,
                r#where: Some("active = 1".to_string()),
            },
            mode: Mode::Incremental,
            delta: Some(DeltaSpec {
                updated_at_column: "updated_at".to_string(),
                deleted_flag_column: None,
                deleted_flag_value: None,
                initial_full_load: None,
            }),
        };

        let sql = build_sql(&mapping, Some("2024-01-01T00:00:00Z"))?;
        assert!(sql.contains("SELECT * FROM analytics.customers"));
        assert!(sql.contains("active = 1"));
        assert!(sql.contains("updated_at > '2024-01-01T00:00:00Z'"));
        Ok(())
    }

    #[test]
    fn build_sql_with_select_is_not_rewritten() -> Result<()> {
        let select = "SELECT id, name FROM custom_source".to_string();
        let mapping = CommonMappingFields {
            name: "custom_query".to_string(),
            source: SourceConfig {
                file: None,
                table: None,
                select: Some(select.clone()),
                r#where: Some("ignored = true".to_string()),
            },
            mode: Mode::Incremental,
            delta: Some(DeltaSpec {
                updated_at_column: "updated_at".to_string(),
                deleted_flag_column: None,
                deleted_flag_value: None,
                initial_full_load: None,
            }),
        };

        let sql = build_sql(&mapping, Some("2024-01-01T00:00:00Z"))?;
        assert_eq!(sql, select);
        Ok(())
    }

    #[test]
    fn build_paged_sql_appends_order_and_limit() -> Result<()> {
        let mapping = CommonMappingFields {
            name: "orders".to_string(),
            source: SourceConfig {
                file: None,
                table: Some("analytics.orders".to_string()),
                select: None,
                r#where: None,
            },
            mode: Mode::Incremental,
            delta: Some(DeltaSpec {
                updated_at_column: "updated_at".to_string(),
                deleted_flag_column: None,
                deleted_flag_value: None,
                initial_full_load: None,
            }),
        };

        let sql = build_paged_sql(&mapping, Some("2024-01-01T00:00:00Z"), 500)?;
        assert!(sql.contains("ORDER BY updated_at ASC LIMIT 500"));
        Ok(())
    }

    /// Optional ClickHouse connectivity smoke test.
    ///
    /// Uses environment variable:
    /// - CLICKHOUSE_URL
    ///
    /// If CLICKHOUSE_URL is not set, the test is a no-op and returns Ok(()).
    #[tokio::test]
    async fn clickhouse_connectivity_smoke_test() -> Result<()> {
        let url = match std::env::var("CLICKHOUSE_URL") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };

        let cfg = ClickHouseConfig {
            url: Some(url),
            host: None,
            port: None,
            user: None,
            password: None,
            database: None,
            fetch_batch_size: None,
            query_timeout_ms: Some(10_000),
        };

        let rows = fetch_rows_from_clickhouse(&cfg, "SELECT 1 AS one").await?;
        assert!(!rows.is_empty());
        Ok(())
    }
}
