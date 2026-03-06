use std::fs;

use anyhow::{anyhow, Context, Result};
use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool, Row, Value};
use serde_json::{Map as JsonMap, Number, Value as JsonValue};

use crate::config::{CommonMappingFields, Config, MariaDbConfig, Mode};

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

/// Fetch all rows for a given mapping, from either a file or MariaDB.
pub async fn fetch_rows_for_mapping(
    cfg: &Config,
    common: &CommonMappingFields,
    watermark: Option<&str>,
) -> Result<Vec<LogicalRow>> {
    if let Some(file) = &common.source.file {
        return load_rows_from_file(file);
    }

    let mariadb_cfg = cfg.mariadb.as_ref().ok_or_else(|| {
        anyhow!(
            "No supported source configured for mapping {} (need `file` or MariaDB)",
            common.name
        )
    })?;
    if mariadb_cfg
        .cdc
        .as_ref()
        .and_then(|c| c.enabled)
        .unwrap_or(false)
    {
        return Err(anyhow!(
            "mariadb.cdc.enabled=true is not implemented yet; use incremental watermark mode for mapping '{}'",
            common.name
        ));
    }

    let sql = build_sql(common, watermark)?;
    fetch_rows_from_mariadb(mariadb_cfg, &sql, &common.name).await
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

    let mariadb_cfg = cfg.mariadb.as_ref().ok_or_else(|| {
        anyhow!(
            "No MariaDB config provided for paged mapping '{}'",
            common.name
        )
    })?;
    if mariadb_cfg
        .cdc
        .as_ref()
        .and_then(|c| c.enabled)
        .unwrap_or(false)
    {
        return Err(anyhow!(
            "mariadb.cdc.enabled=true is not implemented yet; use incremental watermark mode for mapping '{}'",
            common.name
        ));
    }
    let sql = build_paged_sql(common, watermark, page_size)?;
    fetch_rows_from_mariadb(mariadb_cfg, &sql, &common.name).await
}

pub fn incremental_page_size(cfg: &Config) -> Option<usize> {
    cfg.mariadb
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
            "MariaDB source for mapping '{}' must specify `source.table`, `source.select`, or `source.file`",
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

fn mariadb_opts(mariadb_cfg: &MariaDbConfig) -> Result<Opts> {
    if let Some(url) = &mariadb_cfg.url {
        return Opts::from_url(url).with_context(|| "Failed to parse MariaDB URL in `mariadb.url`");
    }

    let host = mariadb_cfg
        .host
        .clone()
        .unwrap_or_else(|| "localhost".to_string());
    let port = mariadb_cfg.port.unwrap_or(3306);

    let mut builder = OptsBuilder::default().ip_or_hostname(host).tcp_port(port);

    if let Some(user) = mariadb_cfg.user.clone() {
        builder = builder.user(Some(user));
    }
    if let Some(password) = mariadb_cfg.password.clone() {
        builder = builder.pass(Some(password));
    }
    if let Some(database) = mariadb_cfg.database.clone() {
        builder = builder.db_name(Some(database));
    }

    Ok(Opts::from(builder))
}

async fn fetch_rows_from_mariadb(
    mariadb_cfg: &MariaDbConfig,
    sql: &str,
    mapping_name: &str,
) -> Result<Vec<LogicalRow>> {
    let opts = mariadb_opts(mariadb_cfg)?;
    let pool = Pool::new(opts);

    let mut conn = pool
        .get_conn()
        .await
        .with_context(|| "Failed to connect to MariaDB")?;

    if let Some(timeout_ms) = mariadb_cfg.query_timeout_ms {
        let timeout_stmt = format!("SET SESSION MAX_EXECUTION_TIME={}", timeout_ms);
        if let Err(e) = conn.query_drop(timeout_stmt).await {
            tracing::warn!(
                mapping = %mapping_name,
                error = %e,
                "Failed to set MAX_EXECUTION_TIME; continuing without session timeout",
            );
        }
    }

    let rows: Vec<Row> = conn.query(sql).await.with_context(|| {
        format!(
            "Failed to execute MariaDB query for mapping '{}' with SQL: {}",
            mapping_name, sql
        )
    })?;

    let mut logical_rows = Vec::with_capacity(rows.len());
    for row in rows {
        logical_rows.push(row_to_logical_row(row)?);
    }

    drop(conn);
    pool.disconnect()
        .await
        .with_context(|| "Failed to disconnect MariaDB pool")?;

    Ok(logical_rows)
}

fn row_to_logical_row(row: Row) -> Result<LogicalRow> {
    let column_names: Vec<String> = row
        .columns_ref()
        .iter()
        .map(|c| c.name_str().to_string())
        .collect();
    let values: Vec<Value> = row.unwrap();

    if column_names.len() != values.len() {
        return Err(anyhow!(
            "MariaDB row shape mismatch: {} columns but {} values",
            column_names.len(),
            values.len()
        ));
    }

    let mut out = JsonMap::with_capacity(values.len());
    for (name, value) in column_names.into_iter().zip(values.into_iter()) {
        out.insert(name, mariadb_value_to_json(value));
    }
    Ok(LogicalRow { values: out })
}

fn mariadb_value_to_json(value: Value) -> JsonValue {
    match value {
        Value::NULL => JsonValue::Null,
        Value::Bytes(bytes) => JsonValue::String(String::from_utf8_lossy(&bytes).to_string()),
        Value::Int(v) => JsonValue::from(v),
        Value::UInt(v) => JsonValue::from(v),
        Value::Float(v) => Number::from_f64(v as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::Double(v) => Number::from_f64(v)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::Date(year, month, day, hour, minute, second, micros) => {
            if hour == 0 && minute == 0 && second == 0 && micros == 0 {
                JsonValue::String(format!("{year:04}-{month:02}-{day:02}"))
            } else if micros == 0 {
                JsonValue::String(format!(
                    "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}"
                ))
            } else {
                JsonValue::String(format!(
                    "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}"
                ))
            }
        }
        Value::Time(is_neg, days, hours, minutes, seconds, micros) => {
            let sign = if is_neg { "-" } else { "" };
            if micros == 0 {
                JsonValue::String(format!("{sign}{days} {hours:02}:{minutes:02}:{seconds:02}"))
            } else {
                JsonValue::String(format!(
                    "{sign}{days} {hours:02}:{minutes:02}:{seconds:02}.{micros:06}"
                ))
            }
        }
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

    /// Optional MariaDB connectivity smoke test.
    ///
    /// Uses environment variable:
    /// - MARIADB_URL
    ///
    /// If MARIADB_URL is not set, the test is a no-op and returns Ok(()).
    #[tokio::test]
    async fn mariadb_connectivity_smoke_test() -> Result<()> {
        let url = match std::env::var("MARIADB_URL") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };

        let cfg = MariaDbConfig {
            url: Some(url),
            host: None,
            port: None,
            user: None,
            password: None,
            database: None,
            fetch_batch_size: None,
            query_timeout_ms: Some(10_000),
            cdc: None,
        };

        let rows = fetch_rows_from_mariadb(&cfg, "SELECT 1 AS one", "smoke").await?;
        assert!(!rows.is_empty());
        Ok(())
    }
}
