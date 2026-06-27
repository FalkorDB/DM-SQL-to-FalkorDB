use std::fs;

use anyhow::{anyhow, Context, Result};
use oracle::Connection;
use serde_json::{Map as JsonMap, Number, Value as JsonValue};

use crate::config::{CommonMappingFields, Config, Mode, OracleConfig};

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

/// Fetch all rows for a given mapping, from either a file or Oracle.
pub async fn fetch_rows_for_mapping(
    cfg: &Config,
    common: &CommonMappingFields,
    watermark: Option<&str>,
) -> Result<Vec<LogicalRow>> {
    if let Some(file) = &common.source.file {
        return load_rows_from_file(file);
    }

    let oracle_cfg = cfg.oracle.as_ref().ok_or_else(|| {
        anyhow!(
            "No supported source configured for mapping {} (need `file` or Oracle)",
            common.name
        )
    })?;

    let sql = build_sql(common, watermark)?;
    fetch_rows_from_oracle(oracle_cfg, &sql, &common.name).await
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

    let oracle_cfg = cfg.oracle.as_ref().ok_or_else(|| {
        anyhow!(
            "No Oracle config provided for paged mapping '{}'",
            common.name
        )
    })?;

    let sql = build_paged_sql(common, watermark, page_size)?;
    fetch_rows_from_oracle(oracle_cfg, &sql, &common.name).await
}

pub fn incremental_page_size(cfg: &Config) -> Option<usize> {
    cfg.oracle
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
            "Oracle source for mapping '{}' must specify `source.table`, `source.select`, or `source.file`",
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
        " ORDER BY {} ASC FETCH NEXT {} ROWS ONLY",
        delta.updated_at_column, page_size
    ));
    Ok(sql)
}

pub fn oracle_connect_parts(oracle_cfg: &OracleConfig) -> Result<(String, String, String)> {
    let user = oracle_cfg
        .user
        .clone()
        .ok_or_else(|| anyhow!("Oracle user is required (oracle.user)"))?;
    let password = oracle_cfg
        .password
        .clone()
        .ok_or_else(|| anyhow!("Oracle password is required (oracle.password)"))?;

    let connect_string = if let Some(connect_string) = &oracle_cfg.connect_string {
        connect_string.clone()
    } else {
        let host = oracle_cfg
            .host
            .clone()
            .unwrap_or_else(|| "localhost".to_string());
        let port = oracle_cfg.port.unwrap_or(1521);
        let service = oracle_cfg
            .service_name
            .clone()
            .unwrap_or_else(|| "FREEPDB1".to_string());
        format!("{host}:{port}/{service}")
    };

    Ok((user, password, connect_string))
}

pub async fn fetch_rows_from_oracle(
    oracle_cfg: &OracleConfig,
    sql: &str,
    mapping_name: &str,
) -> Result<Vec<LogicalRow>> {
    let (user, password, connect_string) = oracle_connect_parts(oracle_cfg)?;
    let sql_owned = sql.to_string();
    let mapping_name_owned = mapping_name.to_string();
    let query_timeout_ms = oracle_cfg.query_timeout_ms;

    tokio::task::spawn_blocking(move || -> Result<Vec<LogicalRow>> {
        let conn = Connection::connect(&user, &password, &connect_string)
            .with_context(|| "Failed to connect to Oracle")?;

        if query_timeout_ms.is_some() {
            tracing::warn!(
                mapping = %mapping_name_owned,
                "oracle.query_timeout_ms is currently advisory; driver-level enforcement is not implemented"
            );
        }

        let mut result_set = conn.query(&sql_owned, &[]).with_context(|| {
            format!(
                "Failed to execute Oracle query for mapping '{}' with SQL: {}",
                mapping_name_owned, sql_owned
            )
        })?;

        let column_names = result_set
            .column_info()
            .iter()
            .map(|c| c.name().to_string())
            .collect::<Vec<_>>();

        let mut logical_rows = Vec::new();
        for row_result in &mut result_set {
            let row = row_result.with_context(|| {
                format!(
                    "Failed to fetch Oracle row for mapping '{}'",
                    mapping_name_owned
                )
            })?;
            logical_rows.push(row_to_logical_row(&row, &column_names));
        }

        Ok(logical_rows)
    })
    .await
    .map_err(|e| anyhow!("Oracle fetch task join error: {e}"))?
}

fn row_to_logical_row(row: &oracle::Row, column_names: &[String]) -> LogicalRow {
    let mut out = JsonMap::new();

    for (idx, val) in row.sql_values().iter().enumerate() {
        let column = column_names
            .get(idx)
            .cloned()
            .unwrap_or_else(|| format!("col_{idx}"));
        out.insert(column, oracle_sql_value_to_json(val));
    }

    LogicalRow { values: out }
}

fn oracle_sql_value_to_json(value: &oracle::SqlValue) -> JsonValue {
    if value.is_null().unwrap_or(false) {
        return JsonValue::Null;
    }

    if let Ok(v) = value.get::<i64>() {
        return JsonValue::from(v);
    }
    if let Ok(v) = value.get::<u64>() {
        return JsonValue::from(v);
    }
    if let Ok(v) = value.get::<f64>() {
        return Number::from_f64(v)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null);
    }
    if let Ok(v) = value.get::<bool>() {
        return JsonValue::Bool(v);
    }
    if let Ok(v) = value.get::<chrono::NaiveDateTime>() {
        return JsonValue::String(v.format("%Y-%m-%d %H:%M:%S%.f").to_string());
    }
    if let Ok(v) = value.get::<chrono::NaiveDate>() {
        return JsonValue::String(v.format("%Y-%m-%d").to_string());
    }
    if let Ok(v) = value.get::<String>() {
        return JsonValue::String(v);
    }

    JsonValue::String(value.to_string())
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
                table: Some("APP.CUSTOMERS".to_string()),
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
        assert!(sql.contains("SELECT * FROM APP.CUSTOMERS"));
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
    fn build_paged_sql_appends_order_and_fetch_first() -> Result<()> {
        let mapping = CommonMappingFields {
            name: "orders".to_string(),
            source: SourceConfig {
                file: None,
                table: Some("APP.ORDERS".to_string()),
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
        assert!(sql.contains("ORDER BY updated_at ASC FETCH NEXT 500 ROWS ONLY"));
        Ok(())
    }
}
