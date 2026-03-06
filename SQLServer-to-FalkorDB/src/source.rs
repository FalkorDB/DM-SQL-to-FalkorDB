use std::fs;

use anyhow::{anyhow, Context, Result};
use futures_util::TryStreamExt;
use serde_json::{Map as JsonMap, Value as JsonValue};
use tiberius::{AuthMethod, Client, Config as DriverConfig, QueryItem};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::config::{CommonMappingFields, Config, Mode, SqlServerConfig};

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

type SqlServerClient = Client<Compat<TcpStream>>;

/// Fetch all rows for a given mapping, from either a file or SQL Server.
pub async fn fetch_rows_for_mapping(
    cfg: &Config,
    common: &CommonMappingFields,
    watermark: Option<&str>,
) -> Result<Vec<LogicalRow>> {
    if let Some(file) = &common.source.file {
        return load_rows_from_file(file);
    }

    let sqlserver_cfg = cfg.sqlserver.as_ref().ok_or_else(|| {
        anyhow!(
            "No supported source configured for mapping {} (need `file` or SQL Server)",
            common.name
        )
    })?;

    let sql = build_sql(common, watermark)?;
    fetch_rows_from_sqlserver(sqlserver_cfg, &sql, &common.name).await
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

    let sqlserver_cfg = cfg.sqlserver.as_ref().ok_or_else(|| {
        anyhow!(
            "No SQL Server config provided for paged mapping '{}'",
            common.name
        )
    })?;
    let sql = build_paged_sql(common, watermark, page_size)?;
    fetch_rows_from_sqlserver(sqlserver_cfg, &sql, &common.name).await
}

pub fn incremental_page_size(cfg: &Config) -> Option<usize> {
    cfg.sqlserver
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
            "SQL Server source for mapping '{}' must specify `source.table`, `source.select`, or `source.file`",
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
        " ORDER BY {} ASC OFFSET 0 ROWS FETCH NEXT {} ROWS ONLY",
        delta.updated_at_column, page_size
    ));
    Ok(sql)
}

fn build_driver_config(sqlserver_cfg: &SqlServerConfig) -> Result<DriverConfig> {
    if let Some(connection_string) = &sqlserver_cfg.connection_string {
        let mut cfg = DriverConfig::from_ado_string(connection_string)
            .with_context(|| "Failed to parse SQL Server `sqlserver.connection_string`")?;
        if sqlserver_cfg.trust_cert.unwrap_or(true) {
            cfg.trust_cert();
        }
        return Ok(cfg);
    }

    let host = sqlserver_cfg
        .host
        .clone()
        .unwrap_or_else(|| "localhost".to_string());
    let port = sqlserver_cfg.port.unwrap_or(1433);
    let user = sqlserver_cfg
        .user
        .clone()
        .unwrap_or_else(|| "sa".to_string());
    let password = sqlserver_cfg.password.clone().unwrap_or_default();
    let database = sqlserver_cfg
        .database
        .clone()
        .unwrap_or_else(|| "master".to_string());

    let mut cfg = DriverConfig::new();
    cfg.host(host);
    cfg.port(port);
    cfg.authentication(AuthMethod::sql_server(user, password));
    cfg.database(database);
    if sqlserver_cfg.trust_cert.unwrap_or(true) {
        cfg.trust_cert();
    }

    Ok(cfg)
}

async fn connect_sqlserver(sqlserver_cfg: &SqlServerConfig) -> Result<SqlServerClient> {
    let driver_cfg = build_driver_config(sqlserver_cfg)?;
    let tcp = TcpStream::connect(driver_cfg.get_addr())
        .await
        .with_context(|| "Failed to open TCP connection to SQL Server")?;
    tcp.set_nodelay(true)
        .with_context(|| "Failed to configure TCP_NODELAY for SQL Server connection")?;

    let client = Client::connect(driver_cfg, tcp.compat_write())
        .await
        .with_context(|| "Failed to connect to SQL Server over TDS")?;

    Ok(client)
}

async fn fetch_rows_from_sqlserver(
    sqlserver_cfg: &SqlServerConfig,
    sql: &str,
    mapping_name: &str,
) -> Result<Vec<LogicalRow>> {
    let mut client = connect_sqlserver(sqlserver_cfg).await?;

    if sqlserver_cfg.query_timeout_ms.is_some() {
        tracing::warn!(
            mapping = %mapping_name,
            "sqlserver.query_timeout_ms is currently advisory; driver-level enforcement is not implemented"
        );
    }

    let wrapped_sql = wrap_sql_for_json_rows(sql);
    let mut stream = client.simple_query(wrapped_sql).await.with_context(|| {
        format!(
            "Failed to execute SQL Server query for mapping '{}' with SQL: {}",
            mapping_name, sql
        )
    })?;

    let mut logical_rows = Vec::new();
    while let Some(item) = stream.try_next().await.with_context(|| {
        format!(
            "Failed to read SQL Server result stream for mapping '{}'",
            mapping_name
        )
    })? {
        if let QueryItem::Row(row) = item {
            let row_json: Option<&str> = row.get("row_json");
            let Some(raw_json) = row_json else {
                continue;
            };

            if raw_json.trim().is_empty() {
                continue;
            }

            let parsed: JsonValue = serde_json::from_str(raw_json).with_context(|| {
                format!(
                    "Failed to parse SQL Server JSON row payload for mapping '{}'",
                    mapping_name
                )
            })?;

            let obj = parsed.as_object().cloned().ok_or_else(|| {
                anyhow!(
                    "Expected SQL Server row JSON object for mapping '{}', got: {}",
                    mapping_name,
                    parsed
                )
            })?;

            logical_rows.push(LogicalRow { values: obj });
        }
    }

    Ok(logical_rows)
}

fn wrap_sql_for_json_rows(sql: &str) -> String {
    format!(
        "SELECT (SELECT src.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES) AS row_json FROM ({}) AS src",
        sql
    )
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
    use crate::config::{CommonMappingFields, DeltaSpec, Mode, SourceConfig, SqlServerConfig};

    #[test]
    fn build_sql_with_table_where_and_watermark() -> Result<()> {
        let mapping = CommonMappingFields {
            name: "customers".to_string(),
            source: SourceConfig {
                file: None,
                table: Some("dbo.customers".to_string()),
                select: None,
                r#where: Some("is_active = 1".to_string()),
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
        assert!(sql.contains("SELECT * FROM dbo.customers"));
        assert!(sql.contains("is_active = 1"));
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
                r#where: Some("ignored = 1".to_string()),
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
    fn build_paged_sql_appends_order_and_fetch() -> Result<()> {
        let mapping = CommonMappingFields {
            name: "orders".to_string(),
            source: SourceConfig {
                file: None,
                table: Some("dbo.orders".to_string()),
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
        assert!(sql.contains("ORDER BY updated_at ASC OFFSET 0 ROWS FETCH NEXT 500 ROWS ONLY"));
        Ok(())
    }

    /// Optional SQL Server connectivity smoke test.
    ///
    /// Uses environment variable:
    /// - SQLSERVER_CONNECTION_STRING
    ///
    /// If SQLSERVER_CONNECTION_STRING is not set, the test is a no-op and returns Ok(()).
    #[tokio::test]
    async fn sqlserver_connectivity_smoke_test() -> Result<()> {
        let connection_string = match std::env::var("SQLSERVER_CONNECTION_STRING") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };

        let cfg = SqlServerConfig {
            connection_string: Some(connection_string),
            host: None,
            port: None,
            user: None,
            password: None,
            database: None,
            trust_cert: Some(true),
            fetch_batch_size: None,
            query_timeout_ms: Some(10_000),
        };

        let rows = fetch_rows_from_sqlserver(&cfg, "SELECT 1 AS one", "smoke").await?;
        assert!(!rows.is_empty());
        Ok(())
    }
}
