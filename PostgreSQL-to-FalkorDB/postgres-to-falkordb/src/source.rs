use anyhow::{anyhow, Context, Result};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::fs;
use tokio_postgres::{Client, NoTls, Row};

use crate::config::{CommonMappingFields, Config, Mode, PostgresConfig};

/// Logical representation of a single row coming from PostgreSQL or a file source.
pub type LogicalRow = JsonMap<String, JsonValue>;

/// Fetch rows for a given mapping, using either a local JSON file or PostgreSQL.
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
        return load_rows_from_file(path, &mapping.name);
    }

    // PostgreSQL source.
    let pg_cfg = cfg
        .postgres
        .as_ref()
        .ok_or_else(|| anyhow!("postgres configuration is required for non-file sources"))?;

    let client = connect_postgres(pg_cfg).await?;

    // If fetch_batch_size is set and this mapping uses a table source with a delta
    // specification, use simple LIMIT/OFFSET paging ordered by the updated_at column.
    if let (Some(batch_size), Some(delta)) = (pg_cfg.fetch_batch_size, &mapping.delta) {
        if batch_size > 0 && mapping.source.select.is_none() {
            return fetch_rows_paged(
                &client,
                mapping,
                watermark,
                &delta.updated_at_column,
                batch_size,
            )
            .await;
        }
    }

    let base_sql = build_sql_for_mapping(mapping, watermark)?;
    let wrapped_sql = format!("SELECT row_to_json(s) AS row FROM ({}) AS s", base_sql);

    let rows = client
        .query(wrapped_sql.as_str(), &[])
        .await
        .with_context(|| {
            format!(
                "Failed to execute PostgreSQL query for mapping {}",
                mapping.name
            )
        })?;

    let logical_rows = rows
        .into_iter()
        .map(row_to_logical_row_from_json)
        .collect::<Result<Vec<_>>>()?;

    Ok(logical_rows)
}

/// Fetch rows using LIMIT/OFFSET paging.
async fn fetch_rows_paged(
    client: &Client,
    mapping: &CommonMappingFields,
    watermark: Option<&str>,
    order_column: &str,
    batch_size: usize,
) -> Result<Vec<LogicalRow>> {
    let base_sql = build_sql_for_mapping(mapping, watermark)?;

    let mut out = Vec::new();
    let mut offset: usize = 0;

    loop {
        let paged_inner = format!(
            "{base} ORDER BY {col} LIMIT {limit} OFFSET {offset}",
            base = base_sql,
            col = order_column,
            limit = batch_size,
            offset = offset,
        );
        let paged_sql = format!("SELECT row_to_json(s) AS row FROM ({}) AS s", paged_inner);

        let rows = client
            .query(paged_sql.as_str(), &[])
            .await
            .with_context(|| {
                format!(
                    "Failed to execute paged PostgreSQL query for mapping {}",
                    mapping.name
                )
            })?;

        let chunk_len = rows.len();
        if chunk_len == 0 {
            break;
        }

        for row in rows {
            out.push(row_to_logical_row_from_json(row)?);
        }

        if chunk_len < batch_size {
            break;
        }

        offset += chunk_len;
    }

    Ok(out)
}

/// Build a SQL statement for a mapping, injecting watermark predicates when applicable.
fn build_sql_for_mapping(mapping: &CommonMappingFields, watermark: Option<&str>) -> Result<String> {
    // Build predicate list from static where + optional delta watermark.
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
            Ok(format!("SELECT * FROM ({}) AS s WHERE {}", select, cond))
        } else {
            Ok(select.clone())
        }
    } else if let Some(table) = &mapping.source.table {
        if let Some(cond) = combined {
            Ok(format!("SELECT * FROM {} WHERE {}", table, cond))
        } else {
            Ok(format!("SELECT * FROM {}", table))
        }
    } else {
        Err(anyhow!(
            "Source for mapping '{}' must specify `source.table`, `source.select`, or `source.file`",
            mapping.name
        ))
    }
}

/// Connect to PostgreSQL using the provided configuration.
async fn connect_postgres(cfg: &PostgresConfig) -> Result<Client> {
    let conn_str = build_conn_string(cfg)?;

    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .with_context(|| "Failed to connect to PostgreSQL")?;

    // Spawn the connection task to drive the connection in the background.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "PostgreSQL connection error");
        }
    });

    // If a per-query timeout is configured, apply it via statement_timeout.
    if let Some(ms) = cfg.query_timeout_ms {
        let stmt = format!("SET statement_timeout = {}", ms);
        client
            .batch_execute(&stmt)
            .await
            .with_context(|| "Failed to set PostgreSQL statement_timeout")?;
    }

    Ok(client)
}

fn build_conn_string(cfg: &PostgresConfig) -> Result<String> {
    if let Some(url) = &cfg.url {
        return Ok(url.clone());
    }

    let host = cfg.host.as_deref().unwrap_or("localhost");
    let port = cfg.port.unwrap_or(5432);
    let user = cfg.user.as_deref().unwrap_or("postgres");
    let dbname = cfg.dbname.as_deref().unwrap_or("postgres");

    let mut parts = vec![
        format!("host={}", host),
        format!("port={}", port),
        format!("user={}", user),
        format!("dbname={}", dbname),
    ];

    if let Some(pw) = &cfg.password {
        parts.push(format!("password={}", pw));
    }

    if let Some(sslmode) = &cfg.sslmode {
        parts.push(format!("sslmode={}", sslmode));
    }

    Ok(parts.join(" "))
}

fn row_to_logical_row_from_json(row: Row) -> Result<LogicalRow> {
    // We expect a single column named "row" produced by row_to_json(s).
    let json: JsonValue = row
        .try_get("row")
        .with_context(|| "Failed to decode row_to_json result as JSON")?;

    let obj = json.as_object().cloned().ok_or_else(|| {
        anyhow!(
            "Expected row_to_json to produce a JSON object, but got: {}",
            json
        )
    })?;

    Ok(obj)
}

fn load_rows_from_file(path: &str, mapping_name: &str) -> Result<Vec<LogicalRow>> {
    let data =
        fs::read_to_string(path).with_context(|| format!("Failed to read input file {}", path))?;
    let value: JsonValue = serde_json::from_str(&data)
        .with_context(|| format!("Failed to parse JSON array from {}", path))?;
    let arr = value.as_array().ok_or_else(|| {
        anyhow!(
            "Expected JSON array of objects in file {} for mapping {}",
            path,
            mapping_name
        )
    })?;

    let mut rows = Vec::with_capacity(arr.len());
    for (idx, v) in arr.iter().enumerate() {
        let obj = v.as_object().ok_or_else(|| {
            anyhow!(
                "Element {} in file {} for mapping {} is not a JSON object",
                idx,
                path,
                mapping_name
            )
        })?;
        rows.push(obj.clone());
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CommonMappingFields, Mode, SourceConfig};

    #[test]
    fn build_sql_with_table_and_where_and_watermark() {
        let mapping = CommonMappingFields {
            name: "test".to_string(),
            source: SourceConfig {
                file: None,
                table: Some("public.customers".to_string()),
                select: None,
                r#where: Some("active = true".to_string()),
            },
            mode: Mode::Incremental,
            delta: Some(crate::config::DeltaSpec {
                updated_at_column: "updated_at".to_string(),
                deleted_flag_column: None,
                deleted_flag_value: None,
                initial_full_load: None,
            }),
        };

        let sql = build_sql_for_mapping(&mapping, Some("2024-01-01T00:00:00Z"))
            .expect("sql should build");
        assert!(sql.contains("public.customers"));
        assert!(sql.contains("active = true"));
        assert!(sql.contains("updated_at >"));
    }

    /// Optional PostgreSQL connectivity smoke test.
    ///
    /// Uses environment variable:
    /// - POSTGRES_URL
    ///
    /// If POSTGRES_URL is not set, the test is a no-op and returns Ok(()).
    #[tokio::test]
    async fn postgres_connectivity_smoke_test() -> Result<()> {
        let url = match std::env::var("POSTGRES_URL") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };

        let pg_cfg = PostgresConfig {
            url: Some(url),
            host: None,
            port: None,
            user: None,
            password: None,
            dbname: None,
            sslmode: None,
            fetch_batch_size: None,
            query_timeout_ms: Some(10_000),
        };

        let client = connect_postgres(&pg_cfg).await?;
        let rows = client.query("SELECT 1 AS one", &[]).await?;
        assert!(!rows.is_empty());
        Ok(())
    }
}
