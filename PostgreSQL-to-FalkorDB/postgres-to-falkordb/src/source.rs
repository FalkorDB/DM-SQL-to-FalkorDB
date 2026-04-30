use anyhow::{anyhow, Context, Result};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::fs;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Connection, NoTls, Row};

use crate::config::{CommonMappingFields, Config, Mode, PostgresConfig};

/// Logical representation of a single row coming from PostgreSQL or a file source.
pub type LogicalRow = JsonMap<String, JsonValue>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EffectiveSslMode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

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
pub(crate) async fn connect_postgres(cfg: &PostgresConfig) -> Result<Client> {
    let conn_str = build_conn_string(cfg)?;
    let (pg_cfg, ssl_mode) = parse_pg_config_with_ssl_mode(&conn_str)?;

    let client = match ssl_mode {
        EffectiveSslMode::Disable => {
            let (client, connection) = pg_cfg
                .connect(NoTls)
                .await
                .with_context(|| "Failed to connect to PostgreSQL without TLS")?;
            spawn_connection(connection);
            client
        }
        EffectiveSslMode::Prefer => {
            let tls_connector = build_tls_connector(ssl_mode)?;
            match pg_cfg.connect(tls_connector).await {
                Ok((client, connection)) => {
                    spawn_connection(connection);
                    client
                }
                Err(tls_err) => {
                    tracing::warn!(
                        error = %tls_err,
                        "TLS connection failed with sslmode=prefer; retrying without TLS"
                    );
                    let (client, connection) = pg_cfg.connect(NoTls).await.with_context(|| {
                        "Failed to connect to PostgreSQL without TLS after TLS fallback"
                    })?;
                    spawn_connection(connection);
                    client
                }
            }
        }
        EffectiveSslMode::Require | EffectiveSslMode::VerifyCa | EffectiveSslMode::VerifyFull => {
            let tls_connector = build_tls_connector(ssl_mode)?;
            let (client, connection) = pg_cfg.connect(tls_connector).await.with_context(|| {
                format!(
                    "Failed to connect to PostgreSQL using TLS (sslmode={})",
                    ssl_mode_label(ssl_mode)
                )
            })?;
            spawn_connection(connection);
            client
        }
    };

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

fn parse_pg_config_with_ssl_mode(
    conn_str: &str,
) -> Result<(tokio_postgres::Config, EffectiveSslMode)> {
    let (normalized_conn_str, override_mode) = normalize_sslmode_for_tokio(conn_str);
    let pg_cfg: tokio_postgres::Config = normalized_conn_str
        .parse()
        .with_context(|| "Failed to parse PostgreSQL connection configuration")?;
    let mode = override_mode.unwrap_or_else(|| match pg_cfg.get_ssl_mode() {
        SslMode::Disable => EffectiveSslMode::Disable,
        SslMode::Prefer => EffectiveSslMode::Prefer,
        SslMode::Require => EffectiveSslMode::Require,
        _ => EffectiveSslMode::Require,
    });
    Ok((pg_cfg, mode))
}

fn ssl_mode_label(mode: EffectiveSslMode) -> &'static str {
    match mode {
        EffectiveSslMode::Disable => "disable",
        EffectiveSslMode::Prefer => "prefer",
        EffectiveSslMode::Require => "require",
        EffectiveSslMode::VerifyCa => "verify-ca",
        EffectiveSslMode::VerifyFull => "verify-full",
    }
}

fn build_tls_connector(mode: EffectiveSslMode) -> Result<MakeTlsConnector> {
    let mut builder = TlsConnector::builder();
    match mode {
        // libpq semantics:
        // - require: encrypted transport, certificate/hostname verification optional.
        // - prefer: attempts TLS first with same relaxed verification, then can fall back.
        EffectiveSslMode::Prefer | EffectiveSslMode::Require => {
            builder.danger_accept_invalid_certs(true);
            builder.danger_accept_invalid_hostnames(true);
        }
        // verify-ca: verify CA chain, skip hostname verification.
        EffectiveSslMode::VerifyCa => {
            builder.danger_accept_invalid_hostnames(true);
        }
        // verify-full: strict cert + hostname verification.
        EffectiveSslMode::VerifyFull => {}
        EffectiveSslMode::Disable => {
            return Err(anyhow!("Cannot build TLS connector when sslmode=disable"));
        }
    }
    let connector = builder
        .build()
        .with_context(|| "Failed to create native TLS connector for PostgreSQL")?;
    Ok(MakeTlsConnector::new(connector))
}

fn is_sslmode_boundary_byte(b: u8) -> bool {
    matches!(b, b'?' | b'&' | b' ' | b'\t' | b'\n' | b'\r')
}

fn normalize_sslmode_for_tokio(conn_str: &str) -> (String, Option<EffectiveSslMode>) {
    let lower = conn_str.to_ascii_lowercase();
    let lower_bytes = lower.as_bytes();

    let mut search_start = 0usize;
    while search_start < lower_bytes.len() {
        let Some(found_at) = lower[search_start..].find("sslmode=") else {
            break;
        };
        let key_start = search_start + found_at;
        if key_start > 0 && !is_sslmode_boundary_byte(lower_bytes[key_start - 1]) {
            search_start = key_start + "sslmode=".len();
            continue;
        }

        let value_start = key_start + "sslmode=".len();
        let mut value_end = value_start;
        while value_end < lower_bytes.len() && !is_sslmode_boundary_byte(lower_bytes[value_end]) {
            value_end += 1;
        }
        let value = &lower[value_start..value_end];
        let override_mode = match value {
            "verify-ca" => Some(EffectiveSslMode::VerifyCa),
            "verify-full" => Some(EffectiveSslMode::VerifyFull),
            _ => None,
        };

        if let Some(mode) = override_mode {
            let mut normalized = String::with_capacity(
                conn_str.len() - (value_end.saturating_sub(value_start)) + "require".len(),
            );
            normalized.push_str(&conn_str[..value_start]);
            normalized.push_str("require");
            normalized.push_str(&conn_str[value_end..]);
            return (normalized, Some(mode));
        }

        search_start = value_end;
    }

    (conn_str.to_string(), None)
}

fn spawn_connection<S, T>(connection: Connection<S, T>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "PostgreSQL connection error");
        }
    });
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

    #[test]
    fn normalize_sslmode_rewrites_verify_full_url_param() {
        let input = "postgresql://user:pass@db.example.com:5432/postgres?sslmode=verify-full";
        let (normalized, mode) = normalize_sslmode_for_tokio(input);
        assert_eq!(mode, Some(EffectiveSslMode::VerifyFull));
        assert!(normalized.contains("sslmode=require"));
    }

    #[test]
    fn normalize_sslmode_rewrites_verify_ca_kv_param() {
        let input = "host=db.example.com port=5432 user=postgres dbname=postgres sslmode=verify-ca";
        let (normalized, mode) = normalize_sslmode_for_tokio(input);
        assert_eq!(mode, Some(EffectiveSslMode::VerifyCa));
        assert!(normalized.contains("sslmode=require"));
    }

    #[test]
    fn parse_pg_config_preserves_prefer_when_no_override() {
        let (pg_cfg, mode) = parse_pg_config_with_ssl_mode(
            "host=localhost port=5432 user=postgres dbname=postgres sslmode=prefer",
        )
        .expect("config should parse");
        assert_eq!(pg_cfg.get_ssl_mode(), SslMode::Prefer);
        assert_eq!(mode, EffectiveSslMode::Prefer);
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
