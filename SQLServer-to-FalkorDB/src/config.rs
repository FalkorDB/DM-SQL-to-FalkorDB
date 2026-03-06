use std::{env, fs, path::Path};

use anyhow::{Context, Result};
use serde::Deserialize;

/// Top-level config: multi-mapping, optional incremental mode, JSON or YAML.
#[derive(Debug, Deserialize)]
pub struct Config {
    pub sqlserver: Option<SqlServerConfig>,
    pub falkordb: FalkorConfig,
    pub state: Option<StateConfig>,
    pub mappings: Vec<EntityMapping>,
}

/// SQL Server connection configuration.
#[derive(Debug, Deserialize)]
pub struct SqlServerConfig {
    /// ADO-style SQL Server connection string, e.g.
    /// "server=tcp:localhost,1433;User Id=sa;Password=...;Database=analytics;TrustServerCertificate=true;".
    #[serde(alias = "url")]
    pub connection_string: Option<String>,
    /// Discrete fields used when `connection_string` is not provided.
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
    #[serde(default)]
    pub trust_cert: Option<bool>,
    #[serde(default)]
    pub fetch_batch_size: Option<usize>,
    #[serde(default)]
    pub query_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct FalkorConfig {
    /// FalkorDB endpoint, e.g. "falkor://127.0.0.1:6379".
    pub endpoint: String,
    /// Target graph name.
    pub graph: String,
    /// Optional batch size override; default is 1000.
    #[serde(default)]
    pub max_unwind_batch_size: Option<usize>,
}

/// Where to persist per-mapping watermarks for incremental loads.
#[derive(Debug, Deserialize)]
pub struct StateConfig {
    pub backend: StateBackendKind,
    /// For file backend: path to JSON/YAML file used to store mapping -> watermark.
    pub file_path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StateBackendKind {
    File,
    Falkordb,
    None,
}

/// Source specification: supports either a local JSON file, a SQL Server table,
/// or a custom SELECT statement.
#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    /// Path to a JSON file containing an array of objects, each representing a row.
    pub file: Option<String>,
    /// Optional table/view name for SQL Server-based sources.
    pub table: Option<String>,
    /// Optional full SELECT statement for SQL Server-based sources.
    pub select: Option<String>,
    /// Optional WHERE clause to append when generating a SELECT from `table`.
    #[serde(rename = "where")]
    pub r#where: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum EntityMapping {
    Node(NodeMappingConfig),
    Edge(EdgeMappingConfig),
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Full,
    Incremental,
}

#[derive(Debug, Deserialize)]
pub struct DeltaSpec {
    pub updated_at_column: String,
    pub deleted_flag_column: Option<String>,
    pub deleted_flag_value: Option<serde_json::Value>,
    #[serde(default)]
    pub initial_full_load: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct CommonMappingFields {
    /// Logical name of the mapping.
    pub name: String,
    /// Source definition for this mapping.
    pub source: SourceConfig,
    #[serde(default = "default_mode_full")]
    pub mode: Mode,
    pub delta: Option<DeltaSpec>,
}

fn default_mode_full() -> Mode {
    Mode::Full
}

#[derive(Debug, Deserialize)]
pub struct NodeMappingConfig {
    #[serde(flatten)]
    pub common: CommonMappingFields,
    /// Cypher labels to apply to created/merged nodes, e.g. ["Customer"].
    pub labels: Vec<String>,
    pub key: NodeKeySpec,
    /// Map of graph property name -> column mapping.
    pub properties: std::collections::HashMap<String, PropertySpec>,
}

#[derive(Debug, Deserialize)]
pub struct EdgeEndpointMatch {
    pub node_mapping: String,
    pub match_on: Vec<MatchOn>,
    pub label_override: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct MatchOn {
    pub column: String,
    pub property: String,
}

#[derive(Debug, Deserialize)]
pub struct EdgeMappingConfig {
    #[serde(flatten)]
    pub common: CommonMappingFields,
    pub relationship: String,
    #[serde(default = "default_direction_out")]
    pub direction: EdgeDirection,
    pub from: EdgeEndpointMatch,
    pub to: EdgeEndpointMatch,
    pub key: Option<EdgeKeySpec>,
    pub properties: std::collections::HashMap<String, PropertySpec>,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum EdgeDirection {
    Out,
    In,
}

fn default_direction_out() -> EdgeDirection {
    EdgeDirection::Out
}

#[derive(Debug, Deserialize)]
pub struct NodeKeySpec {
    /// Column in the source row that contains the unique identifier (for MVP, single-column key).
    pub column: String,
    /// Property name on the node that stores this key.
    pub property: String,
}

#[derive(Debug, Deserialize)]
pub struct EdgeKeySpec {
    pub column: String,
    pub property: String,
}

#[derive(Debug, Deserialize)]
pub struct PropertySpec {
    /// Column name in the source row.
    pub column: String,
}

impl Config {
    /// Load configuration from a JSON or YAML file, based on file extension.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        let contents = fs::read_to_string(path_ref)
            .with_context(|| format!("Failed to read config file {}", path_ref.display()))?;

        let ext = path_ref
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_lowercase();

        let mut cfg: Config = match ext.as_str() {
            "yaml" | "yml" => serde_yaml::from_str(&contents).with_context(|| {
                format!("Failed to parse YAML config from {}", path_ref.display())
            })?,
            _ => serde_json::from_str(&contents).with_context(|| {
                format!("Failed to parse JSON config from {}", path_ref.display())
            })?,
        };

        if let Some(sqlserver_cfg) = cfg.sqlserver.as_mut() {
            resolve_env_ref(
                &mut sqlserver_cfg.connection_string,
                "sqlserver.connection_string",
            )?;
            resolve_env_ref(&mut sqlserver_cfg.host, "sqlserver.host")?;
            resolve_env_ref(&mut sqlserver_cfg.user, "sqlserver.user")?;
            resolve_env_ref(&mut sqlserver_cfg.password, "sqlserver.password")?;
            resolve_env_ref(&mut sqlserver_cfg.database, "sqlserver.database")?;
        }

        Ok(cfg)
    }
}

fn resolve_env_ref(value: &mut Option<String>, field_name: &str) -> Result<()> {
    let Some(current) = value.as_ref() else {
        return Ok(());
    };
    let Some(env_name) = current.strip_prefix('$') else {
        return Ok(());
    };

    let resolved = env::var(env_name).with_context(|| {
        format!(
            "Environment variable {} referenced by {} is not set",
            env_name, field_name
        )
    })?;
    *value = Some(resolved);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::{env, fs, path::PathBuf};

    fn write_temp_file(contents: &str, ext: &str) -> PathBuf {
        let mut path = env::temp_dir();
        path.push(format!("sqlserver_to_falkordb_config_test.{}", ext));
        fs::write(&path, contents).expect("failed to write temp config file");
        path
    }

    #[test]
    fn config_from_yaml_resolves_env_connection_string_and_password() -> Result<()> {
        let conn_var = "SQLSERVER_TEST_CONN_STRING";
        let pw_var = "SQLSERVER_TEST_PASSWORD";
        env::set_var(
            conn_var,
            "server=tcp:localhost,1433;User Id=sa;Password=pass;Database=analytics;TrustServerCertificate=true;",
        );
        env::set_var(pw_var, "super-secret");

        let yaml = r#"
            sqlserver:
              connection_string: "$SQLSERVER_TEST_CONN_STRING"
              user: "sa"
              password: "$SQLSERVER_TEST_PASSWORD"
            falkordb:
              endpoint: "falkor://127.0.0.1:6379"
              graph: "test"
            mappings: []
        "#;

        let path = write_temp_file(yaml, "yaml");
        let cfg = Config::from_file(&path)?;
        let sqlserver = cfg.sqlserver.expect("expected sqlserver config");
        assert_eq!(
            sqlserver.connection_string.as_deref(),
            Some(
                "server=tcp:localhost,1433;User Id=sa;Password=pass;Database=analytics;TrustServerCertificate=true;"
            )
        );
        assert_eq!(sqlserver.password.as_deref(), Some("super-secret"));
        Ok(())
    }

    #[test]
    fn config_from_json_parses_basic_fields() -> Result<()> {
        let json = r#"
            {
              "sqlserver": null,
              "falkordb": {
                "endpoint": "falkor://localhost:6379",
                "graph": "test_graph"
              },
              "state": null,
              "mappings": []
            }
        "#;

        let path = write_temp_file(json, "json");
        let cfg = Config::from_file(&path)?;
        assert!(cfg.sqlserver.is_none());
        assert_eq!(cfg.falkordb.endpoint, "falkor://localhost:6379");
        assert_eq!(cfg.falkordb.graph, "test_graph");
        Ok(())
    }
}
