use std::{env, fs, path::Path};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Top-level config: multi-mapping, optional incremental mode, JSON or YAML.
#[derive(Debug, Deserialize)]
pub struct Config {
    pub bigquery: Option<BigQueryConfig>,
    pub falkordb: FalkorConfig,
    pub state: Option<StateConfig>,
    #[serde(default)]
    pub mappings: Vec<EntityMapping>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct FalkorIndexSpec {
    /// Node labels to index (combined as :LabelA:LabelB in FalkorDB index syntax).
    pub labels: Vec<String>,
    /// Graph property to index.
    pub property: String,
    /// Optional source table provenance for scaffold-generated templates.
    #[serde(default)]
    pub source_table: Option<String>,
    /// Optional source columns provenance for scaffold-generated templates.
    #[serde(default)]
    pub source_columns: Vec<String>,
}

/// BigQuery REST/SQL configuration.
#[derive(Debug, Deserialize, Clone)]
pub struct BigQueryConfig {
    pub project_id: String,
    pub dataset: String,
    /// BigQuery job location (for example `US`, `EU`, `us-central1`).
    pub location: Option<String>,
    /// Optional pre-minted OAuth access token. Can be "$ENV_VAR".
    pub access_token: Option<String>,
    /// Optional service-account JSON key path. Can be "$ENV_VAR".
    pub service_account_key_path: Option<String>,
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
    /// Optional explicit FalkorDB index definitions to apply before processing mappings.
    #[serde(default)]
    pub indexes: Vec<FalkorIndexSpec>,
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

/// Source specification: supports either a local JSON file or a BigQuery table/SELECT.
#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    /// Path to a JSON file containing an array of objects, each representing a row.
    pub file: Option<String>,
    /// Optional table name for BigQuery-based sources.
    pub table: Option<String>,
    /// Optional full SELECT statement for BigQuery-based sources.
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

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
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
    /// Column in the source row that contains the unique identifier.
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

        if let Some(bq_cfg) = cfg.bigquery.as_mut() {
            if let Some(ref token) = bq_cfg.access_token {
                if let Some(env_ref) = token.strip_prefix('$') {
                    let env_name = env_ref;
                    let resolved = env::var(env_name).with_context(|| {
                        format!(
                            "Environment variable {} referenced by bigquery.access_token is not set",
                            env_name
                        )
                    })?;
                    bq_cfg.access_token = Some(resolved);
                }
            }

            if let Some(ref key_path) = bq_cfg.service_account_key_path {
                if let Some(env_ref) = key_path.strip_prefix('$') {
                    let env_name = env_ref;
                    let resolved = env::var(env_name).with_context(|| {
                        format!(
                            "Environment variable {} referenced by bigquery.service_account_key_path is not set",
                            env_name
                        )
                    })?;
                    bq_cfg.service_account_key_path = Some(resolved);
                }
            }
        }

        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::{env, fs, path::PathBuf};

    fn write_temp_file(contents: &str, ext: &str) -> PathBuf {
        let mut path = env::temp_dir();
        path.push(format!("bigquery_to_falkordb_config_test.{}", ext));
        fs::write(&path, contents).expect("failed to write temp config file");
        path
    }

    #[test]
    fn config_from_yaml_resolves_env_values() -> Result<()> {
        let token_var = "BIGQUERY_TEST_TOKEN";
        let key_var = "BIGQUERY_TEST_KEY_PATH";
        env::set_var(token_var, "super-secret");
        env::set_var(key_var, "/tmp/service-account.json");

        let yaml = r#"
            bigquery:
              project_id: "test-project"
              dataset: "analytics"
              location: "US"
              access_token: "$BIGQUERY_TEST_TOKEN"
              service_account_key_path: "$BIGQUERY_TEST_KEY_PATH"
            falkordb:
              endpoint: "falkor://127.0.0.1:6379"
              graph: "test"
            mappings: []
        "#;

        let path = write_temp_file(yaml, "yaml");
        let cfg = Config::from_file(&path)?;
        let bq = cfg.bigquery.expect("expected bigquery config");
        assert_eq!(bq.access_token.as_deref(), Some("super-secret"));
        assert_eq!(
            bq.service_account_key_path.as_deref(),
            Some("/tmp/service-account.json")
        );
        Ok(())
    }

    #[test]
    fn config_from_json_parses_basic_fields() -> Result<()> {
        let json = r#"
            {
              "bigquery": {
                "project_id": "project-a",
                "dataset": "dataset_a",
                "location": "US"
              },
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
        let bq = cfg.bigquery.expect("expected bigquery config");
        assert_eq!(bq.project_id, "project-a");
        assert_eq!(bq.dataset, "dataset_a");
        assert_eq!(bq.location.as_deref(), Some("US"));
        Ok(())
    }
}
