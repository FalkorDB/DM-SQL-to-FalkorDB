use std::{collections::HashMap, env, fs, path::Path};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Top-level config: multi-mapping, optional incremental mode, JSON or YAML.
#[derive(Debug, Deserialize)]
pub struct Config {
    pub iceberg: Option<IcebergConfig>,
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

/// Catalog type for Iceberg.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CatalogType {
    Rest,
    Glue,
    Sql,
}

/// Iceberg catalog configuration.
#[derive(Debug, Deserialize, Clone)]
pub struct CatalogConfig {
    /// Catalog implementation: `rest`, `glue`, or `sql`.
    #[serde(rename = "type")]
    pub catalog_type: CatalogType,
    /// Catalog URI (REST endpoint, Glue endpoint override, or SQL connection string).
    pub uri: Option<String>,
    /// Warehouse location (e.g. `s3://bucket/warehouse` or `file:///tmp/warehouse`).
    pub warehouse: Option<String>,
    /// Optional SQL bind style for the SQL catalog (`qmark` or `dollar`). Defaults to `qmark`.
    pub bind_style: Option<String>,
    /// Optional AWS Glue catalog id.
    pub catalog_id: Option<String>,
    /// Extra properties forwarded to the catalog builder (token, credential, region, etc.).
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// Top-level Iceberg source configuration.
#[derive(Debug, Deserialize, Clone)]
pub struct IcebergConfig {
    pub catalog: CatalogConfig,
    /// Optional default namespace used when listing tables for scaffold.
    pub default_namespace: Option<String>,
    /// Optional branch name to pin (v1: recorded for future use; scans use current snapshot).
    pub branch: Option<String>,
    /// Optional snapshot id to pin (v1: recorded for future use).
    pub snapshot_id: Option<i64>,
    /// Optional extra FileIO / storage properties (s3.region, s3.access-key-id, ...).
    #[serde(default)]
    pub storage_properties: HashMap<String, String>,
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
    /// For file backend: path to JSON file used to store mapping -> watermark.
    pub file_path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StateBackendKind {
    File,
    Falkordb,
    None,
}

/// Source specification for a mapping: Iceberg table identifier or local JSON file.
#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    /// Path to a JSON file containing an array of objects (useful for offline tests).
    pub file: Option<String>,
    /// Iceberg table identifier, e.g. `namespace.table` or `ns1.ns2.table`.
    pub table: Option<String>,
    /// Optional column projection; when empty, all columns are selected.
    #[serde(default)]
    pub columns: Vec<String>,
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

        if let Some(ice) = cfg.iceberg.as_mut() {
            resolve_env_ref(&mut ice.catalog.uri, "iceberg.catalog.uri")?;
            resolve_env_ref(&mut ice.catalog.warehouse, "iceberg.catalog.warehouse")?;
            resolve_env_ref(&mut ice.catalog.catalog_id, "iceberg.catalog.catalog_id")?;
            resolve_env_map(&mut ice.catalog.properties, "iceberg.catalog.properties")?;
            resolve_env_map(
                &mut ice.storage_properties,
                "iceberg.storage_properties",
            )?;
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

fn resolve_env_map(map: &mut HashMap<String, String>, field_name: &str) -> Result<()> {
    for (key, value) in map.iter_mut() {
        if let Some(env_name) = value.strip_prefix('$') {
            let resolved = env::var(env_name).with_context(|| {
                format!(
                    "Environment variable {} referenced by {}.{} is not set",
                    env_name, field_name, key
                )
            })?;
            *value = resolved;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::{env, fs, path::PathBuf};

    fn write_temp_file(contents: &str, ext: &str) -> PathBuf {
        let mut path = env::temp_dir();
        path.push(format!(
            "iceberg_to_falkordb_config_test_{}.{}",
            std::process::id(),
            ext
        ));
        fs::write(&path, contents).expect("failed to write temp config file");
        path
    }

    #[test]
    fn config_from_yaml_resolves_env_refs() -> Result<()> {
        let uri_var = "ICEBERG_TEST_URI";
        let token_var = "ICEBERG_TEST_TOKEN";
        env::set_var(uri_var, "https://catalog.example.com");
        env::set_var(token_var, "secret-token");

        let yaml = r#"
            iceberg:
              catalog:
                type: rest
                uri: "$ICEBERG_TEST_URI"
                warehouse: "s3://warehouse"
                properties:
                  token: "$ICEBERG_TEST_TOKEN"
            falkordb:
              endpoint: "falkor://127.0.0.1:6379"
              graph: "test"
            mappings: []
        "#;

        let path = write_temp_file(yaml, "yaml");
        let cfg = Config::from_file(&path)?;
        let ice = cfg.iceberg.expect("expected iceberg config");
        assert_eq!(ice.catalog.catalog_type, CatalogType::Rest);
        assert_eq!(
            ice.catalog.uri.as_deref(),
            Some("https://catalog.example.com")
        );
        assert_eq!(
            ice.catalog.properties.get("token").map(String::as_str),
            Some("secret-token")
        );
        let _ = fs::remove_file(path);
        Ok(())
    }

    #[test]
    fn config_from_json_parses_basic_fields() -> Result<()> {
        let json = r#"
            {
              "iceberg": null,
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
        assert!(cfg.iceberg.is_none());
        assert_eq!(cfg.falkordb.endpoint, "falkor://localhost:6379");
        assert_eq!(cfg.falkordb.graph, "test_graph");
        let _ = fs::remove_file(path);
        Ok(())
    }
}
