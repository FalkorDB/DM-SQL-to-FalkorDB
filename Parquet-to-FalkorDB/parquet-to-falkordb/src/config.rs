use std::{env, fs, path::Path};

use anyhow::{Context, Result};
use arrow_to_falkordb_bridge::{resolve_object_store_env_refs, ObjectStoreConfig};
use serde::{Deserialize, Serialize};

/// Top-level config: multi-mapping, optional incremental mode, JSON or YAML.
#[derive(Debug, Deserialize)]
pub struct Config {
    pub parquet: Option<ParquetConfig>,
    pub falkordb: FalkorConfig,
    pub state: Option<StateConfig>,
    #[serde(default)]
    pub mappings: Vec<EntityMapping>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct FalkorIndexSpec {
    pub labels: Vec<String>,
    pub property: String,
    #[serde(default)]
    pub source_table: Option<String>,
    #[serde(default)]
    pub source_columns: Vec<String>,
}

/// Global Parquet source defaults (path, object store, listing options).
#[derive(Debug, Clone, Deserialize)]
pub struct ParquetConfig {
    /// Default path/URI (file, directory, or object-store prefix).
    /// Accepts `path` or `file` as aliases.
    #[serde(default, alias = "file")]
    pub path: Option<String>,

    /// Glob pattern relative to path prefix (default: `**/*.parquet`).
    #[serde(default)]
    pub glob: Option<String>,

    /// Extract Hive-style partition columns (`key=value` path segments) into rows.
    #[serde(default)]
    pub hive_partitioning: Option<bool>,

    /// Object-store credentials / connection settings.
    #[serde(default)]
    pub object_store: Option<ObjectStoreConfig>,

    /// Max Arrow rows per RecordBatch when reading (bounded memory).
    #[serde(default)]
    pub batch_size: Option<usize>,
}

impl ParquetConfig {
    pub fn glob_pattern(&self) -> &str {
        self.glob.as_deref().unwrap_or("**/*.parquet")
    }

    pub fn hive_partitioning_enabled(&self) -> bool {
        self.hive_partitioning.unwrap_or(true)
    }

    pub fn batch_size_or_default(&self) -> usize {
        self.batch_size.unwrap_or(65_536).max(1)
    }
}

#[derive(Debug, Deserialize)]
pub struct FalkorConfig {
    pub endpoint: String,
    pub graph: String,
    #[serde(default)]
    pub max_unwind_batch_size: Option<usize>,
    #[serde(default)]
    pub indexes: Vec<FalkorIndexSpec>,
}

#[derive(Debug, Deserialize)]
pub struct StateConfig {
    pub backend: StateBackendKind,
    pub file_path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StateBackendKind {
    File,
    Falkordb,
    None,
}

/// Per-mapping source: path/file override, optional glob.
#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    /// Single file or directory/prefix override for this mapping.
    pub file: Option<String>,
    /// Alias for `file` / directory prefix.
    pub path: Option<String>,
    /// Optional glob override for this mapping.
    pub glob: Option<String>,
}

impl SourceConfig {
    pub fn resolved_path<'a>(&'a self, global: Option<&'a ParquetConfig>) -> Option<&'a str> {
        self.path
            .as_deref()
            .or(self.file.as_deref())
            .or_else(|| global.and_then(|g| g.path.as_deref()))
    }
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

#[derive(Debug, Clone, Deserialize)]
pub struct DeltaSpec {
    /// Column-based watermark (same model as other connectors).
    pub updated_at_column: String,
    pub deleted_flag_column: Option<String>,
    pub deleted_flag_value: Option<serde_json::Value>,
    #[serde(default)]
    pub initial_full_load: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct CommonMappingFields {
    pub name: String,
    pub source: SourceConfig,
    #[serde(default = "default_mode_full")]
    pub mode: Mode,
    pub delta: Option<DeltaSpec>,
    /// When `mode: incremental` and no `delta` is set (or this is true), track a
    /// file-level last-modified cursor instead of a column watermark.
    #[serde(default)]
    pub file_cursor: Option<bool>,
}

fn default_mode_full() -> Mode {
    Mode::Full
}

impl CommonMappingFields {
    /// True when this mapping should use the file last-modified cursor.
    pub fn uses_file_cursor(&self) -> bool {
        matches!(self.mode, Mode::Incremental)
            && (self.file_cursor.unwrap_or(false) || self.delta.is_none())
    }

    /// True when this mapping uses a column watermark via `delta.updated_at_column`.
    pub fn uses_column_watermark(&self) -> bool {
        matches!(self.mode, Mode::Incremental) && self.delta.is_some() && !self.uses_file_cursor()
    }
}

#[derive(Debug, Deserialize)]
pub struct NodeMappingConfig {
    #[serde(flatten)]
    pub common: CommonMappingFields,
    pub labels: Vec<String>,
    pub key: NodeKeySpec,
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
    pub column: String,
    pub property: String,
}

#[derive(Debug, Deserialize)]
pub struct EdgeKeySpec {
    pub column: String,
    pub property: String,
}

#[derive(Debug, Deserialize)]
pub struct PropertySpec {
    pub column: String,
}

impl Config {
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

        if let Some(pq) = cfg.parquet.as_mut() {
            resolve_env_ref(&mut pq.path, "parquet.path")?;
            if let Some(os) = pq.object_store.as_mut() {
                resolve_object_store_env_refs(os)?;
            }
        }

        resolve_env_ref_string(&mut cfg.falkordb.endpoint, "falkordb.endpoint")?;

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
    let env_name = env_name
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or(env_name);
    let resolved = env::var(env_name).with_context(|| {
        format!("Environment variable {env_name} referenced by {field_name} is not set")
    })?;
    *value = Some(resolved);
    Ok(())
}

fn resolve_env_ref_string(value: &mut String, field_name: &str) -> Result<()> {
    if let Some(env_name) = value.strip_prefix('$') {
        let env_name = env_name
            .strip_prefix('{')
            .and_then(|s| s.strip_suffix('}'))
            .unwrap_or(env_name);
        *value = env::var(env_name).with_context(|| {
            format!("Environment variable {env_name} referenced by {field_name} is not set")
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn write_temp(contents: &str, ext: &str) -> PathBuf {
        let mut path = env::temp_dir();
        path.push(format!(
            "parquet_to_falkordb_config_test_{}.{}",
            std::process::id(),
            ext
        ));
        fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn config_from_yaml_resolves_env() {
        env::set_var("PQ_TEST_PATH", "/tmp/lake");
        env::set_var("PQ_TEST_AK", "AKIA");
        let yaml = r#"
            parquet:
              path: "$PQ_TEST_PATH"
              object_store:
                provider: s3
                region: us-east-1
                access_key_id: "$PQ_TEST_AK"
                secret_access_key: "plain"
            falkordb:
              endpoint: "falkor://127.0.0.1:6379"
              graph: "g"
            mappings: []
        "#;
        let path = write_temp(yaml, "yaml");
        let cfg = Config::from_file(&path).unwrap();
        let pq = cfg.parquet.expect("parquet");
        assert_eq!(pq.path.as_deref(), Some("/tmp/lake"));
        assert_eq!(
            pq.object_store
                .as_ref()
                .and_then(|o| o.access_key_id.as_deref()),
            Some("AKIA")
        );
        env::remove_var("PQ_TEST_PATH");
        env::remove_var("PQ_TEST_AK");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn uses_file_cursor_when_incremental_without_delta() {
        let common = CommonMappingFields {
            name: "t".into(),
            source: SourceConfig {
                file: None,
                path: Some("/data".into()),
                glob: None,
            },
            mode: Mode::Incremental,
            delta: None,
            file_cursor: None,
        };
        assert!(common.uses_file_cursor());
        assert!(!common.uses_column_watermark());
    }

    #[test]
    fn uses_column_watermark_with_delta() {
        let common = CommonMappingFields {
            name: "t".into(),
            source: SourceConfig {
                file: None,
                path: Some("/data".into()),
                glob: None,
            },
            mode: Mode::Incremental,
            delta: Some(DeltaSpec {
                updated_at_column: "updated_at".into(),
                deleted_flag_column: None,
                deleted_flag_value: None,
                initial_full_load: None,
            }),
            file_cursor: Some(false),
        };
        assert!(!common.uses_file_cursor());
        assert!(common.uses_column_watermark());
    }
}
