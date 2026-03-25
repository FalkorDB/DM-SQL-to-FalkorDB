use std::{env, fs, path::Path};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Deserializer, Serialize};

/// Top-level config: multi-mapping, optional incremental mode, JSON or YAML.
#[derive(Debug, Deserialize)]
pub struct Config {
    pub spark: Option<SparkConfig>,
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

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaStrategy {
    Preserve,
    JsonStringify,
    DropComplex,
    Flatten,
}

fn default_schema_strategy() -> SchemaStrategy {
    SchemaStrategy::Preserve
}

/// Apache Spark SQL access via Livy interactive sessions.
#[derive(Debug, Deserialize, Clone)]
pub struct SparkConfig {
    /// Livy server URL, for example "http://localhost:8998".
    pub livy_url: String,
    /// Existing Livy session id (typically a SQL-capable session).
    pub session_id: u64,
    /// Statement interpreter kind; defaults to "sql".
    #[serde(default = "default_statement_kind")]
    pub statement_kind: String,
    /// Optional bearer token for Livy auth. Can be "$ENV_VAR".
    pub auth_token: Option<String>,
    /// Optional catalog hint used for scaffold/template generation.
    pub catalog: Option<String>,
    /// Optional schema/namespace hint used for scaffold/template generation.
    pub schema: Option<String>,
    /// HTTP timeout for Livy calls.
    #[serde(default)]
    pub query_timeout_ms: Option<u64>,
    /// Poll interval while waiting for statement completion.
    #[serde(default)]
    pub poll_interval_ms: Option<u64>,
    /// Max polling attempts per statement.
    #[serde(default)]
    pub max_poll_attempts: Option<u32>,
    /// Retry attempts for transient Livy HTTP/transport failures.
    #[serde(default)]
    pub max_retries: Option<u32>,
    /// Base backoff delay in milliseconds for transient retries.
    #[serde(default)]
    pub retry_backoff_ms: Option<u64>,
    /// Max backoff delay in milliseconds for transient retries.
    #[serde(default)]
    pub retry_backoff_max_ms: Option<u64>,
    /// Default row schema strategy for Spark results.
    #[serde(default = "default_schema_strategy")]
    pub schema_strategy: SchemaStrategy,
    /// Max nested object depth to flatten when using `schema_strategy: flatten`.
    #[serde(default)]
    pub flatten_max_depth: Option<u8>,
}

fn default_statement_kind() -> String {
    "sql".to_string()
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

/// Partition and read-hint options inspired by Spark connector partition controls.
#[derive(Debug, Deserialize, Clone)]
pub struct SourcePartitionConfig {
    /// Column/expression used for partition hints and optional range predicates.
    pub column: String,
    /// Target Spark partition count (advisory via SQL hint).
    #[serde(default)]
    pub num_partitions: Option<u32>,
    /// Optional lower bound to constrain reads for the partition column.
    #[serde(default)]
    pub lower_bound: Option<String>,
    /// Optional upper bound to constrain reads for the partition column.
    #[serde(default)]
    pub upper_bound: Option<String>,
}

/// Source specification: supports either a local JSON file or a Spark SQL table/query.
#[derive(Debug, Deserialize, Clone)]
pub struct SourceConfig {
    /// Path to a JSON file containing an array of objects, each representing a row.
    pub file: Option<String>,
    /// Optional table name for Spark SQL-based sources.
    #[serde(alias = "dbtable")]
    pub table: Option<String>,
    /// Optional full SELECT statement for Spark SQL-based sources.
    #[serde(alias = "query", alias = "sql", alias = "statement")]
    pub select: Option<String>,
    /// Optional WHERE clause to append when generating a SELECT from `table`.
    #[serde(rename = "where")]
    pub r#where: Option<String>,
    /// Optional LIMIT cap for result cardinality (Neo4j Spark-like query-count control).
    #[serde(default, alias = "query_count", alias = "queryCount", alias = "count", alias = "limit")]
    pub query_count: Option<u64>,
    /// Optional Spark partition/read hint controls.
    #[serde(default)]
    pub partition: Option<SourcePartitionConfig>,
    /// Optional per-mapping override for row schema handling.
    #[serde(default)]
    pub schema_strategy: Option<SchemaStrategy>,
    /// Optional per-mapping flatten depth override.
    #[serde(default)]
    pub flatten_max_depth: Option<u8>,
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

#[derive(Debug, Deserialize, Clone)]
pub struct DeltaSpec {
    pub updated_at_column: String,
    pub deleted_flag_column: Option<String>,
    pub deleted_flag_value: Option<serde_json::Value>,
    #[serde(default)]
    pub initial_full_load: Option<bool>,
}

#[derive(Debug, Deserialize, Clone)]
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
    #[serde(default, deserialize_with = "deserialize_match_on")]
    pub match_on: Vec<MatchOn>,
    #[serde(default, alias = "labels")]
    pub label_override: Option<Vec<String>>,
    /// Ergonomic shorthand for single-key endpoint matching.
    #[serde(default)]
    pub match_column: Option<String>,
    /// Ergonomic shorthand for single-key endpoint matching.
    #[serde(default)]
    pub match_property: Option<String>,
    /// Ergonomic shorthand for single endpoint label.
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MatchOn {
    pub column: String,
    pub property: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum MatchOnInput {
    One(MatchOn),
    Many(Vec<MatchOn>),
}

fn deserialize_match_on<'de, D>(deserializer: D) -> std::result::Result<Vec<MatchOn>, D::Error>
where
    D: Deserializer<'de>,
{
    let parsed = Option::<MatchOnInput>::deserialize(deserializer)?;
    Ok(match parsed {
        Some(MatchOnInput::One(item)) => vec![item],
        Some(MatchOnInput::Many(items)) => items,
        None => Vec::new(),
    })
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

        // Resolve Spark auth token from environment if the config uses a $VAR reference.
        if let Some(spark_cfg) = cfg.spark.as_mut() {
            if let Some(ref token) = spark_cfg.auth_token {
                if let Some(env_ref) = token.strip_prefix('$') {
                    let env_name = env_ref;
                    let resolved = env::var(env_name).with_context(|| {
                        format!(
                            "Environment variable {} referenced by spark.auth_token is not set",
                            env_name
                        )
                    })?;
                    spark_cfg.auth_token = Some(resolved);
                }
            }
        }

        cfg.normalize_and_validate()?;
        Ok(cfg)
    }

    fn normalize_and_validate(&mut self) -> Result<()> {
        if let Some(spark_cfg) = self.spark.as_ref() {
            validate_spark_config(spark_cfg)?;
        }
        let has_spark = self.spark.is_some();

        for mapping in &mut self.mappings {
            match mapping {
                EntityMapping::Node(node_cfg) => {
                    validate_common_mapping(&node_cfg.common, has_spark)?;
                    if node_cfg.labels.is_empty() {
                        return Err(anyhow!(
                            "Node mapping '{}' must define at least one label",
                            node_cfg.common.name
                        ));
                    }
                    if node_cfg.key.column.trim().is_empty() {
                        return Err(anyhow!(
                            "Node mapping '{}' has an empty key.column",
                            node_cfg.common.name
                        ));
                    }
                    if node_cfg.key.property.trim().is_empty() {
                        return Err(anyhow!(
                            "Node mapping '{}' has an empty key.property",
                            node_cfg.common.name
                        ));
                    }
                }
                EntityMapping::Edge(edge_cfg) => {
                    validate_common_mapping(&edge_cfg.common, has_spark)?;
                    if edge_cfg.relationship.trim().is_empty() {
                        return Err(anyhow!(
                            "Edge mapping '{}' must define relationship",
                            edge_cfg.common.name
                        ));
                    }
                    normalize_edge_endpoint(&mut edge_cfg.from, &edge_cfg.common.name, "from")?;
                    normalize_edge_endpoint(&mut edge_cfg.to, &edge_cfg.common.name, "to")?;
                }
            }
        }

        Ok(())
    }
}

fn validate_spark_config(spark: &SparkConfig) -> Result<()> {
    if spark.livy_url.trim().is_empty() {
        return Err(anyhow!("spark.livy_url must be set"));
    }
    if spark.statement_kind.trim().is_empty() {
        return Err(anyhow!("spark.statement_kind must not be empty"));
    }
    if let Some(ms) = spark.query_timeout_ms {
        if ms == 0 {
            return Err(anyhow!("spark.query_timeout_ms must be > 0 when provided"));
        }
    }
    if let Some(ms) = spark.poll_interval_ms {
        if ms == 0 {
            return Err(anyhow!("spark.poll_interval_ms must be > 0 when provided"));
        }
    }
    if let Some(attempts) = spark.max_poll_attempts {
        if attempts == 0 {
            return Err(anyhow!("spark.max_poll_attempts must be > 0 when provided"));
        }
    }
    if let Some(max_retries) = spark.max_retries {
        if max_retries > 100 {
            return Err(anyhow!(
                "spark.max_retries is too large ({}); expected <= 100",
                max_retries
            ));
        }
    }
    if let Some(base_ms) = spark.retry_backoff_ms {
        if base_ms == 0 {
            return Err(anyhow!(
                "spark.retry_backoff_ms must be > 0 when provided"
            ));
        }
    }
    if let Some(max_ms) = spark.retry_backoff_max_ms {
        if max_ms == 0 {
            return Err(anyhow!(
                "spark.retry_backoff_max_ms must be > 0 when provided"
            ));
        }
    }
    if let (Some(base_ms), Some(max_ms)) = (spark.retry_backoff_ms, spark.retry_backoff_max_ms) {
        if max_ms < base_ms {
            return Err(anyhow!(
                "spark.retry_backoff_max_ms ({}) must be >= spark.retry_backoff_ms ({})",
                max_ms,
                base_ms
            ));
        }
    }
    if let Some(depth) = spark.flatten_max_depth {
        if depth == 0 {
            return Err(anyhow!(
                "spark.flatten_max_depth must be > 0 when provided"
            ));
        }
    }

    Ok(())
}

fn validate_common_mapping(common: &CommonMappingFields, has_spark: bool) -> Result<()> {
    if common.name.trim().is_empty() {
        return Err(anyhow!("mapping.name must not be empty"));
    }
    validate_source_config(&common.name, &common.source)?;
    if !has_spark && (common.source.table.is_some() || common.source.select.is_some()) {
        return Err(anyhow!(
            "Mapping '{}' uses Spark SQL source fields (table/select/query) but `spark` config block is missing",
            common.name
        ));
    }
    if let Mode::Incremental = common.mode {
        if common.delta.is_none() {
            return Err(anyhow!(
                "Mapping '{}' has mode=incremental but no delta block",
                common.name
            ));
        }
    }
    if let Some(delta) = common.delta.as_ref() {
        if delta.updated_at_column.trim().is_empty() {
            return Err(anyhow!(
                "Mapping '{}' has empty delta.updated_at_column",
                common.name
            ));
        }
    }
    Ok(())
}

fn validate_source_config(mapping_name: &str, source: &SourceConfig) -> Result<()> {
    let has_file = source.file.as_ref().map(|v| !v.trim().is_empty()).unwrap_or(false);
    let has_table = source
        .table
        .as_ref()
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false);
    let has_select = source
        .select
        .as_ref()
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false);
    let source_count = [has_file, has_table, has_select]
        .iter()
        .filter(|b| **b)
        .count();
    if source_count != 1 {
        return Err(anyhow!(
            "Mapping '{}' source must define exactly one of source.file, source.table, or source.select/query/sql/statement",
            mapping_name
        ));
    }

    if let Some(query_count) = source.query_count {
        if query_count == 0 {
            return Err(anyhow!(
                "Mapping '{}' has source.query_count=0; expected a positive integer",
                mapping_name
            ));
        }
    }
    if let Some(depth) = source.flatten_max_depth {
        if depth == 0 {
            return Err(anyhow!(
                "Mapping '{}' has source.flatten_max_depth=0; expected > 0",
                mapping_name
            ));
        }
    }
    if let Some(partition) = source.partition.as_ref() {
        if partition.column.trim().is_empty() {
            return Err(anyhow!(
                "Mapping '{}' has source.partition configured but partition.column is empty",
                mapping_name
            ));
        }
        if let Some(num_partitions) = partition.num_partitions {
            if num_partitions == 0 {
                return Err(anyhow!(
                    "Mapping '{}' has source.partition.num_partitions=0; expected > 0",
                    mapping_name
                ));
            }
        }
        let has_lower = partition
            .lower_bound
            .as_ref()
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false);
        let has_upper = partition
            .upper_bound
            .as_ref()
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false);
        if has_lower ^ has_upper {
            return Err(anyhow!(
                "Mapping '{}' must provide both source.partition.lower_bound and source.partition.upper_bound together",
                mapping_name
            ));
        }
    }

    Ok(())
}

fn normalize_edge_endpoint(
    endpoint: &mut EdgeEndpointMatch,
    mapping_name: &str,
    endpoint_name: &str,
) -> Result<()> {
    if endpoint.node_mapping.trim().is_empty() {
        return Err(anyhow!(
            "Edge mapping '{}' endpoint '{}' has empty node_mapping",
            mapping_name,
            endpoint_name
        ));
    }

    if endpoint.label_override.is_none() {
        if let Some(label) = endpoint.label.take() {
            if !label.trim().is_empty() {
                endpoint.label_override = Some(vec![label]);
            }
        }
    }
    if let Some(labels) = endpoint.label_override.as_ref() {
        if labels.is_empty() {
            return Err(anyhow!(
                "Edge mapping '{}' endpoint '{}' has an empty label_override/labels list",
                mapping_name,
                endpoint_name
            ));
        }
    }

    if endpoint.match_on.is_empty() {
        match (
            endpoint.match_column.as_ref().map(|v| v.trim()),
            endpoint.match_property.as_ref().map(|v| v.trim()),
        ) {
            (Some(column), Some(property)) if !column.is_empty() && !property.is_empty() => {
                endpoint.match_on.push(MatchOn {
                    column: column.to_string(),
                    property: property.to_string(),
                });
            }
            (None, None) => {}
            _ => {
                return Err(anyhow!(
                    "Edge mapping '{}' endpoint '{}' shorthand requires both match_column and match_property",
                    mapping_name,
                    endpoint_name
                ))
            }
        }
    }

    if endpoint.match_on.is_empty() {
        return Err(anyhow!(
            "Edge mapping '{}' endpoint '{}' must define match_on (or shorthand match_column/match_property)",
            mapping_name,
            endpoint_name
        ));
    }
    for (idx, item) in endpoint.match_on.iter().enumerate() {
        if item.column.trim().is_empty() || item.property.trim().is_empty() {
            return Err(anyhow!(
                "Edge mapping '{}' endpoint '{}' has empty match_on entry at index {}",
                mapping_name,
                endpoint_name,
                idx
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::{env, fs, path::PathBuf, time::SystemTime};

    fn write_temp_file(contents: &str, ext: &str) -> PathBuf {
        let mut path = env::temp_dir();
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system clock before UNIX_EPOCH")
            .as_nanos();
        path.push(format!("spark_to_falkordb_config_test_{}.{}", nonce, ext));
        fs::write(&path, contents).expect("failed to write temp config file");
        path
    }

    #[test]
    fn config_from_yaml_resolves_env_token() -> Result<()> {
        let env_var = "SPARK_TEST_TOKEN";
        env::set_var(env_var, "super-secret");

        let yaml = r#"
            spark:
              livy_url: "http://localhost:8998"
              session_id: 3
              auth_token: "$SPARK_TEST_TOKEN"
            falkordb:
              endpoint: "falkor://127.0.0.1:6379"
              graph: "test"
            mappings: []
        "#;

        let path = write_temp_file(yaml, "yaml");
        let cfg = Config::from_file(&path)?;
        let spark = cfg.spark.expect("expected spark config");
        assert_eq!(spark.auth_token.as_deref(), Some("super-secret"));
        Ok(())
    }

    #[test]
    fn config_from_json_parses_basic_fields() -> Result<()> {
        let json = r#"
            {
              "spark": null,
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
        assert!(cfg.spark.is_none());
        assert_eq!(cfg.falkordb.endpoint, "falkor://localhost:6379");
        assert_eq!(cfg.falkordb.graph, "test_graph");
        Ok(())
    }

    #[test]
    fn source_aliases_and_edge_shorthand_are_normalized() -> Result<()> {
        let yaml = r#"
spark:
  livy_url: "http://localhost:8998"
  session_id: 2
falkordb:
  endpoint: "falkor://127.0.0.1:6379"
  graph: "test"
mappings:
  - type: node
    name: customers
    source:
      table: "default.customers"
      query_count: 100
      partition:
        column: "customer_id"
        num_partitions: 8
        lower_bound: "1"
        upper_bound: "1000"
    labels: ["Customer"]
    key: { column: "customer_id", property: "id" }
    properties: {}
  - type: node
    name: orders
    source:
      table: "default.orders"
    labels: ["Order"]
    key: { column: "order_id", property: "id" }
    properties: {}
  - type: edge
    name: customer_orders
    source:
      sql: "SELECT customer_id, order_id FROM default.orders"
    relationship: "PLACED"
    from:
      node_mapping: customers
      match_column: customer_id
      match_property: id
      label: Customer
    to:
      node_mapping: orders
      match_on:
        column: order_id
        property: id
    properties: {}
"#;

        let path = write_temp_file(yaml, "yaml");
        let cfg = Config::from_file(&path)?;
        let edge = cfg
            .mappings
            .iter()
            .find_map(|m| match m {
                EntityMapping::Edge(edge) => Some(edge),
                _ => None,
            })
            .expect("expected edge mapping");
        assert_eq!(edge.common.source.select.as_deref(), Some("SELECT customer_id, order_id FROM default.orders"));
        assert_eq!(edge.from.match_on.len(), 1);
        assert_eq!(edge.from.match_on[0].column, "customer_id");
        assert_eq!(edge.from.match_on[0].property, "id");
        assert_eq!(
            edge.from.label_override.as_ref(),
            Some(&vec!["Customer".to_string()])
        );
        Ok(())
    }

    #[test]
    fn invalid_source_selector_combo_fails() {
        let yaml = r#"
spark:
  livy_url: "http://localhost:8998"
  session_id: 3
falkordb:
  endpoint: "falkor://127.0.0.1:6379"
  graph: "test"
mappings:
  - type: node
    name: bad_source
    source:
      table: "default.t1"
      query: "SELECT * FROM default.t1"
    labels: ["T1"]
    key: { column: "id", property: "id" }
    properties: {}
"#;
        let path = write_temp_file(yaml, "yaml");
        let err = Config::from_file(&path).expect_err("expected validation error");
        assert!(err
            .to_string()
            .contains("must define exactly one of source.file, source.table, or source.select/query/sql/statement"));
    }

    #[test]
    fn query_count_must_be_positive() {
        let yaml = r#"
spark:
  livy_url: "http://localhost:8998"
  session_id: 3
falkordb:
  endpoint: "falkor://127.0.0.1:6379"
  graph: "test"
mappings:
  - type: node
    name: bad_limit
    source:
      table: "default.t1"
      query_count: 0
    labels: ["T1"]
    key: { column: "id", property: "id" }
    properties: {}
"#;
        let path = write_temp_file(yaml, "yaml");
        let err = Config::from_file(&path).expect_err("expected validation error");
        assert!(err.to_string().contains("source.query_count=0"));
    }

    #[test]
    fn incremental_mode_requires_delta() {
        let yaml = r#"
spark:
  livy_url: "http://localhost:8998"
  session_id: 3
falkordb:
  endpoint: "falkor://127.0.0.1:6379"
  graph: "test"
mappings:
  - type: node
    name: missing_delta
    source:
      table: "default.t1"
    mode: incremental
    labels: ["T1"]
    key: { column: "id", property: "id" }
    properties: {}
"#;
        let path = write_temp_file(yaml, "yaml");
        let err = Config::from_file(&path).expect_err("expected validation error");
        assert!(err
            .to_string()
            .contains("has mode=incremental but no delta block"));
    }
}
