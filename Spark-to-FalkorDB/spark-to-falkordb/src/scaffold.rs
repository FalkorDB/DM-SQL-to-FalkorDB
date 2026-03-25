use std::collections::{BTreeMap, HashSet};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::config::{Config, SchemaStrategy};
use crate::source::{query_spark_sql, LogicalRow};

#[derive(Debug, Serialize)]
pub struct IntrospectionResult {
    pub schema: SchemaMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMetadata {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub tables: Vec<TableMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub schema: Option<String>,
    pub name: String,
    pub columns: Vec<ColumnMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    pub ordinal_position: u64,
    pub data_type: String,
    pub nullable: Option<bool>,
}

#[derive(Debug, Serialize)]
struct TemplateFalkorIndex {
    labels: Vec<String>,
    property: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_table: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    source_columns: Vec<String>,
}

#[derive(Debug, Serialize)]
struct TemplateConfig {
    spark: TemplateSpark,
    falkordb: TemplateFalkor,
    state: TemplateState,
    mappings: Vec<TemplateMapping>,
}

#[derive(Debug, Serialize)]
struct TemplateSpark {
    livy_url: String,
    session_id: u64,
    statement_kind: String,
    auth_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<String>,
    query_timeout_ms: u64,
    poll_interval_ms: u64,
    max_poll_attempts: u32,
    max_retries: u32,
    retry_backoff_ms: u64,
    retry_backoff_max_ms: u64,
    schema_strategy: SchemaStrategy,
    flatten_max_depth: u8,
}

#[derive(Debug, Serialize)]
struct TemplateFalkor {
    endpoint: String,
    graph: String,
    max_unwind_batch_size: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    indexes: Vec<TemplateFalkorIndex>,
}

#[derive(Debug, Serialize)]
struct TemplateState {
    backend: String,
    file_path: String,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum TemplateMapping {
    Node(TemplateNodeMapping),
}

#[derive(Debug, Serialize)]
struct TemplateCommon {
    name: String,
    source: TemplateSource,
    mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta: Option<TemplateDelta>,
}

#[derive(Debug, Serialize)]
struct TemplateSource {
    table: String,
}

#[derive(Debug, Serialize, Clone)]
struct TemplateDelta {
    updated_at_column: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    deleted_flag_column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deleted_flag_value: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct TemplateNodeMapping {
    #[serde(flatten)]
    common: TemplateCommon,
    labels: Vec<String>,
    key: TemplateNodeKey,
    properties: BTreeMap<String, TemplateProperty>,
}

#[derive(Debug, Serialize)]
struct TemplateNodeKey {
    column: String,
    property: String,
}

#[derive(Debug, Serialize)]
struct TemplateProperty {
    column: String,
}

#[derive(Debug, Clone)]
struct InferredNode {
    mapping_name: String,
    table_ref: String,
    label: String,
    key_column: String,
    key_property: String,
    delta: Option<TemplateDelta>,
    properties: Vec<String>,
}

pub async fn introspect_spark_schema(cfg: &Config) -> Result<IntrospectionResult> {
    let spark_cfg = cfg
        .spark
        .as_ref()
        .ok_or_else(|| anyhow!("spark config block is required for schema introspection"))?;

    let resolved_schema = if spark_cfg.schema.is_some() {
        spark_cfg.schema.clone()
    } else {
        let rows = query_spark_sql(spark_cfg, "SELECT current_database() AS current_database")
            .await
            .unwrap_or_default();
        rows.first()
            .and_then(|r| get_opt_string(r, &["current_database", "CURRENT_DATABASE"]))
    };

    let show_sql = if let Some(schema) = &resolved_schema {
        format!("SHOW TABLES IN {}", quote_identifier(schema))
    } else {
        "SHOW TABLES".to_string()
    };
    let table_rows = query_spark_sql(spark_cfg, &show_sql).await?;

    let mut tables = Vec::new();
    for row in table_rows {
        let table_name = get_opt_string(
            &row,
            &["tableName", "tablename", "table_name", "name", "table"],
        );
        let Some(table_name) = table_name else {
            continue;
        };

        let is_temporary = get_opt_bool(&row, &["isTemporary", "temporary"]).unwrap_or(false);
        if is_temporary {
            continue;
        }

        let row_schema =
            get_opt_string(&row, &["namespace", "database"]).or_else(|| resolved_schema.clone());
        let table_ref = if let Some(schema) = &row_schema {
            format!("{}.{}", schema, table_name)
        } else {
            table_name.clone()
        };
        let describe_sql = format!("DESCRIBE TABLE {}", quote_compound_identifier(&table_ref));
        let describe_rows = query_spark_sql(spark_cfg, &describe_sql)
            .await
            .unwrap_or_default();
        let columns = parse_describe_table_rows(describe_rows);
        if columns.is_empty() {
            continue;
        }

        tables.push(TableMetadata {
            schema: row_schema,
            name: table_name,
            columns,
        });
    }

    tables.sort_by(|a, b| {
        a.schema
            .as_deref()
            .unwrap_or("")
            .cmp(b.schema.as_deref().unwrap_or(""))
            .then(a.name.cmp(&b.name))
    });

    Ok(IntrospectionResult {
        schema: SchemaMetadata {
            catalog: spark_cfg.catalog.clone(),
            schema: resolved_schema,
            tables,
        },
    })
}

pub fn generate_template_yaml(cfg: &Config, schema: &SchemaMetadata) -> Result<String> {
    let (nodes, notes) = infer_nodes(schema);
    let template = build_template_config(cfg, schema, &nodes);
    let yaml = serde_yaml::to_string(&template)?;

    let mut header = vec![
        "# Auto-generated template from source schema introspection.".to_string(),
        "# Review labels, key choices, and incremental delta settings.".to_string(),
        "# Spark schema metadata usually does not expose FK constraints; edges are not auto-inferred."
            .to_string(),
    ];
    if !notes.is_empty() {
        header.push("# Notes requiring manual review:".to_string());
        for note in notes {
            header.push(format!("# - {}", note));
        }
    }

    Ok(format!("{}\n\n{}", header.join("\n"), yaml))
}

fn parse_describe_table_rows(rows: Vec<LogicalRow>) -> Vec<ColumnMetadata> {
    let mut out = Vec::new();
    let mut ordinal = 1u64;

    for row in rows {
        let col_name = get_opt_string(&row, &["col_name", "COL_NAME", "col_name#"])
            .unwrap_or_default()
            .trim()
            .to_string();
        if col_name.is_empty() || col_name.starts_with('#') {
            continue;
        }

        let data_type = get_opt_string(&row, &["data_type", "type", "dataType"])
            .unwrap_or_else(|| "string".to_string());

        let nullable = if data_type.to_ascii_lowercase().contains("not null") {
            Some(false)
        } else {
            None
        };

        out.push(ColumnMetadata {
            name: col_name,
            ordinal_position: ordinal,
            data_type,
            nullable,
        });
        ordinal += 1;
    }

    out
}

fn infer_nodes(schema: &SchemaMetadata) -> (Vec<InferredNode>, Vec<String>) {
    let mut notes = Vec::new();
    let mut nodes = Vec::new();

    for table in &schema.tables {
        let (key_column, key_note) = choose_key_column(table);
        if let Some(note) = key_note {
            let table_ref = table_ref(table);
            notes.push(format!("table '{}': {}", table_ref, note));
        }
        let key_property = default_key_property(&key_column);
        let properties = table
            .columns
            .iter()
            .map(|c| c.name.clone())
            .filter(|c| c != &key_column)
            .collect::<Vec<_>>();

        nodes.push(InferredNode {
            mapping_name: snake_case(&table.name),
            table_ref: table_ref(table),
            label: to_label(&table.name),
            key_column: key_column.clone(),
            key_property,
            delta: infer_delta(table),
            properties,
        });
    }

    nodes.sort_by(|a, b| a.mapping_name.cmp(&b.mapping_name));
    (nodes, notes)
}

fn build_template_config(cfg: &Config, schema: &SchemaMetadata, nodes: &[InferredNode]) -> TemplateConfig {
    let spark_cfg = cfg.spark.as_ref();
    let mut mappings = Vec::new();

    for node in nodes {
        let mut props = BTreeMap::new();
        for prop in &node.properties {
            props.insert(
                prop.clone(),
                TemplateProperty {
                    column: prop.clone(),
                },
            );
        }

        mappings.push(TemplateMapping::Node(TemplateNodeMapping {
            common: TemplateCommon {
                name: node.mapping_name.clone(),
                source: TemplateSource {
                    table: node.table_ref.clone(),
                },
                mode: if node.delta.is_some() {
                    "incremental".to_string()
                } else {
                    "full".to_string()
                },
                delta: node.delta.clone(),
            },
            labels: vec![node.label.clone()],
            key: TemplateNodeKey {
                column: node.key_column.clone(),
                property: node.key_property.clone(),
            },
            properties: props,
        }));
    }

    TemplateConfig {
        spark: TemplateSpark {
            livy_url: spark_cfg
                .map(|s| s.livy_url.clone())
                .unwrap_or_else(|| "$SPARK_LIVY_URL".to_string()),
            session_id: spark_cfg.map(|s| s.session_id).unwrap_or(0),
            statement_kind: spark_cfg
                .map(|s| s.statement_kind.clone())
                .unwrap_or_else(|| "sql".to_string()),
            auth_token: "$SPARK_AUTH_TOKEN".to_string(),
            catalog: schema.catalog.clone(),
            schema: schema.schema.clone(),
            query_timeout_ms: spark_cfg.and_then(|s| s.query_timeout_ms).unwrap_or(60_000),
            poll_interval_ms: spark_cfg.and_then(|s| s.poll_interval_ms).unwrap_or(300),
            max_poll_attempts: spark_cfg.and_then(|s| s.max_poll_attempts).unwrap_or(400),
            max_retries: spark_cfg.and_then(|s| s.max_retries).unwrap_or(3),
            retry_backoff_ms: spark_cfg.and_then(|s| s.retry_backoff_ms).unwrap_or(250),
            retry_backoff_max_ms: spark_cfg
                .and_then(|s| s.retry_backoff_max_ms)
                .unwrap_or(5_000),
            schema_strategy: spark_cfg
                .map(|s| s.schema_strategy)
                .unwrap_or(SchemaStrategy::Preserve),
            flatten_max_depth: spark_cfg.and_then(|s| s.flatten_max_depth).unwrap_or(2),
        },
        falkordb: TemplateFalkor {
            endpoint: "$FALKORDB_ENDPOINT".to_string(),
            graph: if cfg.falkordb.graph.trim().is_empty() {
                "spark_graph".to_string()
            } else {
                cfg.falkordb.graph.clone()
            },
            max_unwind_batch_size: cfg.falkordb.max_unwind_batch_size.unwrap_or(1000),
            indexes: infer_falkordb_indexes(nodes),
        },
        state: TemplateState {
            backend: "file".to_string(),
            file_path: "state.json".to_string(),
        },
        mappings,
    }
}

fn infer_falkordb_indexes(nodes: &[InferredNode]) -> Vec<TemplateFalkorIndex> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();

    for node in nodes {
        let labels = vec![node.label.clone()];
        let dedupe_key = (labels.join(":"), node.key_property.clone());
        if !seen.insert(dedupe_key) {
            continue;
        }

        out.push(TemplateFalkorIndex {
            labels,
            property: node.key_property.clone(),
            source_table: Some(node.table_ref.clone()),
            source_columns: vec![node.key_column.clone()],
        });
    }

    out.sort_by(|a, b| {
        a.labels
            .join(":")
            .cmp(&b.labels.join(":"))
            .then(a.property.cmp(&b.property))
    });
    out
}

fn infer_delta(table: &TableMetadata) -> Option<TemplateDelta> {
    let colset: HashSet<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
    let updated = [
        "updated_at",
        "updatedon",
        "modified_at",
        "last_updated_at",
        "last_update",
    ]
    .iter()
    .find(|c| colset.contains(**c))
    .map(|s| (*s).to_string())?;
    let deleted = ["is_deleted", "deleted", "is_active"]
        .iter()
        .find(|c| colset.contains(**c))
        .map(|s| (*s).to_string());
    let deleted_value = match deleted.as_deref() {
        Some("is_active") => Some(serde_json::Value::from(0)),
        Some(_) => Some(serde_json::Value::from(1)),
        None => None,
    };
    Some(TemplateDelta {
        updated_at_column: updated,
        deleted_flag_column: deleted,
        deleted_flag_value: deleted_value,
    })
}

fn choose_key_column(table: &TableMetadata) -> (String, Option<String>) {
    let normalized = table
        .columns
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect::<Vec<_>>();

    if let Some(idx) = normalized.iter().position(|c| c == "id") {
        return (table.columns[idx].name.clone(), None);
    }

    let singular = singularize(&table.name);
    let preferred = format!("{}_id", singular.to_ascii_lowercase());
    if let Some(idx) = normalized.iter().position(|c| c == &preferred) {
        return (table.columns[idx].name.clone(), None);
    }

    if let Some(idx) = normalized.iter().position(|c| c.ends_with("_id")) {
        return (table.columns[idx].name.clone(), None);
    }

    if let Some(first) = table.columns.first() {
        return (
            first.name.clone(),
            Some("no obvious id column found; using first column as temporary key".to_string()),
        );
    }

    (
        "id".to_string(),
        Some("table has no columns; using synthetic key placeholder".to_string()),
    )
}

fn default_key_property(column: &str) -> String {
    if column.ends_with("_id") {
        "id".to_string()
    } else {
        column.to_string()
    }
}

fn table_ref(table: &TableMetadata) -> String {
    if let Some(schema) = &table.schema {
        format!("{}.{}", schema, table.name)
    } else {
        table.name.clone()
    }
}

fn to_label(name: &str) -> String {
    let singular = singularize(name);
    singular
        .split('_')
        .filter(|s| !s.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<String>()
}

fn singularize(name: &str) -> String {
    if name.ends_with("ies") && name.len() > 3 {
        format!("{}y", &name[..name.len() - 3])
    } else if name.ends_with('s') && name.len() > 1 {
        name[..name.len() - 1].to_string()
    } else {
        name.to_string()
    }
}

fn snake_case(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect::<String>()
}

fn get_opt_string(row: &LogicalRow, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(v) = row.get(*key) {
            match v {
                serde_json::Value::Null => {}
                serde_json::Value::String(s) => return Some(s.clone()),
                serde_json::Value::Number(n) => return Some(n.to_string()),
                serde_json::Value::Bool(b) => {
                    return Some(if *b { "true" } else { "false" }.to_string())
                }
                other => return Some(other.to_string()),
            }
        }
    }
    None
}

fn get_opt_bool(row: &LogicalRow, keys: &[&str]) -> Option<bool> {
    for key in keys {
        if let Some(v) = row.get(*key) {
            match v {
                serde_json::Value::Bool(b) => return Some(*b),
                serde_json::Value::String(s) => {
                    if s.eq_ignore_ascii_case("true") {
                        return Some(true);
                    }
                    if s.eq_ignore_ascii_case("false") {
                        return Some(false);
                    }
                }
                _ => {}
            }
        }
    }
    None
}

fn quote_identifier(ident: &str) -> String {
    format!("`{}`", ident.replace('`', ""))
}

fn quote_compound_identifier(ident: &str) -> String {
    ident
        .split('.')
        .filter(|s| !s.trim().is_empty())
        .map(|p| quote_identifier(p.trim()))
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn singularize_basic() {
        assert_eq!(singularize("customers"), "customer");
        assert_eq!(singularize("categories"), "category");
        assert_eq!(singularize("data"), "data");
    }

    #[test]
    fn infer_delta_detects_last_update() {
        let table = TableMetadata {
            schema: Some("default".to_string()),
            name: "customer".to_string(),
            columns: vec![
                ColumnMetadata {
                    name: "id".to_string(),
                    ordinal_position: 1,
                    data_type: "bigint".to_string(),
                    nullable: Some(false),
                },
                ColumnMetadata {
                    name: "last_update".to_string(),
                    ordinal_position: 2,
                    data_type: "timestamp".to_string(),
                    nullable: Some(true),
                },
            ],
        };
        let delta = infer_delta(&table).expect("expected last_update delta");
        assert_eq!(delta.updated_at_column, "last_update");
    }
}
