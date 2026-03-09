use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::config::{ClickHouseConfig, Config, EdgeDirection};

#[derive(Debug, Serialize)]
pub struct IntrospectionResult {
    pub schema: SchemaMetadata,
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

fn infer_falkordb_indexes(schema: &SchemaMetadata, draft: &GraphDraft) -> Vec<TemplateFalkorIndex> {
    let indexed_columns_by_table: HashMap<String, HashSet<String>> = schema
        .tables
        .iter()
        .map(|table| {
            let table_key = format!("{}.{}", table.schema, table.name);
            let cols = table
                .source_indexes
                .iter()
                .flat_map(|idx| idx.columns.iter().cloned())
                .map(|c| c.to_ascii_lowercase())
                .collect::<HashSet<_>>();
            (table_key, cols)
        })
        .collect();

    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for node in &draft.nodes {
        let Some(indexed_columns) = indexed_columns_by_table.get(&node.table_key) else {
            continue;
        };
        let labels = vec![node.label.clone()];
        let mut candidates = vec![(node.key_property.clone(), node.key_column.clone())];
        candidates.extend(node.properties.iter().cloned().map(|p| (p.clone(), p)));
        for (property, source_column) in candidates {
            if !indexed_columns.contains(&source_column.to_ascii_lowercase()) {
                continue;
            }
            let dedupe_key = (labels.join(":"), property.clone());
            if !seen.insert(dedupe_key) {
                continue;
            }
            out.push(TemplateFalkorIndex {
                labels: labels.clone(),
                property,
                source_table: Some(node.table_key.clone()),
                source_columns: vec![source_column],
            });
        }
    }

    out.sort_by(|a, b| {
        a.labels
            .join(":")
            .cmp(&b.labels.join(":"))
            .then(a.property.cmp(&b.property))
    });
    out
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMetadata {
    pub database: String,
    pub tables: Vec<TableMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub schema: String,
    pub name: String,
    pub columns: Vec<ColumnMetadata>,
    pub primary_key: Vec<String>,
    #[serde(default)]
    pub source_indexes: Vec<SourceIndexMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    pub ordinal_position: u64,
    pub data_type: String,
    pub nullable: bool,
    pub column_default: Option<String>,
    pub is_primary_key: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceIndexMetadata {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub primary: bool,
}

#[derive(Debug, Serialize)]
struct TemplateConfig {
    clickhouse: TemplateClickHouse,
    falkordb: TemplateFalkor,
    state: TemplateState,
    mappings: Vec<TemplateMapping>,
}

#[derive(Debug, Serialize)]
struct TemplateClickHouse {
    url: String,
    user: String,
    password: String,
    database: String,
    fetch_batch_size: usize,
    query_timeout_ms: u64,
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
    Edge(TemplateEdgeMapping),
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
struct TemplateEdgeEndpoint {
    node_mapping: String,
    match_on: Vec<TemplateMatchOn>,
}

#[derive(Debug, Serialize)]
struct TemplateMatchOn {
    column: String,
    property: String,
}

#[derive(Debug, Serialize)]
struct TemplateEdgeMapping {
    #[serde(flatten)]
    common: TemplateCommon,
    relationship: String,
    direction: EdgeDirection,
    from: TemplateEdgeEndpoint,
    to: TemplateEdgeEndpoint,
    properties: BTreeMap<String, TemplateProperty>,
}

#[derive(Debug, Serialize)]
struct TemplateProperty {
    column: String,
}

#[derive(Debug, Clone)]
struct InferredNode {
    table_key: String,
    mapping_name: String,
    label: String,
    key_column: String,
    key_property: String,
    delta: Option<TemplateDelta>,
    properties: Vec<String>,
}

#[derive(Debug, Clone)]
struct InferredEdge {
    mapping_name: String,
    source_table: String,
    relationship: String,
    from_node_mapping: String,
    from_column: String,
    from_property: String,
    to_node_mapping: String,
    to_column: String,
    to_property: String,
}

pub async fn introspect_clickhouse_schema(cfg: &Config) -> Result<IntrospectionResult> {
    let ch_cfg = cfg
        .clickhouse
        .as_ref()
        .ok_or_else(|| anyhow!("clickhouse config block is required for schema introspection"))?;
    let database = ch_cfg
        .database
        .clone()
        .unwrap_or_else(|| "default".to_string());

    let tables = fetch_tables(ch_cfg, &database).await?;
    let columns = fetch_columns(ch_cfg, &database).await?;
    let extra_source_indexes = fetch_source_indexes(ch_cfg, &database)
        .await
        .unwrap_or_default();

    let mut table_map: HashMap<(String, String), TableMetadata> = HashMap::new();
    for (schema, name) in tables {
        table_map.insert(
            (schema.clone(), name.clone()),
            TableMetadata {
                schema,
                name,
                columns: Vec::new(),
                primary_key: Vec::new(),
                source_indexes: Vec::new(),
            },
        );
    }
    for (schema, table, column) in columns {
        if let Some(t) = table_map.get_mut(&(schema, table)) {
            if column.is_primary_key {
                t.primary_key.push(column.name.clone());
            }
            t.columns.push(column);
        }
    }
    for t in table_map.values_mut() {
        t.columns
            .sort_by_key(|c| (c.ordinal_position, c.name.clone()));
        let mut source_indexes = Vec::new();
        if !t.primary_key.is_empty() {
            source_indexes.push(SourceIndexMetadata {
                name: "PRIMARY_KEY".to_string(),
                columns: t.primary_key.clone(),
                unique: true,
                primary: true,
            });
        }
        if let Some(extra) = extra_source_indexes.get(&(t.schema.clone(), t.name.clone())) {
            source_indexes.extend(extra.clone());
        }
        t.source_indexes = source_indexes;
    }
    let mut normalized_tables: Vec<TableMetadata> = table_map.into_values().collect();
    normalized_tables.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(IntrospectionResult {
        schema: SchemaMetadata {
            database,
            tables: normalized_tables,
        },
    })
}

pub fn generate_template_yaml(cfg: &Config, schema: &SchemaMetadata) -> Result<String> {
    let draft = infer_graph_model(schema)?;
    let template = build_template_config(cfg, &draft, schema);
    let yaml = serde_yaml::to_string(&template)?;

    let mut notes = vec![
        "# Auto-generated template from source schema introspection.".to_string(),
        "# ClickHouse usually does not expose FK relationships; inferred edges are heuristic."
            .to_string(),
        "# Review labels, relationships, and key/delta selections.".to_string(),
        "# ClickHouse index metadata is best-effort (primary key + data skipping indexes)."
            .to_string(),
    ];
    if !draft.notes.is_empty() {
        notes.push("# Notes requiring manual review:".to_string());
        for note in &draft.notes {
            notes.push(format!("# - {}", note));
        }
    }

    Ok(format!("{}\n\n{}", notes.join("\n"), yaml))
}

#[derive(Debug)]
struct GraphDraft {
    nodes: Vec<InferredNode>,
    edges: Vec<InferredEdge>,
    notes: Vec<String>,
}

fn infer_graph_model(schema: &SchemaMetadata) -> Result<GraphDraft> {
    let mut notes = Vec::new();
    let mut nodes = Vec::new();

    for table in &schema.tables {
        let table_key = format!("{}.{}", table.schema, table.name);
        let (key_column, key_note) = choose_key_column(table);
        if let Some(n) = key_note {
            notes.push(format!("table '{}': {}", table.name, n));
        }
        let key_property = default_key_property(&key_column);
        let properties = table
            .columns
            .iter()
            .map(|c| c.name.clone())
            .filter(|c| c != &key_column)
            .collect::<Vec<_>>();

        nodes.push(InferredNode {
            table_key,
            mapping_name: snake_case(&table.name),
            label: to_label(&table.name),
            key_column,
            key_property,
            delta: infer_delta(table),
            properties,
        });
    }
    nodes.sort_by(|a, b| a.mapping_name.cmp(&b.mapping_name));

    let node_by_table: HashMap<&str, &InferredNode> =
        nodes.iter().map(|n| (n.table_key.as_str(), n)).collect();
    let mut edges = Vec::new();
    for child in &schema.tables {
        let child_table_key = format!("{}.{}", child.schema, child.name);
        let Some(child_node) = node_by_table.get(child_table_key.as_str()) else {
            continue;
        };
        for col in child.columns.iter().filter(|c| c.name.ends_with("_id")) {
            if col.name == child_node.key_column {
                continue;
            }
            let mut parent_match: Option<&InferredNode> = None;
            for parent in &nodes {
                if parent.table_key == child_table_key {
                    continue;
                }
                if parent.key_column.eq_ignore_ascii_case(&col.name) {
                    if parent_match.is_some() {
                        parent_match = None;
                        break;
                    }
                    parent_match = Some(parent);
                }
            }
            let Some(parent_node) = parent_match else {
                continue;
            };
            edges.push(InferredEdge {
                mapping_name: format!("{}_{}", child_node.mapping_name, snake_case(&col.name)),
                source_table: child.name.clone(),
                relationship: to_rel_name(&col.name),
                from_node_mapping: child_node.mapping_name.clone(),
                from_column: col.name.clone(),
                from_property: child_node.key_property.clone(),
                to_node_mapping: parent_node.mapping_name.clone(),
                to_column: parent_node.key_column.clone(),
                to_property: parent_node.key_property.clone(),
            });
        }
    }
    edges.sort_by(|a, b| a.mapping_name.cmp(&b.mapping_name));

    Ok(GraphDraft {
        nodes,
        edges,
        notes,
    })
}

fn build_template_config(cfg: &Config, draft: &GraphDraft, schema: &SchemaMetadata) -> TemplateConfig {
    let ch_cfg = cfg.clickhouse.as_ref();
    let mut mappings = Vec::new();

    for n in &draft.nodes {
        let mut props = BTreeMap::new();
        for p in &n.properties {
            props.insert(p.clone(), TemplateProperty { column: p.clone() });
        }
        mappings.push(TemplateMapping::Node(TemplateNodeMapping {
            common: TemplateCommon {
                name: n.mapping_name.clone(),
                source: TemplateSource {
                    table: n.table_key.clone(),
                },
                mode: if n.delta.is_some() {
                    "incremental".to_string()
                } else {
                    "full".to_string()
                },
                delta: n.delta.clone(),
            },
            labels: vec![n.label.clone()],
            key: TemplateNodeKey {
                column: n.key_column.clone(),
                property: n.key_property.clone(),
            },
            properties: props,
        }));
    }

    for e in &draft.edges {
        mappings.push(TemplateMapping::Edge(TemplateEdgeMapping {
            common: TemplateCommon {
                name: e.mapping_name.clone(),
                source: TemplateSource {
                    table: format!("{}.{}", schema.database, e.source_table),
                },
                mode: "full".to_string(),
                delta: None,
            },
            relationship: e.relationship.clone(),
            direction: EdgeDirection::Out,
            from: TemplateEdgeEndpoint {
                node_mapping: e.from_node_mapping.clone(),
                match_on: vec![TemplateMatchOn {
                    column: e.from_column.clone(),
                    property: e.from_property.clone(),
                }],
            },
            to: TemplateEdgeEndpoint {
                node_mapping: e.to_node_mapping.clone(),
                match_on: vec![TemplateMatchOn {
                    column: e.to_column.clone(),
                    property: e.to_property.clone(),
                }],
            },
            properties: BTreeMap::new(),
        }));
    }

    TemplateConfig {
        clickhouse: TemplateClickHouse {
            url: ch_cfg
                .and_then(|c| c.url.clone())
                .unwrap_or_else(|| "$CLICKHOUSE_URL".to_string()),
            user: ch_cfg
                .and_then(|c| c.user.clone())
                .unwrap_or_else(|| "default".to_string()),
            password: "$CLICKHOUSE_PASSWORD".to_string(),
            database: schema.database.clone(),
            fetch_batch_size: ch_cfg.and_then(|c| c.fetch_batch_size).unwrap_or(5_000),
            query_timeout_ms: ch_cfg.and_then(|c| c.query_timeout_ms).unwrap_or(60_000),
        },
        falkordb: TemplateFalkor {
            endpoint: "$FALKORDB_ENDPOINT".to_string(),
            graph: if cfg.falkordb.graph.trim().is_empty() {
                format!("{}_graph", schema.database)
            } else {
                cfg.falkordb.graph.clone()
            },
            max_unwind_batch_size: cfg.falkordb.max_unwind_batch_size.unwrap_or(1000),
            indexes: infer_falkordb_indexes(schema, draft),
        },
        state: TemplateState {
            backend: "file".to_string(),
            file_path: "state.json".to_string(),
        },
        mappings,
    }
}

fn infer_delta(table: &TableMetadata) -> Option<TemplateDelta> {
    let colset: HashSet<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
    let updated = ["updated_at", "updatedon", "modified_at", "last_updated_at", "last_update"]
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
    if let Some(pk) = table.primary_key.first() {
        if table.primary_key.len() > 1 {
            return (
                pk.clone(),
                Some("composite primary key detected; using first key column".to_string()),
            );
        }
        return (pk.clone(), None);
    }
    if let Some(id_like) = table
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case("id") || c.name.ends_with("_id"))
    {
        return (
            id_like.name.clone(),
            Some("no primary key metadata found; using id-like column".to_string()),
        );
    }
    if let Some(col) = table.columns.first() {
        return (
            col.name.clone(),
            Some("no primary key metadata found; using first column".to_string()),
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

fn to_rel_name(name: &str) -> String {
    snake_case(name).to_ascii_uppercase()
}

async fn fetch_tables(ch_cfg: &ClickHouseConfig, database: &str) -> Result<Vec<(String, String)>> {
    let sql = format!(
        "SELECT database, name FROM system.tables WHERE database = '{}' AND engine NOT IN ('View','MaterializedView') ORDER BY name",
        escape_sql_literal(database)
    );
    let rows = run_json_each_row_query(ch_cfg, &sql).await?;
    let mut out = Vec::new();
    for row in rows {
        let db = row
            .get("database")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| anyhow!("invalid system.tables row: missing database"))?
            .to_string();
        let table = row
            .get("name")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| anyhow!("invalid system.tables row: missing name"))?
            .to_string();
        out.push((db, table));
    }
    Ok(out)
}

async fn fetch_columns(
    ch_cfg: &ClickHouseConfig,
    database: &str,
) -> Result<Vec<(String, String, ColumnMetadata)>> {
    let sql = format!(
        "SELECT database, table, name, position, type, is_in_primary_key, default_expression FROM system.columns WHERE database = '{}' ORDER BY table, position",
        escape_sql_literal(database)
    );
    let rows = run_json_each_row_query(ch_cfg, &sql).await?;
    let mut out = Vec::new();
    for row in rows {
        let db = row
            .get("database")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| anyhow!("invalid system.columns row: missing database"))?
            .to_string();
        let table = row
            .get("table")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| anyhow!("invalid system.columns row: missing table"))?
            .to_string();
        let name = row
            .get("name")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| anyhow!("invalid system.columns row: missing name"))?
            .to_string();
        let ordinal_position = row
            .get("position")
            .and_then(JsonValue::as_u64)
            .unwrap_or(0);
        let data_type = row
            .get("type")
            .and_then(JsonValue::as_str)
            .unwrap_or("String")
            .to_string();
        let is_primary_key = row
            .get("is_in_primary_key")
            .and_then(|v| {
                v.as_u64()
                    .map(|n| n == 1)
                    .or_else(|| v.as_bool())
                    .or_else(|| v.as_str().map(|s| s == "1"))
            })
            .unwrap_or(false);
        let column_default = row.get("default_expression").and_then(|v| {
            if v.is_null() {
                None
            } else {
                v.as_str().map(ToOwned::to_owned).or_else(|| Some(v.to_string()))
            }
        });
        out.push((
            db,
            table,
            ColumnMetadata {
                name,
                ordinal_position,
                data_type,
                nullable: true,
                column_default,
                is_primary_key,
            },
        ));
    }
    Ok(out)
}

async fn fetch_source_indexes(
    ch_cfg: &ClickHouseConfig,
    database: &str,
) -> Result<HashMap<(String, String), Vec<SourceIndexMetadata>>> {
    let sql = format!(
        "SELECT database, table, name, expr FROM system.data_skipping_indices WHERE database = '{}' ORDER BY table, name",
        escape_sql_literal(database)
    );
    let rows = run_json_each_row_query(ch_cfg, &sql).await?;
    let mut out: HashMap<(String, String), Vec<SourceIndexMetadata>> = HashMap::new();
    for row in rows {
        let db = row
            .get("database")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| anyhow!("invalid system.data_skipping_indices row: missing database"))?
            .to_string();
        let table = row
            .get("table")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| anyhow!("invalid system.data_skipping_indices row: missing table"))?
            .to_string();
        let name = row
            .get("name")
            .and_then(JsonValue::as_str)
            .unwrap_or("skip_index")
            .to_string();
        let expr = row
            .get("expr")
            .and_then(JsonValue::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        if expr.is_empty() {
            continue;
        }
        out.entry((db, table))
            .or_default()
            .push(SourceIndexMetadata {
                name,
                columns: vec![expr],
                unique: false,
                primary: false,
            });
    }
    for indexes in out.values_mut() {
        indexes.sort_by(|a, b| a.name.cmp(&b.name));
    }
    Ok(out)
}

async fn run_json_each_row_query(
    ch_cfg: &ClickHouseConfig,
    sql_without_format: &str,
) -> Result<Vec<serde_json::Map<String, JsonValue>>> {
    let mut client_builder = Client::builder();
    if let Some(timeout_ms) = ch_cfg.query_timeout_ms {
        client_builder = client_builder.timeout(Duration::from_millis(timeout_ms));
    }
    let client = client_builder
        .build()
        .with_context(|| "Failed to build HTTP client for ClickHouse introspection")?;
    let endpoint = ch_cfg.endpoint();
    let sql = format!("{} FORMAT JSONEachRow", sql_without_format);
    let mut request = client.post(endpoint).body(sql);
    if let Some(db) = &ch_cfg.database {
        request = request.query(&[("database", db)]);
    }
    if let Some(user) = &ch_cfg.user {
        request = request.basic_auth(user, ch_cfg.password.clone());
    } else if let Some(password) = &ch_cfg.password {
        request = request.query(&[("password", password)]);
    }
    let response = request
        .send()
        .await
        .with_context(|| "Failed to send ClickHouse schema introspection request")?
        .error_for_status()
        .with_context(|| "ClickHouse returned an error status for introspection query")?;
    let body = response
        .text()
        .await
        .with_context(|| "Failed to read ClickHouse introspection response body")?;
    let mut out = Vec::new();
    for line in body.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: JsonValue = serde_json::from_str(trimmed)
            .with_context(|| "Failed to parse ClickHouse JSONEachRow line")?;
        match value {
            JsonValue::Object(map) => out.push(map),
            _ => return Err(anyhow!("Expected JSON object from JSONEachRow introspection query")),
        }
    }
    Ok(out)
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

