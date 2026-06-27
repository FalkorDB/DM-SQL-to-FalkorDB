use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{anyhow, Context, Result};
use oracle::Connection;
use serde::{Deserialize, Serialize};

use crate::config::{Config, EdgeDirection, OracleConfig};
use crate::source::oracle_connect_parts;

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
            let table_key = qualified_table_key(&table.schema, &table.name);
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
    pub unique_constraints: Vec<UniqueConstraint>,
    pub foreign_keys: Vec<ForeignKeyMetadata>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueConstraint {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyMetadata {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_schema: String,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
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
    oracle: TemplateOracle,
    falkordb: TemplateFalkor,
    state: TemplateState,
    mappings: Vec<TemplateMapping>,
}

#[derive(Debug, Serialize)]
struct TemplateOracle {
    connect_string: String,
    user: String,
    password: String,
    schema: String,
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
    delta: Option<TemplateDelta>,
    properties: Vec<String>,
}

pub async fn introspect_oracle_schema(cfg: &Config) -> Result<IntrospectionResult> {
    let oracle_cfg = cfg
        .oracle
        .as_ref()
        .ok_or_else(|| anyhow!("oracle config block is required for schema introspection"))?;

    let oracle_cfg_owned = OracleIntrospectionConfig::from(oracle_cfg);
    tokio::task::spawn_blocking(move || introspect_oracle_schema_blocking(&oracle_cfg_owned))
        .await
        .map_err(|e| anyhow!("Oracle introspection task join error: {e}"))?
}

#[derive(Debug, Clone)]
struct OracleIntrospectionConfig {
    user: String,
    password: String,
    connect_string: String,
    schema: Option<String>,
}

impl From<&OracleConfig> for OracleIntrospectionConfig {
    fn from(value: &OracleConfig) -> Self {
        let (user, password, connect_string) =
            oracle_connect_parts(value).expect("oracle connect parts should be valid");
        Self {
            user,
            password,
            connect_string,
            schema: value.schema.clone(),
        }
    }
}

fn introspect_oracle_schema_blocking(
    cfg: &OracleIntrospectionConfig,
) -> Result<IntrospectionResult> {
    let conn = Connection::connect(&cfg.user, &cfg.password, &cfg.connect_string)
        .with_context(|| "Failed to connect to Oracle for introspection")?;

    let current_schema: String = conn.query_row_as(
        "SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual",
        &[],
    )?;
    let schema = cfg
        .schema
        .clone()
        .unwrap_or(current_schema)
        .to_ascii_uppercase();

    let tables = fetch_tables(&conn, &schema)?;
    let columns = fetch_columns(&conn, &schema)?;
    let (pk_by_table, unique_by_table) = fetch_keys(&conn, &schema)?;
    let fk_by_table = fetch_foreign_keys(&conn, &schema)?;
    let source_indexes_by_table = fetch_source_indexes(&conn, &schema)?;

    let mut table_map: HashMap<(String, String), TableMetadata> = HashMap::new();
    for (table_schema, name) in tables {
        table_map.insert(
            (table_schema.clone(), name.clone()),
            TableMetadata {
                schema: table_schema,
                name,
                columns: Vec::new(),
                primary_key: Vec::new(),
                unique_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                source_indexes: Vec::new(),
            },
        );
    }

    for (table_schema, table, column) in columns {
        if let Some(t) = table_map.get_mut(&(table_schema, table)) {
            t.columns.push(column);
        }
    }
    for t in table_map.values_mut() {
        t.columns
            .sort_by_key(|c| (c.ordinal_position, c.name.clone()));
    }

    for ((table_schema, table), pk_cols) in pk_by_table {
        if let Some(t) = table_map.get_mut(&(table_schema, table)) {
            t.primary_key = pk_cols;
        }
    }
    for ((table_schema, table), uniques) in unique_by_table {
        if let Some(t) = table_map.get_mut(&(table_schema, table)) {
            t.unique_constraints = uniques;
        }
    }
    for ((table_schema, table), fks) in fk_by_table {
        if let Some(t) = table_map.get_mut(&(table_schema, table)) {
            t.foreign_keys = fks;
        }
    }
    for ((table_schema, table), source_indexes) in source_indexes_by_table {
        if let Some(t) = table_map.get_mut(&(table_schema, table)) {
            t.source_indexes = source_indexes;
        }
    }

    let mut normalized_tables: Vec<TableMetadata> = table_map.into_values().collect();
    normalized_tables.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(IntrospectionResult {
        schema: SchemaMetadata {
            database: schema,
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
        "# Review labels, relationship names, key choices, and incremental delta settings."
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

    let join_tables: HashSet<String> = schema
        .tables
        .iter()
        .filter(|t| looks_like_join_table(t))
        .map(|t| qualified_table_key(&t.schema, &t.name))
        .collect();

    let mut nodes = Vec::new();
    for table in &schema.tables {
        let key = qualified_table_key(&table.schema, &table.name);
        if join_tables.contains(&key) {
            continue;
        }

        let (key_column, key_note) = choose_key_column(table);
        if let Some(n) = key_note {
            notes.push(format!("table '{}': {}", table.name, n));
        }
        let key_property = default_key_property(&key_column);

        let fk_cols: HashSet<&str> = table
            .foreign_keys
            .iter()
            .flat_map(|fk| fk.columns.iter().map(String::as_str))
            .collect();

        let properties = table
            .columns
            .iter()
            .map(|c| c.name.clone())
            .filter(|c| c != &key_column && !fk_cols.contains(c.as_str()))
            .collect::<Vec<_>>();

        nodes.push(InferredNode {
            table_key: key,
            mapping_name: snake_case(&table.name),
            label: to_label(&table.name),
            key_column: key_column.clone(),
            key_property,
            delta: infer_delta(table),
            properties,
        });
    }
    nodes.sort_by(|a, b| a.mapping_name.cmp(&b.mapping_name));

    let node_by_table: HashMap<&str, &InferredNode> =
        nodes.iter().map(|n| (n.table_key.as_str(), n)).collect();

    let mut edges = Vec::new();
    for table in &schema.tables {
        let child_key = qualified_table_key(&table.schema, &table.name);
        if join_tables.contains(&child_key) {
            if table.foreign_keys.len() >= 2 {
                let fk_a = &table.foreign_keys[0];
                let fk_b = &table.foreign_keys[1];
                let from_key = qualified_table_key(&fk_a.referenced_schema, &fk_a.referenced_table);
                let to_key = qualified_table_key(&fk_b.referenced_schema, &fk_b.referenced_table);
                let Some(from_node) = node_by_table.get(from_key.as_str()) else {
                    notes.push(format!(
                        "join table '{}' references missing node table '{}.{}'",
                        table.name, fk_a.referenced_schema, fk_a.referenced_table
                    ));
                    continue;
                };
                let Some(to_node) = node_by_table.get(to_key.as_str()) else {
                    notes.push(format!(
                        "join table '{}' references missing node table '{}.{}'",
                        table.name, fk_b.referenced_schema, fk_b.referenced_table
                    ));
                    continue;
                };

                let fk_cols: HashSet<&str> = table
                    .foreign_keys
                    .iter()
                    .flat_map(|fk| fk.columns.iter().map(String::as_str))
                    .collect();
                let properties = table
                    .columns
                    .iter()
                    .map(|c| c.name.clone())
                    .filter(|c| !fk_cols.contains(c.as_str()))
                    .collect::<Vec<_>>();

                edges.push(InferredEdge {
                    mapping_name: format!(
                        "{}_{}_{}",
                        snake_case(&from_node.mapping_name),
                        snake_case(&table.name),
                        snake_case(&to_node.mapping_name)
                    ),
                    source_table: table.name.clone(),
                    relationship: to_rel_name(&table.name),
                    from_node_mapping: from_node.mapping_name.clone(),
                    from_column: fk_a.columns.first().cloned().unwrap_or_default(),
                    from_property: from_node.key_property.clone(),
                    to_node_mapping: to_node.mapping_name.clone(),
                    to_column: fk_b.columns.first().cloned().unwrap_or_default(),
                    to_property: to_node.key_property.clone(),
                    delta: infer_delta(table),
                    properties,
                });
            }
            continue;
        }

        for fk in &table.foreign_keys {
            let parent_key = qualified_table_key(&fk.referenced_schema, &fk.referenced_table);
            let Some(from_node) = node_by_table.get(child_key.as_str()) else {
                continue;
            };
            let Some(to_node) = node_by_table.get(parent_key.as_str()) else {
                notes.push(format!(
                    "table '{}' foreign key '{}' points to missing node table '{}.{}'",
                    table.name, fk.name, fk.referenced_schema, fk.referenced_table
                ));
                continue;
            };
            if fk.columns.len() != 1 || fk.referenced_columns.len() != 1 {
                notes.push(format!(
                    "table '{}' foreign key '{}' is composite; generated template uses first column pair",
                    table.name, fk.name
                ));
            }

            edges.push(InferredEdge {
                mapping_name: format!("{}_{}", snake_case(&table.name), snake_case(&fk.name)),
                source_table: table.name.clone(),
                relationship: to_rel_name(&fk.name),
                from_node_mapping: from_node.mapping_name.clone(),
                from_column: fk.columns.first().cloned().unwrap_or_default(),
                from_property: from_node.key_property.clone(),
                to_node_mapping: to_node.mapping_name.clone(),
                to_column: fk.referenced_columns.first().cloned().unwrap_or_default(),
                to_property: to_node.key_property.clone(),
                delta: infer_delta(table),
                properties: Vec::new(),
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

fn build_template_config(
    cfg: &Config,
    draft: &GraphDraft,
    schema: &SchemaMetadata,
) -> TemplateConfig {
    let graph_name = cfg.falkordb.graph.clone();
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
        let mut props = BTreeMap::new();
        for p in &e.properties {
            props.insert(p.clone(), TemplateProperty { column: p.clone() });
        }
        mappings.push(TemplateMapping::Edge(TemplateEdgeMapping {
            common: TemplateCommon {
                name: e.mapping_name.clone(),
                source: TemplateSource {
                    table: infer_qualified_source_table(schema, e),
                },
                mode: if e.delta.is_some() {
                    "incremental".to_string()
                } else {
                    "full".to_string()
                },
                delta: e.delta.clone(),
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
            properties: props,
        }));
    }

    let default_schema = cfg
        .oracle
        .as_ref()
        .and_then(|o| o.schema.clone())
        .or_else(|| cfg.oracle.as_ref().and_then(|o| o.user.clone()))
        .unwrap_or_else(|| schema.database.clone());

    TemplateConfig {
        oracle: TemplateOracle {
            connect_string: "$ORACLE_CONNECT_STRING".to_string(),
            user: "$ORACLE_USER".to_string(),
            password: "$ORACLE_PASSWORD".to_string(),
            schema: default_schema,
            fetch_batch_size: 10_000,
            query_timeout_ms: 60_000,
        },
        falkordb: TemplateFalkor {
            endpoint: "$FALKORDB_ENDPOINT".to_string(),
            graph: if graph_name.trim().is_empty() {
                format!("{}_graph", schema.database.to_ascii_lowercase())
            } else {
                graph_name
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
    let lower_columns = table
        .columns
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let colset: HashSet<&str> = lower_columns.iter().map(String::as_str).collect();

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

fn infer_qualified_source_table(schema: &SchemaMetadata, edge: &InferredEdge) -> String {
    if edge.source_table.contains('.') {
        return edge.source_table.clone();
    }
    if let Some(table) = schema
        .tables
        .iter()
        .find(|t| t.name.eq_ignore_ascii_case(&edge.source_table))
    {
        return format!("{}.{}", table.schema, table.name);
    }
    format!("{}.{}", schema.database, edge.source_table)
}

fn choose_key_column(table: &TableMetadata) -> (String, Option<String>) {
    if table.primary_key.len() == 1 {
        return (table.primary_key[0].clone(), None);
    }
    if table.primary_key.len() > 1 {
        return (
            table.primary_key[0].clone(),
            Some("composite primary key detected; using first key column".to_string()),
        );
    }

    for uc in &table.unique_constraints {
        if uc.columns.len() == 1 {
            return (uc.columns[0].clone(), None);
        }
    }

    if let Some(col) = table.columns.first() {
        return (
            col.name.clone(),
            Some("no primary/unique key found; using first column as temporary key".to_string()),
        );
    }

    (
        "id".to_string(),
        Some("table has no columns; using synthetic key placeholder".to_string()),
    )
}

fn looks_like_join_table(table: &TableMetadata) -> bool {
    if table.foreign_keys.len() < 2 {
        return false;
    }
    let fk_cols: HashSet<&str> = table
        .foreign_keys
        .iter()
        .flat_map(|f| f.columns.iter().map(String::as_str))
        .collect();
    let pk_cols: HashSet<&str> = table.primary_key.iter().map(String::as_str).collect();

    let allowed_meta = [
        "created_at",
        "updated_at",
        "created_on",
        "updated_on",
        "is_deleted",
    ];
    table.columns.iter().all(|c| {
        fk_cols.contains(c.name.as_str())
            || pk_cols.contains(c.name.as_str())
            || allowed_meta.contains(&c.name.as_str())
    })
}

fn default_key_property(column: &str) -> String {
    if column.ends_with("_id") {
        "id".to_string()
    } else {
        column.to_string()
    }
}

fn qualified_table_key(schema: &str, table: &str) -> String {
    format!("{}.{}", schema, table)
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

async fn _not_used_marker() {}

fn fetch_tables(conn: &Connection, schema: &str) -> Result<Vec<(String, String)>> {
    let sql = r#"
        SELECT owner, table_name
        FROM all_tables
        WHERE owner = :1
        ORDER BY table_name
    "#;
    let rows = conn.query_as::<(String, String)>(sql, &[&schema])?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn fetch_columns(conn: &Connection, schema: &str) -> Result<Vec<(String, String, ColumnMetadata)>> {
    let sql = r#"
        SELECT owner, table_name, column_name, column_id, nullable, data_type
        FROM all_tab_columns
        WHERE owner = :1
        ORDER BY table_name, column_id
    "#;
    let rows = conn.query_as::<(String, String, String, u64, String, String)>(sql, &[&schema])?;
    let mut out = Vec::new();
    for row in rows {
        let (table_schema, table, column_name, ordinal_position, nullable, data_type) = row?;
        out.push((
            table_schema,
            table,
            ColumnMetadata {
                name: column_name,
                ordinal_position,
                data_type,
                nullable: nullable.eq_ignore_ascii_case("Y"),
                column_default: None,
            },
        ));
    }
    Ok(out)
}

type KeysByTable = HashMap<(String, String), Vec<String>>;
type UniquesByTable = HashMap<(String, String), Vec<UniqueConstraint>>;
type SourceIndexesByTable = HashMap<(String, String), Vec<SourceIndexMetadata>>;

fn fetch_keys(conn: &Connection, schema: &str) -> Result<(KeysByTable, UniquesByTable)> {
    let sql = r#"
        SELECT c.owner, c.table_name, c.constraint_name, c.constraint_type, cc.column_name, cc.position
        FROM all_constraints c
        JOIN all_cons_columns cc
          ON c.owner = cc.owner
         AND c.constraint_name = cc.constraint_name
        WHERE c.owner = :1
          AND c.constraint_type IN ('P', 'U')
        ORDER BY c.table_name, c.constraint_name, cc.position
    "#;
    let rows = conn.query_as::<(String, String, String, String, String, u64)>(sql, &[&schema])?;

    let mut pk_by_table: HashMap<(String, String), Vec<String>> = HashMap::new();
    let mut unique_rows: HashMap<(String, String), BTreeMap<String, Vec<String>>> = HashMap::new();
    for row in rows {
        let (table_schema, table, constraint_name, constraint_type, column_name, _position) = row?;
        if constraint_type == "P" {
            pk_by_table
                .entry((table_schema, table))
                .or_default()
                .push(column_name);
        } else {
            unique_rows
                .entry((table_schema, table))
                .or_default()
                .entry(constraint_name)
                .or_default()
                .push(column_name);
        }
    }

    let mut unique_by_table: UniquesByTable = HashMap::new();
    for (table_key, by_name) in unique_rows {
        let mut constraints = by_name
            .into_iter()
            .map(|(name, columns)| UniqueConstraint { name, columns })
            .collect::<Vec<_>>();
        constraints.sort_by(|a, b| a.name.cmp(&b.name));
        unique_by_table.insert(table_key, constraints);
    }

    Ok((pk_by_table, unique_by_table))
}

fn fetch_foreign_keys(
    conn: &Connection,
    schema: &str,
) -> Result<HashMap<(String, String), Vec<ForeignKeyMetadata>>> {
    let sql = r#"
        SELECT
          c.owner,
          c.table_name,
          c.constraint_name,
          cc.column_name,
          cc.position,
          rc.owner AS referenced_owner,
          rc.table_name AS referenced_table,
          rcc.column_name AS referenced_column
        FROM all_constraints c
        JOIN all_cons_columns cc
          ON c.owner = cc.owner
         AND c.constraint_name = cc.constraint_name
        JOIN all_constraints rc
          ON c.r_owner = rc.owner
         AND c.r_constraint_name = rc.constraint_name
        JOIN all_cons_columns rcc
          ON rc.owner = rcc.owner
         AND rc.constraint_name = rcc.constraint_name
         AND cc.position = rcc.position
        WHERE c.owner = :1
          AND c.constraint_type = 'R'
        ORDER BY c.table_name, c.constraint_name, cc.position
    "#;
    let rows = conn.query_as::<(String, String, String, String, u64, String, String, String)>(
        sql,
        &[&schema],
    )?;

    type FkRow = (String, String, String, String, u64, String, String, String);
    let mut grouped: HashMap<(String, String), BTreeMap<String, Vec<FkRow>>> = HashMap::new();
    for row in rows {
        let record = row?;
        let table_key = (record.0.clone(), record.1.clone());
        grouped
            .entry(table_key)
            .or_default()
            .entry(record.2.clone())
            .or_default()
            .push(record);
    }

    let mut fk_by_table = HashMap::new();
    for (table_key, by_name) in grouped {
        let mut fks = Vec::new();
        for (fk_name, mut rows) in by_name {
            rows.sort_by_key(|r| r.4);
            let referenced_schema = rows[0].5.clone();
            let referenced_table = rows[0].6.clone();
            let columns = rows.iter().map(|r| r.3.clone()).collect::<Vec<_>>();
            let referenced_columns = rows.iter().map(|r| r.7.clone()).collect::<Vec<_>>();
            fks.push(ForeignKeyMetadata {
                name: fk_name,
                columns,
                referenced_schema,
                referenced_table,
                referenced_columns,
            });
        }
        fks.sort_by(|a, b| a.name.cmp(&b.name));
        fk_by_table.insert(table_key, fks);
    }

    Ok(fk_by_table)
}

fn fetch_source_indexes(conn: &Connection, schema: &str) -> Result<SourceIndexesByTable> {
    let sql = r#"
        SELECT i.table_owner, i.table_name, i.index_name, i.uniqueness, ic.column_position, ic.column_name
        FROM all_indexes i
        JOIN all_ind_columns ic
          ON i.owner = ic.index_owner
         AND i.index_name = ic.index_name
        WHERE i.table_owner = :1
        ORDER BY i.table_name, i.index_name, ic.column_position
    "#;
    let rows = conn.query_as::<(String, String, String, String, u64, String)>(sql, &[&schema])?;

    type RawIndexRow = (String, String, String, String, u64, String);
    let mut grouped: HashMap<(String, String), BTreeMap<String, Vec<RawIndexRow>>> = HashMap::new();
    for row in rows {
        let record = row?;
        grouped
            .entry((record.0.clone(), record.1.clone()))
            .or_default()
            .entry(record.2.clone())
            .or_default()
            .push(record);
    }

    let mut out = HashMap::new();
    for (table_key, by_name) in grouped {
        let mut indexes = Vec::new();
        for (name, mut rows) in by_name {
            rows.sort_by_key(|r| r.4);
            let unique = rows
                .first()
                .map(|r| r.3.eq_ignore_ascii_case("UNIQUE"))
                .unwrap_or(false);
            let columns = rows.iter().map(|r| r.5.clone()).collect::<Vec<_>>();
            indexes.push(SourceIndexMetadata {
                name,
                columns,
                unique,
                primary: false,
            });
        }
        indexes.sort_by(|a, b| a.name.cmp(&b.name));
        out.insert(table_key, indexes);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_table(
        name: &str,
        cols: &[&str],
        pk: &[&str],
        uniques: &[(&str, Vec<&str>)],
        fks: &[(&str, Vec<&str>, &str, Vec<&str>)],
    ) -> TableMetadata {
        TableMetadata {
            schema: "APP".to_string(),
            name: name.to_string(),
            columns: cols
                .iter()
                .enumerate()
                .map(|(i, c)| ColumnMetadata {
                    name: (*c).to_string(),
                    ordinal_position: (i + 1) as u64,
                    data_type: "varchar2".to_string(),
                    nullable: true,
                    column_default: None,
                })
                .collect(),
            primary_key: pk.iter().map(|v| (*v).to_string()).collect(),
            unique_constraints: uniques
                .iter()
                .map(|(n, cols)| UniqueConstraint {
                    name: (*n).to_string(),
                    columns: cols.iter().map(|v| (*v).to_string()).collect(),
                })
                .collect(),
            foreign_keys: fks
                .iter()
                .map(|(n, cols, rt, rcols)| ForeignKeyMetadata {
                    name: (*n).to_string(),
                    columns: cols.iter().map(|v| (*v).to_string()).collect(),
                    referenced_schema: "APP".to_string(),
                    referenced_table: (*rt).to_string(),
                    referenced_columns: rcols.iter().map(|v| (*v).to_string()).collect(),
                })
                .collect(),
            source_indexes: vec![],
        }
    }

    #[test]
    fn singularize_basic() {
        assert_eq!(singularize("customers"), "customer");
        assert_eq!(singularize("categories"), "category");
        assert_eq!(singularize("data"), "data");
    }

    #[test]
    fn join_table_detection() {
        let table = mk_table(
            "order_items",
            &["order_id", "product_id", "created_at"],
            &["order_id", "product_id"],
            &[],
            &[
                (
                    "fk_order_items_order",
                    vec!["order_id"],
                    "orders",
                    vec!["order_id"],
                ),
                (
                    "fk_order_items_product",
                    vec!["product_id"],
                    "products",
                    vec!["product_id"],
                ),
            ],
        );
        assert!(looks_like_join_table(&table));
    }

    #[test]
    fn infer_graph_builds_nodes_and_edges() -> Result<()> {
        let customers = mk_table(
            "customers",
            &["customer_id", "name", "updated_at", "is_deleted"],
            &["customer_id"],
            &[],
            &[],
        );
        let orders = mk_table(
            "orders",
            &["order_id", "customer_id", "total", "updated_at"],
            &["order_id"],
            &[],
            &[(
                "fk_orders_customer",
                vec!["customer_id"],
                "customers",
                vec!["customer_id"],
            )],
        );
        let schema = SchemaMetadata {
            database: "APP".to_string(),
            tables: vec![customers, orders],
        };

        let draft = infer_graph_model(&schema)?;
        assert_eq!(draft.nodes.len(), 2);
        assert_eq!(draft.edges.len(), 1);
        assert_eq!(draft.edges[0].relationship, "FK_ORDERS_CUSTOMER");
        Ok(())
    }
}
