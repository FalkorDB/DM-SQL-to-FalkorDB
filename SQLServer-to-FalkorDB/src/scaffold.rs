use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{anyhow, Context, Result};
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use tiberius::{AuthMethod, Client, Config as DriverConfig, QueryItem};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::config::{Config, EdgeDirection, SqlServerConfig};

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

type SqlServerClient = Client<Compat<TcpStream>>;

#[derive(Debug, Serialize)]
struct TemplateConfig {
    sqlserver: TemplateSqlServer,
    falkordb: TemplateFalkor,
    state: TemplateState,
    mappings: Vec<TemplateMapping>,
}

#[derive(Debug, Serialize)]
struct TemplateSqlServer {
    connection_string: String,
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

#[derive(Debug)]
struct GraphDraft {
    nodes: Vec<InferredNode>,
    edges: Vec<InferredEdge>,
    notes: Vec<String>,
}

pub async fn introspect_sqlserver_schema(cfg: &Config) -> Result<IntrospectionResult> {
    let sql_cfg = cfg
        .sqlserver
        .as_ref()
        .ok_or_else(|| anyhow!("sqlserver config block is required for schema introspection"))?;
    let mut client = connect_sqlserver(sql_cfg).await?;

    let db_rows = query_json_rows(&mut client, "SELECT DB_NAME() AS database_name").await?;
    let database = db_rows
        .first()
        .and_then(|r| r.get("database_name"))
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .ok_or_else(|| anyhow!("Failed to determine current SQL Server database"))?;

    let table_rows = query_json_rows(
        &mut client,
        r#"
            SELECT s.name AS schema_name, t.name AS table_name
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            ORDER BY s.name, t.name
        "#,
    )
    .await?;
    let index_rows = query_json_rows(
        &mut client,
        r#"
            SELECT
              s.name AS schema_name,
              t.name AS table_name,
              i.name AS index_name,
              i.is_unique AS is_unique,
              i.is_primary_key AS is_primary,
              ic.key_ordinal AS ordinal_position,
              c.name AS column_name
            FROM sys.indexes i
            JOIN sys.tables t ON t.object_id = i.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c ON c.object_id = i.object_id AND c.column_id = ic.column_id
            WHERE i.index_id > 0
              AND i.is_hypothetical = 0
              AND ic.key_ordinal > 0
            ORDER BY s.name, t.name, i.name, ic.key_ordinal
        "#,
    )
    .await?;
    let column_rows = query_json_rows(
        &mut client,
        r#"
            SELECT
              s.name AS schema_name,
              t.name AS table_name,
              c.name AS column_name,
              c.column_id AS ordinal_position,
              ty.name AS data_type,
              c.is_nullable AS is_nullable,
              OBJECT_DEFINITION(c.default_object_id) AS column_default
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.columns c ON c.object_id = t.object_id
            JOIN sys.types ty ON c.user_type_id = ty.user_type_id
            ORDER BY s.name, t.name, c.column_id
        "#,
    )
    .await?;
    let key_rows = query_json_rows(
        &mut client,
        r#"
            SELECT
              s.name AS schema_name,
              t.name AS table_name,
              kc.name AS constraint_name,
              kc.type_desc AS constraint_type,
              c.name AS column_name,
              ic.key_ordinal AS ordinal_position
            FROM sys.key_constraints kc
            JOIN sys.tables t ON t.object_id = kc.parent_object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            JOIN sys.index_columns ic ON ic.object_id = t.object_id AND ic.index_id = kc.unique_index_id
            JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = ic.column_id
            WHERE kc.type IN ('PK','UQ')
            ORDER BY s.name, t.name, kc.name, ic.key_ordinal
        "#,
    )
    .await?;
    let fk_rows = query_json_rows(
        &mut client,
        r#"
            SELECT
              sch.name AS schema_name,
              t.name AS table_name,
              fk.name AS fk_name,
              c.name AS column_name,
              fkc.constraint_column_id AS ordinal_position,
              sch_ref.name AS referenced_schema,
              t_ref.name AS referenced_table,
              c_ref.name AS referenced_column
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
            JOIN sys.tables t ON t.object_id = fk.parent_object_id
            JOIN sys.schemas sch ON sch.schema_id = t.schema_id
            JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = fkc.parent_column_id
            JOIN sys.tables t_ref ON t_ref.object_id = fk.referenced_object_id
            JOIN sys.schemas sch_ref ON sch_ref.schema_id = t_ref.schema_id
            JOIN sys.columns c_ref ON c_ref.object_id = t_ref.object_id AND c_ref.column_id = fkc.referenced_column_id
            ORDER BY sch.name, t.name, fk.name, fkc.constraint_column_id
        "#,
    )
    .await?;

    let mut table_map: HashMap<(String, String), TableMetadata> = HashMap::new();
    for row in table_rows {
        let schema = as_string(&row, "schema_name")?;
        let table = as_string(&row, "table_name")?;
        table_map.insert(
            (schema.clone(), table.clone()),
            TableMetadata {
                schema,
                name: table,
                columns: Vec::new(),
                primary_key: Vec::new(),
                unique_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                source_indexes: Vec::new(),
            },
        );
    }

    for row in column_rows {
        let schema = as_string(&row, "schema_name")?;
        let table = as_string(&row, "table_name")?;
        if let Some(t) = table_map.get_mut(&(schema, table)) {
            t.columns.push(ColumnMetadata {
                name: as_string(&row, "column_name")?,
                ordinal_position: as_u64(&row, "ordinal_position")?,
                data_type: as_string(&row, "data_type")?,
                nullable: as_bool(&row, "is_nullable"),
                column_default: row
                    .get("column_default")
                    .and_then(JsonValue::as_str)
                    .map(str::to_string),
            });
        }
    }

    type TableKey = (String, String);
    let mut pk_by_table: HashMap<TableKey, Vec<String>> = HashMap::new();
    let mut unique_rows: HashMap<TableKey, BTreeMap<String, Vec<String>>> = HashMap::new();
    for row in key_rows {
        let schema = as_string(&row, "schema_name")?;
        let table = as_string(&row, "table_name")?;
        let key_name = as_string(&row, "constraint_name")?;
        let key_type = as_string(&row, "constraint_type")?;
        let column = as_string(&row, "column_name")?;

        if key_type.contains("PRIMARY_KEY") {
            pk_by_table.entry((schema, table)).or_default().push(column);
        } else {
            unique_rows
                .entry((schema, table))
                .or_default()
                .entry(key_name)
                .or_default()
                .push(column);
        }
    }
    for ((schema, table), pk) in pk_by_table {
        if let Some(t) = table_map.get_mut(&(schema, table)) {
            t.primary_key = pk;
        }
    }
    for ((schema, table), uqs) in unique_rows {
        if let Some(t) = table_map.get_mut(&(schema, table)) {
            t.unique_constraints = uqs
                .into_iter()
                .map(|(name, columns)| UniqueConstraint { name, columns })
                .collect();
            t.unique_constraints.sort_by(|a, b| a.name.cmp(&b.name));
        }
    }

    type FkTuple = (String, String, String, u64, String, String, String);
    let mut fk_group: HashMap<TableKey, BTreeMap<String, Vec<FkTuple>>> = HashMap::new();
    for row in fk_rows {
        let schema = as_string(&row, "schema_name")?;
        let table = as_string(&row, "table_name")?;
        let fk_name = as_string(&row, "fk_name")?;
        let tuple = (
            as_string(&row, "column_name")?,
            as_string(&row, "referenced_schema")?,
            as_string(&row, "referenced_table")?,
            as_u64(&row, "ordinal_position")?,
            as_string(&row, "referenced_column")?,
            schema.clone(),
            table.clone(),
        );
        fk_group
            .entry((schema, table))
            .or_default()
            .entry(fk_name)
            .or_default()
            .push(tuple);
    }
    for ((schema, table), by_name) in fk_group {
        if let Some(t) = table_map.get_mut(&(schema, table)) {
            let mut fks = Vec::new();
            for (name, mut tuples) in by_name {
                tuples.sort_by_key(|v| v.3);
                let referenced_schema = tuples[0].1.clone();
                let referenced_table = tuples[0].2.clone();
                let columns = tuples.iter().map(|v| v.0.clone()).collect::<Vec<_>>();
                let referenced_columns = tuples.iter().map(|v| v.4.clone()).collect::<Vec<_>>();
                fks.push(ForeignKeyMetadata {
                    name,
                    columns,
                    referenced_schema,
                    referenced_table,
                    referenced_columns,
                });
            }
            fks.sort_by(|a, b| a.name.cmp(&b.name));
            t.foreign_keys = fks;
        }
    }

    let mut index_group: HashMap<TableKey, BTreeMap<String, Vec<(String, bool, bool, u64)>>> =
        HashMap::new();
    for row in index_rows {
        let schema = as_string(&row, "schema_name")?;
        let table = as_string(&row, "table_name")?;
        let index_name = as_string(&row, "index_name")?;
        let column_name = as_string(&row, "column_name")?;
        let is_unique = as_bool(&row, "is_unique");
        let is_primary = as_bool(&row, "is_primary");
        let ordinal_position = as_u64(&row, "ordinal_position")?;
        index_group
            .entry((schema, table))
            .or_default()
            .entry(index_name)
            .or_default()
            .push((column_name, is_unique, is_primary, ordinal_position));
    }
    for ((schema, table), by_name) in index_group {
        if let Some(t) = table_map.get_mut(&(schema, table)) {
            let mut source_indexes = Vec::new();
            for (index_name, mut rows) in by_name {
                rows.sort_by_key(|v| v.3);
                let unique = rows.first().map(|v| v.1).unwrap_or(false);
                let primary = rows.first().map(|v| v.2).unwrap_or(false);
                let columns = rows.into_iter().map(|v| v.0).collect::<Vec<_>>();
                source_indexes.push(SourceIndexMetadata {
                    name: index_name,
                    columns,
                    unique,
                    primary,
                });
            }
            source_indexes.sort_by(|a, b| a.name.cmp(&b.name));
            t.source_indexes = source_indexes;
        }
    }

    let mut tables = table_map.into_values().collect::<Vec<_>>();
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    for t in &mut tables {
        t.columns
            .sort_by_key(|c| (c.ordinal_position, c.name.clone()));
    }

    Ok(IntrospectionResult {
        schema: SchemaMetadata { database, tables },
    })
}

pub fn generate_template_yaml(cfg: &Config, schema: &SchemaMetadata) -> Result<String> {
    let draft = infer_graph_model(schema);
    let template = build_template_config(cfg, &draft, schema);
    let yaml = serde_yaml::to_string(&template)?;
    let mut notes = vec![
        "# Auto-generated template from source schema introspection.".to_string(),
        "# Review labels, relationship names, key choices, and incremental delta settings.".to_string(),
    ];
    if !draft.notes.is_empty() {
        notes.push("# Notes requiring manual review:".to_string());
        for note in &draft.notes {
            notes.push(format!("# - {}", note));
        }
    }
    Ok(format!("{}\n\n{}", notes.join("\n"), yaml))
}

fn infer_graph_model(schema: &SchemaMetadata) -> GraphDraft {
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
    for table in &schema.tables {
        let child_key = qualified_table_key(&table.schema, &table.name);
        if join_tables.contains(&child_key) {
            if table.foreign_keys.len() >= 2 {
                let fk_a = &table.foreign_keys[0];
                let fk_b = &table.foreign_keys[1];
                let from_key = qualified_table_key(&fk_a.referenced_schema, &fk_a.referenced_table);
                let to_key = qualified_table_key(&fk_b.referenced_schema, &fk_b.referenced_table);
                let Some(from_node) = node_by_table.get(from_key.as_str()) else {
                    continue;
                };
                let Some(to_node) = node_by_table.get(to_key.as_str()) else {
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
    GraphDraft { nodes, edges, notes }
}

fn build_template_config(cfg: &Config, draft: &GraphDraft, schema: &SchemaMetadata) -> TemplateConfig {
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
                mode: if n.delta.is_some() { "incremental".to_string() } else { "full".to_string() },
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
                mode: if e.delta.is_some() { "incremental".to_string() } else { "full".to_string() },
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
    TemplateConfig {
        sqlserver: TemplateSqlServer {
            connection_string: "$SQLSERVER_CONNECTION_STRING".to_string(),
            fetch_batch_size: 10_000,
            query_timeout_ms: 60_000,
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

async fn connect_sqlserver(sqlserver_cfg: &SqlServerConfig) -> Result<SqlServerClient> {
    let driver_cfg = build_driver_config(sqlserver_cfg)?;
    let tcp = TcpStream::connect(driver_cfg.get_addr())
        .await
        .with_context(|| "Failed to open TCP connection to SQL Server")?;
    tcp.set_nodelay(true)
        .with_context(|| "Failed to configure TCP_NODELAY for SQL Server connection")?;
    let client = Client::connect(driver_cfg, tcp.compat_write())
        .await
        .with_context(|| "Failed to connect to SQL Server over TDS")?;
    Ok(client)
}

fn build_driver_config(sqlserver_cfg: &SqlServerConfig) -> Result<DriverConfig> {
    if let Some(connection_string) = &sqlserver_cfg.connection_string {
        let mut cfg = DriverConfig::from_ado_string(connection_string)
            .with_context(|| "Failed to parse SQL Server `sqlserver.connection_string`")?;
        if sqlserver_cfg.trust_cert.unwrap_or(true) {
            cfg.trust_cert();
        }
        return Ok(cfg);
    }
    let host = sqlserver_cfg
        .host
        .clone()
        .unwrap_or_else(|| "localhost".to_string());
    let port = sqlserver_cfg.port.unwrap_or(1433);
    let user = sqlserver_cfg
        .user
        .clone()
        .unwrap_or_else(|| "sa".to_string());
    let password = sqlserver_cfg.password.clone().unwrap_or_default();
    let database = sqlserver_cfg
        .database
        .clone()
        .unwrap_or_else(|| "master".to_string());
    let mut cfg = DriverConfig::new();
    cfg.host(host);
    cfg.port(port);
    cfg.authentication(AuthMethod::sql_server(user, password));
    cfg.database(database);
    if sqlserver_cfg.trust_cert.unwrap_or(true) {
        cfg.trust_cert();
    }
    Ok(cfg)
}

async fn query_json_rows(client: &mut SqlServerClient, sql: &str) -> Result<Vec<JsonMap<String, JsonValue>>> {
    let wrapped_sql = format!(
        "SELECT (SELECT src.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES) AS row_json FROM ({}) AS src",
        sql
    );
    let mut stream = client.simple_query(wrapped_sql).await?;
    let mut out = Vec::new();
    while let Some(item) = stream.try_next().await? {
        if let QueryItem::Row(row) = item {
            let row_json: Option<&str> = row.get("row_json");
            let Some(raw_json) = row_json else {
                continue;
            };
            if raw_json.trim().is_empty() {
                continue;
            }
            let parsed: JsonValue = serde_json::from_str(raw_json)?;
            let obj = parsed
                .as_object()
                .cloned()
                .ok_or_else(|| anyhow!("Expected SQL Server JSON object row"))?;
            out.push(obj);
        }
    }
    Ok(out)
}

fn as_string(row: &JsonMap<String, JsonValue>, key: &str) -> Result<String> {
    row.get(key)
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .ok_or_else(|| anyhow!("Missing string field '{}'", key))
}

fn as_u64(row: &JsonMap<String, JsonValue>, key: &str) -> Result<u64> {
    row.get(key)
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| anyhow!("Missing numeric field '{}'", key))
}

fn as_bool(row: &JsonMap<String, JsonValue>, key: &str) -> bool {
    row.get(key)
        .and_then(JsonValue::as_bool)
        .or_else(|| row.get(key).and_then(JsonValue::as_u64).map(|v| v != 0))
        .unwrap_or(false)
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
    format!("dbo.{}", edge.source_table)
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
    ("id".to_string(), Some("table has no columns; using synthetic key placeholder".to_string()))
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
    let allowed_meta = ["created_at", "updated_at", "created_on", "updated_on", "is_deleted"];
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn singularize_basic() {
        assert_eq!(singularize("customers"), "customer");
        assert_eq!(singularize("categories"), "category");
        assert_eq!(singularize("data"), "data");
    }
}
