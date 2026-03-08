use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use tokio_postgres::{Client, NoTls};

use crate::config::{Config, EdgeDirection, PostgresConfig};

#[derive(Debug, Serialize)]
pub struct IntrospectionResult {
    pub schema: SchemaMetadata,
}

fn infer_qualified_source_table(schema: &SchemaMetadata, edge: &InferredEdge) -> String {
    let source_lc = edge.source_table.to_ascii_lowercase();
    if let Some(table) = schema
        .tables
        .iter()
        .find(|t| t.name.eq_ignore_ascii_case(&source_lc) || t.name.eq_ignore_ascii_case(&edge.source_table))
    {
        return format!("{}.{}", table.schema, table.name);
    }
    if edge.source_table.contains('.') {
        edge.source_table.clone()
    } else {
        format!("public.{}", edge.source_table)
    }
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

#[derive(Debug, Serialize)]
struct TemplateConfig {
    postgres: TemplatePostgres,
    falkordb: TemplateFalkor,
    state: TemplateState,
    mappings: Vec<TemplateMapping>,
}

#[derive(Debug, Serialize)]
struct TemplatePostgres {
    url: String,
    fetch_batch_size: usize,
    query_timeout_ms: u64,
}

#[derive(Debug, Serialize)]
struct TemplateFalkor {
    endpoint: String,
    graph: String,
    max_unwind_batch_size: usize,
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

pub async fn introspect_postgres_schema(cfg: &Config) -> Result<IntrospectionResult> {
    let pg_cfg = cfg
        .postgres
        .as_ref()
        .ok_or_else(|| anyhow!("postgres config block is required for schema introspection"))?;
    let client = connect_postgres(pg_cfg).await?;

    let database: String = client
        .query_one("SELECT current_database() AS database_name", &[])
        .await?
        .try_get("database_name")?;

    let table_rows = client
        .query(
            r#"
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('pg_catalog','information_schema')
            ORDER BY table_schema, table_name
            "#,
            &[],
        )
        .await?;

    let column_rows = client
        .query(
            r#"
            SELECT table_schema, table_name, column_name, ordinal_position, is_nullable, data_type, column_default
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog','information_schema')
            ORDER BY table_schema, table_name, ordinal_position
            "#,
            &[],
        )
        .await?;

    let key_rows = client
        .query(
            r#"
            SELECT
              tc.table_schema,
              tc.table_name,
              tc.constraint_name,
              tc.constraint_type,
              kcu.column_name,
              kcu.ordinal_position
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_schema = kcu.constraint_schema
             AND tc.table_name = kcu.table_name
             AND tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema NOT IN ('pg_catalog','information_schema')
              AND tc.constraint_type IN ('PRIMARY KEY','UNIQUE')
            ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position
            "#,
            &[],
        )
        .await?;

    let fk_rows = client
        .query(
            r#"
            SELECT
              tc.table_schema,
              tc.table_name,
              tc.constraint_name,
              kcu.column_name,
              kcu.ordinal_position,
              ccu.table_schema AS referenced_schema,
              ccu.table_name AS referenced_table,
              ccu.column_name AS referenced_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_schema = kcu.constraint_schema
             AND tc.table_name = kcu.table_name
             AND tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
              ON tc.constraint_schema = ccu.constraint_schema
             AND tc.constraint_name = ccu.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema NOT IN ('pg_catalog','information_schema')
            ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position
            "#,
            &[],
        )
        .await?;

    let mut table_map: HashMap<(String, String), TableMetadata> = HashMap::new();
    for row in table_rows {
        let schema: String = row.try_get("table_schema")?;
        let name: String = row.try_get("table_name")?;
        table_map.insert(
            (schema.clone(), name.clone()),
            TableMetadata {
                schema,
                name,
                columns: Vec::new(),
                primary_key: Vec::new(),
                unique_constraints: Vec::new(),
                foreign_keys: Vec::new(),
            },
        );
    }

    for row in column_rows {
        let schema: String = row.try_get("table_schema")?;
        let table: String = row.try_get("table_name")?;
        if let Some(t) = table_map.get_mut(&(schema, table)) {
            let nullable: String = row.try_get("is_nullable")?;
            t.columns.push(ColumnMetadata {
                name: row.try_get("column_name")?,
                ordinal_position: row.try_get::<_, i32>("ordinal_position")? as u64,
                data_type: row.try_get("data_type")?,
                nullable: nullable.eq_ignore_ascii_case("YES"),
                column_default: row.try_get("column_default").ok(),
            });
        }
    }

    type TableKey = (String, String);
    let mut pk_by_table: HashMap<TableKey, Vec<String>> = HashMap::new();
    let mut unique_rows: HashMap<TableKey, BTreeMap<String, Vec<String>>> = HashMap::new();
    for row in key_rows {
        let schema: String = row.try_get("table_schema")?;
        let table: String = row.try_get("table_name")?;
        let name: String = row.try_get("constraint_name")?;
        let ctype: String = row.try_get("constraint_type")?;
        let column: String = row.try_get("column_name")?;
        if ctype == "PRIMARY KEY" {
            pk_by_table.entry((schema, table)).or_default().push(column);
        } else {
            unique_rows
                .entry((schema, table))
                .or_default()
                .entry(name)
                .or_default()
                .push(column);
        }
    }
    for ((schema, table), pk) in pk_by_table {
        if let Some(t) = table_map.get_mut(&(schema, table)) {
            t.primary_key = pk;
        }
    }
    for ((schema, table), by_name) in unique_rows {
        if let Some(t) = table_map.get_mut(&(schema, table)) {
            t.unique_constraints = by_name
                .into_iter()
                .map(|(name, columns)| UniqueConstraint { name, columns })
                .collect();
            t.unique_constraints.sort_by(|a, b| a.name.cmp(&b.name));
        }
    }

    type FkTuple = (String, u64, String, String, String);
    let mut fk_group: HashMap<TableKey, BTreeMap<String, Vec<FkTuple>>> = HashMap::new();
    for row in fk_rows {
        let schema: String = row.try_get("table_schema")?;
        let table: String = row.try_get("table_name")?;
        let fk_name: String = row.try_get("constraint_name")?;
        let tuple = (
            row.try_get::<_, String>("column_name")?,
            row.try_get::<_, i32>("ordinal_position")? as u64,
            row.try_get::<_, String>("referenced_schema")?,
            row.try_get::<_, String>("referenced_table")?,
            row.try_get::<_, String>("referenced_column")?,
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
                tuples.sort_by_key(|v| v.1);
                let referenced_schema = tuples[0].2.clone();
                let referenced_table = tuples[0].3.clone();
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
        postgres: TemplatePostgres {
            url: "$POSTGRES_URL".to_string(),
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
        },
        state: TemplateState {
            backend: "file".to_string(),
            file_path: "state.json".to_string(),
        },
        mappings,
    }
}

async fn connect_postgres(cfg: &PostgresConfig) -> Result<Client> {
    let conn_str = build_conn_string(cfg)?;
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .with_context(|| "Failed to connect to PostgreSQL")?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "PostgreSQL connection error");
        }
    });
    Ok(client)
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
    fn mk_table(schema: &str, name: &str, cols: &[&str]) -> TableMetadata {
        TableMetadata {
            schema: schema.to_string(),
            name: name.to_string(),
            columns: cols
                .iter()
                .enumerate()
                .map(|(i, c)| ColumnMetadata {
                    name: (*c).to_string(),
                    ordinal_position: (i + 1) as u64,
                    data_type: "text".to_string(),
                    nullable: true,
                    column_default: None,
                })
                .collect(),
            primary_key: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
        }
    }

    #[test]
    fn singularize_basic() {
        assert_eq!(singularize("customers"), "customer");
        assert_eq!(singularize("categories"), "category");
        assert_eq!(singularize("data"), "data");
    }

    #[test]
    fn infer_delta_detects_pagila_last_update() {
        let table = mk_table("public", "customer", &["customer_id", "last_update"]);
        let delta = infer_delta(&table).expect("expected delta from last_update");
        assert_eq!(delta.updated_at_column, "last_update");
    }

    #[test]
    fn infer_qualified_source_table_prefers_real_schema_table() {
        let schema = SchemaMetadata {
            database: "pagila_falkordb".to_string(),
            tables: vec![mk_table("public", "film_category", &["film_id", "category_id"])],
        };
        let edge = InferredEdge {
            mapping_name: "film_category_edge".to_string(),
            source_table: "film_category".to_string(),
            relationship: "FILM_CATEGORY".to_string(),
            from_node_mapping: "films".to_string(),
            from_column: "film_id".to_string(),
            from_property: "id".to_string(),
            to_node_mapping: "categories".to_string(),
            to_column: "category_id".to_string(),
            to_property: "id".to_string(),
            delta: None,
            properties: vec![],
        };
        assert_eq!(
            infer_qualified_source_table(&schema, &edge),
            "public.film_category"
        );
    }
}
