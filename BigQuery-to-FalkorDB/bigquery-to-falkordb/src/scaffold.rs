use std::collections::{BTreeMap, HashMap, HashSet};

use crate::config::{BigQueryConfig, Config, EdgeDirection};
use crate::source::{query_bigquery_sql, LogicalRow};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};

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
            let table_key = qualified_table_key(&table.catalog, &table.schema, &table.name);
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
    pub catalog: String,
    pub schema: String,
    pub tables: Vec<TableMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub catalog: String,
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
    bigquery: TemplateBigQuery,
    falkordb: TemplateFalkor,
    state: TemplateState,
    mappings: Vec<TemplateMapping>,
}

#[derive(Debug, Serialize)]
struct TemplateBigQuery {
    project_id: String,
    dataset: String,
    location: String,
    /// Preferred auth method in templates.
    access_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    service_account_key_path: Option<String>,
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

pub async fn introspect_bigquery_schema(cfg: &Config) -> Result<IntrospectionResult> {
    let db_cfg = cfg
        .bigquery
        .as_ref()
        .ok_or_else(|| anyhow!("bigquery config block is required for schema introspection"))?;
    let target_catalog = db_cfg.project_id.clone();
    let target_schema = db_cfg.dataset.clone();
    let tables_view = info_schema_view(&target_catalog, &target_schema, "TABLES");
    let columns_view = info_schema_view(&target_catalog, &target_schema, "COLUMNS");

    let tables_sql = format!(
        "SELECT table_catalog, table_schema, table_name \
         FROM {} \
         WHERE table_type = 'BASE TABLE' \
         ORDER BY table_name",
        tables_view
    );
    let columns_sql = format!(
        "SELECT table_catalog, table_schema, table_name, column_name, ordinal_position, is_nullable, data_type, column_default \
         FROM {} \
         ORDER BY table_name, ordinal_position",
        columns_view
    );

    let table_rows = query_bigquery_sql(db_cfg, &tables_sql).await?;
    let column_rows = query_bigquery_sql(db_cfg, &columns_sql).await?;
    let key_rows = fetch_constraint_rows(db_cfg, &target_catalog, &target_schema).await?;
    let fk_rows = fetch_fk_rows(db_cfg, &target_catalog, &target_schema)
        .await
        .unwrap_or_default();

    let mut table_map: HashMap<(String, String, String), TableMetadata> = HashMap::new();
    for row in table_rows {
        let catalog = get_string(&row, "table_catalog")?;
        let schema = get_string(&row, "table_schema")?;
        let table = get_string(&row, "table_name")?;
        table_map.insert(
            (catalog.clone(), schema.clone(), table.clone()),
            TableMetadata {
                catalog,
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
        let catalog = get_string(&row, "table_catalog")?;
        let schema = get_string(&row, "table_schema")?;
        let table = get_string(&row, "table_name")?;
        if let Some(t) = table_map.get_mut(&(catalog, schema, table)) {
            let nullable = get_string(&row, "is_nullable").unwrap_or_else(|_| "YES".to_string());
            t.columns.push(ColumnMetadata {
                name: get_string(&row, "column_name")?,
                ordinal_position: get_u64(&row, "ordinal_position").unwrap_or(0),
                data_type: get_string(&row, "data_type").unwrap_or_else(|_| "string".to_string()),
                nullable: nullable.eq_ignore_ascii_case("YES"),
                column_default: get_opt_string(&row, "column_default"),
            });
        }
    }

    type TableKey = (String, String, String);
    let mut pk_by_table: HashMap<TableKey, Vec<String>> = HashMap::new();
    let mut unique_rows: HashMap<TableKey, BTreeMap<String, Vec<String>>> = HashMap::new();
    for row in key_rows {
        let catalog = get_string(&row, "table_catalog")?;
        let schema = get_string(&row, "table_schema")?;
        let table = get_string(&row, "table_name")?;
        let ctype = get_string(&row, "constraint_type")?;
        let name = get_string(&row, "constraint_name")?;
        let column = get_string(&row, "column_name")?;
        if ctype.eq_ignore_ascii_case("PRIMARY KEY") {
            pk_by_table
                .entry((catalog, schema, table))
                .or_default()
                .push(column);
        } else {
            unique_rows
                .entry((catalog, schema, table))
                .or_default()
                .entry(name)
                .or_default()
                .push(column);
        }
    }
    for ((catalog, schema, table), pk_cols) in pk_by_table {
        if let Some(t) = table_map.get_mut(&(catalog, schema, table)) {
            t.primary_key = pk_cols;
        }
    }
    for ((catalog, schema, table), by_name) in unique_rows {
        if let Some(t) = table_map.get_mut(&(catalog, schema, table)) {
            t.unique_constraints = by_name
                .into_iter()
                .map(|(name, columns)| UniqueConstraint { name, columns })
                .collect();
            t.unique_constraints.sort_by(|a, b| a.name.cmp(&b.name));
        }
    }

    type FkTuple = (String, String, String, String, String, u64);
    let mut fk_group: HashMap<TableKey, BTreeMap<String, Vec<FkTuple>>> = HashMap::new();
    for row in fk_rows {
        let catalog = get_string(&row, "table_catalog")?;
        let schema = get_string(&row, "table_schema")?;
        let table = get_string(&row, "table_name")?;
        let fk_name = get_string(&row, "constraint_name")?;
        let tuple = (
            get_string(&row, "column_name")?,
            get_string(&row, "referenced_table_schema")?,
            get_string(&row, "referenced_table_name")?,
            get_string(&row, "referenced_column_name")?,
            get_string(&row, "table_schema")?,
            get_u64(&row, "ordinal_position").unwrap_or(0),
        );
        fk_group
            .entry((catalog, schema, table))
            .or_default()
            .entry(fk_name)
            .or_default()
            .push(tuple);
    }
    for ((catalog, schema, table), by_name) in fk_group {
        if let Some(t) = table_map.get_mut(&(catalog, schema, table)) {
            let mut fks = Vec::new();
            for (name, mut tuples) in by_name {
                tuples.sort_by_key(|v| v.5);
                let referenced_schema = tuples[0].1.clone();
                let referenced_table = tuples[0].2.clone();
                let columns = tuples.iter().map(|v| v.0.clone()).collect::<Vec<_>>();
                let referenced_columns = tuples.iter().map(|v| v.3.clone()).collect::<Vec<_>>();
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
    for t in table_map.values_mut() {
        t.source_indexes = build_best_effort_source_indexes(t);
    }

    let mut tables = table_map.into_values().collect::<Vec<_>>();
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    for t in &mut tables {
        t.columns
            .sort_by_key(|c| (c.ordinal_position, c.name.clone()));
    }

    Ok(IntrospectionResult {
        schema: SchemaMetadata {
            catalog: target_catalog,
            schema: target_schema,
            tables,
        },
    })
}

pub fn generate_template_yaml(cfg: &Config, schema: &SchemaMetadata) -> Result<String> {
    let draft = infer_graph_model(schema);
    let template = build_template_config(cfg, &draft, schema);
    let yaml = serde_yaml::to_string(&template)?;
    let mut notes = vec![
        "# Auto-generated template from source schema introspection.".to_string(),
        "# Review labels, relationship names, key choices, and incremental delta settings."
            .to_string(),
        "# BigQuery PK/FK metadata is informational and may be partial; scaffold inference is best-effort."
            .to_string(),
        "# BigQuery source index metadata is best-effort and currently inferred from PK/UNIQUE constraints."
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

fn build_best_effort_source_indexes(table: &TableMetadata) -> Vec<SourceIndexMetadata> {
    let mut out = Vec::new();
    if !table.primary_key.is_empty() {
        out.push(SourceIndexMetadata {
            name: "PRIMARY_KEY".to_string(),
            columns: table.primary_key.clone(),
            unique: true,
            primary: true,
        });
    }
    for uc in &table.unique_constraints {
        if uc.columns.is_empty() {
            continue;
        }
        out.push(SourceIndexMetadata {
            name: uc.name.clone(),
            columns: uc.columns.clone(),
            unique: true,
            primary: false,
        });
    }
    out
}

fn infer_graph_model(schema: &SchemaMetadata) -> GraphDraft {
    let mut notes = Vec::new();
    let join_tables: HashSet<String> = schema
        .tables
        .iter()
        .filter(|t| looks_like_join_table(t))
        .map(|t| qualified_table_key(&t.catalog, &t.schema, &t.name))
        .collect();

    let mut nodes = Vec::new();
    for table in &schema.tables {
        let key = qualified_table_key(&table.catalog, &table.schema, &table.name);
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
        let child_key = qualified_table_key(&table.catalog, &table.schema, &table.name);
        if join_tables.contains(&child_key) {
            if table.foreign_keys.len() >= 2 {
                let fk_a = &table.foreign_keys[0];
                let fk_b = &table.foreign_keys[1];
                let from_key = qualified_table_key(
                    &schema.catalog,
                    &fk_a.referenced_schema,
                    &fk_a.referenced_table,
                );
                let to_key = qualified_table_key(
                    &schema.catalog,
                    &fk_b.referenced_schema,
                    &fk_b.referenced_table,
                );
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
                    source_table: format!("{}.{}.{}", table.catalog, table.schema, table.name),
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
            let parent_key =
                qualified_table_key(&schema.catalog, &fk.referenced_schema, &fk.referenced_table);
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
                source_table: format!("{}.{}.{}", table.catalog, table.schema, table.name),
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
    GraphDraft {
        nodes,
        edges,
        notes,
    }
}

fn build_template_config(
    cfg: &Config,
    draft: &GraphDraft,
    schema: &SchemaMetadata,
) -> TemplateConfig {
    let db_cfg = cfg.bigquery.as_ref();
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
    TemplateConfig {
        bigquery: TemplateBigQuery {
            project_id: db_cfg
                .map(|d| d.project_id.clone())
                .unwrap_or_else(|| "$BIGQUERY_PROJECT_ID".to_string()),
            dataset: schema.schema.clone(),
            location: db_cfg
                .and_then(|d| d.location.clone())
                .unwrap_or_else(|| "US".to_string()),
            access_token: "$BIGQUERY_ACCESS_TOKEN".to_string(),
            service_account_key_path: Some("$BIGQUERY_SERVICE_ACCOUNT_KEY_PATH".to_string()),
            query_timeout_ms: db_cfg.and_then(|d| d.query_timeout_ms).unwrap_or(60_000),
        },
        falkordb: TemplateFalkor {
            endpoint: "$FALKORDB_ENDPOINT".to_string(),
            graph: if cfg.falkordb.graph.trim().is_empty() {
                format!("{}_{}_graph", schema.catalog, schema.schema)
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
    if edge.source_table.split('.').count() >= 3 {
        return edge.source_table.clone();
    }
    format!("{}.{}.{}", schema.catalog, schema.schema, edge.source_table)
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

fn qualified_table_key(catalog: &str, schema: &str, table: &str) -> String {
    format!("{}.{}.{}", catalog, schema, table)
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

fn get_string(row: &JsonMap<String, JsonValue>, key: &str) -> Result<String> {
    get_opt_string(row, key).ok_or_else(|| anyhow!("Missing required field '{}'", key))
}

fn get_opt_string(row: &JsonMap<String, JsonValue>, key: &str) -> Option<String> {
    row.get(key).and_then(|v| match v {
        JsonValue::Null => None,
        JsonValue::String(s) => Some(s.clone()),
        JsonValue::Number(n) => Some(n.to_string()),
        JsonValue::Bool(b) => Some(if *b { "true" } else { "false" }.to_string()),
        other => Some(other.to_string()),
    })
}

fn get_u64(row: &JsonMap<String, JsonValue>, key: &str) -> Result<u64> {
    let Some(v) = row.get(key) else {
        return Err(anyhow!("Missing numeric field '{}'", key));
    };
    match v {
        JsonValue::Number(n) => n
            .as_u64()
            .or_else(|| n.as_i64().map(|i| i as u64))
            .ok_or_else(|| anyhow!("Invalid numeric value for '{}'", key)),
        JsonValue::String(s) => s
            .parse::<u64>()
            .map_err(|_| anyhow!("Invalid numeric string value for '{}'", key)),
        _ => Err(anyhow!("Invalid numeric type for '{}'", key)),
    }
}

async fn fetch_constraint_rows(
    cfg: &BigQueryConfig,
    catalog: &str,
    schema: &str,
) -> Result<Vec<LogicalRow>> {
    let constraints_view = info_schema_view(catalog, schema, "TABLE_CONSTRAINTS");
    let key_usage_view = info_schema_view(catalog, schema, "KEY_COLUMN_USAGE");
    let sql = format!(
        "SELECT tc.table_catalog, tc.table_schema, tc.table_name, tc.constraint_name, tc.constraint_type, kcu.column_name, kcu.ordinal_position \
         FROM {} tc \
         JOIN {} kcu \
           ON tc.table_catalog = kcu.table_catalog \
          AND tc.table_schema = kcu.table_schema \
          AND tc.table_name = kcu.table_name \
          AND tc.constraint_name = kcu.constraint_name \
         WHERE tc.constraint_type IN ('PRIMARY KEY','UNIQUE') \
         ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position",
        constraints_view, key_usage_view
    );
    query_bigquery_sql(cfg, &sql)
        .await
        .or_else(|_| Ok(Vec::new()))
}

async fn fetch_fk_rows(
    cfg: &BigQueryConfig,
    catalog: &str,
    schema: &str,
) -> Result<Vec<LogicalRow>> {
    let constraints_view = info_schema_view(catalog, schema, "TABLE_CONSTRAINTS");
    let key_usage_view = info_schema_view(catalog, schema, "KEY_COLUMN_USAGE");
    let constraint_usage_view = info_schema_view(catalog, schema, "CONSTRAINT_COLUMN_USAGE");
    let sql = format!(
        "SELECT tc.table_catalog, tc.table_schema, tc.table_name, tc.constraint_name, \
                kcu.column_name, kcu.ordinal_position, \
                ccu.table_schema AS referenced_table_schema, \
                ccu.table_name AS referenced_table_name, \
                ccu.column_name AS referenced_column_name \
         FROM {} tc \
         JOIN {} kcu \
           ON tc.table_catalog = kcu.table_catalog \
          AND tc.table_schema = kcu.table_schema \
          AND tc.table_name = kcu.table_name \
          AND tc.constraint_name = kcu.constraint_name \
         JOIN {} ccu \
           ON ccu.constraint_catalog = tc.constraint_catalog \
          AND ccu.constraint_schema = tc.constraint_schema \
          AND ccu.constraint_name = tc.constraint_name \
         WHERE tc.constraint_type = 'FOREIGN KEY' \
         ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position",
        constraints_view, key_usage_view, constraint_usage_view
    );
    query_bigquery_sql(cfg, &sql).await
}
fn info_schema_view(catalog: &str, schema: &str, view: &str) -> String {
    let catalog = catalog.replace('`', "");
    let schema = schema.replace('`', "");
    let view = view.replace('`', "");
    format!("`{}.{}.INFORMATION_SCHEMA.{}`", catalog, schema, view)
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
            catalog: "main".to_string(),
            schema: "default".to_string(),
            name: "customer".to_string(),
            columns: vec![
                ColumnMetadata {
                    name: "id".to_string(),
                    ordinal_position: 1,
                    data_type: "bigint".to_string(),
                    nullable: false,
                    column_default: None,
                },
                ColumnMetadata {
                    name: "last_update".to_string(),
                    ordinal_position: 2,
                    data_type: "timestamp".to_string(),
                    nullable: true,
                    column_default: None,
                },
            ],
            primary_key: vec!["id".to_string()],
            unique_constraints: vec![],
            foreign_keys: vec![],
            source_indexes: vec![],
        };
        let delta = infer_delta(&table).expect("expected last_update delta");
        assert_eq!(delta.updated_at_column, "last_update");
    }
}
