use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use snowflake_connector_rs::{
    SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig, SnowflakeRow,
};

use crate::config::{Config, EdgeDirection, SnowflakeConfig};

#[derive(Debug, Serialize)]
pub struct IntrospectionResult {
    pub schema: SchemaMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMetadata {
    pub database: String,
    pub schema: String,
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
    snowflake: TemplateSnowflake,
    falkordb: TemplateFalkor,
    state: TemplateState,
    mappings: Vec<TemplateMapping>,
}

#[derive(Debug, Serialize)]
struct TemplateSnowflake {
    account: String,
    user: String,
    password: String,
    warehouse: String,
    database: String,
    schema: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<String>,
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

pub async fn introspect_snowflake_schema(cfg: &Config) -> Result<IntrospectionResult> {
    let sf_cfg = cfg
        .snowflake
        .as_ref()
        .ok_or_else(|| anyhow!("snowflake config block is required for schema introspection"))?;
    let session = snowflake_session(sf_cfg).await?;

    let database = sf_cfg.database.clone();
    let schema_name = sf_cfg.schema.clone();
    let tables = fetch_tables(&session, &database, &schema_name).await?;
    let columns = fetch_columns(&session, &database, &schema_name).await?;
    let (pk_by_table, unique_by_table) = fetch_keys(&session, &database, &schema_name).await?;
    let fk_by_table = fetch_foreign_keys(&session, &database, &schema_name)
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
                unique_constraints: Vec::new(),
                foreign_keys: Vec::new(),
            },
        );
    }

    for (schema, table, column) in columns {
        if let Some(t) = table_map.get_mut(&(schema, table)) {
            t.columns.push(column);
        }
    }
    for t in table_map.values_mut() {
        t.columns
            .sort_by_key(|c| (c.ordinal_position, c.name.clone()));
    }

    for ((schema, table), pk_cols) in pk_by_table {
        if let Some(t) = table_map.get_mut(&(schema, table)) {
            t.primary_key = pk_cols;
        }
    }
    for ((schema, table), uniques) in unique_by_table {
        if let Some(t) = table_map.get_mut(&(schema, table)) {
            t.unique_constraints = uniques;
        }
    }
    for ((schema, table), fks) in fk_by_table {
        if let Some(t) = table_map.get_mut(&(schema, table)) {
            t.foreign_keys = fks;
        }
    }

    let mut normalized_tables: Vec<TableMetadata> = table_map.into_values().collect();
    normalized_tables.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(IntrospectionResult {
        schema: SchemaMetadata {
            database,
            schema: schema_name,
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
    notes.push("# Snowflake FK metadata may be partial depending on source constraints/privileges."
        .to_string());
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

    Ok(GraphDraft {
        nodes,
        edges,
        notes,
    })
}

fn build_template_config(cfg: &Config, draft: &GraphDraft, schema: &SchemaMetadata) -> TemplateConfig {
    let sf_cfg = cfg.snowflake.as_ref();
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
                    table: format!(
                        "{}.{}.{}",
                        schema.database,
                        schema.schema,
                        n.table_key
                            .rsplit('.')
                            .next()
                            .unwrap_or(n.table_key.as_str())
                    ),
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
                    table: format!("{}.{}.{}", schema.database, schema.schema, e.source_table),
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
        snowflake: TemplateSnowflake {
            account: sf_cfg
                .map(|s| s.account.clone())
                .unwrap_or_else(|| "$SNOWFLAKE_ACCOUNT".to_string()),
            user: sf_cfg
                .map(|s| s.user.clone())
                .unwrap_or_else(|| "$SNOWFLAKE_USER".to_string()),
            password: "$SNOWFLAKE_PASSWORD".to_string(),
            warehouse: sf_cfg
                .map(|s| s.warehouse.clone())
                .unwrap_or_else(|| "$SNOWFLAKE_WAREHOUSE".to_string()),
            database: schema.database.clone(),
            schema: schema.schema.clone(),
            role: sf_cfg.and_then(|s| s.role.clone()),
            fetch_batch_size: sf_cfg.and_then(|s| s.fetch_batch_size).unwrap_or(10_000),
            query_timeout_ms: sf_cfg.and_then(|s| s.query_timeout_ms).unwrap_or(60_000),
        },
        falkordb: TemplateFalkor {
            endpoint: "$FALKORDB_ENDPOINT".to_string(),
            graph: if cfg.falkordb.graph.trim().is_empty() {
                format!("{}_graph", schema.database.to_lowercase())
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
    if let Some(col) = table
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case("id"))
        .or_else(|| table.columns.first())
    {
        return (
            col.name.clone(),
            Some("no primary/unique key found; using fallback key column".to_string()),
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

async fn snowflake_session(sf_cfg: &SnowflakeConfig) -> Result<snowflake_connector_rs::SnowflakeSession> {
    let auth = if let Some(key_path) = &sf_cfg.private_key_path {
        let pem = std::fs::read_to_string(key_path)?;
        let pass_bytes = sf_cfg.password.as_deref().unwrap_or("").as_bytes().to_vec();
        SnowflakeAuthMethod::KeyPair {
            encrypted_pem: pem,
            password: pass_bytes,
        }
    } else if let Some(pw) = &sf_cfg.password {
        SnowflakeAuthMethod::Password(pw.clone())
    } else {
        return Err(anyhow!(
            "SnowflakeConfig.password or private_key_path must be set for authentication"
        ));
    };
    let config = SnowflakeClientConfig {
        account: sf_cfg.account.clone(),
        warehouse: Some(sf_cfg.warehouse.clone()),
        database: Some(sf_cfg.database.clone()),
        schema: Some(sf_cfg.schema.clone()),
        role: sf_cfg.role.clone(),
        timeout: sf_cfg
            .query_timeout_ms
            .map(std::time::Duration::from_millis),
    };
    let client = SnowflakeClient::new(&sf_cfg.user, auth, config)?;
    client.create_session().await.map_err(|e| anyhow!(e))
}

async fn fetch_tables(
    session: &snowflake_connector_rs::SnowflakeSession,
    database: &str,
    schema: &str,
) -> Result<Vec<(String, String)>> {
    let sql = format!(
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM {}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME",
        database, schema
    );
    let rows = session.query(sql.as_str()).await?;
    let mut out = Vec::new();
    for row in rows {
        let map = snowflake_row_to_json_map(row)?;
        let table_schema = get_req_string(&map, "TABLE_SCHEMA")?;
        let table_name = get_req_string(&map, "TABLE_NAME")?;
        out.push((table_schema, table_name));
    }
    Ok(out)
}

async fn fetch_columns(
    session: &snowflake_connector_rs::SnowflakeSession,
    database: &str,
    schema: &str,
) -> Result<Vec<(String, String, ColumnMetadata)>> {
    let sql = format!(
        "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE, COLUMN_DEFAULT FROM {}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{}' ORDER BY TABLE_NAME, ORDINAL_POSITION",
        database, schema
    );
    let rows = session.query(sql.as_str()).await?;
    let mut out = Vec::new();
    for row in rows {
        let map = snowflake_row_to_json_map(row)?;
        let table_schema = get_req_string(&map, "TABLE_SCHEMA")?;
        let table_name = get_req_string(&map, "TABLE_NAME")?;
        let column_name = get_req_string(&map, "COLUMN_NAME")?;
        let ordinal_position = get_req_u64(&map, "ORDINAL_POSITION")?;
        let is_nullable = get_req_string(&map, "IS_NULLABLE")?;
        let data_type = get_req_string(&map, "DATA_TYPE")?;
        let column_default = get_opt_string(&map, "COLUMN_DEFAULT");
        out.push((
            table_schema,
            table_name,
            ColumnMetadata {
                name: column_name,
                ordinal_position,
                data_type,
                nullable: is_nullable.eq_ignore_ascii_case("YES"),
                column_default,
            },
        ));
    }
    Ok(out)
}

type KeysByTable = HashMap<(String, String), Vec<String>>;
type UniquesByTable = HashMap<(String, String), Vec<UniqueConstraint>>;

async fn fetch_keys(
    session: &snowflake_connector_rs::SnowflakeSession,
    database: &str,
    schema: &str,
) -> Result<(KeysByTable, UniquesByTable)> {
    let sql = format!(
        "SELECT tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE, kcu.COLUMN_NAME, kcu.ORDINAL_POSITION FROM {}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc JOIN {}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_CATALOG = kcu.CONSTRAINT_CATALOG AND tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA AND tc.TABLE_NAME = kcu.TABLE_NAME WHERE tc.TABLE_SCHEMA = '{}' AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY','UNIQUE') ORDER BY tc.TABLE_NAME, tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION",
        database, database, schema
    );
    let rows = session.query(sql.as_str()).await?;

    let mut pk_by_table: HashMap<(String, String), Vec<String>> = HashMap::new();
    let mut unique_rows: HashMap<(String, String), BTreeMap<String, Vec<String>>> = HashMap::new();
    for row in rows {
        let map = snowflake_row_to_json_map(row)?;
        let table_schema = get_req_string(&map, "TABLE_SCHEMA")?;
        let table_name = get_req_string(&map, "TABLE_NAME")?;
        let constraint_name = get_req_string(&map, "CONSTRAINT_NAME")?;
        let constraint_type = get_req_string(&map, "CONSTRAINT_TYPE")?;
        let column_name = get_req_string(&map, "COLUMN_NAME")?;
        if constraint_type == "PRIMARY KEY" {
            pk_by_table
                .entry((table_schema, table_name))
                .or_default()
                .push(column_name);
        } else {
            unique_rows
                .entry((table_schema, table_name))
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

async fn fetch_foreign_keys(
    session: &snowflake_connector_rs::SnowflakeSession,
    database: &str,
    schema: &str,
) -> Result<HashMap<(String, String), Vec<ForeignKeyMetadata>>> {
    let sql = format!(
        "SELECT tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.ORDINAL_POSITION, ccu.TABLE_SCHEMA AS REFERENCED_TABLE_SCHEMA, ccu.TABLE_NAME AS REFERENCED_TABLE_NAME, ccu.COLUMN_NAME AS REFERENCED_COLUMN_NAME FROM {db}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc JOIN {db}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_CATALOG = kcu.CONSTRAINT_CATALOG AND tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA AND tc.TABLE_NAME = kcu.TABLE_NAME JOIN {db}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu ON ccu.CONSTRAINT_CATALOG = tc.CONSTRAINT_CATALOG AND ccu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME WHERE tc.TABLE_SCHEMA = '{schema}' AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY' ORDER BY tc.TABLE_NAME, tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION",
        db = database,
        schema = schema
    );
    let rows = session.query(sql.as_str()).await?;
    type FkRow = (String, String, String, String, u64, String, String, String);
    let mut grouped: HashMap<(String, String), BTreeMap<String, Vec<FkRow>>> = HashMap::new();
    for row in rows {
        let map = snowflake_row_to_json_map(row)?;
        let record = (
            get_req_string(&map, "TABLE_SCHEMA")?,
            get_req_string(&map, "TABLE_NAME")?,
            get_req_string(&map, "CONSTRAINT_NAME")?,
            get_req_string(&map, "COLUMN_NAME")?,
            get_req_u64(&map, "ORDINAL_POSITION")?,
            get_req_string(&map, "REFERENCED_TABLE_SCHEMA")?,
            get_req_string(&map, "REFERENCED_TABLE_NAME")?,
            get_req_string(&map, "REFERENCED_COLUMN_NAME")?,
        );
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

fn snowflake_row_to_json_map(row: SnowflakeRow) -> Result<JsonMap<String, JsonValue>> {
    let mut values = JsonMap::new();
    for name in row.column_names() {
        let key = name.to_string();
        let json_val: JsonValue = match row.get::<JsonValue>(&key) {
            Ok(v) => v,
            Err(_) => {
                let s: String = row.get(&key)?;
                JsonValue::String(s)
            }
        };
        values.insert(key, json_val);
    }
    Ok(values)
}

fn get_req_string(map: &JsonMap<String, JsonValue>, key: &str) -> Result<String> {
    get_opt_string(map, key).ok_or_else(|| anyhow!("missing required string value for {}", key))
}

fn get_opt_string(map: &JsonMap<String, JsonValue>, key: &str) -> Option<String> {
    lookup_ci(map, key).and_then(|v| match v {
        JsonValue::Null => None,
        JsonValue::String(s) => Some(s.clone()),
        JsonValue::Number(n) => Some(n.to_string()),
        JsonValue::Bool(b) => Some(if *b { "true" } else { "false" }.to_string()),
        other => Some(other.to_string()),
    })
}

fn get_req_u64(map: &JsonMap<String, JsonValue>, key: &str) -> Result<u64> {
    let v = lookup_ci(map, key).ok_or_else(|| anyhow!("missing required numeric value for {}", key))?;
    match v {
        JsonValue::Number(n) => n
            .as_u64()
            .or_else(|| n.as_i64().map(|i| i as u64))
            .ok_or_else(|| anyhow!("invalid numeric value for {}", key)),
        JsonValue::String(s) => s
            .parse::<u64>()
            .map_err(|_| anyhow!("invalid numeric string value for {}", key)),
        _ => Err(anyhow!("invalid numeric type for {}", key)),
    }
}

fn lookup_ci<'a>(map: &'a JsonMap<String, JsonValue>, key: &str) -> Option<&'a JsonValue> {
    map.get(key)
        .or_else(|| map.get(&key.to_ascii_uppercase()))
        .or_else(|| map.get(&key.to_ascii_lowercase()))
}

