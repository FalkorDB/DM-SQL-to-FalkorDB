use std::collections::{BTreeMap, HashSet};

use anyhow::{anyhow, Result};
use iceberg::{Catalog, NamespaceIdent};
use serde::{Deserialize, Serialize};

use crate::config::{CatalogType, Config};
use crate::source::{open_catalog, parse_table_ident};

#[derive(Debug, Serialize)]
pub struct IntrospectionResult {
    pub schema: SchemaMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMetadata {
    pub catalog_type: String,
    pub warehouse: Option<String>,
    pub tables: Vec<TableMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub identifier: String,
    pub namespace: Vec<String>,
    pub name: String,
    pub columns: Vec<ColumnMetadata>,
    pub partition_fields: Vec<PartitionFieldMetadata>,
    pub primary_key_guess: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    pub data_type: String,
    pub required: bool,
    pub field_id: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionFieldMetadata {
    pub name: String,
    pub source_id: i32,
    pub transform: String,
}

#[derive(Debug, Serialize)]
struct TemplateConfig {
    iceberg: TemplateIceberg,
    falkordb: TemplateFalkor,
    state: TemplateState,
    mappings: Vec<TemplateMapping>,
}

#[derive(Debug, Serialize)]
struct TemplateIceberg {
    catalog: TemplateCatalog,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_namespace: Option<String>,
}

#[derive(Debug, Serialize)]
struct TemplateCatalog {
    #[serde(rename = "type")]
    catalog_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    warehouse: Option<String>,
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
struct TemplateFalkorIndex {
    labels: Vec<String>,
    property: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_table: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    source_columns: Vec<String>,
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
struct TemplateNodeMapping {
    name: String,
    source: TemplateSource,
    mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta: Option<TemplateDelta>,
    labels: Vec<String>,
    key: TemplateNodeKey,
    properties: BTreeMap<String, TemplateProperty>,
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
struct TemplateNodeKey {
    column: String,
    property: String,
}

#[derive(Debug, Serialize)]
struct TemplateProperty {
    column: String,
}

/// Introspect Iceberg tables from the configured catalog.
///
/// When mappings declare `source.table`, those tables are loaded.
/// Otherwise the default_namespace (or all root namespaces) is listed.
pub async fn introspect_iceberg_schema(cfg: &Config) -> Result<IntrospectionResult> {
    let ice = cfg
        .iceberg
        .as_ref()
        .ok_or_else(|| anyhow!("iceberg config is required for schema introspection"))?;

    let catalog = open_catalog(ice).await?;

    let mut table_idents = Vec::new();
    let mut seen = HashSet::new();

    for mapping in &cfg.mappings {
        let table = match mapping {
            crate::config::EntityMapping::Node(n) => n.common.source.table.as_deref(),
            crate::config::EntityMapping::Edge(e) => e.common.source.table.as_deref(),
        };
        if let Some(t) = table {
            if seen.insert(t.to_string()) {
                table_idents.push(parse_table_ident(t)?);
            }
        }
    }

    if table_idents.is_empty() {
        if let Some(ns) = &ice.default_namespace {
            let ns_ident = NamespaceIdent::from_strs(ns.split('.').map(|s| s.to_string()))
                .map_err(|e| anyhow!("Invalid default_namespace '{ns}': {e}"))?;
            let tables = catalog
                .list_tables(&ns_ident)
                .await
                .map_err(|e| anyhow!("Failed to list tables in namespace '{ns}': {e}"))?;
            table_idents.extend(tables);
        } else {
            let namespaces = catalog
                .list_namespaces(None)
                .await
                .map_err(|e| anyhow!("Failed to list namespaces: {e}"))?;
            for ns in namespaces {
                match catalog.list_tables(&ns).await {
                    Ok(tables) => table_idents.extend(tables),
                    Err(e) => tracing::warn!(error = %e, "Failed to list tables in namespace"),
                }
            }
        }
    }

    let mut tables = Vec::new();
    for ident in table_idents {
        match load_table_metadata(catalog.as_ref(), &ident).await {
            Ok(meta) => tables.push(meta),
            Err(e) => {
                tracing::warn!(table = %ident, error = %e, "Skipping table during introspection")
            }
        }
    }

    let catalog_type = match ice.catalog.catalog_type {
        CatalogType::Rest => "rest",
        CatalogType::Glue => "glue",
        CatalogType::Sql => "sql",
    };

    Ok(IntrospectionResult {
        schema: SchemaMetadata {
            catalog_type: catalog_type.to_string(),
            warehouse: ice.catalog.warehouse.clone(),
            tables,
        },
    })
}

async fn load_table_metadata(
    catalog: &dyn Catalog,
    ident: &iceberg::TableIdent,
) -> Result<TableMetadata> {
    let table = catalog
        .load_table(ident)
        .await
        .map_err(|e| anyhow!("load_table failed: {e}"))?;

    let meta = table.metadata();
    let schema = meta.current_schema();

    let mut columns = Vec::new();
    for field in schema.as_struct().fields() {
        columns.push(ColumnMetadata {
            name: field.name.clone(),
            data_type: format!("{}", field.field_type),
            required: field.required,
            field_id: field.id,
        });
    }

    let mut partition_fields = Vec::new();
    let spec = meta.default_partition_spec();
    for pf in spec.fields() {
        partition_fields.push(PartitionFieldMetadata {
            name: pf.name.clone(),
            source_id: pf.source_id,
            transform: format!("{:?}", pf.transform),
        });
    }

    let primary_key_guess = guess_key_columns(&columns);

    let ns: Vec<String> = ident.namespace().clone().to_vec();
    let identifier = format!("{}.{}", ns.join("."), ident.name());

    Ok(TableMetadata {
        identifier,
        namespace: ns,
        name: ident.name().to_string(),
        columns,
        partition_fields,
        primary_key_guess,
    })
}

fn guess_key_columns(columns: &[ColumnMetadata]) -> Vec<String> {
    let preferred = ["id", "pk", "uuid", "guid", "key"];
    for pref in preferred {
        if let Some(c) = columns.iter().find(|c| {
            c.name.eq_ignore_ascii_case(pref)
                || c.name.to_ascii_lowercase().ends_with("_id")
                    && c.name.eq_ignore_ascii_case(&format!("{pref}"))
        }) {
            return vec![c.name.clone()];
        }
    }
    // Any column ending with _id
    if let Some(c) = columns
        .iter()
        .find(|c| c.name.to_ascii_lowercase().ends_with("_id"))
    {
        return vec![c.name.clone()];
    }
    // First required column
    if let Some(c) = columns.iter().find(|c| c.required) {
        return vec![c.name.clone()];
    }
    columns
        .first()
        .map(|c| vec![c.name.clone()])
        .unwrap_or_default()
}

fn guess_updated_at_column(columns: &[ColumnMetadata]) -> Option<String> {
    let candidates = [
        "updated_at",
        "updated_at_ts",
        "last_updated",
        "modified_at",
        "modification_time",
        "update_time",
        "ts",
        "timestamp",
    ];
    for name in candidates {
        if let Some(c) = columns.iter().find(|c| c.name.eq_ignore_ascii_case(name)) {
            return Some(c.name.clone());
        }
    }
    columns
        .iter()
        .find(|c| {
            let n = c.name.to_ascii_lowercase();
            let t = c.data_type.to_ascii_lowercase();
            (n.contains("update") || n.contains("modified"))
                && (t.contains("timestamp") || t.contains("timestamptz") || t.contains("long"))
        })
        .map(|c| c.name.clone())
}

fn guess_deleted_flag(columns: &[ColumnMetadata]) -> Option<String> {
    let candidates = ["is_deleted", "deleted", "is_delete", "_deleted"];
    for name in candidates {
        if let Some(c) = columns.iter().find(|c| c.name.eq_ignore_ascii_case(name)) {
            return Some(c.name.clone());
        }
    }
    None
}

fn to_label(table_name: &str) -> String {
    let mut out = String::new();
    let mut cap = true;
    for ch in table_name.chars() {
        if ch == '_' || ch == '-' || ch == '.' {
            cap = true;
            continue;
        }
        if cap {
            out.extend(ch.to_uppercase());
            cap = false;
        } else {
            out.push(ch);
        }
    }
    if out.ends_with('s') && out.len() > 1 {
        out.pop();
    }
    if out.is_empty() {
        "Node".to_string()
    } else {
        out
    }
}

/// Generate a starter YAML mapping template from introspected schema.
/// v1: one node mapping per table; no automatic edge inference.
pub fn generate_template_yaml(cfg: &Config, schema: &SchemaMetadata) -> Result<String> {
    let ice = cfg
        .iceberg
        .as_ref()
        .ok_or_else(|| anyhow!("iceberg config is required for template generation"))?;

    let catalog_type = match ice.catalog.catalog_type {
        CatalogType::Rest => "rest",
        CatalogType::Glue => "glue",
        CatalogType::Sql => "sql",
    };

    let mut mappings = Vec::new();
    let mut indexes = Vec::new();

    for table in &schema.tables {
        let label = to_label(&table.name);
        let key_col = table.primary_key_guess.first().cloned().unwrap_or_else(|| {
            table
                .columns
                .first()
                .map(|c| c.name.clone())
                .unwrap_or_else(|| "id".to_string())
        });

        let mut properties = BTreeMap::new();
        for col in &table.columns {
            if col.name == key_col {
                continue;
            }
            properties.insert(
                col.name.clone(),
                TemplateProperty {
                    column: col.name.clone(),
                },
            );
        }

        let delta =
            guess_updated_at_column(&table.columns).map(|updated_at_column| TemplateDelta {
                updated_at_column,
                deleted_flag_column: guess_deleted_flag(&table.columns),
                deleted_flag_value: guess_deleted_flag(&table.columns)
                    .map(|_| serde_json::Value::Bool(true)),
            });

        let mode = if delta.is_some() {
            "incremental".to_string()
        } else {
            "full".to_string()
        };

        let mapping_name = table.name.to_ascii_lowercase();

        indexes.push(TemplateFalkorIndex {
            labels: vec![label.clone()],
            property: "id".to_string(),
            source_table: Some(table.identifier.clone()),
            source_columns: vec![key_col.clone()],
        });

        mappings.push(TemplateMapping::Node(TemplateNodeMapping {
            name: mapping_name,
            source: TemplateSource {
                table: table.identifier.clone(),
            },
            mode,
            delta,
            labels: vec![label],
            key: TemplateNodeKey {
                column: key_col,
                property: "id".to_string(),
            },
            properties,
        }));
    }

    let template = TemplateConfig {
        iceberg: TemplateIceberg {
            catalog: TemplateCatalog {
                catalog_type: catalog_type.to_string(),
                uri: ice.catalog.uri.clone(),
                warehouse: ice.catalog.warehouse.clone(),
            },
            default_namespace: ice.default_namespace.clone(),
        },
        falkordb: TemplateFalkor {
            endpoint: cfg.falkordb.endpoint.clone(),
            graph: cfg.falkordb.graph.clone(),
            max_unwind_batch_size: cfg.falkordb.max_unwind_batch_size.unwrap_or(1000),
            indexes,
        },
        state: TemplateState {
            backend: "file".to_string(),
            file_path: "iceberg_state.json".to_string(),
        },
        mappings,
    };

    let yaml = serde_yaml::to_string(&template)?;
    Ok(format!(
        "# Auto-generated Iceberg → FalkorDB template\n\
         # Review key/property mappings and add edge mappings manually.\n\
         # v1 does not infer foreign-key edges from Iceberg metadata.\n\
         {yaml}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_inflection_basic() {
        assert_eq!(to_label("customers"), "Customer");
        assert_eq!(to_label("order_items"), "OrderItem");
    }

    #[test]
    fn guess_key_prefers_id() {
        let cols = vec![
            ColumnMetadata {
                name: "name".into(),
                data_type: "string".into(),
                required: true,
                field_id: 1,
            },
            ColumnMetadata {
                name: "id".into(),
                data_type: "long".into(),
                required: true,
                field_id: 2,
            },
        ];
        assert_eq!(guess_key_columns(&cols), vec!["id".to_string()]);
    }
}
