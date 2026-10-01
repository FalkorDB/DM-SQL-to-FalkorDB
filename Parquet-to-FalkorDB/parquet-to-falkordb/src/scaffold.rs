//! Schema introspection from Parquet footers + starter YAML template generation.

use std::collections::{BTreeMap, HashSet};

use anyhow::{anyhow, Result};
use arrow_schema::{DataType, Schema as ArrowSchema};
use serde::{Deserialize, Serialize};

use crate::config::{Config, EdgeDirection};
use crate::source::introspect_schema_from_path;

#[derive(Debug, Serialize)]
pub struct IntrospectionResult {
    pub schema: SchemaMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMetadata {
    pub path: String,
    pub sample_file: String,
    pub columns: Vec<ColumnMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    pub ordinal_position: u64,
    pub data_type: String,
    pub arrow_type: String,
    pub nullable: bool,
}

#[derive(Debug, Serialize)]
struct TemplateConfig {
    parquet: TemplateParquet,
    falkordb: TemplateFalkor,
    state: TemplateState,
    mappings: Vec<TemplateMapping>,
}

#[derive(Debug, Serialize)]
struct TemplateParquet {
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    glob: Option<String>,
    hive_partitioning: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    object_store: Option<TemplateObjectStore>,
}

#[derive(Debug, Serialize)]
struct TemplateObjectStore {
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    access_key_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    secret_access_key: Option<String>,
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
}

#[derive(Debug, Serialize)]
struct TemplateNodeMapping {
    name: String,
    source: TemplateSource,
    mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta: Option<TemplateDelta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_cursor: Option<bool>,
    labels: Vec<String>,
    key: TemplateNodeKey,
    properties: BTreeMap<String, TemplateProperty>,
}

#[derive(Debug, Serialize)]
struct TemplateSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
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

// Keep EdgeDirection referenced for future edge inference.
#[allow(dead_code)]
fn _edge_dir() -> EdgeDirection {
    EdgeDirection::Out
}

pub async fn introspect_parquet_schema(cfg: &Config) -> Result<IntrospectionResult> {
    let pq = cfg
        .parquet
        .as_ref()
        .ok_or_else(|| anyhow!("parquet config block is required for schema introspection"))?;
    let path = pq
        .path
        .as_deref()
        .ok_or_else(|| anyhow!("parquet.path is required for schema introspection"))?;
    let glob = pq.glob_pattern();
    let object_store = pq.object_store.clone().unwrap_or_default();

    let (arrow_schema, sample) = introspect_schema_from_path(&object_store, path, glob).await?;
    let columns = arrow_schema_to_columns(&arrow_schema);

    Ok(IntrospectionResult {
        schema: SchemaMetadata {
            path: path.to_string(),
            sample_file: sample.key,
            columns,
        },
    })
}

pub fn arrow_schema_to_columns(schema: &ArrowSchema) -> Vec<ColumnMetadata> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| ColumnMetadata {
            name: f.name().clone(),
            ordinal_position: (i as u64) + 1,
            data_type: human_type(f.data_type()),
            arrow_type: format!("{:?}", f.data_type()),
            nullable: f.is_nullable(),
        })
        .collect()
}

fn human_type(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "boolean".into(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => "integer".into(),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            "unsigned_integer".into()
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "float".into(),
        DataType::Utf8 | DataType::LargeUtf8 => "string".into(),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => "binary".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Timestamp(_, _) => "timestamp".into(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "decimal".into(),
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => "list".into(),
        DataType::Struct(_) => "struct".into(),
        DataType::Map(_, _) => "map".into(),
        other => format!("{other:?}"),
    }
}

pub fn generate_template_yaml(cfg: &Config, schema: &SchemaMetadata) -> Result<String> {
    let col_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    let colset: HashSet<&str> = col_names.iter().copied().collect();

    let (key_column, key_note) = choose_key_column(&col_names);
    let key_property = if key_column.ends_with("_id") {
        "id".to_string()
    } else {
        key_column.clone()
    };

    let delta = infer_delta(&colset);
    let use_file_cursor = delta.is_none();

    let mut props = BTreeMap::new();
    for c in &schema.columns {
        if c.name == key_column {
            continue;
        }
        props.insert(
            c.name.clone(),
            TemplateProperty {
                column: c.name.clone(),
            },
        );
    }

    let dataset_name = dataset_name_from_path(&schema.path);
    let label = to_label(&dataset_name);

    let pq = cfg.parquet.as_ref();
    let object_store = pq
        .and_then(|p| p.object_store.as_ref())
        .map(|os| TemplateObjectStore {
            provider: os.provider.clone().or(os.scheme.clone()),
            region: os.region.clone(),
            access_key_id: os
                .access_key_id
                .as_ref()
                .map(|_| "$AWS_ACCESS_KEY_ID".to_string()),
            secret_access_key: os
                .secret_access_key
                .as_ref()
                .map(|_| "$AWS_SECRET_ACCESS_KEY".to_string()),
        });

    let template = TemplateConfig {
        parquet: TemplateParquet {
            path: schema.path.clone(),
            glob: pq.and_then(|p| p.glob.clone()),
            hive_partitioning: pq.map(|p| p.hive_partitioning_enabled()).unwrap_or(true),
            object_store,
        },
        falkordb: TemplateFalkor {
            endpoint: if cfg.falkordb.endpoint.trim().is_empty() {
                "$FALKORDB_ENDPOINT".into()
            } else if cfg.falkordb.endpoint.starts_with('$') {
                cfg.falkordb.endpoint.clone()
            } else {
                "$FALKORDB_ENDPOINT".into()
            },
            graph: if cfg.falkordb.graph.trim().is_empty() {
                format!("{dataset_name}_graph")
            } else {
                cfg.falkordb.graph.clone()
            },
            max_unwind_batch_size: cfg.falkordb.max_unwind_batch_size.unwrap_or(1000),
        },
        state: TemplateState {
            backend: "file".into(),
            file_path: "parquet_state.json".into(),
        },
        mappings: vec![TemplateMapping::Node(TemplateNodeMapping {
            name: snake_case(&dataset_name),
            source: TemplateSource { path: None },
            mode: if delta.is_some() || use_file_cursor {
                "incremental".into()
            } else {
                "full".into()
            },
            delta,
            file_cursor: if use_file_cursor { Some(true) } else { None },
            labels: vec![label],
            key: TemplateNodeKey {
                column: key_column,
                property: key_property,
            },
            properties: props,
        })],
    };

    let yaml = serde_yaml::to_string(&template)?;
    let mut notes = vec![
        "# Auto-generated template from Parquet footer introspection.".to_string(),
        format!("# Sample file: {}", schema.sample_file),
        "# Review labels, key selection, and incremental mode.".to_string(),
    ];
    if let Some(n) = key_note {
        notes.push(format!("# Note: {n}"));
    }
    Ok(format!("{}\n\n{}", notes.join("\n"), yaml))
}

fn choose_key_column(cols: &[&str]) -> (String, Option<String>) {
    if let Some(id) = cols
        .iter()
        .find(|c| c.eq_ignore_ascii_case("id") || c.ends_with("_id"))
    {
        return ((*id).to_string(), None);
    }
    if let Some(c) = cols.first() {
        return (
            (*c).to_string(),
            Some("no id-like column found; using first column as key".into()),
        );
    }
    (
        "id".into(),
        Some("schema has no columns; using synthetic key placeholder".into()),
    )
}

fn infer_delta(colset: &HashSet<&str>) -> Option<TemplateDelta> {
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

fn dataset_name_from_path(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    let name = trimmed.rsplit('/').next().unwrap_or("dataset");
    let name = name.trim_end_matches(".parquet");
    if name.is_empty() {
        "dataset".into()
    } else {
        name.to_string()
    }
}

fn to_label(name: &str) -> String {
    let singular = if name.ends_with("ies") && name.len() > 3 {
        format!("{}y", &name[..name.len() - 3])
    } else if name.ends_with('s') && name.len() > 1 {
        name[..name.len() - 1].to_string()
    } else {
        name.to_string()
    };
    singular
        .split(|c: char| !c.is_ascii_alphanumeric())
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

fn snake_case(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType as AD, Field, Schema as ArrowSchemaFull};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    #[tokio::test]
    async fn introspect_and_template_from_local_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("customers.parquet");
        let schema = Arc::new(ArrowSchemaFull::new(vec![
            Field::new("id", AD::Int64, false),
            Field::new("email", AD::Utf8, true),
            Field::new("updated_at", AD::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("a@b.c")])),
                Arc::new(StringArray::from(vec![Some("2024-01-01T00:00:00Z")])),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let cfg = Config {
            parquet: Some(crate::config::ParquetConfig {
                path: Some(path.to_string_lossy().to_string()),
                glob: None,
                hive_partitioning: Some(false),
                object_store: None,
                batch_size: None,
            }),
            falkordb: crate::config::FalkorConfig {
                endpoint: "falkor://127.0.0.1:6379".into(),
                graph: "g".into(),
                max_unwind_batch_size: Some(100),
                indexes: vec![],
            },
            state: None,
            mappings: vec![],
        };

        let result = introspect_parquet_schema(&cfg).await.unwrap();
        assert!(result.schema.columns.iter().any(|c| c.name == "id"));
        assert!(result.schema.columns.iter().any(|c| c.name == "email"));

        let yaml = generate_template_yaml(&cfg, &result.schema).unwrap();
        assert!(
            yaml.contains("type: node")
                || yaml.contains("type:node")
                || yaml.contains("Node")
                || yaml.contains("customers")
                || yaml.contains("label")
        );
        assert!(yaml.contains("updated_at"));
        assert!(yaml.contains("incremental"));
    }

    #[test]
    fn arrow_schema_to_columns_maps_types() {
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]);
        let cols = arrow_schema_to_columns(&schema);
        assert_eq!(cols[0].data_type, "integer");
        assert_eq!(cols[1].data_type, "list");
    }
}
