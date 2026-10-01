use std::{collections::HashMap, fs, sync::Arc};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use futures::TryStreamExt;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_glue::{
    GlueCatalogBuilder, GLUE_CATALOG_PROP_CATALOG_ID, GLUE_CATALOG_PROP_URI,
    GLUE_CATALOG_PROP_WAREHOUSE,
};
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_catalog_sql::{
    SqlBindStyle, SqlCatalogBuilder, SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI,
    SQL_CATALOG_PROP_WAREHOUSE,
};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::arrow_bridge::record_batch_to_logical_rows;
use crate::config::{
    CatalogConfig, CatalogType, CommonMappingFields, Config, IcebergConfig, Mode,
};

/// Logical row abstraction used by the mapping layer.
#[derive(Debug, Clone)]
pub struct LogicalRow {
    pub values: JsonMap<String, JsonValue>,
}

impl LogicalRow {
    pub fn get(&self, key: &str) -> Option<&JsonValue> {
        self.values.get(key)
    }
}

/// Open an Iceberg catalog from config.
pub async fn open_catalog(ice: &IcebergConfig) -> Result<Arc<dyn Catalog>> {
    let factory = Arc::new(OpenDalResolvingStorageFactory::default());
    let props = build_catalog_props(&ice.catalog, &ice.storage_properties);

    match ice.catalog.catalog_type {
        CatalogType::Rest => {
            let catalog = RestCatalogBuilder::default()
                .with_storage_factory(factory)
                .load("rest", props)
                .await
                .map_err(|e| anyhow!("Failed to open REST Iceberg catalog: {e}"))?;
            Ok(Arc::new(catalog))
        }
        CatalogType::Glue => {
            let catalog = GlueCatalogBuilder::default()
                .with_storage_factory(factory)
                .load("glue", props)
                .await
                .map_err(|e| anyhow!("Failed to open Glue Iceberg catalog: {e}"))?;
            Ok(Arc::new(catalog))
        }
        CatalogType::Sql => {
            let catalog = SqlCatalogBuilder::default()
                .with_storage_factory(factory)
                .load("sql", props)
                .await
                .map_err(|e| anyhow!("Failed to open SQL Iceberg catalog: {e}"))?;
            Ok(Arc::new(catalog))
        }
    }
}

fn build_catalog_props(
    catalog: &CatalogConfig,
    storage_props: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.extend(catalog.properties.clone());
    props.extend(storage_props.clone());

    match catalog.catalog_type {
        CatalogType::Rest => {
            if let Some(uri) = &catalog.uri {
                props.insert(REST_CATALOG_PROP_URI.to_string(), uri.clone());
            }
            if let Some(wh) = &catalog.warehouse {
                props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), wh.clone());
            }
        }
        CatalogType::Glue => {
            if let Some(uri) = &catalog.uri {
                props.insert(GLUE_CATALOG_PROP_URI.to_string(), uri.clone());
            }
            if let Some(wh) = &catalog.warehouse {
                props.insert(GLUE_CATALOG_PROP_WAREHOUSE.to_string(), wh.clone());
            }
            if let Some(id) = &catalog.catalog_id {
                props.insert(GLUE_CATALOG_PROP_CATALOG_ID.to_string(), id.clone());
            }
        }
        CatalogType::Sql => {
            if let Some(uri) = &catalog.uri {
                props.insert(SQL_CATALOG_PROP_URI.to_string(), uri.clone());
            }
            if let Some(wh) = &catalog.warehouse {
                props.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), wh.clone());
            }
let bind = catalog
                .bind_style
                .as_deref()
                .unwrap_or("qmark")
                .to_ascii_lowercase();
            // Property values match SqlBindStyle Display / FromStr (see iceberg-catalog-sql).
let style = if bind == "dollar" || bind == "$" || bind == "postgres" {
                SqlBindStyle::DollarNumeric.to_string()
            } else {
                SqlBindStyle::QMark.to_string()
            };
            props.insert(SQL_CATALOG_PROP_BIND_STYLE.to_string(), style);
        }
    }

    props
}

/// Parse a dotted table identifier (`ns.table` or `a.b.table`) into `TableIdent`.
pub fn parse_table_ident(table: &str) -> Result<TableIdent> {
    let parts: Vec<&str> = table.split('.').filter(|p| !p.is_empty()).collect();
    if parts.len() < 2 {
        return Err(anyhow!(
            "Iceberg table identifier must be 'namespace.table' (got '{table}')"
        ));
    }
    let name = parts[parts.len() - 1].to_string();
    let ns_parts: Vec<String> = parts[..parts.len() - 1]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let ns = NamespaceIdent::from_vec(ns_parts)
        .map_err(|e| anyhow!("Invalid namespace in table identifier '{table}': {e}"))?;
    Ok(TableIdent::new(ns, name))
}

/// Fetch all rows for a given mapping from a file or Iceberg table.
pub async fn fetch_rows_for_mapping(
    cfg: &Config,
    common: &CommonMappingFields,
    watermark: Option<&str>,
) -> Result<Vec<LogicalRow>> {
    if let Some(file) = &common.source.file {
        let rows = load_rows_from_file(file)?;
        return Ok(filter_rows_by_watermark(rows, common, watermark));
    }

    let ice = cfg
        .iceberg
        .as_ref()
        .ok_or_else(|| anyhow!("No iceberg config provided for mapping '{}'", common.name))?;

    let table_name = common
        .source
        .table
        .as_ref()
        .ok_or_else(|| anyhow!("Mapping '{}' requires source.table or source.file", common.name))?;

    let catalog = open_catalog(ice).await?;
    let ident = parse_table_ident(table_name)?;
    let table = catalog
        .load_table(&ident)
        .await
        .map_err(|e| anyhow!("Failed to load Iceberg table '{table_name}': {e}"))?;

    // Full scan (ADR-0001). Optional column projection.
    let mut scan_builder = table.scan();
    if common.source.columns.is_empty() {
        scan_builder = scan_builder.select_all();
    } else {
        scan_builder = scan_builder.select(common.source.columns.iter().map(|s| s.as_str()));
    }

    // Note: branch/snapshot_id pins are accepted in config for forward-compat but
    // v1 always scans the table's current snapshot (full scan + watermark).
    let _ = (&ice.branch, ice.snapshot_id);

    let scan = scan_builder
        .build()
        .map_err(|e| anyhow!("Failed to build Iceberg scan for '{table_name}': {e}"))?;

    let stream = scan
        .to_arrow()
        .await
        .map_err(|e| anyhow!("Failed to start Iceberg Arrow scan for '{table_name}': {e}"))?;

    let batches: Vec<_> = stream
        .try_collect()
        .await
        .map_err(|e| anyhow!("Error reading Iceberg Arrow batches for '{table_name}': {e}"))?;

    let mut rows = Vec::new();
    for batch in &batches {
        let mut batch_rows = record_batch_to_logical_rows(batch)
            .with_context(|| format!("Failed to convert Arrow batch for '{table_name}'"))?;
        rows.append(&mut batch_rows);
    }

    Ok(filter_rows_by_watermark(rows, common, watermark))
}

/// Client-side watermark filter (column-watermark model per ADR-0001).
pub fn filter_rows_by_watermark(
    rows: Vec<LogicalRow>,
    common: &CommonMappingFields,
    watermark: Option<&str>,
) -> Vec<LogicalRow> {
    let (Mode::Incremental, Some(delta), Some(wm)) =
        (common.mode, common.delta.as_ref(), watermark)
    else {
        return rows;
    };

    let Some(wm_ts) = parse_timestamp_value(&JsonValue::String(wm.to_string())) else {
        tracing::warn!(
            mapping = %common.name,
            watermark = %wm,
            "Could not parse watermark; returning unfiltered rows"
        );
        return rows;
    };

    let col = &delta.updated_at_column;
    rows.into_iter()
        .filter(|row| {
            row.get(col)
                .and_then(parse_timestamp_value)
                .map(|ts| ts > wm_ts)
                .unwrap_or(true)
        })
        .collect()
}

fn parse_timestamp_value(value: &JsonValue) -> Option<DateTime<Utc>> {
    match value {
        JsonValue::String(s) => DateTime::parse_from_rfc3339(s)
            .map(|dt| dt.with_timezone(&Utc))
            .or_else(|_| {
                NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                    .map(|ndt| Utc.from_utc_datetime(&ndt))
            })
            .or_else(|_| {
                NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                    .map(|ndt| Utc.from_utc_datetime(&ndt))
            })
            .ok(),
        JsonValue::Number(n) => {
            // Treat large numbers as epoch millis / micros heuristics.
            if let Some(i) = n.as_i64() {
                if i > 10_000_000_000_000 {
                    // micros
                    let secs = i.div_euclid(1_000_000);
                    let nanos = (i.rem_euclid(1_000_000) * 1_000) as u32;
                    Utc.timestamp_opt(secs, nanos).single()
                } else if i > 10_000_000_000 {
                    // millis
                    let secs = i.div_euclid(1_000);
                    let nanos = (i.rem_euclid(1_000) * 1_000_000) as u32;
                    Utc.timestamp_opt(secs, nanos).single()
                } else {
                    Utc.timestamp_opt(i, 0).single()
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

fn load_rows_from_file(path: &str) -> Result<Vec<LogicalRow>> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("Failed to read input file {path}"))?;

    let value: JsonValue = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse JSON input from {path}"))?;

    let arr = value
        .as_array()
        .cloned()
        .ok_or_else(|| anyhow!("Expected top-level JSON array in input file {path}"))?;

    let mut rows = Vec::with_capacity(arr.len());
    for (idx, v) in arr.into_iter().enumerate() {
        match v {
            JsonValue::Object(map) => rows.push(LogicalRow { values: map }),
            _ => {
                return Err(anyhow!(
                    "Row at index {idx} in {path} is not a JSON object"
                ))
            }
        }
    }

    Ok(rows)
}

/// Paging is not used for Iceberg (full scan streams Arrow batches internally).
pub fn should_use_incremental_paging(_cfg: &Config, _common: &CommonMappingFields) -> bool {
    false
}

pub fn incremental_page_size(_cfg: &Config) -> Option<usize> {
    None
}

pub async fn fetch_rows_page_for_incremental_table(
    _cfg: &Config,
    _common: &CommonMappingFields,
    _watermark: Option<&str>,
    _page_size: usize,
) -> Result<Vec<LogicalRow>> {
    Err(anyhow!(
        "Paged incremental fetch is not supported for Iceberg sources"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DeltaSpec, Mode, SourceConfig};

    #[test]
    fn parse_table_ident_two_and_three_parts() {
        let t = parse_table_ident("sales.orders").unwrap();
        assert_eq!(t.name(), "orders");
        assert_eq!(t.namespace().to_vec(), vec!["sales".to_string()]);

        let t = parse_table_ident("a.b.orders").unwrap();
        assert_eq!(t.name(), "orders");
        assert_eq!(
            t.namespace().to_vec(),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn watermark_filter_keeps_newer_rows() {
        let common = CommonMappingFields {
            name: "orders".into(),
            source: SourceConfig {
                file: None,
                table: Some("sales.orders".into()),
                columns: vec![],
            },
            mode: Mode::Incremental,
            delta: Some(DeltaSpec {
                updated_at_column: "updated_at".into(),
                deleted_flag_column: None,
                deleted_flag_value: None,
                initial_full_load: None,
            }),
        };

        let rows = vec![
            LogicalRow {
                values: JsonMap::from_iter([
                    ("id".into(), JsonValue::from(1)),
                    (
                        "updated_at".into(),
                        JsonValue::String("2024-01-01T00:00:00Z".into()),
                    ),
                ]),
            },
            LogicalRow {
                values: JsonMap::from_iter([
                    ("id".into(), JsonValue::from(2)),
                    (
                        "updated_at".into(),
                        JsonValue::String("2024-06-01T00:00:00Z".into()),
                    ),
                ]),
            },
        ];

        let filtered = filter_rows_by_watermark(rows, &common, Some("2024-03-01T00:00:00Z"));
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].get("id"), Some(&JsonValue::from(2)));
    }
}
