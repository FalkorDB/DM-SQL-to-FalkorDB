//! Integration tests against a local SQL (sqlite) catalog + file:// warehouse.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use futures::TryStreamExt;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
use iceberg_catalog_sql::{
    SqlBindStyle, SqlCatalogBuilder, SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI,
    SQL_CATALOG_PROP_WAREHOUSE,
};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use sqlx::migrate::MigrateDatabase;
use tempfile::TempDir;

use crate::arrow_bridge::record_batch_to_logical_rows;
use crate::config::{CatalogConfig, CatalogType, IcebergConfig};
use crate::source::{open_catalog, parse_table_ident};

async fn setup_catalog(tmp: &TempDir) -> Result<(Arc<dyn Catalog>, String, String)> {
    let warehouse_path = tmp.path().join("warehouse");
    std::fs::create_dir_all(&warehouse_path)?;
    // OpenDAL resolving factory auto-detects file://; plain paths also work with LocalFs,
    // but we stick to file:// for consistency with ADR-0001.
    let warehouse = format!("file://{}", warehouse_path.display());
    let catalog_db_path = tmp.path().join("catalog.db");
    // sqlx expects `sqlite:/abs/path` (single slash after scheme for absolute paths is ok as sqlite:PATH)
    let catalog_db = format!("sqlite:{}", catalog_db_path.display());
    sqlx::Sqlite::create_database(&catalog_db)
        .await
        .map_err(|e| anyhow::anyhow!("create sqlite db: {e}"))?;

    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), catalog_db.clone());
    props.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
    props.insert(
        SQL_CATALOG_PROP_BIND_STYLE.to_string(),
        SqlBindStyle::QMark.to_string(),
    );

    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::default()))
        .load("sql", props)
        .await
        .map_err(|e| anyhow::anyhow!("open sql catalog: {e}"))?;
    Ok((Arc::new(catalog), catalog_db, warehouse))
}

fn test_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "amount", Type::Primitive(PrimitiveType::Double)).into(),
            NestedField::optional(4, "updated_at", Type::Primitive(PrimitiveType::Timestamptz))
                .into(),
        ])
        .build()
        .expect("schema")
}

#[tokio::test]
async fn sql_catalog_create_and_scan_roundtrip() -> Result<()> {
let tmp = TempDir::new()?;
    let (catalog, catalog_db, warehouse) = setup_catalog(&tmp).await?;

    let ns = NamespaceIdent::new("sales".into());
    catalog
        .create_namespace(&ns, HashMap::new())
        .await
        .map_err(|e| anyhow::anyhow!("create_namespace: {e}"))?;

    let schema = test_schema();
    let creation = TableCreation::builder()
        .name("orders".to_string())
        .schema(schema)
        .build();

    let table = catalog
        .create_table(&ns, creation)
        .await
        .map_err(|e| anyhow::anyhow!("create_table: {e}"))?;

    let meta = table.metadata();
    assert_eq!(meta.current_schema().as_struct().fields().len(), 4);

    let stream = table
        .scan()
        .select_all()
        .build()
        .map_err(|e| anyhow::anyhow!("build scan: {e}"))?
        .to_arrow()
        .await
        .map_err(|e| anyhow::anyhow!("to_arrow: {e}"))?;
    let batches: Vec<_> = stream
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("collect: {e}"))?;
    let mut rows = Vec::new();
    for b in &batches {
        rows.extend(record_batch_to_logical_rows(b)?);
    }
    assert!(rows.is_empty());

    let ice = IcebergConfig {
        catalog: CatalogConfig {
            catalog_type: CatalogType::Sql,
            uri: Some(catalog_db),
            warehouse: Some(warehouse),
            bind_style: Some("qmark".into()),
            catalog_id: None,
            properties: HashMap::new(),
        },
        default_namespace: Some("sales".into()),
        branch: None,
        snapshot_id: None,
        storage_properties: HashMap::new(),
    };
    let opened = open_catalog(&ice).await?;
    let ident = parse_table_ident("sales.orders")?;
    let loaded = opened
        .load_table(&ident)
        .await
        .map_err(|e| anyhow::anyhow!("reload: {e}"))?;
    assert_eq!(loaded.identifier().name(), "orders");

    Ok(())
}

#[tokio::test]
async fn rest_catalog_live_smoke() -> Result<()> {
    let uri = match std::env::var("ICEBERG_REST_URI") {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };
    let warehouse = std::env::var("ICEBERG_REST_WAREHOUSE").ok();
    let table_name = std::env::var("ICEBERG_REST_TABLE").unwrap_or_else(|_| "default.test".into());

    let ice = IcebergConfig {
        catalog: CatalogConfig {
            catalog_type: CatalogType::Rest,
            uri: Some(uri),
            warehouse,
            bind_style: None,
            catalog_id: None,
            properties: HashMap::new(),
        },
        default_namespace: None,
        branch: None,
        snapshot_id: None,
        storage_properties: HashMap::new(),
    };

    let catalog = open_catalog(&ice).await?;
    let ident = parse_table_ident(&table_name)?;
    let table = catalog
        .load_table(&ident)
        .await
        .map_err(|e| anyhow::anyhow!("load_table: {e}"))?;
    let stream = table
        .scan()
        .select_all()
        .build()
        .map_err(|e| anyhow::anyhow!("scan: {e}"))?
        .to_arrow()
        .await
        .map_err(|e| anyhow::anyhow!("to_arrow: {e}"))?;
    let batches: Vec<_> = stream
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("collect: {e}"))?;
    let mut total = 0usize;
    for b in &batches {
        total += record_batch_to_logical_rows(b)?.len();
    }
    eprintln!("REST smoke scan complete: {total} rows");
    Ok(())
}
