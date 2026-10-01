//! Shared bridge between Arrow/object storage and FalkorDB loaders.
//!
//! # Public API (stable for Iceberg-to-FalkorDB and Parquet-to-FalkorDB)
//!
//! ## Arrow → LogicalRow
//! - [`LogicalRow`] — `serde_json::Map<String, Value>` alias
//! - [`record_batch_to_logical_rows`] — convert one `RecordBatch` to rows
//! - [`array_value_to_json`] — convert a single Arrow array cell to JSON
//! - [`normalise_property_value`] — FalkorDB-safe property normalisation
//!
//! ## OpenDAL storage
//! - [`ObjectStoreConfig`] — serde-deserializable storage credentials/config
//! - [`resolve_object_store_env_refs`] — `$VAR` env resolution
//! - [`build_operator`] — build an OpenDAL `Operator` from config + path/URI
//! - [`parse_storage_uri`] — split scheme/bucket/key from a URI or local path
//! - [`ParsedUri`] — result of URI parsing

pub mod convert;
pub mod storage;

pub use convert::{
    array_value_to_json, normalise_property_value, record_batch_to_logical_rows, LogicalRow,
};
pub use storage::{
    build_operator, join_key, parent_key, parse_storage_uri, resolve_object_store_env_refs,
    ObjectStoreConfig, ParsedUri,
};
