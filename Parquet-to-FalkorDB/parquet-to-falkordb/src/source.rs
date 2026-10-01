//! Parquet source: list files via OpenDAL, stream row groups, convert to LogicalRow.

use std::collections::BTreeMap;

use anyhow::{anyhow, Context, Result};
use arrow::record_batch::RecordBatch;
use arrow_to_falkordb_bridge::{
    build_operator, record_batch_to_logical_rows, ObjectStoreConfig, LogicalRow as BridgeRow,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use opendal::Operator;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::config::{CommonMappingFields, Config, Mode, ParquetConfig};

/// Logical row used by mapping/sink layers (matches other connectors).
#[derive(Debug, Clone)]
pub struct LogicalRow {
    pub values: JsonMap<String, JsonValue>,
}

impl LogicalRow {
    pub fn get(&self, key: &str) -> Option<&JsonValue> {
        self.values.get(key)
    }

    pub fn from_bridge(row: BridgeRow) -> Self {
        Self { values: row }
    }
}

/// A listed Parquet object with optional last-modified timestamp.
#[derive(Debug, Clone)]
pub struct ListedFile {
    pub key: String,
    pub last_modified: Option<DateTime<Utc>>,
    pub size: Option<u64>,
}

/// Fetch all rows for a mapping from Parquet file(s).
///
/// - Column watermark (`delta.updated_at_column`): all matching files are read; rows with
///   `updated_at <= watermark` are dropped.
/// - File cursor (`mode: incremental` without delta, or `file_cursor: true`): only files with
///   `last_modified > watermark` are read. Returns `(rows, new_file_cursor)`.
pub async fn fetch_rows_for_mapping(
    cfg: &Config,
    common: &CommonMappingFields,
    watermark: Option<&str>,
) -> Result<(Vec<LogicalRow>, Option<String>)> {
    let pq = cfg.parquet.as_ref();
    let path = common
        .source
        .resolved_path(pq)
        .ok_or_else(|| {
            anyhow!(
                "No parquet path configured for mapping '{}' (set parquet.path or source.path/file)",
                common.name
            )
        })?;

    let object_store = pq
        .and_then(|p| p.object_store.clone())
        .unwrap_or_default();
    let glob = common
        .source
        .glob
        .as_deref()
        .or_else(|| pq.map(|p| p.glob_pattern()))
        .unwrap_or("**/*.parquet");
    let hive = pq.map(|p| p.hive_partitioning_enabled()).unwrap_or(true);
    let batch_size = pq.map(|p| p.batch_size_or_default()).unwrap_or(65_536);

    let (op, base_key) = build_operator(&object_store, path)?;
    let mut files = list_parquet_files(&op, &base_key, glob).await?;

    let mut new_file_cursor: Option<String> = None;

    if common.uses_file_cursor() {
        let cursor = watermark.and_then(parse_file_cursor);
        if let Some(cut) = cursor {
            files.retain(|f| match f.last_modified {
                Some(ts) => ts > cut,
                None => true, // keep files without mtime to be safe
            });
        }
        new_file_cursor = max_last_modified(&files).map(|ts| format_file_cursor(ts));
    }

    let mut all_rows = Vec::new();
    for file in &files {
        let rows = read_parquet_file(&op, &file.key, batch_size, hive, &base_key).await?;
        all_rows.extend(rows);
    }

    if common.uses_column_watermark() {
        if let (Some(delta), Some(wm)) = (common.delta.as_ref(), watermark) {
            all_rows = filter_rows_by_watermark(all_rows, &delta.updated_at_column, wm);
        }
    }

    Ok((all_rows, new_file_cursor))
}

/// List Parquet objects under `base_key` matching `glob_pat`.
pub async fn list_parquet_files(
    op: &Operator,
    base_key: &str,
    glob_pat: &str,
) -> Result<Vec<ListedFile>> {
    let parsed_glob = glob::Pattern::new(glob_pat)
        .with_context(|| format!("invalid glob pattern '{glob_pat}'"))?;

    // If base_key itself looks like a single file, just return it.
    let base_trimmed = base_key.trim_end_matches('/');
    if base_trimmed.to_ascii_lowercase().ends_with(".parquet") {
        let meta = op.stat(base_trimmed).await.ok();
        let last_modified = meta.as_ref().and_then(meta_last_modified);
        let size = meta.as_ref().map(|m| m.content_length());
        return Ok(vec![ListedFile {
            key: base_trimmed.to_string(),
            last_modified,
            size,
        }]);
    }

    let list_path = if base_key.is_empty() || base_key.ends_with('/') {
        base_key.to_string()
    } else {
        format!("{base_key}/")
    };

    let entries: Vec<opendal::Entry> = op
        .lister_with(&list_path)
        .recursive(true)
        .await
        .with_context(|| format!("failed to list path '{list_path}'"))?
        .try_collect()
        .await
        .with_context(|| format!("failed while listing '{list_path}'"))?;

    let mut out = Vec::new();
    for entry in entries {
        if !entry.metadata().is_file() {
            continue;
        }
        let key = entry.path().to_string();
        // Match glob against path relative to base, and against full key / basename.
        let rel = key
            .strip_prefix(base_trimmed.trim_start_matches('/'))
            .unwrap_or(key.as_str())
            .trim_start_matches('/');
        let name = key.rsplit('/').next().unwrap_or(key.as_str());
        let matches = parsed_glob.matches(rel)
            || parsed_glob.matches(key.as_str())
            || parsed_glob.matches(name)
            || (glob_pat.contains("**") && name.to_ascii_lowercase().ends_with(".parquet"));
        if !matches {
            // Also accept any .parquet when glob is the default recursive pattern.
            if !(glob_pat == "**/*.parquet" && name.to_ascii_lowercase().ends_with(".parquet")) {
                continue;
            }
        }
        if !name.to_ascii_lowercase().ends_with(".parquet") {
            continue;
        }
        let meta = entry.metadata();
        out.push(ListedFile {
            key,
            last_modified: meta_last_modified(&meta),
            size: Some(meta.content_length()),
        });
    }

    out.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(out)
}

fn meta_last_modified(meta: &opendal::Metadata) -> Option<DateTime<Utc>> {
    // OpenDAL 0.54 already returns chrono::DateTime<Utc>.
    meta.last_modified()
}

fn max_last_modified(files: &[ListedFile]) -> Option<DateTime<Utc>> {
    files.iter().filter_map(|f| f.last_modified).max()
}

pub fn format_file_cursor(ts: DateTime<Utc>) -> String {
    format!("file_cursor:{}", ts.to_rfc3339())
}

pub fn parse_file_cursor(s: &str) -> Option<DateTime<Utc>> {
    let raw = s.strip_prefix("file_cursor:").unwrap_or(s);
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

/// Read one Parquet object fully into logical rows (streaming batches).
pub async fn read_parquet_file(
    op: &Operator,
    key: &str,
    batch_size: usize,
    hive_partitioning: bool,
    base_key: &str,
) -> Result<Vec<LogicalRow>> {
    let bytes = op
        .read(key)
        .await
        .with_context(|| format!("failed to read parquet object '{key}'"))?
        .to_bytes();

    let partitions = if hive_partitioning {
        extract_hive_partitions(key, base_key)
    } else {
        BTreeMap::new()
    };

    read_parquet_bytes(&bytes, batch_size, &partitions)
}

/// Sync path used by scaffold and tests: decode Parquet bytes → rows.
pub fn read_parquet_bytes(
    bytes: &[u8],
    batch_size: usize,
    partitions: &BTreeMap<String, String>,
) -> Result<Vec<LogicalRow>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
        .context("failed to open parquet reader")?
        .with_batch_size(batch_size)
        .build()
        .context("failed to build parquet record batch reader")?;

    let mut rows = Vec::new();
    for batch_res in reader {
        let batch = batch_res.context("failed to read parquet record batch")?;
        let mut batch_rows = record_batch_to_rows(&batch)?;
        if !partitions.is_empty() {
            for row in &mut batch_rows {
                for (k, v) in partitions {
                    row.values
                        .entry(k.clone())
                        .or_insert_with(|| JsonValue::String(v.clone()));
                }
            }
        }
        rows.extend(batch_rows);
    }
    Ok(rows)
}

fn record_batch_to_rows(batch: &RecordBatch) -> Result<Vec<LogicalRow>> {
    let bridge_rows = record_batch_to_logical_rows(batch)?;
    Ok(bridge_rows.into_iter().map(LogicalRow::from_bridge).collect())
}

/// Parse Hive-style `key=value` segments from the object key relative to base.
pub fn extract_hive_partitions(key: &str, base_key: &str) -> BTreeMap<String, String> {
    let base = base_key.trim_matches('/');
    let rel = key
        .trim_matches('/')
        .strip_prefix(base)
        .unwrap_or(key)
        .trim_matches('/');
    // Drop the filename.
    let dir = match rel.rfind('/') {
        Some(i) => &rel[..i],
        None => return BTreeMap::new(),
    };
    let mut out = BTreeMap::new();
    for seg in dir.split('/') {
        if let Some((k, v)) = seg.split_once('=') {
            if !k.is_empty() {
                out.insert(k.to_string(), v.to_string());
            }
        }
    }
    out
}

fn filter_rows_by_watermark(
    rows: Vec<LogicalRow>,
    column: &str,
    watermark: &str,
) -> Vec<LogicalRow> {
    rows.into_iter()
        .filter(|row| {
            let Some(v) = row.get(column) else {
                return true;
            };
            let s = match v {
                JsonValue::String(s) => s.as_str(),
                other => {
                    // Compare via string form for numbers/etc.
                    return other.to_string().as_str() > watermark;
                }
            };
            // Strict greater-than, matching SQL connectors.
            s > watermark
        })
        .collect()
}

/// Introspect schema from the first matching Parquet file's footer.
pub async fn introspect_schema_from_path(
    object_store: &ObjectStoreConfig,
    path: &str,
    glob: &str,
) -> Result<(arrow_schema::Schema, ListedFile)> {
    let (op, base_key) = build_operator(object_store, path)?;
    let files = list_parquet_files(&op, &base_key, glob).await?;
    let file = files
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no parquet files found under '{path}'"))?;
    let bytes = op.read(&file.key).await?.to_bytes();
    let schema = parquet_schema_from_bytes(&bytes)?;
    Ok((schema, file))
}

pub fn parquet_schema_from_bytes(bytes: &[u8]) -> Result<arrow_schema::Schema> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
        .context("failed to open parquet for schema introspection")?;
    let schema = builder.schema().as_ref().clone();
    Ok(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    fn write_test_parquet(path: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        // 2024-01-01 and 2024-06-01 in us
        let t1 = 1_704_067_200_000_000i64;
        let t2 = 1_717_200_000_000_000i64;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(t1),
                    Some(t2),
                    Some(t2),
                ])),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn reads_local_parquet_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part.parquet");
        write_test_parquet(&path);

        let cfg = Config {
            parquet: Some(ParquetConfig {
                path: Some(path.to_string_lossy().to_string()),
                glob: None,
                hive_partitioning: Some(false),
                object_store: None,
                batch_size: Some(1024),
            }),
            falkordb: crate::config::FalkorConfig {
                endpoint: "falkor://127.0.0.1:6379".into(),
                graph: "t".into(),
                max_unwind_batch_size: None,
                indexes: vec![],
            },
            state: None,
            mappings: vec![],
        };
        let common = CommonMappingFields {
            name: "t".into(),
            source: crate::config::SourceConfig {
                file: None,
                path: None,
                glob: None,
            },
            mode: Mode::Full,
            delta: None,
            file_cursor: None,
        };
        let (rows, cursor) = fetch_rows_for_mapping(&cfg, &common, None).await.unwrap();
        assert_eq!(rows.len(), 3);
        assert!(cursor.is_none());
        assert_eq!(rows[0].get("id"), Some(&JsonValue::from(1)));
        assert_eq!(rows[0].get("name"), Some(&JsonValue::String("a".into())));
    }

    #[tokio::test]
    async fn hive_partitions_injected() {
        let dir = tempfile::tempdir().unwrap();
        let part_dir = dir.path().join("country=US").join("year=2024");
        std::fs::create_dir_all(&part_dir).unwrap();
        let path = part_dir.join("data.parquet");
        write_test_parquet(&path);

        let cfg = Config {
            parquet: Some(ParquetConfig {
                path: Some(dir.path().to_string_lossy().to_string()),
                glob: Some("**/*.parquet".into()),
                hive_partitioning: Some(true),
                object_store: None,
                batch_size: None,
            }),
            falkordb: crate::config::FalkorConfig {
                endpoint: "falkor://127.0.0.1:6379".into(),
                graph: "t".into(),
                max_unwind_batch_size: None,
                indexes: vec![],
            },
            state: None,
            mappings: vec![],
        };
        let common = CommonMappingFields {
            name: "t".into(),
            source: crate::config::SourceConfig {
                file: None,
                path: None,
                glob: None,
            },
            mode: Mode::Full,
            delta: None,
            file_cursor: None,
        };
        let (rows, _) = fetch_rows_for_mapping(&cfg, &common, None).await.unwrap();
        assert!(!rows.is_empty());
        assert_eq!(rows[0].get("country"), Some(&JsonValue::String("US".into())));
        assert_eq!(rows[0].get("year"), Some(&JsonValue::String("2024".into())));
    }

    #[test]
    fn extract_hive_partitions_basic() {
        let m = extract_hive_partitions(
            "lake/customers/country=US/year=2024/part-0.parquet",
            "lake/customers",
        );
        assert_eq!(m.get("country").map(String::as_str), Some("US"));
        assert_eq!(m.get("year").map(String::as_str), Some("2024"));
    }

    #[test]
    fn file_cursor_roundtrip() {
        let ts = DateTime::parse_from_rfc3339("2024-06-01T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let s = format_file_cursor(ts);
        assert!(s.starts_with("file_cursor:"));
        let back = parse_file_cursor(&s).unwrap();
        assert_eq!(back, ts);
    }

    #[test]
    fn filter_by_watermark_keeps_newer() {
        let rows = vec![
            LogicalRow {
                values: JsonMap::from_iter([
                    ("id".into(), JsonValue::from(1)),
                    ("updated_at".into(), JsonValue::String("2024-01-01T00:00:00Z".into())),
                ]),
            },
            LogicalRow {
                values: JsonMap::from_iter([
                    ("id".into(), JsonValue::from(2)),
                    ("updated_at".into(), JsonValue::String("2024-06-01T00:00:00Z".into())),
                ]),
            },
        ];
        let filtered = filter_rows_by_watermark(rows, "updated_at", "2024-03-01T00:00:00Z");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].get("id"), Some(&JsonValue::from(2)));
    }

    /// Optional S3 smoke test. Set PARQUET_S3_URI (e.g. s3://bucket/prefix/file.parquet)
    /// plus standard AWS env vars. No-op when unset.
    #[tokio::test]
    async fn s3_connectivity_smoke_test() -> Result<()> {
        let uri = match std::env::var("PARQUET_S3_URI") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        let mut os = ObjectStoreConfig {
            provider: Some("s3".into()),
            region: std::env::var("AWS_REGION").ok().or_else(|| std::env::var("AWS_DEFAULT_REGION").ok()),
            access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok(),
            secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
            endpoint: std::env::var("AWS_ENDPOINT_URL").ok(),
            ..Default::default()
        };
        arrow_to_falkordb_bridge::resolve_object_store_env_refs(&mut os)?;
        let (op, key) = build_operator(&os, &uri)?;
        let files = list_parquet_files(&op, &key, "**/*.parquet").await?;
        assert!(
            !files.is_empty(),
            "expected at least one parquet object under {uri}"
        );
        Ok(())
    }
}
