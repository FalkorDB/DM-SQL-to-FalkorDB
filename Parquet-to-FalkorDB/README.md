# Parquet-to-FalkorDB Loader

Rust CLI tool to migrate and continuously sync data from Parquet files (local filesystem or object storage) into FalkorDB using declarative JSON/YAML mappings.

## Features

- Local files/directories and object-store URIs (`s3://`, `gs://`/`gcs://`, `az://`/`abfs://`)
- Glob/prefix multi-file datasets
- Optional Hive-style partition column extraction (`key=value` path segments)
- Node + edge mappings
- Full and incremental sync modes:
  - Column watermark via `delta.updated_at_column` (same model as other connectors)
  - File-level last-modified cursor for append-only datasets (`file_cursor: true` or incremental without `delta`)
- Schema scaffolding via `--introspect-schema` and `--generate-template`
- Optional soft-delete handling via `delta.deleted_flag_*`
- Purge modes: `--purge-graph`, `--purge-mapping`
- Daemon mode (`--daemon --interval-secs <N>`)
- Prometheus-style metrics endpoint

## Build

From the inner crate directory:

```bash
cd parquet-to-falkordb
cargo build --release
```

## Configuration

Config can be YAML or JSON.

```yaml
parquet:
  path: "s3://my-bucket/lake/customers/"   # or local path / single .parquet file
  glob: "**/*.parquet"
  hive_partitioning: true
  batch_size: 65536
  object_store:
    provider: s3
    region: "us-east-1"
    access_key_id: "$AWS_ACCESS_KEY_ID"
    secret_access_key: "$AWS_SECRET_ACCESS_KEY"
    # endpoint: "http://localhost:9000"    # MinIO / custom S3

falkordb:
  endpoint: "falkor://127.0.0.1:6379"
  graph: "parquet_graph"
  max_unwind_batch_size: 1000

state:
  backend: "file"
  file_path: "parquet_state.json"

mappings:
  - type: node
    name: customers
    source:
      path: "s3://my-bucket/lake/customers/"
    mode: incremental
    delta:
      updated_at_column: "updated_at"
      deleted_flag_column: "is_deleted"
      deleted_flag_value: 1
      initial_full_load: true
    labels: ["Customer"]
    key:
      column: "id"
      property: "id"
    properties:
      email: { column: "email" }
```

### Source options

- Global `parquet.path` / `parquet.file` sets the default dataset location.
- Per-mapping `source.path` / `source.file` overrides the global path.
- `parquet.glob` / `source.glob` filters files (default `**/*.parquet`).
- `parquet.hive_partitioning` (default `true`) injects Hive partition keys into each row.

### Incremental modes

1. **Column watermark** — set `mode: incremental` and `delta.updated_at_column`. Rows with timestamps ≤ the stored watermark are skipped. Soft deletes use `deleted_flag_column` / `deleted_flag_value`.
2. **File cursor** — set `mode: incremental` without `delta`, or set `file_cursor: true`. Only objects whose `last_modified` is newer than the stored cursor are read. State values are stored as `file_cursor:<RFC3339>`.

### Environment variable resolution

Values beginning with `$` (or `${VAR}`) are resolved from the environment for:

- `parquet.path`
- `falkordb.endpoint`
- all `parquet.object_store.*` string fields

## Running

### Single run

```bash
cargo run --release --manifest-path parquet-to-falkordb/Cargo.toml -- \
  --config parquet_sample_to_falkordb.yaml
```

### Scaffold from Parquet footer

```bash
cargo run --release --manifest-path parquet-to-falkordb/Cargo.toml -- \
  --config parquet_sample_to_falkordb.yaml \
  --introspect-schema

cargo run --release --manifest-path parquet-to-falkordb/Cargo.toml -- \
  --config parquet_sample_to_falkordb.yaml \
  --generate-template \
  --output parquet.scaffold.yaml
```

### Purge / daemon

```bash
cargo run --release --manifest-path parquet-to-falkordb/Cargo.toml -- \
  --config parquet_sample_to_falkordb.yaml \
  --purge-graph

cargo run --release --manifest-path parquet-to-falkordb/Cargo.toml -- \
  --config parquet_sample_to_falkordb.yaml \
  --daemon --interval-secs 60
```

## Metrics

Default endpoint: `0.0.0.0:9996`

Override with `--metrics-port` or `PARQUET_TO_FALKORDB_METRICS_PORT`.

Metric prefix: `parquet_to_falkordb_`

- `parquet_to_falkordb_runs`
- `parquet_to_falkordb_failed_runs`
- `parquet_to_falkordb_rows_fetched`
- `parquet_to_falkordb_rows_written`
- `parquet_to_falkordb_rows_deleted`
- `parquet_to_falkordb_mapping_*{mapping="<name>"}`

## Shared bridge crate

Arrow `RecordBatch` → JSON conversion and OpenDAL operator building live in
`common/arrow-to-falkordb-bridge`, also consumed by Iceberg-to-FalkorDB.

## Example config

See `parquet_sample_to_falkordb.yaml` in this directory.
