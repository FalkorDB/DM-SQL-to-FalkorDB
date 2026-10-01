# Iceberg-to-FalkorDB Loader

A Rust CLI tool that loads and incrementally syncs Apache Iceberg tables into a FalkorDB graph using a declarative JSON/YAML configuration.

Design mirrors other connectors in this repo (ClickHouse, Databricks, …): you describe how Iceberg tables map to graph nodes and edges, and the tool handles Arrow batch conversion, UNWIND+MERGE upserts, optional soft deletes, and incremental updates based on a watermark column.

## Features

- **Iceberg source**
  - Catalogs: REST (`iceberg-catalog-rest`), AWS Glue (`iceberg-catalog-glue`), SQL/sqlite (`iceberg-catalog-sql`) for local/lightweight setups.
  - Storage via `iceberg-storage-opendal`'s `OpenDalResolvingStorageFactory` (auto-detects `file://`, `s3://`, `gs://`, `azblob://`).
  - Full `table.scan().to_arrow()` path (stable; correctly applies merge-on-read deletes — see `ADR-0001-source-access.md`).
- **Schema scaffolding**
  - `--introspect-schema` and `--generate-template` derive starter node mappings from Iceberg table metadata.
  - No automatic cross-table FK/edge inference in v1 — edges are defined manually.
- **FalkorDB sink**
  - Writes nodes and edges using Cypher `UNWIND` + `MERGE`.
  - Applies explicit `falkordb.indexes` plus implicit indexes on node keys and edge endpoint `match_on` properties.
- **Incremental sync with column watermark**
  - Per-mapping `mode: full` or `mode: incremental`.
  - Watermark column (`delta.updated_at_column`) filters rows client-side after a full scan.
  - Optional soft delete via `delta.deleted_flag_column` / `delta.deleted_flag_value`.
  - Native Iceberg snapshot-incremental scanning is intentionally **not** used in v1 (see ADR-0001).
- **Persistent state**
  - File-backed state (`state.backend: file`) stores per-mapping watermarks between runs.
- **Daemon / purge / metrics**
  - `--daemon`, `--purge-graph`, `--purge-mapping`, Prometheus metrics on port 9995 (prefix `iceberg_to_falkordb_`).

## Quick Start

### 1. Build

```bash
cd Iceberg-to-FalkorDB/iceberg-to-falkordb
cargo build --release
```

### 2. Configure

See `iceberg_sample_to_falkordb.yaml`. Minimal REST example:

```yaml
iceberg:
  catalog:
    type: rest
    uri: "$ICEBERG_CATALOG_URI"
    warehouse: "s3://my-bucket/warehouse"
    properties:
      token: "$ICEBERG_TOKEN"
  storage_properties:
    s3.region: "us-east-1"
    s3.access-key-id: "$AWS_ACCESS_KEY_ID"
    s3.secret-access-key: "$AWS_SECRET_ACCESS_KEY"

falkordb:
  endpoint: "falkor://127.0.0.1:6379"
  graph: "lake_graph"

state:
  backend: file
  file_path: "iceberg_state.json"

mappings:
  - type: node
    name: orders
    source:
      table: "sales.orders"
    mode: incremental
    delta:
      updated_at_column: "updated_at"
    labels: ["Order"]
    key:
      column: "order_id"
      property: "id"
    properties:
      total: { column: "total" }
```

Local/dev SQL catalog + filesystem warehouse:

```yaml
iceberg:
  catalog:
    type: sql
    uri: "sqlite:///tmp/iceberg_catalog.db"
    warehouse: "file:///tmp/iceberg_warehouse"
    bind_style: qmark
```

Secrets support the `$VARIABLE` environment-reference convention used by every other connector.

### 3. Run

```bash
cargo run --release -- --config ../iceberg_sample_to_falkordb.yaml
```

Daemon mode:

```bash
cargo run --release -- --config ../iceberg_sample_to_falkordb.yaml --daemon --interval-secs 300
```

### Scaffold from Iceberg metadata

```bash
cargo run --release -- --config path/to/config.yaml --introspect-schema
cargo run --release -- --config path/to/config.yaml --generate-template --output iceberg.scaffold.yaml
```

### Metrics

Default endpoint: `http://127.0.0.1:9995/`

- CLI: `--metrics-port <port>`
- Env: `ICEBERG_TO_FALKORDB_METRICS_PORT`

Metric names use the `iceberg_to_falkordb_` prefix (runs, failed_runs, rows_fetched/written/deleted, plus per-mapping variants).

## Architecture notes

See `ADR-0001-source-access.md` for the Phase 0 feasibility spike and the binding v1 decisions:

1. Full `table.scan()` only (no snapshot-incremental).
2. Column-watermark incremental model shared with other connectors.
3. Catalogs: REST + Glue + SQL; HMS deferred.
4. Storage: `OpenDalResolvingStorageFactory` directly.

## License

Apache-2.0 (same as this repository).
