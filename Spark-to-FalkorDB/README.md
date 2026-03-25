# Spark-to-FalkorDB Loader

A Rust CLI tool that loads and incrementally syncs tabular data from Apache Spark SQL into a FalkorDB graph using declarative JSON/YAML mappings.

This implementation uses Livy interactive sessions as the Spark source transport, then reuses the same FalkorDB sink semantics used across the other loaders (UNWIND + MERGE, file-backed watermarks, optional soft deletes, and per-mapping metrics).

## Features

- **Spark SQL source (via Livy)**
  - Submits SQL statements to an existing Livy session (`spark.session_id`).
  - Supports `table` + optional `where` or full custom `select`/`query`/`sql`/`statement`.
  - Supports `source.query_count` and Spark partition hints (`source.partition.*`).
- **Neo4j-Spark-like read controls**
  - Optional `source.query_count` row cap per mapping.
  - Optional partition hint/range controls (`source.partition.column`, `num_partitions`, `lower_bound`, `upper_bound`).
  - Edge endpoint ergonomics: `match_on` accepts object or list, plus `match_column`/`match_property` shorthand.
- **Schema handling strategy**
  - `spark.schema_strategy` / `source.schema_strategy`: `preserve`, `json_stringify`, `drop_complex`, `flatten`.
  - Optional `flatten_max_depth` at Spark-global or per-source level.
- **Schema scaffolding**
  - Supports `--introspect-schema` and `--generate-template`.
  - Introspection uses `SHOW TABLES` and `DESCRIBE TABLE`.
- **FalkorDB sink**
  - Writes nodes and edges using Cypher `UNWIND` + `MERGE`.
  - Applies explicit `falkordb.indexes` plus inferred indexes on node keys and edge endpoint `match_on` properties.
- **Incremental sync with delta**
  - Per-mapping `mode: full` or `mode: incremental`.
  - Watermark column (`delta.updated_at_column`) fetches only changed rows.
  - Optional soft delete semantics via `delta.deleted_flag_column` and `delta.deleted_flag_value`.
- **Persistent state**
  - File-backed state (`state.backend: file`) stores per-mapping watermarks.
- **Metrics**
  - Prometheus-style endpoint with global and per-mapping counters.
  - Configurable metrics port (`--metrics-port` / `SPARK_TO_FALKORDB_METRICS_PORT`).
- **Operational hardening**
  - Transient retry/backoff for Livy submission/poll calls (`spark.max_retries`, `spark.retry_backoff_ms`, `spark.retry_backoff_max_ms`).
  - Classified Spark/Livy errors (authentication, throttling, timeout, transport, statement failure).

## Quick Start

### 1. Build

From the crate directory:

```bash
cargo build --release
```

### 2. Configure Spark and FalkorDB

Example YAML config:

```yaml
spark:
  livy_url: "http://localhost:8998"
  session_id: 1
  statement_kind: "sql"
  auth_token: "$SPARK_AUTH_TOKEN"
  schema: "default"
  query_timeout_ms: 60000
  poll_interval_ms: 300
  max_poll_attempts: 400
  max_retries: 3
  retry_backoff_ms: 250
  retry_backoff_max_ms: 5000
  schema_strategy: preserve
  flatten_max_depth: 2

falkordb:
  endpoint: "falkor://127.0.0.1:6379"
  graph: "spark_graph"
  max_unwind_batch_size: 1000
  indexes:
    - labels: ["Customer"]
      property: "id"

state:
  backend: file
  file_path: "spark_state.json"

mappings:
  - type: node
    name: customers
    source:
      table: "default.customers"
      where: "is_active = true"
      query_count: 500000
      partition:
        column: "customer_id"
        num_partitions: 8
        lower_bound: "1"
        upper_bound: "1000000"
    mode: incremental
    delta:
      updated_at_column: "updated_at"
      deleted_flag_column: "is_deleted"
      deleted_flag_value: true
    labels: ["Customer"]
    key:
      column: "customer_id"
      property: "id"
    properties:
      name: { column: "name" }
      email: { column: "email" }
```

Notes:

- `spark.session_id` should reference an existing Livy interactive session.
- `spark.statement_kind` is usually `sql`.
- `spark.auth_token` can be a direct value or an environment reference like `$SPARK_AUTH_TOKEN`.

### 3. Run

```bash
export SPARK_AUTH_TOKEN="..."

# Run sync once
cargo run --release -- --config path/to/config.yaml
```

### End-to-end sample configs

- Direct query-source sample: `Spark-to-FalkorDB/spark_to_falkordb_e2e.sample.yaml`
  - Uses `source.select` over JSON files under `Spark-to-FalkorDB/sample_data/`.
- Table-source sample: `Spark-to-FalkorDB/spark_to_falkordb_e2e.tables.sample.yaml`
  - Uses `source.table` names (`customers_src`, `products_src`, `orders_src`).
  - Requires bootstrap SQL in the same Livy session:
    - `Spark-to-FalkorDB/sample_data/bootstrap_table_sources.sql`

Table-source bootstrap sequence:

1. Create (or pick) a Livy SQL session and set that id in `spark.session_id`.
2. Execute each statement from `Spark-to-FalkorDB/sample_data/bootstrap_table_sources.sql` in that same session.
3. Run the table-source sample config.

If you restart Livy or create a new session, recreate the temp views before rerunning the table-source sample.

### Scaffold from Spark schema

Print normalized schema summary:

```bash
cargo run --release -- \
  --config path/to/config.yaml \
  --introspect-schema
```

Generate starter mapping template:

```bash
cargo run --release -- \
  --config path/to/config.yaml \
  --generate-template \
  --output spark.scaffold.yaml
```

## Metrics and monitoring

Default endpoint:

- `0.0.0.0:9997`

Override with:

- CLI flag: `--metrics-port <port>`
- Env var: `SPARK_TO_FALKORDB_METRICS_PORT`

Fetch metrics:

```bash
curl http://localhost:9997/
```

Exposed metric names:

- `spark_to_falkordb_runs`
- `spark_to_falkordb_failed_runs`
- `spark_to_falkordb_rows_fetched`
- `spark_to_falkordb_rows_written`
- `spark_to_falkordb_rows_deleted`
- `spark_to_falkordb_mapping_runs{mapping="<name>"}`
- `spark_to_falkordb_mapping_failed_runs{mapping="<name>"}`
- `spark_to_falkordb_mapping_rows_fetched{mapping="<name>"}`
- `spark_to_falkordb_mapping_rows_written{mapping="<name>"}`
- `spark_to_falkordb_mapping_rows_deleted{mapping="<name>"}`

## Configuration reference (overview)

### `spark` section

- `livy_url`: Livy base URL (for example `http://localhost:8998`).
- `session_id`: existing Livy session id to run statements in.
- `statement_kind`: Livy statement kind (default `sql`).
- `auth_token`: optional bearer token (`$ENV_VAR` supported).
- `catalog`, `schema`: optional scaffold hints.
- `query_timeout_ms`: HTTP timeout per Livy request.
- `poll_interval_ms`: statement polling interval.
- `max_poll_attempts`: max poll attempts before timeout.
- `max_retries`: transient Livy retry attempts per request.
- `retry_backoff_ms`: base exponential backoff delay for retries.
- `retry_backoff_max_ms`: max capped retry delay.
- `schema_strategy`: row handling strategy (`preserve`, `json_stringify`, `drop_complex`, `flatten`).
- `flatten_max_depth`: max nested depth when using `flatten`.

### `falkordb` section

- `endpoint`: FalkorDB connection string.
- `graph`: target graph name.
- `max_unwind_batch_size`: max rows per UNWIND batch.
- `indexes`: optional explicit FalkorDB indexes.

### `state` section

- `backend`: `file` or `none` (file is recommended for incremental mode).
- `file_path`: path to watermark state JSON.

### `mappings` section

Each mapping is either `node` or `edge`.

Common fields:

- `name`: mapping identifier.
- `source.file`: optional local JSON array (for tests/local runs).
- `source.table`: source table.
- `source.select`/`source.query`/`source.sql`/`source.statement`: custom SQL query aliases.
- `source.where`: predicate for table-based sources.
- `source.query_count`: optional result row cap (`LIMIT`).
- `source.partition`: optional partition/read hints:
  - `column`
  - `num_partitions` (adds Spark `REPARTITION` hint)
  - `lower_bound` + `upper_bound` (adds range predicate)
- `source.schema_strategy`: per-mapping schema strategy override.
- `source.flatten_max_depth`: per-mapping flatten depth override.
- `mode`: `full` or `incremental`.
- `delta.updated_at_column`: incremental watermark column.
- `delta.deleted_flag_column` / `delta.deleted_flag_value`: optional soft delete controls.

Edge endpoint ergonomics:

- `match_on` can be either a list or a single object.
- You can also use shorthand `match_column` + `match_property` for single-key endpoint matching.
- `label_override` also accepts `labels`; single `label` shorthand is supported.

## Tests

From the crate directory:

```bash
cargo test
```

Included tests:

- Config parsing tests (YAML/JSON and env var resolution).
- Optional Spark connectivity smoke test using:
  - `SPARK_LIVY_URL`
  - `SPARK_LIVY_SESSION_ID`
  - `SPARK_LIVY_AUTH_TOKEN` (optional)
- Optional file → FalkorDB end-to-end test using:
  - `FALKORDB_ENDPOINT`
  - optional `FALKORDB_GRAPH`

## Limitations and future work

- Source transport currently targets Livy interactive sessions.
- Scaffolding currently infers node mappings from table/column metadata and does not auto-infer foreign-key edges (Spark catalogs often omit FK metadata).
- `state.backend: falkordb` is not implemented yet; file-backed state is supported.
- Complex/engine-specific Spark SQL data types may require manual mapping adjustments.

## Troubleshooting quick notes

- `classification=authentication`: check `spark.auth_token` and session permissions.
- `classification=throttled` or `status=429`: increase backoff and reduce `source.query_count`.
- `classification=statement_timeout`: increase `spark.max_poll_attempts`, tune query, or shrink row scope with `source.where` / `source.partition`.
- If complex payload columns break downstream mapping, switch to `schema_strategy: json_stringify` or `drop_complex`.
