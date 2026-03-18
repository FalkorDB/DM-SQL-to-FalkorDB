# BigQuery-to-FalkorDB Loader
Rust CLI tool that migrates and incrementally syncs data from **BigQuery (GoogleSQL/ANSI SQL mode)** into FalkorDB using declarative JSON/YAML mappings.

## Features
- BigQuery source via REST API (`jobs.query` + `jobs.getQueryResults`)
- Explicit ANSI SQL mode (`useLegacySql = false`)
- Source styles:
  - `source.table` (+ optional `source.where`)
  - `source.select` (custom SQL)
  - `source.file` (local JSON for testing)
- Incremental loads with per-mapping watermarks (`delta.updated_at_column`)
- Optional soft deletes (`delta.deleted_flag_*`)
- Optional purge modes (`--purge-graph`, `--purge-mapping <name>`)
- Daemon mode (`--daemon --interval-secs <seconds>`)
- Schema introspection + scaffold template generation:
  - `--introspect-schema`
  - `--generate-template`
- Prometheus-style metrics endpoint

## Build
```bash
cd BigQuery-to-FalkorDB/bigquery-to-falkordb
cargo build --release
```

## Configuration
Example:
```yaml
bigquery:
  project_id: "your-gcp-project"
  dataset: "analytics"
  location: "US"
  access_token: "$BIGQUERY_ACCESS_TOKEN"
  # Alternative auth:
  # service_account_key_path: "/absolute/path/to/service-account.json"
  query_timeout_ms: 60000

falkordb:
  endpoint: "falkor://127.0.0.1:6379"
  graph: "bigquery_graph"
  max_unwind_batch_size: 1000

state:
  backend: file
  file_path: "state.json"

mappings:
  - type: node
    name: customers
    source:
      table: "analytics.customers"
      where: "is_active = true"
    mode: incremental
    delta:
      updated_at_column: "updated_at"
      deleted_flag_column: "is_deleted"
      deleted_flag_value: true
    labels: ["Customer"]
    key:
      column: "customer_id"
      property: "customer_id"
    properties:
      email:   { column: "email" }
      country: { column: "country" }
```

Authentication:
- `bigquery.access_token`: direct token or `$ENV_VAR`
- `bigquery.service_account_key_path`: path to a service account JSON key (used to mint short-lived OAuth token)

One of the auth methods must be set.

Table qualification behavior:
- `table` with 1 part: `<project_id>.<dataset>.<table>`
- `table` with 2 parts: `<project_id>.<dataset>.<table>`
- `table` with 3 parts: used as-is
- generated table refs are backtick-quoted

## Run
Single run:
```bash
cargo run --release -- --config ../bigquery_sample.yaml
```

Single run with purge:
```bash
# Purge whole graph before load
cargo run --release -- --config ../bigquery_sample.yaml --purge-graph

# Purge only selected mappings before load
cargo run --release -- --config ../bigquery_sample.yaml --purge-mapping customers --purge-mapping orders_customer
```

Continuous sync:
```bash
cargo run --release -- --config ../bigquery_sample.yaml --daemon --interval-secs 60
```

In daemon mode, purge flags are applied only on the first sync run.

## Scaffold
Introspect schema:
```bash
cargo run --release -- --config ../bigquery_sample.yaml --introspect-schema
```

Generate template:
```bash
cargo run --release -- --config ../bigquery_sample.yaml --generate-template --output bigquery.scaffold.yaml
```

Scaffold uses BigQuery `INFORMATION_SCHEMA` and best-effort PK/UNIQUE/FK metadata inference.
BigQuery constraint metadata can be partial and informational-only, so generated mappings must be reviewed.

## Metrics
Default endpoint: `http://127.0.0.1:9995/`

Override with:
- `--metrics-port`
- `BIGQUERY_TO_FALKORDB_METRICS_PORT`

## Included example configs
- `bigquery_check.yaml`: connectivity/no-op mapping
- `bigquery_sample.yaml`: starter incremental nodes + edge mapping

## Tests
```bash
cargo test
```

Optional BigQuery connectivity smoke test is enabled when these env vars are present:
- `BIGQUERY_PROJECT_ID`
- `BIGQUERY_DATASET`
- and one of:
  - `BIGQUERY_ACCESS_TOKEN`
  - `BIGQUERY_SERVICE_ACCOUNT_KEY_PATH`
