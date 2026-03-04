# ClickHouse-to-FalkorDB Loader

Rust CLI tool to migrate and continuously sync data from ClickHouse into FalkorDB using declarative JSON/YAML mappings.

## Features

- ClickHouse source over HTTP (`FORMAT JSONEachRow`)
- File source for local/testing workflows (`source.file`)
- Node + edge mappings
- Full and incremental sync modes
- Optional soft-delete handling via `delta.deleted_flag_*`
- Optional purge modes:
  - whole graph (`--purge-graph`)
  - selected mappings (`--purge-mapping`)
- Daemon mode (`--daemon --interval-secs <N>`)
- Prometheus-style metrics endpoint

## Build

From this directory:

```bash
cargo build --release
```

## Configuration

Config can be YAML or JSON.

Top-level structure:

```yaml
clickhouse:
  # Either full URL:
  url: "$CLICKHOUSE_URL"          # e.g. http://localhost:8123/
  # ...or discrete fields:
  # host: "localhost"
  # port: 8123
  user: "default"
  password: "$CLICKHOUSE_PASSWORD"
  database: "analytics"
  query_timeout_ms: 60000
  fetch_batch_size: 5000          # optional; enables incremental page fetches

falkordb:
  endpoint: "falkor://127.0.0.1:6379"
  graph: "clickhouse_graph"
  max_unwind_batch_size: 1000

state:
  backend: "file"
  file_path: "state.json"

mappings:
  - type: node
    name: customers
    source:
      table: "analytics.customers"
      where: "active = 1"
    mode: incremental
    delta:
      updated_at_column: "updated_at"
      deleted_flag_column: "is_deleted"
      deleted_flag_value: 1
      initial_full_load: true
    labels: ["Customer"]
    key:
      column: "customer_id"
      property: "customer_id"
    properties:
      email: { column: "email" }
      country: { column: "country" }
```

### Source options

Each mapping can use one of:

- `source.file`
- `source.table` (+ optional `source.where`)
- `source.select`

Notes:

- `source.select` is treated as user-owned SQL and used as-is.
- For `source.table`, incremental watermark predicates are appended by the loader.
- If `fetch_batch_size` is set and mapping is incremental with `delta` + `source.table`, the loader fetches rows in pages ordered by `delta.updated_at_column`.

### Environment variable resolution

In config, values beginning with `$` are resolved from environment variables for:

- `clickhouse.url`
- `clickhouse.host`
- `clickhouse.user`
- `clickhouse.password`
- `clickhouse.database`

## Running

### Single run

```bash
cargo run --release -- \
  --config clickhouse.incremental.yaml
```

### Purge full graph before load

```bash
cargo run --release -- \
  --config clickhouse.incremental.yaml \
  --purge-graph
```

### Purge selected mappings

```bash
cargo run --release -- \
  --config clickhouse.incremental.yaml \
  --purge-mapping customers \
  --purge-mapping customer_orders
```

### Daemon mode

```bash
cargo run --release -- \
  --config clickhouse.incremental.yaml \
  --daemon \
  --interval-secs 60
```

Purge options are applied only on the first daemon run.

## Metrics

The tool starts a Prometheus-style metrics endpoint on:

- `0.0.0.0:9991` (default)

Override with:

- CLI: `--metrics-port <port>`
- env: `CLICKHOUSE_TO_FALKORDB_METRICS_PORT`

Fetch metrics:

```bash
curl http://localhost:9991/
```

Exposed metric names:

- `clickhouse_to_falkordb_runs`
- `clickhouse_to_falkordb_failed_runs`
- `clickhouse_to_falkordb_rows_fetched`
- `clickhouse_to_falkordb_rows_written`
- `clickhouse_to_falkordb_rows_deleted`
- `clickhouse_to_falkordb_mapping_runs{mapping="<name>"}`
- `clickhouse_to_falkordb_mapping_failed_runs{mapping="<name>"}`
- `clickhouse_to_falkordb_mapping_rows_fetched{mapping="<name>"}`
- `clickhouse_to_falkordb_mapping_rows_written{mapping="<name>"}`
- `clickhouse_to_falkordb_mapping_rows_deleted{mapping="<name>"}`

## Example configs in this directory

- `clickhouse.incremental.yaml`
- `clickhouse.event_table.yaml`
- `example.file.yaml`

## Tests

```bash
cargo test
```

Includes:

- config parsing tests
- SQL builder tests
- optional ClickHouse connectivity smoke test (`CLICKHOUSE_URL`)
- optional FalkorDB connectivity smoke test (`FALKORDB_ENDPOINT`)
- optional end-to-end file -> FalkorDB test (`FALKORDB_ENDPOINT`)
