# SQLServer-to-FalkorDB Loader
Rust CLI tool to migrate and continuously sync data from SQL Server into FalkorDB using declarative JSON/YAML mappings.

## Features
- SQL Server source over native TDS protocol (`tiberius`)
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
sqlserver:
  # Prefer an ADO connection string:
  connection_string: "$SQLSERVER_CONNECTION_STRING"
  # Example value:
  # "server=tcp:localhost,1433;User Id=sa;Password=...;Database=analytics;TrustServerCertificate=true;"

  # Optional discrete fields when connection_string is not set:
  # host: "localhost"
  # port: 1433
  # user: "sa"
  # password: "$SQLSERVER_PASSWORD"
  # database: "analytics"

  trust_cert: true
  query_timeout_ms: 60000
  fetch_batch_size: 5000   # optional; enables incremental page fetches

falkordb:
  endpoint: "falkor://127.0.0.1:6379"
  graph: "sqlserver_graph"
  max_unwind_batch_size: 1000

state:
  backend: "file"
  file_path: "state.json"

mappings:
  - type: node
    name: customers
    source:
      table: "dbo.customers"
      where: "is_active = 1"
    mode: incremental
    delta:
      updated_at_column: "updated_at"
      deleted_flag_column: "is_deleted"
      deleted_flag_value: true
      initial_full_load: true
    labels: ["Customer"]
    key:
      column: "customer_id"
      property: "customer_id"
    properties:
      email: { column: "email" }
      city: { column: "city" }
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
- `sqlserver.connection_string`
- `sqlserver.host`
- `sqlserver.user`
- `sqlserver.password`
- `sqlserver.database`

## End-to-end sample dataset migration
Included files:
- Schema SQL: `sample_data/sqlserver-sample-schema.sql`
- Seed data SQL: `sample_data/sqlserver-sample-data.sql`
- Loader config: `sqlserver_sample_to_falkordb.yaml`

### 1) Create and seed the sample SQL Server database
```bash
sqlcmd -S localhost,1433 -U sa -P "$SQLSERVER_PASSWORD" -i sample_data/sqlserver-sample-schema.sql
sqlcmd -S localhost,1433 -U sa -P "$SQLSERVER_PASSWORD" -i sample_data/sqlserver-sample-data.sql
```

### 2) Run the migration into FalkorDB
```bash
export SQLSERVER_CONNECTION_STRING="server=tcp:localhost,1433;User Id=sa;Password=${SQLSERVER_PASSWORD};Database=sqlserver_falkordb_sample;TrustServerCertificate=true;"
export FALKORDB_ENDPOINT="falkor://127.0.0.1:6379"

cargo run --release --   --config sqlserver_sample_to_falkordb.yaml
```

## Running
### Scaffold mappings from source schema (new)
Generate a schema summary:
```bash
cargo run --release -- \
  --config sqlserver.incremental.yaml \
  --introspect-schema
```
Generate a starter YAML template:
```bash
cargo run --release -- \
  --config sqlserver.incremental.yaml \
  --generate-template \
  --output sqlserver.generated.template.yaml
```
Notes:
- Scaffold flags are read-only against the source and do not execute migration.
- Scaffold mode cannot be combined with daemon/purge flags.
- The generated template is a starting point and should be reviewed before production use.
### Single run
```bash
cargo run --release --   --config sqlserver.incremental.yaml
```

### Purge full graph before load
```bash
cargo run --release --   --config sqlserver.incremental.yaml   --purge-graph
```

### Purge selected mappings
```bash
cargo run --release --   --config sqlserver.incremental.yaml   --purge-mapping customers   --purge-mapping customer_orders
```

### Daemon mode
```bash
cargo run --release --   --config sqlserver.incremental.yaml   --daemon   --interval-secs 60
```
Purge options are applied only on the first daemon run.

## Metrics
The tool starts a Prometheus-style metrics endpoint on:
- `0.0.0.0:9996` (default)

Override with:
- CLI: `--metrics-port <port>`
- Env: `SQLSERVER_TO_FALKORDB_METRICS_PORT`

Fetch metrics:
```bash
curl http://localhost:9996/
```

Exposed metric names:
- `sqlserver_to_falkordb_runs`
- `sqlserver_to_falkordb_failed_runs`
- `sqlserver_to_falkordb_rows_fetched`
- `sqlserver_to_falkordb_rows_written`
- `sqlserver_to_falkordb_rows_deleted`
- `sqlserver_to_falkordb_mapping_runs{mapping="<name>"}`
- `sqlserver_to_falkordb_mapping_failed_runs{mapping="<name>"}`
- `sqlserver_to_falkordb_mapping_rows_fetched{mapping="<name>"}`
- `sqlserver_to_falkordb_mapping_rows_written{mapping="<name>"}`
- `sqlserver_to_falkordb_mapping_rows_deleted{mapping="<name>"}`

## Example configs in this directory
- `sqlserver.incremental.yaml`
- `sqlserver.event_table.yaml`
- `sqlserver_sample_to_falkordb.yaml`
- `example.file.yaml`

## Tests
```bash
cargo test
```

Includes:
- config parsing tests
- SQL builder tests
- optional SQL Server connectivity smoke test (`SQLSERVER_CONNECTION_STRING`)
- optional FalkorDB connectivity smoke test (`FALKORDB_ENDPOINT`)
- optional end-to-end file -> FalkorDB test (`FALKORDB_ENDPOINT`)
