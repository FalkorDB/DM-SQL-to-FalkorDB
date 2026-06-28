# Oracle-to-FalkorDB Loader
Rust CLI tool to migrate and continuously sync data from Oracle into FalkorDB using declarative JSON/YAML mappings.

## Features
- Oracle source over native OCI driver (`oracle` crate)
- File source for local/testing workflows (`source.file`)
- Node + edge mappings
- Full and incremental sync modes
- CDC mode using Oracle LogMiner SCN polling
- Schema scaffolding via `--introspect-schema` and `--generate-template`
- Optional soft-delete handling via `delta.deleted_flag_*`
- Optional purge modes (`--purge-graph`, `--purge-mapping`)
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
oracle:
  connect_string: "$ORACLE_CONNECT_STRING"  # e.g. localhost:1521/FREEPDB1
  user: "$ORACLE_USER"
  password: "$ORACLE_PASSWORD"
  schema: "APP"
  query_timeout_ms: 60000
  fetch_batch_size: 5000
  cdc:
    enabled: true
    poll_interval_secs: 5
    max_scn_window: 50000
    start_scn: 0

falkordb:
  endpoint: "$FALKORDB_ENDPOINT"
  graph: "oracle_graph"
  max_unwind_batch_size: 1000

state:
  backend: "file"
  file_path: "state.json"

mappings:
  - type: node
    name: customers
    source:
      table: "APP.CUSTOMERS"
    mode: incremental
    delta:
      updated_at_column: "UPDATED_AT"
    labels: ["Customer"]
    key:
      column: "CUSTOMER_ID"
      property: "id"
    properties:
      email: { column: "EMAIL" }
      country: { column: "COUNTRY" }
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
- `oracle.connect_string`
- `oracle.host`
- `oracle.service_name`
- `oracle.user`
- `oracle.password`
- `oracle.schema`

## Running
### Scaffold mappings from source schema
Generate a schema summary:
```bash
cargo run --release -- \
  --config oracle.incremental.yaml \
  --introspect-schema
```

Generate a starter YAML template:
```bash
cargo run --release -- \
  --config oracle.incremental.yaml \
  --generate-template \
  --output oracle.generated.template.yaml
```

### Single run
```bash
cargo run --release -- \
  --config oracle.incremental.yaml
```

### Daemon mode
```bash
cargo run --release -- \
  --config oracle.incremental.yaml \
  --daemon \
  --interval-secs 60
```

### CDC mode (LogMiner)
```bash
cargo run --release -- \
  --config oracle.cdc.yaml
```

## Metrics
Default endpoint: `0.0.0.0:9998`

Override with:
- CLI: `--metrics-port <port>`
- env: `ORACLE_TO_FALKORDB_METRICS_PORT`

Fetch metrics:
```bash
curl http://localhost:9998/
```