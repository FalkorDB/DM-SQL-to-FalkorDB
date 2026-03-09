# MariaDB-to-FalkorDB Loader
Rust CLI tool to migrate and continuously sync data from MariaDB into FalkorDB using declarative JSON/YAML mappings.
## Features
- MariaDB source over native protocol (`mysql_async`)
- File source for local/testing workflows (`source.file`)
- Node + edge mappings
- Full and incremental sync modes
- Schema scaffolding via `--introspect-schema` and `--generate-template` (with `--output` support)
- Optional soft-delete handling via `delta.deleted_flag_*`
- Optional purge modes:
  - whole graph (`--purge-graph`)
  - selected mappings (`--purge-mapping`)
- Daemon mode (`--daemon --interval-secs <N>`)
- Prometheus-style metrics endpoint
- Optional `mariadb.cdc` config block (schema scaffold for upcoming binlog/CDC mode)
## Build
From this directory:
```bash
cargo build --release
```
## Configuration
Config can be YAML or JSON.
Top-level structure:
```yaml
mariadb:
  # Either full URL:
  url: "$MARIADB_URL"               # e.g. mysql://root:pass@localhost:3306/analytics
  # ...or discrete fields:
  # host: "localhost"
  # port: 3306
  user: "root"
  password: "$MARIADB_PASSWORD"
  database: "analytics"
  query_timeout_ms: 60000
  fetch_batch_size: 5000          # optional; enables incremental page fetches
  cdc:                            # optional scaffold for future native binlog mode
    enabled: false
    mode: "binlog"
    server_id: 101

falkordb:
  endpoint: "falkor://127.0.0.1:6379"
  graph: "mariadb_graph"
  max_unwind_batch_size: 1000
  indexes:                      # optional explicit FalkorDB indexes
    - labels: ["Customer"]
      property: "customer_id"
      source_table: "analytics.customers"   # optional provenance metadata
      source_columns: ["customer_id"]       # optional provenance metadata

state:
  backend: "file"
  file_path: "state.json"

mappings:
  - type: node
    name: customers
    source:
      table: "analytics.customers"
      where: "active = true"
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
- `mariadb.url`
- `mariadb.host`
- `mariadb.user`
- `mariadb.password`
- `mariadb.database`
### FalkorDB index behavior
- The loader always attempts required implicit indexes before writes:
  - node key properties
  - edge endpoint `match_on` properties
- You can also provide explicit indexes in `falkordb.indexes`.
- Explicit and implicit indexes are deduplicated and applied in both initial and incremental runs.

## End-to-end sample dataset migration
This directory includes a complete sample dataset and config so you can run an end-to-end MariaDB → FalkorDB migration quickly.

Included files:
- Schema SQL: `sample_data/mariadb-sample-schema.sql`
- Seed data SQL: `sample_data/mariadb-sample-data.sql`
- Loader config: `mariadb_sample_to_falkordb.yaml`

### 1) Create and seed the sample MariaDB database
From `MariaDB-to-FalkorDB/`:

```bash
mariadb -u root -p < sample_data/mariadb-sample-schema.sql
mariadb -u root -p < sample_data/mariadb-sample-data.sql
```

This creates database `mariadb_falkordb_sample` with:
- `customers`
- `products`
- `orders`
- `order_items`

### 2) Run the migration into FalkorDB
```bash
export MARIADB_URL="mysql://root:password@localhost:3306/mariadb_falkordb_sample"
export FALKORDB_ENDPOINT="falkor://127.0.0.1:6379"

cargo run --release -- \
  --config mariadb_sample_to_falkordb.yaml
```

The sample config creates:
- `(:Customer)` nodes from `customers`
- `(:Product)` nodes from `products`
- `(:Order)` nodes from `orders`
- `(:Customer)-[:PLACED]->(:Order)` edges from `orders`
- `(:Order)-[:CONTAINS {quantity, unit_price}]->(:Product)` edges from `order_items`

### 3) Validate incremental sync behavior
Apply a few source changes in MariaDB:

```bash
mariadb -u root -p mariadb_falkordb_sample <<'SQL'
UPDATE customers
SET city = 'Munich', updated_at = NOW(6)
WHERE customer_id = 1;

INSERT INTO orders (order_id, customer_id, status, ordered_at, total_amount, is_deleted, updated_at)
VALUES (5004, 1, 'PAID', NOW(6), 49.00, 0, NOW(6));

INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, is_deleted, updated_at)
VALUES (9005, 5004, 102, 1, 49.00, 0, NOW(6));

UPDATE products
SET is_deleted = 1, updated_at = NOW(6)
WHERE product_id = 103;
SQL
```

Re-run:

```bash
cargo run --release -- \
  --config mariadb_sample_to_falkordb.yaml
```

Expected behavior:
- Updated customer properties are upserted.
- New order and order-item relationships are added.
- Product `103` is deleted from the graph due to `is_deleted = 1` (soft-delete rule in `delta`).
## Running
### Scaffold mappings from source schema (new)
Generate a schema summary:
```bash
cargo run --release -- \
  --config mariadb.incremental.yaml \
  --introspect-schema
```
Generate a starter YAML template:
```bash
cargo run --release -- \
  --config mariadb.incremental.yaml \
  --generate-template \
  --output mariadb.generated.template.yaml
```
Notes:
- Scaffold flags are read-only against the source and do not execute migration.
- Scaffold mode cannot be combined with daemon/purge flags.
- The generated template is a starting point and should be reviewed before production use.
### Single run
```bash
cargo run --release -- \
  --config mariadb.incremental.yaml
```
### Purge full graph before load
```bash
cargo run --release -- \
  --config mariadb.incremental.yaml \
  --purge-graph
```
### Purge selected mappings
```bash
cargo run --release -- \
  --config mariadb.incremental.yaml \
  --purge-mapping customers \
  --purge-mapping customer_orders
```
### Daemon mode
```bash
cargo run --release -- \
  --config mariadb.incremental.yaml \
  --daemon \
  --interval-secs 60
```
Purge options are applied only on the first daemon run.
## Metrics
The tool starts a Prometheus-style metrics endpoint on:
- `0.0.0.0:9997` (default)
Override with:
- CLI: `--metrics-port <port>`
- env: `MARIADB_TO_FALKORDB_METRICS_PORT`
Fetch metrics:
```bash
curl http://localhost:9997/
```
Exposed metric names:
- `mariadb_to_falkordb_runs`
- `mariadb_to_falkordb_failed_runs`
- `mariadb_to_falkordb_rows_fetched`
- `mariadb_to_falkordb_rows_written`
- `mariadb_to_falkordb_rows_deleted`
- `mariadb_to_falkordb_mapping_runs{mapping="<name>"}`
- `mariadb_to_falkordb_mapping_failed_runs{mapping="<name>"}`
- `mariadb_to_falkordb_mapping_rows_fetched{mapping="<name>"}`
- `mariadb_to_falkordb_mapping_rows_written{mapping="<name>"}`
- `mariadb_to_falkordb_mapping_rows_deleted{mapping="<name>"}`
## Example configs in this directory
- `mariadb.incremental.yaml`
- `mariadb.event_table.yaml`
- `mariadb_sample_to_falkordb.yaml`
- `example.file.yaml`
## Tests
```bash
cargo test
```
Includes:
- config parsing tests
- SQL builder tests
- optional MariaDB connectivity smoke test (`MARIADB_URL`)
- optional FalkorDB connectivity smoke test (`FALKORDB_ENDPOINT`)
- optional end-to-end file -> FalkorDB test (`FALKORDB_ENDPOINT`)
