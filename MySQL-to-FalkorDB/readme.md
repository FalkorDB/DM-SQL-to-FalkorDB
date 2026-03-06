# MySQL-to-FalkorDB Loader
Rust CLI tool to migrate and continuously sync data from MySQL into FalkorDB using declarative JSON/YAML mappings.
## Features
- MySQL source over native protocol (`mysql_async`)
- File source for local/testing workflows (`source.file`)
- Node + edge mappings
- Full and incremental sync modes
- Optional soft-delete handling via `delta.deleted_flag_*`
- Optional purge modes:
  - whole graph (`--purge-graph`)
  - selected mappings (`--purge-mapping`)
- Daemon mode (`--daemon --interval-secs <N>`)
- Prometheus-style metrics endpoint
- Optional `mysql.cdc` config block (schema scaffold for upcoming binlog/CDC mode)
## Build
From this directory:
```bash
cargo build --release
```
## Configuration
Config can be YAML or JSON.
Top-level structure:
```yaml
mysql:
  # Either full URL:
  url: "$MYSQL_URL"               # e.g. mysql://root:pass@localhost:3306/analytics
  # ...or discrete fields:
  # host: "localhost"
  # port: 3306
  user: "root"
  password: "$MYSQL_PASSWORD"
  database: "analytics"
  query_timeout_ms: 60000
  fetch_batch_size: 5000          # optional; enables incremental page fetches
  cdc:                            # optional scaffold for future native binlog mode
    enabled: false
    mode: "binlog"
    server_id: 101

falkordb:
  endpoint: "falkor://127.0.0.1:6379"
  graph: "mysql_graph"
  max_unwind_batch_size: 1000

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
- `mysql.url`
- `mysql.host`
- `mysql.user`
- `mysql.password`
- `mysql.database`

## End-to-end sample dataset migration
This directory includes a complete sample dataset and config so you can run an end-to-end MySQL → FalkorDB migration quickly.

Included files:
- Schema SQL: `sample_data/mysql-sample-schema.sql`
- Seed data SQL: `sample_data/mysql-sample-data.sql`
- Loader config: `mysql_sample_to_falkordb.yaml`

### 1) Create and seed the sample MySQL database
From `MySQL-to-FalkorDB/`:

```bash
mysql -u root -p < sample_data/mysql-sample-schema.sql
mysql -u root -p < sample_data/mysql-sample-data.sql
```

This creates database `mysql_falkordb_sample` with:
- `customers`
- `products`
- `orders`
- `order_items`

### 2) Run the migration into FalkorDB
```bash
export MYSQL_URL="mysql://root:password@localhost:3306/mysql_falkordb_sample"
export FALKORDB_ENDPOINT="falkor://127.0.0.1:6379"

cargo run --release -- \
  --config mysql_sample_to_falkordb.yaml
```

The sample config creates:
- `(:Customer)` nodes from `customers`
- `(:Product)` nodes from `products`
- `(:Order)` nodes from `orders`
- `(:Customer)-[:PLACED]->(:Order)` edges from `orders`
- `(:Order)-[:CONTAINS {quantity, unit_price}]->(:Product)` edges from `order_items`

### 3) Validate incremental sync behavior
Apply a few source changes in MySQL:

```bash
mysql -u root -p mysql_falkordb_sample <<'SQL'
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
  --config mysql_sample_to_falkordb.yaml
```

Expected behavior:
- Updated customer properties are upserted.
- New order and order-item relationships are added.
- Product `103` is deleted from the graph due to `is_deleted = 1` (soft-delete rule in `delta`).
## Running
### Single run
```bash
cargo run --release -- \
  --config mysql.incremental.yaml
```
### Purge full graph before load
```bash
cargo run --release -- \
  --config mysql.incremental.yaml \
  --purge-graph
```
### Purge selected mappings
```bash
cargo run --release -- \
  --config mysql.incremental.yaml \
  --purge-mapping customers \
  --purge-mapping customer_orders
```
### Daemon mode
```bash
cargo run --release -- \
  --config mysql.incremental.yaml \
  --daemon \
  --interval-secs 60
```
Purge options are applied only on the first daemon run.
## Metrics
The tool starts a Prometheus-style metrics endpoint on:
- `0.0.0.0:9995` (default)
Override with:
- CLI: `--metrics-port <port>`
- env: `MYSQL_TO_FALKORDB_METRICS_PORT`
Fetch metrics:
```bash
curl http://localhost:9995/
```
Exposed metric names:
- `mysql_to_falkordb_runs`
- `mysql_to_falkordb_failed_runs`
- `mysql_to_falkordb_rows_fetched`
- `mysql_to_falkordb_rows_written`
- `mysql_to_falkordb_rows_deleted`
- `mysql_to_falkordb_mapping_runs{mapping="<name>"}`
- `mysql_to_falkordb_mapping_failed_runs{mapping="<name>"}`
- `mysql_to_falkordb_mapping_rows_fetched{mapping="<name>"}`
- `mysql_to_falkordb_mapping_rows_written{mapping="<name>"}`
- `mysql_to_falkordb_mapping_rows_deleted{mapping="<name>"}`
## Example configs in this directory
- `mysql.incremental.yaml`
- `mysql.event_table.yaml`
- `mysql_sample_to_falkordb.yaml`
- `example.file.yaml`
## Tests
```bash
cargo test
```
Includes:
- config parsing tests
- SQL builder tests
- optional MySQL connectivity smoke test (`MYSQL_URL`)
- optional FalkorDB connectivity smoke test (`FALKORDB_ENDPOINT`)
- optional end-to-end file -> FalkorDB test (`FALKORDB_ENDPOINT`)
