# Databricks-to-FalkorDB Loader

A Rust CLI tool that loads and incrementally syncs tabular data from Databricks (via Databricks SQL) into a FalkorDB graph using a declarative JSON/YAML configuration.

The design mirrors the Snowflake-to-FalkorDB loader: you describe how Databricks tables map to graph nodes and edges, and the tool handles batching, UNWIND+MERGE upserts, optional soft deletes, and incremental updates based on a watermark column.

## Features

- **Databricks SQL source**
  - Connects to a Databricks SQL warehouse via the REST Statement Execution API.
  - Supports `table` + optional `where` or full custom `select` queries.
- **FalkorDB sink**
  - Writes nodes and edges using Cypher `UNWIND` + `MERGE`.
  - Automatically creates indexes on node key properties for better MERGE/MATCH performance.
- **Incremental sync with delta**
  - Per-mapping `mode: full` or `mode: incremental`.
  - Watermark column (`delta.updated_at_column`) used to fetch only new/updated rows.
  - Optional soft delete semantics using `delta.deleted_flag_column` and `delta.deleted_flag_value`.
- **Persistent state**
  - File-backed state (`state.backend: file`) stores per-mapping watermarks between runs.
  - Safe to stop and restart; sync resumes from the last watermark.

## Quick Start

### 1. Build

From the crate directory:

```bash
cargo build --release
```

### 2. Configure Databricks and FalkorDB

Example YAML config:

```yaml
databricks:
  host: "adb-1234567890123456.7.azuredatabricks.net"
  http_path: "/sql/1.0/warehouses/your_warehouse_id"
  access_token: "$DATABRICKS_TOKEN"
  query_timeout_ms: 600000

falkordb:
  endpoint: "falkor://127.0.0.1:6379"
  graph: "databricks_graph"
  max_unwind_batch_size: 1000

state:
  backend: file
  file_path: "dbx_state.json"

mappings:
  - type: node
    name: customers
    source:
      table: "main.default.customers"
      where: "active = true"
    mode: incremental
    delta:
      updated_at_column: "updated_at"
      deleted_flag_column: "is_deleted"
      deleted_flag_value: true
    labels: ["Customer"]
    key:
      column: "id"
      property: "id"
    properties:
      name:  { column: "name" }
      email: { column: "email" }
```

Notes:

- `access_token` can be given directly or as an environment reference like `$DATABRICKS_TOKEN`; the tool resolves it from the environment.
- `updated_at_column` should be a timestamp-like column; first run does a full load, subsequent runs use `updated_at > last_watermark`.
- When `is_deleted` matches `deleted_flag_value`, the corresponding node (or edge, if configured on an edge mapping) is deleted from FalkorDB.

### 3. Run

```bash
# Environment variables (example)
export DATABRICKS_TOKEN="..."
export FALKORDB_ENDPOINT="falkor://127.0.0.1:6379"   # for tests if needed

# Run sync once
cargo run --release -- --config path/to/config.yaml
```

The tool will:

1. Load the config.
2. Connect to FalkorDB and ensure indexes exist for all node key properties.
3. Load existing watermarks from the state file (if present).
4. For each mapping:
   - Build a Databricks SQL query with optional `where` and watermark predicate.
   - Fetch rows (following result chunks if needed).
   - Split rows into active vs. deleted (if `delta.deleted_flag_*` is configured).
   - Upsert active rows as nodes/edges.
   - Delete soft-deleted nodes/edges.
   - Update and persist the watermark.

## Concrete Databricks Table → Graph Example

Assume you have a Databricks table `main.analytics.orders` with the following columns:

- `order_id` (BIGINT): unique order identifier
- `customer_id` (BIGINT): foreign key to a customers table
- `order_ts` (TIMESTAMP): order time
- `total_usd` (DECIMAL): order total
- `status` (STRING): e.g. `"OPEN"`, `"CANCELLED"`, `"COMPLETED"`
- `is_deleted` (BOOLEAN): soft delete flag maintained by your ETL
- `updated_at` (TIMESTAMP): last modification time for the row

You might want the following graph model:

- Node: `Order` with key `order_id`, properties `{ total_usd, status, order_ts }`.
- Node: `Customer` (assuming it is loaded by a separate mapping).
- Edge: `(:Customer)-[:PLACED]->(:Order)` linking customers to their orders.

Example mappings (only showing relevant parts):

```yaml
databricks:
  host: "adb-...azuredatabricks.net"
  http_path: "/sql/1.0/warehouses/your_warehouse_id"
  access_token: "$DATABRICKS_TOKEN"

falkordb:
  endpoint: "falkor://127.0.0.1:6379"
  graph: "orders_graph"

state:
  backend: file
  file_path: "orders_state.json"

mappings:
  # 1) Order nodes
  - type: node
    name: orders
    source:
      table: "main.analytics.orders"
    mode: incremental
    delta:
      updated_at_column: "updated_at"
      deleted_flag_column: "is_deleted"
      deleted_flag_value: true
    labels: ["Order"]
    key:
      column: "order_id"
      property: "order_id"
    properties:
      total_usd: { column: "total_usd" }
      status:    { column: "status" }
      order_ts:  { column: "order_ts" }

  # 2) Customer nodes (simplified example; typically loaded from another table)
  - type: node
    name: customers
    source:
      table: "main.analytics.customers"
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
      name:  { column: "name" }
      email: { column: "email" }

  # 3) Edges: Customer -> Order
  - type: edge
    name: customer_orders
    source:
      table: "main.analytics.orders"
    mode: incremental
    delta:
      updated_at_column: "updated_at"
      deleted_flag_column: "is_deleted"
      deleted_flag_value: true
    relationship: "PLACED"
    direction: out      # Customer -[:PLACED]-> Order
    from:
      node_mapping: customers
      match_on:
        - column: "customer_id"
          property: "customer_id"
    to:
      node_mapping: orders
      match_on:
        - column: "order_id"
          property: "order_id"
    properties:
      order_ts: { column: "order_ts" }
```

With this configuration:

- First run will load all current orders and customers and create `PLACED` edges.
- Subsequent runs will:
  - Fetch only rows where `updated_at > <last_watermark>`.
  - Upsert changed orders/customers.
  - Create or update `PLACED` edges for new/updated rows.
  - Remove orders and edges for rows where `is_deleted = true`.

## Configuration Reference (Overview)

### `databricks` section

- `host`: Databricks workspace hostname, e.g. `adb-...azuredatabricks.net`.
- `http_path`: HTTP path to the SQL warehouse or endpoint (last segment is used as `warehouse_id`).
- `access_token`: Databricks access token (can be `$ENV_VAR`).
- `catalog`, `schema`: currently unused placeholders for future defaults.
- `fetch_batch_size`: currently unused; paging relies on Databricks chunks.
- `query_timeout_ms`: HTTP client timeout per statement.

### `falkordb` section

- `endpoint`: FalkorDB connection string, e.g. `falkor://127.0.0.1:6379`.
- `graph`: target graph name.
- `max_unwind_batch_size`: max rows per UNWIND batch when writing.

### `state` section

- `backend`: currently `file` or `none`.
- `file_path`: path to the JSON file storing watermarks (default `state.json` if omitted).

### `mappings` section

Each mapping is either a `node` or `edge` mapping.

Common fields (`CommonMappingFields`):

- `name`: logical name of the mapping.
- `source`:
  - `file`: optional path to a local JSON file (array of objects) for testing.
  - `table`: Databricks table name, e.g. `catalog.schema.table`.
  - `select`: custom SELECT statement (wrapped as a subquery if a watermark predicate is applied).
  - `where`: optional additional SQL predicate.
- `mode`: `full` or `incremental`.
- `delta` (optional):
  - `updated_at_column`: used as watermark for incremental loads.
  - `deleted_flag_column`: optional soft delete flag column.
  - `deleted_flag_value`: value considered as deleted (e.g. `true`, `1`, or `'Y'`).
  - `initial_full_load`: reserved for future use.

Node mappings:

- `labels`: list of labels, e.g. `["Customer"]`.
- `key`:
  - `column`: source column that uniquely identifies the node.
  - `property`: property name on the node storing that key.
- `properties`: map from graph property name to `{ column: <source_column> }`.

Edge mappings:

- `relationship`: relationship type, e.g. `"PURCHASED"`.
- `direction`: `"out"` or `"in"` from `from` to `to`.
- `from` / `to`:
  - `node_mapping`: name of a node mapping.
  - `match_on`: list of `{ column, property }` describing how to find endpoint nodes.
  - `label_override`: optional labels overriding those of the node mapping.
- `key` (optional): edge identifier (`column` and `property`).
- `properties`: same structure as for node properties.

## Tests

From the crate directory:

```bash
cargo test
```

Tests include:

- Config parsing tests (JSON/YAML, env var resolution).
- Optional Databricks connectivity smoke test:
  - Uses `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN` if set.
  - Skips itself (returns `Ok(())`) when not configured.
- Optional end-to-end file → FalkorDB test:
  - Uses `FALKORDB_ENDPOINT` (and optional `FALKORDB_GRAPH`).
  - Skips when `FALKORDB_ENDPOINT` is not set.

## Limitations and Future Work

- Only the Databricks SQL Statement Execution API is supported (no streaming/CDC yet).
- Delta support is based on a single `updated_at_column` per mapping; advanced scenarios (e.g. Databricks change data feed) are not yet wired.
- `state.backend: falkordb` is not implemented yet; today only file-backed state is supported.

Despite these limitations, the tool is suitable for many batch and incremental loading scenarios from Databricks into FalkorDB.
