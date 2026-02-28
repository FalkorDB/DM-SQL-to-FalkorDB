# Snowflake-to-FalkorDB Migration Tool

Rust-based CLI to migrate and continuously sync structured data from Snowflake into a FalkorDB graph, using a declarative mapping.

## Features

- Live Snowflake integration via `snowflake-connector-rs`
- JSON/YAML config describing mappings from tables/columns to nodes and edges
- Batched, parameterized Cypher `UNWIND` + `MERGE` into FalkorDB
- Incremental sync using an `updated_at` watermark per mapping
- Delete semantics via `deleted_flag` columns
- Purge options (entire graph or selected mappings)
- Daemon mode for periodic syncs
- Global and per-mapping metrics via HTTP endpoint

## Installation

Prerequisites:

- Rust toolchain (stable)
- Snowflake account with a user/warehouse/role that can read the relevant tables
- FalkorDB instance reachable from where you run the tool

Clone the repo and build:

```bash
cargo build --release
```

The binary will be at `target/release/snowflake_to_falkordb`.

## Configuration

Config is JSON or YAML, auto-detected by file extension.

### Top-level structure

```yaml
snowflake:
  account: "MY_ACCOUNT"
  user: "LOAD_USER"
  # one of the following auth methods:
  password: "********"             # password auth
  # private_key_path: "/path/to/key.pem"  # keypair auth (PEM)
  warehouse: "WH"
  database: "DB"
  schema: "PUBLIC"
  role: "SYSADMIN"
  query_timeout_ms: 60000

falkordb:
  endpoint: "falkor://127.0.0.1:6379"
  graph: "customer_graph"
  max_unwind_batch_size: 1000

state:
  backend: "file"                  # or "none" / "falkordb" (file is implemented)
  file_path: "state.json"          # optional, default: state.json

mappings:
  - type: node
    name: customers
    source:
      table: "DB.PUBLIC.CUSTOMERS" # or "select: ..." for custom SQL
      where: "ACTIVE = TRUE"       # optional extra predicate
    mode: incremental               # or "full"
    delta:
      updated_at_column: "UPDATED_AT"
      deleted_flag_column: "IS_DELETED"
      deleted_flag_value: true
      initial_full_load: true       # full once, then incremental
    labels: ["Customer"]
    key:
      column: "CUSTOMER_ID"
      property: "customer_id"
    properties:
      email:   { column: "EMAIL" }
      country: { column: "COUNTRY" }

  - type: node
    name: orders
    source:
      table: "DB.PUBLIC.ORDERS"
    mode: incremental
    delta:
      updated_at_column: "UPDATED_AT"
    labels: ["Order"]
    key:
      column: "ORDER_ID"
      property: "order_id"
    properties:
      amount: { column: "AMOUNT" }

  - type: edge
    name: customer_orders
    source:
      table: "DB.PUBLIC.ORDERS"
    mode: incremental
    delta:
      updated_at_column: "UPDATED_AT"
    relationship: "PURCHASED"
    direction: out                   # from customer to order
    from:
      node_mapping: customers
      match_on:
        - column: "CUSTOMER_ID"
          property: "customer_id"
    to:
      node_mapping: orders
      match_on:
        - column: "ORDER_ID"
          property: "order_id"
    key:
      column: "ORDER_ID"            # optional unique edge id
      property: "order_id"
    properties: {}
```

Key points:

- `source.table` + optional `source.where` are used to generate SELECT statements.
- `source.stream` can point at a Snowflake stream; the tool will generate `SELECT * FROM <stream>` (plus optional `WHERE`) and rely on the stream itself to expose only new/changed rows.
- If `delta.updated_at_column` is set and a watermark exists **and** `source.table` is used, the tool adds:
  - `AND updated_at_column > '<last_watermark>'` to the query.
- If `source.select` is used, the query is taken as-is (you manage watermark predicates manually).

Watermarks per mapping are stored in the `state` backend (currently `file`), keyed by mapping name. When `source.stream` is used, Snowflake manages change tracking internally for the stream; you can still use `delta.deleted_flag_column`/`deleted_flag_value` (for example, pointing at `METADATA$ACTION = 'DELETE'`) to let the loader translate stream events into node/edge deletes in FalkorDB.

## Running the tool

### Single run

```bash
cargo run --release -- \
  --config path/to/config.yaml
```

This will:

1. Connect to Snowflake and FalkorDB.
2. For each mapping:
   - Load rows from Snowflake (full or incremental).
   - Transform to nodes/edges.
   - Write in batches via `UNWIND` + `MERGE`.
   - Apply deletes if `deleted_flag_column`/`deleted_flag_value` are configured.
   - Update watermarks.

### Purge modes

#### Purge entire graph

```bash
cargo run --release -- \
  --config path/to/config.yaml \
  --purge-graph
```

This runs `MATCH (n) DETACH DELETE n` before loading.

#### Purge specific mappings

```bash
cargo run --release -- \
  --config path/to/config.yaml \
  --purge-mapping customers \
  --purge-mapping customer_orders
```

- Node mapping purge removes all nodes with the mapping's labels.
- Edge mapping purge removes all relationships of that mapping's relationship type between the associated labels.

### Daemon mode (periodic sync)

```bash
cargo run --release -- \
  --config path/to/config.yaml \
  --daemon \
  --interval-secs 300
```

Behavior:

- Runs an initial sync (optionally with purge flags) and then repeats every `interval-secs` seconds.
- On subsequent runs, purge flags are ignored; only incremental syncs run.
- Errors per run are logged via `tracing` and counted in metrics.

## Authentication to Snowflake

Two modes are supported via `SnowflakeConfig`:

1. **Password auth**

   ```yaml
   snowflake:
     account: "MY_ACCOUNT"
     user: "LOAD_USER"
     password: "********"
     # ...
   ```

2. **Keypair auth** (encrypted PEM)

   ```yaml
   snowflake:
     account: "MY_ACCOUNT"
     user: "LOAD_USER"
     private_key_path: "/path/to/key.pem"  # PEM file
     password: "passphrase"                # optional PEM passphrase
     # ...
   ```

If `private_key_path` is set, the tool uses keypair auth; otherwise it falls back to password auth. One of `password` or `private_key_path` must be set.

### Snowflake batch loading

For large incremental loads you can control how many rows are fetched per round trip from Snowflake using `fetch_batch_size` on `SnowflakeConfig`:

```yaml
snowflake:
  account: "MY_ACCOUNT"
  user: "LOAD_USER"
  password: "$SNOWFLAKE_PASSWORD"
  warehouse: "WH"
  database: "DB"
  schema: "PUBLIC"
  query_timeout_ms: 60000
  fetch_batch_size: 10000   # optional; default is to fetch all rows in one query
```

When `fetch_batch_size` is set to a positive value and **all** of the following are true for a mapping:

- `source.table` is used (not `source.select`), so the tool generates the `SELECT`.
- A `delta` block is configured, with `updated_at_column` set.

the tool will fetch rows from Snowflake in pages using `ORDER BY <updated_at_column> LIMIT <batch_size> OFFSET <n>`, appending all pages into a single in-memory batch for that mapping. This keeps each individual Snowflake result set bounded while preserving the same semantics as a single `SELECT` with the incremental predicate.

If `fetch_batch_size` is not set, or if the mapping uses `source.select` (custom SQL) or has no `delta` block, the tool falls back to a single query that returns all rows for that mapping.

## Metrics and Monitoring

A lightweight HTTP metrics server is started automatically on:

- `0.0.0.0:9898`

Fetch metrics:

```bash
curl http://localhost:9898/
```

Example output (Prometheus-style):

```text
snowflake_to_falkordb_runs 3
snowflake_to_falkordb_failed_runs 0
snowflake_to_falkordb_rows_fetched 12345
snowflake_to_falkordb_rows_written 12000
snowflake_to_falkordb_rows_deleted 345
snowflake_to_falkordb_mapping_runs{mapping="customers"} 3
snowflake_to_falkordb_mapping_failed_runs{mapping="customers"} 0
snowflake_to_falkordb_mapping_rows_fetched{mapping="customers"} 8000
snowflake_to_falkordb_mapping_rows_written{mapping="customers"} 7800
snowflake_to_falkordb_mapping_rows_deleted{mapping="customers"} 200
snowflake_to_falkordb_mapping_runs{mapping="orders"} 3
...
```

These metrics let you see, per mapping, how many rows were fetched, written, and deleted, and how often each mapping ran.

## Operational notes

- **Idempotency**: node/edge writes use `MERGE` on configured keys, so re-running the same data is safe.
- **Incremental safety**: watermarks are only advanced after successful writes; if a run fails mid-way, the next run will retry from the last successful watermark.
- **Deletes**: any row where `deleted_flag_column == deleted_flag_value` is treated as deleted:
  - Node mappings: matching nodes are `DETACH DELETE`d.
  - Edge mappings: matching relationships are `DELETE`d.
- **Ordering**: mappings are processed in the order listed; for edges, the referenced node mappings must exist in the config.
- **Logging**: uses `tracing` with log level controlled by `RUST_LOG`, e.g. `RUST_LOG=info`.

## Troubleshooting

- Check metrics at `http://localhost:9898/` to see if a particular mapping is stuck (e.g. zero rows written or growing failed runs).
- Enable debug logging:

```bash
RUST_LOG=debug,snowflake_to_falkordb=debug cargo run --release -- --config cfg.yaml
```

- Verify Snowflake connectivity and credentials if you see errors during `fetch_rows_for_mapping`.
- Verify FalkorDB endpoint and graph name if you see Cypher execution errors.

## Example Snowflake configs

This repo includes two ready-to-use Snowflake configs:

- `snowflake_check.yaml` – simple connectivity check that queries `INFORMATION_SCHEMA.TABLES` with a `WHERE 1 = 0` predicate (no data returned; just validates login and metadata access).
- `snowflake_menu.yaml` – full example that builds a small menu graph from Snowflake sample data.

Both rely on an environment variable for the Snowflake password:

- In the config you will see `password: $SNOWFLAKE_PASSWORD`.
- At runtime, `Config::from_file` treats a leading `$` in `snowflake.password` as an environment variable name and substitutes it.

To run the menu example end-to-end (after provisioning the sample data and FalkorDB):

```bash
export SNOWFLAKE_PASSWORD=...   # password for the Snowflake user
cargo run --release -- --config snowflake_menu.yaml
```

This will:

- Read from `SNOWFLAKE_LEARNING_DB.SHAHARBIRON_LOAD_SAMPLE_DATA_FROM_S3.MENU`.
- Create `MenuItem`, `MenuType`, `TruckBrand`, `ItemCategory`, and `ItemSubcategory` nodes.
- Create edges such as `(:MenuItem)-[:IN_MENU_TYPE]->(:MenuType)` and `(:MenuItem)-[:SOLD_BY]->(:TruckBrand)`.
- Load into the `snowflake_menu` graph in FalkorDB.

If you only want to validate Snowflake connectivity without loading data into a meaningful graph, you can instead use:

```bash
export SNOWFLAKE_PASSWORD=...
cargo run --release -- --config snowflake_check.yaml
```

## Tests

A small test suite is provided to validate config parsing, connectivity, and a minimal end-to-end load. All tests live alongside the code in the `src/` modules and can be run with:

```bash
cargo test
```

### Pure unit tests

These run entirely in-memory and do not require external services:

- Config parsing tests (`src/config.rs`):
  - Verify YAML and JSON configs load correctly.
  - Verify `$ENV_VAR` resolution for `snowflake.password`.

### Optional integration tests (Snowflake & FalkorDB)

The following tests are **no-ops** unless the corresponding environment variables are set. This keeps `cargo test` safe in environments where Snowflake or FalkorDB are not available.

#### Snowflake connectivity

- Test: `source::tests::snowflake_connectivity_smoke_test` (`src/source.rs`).
- Env vars required for the test to actually hit Snowflake:
  - `SNOWFLAKE_ACCOUNT`
  - `SNOWFLAKE_USER`
  - `SNOWFLAKE_PASSWORD`
  - `SNOWFLAKE_WAREHOUSE`
  - `SNOWFLAKE_DATABASE`
  - `SNOWFLAKE_SCHEMA`

If any of these are missing, the test returns `Ok(())` without running a query. With them set, it runs a small `SELECT 1 AS ONE` via the same Snowflake client code used in production.

#### FalkorDB connectivity

- Test: `sink_async::tests::falkordb_connectivity_smoke_test` (`src/sink_async.rs`).
- Env vars:
  - `FALKORDB_ENDPOINT` (e.g. `falkor://127.0.0.1:6379`)
  - `FALKORDB_GRAPH` (optional, defaults to `snowflake_to_falkordb_test`)

If `FALKORDB_ENDPOINT` is not set, the test is a no-op. Otherwise it connects to FalkorDB and runs a simple `RETURN 1` query.

#### End-to-end file → FalkorDB load

- Test: `orchestrator::tests::end_to_end_file_load_into_falkordb` (`src/orchestrator.rs`).
- Env vars:
  - `FALKORDB_ENDPOINT` (required)
  - `FALKORDB_GRAPH` (optional, defaults to `snowflake_to_falkordb_load_test`)

This test:

- Writes a tiny JSON array to a temp file:
  - `[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]`.
- Constructs an in-memory config using `source.file` to read that JSON.
- Defines a simple node mapping (`TestNode` label, `id` and `name` properties).
- Calls `run_once`, exercising the full source → mapping → async sink → FalkorDB pipeline.

If `FALKORDB_ENDPOINT` is not set, the test returns `Ok(())` without touching FalkorDB.
