# PostgreSQL-to-FalkorDB Loader

Rust-based CLI tool to migrate and continuously sync structured data from PostgreSQL into a FalkorDB graph, using a declarative mapping.

The design mirrors the existing Snowflake-to-FalkorDB and Databricks-to-FalkorDB loaders: you describe how PostgreSQL tables map to graph nodes and edges in a JSON/YAML config, and the tool handles batching, UNWIND+MERGE upserts, optional soft deletes, and incremental updates based on a watermark column.

## Features

- **PostgreSQL source**
  - Connects to PostgreSQL using `tokio-postgres`.
  - Supports `table` + optional `where` or full custom `select` queries per mapping.
  - Optional paging via `fetch_batch_size` for large incremental loads.
- **FalkorDB sink**
  - Writes nodes and edges using Cypher `UNWIND` + `MERGE`.
  - Automatically creates indexes on node key properties for better MERGE/MATCH performance.
- **Incremental sync with delta**
  - Per-mapping `mode: full` or `mode: incremental`.
  - Watermark column (`delta.updated_at_column`) used to fetch only new/updated rows.
  - Optional soft delete semantics using `delta.deleted_flag_column` and `delta.deleted_flag_value`.
  - Optional `delta.initial_full_load: false` to skip the initial backfill and start syncing "from now".
- **Persistent state**
  - File-backed state (`state.backend: file`) stores per-mapping watermarks between runs.
  - Safe to stop and restart; sync resumes from the last watermark.
- **Online / daemon mode**
  - Optional daemon mode that repeats syncs at a fixed interval to maintain one-way sync.

## Layout

This directory contains:

- `postgres-to-falkordb/` – Rust crate implementing the CLI tool.
  - `src/config.rs` – configuration model (Postgres, FalkorDB, state, mappings).
  - `src/source.rs` – PostgreSQL + file sources.
  - `src/mapping.rs` – row-to-node/edge mapping.
  - `src/sink.rs`, `src/sink_async.rs` – FalkorDB write path.
  - `src/state.rs` – file-backed watermark store.
  - `src/orchestrator.rs` – main sync loop (full/incremental, daemon).
  - `src/main.rs` – CLI entrypoint.
- `postgres-to-falkordb/example.config.yaml` – example config showing nodes and edges from PostgreSQL tables.

## Configuration

Config can be JSON or YAML and is auto-detected by file extension.

### Top-level structure

```yaml
postgres:
  # Either provide a full URL or discrete connection fields.
  url: "$POSTGRES_URL"          # e.g. postgres://user:pass@host:5432/db
  # host: "localhost"
  # port: 5432
  # user: "loader"
  # password: "$POSTGRES_PASSWORD"
  # dbname: "source_db"
  # sslmode: "prefer"
  fetch_batch_size: 10000       # optional; enables paged SELECTs for incremental loads
  query_timeout_ms: 60000       # optional; sets statement_timeout per session

falkordb:
  endpoint: "falkor://127.0.0.1:6379"
  graph: "postgres_graph"
  max_unwind_batch_size: 1000

state:
  backend: "file"               # or "none" / "falkordb" (file is implemented)
  file_path: "state.json"       # optional, default: state.json

mappings:
  - type: node
    name: customers
    source:
      table: "public.customers"
      where: "active = true"    # optional extra predicate
    mode: incremental            # or "full"
    delta:
      updated_at_column: "updated_at"
      deleted_flag_column: "is_deleted"
      deleted_flag_value: true
      initial_full_load: true    # set to false to skip the initial full backfill
    labels: ["Customer"]
    key:
      column: "id"
      property: "id"
    properties:
      email: { column: "email" }
      name:  { column: "name" }

  - type: node
    name: orders
    source:
      table: "public.orders"
    mode: incremental
    delta:
      updated_at_column: "updated_at"
    labels: ["Order"]
    key:
      column: "id"
      property: "id"
    properties:
      total:  { column: "total" }
      status: { column: "status" }

  - type: edge
    name: customer_orders
    source:
      table: "public.orders"
    mode: incremental
    delta:
      updated_at_column: "updated_at"
    relationship: "PLACED"
    direction: out               # from customer to order
    from:
      node_mapping: customers
      match_on:
        - column: "customer_id"
          property: "id"
    to:
      node_mapping: orders
      match_on:
        - column: "id"
          property: "id"
    properties:
      ordered_at: { column: "ordered_at" }
```

Key points:

- `source.table` + optional `source.where` are used to generate `SELECT` statements.
- If `delta.updated_at_column` is set and a watermark exists, the tool adds
  `AND <updated_at_column> > '<last_watermark>'` for incremental mappings.
- If `source.select` is used, the query is taken as-is or wrapped as a subquery when a watermark predicate is needed.
- `fetch_batch_size` + `delta` on a table mapping cause the tool to page results with `ORDER BY <updated_at_column> LIMIT <batch> OFFSET <n>`.

Watermarks per mapping are stored in the `state` backend, keyed by mapping name.

- If `delta.initial_full_load` is **unset** or `true` for an incremental mapping and no watermark exists, the first run does a full load.
- If `delta.initial_full_load: false` and no watermark exists, the first run *skips* that mapping and seeds its watermark to "now", so subsequent runs only see new changes.

## Example: Pagila sample – from rows to graph

This repository includes a concrete end-to-end example based on the Pagila sample schema:

- SQL to create and populate the database in `sample_data/pagila-schema.sql` and `sample_data/pagila-data.sql`.
- A loader config at `postgres-to-falkordb/pagila_to_falkordb.yaml` that maps Pagila tables to graph nodes and edges.

### Sample node mapping: Country

Config excerpt:

```yaml
mappings:
  - type: node
    name: countries
    source:
      table: "public.country"
    mode: incremental
    delta:
      updated_at_column: "last_update"
    labels: ["Country"]
    key:
      column: "country_id"
      property: "id"
    properties:
      name: { column: "country" }
```

Given a PostgreSQL row:

```sql
SELECT country_id, country, last_update
FROM public.country
WHERE country_id = 1;
-- 1, 'Afghanistan', '2006-02-15 09:44:00'
```

The loader:

1. Executes the query (internally wrapped with `row_to_json`) and receives a JSON object like:
   `{"country_id": 1, "country": "Afghanistan", "last_update": "2006-02-15T09:44:00Z"}`.
2. Builds a node key from `country_id` and maps properties according to the config.
3. Writes/merges the node in FalkorDB as a `Country` node with `id = 1` and `name = "Afghanistan"`.

### Sample edge mapping: City IN_COUNTRY Country

Config excerpt:

```yaml
  - type: edge
    name: city_in_country
    source:
      table: "public.city"
    mode: incremental
    delta:
      updated_at_column: "last_update"
    relationship: "IN_COUNTRY"
    direction: out   # (:City)-[:IN_COUNTRY]->(:Country)
    from:
      node_mapping: cities
      match_on:
        - column: "city_id"
          property: "id"
    to:
      node_mapping: countries
      match_on:
        - column: "country_id"
          property: "id"
    properties: {}
```

For a source row like:

```sql
SELECT city_id, city, country_id, last_update
FROM public.city
WHERE city_id = 1;
-- 1, 'A Corua (Orense)', 87, '2006-02-15 09:45:00'
```

The loader:

1. Turns it into a JSON row via `row_to_json`.
2. Looks up the `City` and `Country` nodes by their `id` properties.
3. Creates or merges the edge `(:City { id: 1 })-[:IN_COUNTRY]->(:Country { id: 87 })`.

Similar mappings in `pagila_to_falkordb.yaml` create `Customer`, `Film`, and `Category` nodes and edges such as `LIVES_IN`, `IN_CATEGORY`, and `RENTED` based on joins defined in the `source.select` clauses.

## Running the tool

From the crate directory:

```bash
cargo build --release
```

Example single run:

```bash
export POSTGRES_URL="postgres://user:pass@localhost:5432/source_db"
export FALKORDB_ENDPOINT="falkor://127.0.0.1:6379"

cargo run --release -- \
  --config postgres-to-falkordb/example.config.yaml
```

This will:

1. Load the config.
2. Connect to PostgreSQL and FalkorDB.
3. Ensure indexes exist on all node key properties (per node mapping).
4. Load existing watermarks from the state file (if present).
5. For each mapping:
   - Build a PostgreSQL query with optional `where` and watermark predicate.
   - Fetch rows (optionally in pages when `fetch_batch_size` is set).
   - Split rows into active vs. deleted (if `delta.deleted_flag_*` is configured).
   - Upsert active rows as nodes/edges.
   - Delete soft-deleted nodes/edges.
   - Update and persist the watermark.

### Daemon mode (continuous sync)

```bash
cargo run --release -- \
  --config postgres-to-falkordb/example.config.yaml \
  --daemon \
  --interval-secs 60
```

Behavior:

- Runs `run_once` every `interval-secs` seconds.
- On each run, processes all mappings in order.
- Maintains one-way sync by applying only new/updated rows since the last watermark.

## Operational notes

- **Idempotency**: node/edge writes use `MERGE` on configured keys, so re-running the same data is safe.
- **Incremental safety**: watermarks are only advanced after successful writes; if a run fails mid-way, the next run will retry from the last successful watermark.
- **Deletes**: any row where `deleted_flag_column == deleted_flag_value` is treated as deleted:
  - Node mappings: matching nodes are `DETACH DELETE`d.
  - Edge mappings: matching relationships are `DELETE`d.
- **Ordering**: mappings are processed in the order listed; for edges, the referenced node mappings must exist in the config.
- **Indexes**: for each node mapping, the tool attempts `CREATE INDEX ON :Label(...key property...)` once per `(labels, property)` pair, logging but not failing if the index already exists.
- **Logging**: uses `tracing` with log level controlled by `RUST_LOG`, e.g. `RUST_LOG=info`.

## Testing

From the crate directory:

```bash
cd postgres-to-falkordb
cargo test
```

Tests include:

- Config parsing tests (JSON/YAML, env var resolution for Postgres URL/password).
- SQL builder tests for combinations of `table`/`where`/watermark.
- Optional PostgreSQL connectivity smoke test, gated by `POSTGRES_URL`.
- Optional end-to-end file → FalkorDB test, gated by `FALKORDB_ENDPOINT`.

These tests are safe to run in environments without PostgreSQL or FalkorDB; the integration-style tests become no-ops when the relevant env vars are unset.
