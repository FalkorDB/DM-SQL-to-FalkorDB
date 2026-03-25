# FalkorDB migration & sync CLI tools

This repository aggregates multiple command line loaders that migrate and/or incrementally sync data from common SQL systems into FalkorDB using declarative JSON/YAML mappings.
It includes a control plane web tool to initiate and track data migration (ETL/CDC) runs.

## Menu

- [Prerequisites](#prerequisites)
- [Tools](#tools)
  - [BigQuery → FalkorDB](#tool-bigquery)
  - [ClickHouse → FalkorDB](#tool-clickhouse)
  - [Databricks → FalkorDB](#tool-databricks)
  - [Spark → FalkorDB](#tool-spark)
  - [MariaDB → FalkorDB](#tool-mariadb)
  - [MySQL → FalkorDB](#tool-mysql)
  - [PostgreSQL → FalkorDB](#tool-postgresql)
  - [Snowflake → FalkorDB](#tool-snowflake)
  - [SQL Server → FalkorDB](#tool-sqlserver)
  - [Control plane (web UI + API)](#tool-control-plane)
- [Metrics exposed by each tool](#metrics-exposed-by-each-tool)
- [Common concepts](#common-concepts-applies-to-the-rust-loaders)
- [Scaffold schema + template generation behavior](#scaffold-schema--template-generation-behavior)
- [FalkorDB connection](#falkordb-connection)

## Prerequisites

- Rust toolchain (Cargo)
- Node.js + npm (optional; for the control plane UI)
- Network access to your source system (BigQuery / ClickHouse / Databricks / Spark / MariaDB / MySQL / PostgreSQL / Snowflake / SQL Server)
- A reachable FalkorDB endpoint (for example `falkor://127.0.0.1:6379`)

## Tools

Tool list view in the Control Plane

<img width="1155" height="632" alt="dm-sql-8-tools" src="https://github.com/user-attachments/assets/1e6557c1-447f-4deb-89ac-e9686b893060" />


Configuration File Editor with Graph Schema preview

<img width="1146" height="765" alt="DM-UI-graph-schema-canvas" src="https://github.com/user-attachments/assets/f80d7e8b-5983-4f45-902e-82489f190a1f" />



<a id="tool-bigquery"></a>
### BigQuery → FalkorDB

- Location: `BigQuery-to-FalkorDB/`
- What it does: Loads and incrementally syncs tabular data from BigQuery into FalkorDB using GoogleSQL (ANSI SQL mode) over BigQuery REST APIs, with optional purge modes and daemon mode.
- Scaffolding: supports `--introspect-schema` and `--generate-template` using BigQuery `INFORMATION_SCHEMA` metadata; emits fully-qualified source tables and inferred node/edge mappings where metadata is available.
- Documentation: [BigQuery-to-FalkorDB/README.md](BigQuery-to-FalkorDB/README.md)
- Scaffold behavior: see [Scaffold schema + template generation behavior](#scaffold-schema--template-generation-behavior)

Quick start (from the crate directory):

```bash
cd BigQuery-to-FalkorDB/bigquery-to-falkordb
cargo build --release

# Run once
cargo run --release -- --config ../bigquery_sample.yaml

# Continuous sync
cargo run --release -- --config ../bigquery_sample.yaml --daemon --interval-secs 60
```
---

<a id="tool-clickhouse"></a>

### ClickHouse → FalkorDB

- Location: `ClickHouse-to-FalkorDB/`
- What it does: Migrates and continuously syncs data from ClickHouse into FalkorDB (supports full/incremental modes, optional purge modes, and daemon mode).
- Scaffolding: supports schema introspection and starter template generation via `--introspect-schema` and `--generate-template`; infers node mappings from tables and conservative relationship mappings from id-like columns with review notes.
- Documentation: [ClickHouse-to-FalkorDB/readme.md](ClickHouse-to-FalkorDB/readme.md)
- Scaffold behavior: see [Scaffold schema + template generation behavior](#scaffold-schema--template-generation-behavior)

Quick start (from the crate directory):

```bash
cd ClickHouse-to-FalkorDB
cargo build --release

# Single run
cargo run --release -- --config clickhouse.incremental.yaml

# Continuous sync
cargo run --release -- --config clickhouse.incremental.yaml --daemon --interval-secs 60
```
---

<a id="tool-databricks"></a>

### Databricks → FalkorDB

- Location: `Databricks-to-FalkorDB/`
- What it does: Loads and incrementally syncs tabular data from Databricks (Databricks SQL / warehouses) into FalkorDB based on a JSON/YAML mapping config.
- Scaffolding: supports `--introspect-schema` and `--generate-template` using Databricks `information_schema`; emits catalog/schema-qualified source tables and inferred node/edge mappings where metadata is available.
- Documentation: [Databricks-to-FalkorDB/README.md](Databricks-to-FalkorDB/README.md)
- Scaffold behavior: see [Scaffold schema + template generation behavior](#scaffold-schema--template-generation-behavior)

Quick start (from the crate directory):

```bash
cd Databricks-to-FalkorDB/databricks-to-falkordb
cargo build --release

# Run once
cargo run --release -- --config path/to/config.yaml
```

Most configs reference environment variables for secrets (for example `$DATABRICKS_TOKEN`).
---
<a id="tool-spark"></a>

### Spark → FalkorDB

- Location: `Spark-to-FalkorDB/`
- What it does: Loads and incrementally syncs Spark SQL result sets into FalkorDB using a declarative mapping config.
- Source transport: Apache Livy interactive sessions (`spark.livy_url` + `spark.session_id`), supporting table-based and custom query sources.
- Parity controls: supports `source.query_count`, partition hints/ranges (`source.partition.*`), schema strategy controls (`preserve`/`json_stringify`/`drop_complex`/`flatten`), and edge endpoint match shorthand.
- Hardening: includes transient Livy retry/backoff controls and clearer classified error messages for auth/throttle/timeout/statement failures.
- Scaffolding: supports `--introspect-schema` and `--generate-template` via `SHOW TABLES` and `DESCRIBE TABLE` metadata.
- Documentation: [Spark-to-FalkorDB/README.md](Spark-to-FalkorDB/README.md)
- Scaffold behavior: see [Scaffold schema + template generation behavior](#scaffold-schema--template-generation-behavior)

Quick start (from the crate directory):

```bash
cd Spark-to-FalkorDB/spark-to-falkordb
cargo build --release

# Run once
cargo run --release -- --config path/to/config.yaml
```
---

<a id="tool-mariadb"></a>

### MariaDB → FalkorDB

- Location: `MariaDB-to-FalkorDB/`
- What it does: Migrates and continuously syncs data from MariaDB into FalkorDB (supports full/incremental modes, optional purge modes, and daemon mode).
- Scaffolding: supports schema extraction and template generation from metadata (`information_schema`) with PK/UK/FK-based inference, join-table heuristics, and schema-qualified source table output.
- Documentation: [MariaDB-to-FalkorDB/readme.md](MariaDB-to-FalkorDB/readme.md)
- Scaffold behavior: see [Scaffold schema + template generation behavior](#scaffold-schema--template-generation-behavior)
- End-to-end sample: `MariaDB-to-FalkorDB/sample_data/` + `MariaDB-to-FalkorDB/mariadb_sample_to_falkordb.yaml`

Quick start (from the crate directory):

```bash
cd MariaDB-to-FalkorDB
cargo build --release

# Single run
cargo run --release -- --config mariadb.incremental.yaml

# Continuous sync
cargo run --release -- --config mariadb.incremental.yaml --daemon --interval-secs 60
```
---

<a id="tool-mysql"></a>

### MySQL → FalkorDB

- Location: `MySQL-to-FalkorDB/`
- What it does: Migrates and continuously syncs data from MySQL into FalkorDB (supports full/incremental modes, optional purge modes, and daemon mode).
- Scaffolding: supports schema extraction and template generation from metadata (`information_schema`) with PK/UK/FK-based inference, join-table heuristics, and schema-qualified source table output.
- Documentation: [MySQL-to-FalkorDB/readme.md](MySQL-to-FalkorDB/readme.md)
- Scaffold behavior: see [Scaffold schema + template generation behavior](#scaffold-schema--template-generation-behavior)
- End-to-end sample: `MySQL-to-FalkorDB/sample_data/` + `MySQL-to-FalkorDB/mysql_sample_to_falkordb.yaml`

Quick start (from the crate directory):

```bash
cd MySQL-to-FalkorDB
cargo build --release

# Single run
cargo run --release -- --config mysql.incremental.yaml

# Continuous sync
cargo run --release -- --config mysql.incremental.yaml --daemon --interval-secs 60
```
---

<a id="tool-postgresql"></a>

### PostgreSQL → FalkorDB

- Location: `PostgreSQL-to-FalkorDB/`
- What it does: Migrates and continuously syncs data from PostgreSQL into FalkorDB (supports full or incremental mode; optional daemon mode).
- Scaffolding: supports schema introspection and template generation from PostgreSQL catalogs with qualified `schema.table` output and incremental delta detection including `last_update`.
- Documentation: [PostgreSQL-to-FalkorDB/README.md](PostgreSQL-to-FalkorDB/README.md)
- Scaffold behavior: see [Scaffold schema + template generation behavior](#scaffold-schema--template-generation-behavior)

Quick start (from the crate directory):

```bash
cd PostgreSQL-to-FalkorDB/postgres-to-falkordb
cargo build --release

# Single run
cargo run --release -- --config example.config.yaml

# Continuous sync
cargo run --release -- --config example.config.yaml --daemon --interval-secs 60
```
---

<a id="tool-snowflake"></a>

### Snowflake → FalkorDB

- Location: `Snowflake-to-FalkorDB/`
- What it does: Migrates and continuously syncs structured data from Snowflake into FalkorDB (supports incremental watermarks, optional purge modes, and daemon mode).
- Scaffolding: supports schema introspection and template generation from Snowflake metadata views, emitting fully-qualified source tables and best-effort FK-derived edge mappings with ambiguity notes.
- Documentation: [Snowflake-to-FalkorDB/README.md](Snowflake-to-FalkorDB/README.md)
- Scaffold behavior: see [Scaffold schema + template generation behavior](#scaffold-schema--template-generation-behavior)

Quick start (from the crate directory):

```bash
cd Snowflake-to-FalkorDB
cargo build --release

# Single run
cargo run --release -- --config path/to/config.yaml

# Continuous sync
cargo run --release -- --config path/to/config.yaml --daemon --interval-secs 300
```
---

<a id="tool-sqlserver"></a>

### SQL Server → FalkorDB

- Location: `SQLServer-to-FalkorDB/`
- What it does: Migrates and continuously syncs data from SQL Server into FalkorDB (supports full/incremental modes, optional purge modes, and daemon mode).
- Scaffolding: supports schema introspection and template generation from SQL Server system catalogs with PK/UK/FK inference, join-table detection, and qualified `schema.table` sources.
- Documentation: [SQLServer-to-FalkorDB/readme.md](SQLServer-to-FalkorDB/readme.md)
- Scaffold behavior: see [Scaffold schema + template generation behavior](#scaffold-schema--template-generation-behavior)
- End-to-end sample: `SQLServer-to-FalkorDB/sample_data/` + `SQLServer-to-FalkorDB/sqlserver_sample_to_falkordb.yaml`

Quick start (from the crate directory):

```bash
cd SQLServer-to-FalkorDB
cargo build --release

# Single run
cargo run --release -- --config sqlserver.incremental.yaml

# Continuous sync
cargo run --release -- --config sqlserver.incremental.yaml --daemon --interval-secs 60
```
---

<a id="tool-control-plane"></a>

### Control plane (web UI + API)

- Location: `control-plane/` (`control-plane/server` + `control-plane/ui`)
- What it does: Runs alongside the loaders and provides a web UI + REST API to:
  - Discover tools by scanning the repo for `tool.manifest.json`
  - Create/edit per-tool configs (YAML or JSON) with a syntax-highlighted editor
  - Start runs (one-shot or daemon) and stop running jobs
  - Auto-configure internal metrics collector ports for metrics-capable tools when launching runs
  - Stream logs live (SSE) and keep run history (SQLite)
  - View run log output after the fact (persisted per-run log file)
  - Inspect and clear file-backed incremental state (watermarks) per config
  - View per-tool runtime metrics (including per-mapping counters where supported), persisted in the control-plane database

Quick start (server):

```bash
cd control-plane/server

# Optional: require an API key for all /api routes (except /api/health)
export CONTROL_PLANE_API_KEY="..."

cargo run --release
# UI (if built) + API will be on http://localhost:3003
```

UI development (optional):

```bash
cd control-plane/ui
npm install
npm run dev
# Vite runs on http://localhost:5173 and proxies /api to http://localhost:3003
```

Configuration:

- `CONTROL_PLANE_BIND` (default: `0.0.0.0:3003`)
- `CONTROL_PLANE_REPO_ROOT` (optional; migration repo root to scan for tool manifests)
- `CONTROL_PLANE_DATA_DIR` (default: `control-plane/data/`)
- `CONTROL_PLANE_UI_DIST` (default: `control-plane/ui/dist/`; if missing, the API still works)
- `CONTROL_PLANE_API_KEY` (optional; if set, calls must include `Authorization: Bearer <key>`)

Notes:

- The config editor supports YAML/JSON syntax highlighting (Auto/YAML/JSON selector).
- The config editor provides 4 viewer tabs: **Config file**, **Extracted schema**, **Generated template**, and **Graph visualization**.
- **Graph visualization** is derived from the selected config file mappings (config-first), with scaffold-template fallback only when mappings are missing/unusable.
- The UI has an "API key" button that stores the key in browser localStorage.
- The log stream endpoint uses Server-Sent Events. Since `EventSource` can’t set headers, the UI falls back to `?api_key=<token>` for SSE when an API key is configured.
- Runtime data lives under `CONTROL_PLANE_DATA_DIR` (by default `control-plane/data/`), including a SQLite DB (`control-plane.sqlite`) and per-run artifacts/logs under `runs/<run_id>/`.
- Runs are executed locally on the machine running the control plane server (it spawns the underlying CLI tools).
- Metrics endpoints/ports are internal collector settings from each tool manifest and are not shown in the Metrics UI.

Selected API endpoints:

- `GET /api/health`
- `GET /api/tools`, `GET /api/tools/:tool_id`
- `POST /api/tools/:tool_id/scaffold-template` (generate mapping template from source schema for supported tools)
- `POST /api/tools/:tool_id/schema-graph-preview` (build canvas graph preview from config mappings; returns warnings and derivation source)
- `GET /api/configs`, `POST /api/configs`
- `GET /api/configs/:config_id`, `PUT /api/configs/:config_id`
- `GET /api/configs/:config_id/state`, `POST /api/configs/:config_id/state/clear`
- `GET /api/runs`, `POST /api/runs`
- `GET /api/runs/:run_id`, `POST /api/runs/:run_id/stop`
- `GET /api/runs/:run_id/events` (SSE)
- `GET /api/runs/:run_id/logs` (persisted log lines for viewing past runs)
- `GET /api/metrics` (all tools metrics snapshot; optional `?config_id=<uuid>` to scope to one ETL config)
- `GET /api/metrics/:tool_id` (single tool metrics snapshot; optional `?config_id=<uuid>`)

### Control plane metrics option (`tool.manifest.json`)

For tools that expose runtime metrics, configure both:

- `capabilities.supports_metrics: true`
- a `metrics` section in the manifest

The control plane uses this in two places:

1. **Run start**: when a run is started, the server parses the port from `metrics.endpoint` and adds `--metrics-port <port>` to the tool invocation.
2. **Metrics collection + persistence**: while a run is active, the server polls the raw endpoint, filters samples by `metricPrefix`, groups per-mapping samples by `mappingLabel` (default `mapping`), and stores snapshots in SQLite.

`/api/metrics` and `/api/metrics/:tool_id` now serve the latest persisted snapshot, so metrics remain available even after tool processes stop.
Both endpoints support optional `config_id` filtering, which is useful when multiple ETL configurations use the same tool (for example, different source tables mapped to different destination graphs).
When `config_id` is provided, the control plane returns the latest persisted snapshot for that specific config context instead of the latest snapshot across all configs of the tool.
The Metrics UI reads these persisted snapshots and does not display raw scrape endpoint/port details.

`metrics` fields:

- `endpoint`: HTTP endpoint to scrape (internal collector setting, not shown in UI; for example `http://127.0.0.1:9993/`)
- `format`: currently `prometheus_text`
- `metricPrefix`: prefix used to match this tool’s metric names
- `mappingLabel`: label key used for per-mapping metrics

Adding a new tool to the control plane:

- Add a `tool.manifest.json` anywhere under the repo root (the control plane scans to depth 4).
- The manifest declares how to run the tool and which optional features it supports (daemon/purge/etc.).

Minimal example:

```json
{
  "id": "my_tool",
  "displayName": "My Source → FalkorDB",
  "description": "...",
  "workingDir": "path/to/tool/dir",
  "executable": {
    "type": "cargo",
    "manifestPath": "path/to/Cargo.toml",
    "release": true
  },
  "capabilities": {
    "supports_daemon": false,
    "supports_purge_graph": false,
    "supports_purge_mapping": false,
    "supports_metrics": false
  },
  "config": {
    "fileExtensions": [".yaml", ".yml", ".json"],
    "examples": []
  },
  "metrics": {
    "endpoint": "http://127.0.0.1:9999/",
    "format": "prometheus_text",
    "metricPrefix": "my_tool_to_falkordb_",
    "mappingLabel": "mapping"
  }
}
```

## Metrics exposed by each tool

### BigQuery → FalkorDB (`bigquery_to_falkordb_`)

- `bigquery_to_falkordb_runs`
- `bigquery_to_falkordb_failed_runs`
- `bigquery_to_falkordb_rows_fetched`
- `bigquery_to_falkordb_rows_written`
- `bigquery_to_falkordb_rows_deleted`
- `bigquery_to_falkordb_mapping_runs{mapping="<name>"}`
- `bigquery_to_falkordb_mapping_failed_runs{mapping="<name>"}`
- `bigquery_to_falkordb_mapping_rows_fetched{mapping="<name>"}`
- `bigquery_to_falkordb_mapping_rows_written{mapping="<name>"}`
- `bigquery_to_falkordb_mapping_rows_deleted{mapping="<name>"}`

### ClickHouse → FalkorDB (`clickhouse_to_falkordb_`)

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

### Databricks → FalkorDB (`databricks_to_falkordb_`)

- `databricks_to_falkordb_runs`
- `databricks_to_falkordb_failed_runs`
- `databricks_to_falkordb_rows_fetched`
- `databricks_to_falkordb_rows_written`
- `databricks_to_falkordb_rows_deleted`
- `databricks_to_falkordb_mapping_runs{mapping="<name>"}`
- `databricks_to_falkordb_mapping_failed_runs{mapping="<name>"}`
- `databricks_to_falkordb_mapping_rows_fetched{mapping="<name>"}`
- `databricks_to_falkordb_mapping_rows_written{mapping="<name>"}`
- `databricks_to_falkordb_mapping_rows_deleted{mapping="<name>"}`
### Spark → FalkorDB (`spark_to_falkordb_`)

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

### MariaDB → FalkorDB (`mariadb_to_falkordb_`)

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

### MySQL → FalkorDB (`mysql_to_falkordb_`)

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

### PostgreSQL → FalkorDB (`postgres_to_falkordb_`)

- `postgres_to_falkordb_runs`
- `postgres_to_falkordb_failed_runs`
- `postgres_to_falkordb_rows_fetched`
- `postgres_to_falkordb_rows_written`
- `postgres_to_falkordb_rows_deleted`
- `postgres_to_falkordb_mapping_runs{mapping="<name>"}`
- `postgres_to_falkordb_mapping_failed_runs{mapping="<name>"}`
- `postgres_to_falkordb_mapping_rows_fetched{mapping="<name>"}`
- `postgres_to_falkordb_mapping_rows_written{mapping="<name>"}`
- `postgres_to_falkordb_mapping_rows_deleted{mapping="<name>"}`

### Snowflake → FalkorDB (`snowflake_to_falkordb_`)

- `snowflake_to_falkordb_runs`
- `snowflake_to_falkordb_failed_runs`
- `snowflake_to_falkordb_rows_fetched`
- `snowflake_to_falkordb_rows_written`
- `snowflake_to_falkordb_rows_deleted`
- `snowflake_to_falkordb_mapping_runs{mapping="<name>"}`
- `snowflake_to_falkordb_mapping_failed_runs{mapping="<name>"}`
- `snowflake_to_falkordb_mapping_rows_fetched{mapping="<name>"}`
- `snowflake_to_falkordb_mapping_rows_written{mapping="<name>"}`
- `snowflake_to_falkordb_mapping_rows_deleted{mapping="<name>"}`

### SQL Server → FalkorDB (`sqlserver_to_falkordb_`)

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

## Common concepts (applies to the Rust loaders)

- **Declarative mapping**: You define how source rows map to graph nodes and edges.
- **Idempotent upserts**: Writes use Cypher `UNWIND` + `MERGE` based on configured keys.
- **FalkorDB index creation (performance)**:
  - Loaders create required indexes before writes for node key properties and edge endpoint `match_on` properties.
  - You can also define explicit indexes via `falkordb.indexes` in connector configs.
  - Explicit and implicit indexes are deduplicated and applied for both initial and incremental runs.
  - Scaffold-generated templates may include suggested `falkordb.indexes` entries when source index metadata is available (best effort by source engine).
- **Incremental sync**: When configured with a watermark column (e.g. `updated_at`), the loader fetches only rows newer than the last successful run.
- **Soft deletes (optional)**: A configured deleted-flag column/value can be interpreted as deletes in FalkorDB.
- **State**: Watermarks are typically stored in a file-backed state JSON so runs can resume safely.

## Scaffold schema + template generation behavior

Most SQL-style loaders in this repository support scaffold mode:

- BigQuery
- ClickHouse
- Databricks
- Spark
- MariaDB
- MySQL
- PostgreSQL
- Snowflake
- SQL Server

Scaffold mode is exposed through CLI flags:

- `--introspect-schema`: introspects source metadata and prints a normalized schema summary.
- `--generate-template`: generates a starter YAML mapping template inferred from schema metadata.
- `--output <path>`: writes generated template to file (otherwise prints to stdout).

### How template inference works

- Default rule: each table becomes a node mapping.
- Foreign keys become edge mappings.
- Join tables (tables dominated by FK columns) may be inferred as edge mappings with optional edge properties.
- Key selection prefers:
  1) primary key,
  2) single-column unique key,
  3) fallback first/id-like column with review notes.
- Incremental `delta` is inferred only when common update/delete columns are found (for example `updated_at`, `last_update`, `is_deleted`).

### Important expectations

- Generated templates are **starter scaffolds**, not guaranteed production-ready configs.
- Scaffold relies on schema metadata and cannot reliably infer business-specific joins that require custom `source.select` SQL.
- You should always review and adjust:
  - relationship names,
  - key/property choices,
  - incremental/delete semantics,
  - custom edge sources that depend on multi-table joins.

### Control plane scaffold flow

In the control plane Config Editor:

- **Preview schema** calls scaffold introspection and shows extracted schema.
- **Generate template** calls scaffold template generation and shows generated YAML.
- **Use as config** copies generated template into the editable config tab.
- **Preview graph** renders graph topology in the Graph visualization tab from the selected config mappings (`node`/`edge`), including non-fatal warnings for partial mappings.

## FalkorDB connection

Each tool’s config describes the FalkorDB endpoint and graph name. Typical endpoints look like:

- `falkor://127.0.0.1:6379`

See each tool’s README for the exact configuration schema.
