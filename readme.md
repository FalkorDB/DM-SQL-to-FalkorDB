# FalkorDB migration & sync CLI tools

This repository aggregates multiple command line loaders that migrate and/or incrementally sync data from common SQL systems into FalkorDB using declarative JSON/YAML mappings.
It includes a control plane web tool to initiate and track data migration runs.

## Prerequisites

- Rust toolchain (Cargo)
- Node.js + npm (optional; for the control plane UI)
- Network access to your source system (MySQL / Databricks / PostgreSQL / Snowflake / ClickHouse)
- A reachable FalkorDB endpoint (for example `falkor://127.0.0.1:6379`)

## Tools

### Databricks → FalkorDB

- Location: `Databricks-to-FalkorDB/`
- What it does: Loads and incrementally syncs tabular data from Databricks (Databricks SQL / warehouses) into FalkorDB based on a JSON/YAML mapping config.
- Documentation: [Databricks-to-FalkorDB/README.md](Databricks-to-FalkorDB/README.md)

Quick start (from the crate directory):

```bash
cd Databricks-to-FalkorDB/databricks-to-falkordb
cargo build --release

# Run once
cargo run --release -- --config path/to/config.yaml
```

Most configs reference environment variables for secrets (for example `$DATABRICKS_TOKEN`).

### Snowflake → FalkorDB

- Location: `Snowflake-to-FalkorDB/`
- What it does: Migrates and continuously syncs structured data from Snowflake into FalkorDB (supports incremental watermarks, optional purge modes, and daemon mode).
- Documentation: [Snowflake-to-FalkorDB/README.md](Snowflake-to-FalkorDB/README.md)

Quick start (from the crate directory):

```bash
cd Snowflake-to-FalkorDB
cargo build --release

# Single run
cargo run --release -- --config path/to/config.yaml

# Continuous sync
cargo run --release -- --config path/to/config.yaml --daemon --interval-secs 300
```

### PostgreSQL → FalkorDB

- Location: `PostgreSQL-to-FalkorDB/`
- What it does: Migrates and continuously syncs data from PostgreSQL into FalkorDB (supports full or incremental mode; optional daemon mode).
- Documentation: [PostgreSQL-to-FalkorDB/README.md](PostgreSQL-to-FalkorDB/README.md)

Quick start (from the crate directory):

```bash
cd PostgreSQL-to-FalkorDB/postgres-to-falkordb
cargo build --release

# Single run
cargo run --release -- --config example.config.yaml

# Continuous sync
cargo run --release -- --config example.config.yaml --daemon --interval-secs 60
```

### MySQL → FalkorDB

- Location: `MySQL-to-FalkorDB/`
- What it does: Migrates and continuously syncs data from MySQL into FalkorDB (supports full/incremental modes, optional purge modes, and daemon mode).
- Documentation: [MySQL-to-FalkorDB/readme.md](MySQL-to-FalkorDB/readme.md)
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
- The UI has an "API key" button that stores the key in browser localStorage.
- The log stream endpoint uses Server-Sent Events. Since `EventSource` can’t set headers, the UI falls back to `?api_key=<token>` for SSE when an API key is configured.
- Runtime data lives under `CONTROL_PLANE_DATA_DIR` (by default `control-plane/data/`), including a SQLite DB (`control-plane.sqlite`) and per-run artifacts/logs under `runs/<run_id>/`.
- Runs are executed locally on the machine running the control plane server (it spawns the underlying CLI tools).
- Metrics endpoints/ports are internal collector settings from each tool manifest and are not shown in the Metrics UI.

Selected API endpoints:

- `GET /api/health`
- `GET /api/tools`, `GET /api/tools/:tool_id`
- `GET /api/configs`, `POST /api/configs`
- `GET /api/configs/:config_id`, `PUT /api/configs/:config_id`
- `GET /api/configs/:config_id/state`, `POST /api/configs/:config_id/state/clear`
- `GET /api/runs`, `POST /api/runs`
- `GET /api/runs/:run_id`, `POST /api/runs/:run_id/stop`
- `GET /api/runs/:run_id/events` (SSE)
- `GET /api/runs/:run_id/logs` (persisted log lines for viewing past runs)
- `GET /api/metrics` (all tools metrics snapshot)
- `GET /api/metrics/:tool_id` (single tool metrics snapshot)

### Control plane metrics option (`tool.manifest.json`)

For tools that expose runtime metrics, configure both:

- `capabilities.supports_metrics: true`
- a `metrics` section in the manifest

The control plane uses this in two places:

1. **Run start**: when a run is started, the server parses the port from `metrics.endpoint` and adds `--metrics-port <port>` to the tool invocation.
2. **Metrics collection + persistence**: while a run is active, the server polls the raw endpoint, filters samples by `metricPrefix`, groups per-mapping samples by `mappingLabel` (default `mapping`), and stores snapshots in SQLite.

`/api/metrics` and `/api/metrics/:tool_id` now serve the latest persisted snapshot, so metrics remain available even after tool processes stop.
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

All current loaders expose the same metric shape with tool-specific prefixes.
Default scrape endpoints are configured per tool manifest for control-plane collection and are not displayed in the UI.

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

## Common concepts (applies to the Rust loaders)

- **Declarative mapping**: You define how source rows map to graph nodes and edges.
- **Idempotent upserts**: Writes use Cypher `UNWIND` + `MERGE` based on configured keys.
- **Incremental sync**: When configured with a watermark column (e.g. `updated_at`), the loader fetches only rows newer than the last successful run.
- **Soft deletes (optional)**: A configured deleted-flag column/value can be interpreted as deletes in FalkorDB.
- **State**: Watermarks are typically stored in a file-backed state JSON so runs can resume safely.

## FalkorDB connection

Each tool’s config describes the FalkorDB endpoint and graph name. Typical endpoints look like:

- `falkor://127.0.0.1:6379`

See each tool’s README for the exact configuration schema.
