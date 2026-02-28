CREATE TABLE IF NOT EXISTS tools (
  id TEXT PRIMARY KEY,
  manifest_json TEXT NOT NULL,
  loaded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS configs (
  id TEXT PRIMARY KEY,
  tool_id TEXT NOT NULL,
  name TEXT NOT NULL,
  content TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(tool_id) REFERENCES tools(id)
);

CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  tool_id TEXT NOT NULL,
  config_id TEXT NOT NULL,
  mode TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  exit_code INTEGER,
  error TEXT,
  FOREIGN KEY(tool_id) REFERENCES tools(id),
  FOREIGN KEY(config_id) REFERENCES configs(id)
);
