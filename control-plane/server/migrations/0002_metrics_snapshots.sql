CREATE TABLE IF NOT EXISTS tool_metrics_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tool_id TEXT NOT NULL,
  run_id TEXT,
  fetched_at TEXT NOT NULL,
  view_json TEXT NOT NULL,
  FOREIGN KEY(tool_id) REFERENCES tools(id),
  FOREIGN KEY(run_id) REFERENCES runs(id)
);

CREATE INDEX IF NOT EXISTS idx_tool_metrics_snapshots_tool_id_id
  ON tool_metrics_snapshots(tool_id, id DESC);

CREATE INDEX IF NOT EXISTS idx_tool_metrics_snapshots_run_id_id
  ON tool_metrics_snapshots(run_id, id DESC);
