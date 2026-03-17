ALTER TABLE tool_metrics_snapshots
  ADD COLUMN config_id TEXT;

CREATE INDEX IF NOT EXISTS idx_tool_metrics_snapshots_tool_config_id_id
  ON tool_metrics_snapshots(tool_id, config_id, id DESC);

CREATE INDEX IF NOT EXISTS idx_tool_metrics_snapshots_config_id_id
  ON tool_metrics_snapshots(config_id, id DESC);
