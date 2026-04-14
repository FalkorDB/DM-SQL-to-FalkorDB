ALTER TABLE runs
  ADD COLUMN backend TEXT NOT NULL DEFAULT 'local';

ALTER TABLE runs
  ADD COLUMN execution_ref_json TEXT;

CREATE INDEX IF NOT EXISTS idx_runs_backend_started_at
  ON runs(backend, started_at DESC);
