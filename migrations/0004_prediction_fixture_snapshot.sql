CREATE TABLE IF NOT EXISTS prediction_fixture_snapshot (
  id INTEGER PRIMARY KEY,
  run_id INTEGER NOT NULL REFERENCES prediction_run(id) ON DELETE CASCADE,
  fixture_id VARCHAR(64) NOT NULL,
  snapshot_json TEXT NOT NULL,
  created_at DATETIME NOT NULL,
  CONSTRAINT uix_prediction_fixture_snapshot UNIQUE(run_id,fixture_id)
);
CREATE INDEX IF NOT EXISTS ix_prediction_fixture_snapshot_run_id ON prediction_fixture_snapshot(run_id);
ALTER TABLE market_prediction ADD COLUMN fixture_snapshot_id INTEGER REFERENCES prediction_fixture_snapshot(id);
CREATE INDEX IF NOT EXISTS ix_market_prediction_fixture_snapshot_id ON market_prediction(fixture_snapshot_id);
