CREATE TABLE IF NOT EXISTS model_version (
  id INTEGER PRIMARY KEY,
  version VARCHAR(80) NOT NULL UNIQUE,
  algorithm_family VARCHAR(80) NOT NULL,
  mode VARCHAR(30) NOT NULL DEFAULT 'production',
  status VARCHAR(30) NOT NULL DEFAULT 'baseline',
  config_json TEXT,
  created_at DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_model_version_version ON model_version(version);

CREATE TABLE IF NOT EXISTS prediction_run (
  id INTEGER PRIMARY KEY,
  user_id INTEGER REFERENCES user(id),
  algorithm VARCHAR(80) NOT NULL,
  model_version VARCHAR(80) NOT NULL,
  mode VARCHAR(30) NOT NULL DEFAULT 'production',
  target_date VARCHAR(10) NOT NULL,
  started_at DATETIME NOT NULL,
  finished_at DATETIME,
  duration_ms INTEGER,
  status VARCHAR(30) NOT NULL DEFAULT 'running',
  parameters_json TEXT,
  fixture_count INTEGER NOT NULL DEFAULT 0,
  candidate_count INTEGER NOT NULL DEFAULT 0,
  approved_count INTEGER NOT NULL DEFAULT 0,
  rejected_count INTEGER NOT NULL DEFAULT 0,
  fallback_used VARCHAR(120),
  errors_json TEXT,
  created_at DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_prediction_run_user_id ON prediction_run(user_id);
CREATE INDEX IF NOT EXISTS ix_prediction_run_model_version ON prediction_run(model_version);
CREATE INDEX IF NOT EXISTS ix_prediction_run_target_date ON prediction_run(target_date);
CREATE INDEX IF NOT EXISTS ix_prediction_run_status ON prediction_run(status);

CREATE TABLE IF NOT EXISTS market_prediction (
  id INTEGER PRIMARY KEY,
  run_id INTEGER NOT NULL REFERENCES prediction_run(id) ON DELETE CASCADE,
  model_version VARCHAR(80) NOT NULL,
  fixture_id VARCHAR(64) NOT NULL,
  target_date VARCHAR(10) NOT NULL,
  kickoff_at VARCHAR(40), competition VARCHAR(160), home_team VARCHAR(160), away_team VARCHAR(160),
  market_type VARCHAR(100) NOT NULL, market_group VARCHAR(100), scope VARCHAR(20), direction VARCHAR(10), line FLOAT,
  raw_frequency FLOAT, recent_frequency FLOAT, adjusted_frequency FLOAT,
  consistency_score FLOAT, data_quality_score FLOAT, context_score FLOAT, confidence_score FLOAT,
  sample_size INTEGER, individual_odd FLOAT,
  status VARCHAR(50) NOT NULL, rejection_reason VARCHAR(100), rejection_reasons_json TEXT,
  strengths_json TEXT, weaknesses_json TEXT, snapshot_json TEXT NOT NULL, created_at DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_market_prediction_run_id ON market_prediction(run_id);
CREATE INDEX IF NOT EXISTS ix_market_prediction_model_version ON market_prediction(model_version);
CREATE INDEX IF NOT EXISTS ix_market_prediction_fixture_id ON market_prediction(fixture_id);
CREATE INDEX IF NOT EXISTS ix_market_prediction_target_date ON market_prediction(target_date);
CREATE INDEX IF NOT EXISTS ix_market_prediction_status ON market_prediction(status);
CREATE INDEX IF NOT EXISTS ix_market_prediction_rejection_reason ON market_prediction(rejection_reason);
CREATE INDEX IF NOT EXISTS ix_market_prediction_model_date_status ON market_prediction(model_version,target_date,status);

INSERT OR IGNORE INTO model_version(version,algorithm_family,mode,status,config_json,created_at)
VALUES ('legacy_v1','statistical_candidate_engine','production','baseline','{}',CURRENT_TIMESTAMP);
INSERT OR IGNORE INTO model_version(version,algorithm_family,mode,status,config_json,created_at)
VALUES ('greenhunter_v2_shadow','statistical_shadow','shadow','prepared','{}',CURRENT_TIMESTAMP);
