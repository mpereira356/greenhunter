ALTER TABLE prediction_run ADD COLUMN run_type VARCHAR(30) NOT NULL DEFAULT 'PROSPECTIVE_SHADOW';
CREATE INDEX IF NOT EXISTS ix_prediction_run_run_type ON prediction_run(run_type);

CREATE TABLE IF NOT EXISTS backtest_run (
  id INTEGER PRIMARY KEY,
  run_type VARCHAR(30) NOT NULL DEFAULT 'BACKTEST',
  legacy_model_version VARCHAR(80) NOT NULL DEFAULT 'legacy_v1',
  shadow_model_version VARCHAR(80) NOT NULL DEFAULT 'greenhunter_v2_shadow',
  methodology_version VARCHAR(80) NOT NULL DEFAULT 'walk_forward_v1',
  development_end DATETIME,
  evaluation_start DATETIME,
  period_start DATETIME,
  period_end DATETIME,
  status VARCHAR(30) NOT NULL DEFAULT 'running',
  parameters_json TEXT,
  metrics_json TEXT,
  fixture_count INTEGER NOT NULL DEFAULT 0,
  candidate_count INTEGER NOT NULL DEFAULT 0,
  resolved_count INTEGER NOT NULL DEFAULT 0,
  unresolved_count INTEGER NOT NULL DEFAULT 0,
  leakage_blocked_count INTEGER NOT NULL DEFAULT 0,
  duration_ms INTEGER,
  created_at DATETIME NOT NULL,
  finished_at DATETIME
);
CREATE INDEX IF NOT EXISTS ix_backtest_run_run_type ON backtest_run(run_type);
CREATE INDEX IF NOT EXISTS ix_backtest_run_status ON backtest_run(status);

CREATE TABLE IF NOT EXISTS backtest_observation (
  id INTEGER PRIMARY KEY,
  backtest_run_id INTEGER NOT NULL REFERENCES backtest_run(id) ON DELETE CASCADE,
  logical_key VARCHAR(255) NOT NULL,
  historical_match_id INTEGER NOT NULL REFERENCES historical_match(id),
  fixture_external_id VARCHAR(80) NOT NULL,
  kickoff_at DATETIME NOT NULL,
  prediction_time DATETIME NOT NULL,
  league VARCHAR(180),
  market_family VARCHAR(80) NOT NULL,
  market_type VARCHAR(100) NOT NULL,
  line FLOAT NOT NULL,
  scope VARCHAR(20) NOT NULL,
  period VARCHAR(20) NOT NULL DEFAULT 'full_time',
  legacy_score FLOAT,
  legacy_adjusted_probability FLOAT,
  legacy_status VARCHAR(50),
  v2_score FLOAT,
  uncertainty_score FLOAT,
  data_quality_v2 FLOAT,
  raw_frequency FLOAT,
  recent_frequency FLOAT,
  effective_sample_size FLOAT,
  team_strength FLOAT,
  opponent_strength FLOAT,
  strength_delta FLOAT,
  production_avg FLOAT,
  concession_avg FLOAT,
  matchup_available BOOLEAN NOT NULL DEFAULT 0,
  strength_available BOOLEAN NOT NULL DEFAULT 0,
  temporal_confidence VARCHAR(20) NOT NULL,
  max_source_timestamp DATETIME,
  settlement VARCHAR(30) NOT NULL,
  actual_value FLOAT,
  target INTEGER,
  feature_snapshot_json TEXT NOT NULL,
  ablation_scores_json TEXT NOT NULL,
  leakage_reason VARCHAR(180),
  prospective_shadow BOOLEAN NOT NULL DEFAULT 0,
  dataset_partition VARCHAR(20) NOT NULL DEFAULT 'evaluation',
  created_at DATETIME NOT NULL,
  CONSTRAINT uix_backtest_run_logical_key UNIQUE(backtest_run_id,logical_key)
);
CREATE INDEX IF NOT EXISTS ix_backtest_observation_run_id ON backtest_observation(backtest_run_id);
CREATE INDEX IF NOT EXISTS ix_backtest_observation_historical_match_id ON backtest_observation(historical_match_id);
CREATE INDEX IF NOT EXISTS ix_backtest_observation_fixture_external_id ON backtest_observation(fixture_external_id);
CREATE INDEX IF NOT EXISTS ix_backtest_observation_kickoff_at ON backtest_observation(kickoff_at);
CREATE INDEX IF NOT EXISTS ix_backtest_observation_market_family ON backtest_observation(market_family);
CREATE INDEX IF NOT EXISTS ix_backtest_observation_settlement ON backtest_observation(settlement);
CREATE INDEX IF NOT EXISTS ix_backtest_observation_dataset_partition ON backtest_observation(dataset_partition);
CREATE INDEX IF NOT EXISTS ix_backtest_market_settlement ON backtest_observation(market_family,settlement);
