ALTER TABLE market_prediction ADD COLUMN feature_snapshot_json TEXT;
ALTER TABLE market_prediction ADD COLUMN v2_statistical_score FLOAT;
ALTER TABLE market_prediction ADD COLUMN uncertainty_score FLOAT;
ALTER TABLE market_prediction ADD COLUMN data_quality_v2 FLOAT;
ALTER TABLE market_prediction ADD COLUMN temporal_reliability VARCHAR(30);
ALTER TABLE market_prediction ADD COLUMN legacy_prediction_id INTEGER REFERENCES market_prediction(id);
CREATE INDEX IF NOT EXISTS ix_market_prediction_legacy_prediction_id ON market_prediction(legacy_prediction_id);

CREATE TABLE IF NOT EXISTS team_strength_snapshot (
  id INTEGER PRIMARY KEY,
  historical_match_id INTEGER NOT NULL REFERENCES historical_match(id) ON DELETE CASCADE,
  team_identity_id INTEGER NOT NULL REFERENCES team_identity(id),
  opponent_identity_id INTEGER NOT NULL REFERENCES team_identity(id),
  competition_key VARCHAR(180) NOT NULL,
  model_version VARCHAR(80) NOT NULL DEFAULT 'elo_v1',
  is_home BOOLEAN NOT NULL,
  kickoff_at DATETIME NOT NULL,
  pre_rating FLOAT NOT NULL,
  opponent_pre_rating FLOAT NOT NULL,
  expected_score FLOAT NOT NULL,
  actual_score FLOAT NOT NULL,
  post_rating FLOAT NOT NULL,
  created_at DATETIME NOT NULL,
  CONSTRAINT uix_team_strength_match_team_model UNIQUE(historical_match_id,team_identity_id,competition_key,model_version)
);
CREATE INDEX IF NOT EXISTS ix_team_strength_snapshot_historical_match_id ON team_strength_snapshot(historical_match_id);
CREATE INDEX IF NOT EXISTS ix_team_strength_snapshot_team_identity_id ON team_strength_snapshot(team_identity_id);
CREATE INDEX IF NOT EXISTS ix_team_strength_snapshot_competition_key ON team_strength_snapshot(competition_key);
CREATE INDEX IF NOT EXISTS ix_team_strength_snapshot_kickoff_at ON team_strength_snapshot(kickoff_at);
