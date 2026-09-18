ALTER TABLE market_prediction ADD COLUMN v2_calibrated_probability FLOAT;
ALTER TABLE market_prediction ADD COLUMN calibration_source VARCHAR(80);
ALTER TABLE market_prediction ADD COLUMN settlement_status VARCHAR(30) NOT NULL DEFAULT 'PENDING';
ALTER TABLE market_prediction ADD COLUMN actual_value FLOAT;
ALTER TABLE market_prediction ADD COLUMN settled_at DATETIME;
ALTER TABLE market_prediction ADD COLUMN selected_conservative BOOLEAN NOT NULL DEFAULT 0;
ALTER TABLE market_prediction ADD COLUMN selected_balanced BOOLEAN NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS ix_market_prediction_settlement_status ON market_prediction(settlement_status);

ALTER TABLE backtest_observation ADD COLUMN v2_calibrated_probability FLOAT;
ALTER TABLE backtest_observation ADD COLUMN calibration_source VARCHAR(80);

CREATE TABLE IF NOT EXISTS calibration_artifact (
  id INTEGER PRIMARY KEY,
  model_version VARCHAR(80) NOT NULL,
  method VARCHAR(40) NOT NULL,
  scope VARCHAR(80) NOT NULL DEFAULT 'GLOBAL',
  trained_through DATETIME NOT NULL,
  minimum_family_sample INTEGER NOT NULL,
  sample_count INTEGER NOT NULL,
  config_json TEXT NOT NULL,
  validation_metrics_json TEXT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_calibration_artifact_model_version ON calibration_artifact(model_version);
CREATE INDEX IF NOT EXISTS ix_calibration_artifact_is_active ON calibration_artifact(is_active);

CREATE TABLE IF NOT EXISTS market_odds_snapshot (
  id INTEGER PRIMARY KEY,
  market_prediction_id INTEGER REFERENCES market_prediction(id),
  fixture_id VARCHAR(64) NOT NULL,
  market_type VARCHAR(100) NOT NULL,
  line FLOAT,
  side VARCHAR(20),
  bookmaker VARCHAR(120) NOT NULL,
  odds_value FLOAT NOT NULL,
  captured_at DATETIME NOT NULL,
  fixture_kickoff DATETIME NOT NULL,
  created_at DATETIME NOT NULL,
  CONSTRAINT uix_market_odds_prediction_bookmaker_capture UNIQUE(market_prediction_id,bookmaker,captured_at)
);
CREATE INDEX IF NOT EXISTS ix_market_odds_snapshot_market_prediction_id ON market_odds_snapshot(market_prediction_id);
CREATE INDEX IF NOT EXISTS ix_market_odds_snapshot_fixture_id ON market_odds_snapshot(fixture_id);
CREATE INDEX IF NOT EXISTS ix_market_odds_snapshot_captured_at ON market_odds_snapshot(captured_at);
CREATE INDEX IF NOT EXISTS ix_market_odds_fixture_market ON market_odds_snapshot(fixture_id,market_type);
