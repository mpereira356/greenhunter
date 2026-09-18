ALTER TABLE prediction_run ADD COLUMN operational_metrics_json TEXT;
ALTER TABLE prediction_run ADD COLUMN alert_status VARCHAR(40);

ALTER TABLE market_prediction ADD COLUMN configuration_version VARCHAR(120);
ALTER TABLE market_prediction ADD COLUMN prediction_timestamp DATETIME;
ALTER TABLE market_prediction ADD COLUMN official_pre_match_snapshot BOOLEAN NOT NULL DEFAULT 0;
ALTER TABLE market_prediction ADD COLUMN prospective_validity VARCHAR(40) NOT NULL DEFAULT 'VALID';
ALTER TABLE market_prediction ADD COLUMN settlement_attempts INTEGER NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS ix_market_prediction_configuration_version ON market_prediction(configuration_version);
CREATE INDEX IF NOT EXISTS ix_market_prediction_official_pre_match_snapshot ON market_prediction(official_pre_match_snapshot);
CREATE INDEX IF NOT EXISTS ix_market_prediction_prospective_validity ON market_prediction(prospective_validity);
