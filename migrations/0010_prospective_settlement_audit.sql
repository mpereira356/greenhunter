ALTER TABLE market_prediction ADD COLUMN settlement_first_attempt_at DATETIME;
ALTER TABLE market_prediction ADD COLUMN settlement_last_attempt_at DATETIME;
ALTER TABLE market_prediction ADD COLUMN settlement_failure_reason VARCHAR(60);
ALTER TABLE market_prediction ADD COLUMN settlement_source VARCHAR(80);
ALTER TABLE market_prediction ADD COLUMN settlement_audit_json TEXT;
CREATE INDEX IF NOT EXISTS ix_market_prediction_settlement_failure_reason ON market_prediction(settlement_failure_reason);
