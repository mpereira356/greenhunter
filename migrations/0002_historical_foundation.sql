CREATE TABLE IF NOT EXISTS team_identity (
  id INTEGER PRIMARY KEY, canonical_name VARCHAR(180) NOT NULL, source VARCHAR(40), external_id VARCHAR(80), created_at DATETIME NOT NULL,
  CONSTRAINT uix_team_identity_source_external UNIQUE(source,external_id)
);
CREATE TABLE IF NOT EXISTS team_alias (
  id INTEGER PRIMARY KEY, team_identity_id INTEGER NOT NULL REFERENCES team_identity(id) ON DELETE CASCADE,
  source VARCHAR(40) NOT NULL, alias VARCHAR(180) NOT NULL, normalized_alias VARCHAR(180) NOT NULL,
  confirmed BOOLEAN NOT NULL DEFAULT 0, created_at DATETIME NOT NULL,
  CONSTRAINT uix_team_alias_source_name UNIQUE(source,normalized_alias)
);
CREATE INDEX IF NOT EXISTS ix_team_alias_team_identity_id ON team_alias(team_identity_id);

CREATE TABLE IF NOT EXISTS historical_match (
  id INTEGER PRIMARY KEY, source VARCHAR(40) NOT NULL, external_id VARCHAR(80) NOT NULL, source_url TEXT,
  kickoff_at DATETIME, kickoff_original VARCHAR(80), kickoff_timezone VARCHAR(80),
  historical_date_available BOOLEAN NOT NULL DEFAULT 0, league VARCHAR(180), season VARCHAR(80),
  home_team VARCHAR(180), away_team VARCHAR(180),
  home_team_identity_id INTEGER REFERENCES team_identity(id), away_team_identity_id INTEGER REFERENCES team_identity(id),
  home_score INTEGER, away_score INTEGER, status VARCHAR(30), collected_at DATETIME NOT NULL, updated_at DATETIME NOT NULL,
  CONSTRAINT uix_historical_match_source_external UNIQUE(source,external_id)
);
CREATE INDEX IF NOT EXISTS ix_historical_match_historical_date_available ON historical_match(historical_date_available);

CREATE TABLE IF NOT EXISTS historical_match_stat (
  id INTEGER PRIMARY KEY, historical_match_id INTEGER NOT NULL REFERENCES historical_match(id) ON DELETE CASCADE,
  period VARCHAR(20) NOT NULL DEFAULT 'full_time', side VARCHAR(20) NOT NULL DEFAULT 'total', stat_key VARCHAR(80) NOT NULL,
  value FLOAT, available BOOLEAN NOT NULL DEFAULT 0, source VARCHAR(40) NOT NULL DEFAULT 'betsapi', collected_at DATETIME NOT NULL,
  CONSTRAINT uix_historical_match_stat_observation UNIQUE(historical_match_id,period,side,stat_key,source)
);
CREATE INDEX IF NOT EXISTS ix_historical_match_stat_historical_match_id ON historical_match_stat(historical_match_id);
