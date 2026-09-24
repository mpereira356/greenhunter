ALTER TABLE live_game_state ADD COLUMN first_half_provisional_json TEXT;
ALTER TABLE live_game_state ADD COLUMN first_half_provisional_minute INTEGER;
ALTER TABLE live_game_state ADD COLUMN first_half_provisional_time_text VARCHAR(40);
ALTER TABLE live_game_state ADD COLUMN first_half_snapshot_status VARCHAR(24);
ALTER TABLE live_game_state ADD COLUMN first_half_snapshot_confirmed_at DATETIME;
