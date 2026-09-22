import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from flask import Flask

from app.extensions import db
from app.models import LiveGameState, MarketPrediction, PredictionRun
from app.services.shadow_settlement import settle_one, settle_prospective_predictions


class ShadowSettlementAuditTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.app = Flask(__name__)
        self.app.config.update(SQLALCHEMY_DATABASE_URI=f"sqlite:///{Path(self.temp.name) / 'test.db'}",
                               SQLALCHEMY_TRACK_MODIFICATIONS=False)
        db.init_app(self.app); self.context = self.app.app_context(); self.context.push(); db.create_all()

    def tearDown(self):
        db.session.remove(); db.drop_all(); self.context.pop(); self.temp.cleanup()

    def prediction(self, market="over15", group="goals", scope="total", line=1.5):
        run = PredictionRun(algorithm="daily_shadow_observer", model_version="legacy_v1", mode="shadow",
                            run_type="PROSPECTIVE_SHADOW", target_date="2026-09-18", status="completed")
        db.session.add(run); db.session.flush()
        row = MarketPrediction(run_id=run.id, model_version="greenhunter_v2_shadow", fixture_id="42",
            target_date="2026-09-18", kickoff_at="2026-09-18T12:00:00", market_type=market,
            market_group=group, scope=scope, direction="over", line=line, status="SHADOW",
            settlement_status="UNRESOLVED", prospective_validity="VALID", snapshot_json="{}")
        db.session.add(row); db.session.flush(); return row

    def state(self, stats=None, events=None, score="2 x 1"):
        return LiveGameState(game_id="42", home_team="A", away_team="B", time_text="FT", minute=90,
            score=score, stats_json=json.dumps(stats or {}), events_json=json.dumps(events or []))

    def test_final_score_recovers_goals_without_historical_match(self):
        row, state = self.prediction(), self.state()
        self.assertEqual(settle_one(row, None, {}, datetime(2026, 9, 19), state), "GREEN")
        self.assertEqual(row.actual_value, 3); self.assertEqual(row.settlement_source, "live_game_state.final_score")
        self.assertIsNone(row.settlement_failure_reason); self.assertEqual(len(json.loads(row.settlement_audit_json)), 1)

    def test_each_market_is_independent_and_missing_reason_is_structured(self):
        row = self.prediction("fouls_home", "fouls_home", "home", 7.5)
        state = self.state({"Corners":{"home":5,"away":4,"total":9}})
        self.assertEqual(settle_one(row, None, {}, datetime(2026, 9, 19), state), "UNRESOLVED")
        self.assertEqual(row.settlement_failure_reason, "FOULS_MISSING")
        self.assertNotEqual(row.settlement_failure_reason, "CORNERS_MISSING")

    def test_first_half_uses_explicit_ht_score_and_corner_stat(self):
        events = [{"kind":"score_after_ht", "text":"Score After First Half - 1-1"}]
        goal = self.prediction("goal_ht", "goal_ht", "total", .5)
        self.assertEqual(settle_one(goal, None, {}, datetime(2026, 9, 19), self.state(events=events)), "GREEN")
        corner = self.prediction("corners_1h", "corners_1h", "total", 2.5)
        self.assertEqual(settle_one(corner, None, {}, datetime(2026, 9, 19),
                                   self.state({"Corners (Half)":{"home":2,"away":1,"total":3}})), "GREEN")

    def test_batch_uses_exact_fixture_id_and_preserves_failed_attempt_history(self):
        row = self.prediction("cards", "cards", "total", 4.5)
        row.settlement_failure_reason = "FIXTURE_RESULT_NOT_FOUND"
        db.session.add(self.state({"Yellow Card":{"home":2,"away":3,"total":5},
                                   "Red Card":{"home":0,"away":0,"total":0}})); db.session.commit()
        counts = settle_prospective_predictions("2026-09-18", datetime(2026, 9, 19))
        self.assertEqual(counts["GREEN"], 1); self.assertEqual(row.settlement_attempts, 1)
        attempt = json.loads(row.settlement_audit_json)[0]
        self.assertEqual(attempt["previous_reason"], "FIXTURE_RESULT_NOT_FOUND")


if __name__ == "__main__": unittest.main()
