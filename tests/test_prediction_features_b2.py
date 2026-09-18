import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from flask import Flask

from app.extensions import db
from app.models import HistoricalMatch, TeamIdentity, TeamStrengthSnapshot
from app.services.matchup import build_matchup_features
from app.services.prediction_features import build_feature_snapshot, effective_sample_size, series_features
from app.services.strength import StrengthEngine, expected_score


class FeatureEngineUnitTest(unittest.TestCase):
    def test_recency_effective_sample_and_no_invented_age(self):
        rows = [
            {"value": 0, "historical_date_available": True, "kickoff_at": "2026-01-01T12:00"},
            {"value": 3, "historical_date_available": True, "kickoff_at": "2026-09-09T12:00"},
            {"value": 3, "historical_date_available": False, "kickoff_at": None},
        ]
        result = series_features(rows, 1.5, "over", datetime(2026, 9, 10, 12), 3)
        self.assertGreater(result["recent_weighted_frequency"], 0.5)
        self.assertEqual(result["temporal_count"], 2)
        self.assertEqual(result["raw_sample_size"], 3)
        self.assertEqual(result["max_source_timestamp"], "2026-09-09T12:00:00")
        self.assertAlmostEqual(effective_sample_size([1, 1, 1]), 3)
        self.assertLess(effective_sample_size([1, .01, .01]), 1.1)

    def test_future_is_excluded_and_h2h_is_secondary(self):
        groups = {
            "H2H": {"count": 2, "history_values": {"over15": [{"value": 3}, {"value": 3}]}},
            "Mandante": {"count": 4, "history_values": {"over15": [{"value": 0}] * 4}},
            "Visitante": {"count": 4, "history_values": {"over15": [{"value": 0}] * 4}},
        }
        candidate = {"marketType": "over15", "marketGroup": "goals_ft", "scope": "total",
                     "line": 1.5, "direction": "over", "kickoffAt": "2026-09-10T12:00"}
        feature = build_feature_snapshot(candidate, {"groups": groups})
        self.assertEqual(feature["h2h_frequency"], 1)
        self.assertLess(feature["v2_statistical_score"], 60)
        future = series_features([
            {"value": 3, "historical_date_available": True, "kickoff_at": "2026-09-10T12:00"},
            {"value": 3, "historical_date_available": True, "kickoff_at": "2026-09-11T12:00"},
        ], 1.5, "over", datetime(2026, 9, 10, 12), 2)
        self.assertEqual(future["raw_sample_size"], 0)
        self.assertEqual(future["future_rows_excluded"], 2)

    def test_same_mean_different_dispersion_changes_uncertainty(self):
        candidate = {"marketType": "corners", "marketGroup": "corners", "scope": "home",
                     "line": 4.5, "direction": "over", "kickoffAt": "2026-09-10T12:00"}
        def snapshot(values):
            return {"groups": {"H2H": {}, "Visitante": {}, "Mandante": {
                "count": 6, "history_values": {"team_corners_avg": [{"value": value} for value in values]}
            }}}
        stable = build_feature_snapshot(candidate, snapshot([5] * 6))
        volatile = build_feature_snapshot(candidate, snapshot([0, 10, 1, 9, 2, 8]))
        self.assertGreater(volatile["uncertainty_score"], stable["uncertainty_score"])

    def test_matchup_combines_production_concession_unknown_and_real_zero(self):
        candidate = {"marketGroup": "corners", "scope": "home"}
        snapshot = {"groups": {
            "Mandante": {"count": 3, "history_values": {"team_corners_avg": [{"value": 0}] * 3}},
            "Visitante": {"count": 3, "history_values": {"opponent_corners": [{"value": 4}] * 3}},
        }}
        result = build_matchup_features(candidate, snapshot)
        self.assertTrue(result["matchup_available"])
        self.assertEqual(result["production_avg"], 0)
        self.assertEqual(result["combined_expected_value"], 2)
        snapshot["groups"]["Visitante"]["history_values"] = {}
        result = build_matchup_features(candidate, snapshot)
        self.assertFalse(result["matchup_available"])
        self.assertIsNone(result["opponent_concession_avg"])


class StrengthEngineTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.app = Flask(__name__)
        self.app.config.update(SQLALCHEMY_DATABASE_URI=f"sqlite:///{Path(self.temp.name) / 'b2.db'}",
                               SQLALCHEMY_TRACK_MODIFICATIONS=False)
        db.init_app(self.app)
        self.context = self.app.app_context()
        self.context.push()
        db.create_all()
        self.a = TeamIdentity(canonical_name="A", source="betsapi", external_id="a")
        self.b = TeamIdentity(canonical_name="B", source="betsapi", external_id="b")
        db.session.add_all([self.a, self.b]); db.session.flush()

    def tearDown(self):
        db.session.remove(); db.drop_all(); self.context.pop(); self.temp.cleanup()

    def _match(self, external_id, kickoff, home_score, away_score):
        row = HistoricalMatch(source="betsapi", external_id=external_id, kickoff_at=kickoff,
                              historical_date_available=True, league="Liga", home_team="A", away_team="B",
                              home_team_identity_id=self.a.id, away_team_identity_id=self.b.id,
                              home_score=home_score, away_score=away_score, status="finished")
        db.session.add(row); return row

    def test_elo_uses_pre_match_rating_home_advantage_and_no_future_rewrite(self):
        first = self._match("1", datetime(2026, 1, 1), 1, 0)
        db.session.commit()
        engine = StrengthEngine(base_rating=1500, k_factor=24, home_advantage=65)
        engine.rebuild()
        snapshot = TeamStrengthSnapshot.query.filter_by(historical_match_id=first.id,
                                                        team_identity_id=self.a.id).one()
        self.assertEqual(snapshot.pre_rating, 1500)
        self.assertGreater(snapshot.expected_score, .5)
        first_post = snapshot.post_rating
        self._match("2", datetime(2026, 2, 1), 0, 1)
        db.session.commit(); engine.rebuild()
        snapshots = TeamStrengthSnapshot.query.filter_by(team_identity_id=self.a.id).order_by(
            TeamStrengthSnapshot.kickoff_at).all()
        self.assertAlmostEqual(snapshots[0].post_rating, first_post)
        self.assertAlmostEqual(snapshots[1].pre_rating, first_post)
        self.assertGreater(expected_score(1500, 1500, 65), .5)


if __name__ == "__main__":
    unittest.main()
