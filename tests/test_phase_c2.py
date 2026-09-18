import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask

from app.extensions import db
from app.models import (HistoricalMatch, HistoricalMatchStat, MarketOddsSnapshot,
                        MarketPrediction, PredictionRun, TeamIdentity)
from app.services.backtesting.cluster_bootstrap import cluster_bootstrap
from app.services.calibration import (apply_calibration, shrink_score,
                                      train_temporal_calibration)
from app.services.predictions import append_prediction_candidates, start_prediction_run
from app.services.shadow_settlement import settle_one


def calibration_row(index, family="goals", target=None):
    return {
        "fixture_external_id": f"f-{index}", "kickoff_at": datetime(2026, 1, 1) + timedelta(hours=index),
        "market_family": family, "v2_score": 90 if index % 2 else 70,
        "legacy_score": 60, "target": index % 2 if target is None else target,
        "effective_sample_size": 6, "uncertainty_score": 40,
        "data_quality_v2": 70, "temporal_confidence": "MEDIUM",
    }


class PhaseC2MathTest(unittest.TestCase):
    def test_temporal_calibration_never_uses_evaluation_and_fallbacks(self):
        development = [calibration_row(index, "goals" if index < 450 else "cards") for index in range(500)]
        artifact = train_temporal_calibration(development)
        self.assertEqual(artifact["global_model"]["sample_count"], 500)
        self.assertLess(datetime.fromisoformat(artifact["internal_validation_start"]), datetime(2027, 1, 1))
        _, source = apply_calibration(artifact, calibration_row(900, "rare_family", 1))
        self.assertEqual(source, "GLOBAL_FALLBACK")
        self.assertEqual(artifact["family_decisions"]["cards"]["reason"], "MINIMUM_SAMPLE")

    def test_shrinkage_penalizes_small_uncertain_extremes(self):
        fragile = shrink_score(100, sample=3, uncertainty=80, data_quality=30, temporal_confidence="MEDIUM")
        strong = shrink_score(100, sample=30, uncertainty=10, data_quality=95, temporal_confidence="HIGH")
        self.assertLess(fragile, strong)
        self.assertLess(strong, 1)
        self.assertGreater(fragile, .5)

    def test_cluster_bootstrap_is_reproducible_and_keeps_fixture_clusters(self):
        rows = []
        for fixture in range(8):
            for market in range(3):
                rows.append({"fixture_external_id": str(fixture), "target": (fixture + market) % 2,
                             "legacy_score": 55 + fixture, "v2_score": 50 + fixture * 2})
        first = cluster_bootstrap(rows, "v2_score", repetitions=30, seed=42)
        second = cluster_bootstrap(rows, "v2_score", repetitions=30, seed=42)
        self.assertEqual(first, second)
        self.assertEqual(first["fixture_clusters"], 8)
        self.assertEqual(first["candidate_count"], 24)


class PhaseC2DatabaseTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.app = Flask(__name__)
        self.app.config.update(SQLALCHEMY_DATABASE_URI=f"sqlite:///{Path(self.temp.name) / 'c2.db'}",
                               SQLALCHEMY_TRACK_MODIFICATIONS=False)
        db.init_app(self.app); self.context = self.app.app_context(); self.context.push(); db.create_all()

    def tearDown(self):
        db.session.remove(); db.drop_all(); self.context.pop(); self.temp.cleanup()

    def test_prospective_prediction_preserves_legacy_and_captures_real_pre_kickoff_odd(self):
        run = start_prediction_run(None, {"target_date": "2030-01-01", "fixture_count": 1})
        candidate = {
            "fixtureId": "future-1", "marketType": "over15", "marketGroup": "goals",
            "scope": "total", "direction": "over", "line": 1.5,
            "kickoffAt": "2030-01-01T20:00:00", "competitionName": "Liga",
            "homeTeam": "A", "awayTeam": "B", "rawProbability": 80,
            "confidenceScore": 77, "status": "APPROVED", "individualOdd": 1.8,
            "bookmaker": "Book Test", "sourceStats": {},
        }
        append_prediction_candidates(run, {"candidates": [candidate], "fixture_snapshots": {}})
        legacy = MarketPrediction.query.filter_by(model_version="legacy_v1").one()
        shadow = MarketPrediction.query.filter_by(model_version="greenhunter_v2_shadow").one()
        self.assertEqual(legacy.confidence_score, 77)
        self.assertEqual(legacy.status, "APPROVED")
        self.assertEqual(shadow.settlement_status, "PENDING")
        odds = MarketOddsSnapshot.query.one()
        self.assertLess(odds.captured_at, odds.fixture_kickoff)
        self.assertEqual(odds.bookmaker, "Book Test")

    def test_idempotent_green_red_void_and_unresolved_settlement(self):
        run = PredictionRun(algorithm="x", model_version="greenhunter_v2_shadow", mode="shadow",
                            run_type="PROSPECTIVE_SHADOW", target_date="2026-01-02")
        a = TeamIdentity(canonical_name="A", source="betsapi", external_id="a")
        b = TeamIdentity(canonical_name="B", source="betsapi", external_id="b")
        db.session.add_all([run, a, b]); db.session.flush()
        match = HistoricalMatch(source="betsapi", external_id="m", kickoff_at=datetime(2026, 1, 2),
                                historical_date_available=True, league="Liga", home_team="A", away_team="B",
                                home_team_identity_id=a.id, away_team_identity_id=b.id,
                                home_score=2, away_score=1, status="finished")
        db.session.add(match); db.session.flush()
        base = dict(run_id=run.id, model_version="greenhunter_v2_shadow", fixture_id="m",
                    target_date="2026-01-02", market_group="goals", scope="total", direction="over",
                    snapshot_json="{}", status="SHADOW_STRONG")
        green = MarketPrediction(market_type="over15", line=1.5, **base)
        red = MarketPrediction(market_type="over25", line=3.5, **base)
        unresolved = MarketPrediction(market_type="corners", line=8.5, market_group="corners",
                                      **{key: value for key, value in base.items() if key != "market_group"})
        void_match = HistoricalMatch(source="betsapi", external_id="void", kickoff_at=datetime(2026, 1, 2),
                                     historical_date_available=True, league="Liga", home_team="A", away_team="B",
                                     home_team_identity_id=a.id, away_team_identity_id=b.id,
                                     status="cancelled")
        void = MarketPrediction(market_type="over15", line=1.5,
                                **{**base, "fixture_id": "void"})
        db.session.add_all([green, red, unresolved, void_match, void]); db.session.commit()
        stats = {match.id: {}}
        self.assertEqual(settle_one(green, match, stats), "GREEN")
        self.assertEqual(settle_one(red, match, stats), "RED")
        self.assertEqual(settle_one(unresolved, match, stats), "UNRESOLVED")
        self.assertEqual(settle_one(void, void_match, {}), "VOID")
        self.assertEqual(settle_one(green, match, stats), "GREEN")


if __name__ == "__main__":
    unittest.main()
