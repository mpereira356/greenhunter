import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask
from sqlalchemy.exc import IntegrityError

from app.extensions import db
from app.models import (BacktestObservation, BacktestRun, HistoricalMatch,
                        TeamIdentity, TeamStrengthSnapshot)
from app.services.backtesting.leakage import TemporalAudit, audit_temporal_sources
from app.services.backtesting.metrics import binary_metrics, score_bucket
from app.services.backtesting.legacy_replay import evaluate_legacy
from app.services.backtesting.walk_forward import MARKETS, _ablation, logical_key, run_walk_forward, settle_market
from app.services.predictions import start_prediction_run
from app.services.strength import StrengthEngine


class PhaseCUnitTest(unittest.TestCase):
    def test_leakage_blocks_equal_future_and_unsafe_timezone(self):
        kickoff = datetime(2026, 9, 10, 12)
        for timestamp in (kickoff, kickoff + timedelta(seconds=1)):
            audit = audit_temporal_sources(kickoff, [{"kickoff_at": timestamp, "kickoff_timezone": "UTC"}])
            self.assertFalse(audit.allowed)
            self.assertEqual(audit.confidence, "UNSAFE")
        unsafe = audit_temporal_sources(kickoff, [{"kickoff_at": kickoff - timedelta(hours=2),
                                                   "kickoff_timezone": "SOURCE_LOCAL_UNKNOWN"}])
        self.assertFalse(unsafe.allowed)
        safe = audit_temporal_sources(kickoff, [{"kickoff_at": kickoff - timedelta(days=2),
                                                 "kickoff_timezone": "SOURCE_LOCAL_UNKNOWN"}])
        self.assertTrue(safe.allowed)
        self.assertEqual(safe.confidence, "MEDIUM")

    def test_settlement_and_buckets(self):
        allowed = TemporalAudit(True, "HIGH", datetime(2026, 1, 1))
        blocked = TemporalAudit(False, "UNSAFE", datetime(2026, 1, 2), "future")
        self.assertEqual(settle_market(3, 2.5, allowed), ("GREEN", 1))
        self.assertEqual(settle_market(2, 2.5, allowed), ("RED", 0))
        self.assertEqual(settle_market(None, 2.5, allowed), ("UNRESOLVED", None))
        self.assertEqual(settle_market(3, 2.5, blocked), ("LEAKAGE_BLOCKED", None))
        self.assertEqual(score_bucket(84.9), "80-89")
        metrics = binary_metrics([{"score": 90, "target": 1}, {"score": 70, "target": 0}], "score")
        self.assertEqual((metrics["green"], metrics["red"]), (1, 1))

    def test_ablation_variants_are_separate(self):
        features = {
            "team_relevant_frequency": .8,
            "series": {"home": {"recent_weighted_frequency": .7}},
            "strength": {"current_vs_historical_strength_delta": 80},
            "matchup": {"combined_expected_value": 5},
            "uncertainty_score": .4, "v2_statistical_score": 72,
        }
        spec = next(item for item in MARKETS if item.scope == "home" and item.family == "corners")
        values = _ablation(features, spec)
        self.assertEqual(set(values), {"A_base", "B_recency", "C_strength", "D_matchup",
                                       "E_dispersion_uncertainty", "F_recency_strength_matchup", "G_v2_complete"})
        self.assertNotEqual(values["A_base"], values["C_strength"])

    def test_legacy_replay_matches_frozen_javascript_fixture(self):
        replay = evaluate_legacy({
            "h2h": [2, 3, 1, 4], "home": [2, 2, 3, 1, 4, 3], "away": [2, 1, 2, 3, 3, 4],
        }, 1.5, "total")
        self.assertEqual(replay["raw_probability"], 82.4)
        self.assertEqual(replay["adjusted_probability"], 74.4)
        self.assertEqual(replay["confidence_score"], 74.2)
        self.assertEqual(replay["status"], "REJECTED")


class PhaseCDatabaseTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.app = Flask(__name__)
        self.app.config.update(SQLALCHEMY_DATABASE_URI=f"sqlite:///{Path(self.temp.name) / 'phase-c.db'}",
                               SQLALCHEMY_TRACK_MODIFICATIONS=False)
        db.init_app(self.app); self.context = self.app.app_context(); self.context.push(); db.create_all()

    def tearDown(self):
        db.session.remove(); db.drop_all(); self.context.pop(); self.temp.cleanup()

    def test_run_types_and_deduplication(self):
        prospective = start_prediction_run(None, {"target_date": "2026-09-18", "fixture_count": 0})
        self.assertEqual(prospective.run_type, "PROSPECTIVE_SHADOW")
        backtest = BacktestRun(run_type="BACKTEST", parameters_json="{}")
        a = TeamIdentity(canonical_name="A", source="betsapi", external_id="a")
        b = TeamIdentity(canonical_name="B", source="betsapi", external_id="b")
        db.session.add_all([backtest, a, b]); db.session.flush()
        match = HistoricalMatch(source="betsapi", external_id="fixture", kickoff_at=datetime(2026, 1, 1),
                                historical_date_available=True, league="Liga", home_team_identity_id=a.id,
                                away_team_identity_id=b.id, home_score=2, away_score=1)
        db.session.add(match); db.session.flush()
        kwargs = dict(backtest_run_id=backtest.id, logical_key=logical_key(match, MARKETS[0]),
                      historical_match_id=match.id, fixture_external_id=match.external_id,
                      kickoff_at=match.kickoff_at, prediction_time=match.kickoff_at - timedelta(minutes=1),
                      market_family="goals", market_type="over15", line=1.5, scope="total",
                      period="full_time", temporal_confidence="HIGH", settlement="GREEN",
                      feature_snapshot_json="{}", ablation_scores_json="{}", dataset_partition="evaluation")
        db.session.add_all([BacktestObservation(**kwargs), BacktestObservation(**kwargs)])
        with self.assertRaises(IntegrityError):
            db.session.commit()

    def test_strength_snapshot_is_strictly_pre_match(self):
        a = TeamIdentity(canonical_name="A", source="betsapi", external_id="a")
        b = TeamIdentity(canonical_name="B", source="betsapi", external_id="b")
        db.session.add_all([a, b]); db.session.flush()
        first = HistoricalMatch(
            source="betsapi", external_id="strength-1", kickoff_at=datetime(2026, 1, 1),
            historical_date_available=True, league="Liga", home_team="A", away_team="B",
            home_team_identity_id=a.id, away_team_identity_id=b.id, home_score=2, away_score=0,
        )
        second = HistoricalMatch(
            source="betsapi", external_id="strength-2", kickoff_at=datetime(2026, 1, 8),
            historical_date_available=True, league="Liga", home_team="B", away_team="A",
            home_team_identity_id=b.id, away_team_identity_id=a.id, home_score=1, away_score=1,
        )
        db.session.add_all([first, second]); db.session.commit()
        StrengthEngine().rebuild()
        first_a = TeamStrengthSnapshot.query.filter_by(
            historical_match_id=first.id, team_identity_id=a.id).one()
        second_a = TeamStrengthSnapshot.query.filter_by(
            historical_match_id=second.id, team_identity_id=a.id).one()
        self.assertEqual(first_a.pre_rating, 1500.0)
        self.assertAlmostEqual(second_a.pre_rating, first_a.post_rating)
        self.assertLess(first_a.kickoff_at, second_a.kickoff_at)

    def test_walk_forward_is_temporal_and_backtest_only(self):
        a = TeamIdentity(canonical_name="A", source="betsapi", external_id="a")
        b = TeamIdentity(canonical_name="B", source="betsapi", external_id="b")
        db.session.add_all([a, b]); db.session.flush()
        for index in range(4):
            db.session.add(HistoricalMatch(
                source="betsapi", external_id=str(index), kickoff_at=datetime(2026, 1, 1) + timedelta(days=index * 3),
                kickoff_timezone="SOURCE_LOCAL_UNKNOWN", historical_date_available=True, league="Liga",
                home_team="A", away_team="B", home_team_identity_id=a.id, away_team_identity_id=b.id,
                home_score=2 if index % 2 == 0 else 0, away_score=1, status="finished",
            ))
        db.session.commit()
        run, report = run_walk_forward(max_fixtures=4, sample_limit=3)
        self.assertEqual(run.run_type, "BACKTEST")
        self.assertEqual(run.status, "completed")
        self.assertGreater(run.candidate_count, 0)
        self.assertIn("evaluation", report)
        self.assertIn("model_comparison_by_market", report["evaluation"])
        self.assertIn("ablation_by_market", report["evaluation"])
        observations = BacktestObservation.query.filter_by(backtest_run_id=run.id).all()
        self.assertTrue(all(item.max_source_timestamp is None or item.max_source_timestamp < item.kickoff_at
                            for item in observations))
        self.assertTrue(all(not item.prospective_shadow for item in observations))


if __name__ == "__main__":
    unittest.main()
