import json
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime
from pathlib import Path

from flask import Flask

from app.extensions import db
from app.models import (CalibrationArtifact, MarketOddsSnapshot, MarketPrediction,
                        PredictionRun)
from app.services.frozen_shadow import FROZEN_VERSION, ensure_frozen_version
from app.services.predictions import append_prediction_candidates, start_prediction_run
from app.services.prospective_reporting import daily_report, report_for_period
from app.services.prospective_shadow import collect_cached_prospective_shadow


class PhaseC3Test(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.app = Flask(__name__)
        self.app.config.update(SQLALCHEMY_DATABASE_URI=f"sqlite:///{Path(self.temp.name) / 'c3.db'}",
                               SQLALCHEMY_TRACK_MODIFICATIONS=False)
        db.init_app(self.app); self.context = self.app.app_context(); self.context.push(); db.create_all()
        config = {"selected_global_method": "platt", "global_model": {
            "method": "platt", "a": 1.0, "b": 0.0, "sample_count": 100, "uses_shrinkage": True,
        }, "family_models": {}}
        db.session.add(CalibrationArtifact(
            model_version="greenhunter_v2_shadow", method="platt", scope="GLOBAL",
            trained_through=datetime(2026, 8, 30), minimum_family_sample=300,
            sample_count=100, config_json=json.dumps(config), validation_metrics_json="{}", is_active=True,
        )); db.session.commit()

    def tearDown(self):
        db.session.remove(); db.drop_all(); self.context.pop(); self.temp.cleanup()

    def _candidate(self, kickoff, bookmaker=None):
        value = {"fixtureId": "fixture", "marketType": "over15", "marketGroup": "goals",
                 "scope": "total", "direction": "over", "line": 1.5, "kickoffAt": kickoff,
                 "competitionName": "Liga", "homeTeam": "A", "awayTeam": "B",
                 "rawProbability": 80, "confidenceScore": 77, "status": "APPROVED",
                 "individualOdd": 1.8, "sourceStats": {}}
        if bookmaker: value["bookmaker"] = bookmaker
        return value

    def test_frozen_configuration_has_stable_hash_and_version(self):
        first, payload = ensure_frozen_version()
        second, repeated = ensure_frozen_version()
        self.assertEqual(first.id, second.id)
        self.assertEqual(payload["configuration_sha256"], repeated["configuration_sha256"])
        self.assertEqual(first.version, FROZEN_VERSION)

    def test_late_prediction_is_rejected_and_post_kickoff_odd_is_not_saved(self):
        ensure_frozen_version()
        run = start_prediction_run(None, {"target_date": "2020-01-01", "fixture_count": 1})
        append_prediction_candidates(run, {"candidates": [self._candidate("2020-01-01T12:00:00", "Book")],
                                           "fixture_snapshots": {}})
        shadow = MarketPrediction.query.filter_by(model_version="greenhunter_v2_shadow").one()
        self.assertEqual(shadow.prospective_validity, "LATE_PREDICTION_REJECTED")
        self.assertFalse(shadow.official_pre_match_snapshot)
        self.assertEqual(MarketOddsSnapshot.query.count(), 0)

    def test_daily_run_marks_future_prediction_as_official(self):
        ensure_frozen_version()
        run = start_prediction_run(None, {"target_date": "2030-01-01", "fixture_count": 1})
        run.algorithm = "daily_shadow_observer"; db.session.commit()
        append_prediction_candidates(run, {"candidates": [self._candidate("2030-01-01T12:00:00")],
                                           "fixture_snapshots": {}})
        shadow = MarketPrediction.query.filter_by(model_version="greenhunter_v2_shadow").one()
        self.assertTrue(shadow.official_pre_match_snapshot)
        self.assertEqual(shadow.configuration_version, FROZEN_VERSION)
        self.assertEqual(MarketOddsSnapshot.query.count(), 0)  # bookmaker ausente

    def test_daily_and_accumulated_reports_include_pending_and_checkpoint(self):
        ensure_frozen_version()
        run = PredictionRun(algorithm="daily_shadow_observer", model_version="legacy_v1", mode="shadow",
                            run_type="PROSPECTIVE_SHADOW", target_date="2026-09-18", status="completed")
        db.session.add(run); db.session.flush()
        legacy = MarketPrediction(run_id=run.id, model_version="legacy_v1", fixture_id="x",
                                  target_date="2026-09-18", market_type="over15", confidence_score=70,
                                  status="APPROVED", snapshot_json="{}")
        db.session.add(legacy); db.session.flush()
        db.session.add(MarketPrediction(
            run_id=run.id, model_version="greenhunter_v2_shadow", configuration_version=FROZEN_VERSION,
            legacy_prediction_id=legacy.id, fixture_id="x", target_date="2026-09-18",
            market_type="over15", market_group="goals", v2_statistical_score=75,
            v2_calibrated_probability=70, status="SHADOW_MODERATE", settlement_status="PENDING",
            prospective_validity="VALID", official_pre_match_snapshot=True, snapshot_json="{}",
        )); db.session.commit()
        daily = daily_report("2026-09-18")
        self.assertEqual(daily["pending"], 1)
        accumulated = report_for_period(None, end_date="2026-09-18", include_bootstrap_if_checkpoint=False)
        self.assertFalse(accumulated["checkpoint"]["ready"])
        self.assertIn("drift", accumulated)

    def test_zero_candidate_completed_run_does_not_block_retry(self):
        ensure_frozen_version()
        run = PredictionRun(algorithm="daily_shadow_observer", model_version="legacy_v1", mode="shadow",
                            run_type="PROSPECTIVE_SHADOW", target_date="2030-01-01", status="completed",
                            fixture_count=0, candidate_count=0, alert_status="ZERO_FIXTURES")
        db.session.add(run); db.session.commit()
        with patch("app.services.prospective_shadow.get_matchday", return_value={"matches": []}), \
             patch("app.services.prospective_shadow.attach_qualplacar_odds", return_value=0):
            retried = collect_cached_prospective_shadow("2030-01-01")
        self.assertEqual(retried.id, run.id)
        self.assertEqual(retried.status, "failed")
        self.assertEqual(retried.alert_status, "NO_ELIGIBLE_FIXTURES")


if __name__ == "__main__":
    unittest.main()
