import json
import shutil
import tempfile
import unittest
from pathlib import Path

from flask import Flask

from app.extensions import db
from app.models import HistoricalMatch, HistoricalMatchStat, MarketPrediction, ModelVersion, PredictionFixtureSnapshot, PredictionRun, SavedTicketLeg
from app.services.historical_data import persist_historical_data
from app.services.migrations import run_schema_migrations
from app.services.predictions import append_prediction_candidates, finish_prediction_run, start_prediction_run


class PredictionFoundationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "test.db"
        self.app = Flask(__name__)
        self.app.config.update(
            SQLALCHEMY_DATABASE_URI=f"sqlite:///{self.database}",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
        )
        db.init_app(self.app)
        self.context = self.app.app_context()
        self.context.push()
        db.create_all()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        self.context.pop()
        self.temp.cleanup()

    def test_records_approved_rejected_model_and_immutable_snapshot(self):
        run = start_prediction_run(None, {
            "target_date": "2026-09-18", "fixture_count": 1,
            "parameters": {"generator_count": 3, "enabled_markets": ["over15"]},
        })
        fixture_snapshot = {
            "cache_schema_version": 2,
            "groups": {"H2H": {"history_values": {"over15": [{"value": 3}]}}},
        }
        candidates = [
            {
                "fixtureId": "100", "competitionName": "Liga", "homeTeam": "A", "awayTeam": "B",
                "marketType": "over15", "marketGroup": "goals_ft", "scope": "total", "line": 1.5,
                "status": "APPROVED", "rejectionReasons": [], "rawProbability": 100,
                "adjustedProbability": 88, "confidenceScore": 82,
                "sourceStats": {"h2h": {"samples": 6}, "home": None, "away": None},
            },
            {
                "fixtureId": "100", "competitionName": "Liga", "homeTeam": "A", "awayTeam": "B",
                "marketType": "over25", "marketGroup": "goals_ft", "scope": "total", "line": 2.5,
                "status": "REJECTED", "rejectionReasons": ["LOW_SAMPLE"], "rawProbability": 100,
                "adjustedProbability": 60, "confidenceScore": 61,
                "sourceStats": {"h2h": {"samples": 2}, "home": None, "away": None},
            },
            {
                "fixtureId": "100", "competitionName": "Liga", "homeTeam": "A", "awayTeam": "B",
                "marketType": "btts", "marketGroup": "btts", "scope": "total", "line": .5,
                "status": "ALTERNATIVE", "rejectionReasons": ["LOW_CONFIDENCE"], "rawProbability": 67,
                "adjustedProbability": 65, "confidenceScore": 66,
                "sourceStats": {"h2h": {"samples": 6}, "home": {"samples": 6}, "away": {"samples": 6}},
            },
        ]
        self.assertEqual(append_prediction_candidates(run, {
            "candidates": candidates, "fixture_snapshots": {"100": fixture_snapshot},
        }), 3)
        fixture_snapshot["groups"]["H2H"]["history_values"]["over15"][0]["value"] = 0
        finish_prediction_run(run)

        stored = MarketPrediction.query.order_by(MarketPrediction.id).all()
        legacy = [row for row in stored if row.model_version == "legacy_v1"]
        shadow = [row for row in stored if row.model_version == "greenhunter_v2_shadow"]
        self.assertEqual([row.status for row in legacy], ["APPROVED", "REJECTED_LOW_SAMPLE", "ALTERNATIVE"])
        self.assertEqual(legacy[1].rejection_reason, "LOW_SAMPLE")
        self.assertEqual(legacy[2].rejection_reason, "LOW_CONFIDENCE")
        self.assertEqual([row.legacy_prediction_id for row in shadow], [row.id for row in legacy])
        self.assertTrue(all(row.feature_snapshot_json for row in shadow))
        snapshot = PredictionFixtureSnapshot.query.one()
        self.assertEqual(json.loads(snapshot.snapshot_json)["groups"]["H2H"]["history_values"]["over15"][0]["value"], 3)
        self.assertEqual(run.candidate_count, 3)
        self.assertEqual((run.approved_count, run.rejected_count), (1, 2))
        self.assertEqual(run.status, "completed")
        self.assertIsNotNone(ModelVersion.query.filter_by(version="greenhunter_v2_shadow").first())

    def test_historical_zero_is_observation_and_missing_is_unknown(self):
        persist_historical_data({"h2h": [{
            "source": "betsapi", "external_id": "77", "url": "https://betsapi.com/r/77/a-vs-b",
            "historical_date_available": True, "kickoff_at": "2026-09-10T19:30",
            "kickoff_original": "2026/09/10 19:30", "kickoff_timezone": "SOURCE_LOCAL_UNKNOWN",
            "history_home_team": "A", "history_away_team": "B",
            "history_home_external_id": "team-a", "history_away_external_id": "team-b",
            "home": 0, "away": 0,
            "corners_home": 0, "corners_away": None,
        }], "home": [], "away": []})
        match = HistoricalMatch.query.one()
        self.assertTrue(match.historical_date_available)
        stats = {(row.stat_key, row.side): row for row in HistoricalMatchStat.query.all()}
        self.assertTrue(stats[("corners_home", "home")].available)
        self.assertEqual(stats[("corners_home", "home")].value, 0)
        self.assertFalse(stats[("corners_away", "away")].available)
        self.assertIsNone(stats[("corners_away", "away")].value)

    def test_migrations_are_idempotent_and_old_ticket_odd_is_nullable(self):
        migration_root = Path(self.temp.name) / "migrations"
        migration_root.mkdir()
        project_migrations = Path(__file__).resolve().parents[1] / "migrations"
        for source in project_migrations.glob("*.sql"):
            shutil.copy2(source, migration_root / source.name)
        first = run_schema_migrations(db.engine, migration_root)
        second = run_schema_migrations(db.engine, migration_root)
        self.assertTrue(first)
        self.assertEqual(second, [])
        columns = {row[1] for row in db.session.execute(db.text("PRAGMA table_info('saved_ticket_leg')"))}
        self.assertIn("individual_odd", columns)
        self.assertTrue(SavedTicketLeg.individual_odd.nullable)


if __name__ == "__main__":
    unittest.main()
