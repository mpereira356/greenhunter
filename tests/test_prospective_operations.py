import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from app.services import prospective_operations


class ProspectiveOperationsTest(unittest.TestCase):
    def test_health_is_durable_and_next_run_is_three_am_sao_paulo(self):
        with tempfile.TemporaryDirectory() as temp, \
             patch.object(prospective_operations, "STATE_PATH", Path(temp) / "health.json"), \
             patch("app.services.prospective_operations.now_sp", return_value=datetime(2026, 9, 21, 21, 0)):
            prospective_operations.update_prospective_health(status="SUCCESS", last_official_date="2026-09-21")
            health = prospective_operations.prospective_health()
            self.assertEqual(health["status"], "SUCCESS")
            self.assertEqual(health["last_official_date"], "2026-09-21")
            self.assertEqual(health["next_collection_at"], "2026-09-22T03:00:00")
            self.assertEqual(health["timezone"], "America/Sao_Paulo")
