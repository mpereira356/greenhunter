import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.services.worker import _is_women_game, _rule_blocks_women


class WomenRuleScopeTest(unittest.TestCase):
    def test_detects_women_markers_in_league_or_team(self):
        self.assertTrue(_is_women_game("England Super League Women", "Chelsea", "Arsenal"))
        self.assertTrue(_is_women_game("Friendlies", "Chelsea Women", "Arsenal Women"))
        self.assertTrue(_is_women_game("México Liga Femenil", "Time A", "Time B"))
        self.assertFalse(_is_women_game("England Premier League", "Chelsea", "Arsenal"))

    @patch.dict("app.services.worker.os.environ", {"RULE_WOMEN_BLOCKED_USER_IDS": "1"})
    def test_block_is_scoped_to_requested_user(self):
        self.assertTrue(_rule_blocks_women(SimpleNamespace(user_id=1)))
        self.assertFalse(_rule_blocks_women(SimpleNamespace(user_id=4)))


if __name__ == "__main__":
    unittest.main()
