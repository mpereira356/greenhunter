import unittest
from types import SimpleNamespace

from app.services.worker import (
    _exact_score_matches,
    _exact_score_target,
    _is_confirmed_full_time,
)


class ExactScoreTest(unittest.TestCase):
    def setUp(self):
        self.rule = SimpleNamespace(name="Entrar placar correto - 2X1")

    def test_extracts_target_from_rule_name(self):
        self.assertEqual(_exact_score_target(self.rule), (2, 1))

    def test_matches_either_home_or_away_orientation(self):
        self.assertTrue(_exact_score_matches(self.rule, "2 x 1"))
        self.assertTrue(_exact_score_matches(self.rule, "1 x 2"))

    def test_rejects_other_final_scores(self):
        self.assertFalse(_exact_score_matches(self.rule, "2 x 2"))
        self.assertFalse(_exact_score_matches(self.rule, "0 x 2"))

    def test_requires_explicit_full_time_marker(self):
        self.assertTrue(_is_confirmed_full_time("FT"))
        self.assertTrue(_is_confirmed_full_time("Score After Full Time"))
        self.assertFalse(_is_confirmed_full_time("90'"))
        self.assertFalse(_is_confirmed_full_time("90+7'"))


if __name__ == "__main__":
    unittest.main()
