import unittest
from types import SimpleNamespace

from app.services.worker import (
    _exact_score_matches,
    _exact_score_stage,
    _half_time_score_from_events,
    _exact_score_stage_reached,
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

    def test_ht_exact_score_waits_for_explicit_interval(self):
        rule = SimpleNamespace(outcome_green_stage="HT")
        self.assertFalse(_exact_score_stage_reached(rule, "45'", 45))
        self.assertTrue(_exact_score_stage_reached(rule, "HT", 45))
        self.assertFalse(_exact_score_stage_reached(rule, "2nd Half", 46))

    def test_ft_exact_score_still_waits_for_final_marker(self):
        rule = SimpleNamespace(outcome_green_stage="FT")
        self.assertFalse(_exact_score_stage_reached(rule, "90+7'", 97))
        self.assertTrue(_exact_score_stage_reached(rule, "FT", 97))

    def test_legacy_ht_stage_with_90_minute_deadline_is_full_time(self):
        rule = SimpleNamespace(outcome_green_stage="HT", outcome_green_minute=90, outcome_red_minute=90)
        self.assertEqual(_exact_score_stage(rule), "FT")
        self.assertFalse(_exact_score_stage_reached(rule, "HT", 45))
        self.assertTrue(_exact_score_stage_reached(rule, "FT", 90))

    def test_reconstructs_half_time_score_after_match_has_advanced(self):
        alert = SimpleNamespace(home_team="Home FC", away_team="Away FC")
        events = [
            {"kind": "goal", "minute": 12, "team": "Home FC", "time_text": "12'"},
            {"kind": "goal", "minute": 45, "team": "Home FC", "time_text": "45+2'"},
            {"kind": "goal", "minute": 47, "team": "Away FC", "time_text": "47'"},
        ]
        self.assertEqual(_half_time_score_from_events(alert, events), "2 x 0")


if __name__ == "__main__":
    unittest.main()
