import unittest

from app.services.match_analysis import _phase_metrics


class MatchAnalysisBothTeamsScoreTest(unittest.TestCase):
    def test_btts_uses_final_scores_without_period_details(self):
        items = [
            {"history_home_team": "Casa A", "history_away_team": "Fora A", "home": 2, "away": 1, "total": 3},
            {"history_home_team": "Casa B", "history_away_team": "Fora B", "home": 1, "away": 0, "total": 1},
            {"history_home_team": "Casa C", "history_away_team": "Fora C", "home": 3, "away": 2, "total": 5},
        ]

        phase = _phase_metrics(items)

        self.assertEqual(phase["samples"], 0)
        self.assertEqual([row["value"] for row in phase["history_values"]["btts"]], [1, 0, 1])
        self.assertEqual(phase["history_values"]["btts"][0]["home_value"], 2)
        self.assertEqual(len(phase["history_values"]["over15"]), 3)


if __name__ == "__main__":
    unittest.main()
