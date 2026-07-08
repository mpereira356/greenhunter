import unittest

from app.services.scraper import _archived_full_time_snapshot, _event_timeline_snapshot


class ArchivedMatchSnapshotTest(unittest.TestCase):
    def test_reconstructs_score_and_event_stats(self):
        events = [
            {"kind": "corner", "team": "Home", "text": "1' - Corner - Home"},
            {"kind": "corner", "team": "Away", "text": "2' - Corner - Away"},
            {"kind": "score_after_ft", "team": None, "text": "Score After Full Time - 2-1 - 0,0"},
        ]

        snapshot = _archived_full_time_snapshot(events, "Home", "Away")

        self.assertEqual(snapshot["score"], "2 x 1")
        self.assertEqual(snapshot["minute"], 90)
        self.assertEqual(snapshot["stats"]["Goals"], {"home": 2, "away": 1, "total": 3})
        self.assertEqual(snapshot["stats"]["Corners"], {"home": 1, "away": 1, "total": 2})

    def test_returns_none_without_full_time_marker(self):
        self.assertIsNone(_archived_full_time_snapshot([], "Home", "Away"))

    def test_reconstructs_timeline_without_full_time_marker(self):
        events = [
            {"kind": "goal", "team": "Home", "minute": 55, "text": "55' - Goal - Home"},
            {"kind": "corner", "team": "Away", "minute": 91, "text": "90+1' - Corner - Away"},
        ]

        snapshot = _event_timeline_snapshot(events, "Home", "Away")

        self.assertEqual(snapshot["score"], "1 x 0")
        self.assertEqual(snapshot["minute"], 91)
        self.assertEqual(snapshot["stats"]["Corners"]["away"], 1)


if __name__ == "__main__":
    unittest.main()
