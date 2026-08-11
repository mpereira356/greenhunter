import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.services.scraper import fetch_match_stats


class CurrentStatsTableTest(unittest.TestCase):
    def test_uses_current_table_instead_of_half_snapshot(self):
        html = """
        <html><head><title>Home vs Away - BetsAPI</title></head><body>
          <table class="table table-sm mb-0">
            <tr><td>Home</td><td>ESTAT.</td><td>Away</td></tr>
            <tr><td>3</td><td>Golos</td><td>3</td></tr>
            <tr><td>1</td><td>Cantos</td><td>3</td></tr>
            <tr><td>3</td><td>À Baliza</td><td>3</td></tr>
            <tr><td>33%</td><td>% de Posse</td><td>67%</td></tr>
            <tr><td>3</td><td>Substituições</td><td>3</td></tr>
          </table>
          <table class="table table-sm">
            <tr><td>Home</td><td>Half</td><td>Away</td></tr>
            <tr><td>2</td><td>Golos</td><td>0</td></tr>
            <tr><td>2</td><td>À Baliza</td><td>0</td></tr>
            <tr><td>31%</td><td>% de Posse</td><td>69%</td></tr>
            <tr><td>2</td><td>Substituições</td><td>2</td></tr>
          </table>
        </body></html>
        """
        response = SimpleNamespace(status_code=200, text=html)
        with patch("app.services.scraper.get_with_fallback", return_value=response):
            payload = fetch_match_stats(None, "https://pt.betsapi.com/soccer/r/1/home-vs-away")

        self.assertEqual(payload["score"], "3 x 3")
        self.assertEqual(payload["stats"]["On Target"], {"home": 3, "away": 3, "total": 6})
        self.assertEqual(payload["stats"]["Possession"], {"home": 33, "away": 67, "total": 100})
        self.assertEqual(payload["stats"]["Substitutions"], {"home": 3, "away": 3, "total": 6})

    def test_does_not_mix_historical_tables_into_current_stats(self):
        html = """
        <html><head><title>Home vs Away - BetsAPI</title></head><body>
          <table class="table table-sm mb-0">
            <tr><td>Home</td><td>Stats</td><td>Away</td></tr>
            <tr><td>1</td><td>Corners</td><td>2</td></tr>
          </table>
          <table class="table table-sm mb-0">
            <tr><td>Home</td><td>Average</td><td>Away</td></tr>
            <tr><td>8</td><td>Corners</td><td>9</td></tr>
          </table>
        </body></html>
        """
        response = SimpleNamespace(status_code=200, text=html)
        with patch("app.services.scraper.get_with_fallback", return_value=response):
            payload = fetch_match_stats(None, "https://pt.betsapi.com/soccer/r/1/home-vs-away")

        self.assertEqual(payload["stats"]["Corners"], {"home": 1, "away": 2, "total": 3})


if __name__ == "__main__":
    unittest.main()
