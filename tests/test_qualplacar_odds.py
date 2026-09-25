import unittest
from unittest.mock import patch

from app.services import qualplacar_odds as service


class QualPlacarOddsTest(unittest.TestCase):
    def test_common_club_suffixes_do_not_hide_valid_odds(self):
        candidate = {
            "league": "CHAMPIONSHIP", "home": "Norwich City", "away": "Birmingham City",
            "time": "15:45", "home_odd": 1.83, "draw_odd": 3.66, "away_odd": 4.08,
        }
        match = {
            "league": "England Championship", "home_team": "norwich",
            "away_team": "birmingham", "time": "15:45",
        }

        self.assertEqual(service._find_odds(match, [candidate]), candidate)

    def test_united_suffix_does_not_hide_valid_odds(self):
        candidate = {
            "league": "CARABAO CUP", "home": "Chelsea", "away": "Leeds United",
            "time": "16:00", "home_odd": 1.56, "draw_odd": 4.24, "away_odd": 5.55,
        }
        match = {
            "league": "England EFL Cup", "home_team": "chelsea",
            "away_team": "leeds", "time": "16:00",
        }

        self.assertEqual(service._find_odds(match, [candidate]), candidate)

    def test_exact_teams_match_when_source_time_contains_date(self):
        candidate = {
            "id": "19609788", "league": "MAJOR LEAGUE SOCCER",
            "home": "Houston Dynamo", "away": "Real Salt Lake", "time": "21:30",
            "home_odd": 1.96, "draw_odd": 3.76, "away_odd": 3.69,
        }
        match = {
            "league": "USA MLS", "home_team": "Houston Dynamo",
            "away_team": "Real Salt Lake", "time": "09/09 21:30",
        }

        self.assertEqual(service._find_odds(match, [candidate]), candidate)

    def test_abbreviated_team_names_match_full_names_at_same_time(self):
        candidate = {
            "id": "19736839", "league": "PRIMEIRA LIGA",
            "home": "Estrela Amadora", "away": "Sporting Braga", "time": "16:15",
            "home_odd": 5.07, "draw_odd": 3.82, "away_odd": 1.68,
        }
        match = {
            "league": "Portugal Primeira Liga", "home_team": "estrela",
            "away_team": "braga", "time": "16:15",
        }

        self.assertEqual(service._find_odds(match, [candidate]), candidate)

    def test_generic_single_token_is_not_used_as_alias(self):
        self.assertFalse(service._single_token_alias("Sporting", "Sporting Braga"))
        self.assertFalse(service._single_token_alias("United", "Manchester United"))

    @patch("app.services.qualplacar_odds.get_qualplacar_odds")
    def test_attach_odds_matches_aliases_and_same_time(self, get_odds):
        get_odds.return_value = [{
            "league": "COPA LIBERTADORES", "home": "Estudiantes", "away": "Corinthians",
            "time": "21:30", "home_odd": 2.19, "draw_odd": 2.86, "away_odd": 4.01,
        }]
        matches = [{
            "league": "Copa Libertadores", "home_team": "estudiantes lp",
            "away_team": "corinthians", "time": "21:30",
        }]
        self.assertEqual(service.attach_qualplacar_odds(matches, "2026-09-09"), 1)
        self.assertEqual(matches[0]["home_win_odd"], 2.19)
        self.assertEqual(matches[0]["away_win_odd"], 4.01)

    @patch("app.services.qualplacar_odds.get_qualplacar_odds")
    def test_attach_odds_does_not_match_different_time(self, get_odds):
        get_odds.return_value = [{
            "league": "SERIE B", "home": "Fortaleza", "away": "Avaí",
            "time": "19:30", "home_odd": 1.58, "draw_odd": 3.56, "away_odd": 6.4,
        }]
        matches = [{
            "league": "Brazil Serie B", "home_team": "fortaleza", "away_team": "avai",
            "time": "21:30", "home_win_odd": 9.99,
        }]
        self.assertEqual(service.attach_qualplacar_odds(matches, "2026-09-09"), 0)
        self.assertNotIn("home_win_odd", matches[0])
        self.assertNotIn("away_win_odd", matches[0])

    @patch("app.services.qualplacar_odds.requests.post")
    def test_fetch_odds_keeps_games_with_other_markets(self, post):
        post.return_value.raise_for_status.return_value = None
        post.return_value.json.return_value = {
            "ok": True,
            "result": [{"liga": "Liga", "jogos": [
                {"id": 10, "mandante": "Casa", "visitante": "Fora", "data_descricao": "18:00",
                 "odd_mandante": 1.8, "odd_empate": 3.2, "odd_visitante": 4.1},
                {"id": 11, "mandante": "Sem", "visitante": "Odd", "data_descricao": "19:00",
                 "odd_mandante": None, "odd_empate": None, "odd_visitante": None},
            ]}],
        }
        self.assertEqual(service._fetch_odds("2026-09-09"), [{
            "id": "10",
            "league": "Liga", "home": "Casa", "away": "Fora", "time": "18:00",
            "home_odd": 1.8, "draw_odd": 3.2, "away_odd": 4.1,
        }, {
            "id": "11",
            "league": "Liga", "home": "Sem", "away": "Odd", "time": "19:00",
            "home_odd": None, "draw_odd": None, "away_odd": None,
        }])

    def test_selection_market_maps_supported_exact_markets(self):
        self.assertEqual(service._selection_market({"marketKey": "over15", "direction": "over", "selectedLine": 1.5}), (80, "over", 1.5))
        self.assertEqual(service._selection_market({"marketKey": "goals_1h", "direction": "under", "selectedLine": .5}), (28, "under", .5))
        self.assertEqual(service._selection_market({"marketKey": "btts"}), (14, "yes", None))
        self.assertEqual(service._selection_market({"marketKey": "corners_total", "selectedLine": 8.5}), (67, "over", 9))
        self.assertEqual(service._selection_market({"marketKey": "team_goals_home", "selectedLine": 1.5}), (20, "over", 1.5))
        self.assertEqual(service._selection_market({"marketKey": "team_goals_away", "direction": "under", "selectedLine": .5}), (21, "under", .5))
        self.assertEqual(service._selection_market({"marketKey": "cards_total", "selectedLine": 4.5}), (None, None, None))

    @patch("app.services.qualplacar_odds._get_detail")
    @patch("app.services.qualplacar_odds.get_qualplacar_odds")
    def test_bet365_selection_odds_uses_bookmaker_and_exact_line(self, get_odds, get_detail):
        get_odds.return_value = [{"id": "99", "league": "Liga", "home": "Casa", "away": "Fora", "time": "18:00", "home_odd": 1.8, "away_odd": 4.1}]
        get_detail.return_value = {"result": {"analise_pre_jogo": {"odds": {"data": [{
            "id": 80, "bookmaker": {"data": [
                {"name": "Outra", "odds": {"data": [{"label": "Over", "total": "1.5", "value": "1.99"}]}},
                {"name": "bet365", "odds": {"data": [{"label": "Over", "total": "1.5", "value": "1.14"}]}},
            ]}
        }]}}}}
        match = {"league": "Liga", "home_team": "Casa", "away_team": "Fora", "time": "18:00"}
        selections = [{"id": "leg-1", "marketKey": "over15", "direction": "over", "selectedLine": 1.5}]
        self.assertEqual(service.bet365_selection_odds(match, "2026-09-09", selections), {"leg-1": 1.14})

    @patch("app.services.qualplacar_odds._get_detail")
    @patch("app.services.qualplacar_odds.get_qualplacar_odds")
    def test_total_corner_odds_keep_75_and_85_lines_separate(self, get_odds, get_detail):
        get_odds.return_value = [{"id": "99", "league": "Liga", "home": "Casa", "away": "Fora", "time": "18:00"}]
        get_detail.return_value = {"result": {"analise_pre_jogo": {"odds": {"data": [{
            "id": 67,
            "bookmaker": {"data": [{
                "name": "bet365",
                "odds": {"data": [
                    {"label": "Over", "total": "8", "value": "1.55"},
                    {"label": "Over", "total": "9", "value": "1.95"},
                ]},
            }]},
        }]}}}}
        match = {"league": "Liga", "home_team": "Casa", "away_team": "Fora", "time": "18:00"}
        selections = [
            {"id": "corners-75", "marketKey": "corners_total", "direction": "over", "selectedLine": 7.5},
            {"id": "corners-85", "marketKey": "corners_total", "direction": "over", "selectedLine": 8.5},
        ]

        self.assertEqual(service.bet365_selection_odds(match, "2026-09-09", selections), {
            "corners-75": 1.55,
            "corners-85": 1.95,
        })

    @patch("app.services.qualplacar_odds._get_detail")
    @patch("app.services.qualplacar_odds.get_qualplacar_odds")
    def test_bookmaker_selection_odds_returns_all_comparable_houses(self, get_odds, get_detail):
        get_odds.return_value = [{"id": "99", "league": "Liga", "home": "Casa", "away": "Fora", "time": "18:00", "home_odd": 1.8, "away_odd": 4.1}]
        get_detail.return_value = {"result": {"analise_pre_jogo": {"odds": {"data": [{
            "id": 80, "bookmaker": {"data": [
                {"name": "bet365", "odds": {"data": [{"label": "Over", "total": "1.5", "value": "1.83"}]}},
                {"name": "Betano", "odds": {"data": [{"label": "Over", "total": "1.5", "value": "1.90"}]}},
                {"name": "Linha diferente", "odds": {"data": [{"label": "Over", "total": "2.5", "value": "9.99"}]}},
            ]}
        }]}}}}
        match = {"league": "Liga", "home_team": "Casa", "away_team": "Fora", "time": "18:00"}
        selections = [{"id": "leg-1", "marketKey": "over15", "direction": "over", "selectedLine": 1.5}]

        self.assertEqual(service.bookmaker_selection_odds(match, "2026-09-09", selections), {
            "leg-1": [{"name": "Betano", "odd": 1.9}, {"name": "bet365", "odd": 1.83}]
        })


if __name__ == "__main__":
    unittest.main()
