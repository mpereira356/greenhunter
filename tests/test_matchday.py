import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

from app.services.matchday import _fetch_from_public, _is_excluded_youth_match, get_matchday, parse_matchday_html, save_matchday_summary_cache
from app.services.match_analysis import _h2h_team_goal_rows, _h2h_total_goal_rows, _phase_metrics
from app.services.scraper import _find_history_tables, _parse_history_table, _scheduled_time_from_history_page, _team_name_in_text
from app.services.tickets import _is_under_market, _leg_value
from app.main.routes import _is_upcoming_match_today
from bs4 import BeautifulSoup


class MatchdayParserTest(unittest.TestCase):
    def test_history_row_preserves_explicit_date_and_external_identity(self):
        table = BeautifulSoup("""
        <table><tr>
          <td class="league_n"><a>Brazil Serie A</a></td>
          <td>2026/09/10 19:30</td>
          <td><a href="/soccer/r/998877/palmeiras-vs-santos">Palmeiras v Santos</a></td>
          <td>2-1</td>
        </tr></table>
        """, "html.parser").table
        item = _parse_history_table(table)[0]
        self.assertEqual(item["external_id"], "998877")
        self.assertEqual(item["kickoff_at"], "2026-09-10T19:30")
        self.assertEqual(item["kickoff_timezone"], "SOURCE_LOCAL_UNKNOWN")
        self.assertTrue(item["historical_date_available"])
        self.assertEqual(item["league"], "Brazil Serie A")
        self.assertEqual(item["history_home_team"], "Palmeiras")
        self.assertEqual(item["history_away_team"], "Santos")

    def test_history_row_without_explicit_year_keeps_date_unknown(self):
        table = BeautifulSoup("""
        <table><tr><td>09/10 19:30</td>
        <td><a href="/soccer/r/1122/a-vs-b">A v B</a></td><td>0-0</td></tr></table>
        """, "html.parser").table
        item = _parse_history_table(table)[0]
        self.assertFalse(item["historical_date_available"])
        self.assertNotIn("kickoff_at", item)

    def test_history_row_preserves_explicit_team_ids_without_name_merging(self):
        table = BeautifulSoup("""
        <table><tr><td>2026/09/10 19:30</td>
        <td><a href="/soccer/t/10/palmeiras">Palmeiras</a> v
        <a href="/soccer/t/20/santos">Santos</a></td>
        <td><a href="/soccer/r/1133/palmeiras-vs-santos">2-0</a></td></tr></table>
        """, "html.parser").table
        item = _parse_history_table(table)[0]
        self.assertEqual(item["history_home_external_id"], "10")
        self.assertEqual(item["history_away_external_id"], "20")

    @patch("app.services.matchday.os.replace")
    @patch("app.services.matchday.os.makedirs")
    def test_does_not_cache_empty_matchday_summary(self, makedirs, replace):
        save_matchday_summary_cache(
            "2026-09-08", "13085248", 6,
            {"ok": True, "status": "empty", "groups": {}},
        )

        makedirs.assert_not_called()
        replace.assert_not_called()

    def test_h2h_team_goals_follow_team_when_home_and_away_swap(self):
        matches = [
            {"history_home_team": "Panathinaikos", "history_away_team": "PAOK", "home": 2, "away": 0},
            {"history_home_team": "PAOK", "history_away_team": "Panathinaikos FC", "home": 1, "away": 3},
        ]
        self.assertEqual([row["value"] for row in _h2h_team_goal_rows(matches, "Panathinaikos")], [2, 3])
        self.assertEqual([row["value"] for row in _h2h_total_goal_rows(matches)], [2, 4])

    def test_today_filter_keeps_only_upcoming_live_or_unknown_time(self):
        current = datetime(2026, 9, 4, 14, 30)
        self.assertFalse(_is_upcoming_match_today({"time": "14:29"}, current))
        self.assertTrue(_is_upcoming_match_today({"time": "14:30"}, current))
        self.assertTrue(_is_upcoming_match_today({"time": "18:00"}, current))
        self.assertTrue(_is_upcoming_match_today({"time": "-"}, current))
        self.assertTrue(_is_upcoming_match_today({"time": "10:00", "is_live": True}, current))

    def test_resolves_team_goal_market_for_each_side(self):
        state = SimpleNamespace(score="2 x 1")
        home_leg = SimpleNamespace(market_key="team_goals_home")
        away_leg = SimpleNamespace(market_key="team_goals_away")
        self.assertEqual(_leg_value(home_leg, state, {}), 2)
        self.assertEqual(_leg_value(away_leg, state, {}), 1)

    def test_resolves_under_markets_and_direction(self):
        state = SimpleNamespace(score="1 x 0")
        self.assertEqual(_leg_value(SimpleNamespace(market_key="under25"), state, {}), 1)
        self.assertEqual(_leg_value(SimpleNamespace(market_key="team_goals_under_home"), state, {}), 1)
        self.assertTrue(_is_under_market(SimpleNamespace(market_key="corners_under_total")))
        self.assertFalse(_is_under_market(SimpleNamespace(market_key="corners_total")))

    @patch.dict("app.services.matchday.os.environ", {"MATCHDAY_MAX_PAGES": "3"})
    @patch("app.services.matchday.BASE_URLS", ["https://example.test"])
    @patch("app.services.matchday.parse_matchday_html")
    @patch("app.services.matchday.get_with_fallback")
    @patch("app.services.matchday.make_session")
    def test_public_fetch_continues_after_filtered_empty_page(
        self, make_session, get_with_fallback, parse_html
    ):
        get_with_fallback.return_value = SimpleNamespace(status_code=200, text="html")
        first = {"game_id": "1", "day": "2026-09-03"}
        third = {"game_id": "3", "day": "2026-09-03"}
        parse_html.side_effect = [[first], [], [third]]

        matches = _fetch_from_public("2026-09-03")

        self.assertEqual(matches, [first, third])
        self.assertEqual(get_with_fallback.call_count, 3)

    @patch("app.services.matchday._save_cache")
    @patch("app.services.matchday.time.sleep")
    @patch("app.services.matchday._fetch_from_public")
    @patch("app.services.matchday._load_cache", return_value=None)
    def test_retries_empty_first_fetch_before_rendering_new_day(
        self, _load_cache, fetch, sleep, save_cache
    ):
        match = {
            "game_id": "123",
            "url": "https://betsapi.com/r/123",
            "time": "19:30",
            "day": "2026-09-03",
            "league": "Brazil Serie A",
            "home_team": "Palmeiras",
            "away_team": "Santos",
        }
        fetch.side_effect = [[], [match]]

        payload = get_matchday("2026-09-03")

        self.assertEqual(payload["matches"], [match])
        self.assertEqual(fetch.call_count, 2)
        sleep.assert_called_once_with(0.5)
        save_cache.assert_called_once()

    @patch("app.services.matchday._save_cache")
    @patch("app.services.matchday.time.sleep")
    @patch("app.services.matchday._fetch_from_public")
    @patch("app.services.matchday._load_cache")
    def test_does_not_replace_full_cache_with_partial_provider_response(
        self, load_cache, fetch, sleep, save_cache
    ):
        cached_matches = [{"game_id": str(index)} for index in range(20)]
        load_cache.side_effect = [
            {"cache_version": 7, "day": "2026-09-03", "matches": cached_matches},
            None,
        ]
        fetch.side_effect = [[{"game_id": "partial"}], [{"game_id": "partial"}]]

        payload = get_matchday("2026-09-03", force_refresh=True)

        self.assertEqual(payload["matches"], cached_matches)
        self.assertTrue(payload["stale"])
        save_cache.assert_not_called()

    def test_extracts_authoritative_clock_from_event_history_page(self):
        soup = BeautifulSoup("<main>Apia L Tigers vs Sydney United 58 2026/07/25 08:30</main>", "html.parser")
        self.assertEqual(_scheduled_time_from_history_page(soup), "08:30")

    def test_does_not_use_a_clock_from_a_history_table(self):
        soup = BeautifulSoup("<h1>United SC vs Police</h1><table><tr><td>2026/08/01 09:30</td></tr></table>", "html.parser")
        self.assertIsNone(_scheduled_time_from_history_page(soup))

    def test_parses_fixture_rows_and_skips_esoccer(self):
        html = """
        <table>
          <tr id="r_123">
            <td class="league_n"><a>Brazil Serie A</a></td>
            <td><span class="race-time">08/11 19:30</span></td>
            <td><a href="/r/123/palmeiras-vs-santos">Palmeiras v Santos</a></td>
          </tr>
          <tr id="r_999">
            <td class="league_n"><a>Esoccer Battle</a></td>
            <td><span class="race-time">20:00</span></td>
            <td><a href="/r/999/a-vs-b">A v B</a></td>
          </tr>
        </table>
        """

        matches = parse_matchday_html(html)

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["game_id"], "123")
        self.assertEqual(matches[0]["home_team"], "Palmeiras")
        self.assertEqual(matches[0]["away_team"], "Santos")
        self.assertEqual(matches[0]["time"], "16:30")
        self.assertEqual(matches[0]["day"], "2026-08-11")
        self.assertEqual(matches[0]["league"], "Brazil Serie A")

    def test_filters_rows_by_page_date_metadata(self):
        html = """
        <tr><td class="league_n"><a>Brazil</a></td><td>08/12 15:00</td>
        <td><a href="/r/456/a-vs-b">A v B</a></td></tr>
        """
        matches = parse_matchday_html(html, reference_day="2026-08-12")
        self.assertEqual(matches[0]["day"], "2026-08-12")

    def test_converts_public_fixture_utc_clock_to_sao_paulo(self):
        html = """
        <tr><td class="league_n"><a>Australia Cup</a></td><td>08/12 09:30</td>
        <td><a href="/soccer/r/12345277/apia-vs-sydney">Apia v Sydney</a></td></tr>
        """
        match = parse_matchday_html(html, reference_day="2026-08-12")[0]
        self.assertEqual(match["time"], "06:30")
        self.assertEqual(match["day"], "2026-08-12")

    def test_keeps_full_betsapi_event_clock_without_double_conversion(self):
        html = """
        <tr><td class="league_n"><a>Australia Cup</a></td><td>2026/08/12 06:30</td>
        <td><a href="/soccer/r/12345277/apia-vs-sydney">Apia v Sydney</a></td></tr>
        """
        match = parse_matchday_html(html, reference_day="2026-08-12")[0]
        self.assertEqual(match["time"], "06:30")
        self.assertEqual(match["day"], "2026-08-12")

    def test_utc_conversion_can_move_fixture_to_previous_local_day(self):
        html = """
        <tr><td class="league_n"><a>League</a></td><td>08/12 01:30</td>
        <td><a href="/soccer/r/7/a-vs-b">A v B</a></td></tr>
        """
        match = parse_matchday_html(html, reference_day="2026-08-11")[0]
        self.assertEqual(match["time"], "22:30")
        self.assertEqual(match["day"], "2026-08-11")

    def test_calculates_card_and_optional_throw_in_markets(self):
        metrics = _phase_metrics(
            [
                {
                    "goals_ht": 1,
                    "goals_2h": 0,
                    "corners_ht": 2,
                    "corners_2h": 4,
                    "yellow_cards_ht": 1,
                    "yellow_cards_2h": 3,
                    "red_cards_ht": 0,
                    "red_cards_2h": 0,
                    "throw_ins_total": 36,
                    "offsides_total": 4,
                },
                {
                    "goals_ht": 0,
                    "goals_2h": 1,
                    "corners_ht": 1,
                    "corners_2h": 3,
                    "yellow_cards_ht": 0,
                    "yellow_cards_2h": 2,
                    "red_cards_ht": 0,
                    "red_cards_2h": 0,
                    "throw_ins_total": None,
                    "offsides_total": 2,
                },
            ]
        )
        self.assertEqual(metrics["avg_cards_total"], 3.0)
        self.assertEqual(metrics["card_lines"]["2.5"], 50)
        self.assertEqual(metrics["throw_ins_samples"], 1)
        self.assertEqual(metrics["throw_in_lines"]["34.5"], 100)
        self.assertEqual(metrics["offsides_samples"], 2)
        self.assertEqual(metrics["avg_offsides"], 3.0)

    def test_keeps_halftime_sample_without_inventing_corners_or_cards(self):
        metrics = _phase_metrics([
            {"goals_ht": 1, "goals_2h": 1, "corners_ht": None, "corners_2h": None,
             "yellow_cards_ht": None, "yellow_cards_2h": None},
            {"goals_ht": 0, "goals_2h": 2, "corners_ht": 2, "corners_2h": 3,
             "yellow_cards_ht": 1, "yellow_cards_2h": 2, "red_cards_ht": 0, "red_cards_2h": 0},
        ])
        self.assertEqual(metrics["samples"], 2)
        self.assertEqual(metrics["corners_samples"], 1)
        self.assertEqual(metrics["cards_samples"], 1)
        self.assertEqual(metrics["avg_corners_1h"], 2.0)
        self.assertEqual(metrics["avg_cards_total"], 3.0)

    def test_period_history_keeps_team_breakdown_for_totals(self):
        metrics = _phase_metrics([{
            "history_home_team": "Time X", "history_away_team": "Time Y",
            "goals_ht": 1, "goals_2h": 2,
            "corners_ht": 5, "corners_2h": 4, "corners_ht_home": 2, "corners_ht_away": 3,
            "corners_2h_home": 1, "corners_2h_away": 3,
            "yellow_cards_ht": 3, "red_cards_ht": 0, "yellow_cards_2h": 2, "red_cards_2h": 0,
            "cards_ht_home": 1, "cards_ht_away": 2, "cards_2h_home": 2, "cards_2h_away": 0,
        }])
        first_corner = metrics["history_values"]["corners_1h"][0]
        first_cards = metrics["history_values"]["cards_1h"][0]
        self.assertEqual((first_corner["home_value"], first_corner["away_value"]), (2, 3))
        self.assertEqual((first_cards["home_value"], first_cards["away_value"]), (1, 2))

    def test_excludes_requested_youth_age_variations(self):
        for name in ("Brasil Sub-20", "Portugal Sub19", "Premier League U18", "England U-17", "Under 20", "Brazil Sub 21", "Premier League U21", "Hong Kong U-22", "Argentina Sub22", "Under-22"):
            self.assertTrue(_is_excluded_youth_match(name))
        self.assertFalse(_is_excluded_youth_match("Brazil Serie A", "Palmeiras", "Santos"))

    def test_assigns_two_history_tables_to_home_and_away_without_h2h(self):
        html = """
        <table><tr><td><a href="/soccer/r/1/a-vs-deportivo-colonia">A v Deportivo Colonia</a></td><td>1-0</td></tr></table>
        <table><tr><td><a href="/soccer/r/2/b-vs-deportivo-italiano">B v Deportivo Italiano</a></td><td>0-2</td></tr></table>
        """
        tables = _find_history_tables(
            BeautifulSoup(html, "html.parser"),
            home_team="Deportivo Colonia",
            away_team="Deportivo Italiano",
        )
        self.assertNotIn("h2h", tables)
        self.assertIn("Deportivo Colonia", tables["home"].get_text(" "))
        self.assertIn("Deportivo Italiano", tables["away"].get_text(" "))

    def test_team_name_matching_ignores_common_club_suffix(self):
        self.assertTrue(_team_name_in_text("Ipswich FC", "Ipswich v North Star"))
        self.assertTrue(_team_name_in_text("FC Barcelona", "Barcelona v Sevilla"))
        self.assertFalse(_team_name_in_text("Ipswich FC", "Brisbane Roar v North Star"))

    def test_matches_short_team_acronym_as_a_complete_word(self):
        self.assertTrue(_team_name_in_text("CRB", "Ceará v CRB"))
        self.assertTrue(_team_name_in_text("ABC", "ABC v Náutico"))
        self.assertFalse(_team_name_in_text("CRB", "Scrabble United v Ceará"))

    def test_matches_club_prefixes_and_connectors_in_team_name(self):
        self.assertTrue(_team_name_in_text("Talleres Cordoba", "Platense v CA Talleres de Córdoba"))
        self.assertTrue(_team_name_in_text("Talleres Cordoba", "CA Talleres de Córdoba v Vélez"))

    def test_matches_translated_reserve_team_names(self):
        self.assertTrue(_team_name_in_text("River Plate Reserves", "River Plate - Reservas v Racing"))
        self.assertTrue(_team_name_in_text("Godoy Cruz Reserves", "Godoy Cruz - Reservas v Sarmiento"))

    def test_matches_women_team_abbreviations_across_domains(self):
        self.assertTrue(_team_name_in_text("Taubate Women", "Taubaté (F) v Itabirito (F)"))
        self.assertTrue(_team_name_in_text("Palmeiras Women", "Mixto EC (W) v Palmeiras (F)"))

    def test_parses_portuguese_reserve_history_sections(self):
        html = """
        <h3>Confronto Direto</h3>
        <table><tr><td><a href="/soccer/r/1/river-v-godoy">River Plate - Reservas v Godoy Cruz - Reservas</a></td><td>2-1</td></tr></table>
        <h3>River Plate - Reservas - Partidas Recentes</h3>
        <table><tr><td><a href="/soccer/r/2/river-v-racing">River Plate - Reservas v Racing - Reservas</a></td><td>3-1</td></tr></table>
        <h3>Godoy Cruz - Reservas - Partidas Recentes</h3>
        <table><tr><td><a href="/soccer/r/3/godoy-v-sarmiento">Godoy Cruz - Reservas v Sarmiento - Reservas</a></td><td>1-0</td></tr></table>
        """
        tables = _find_history_tables(
            BeautifulSoup(html, "html.parser"),
            home_team="River Plate Reserves",
            away_team="Godoy Cruz Reserves",
        )
        self.assertIn("Godoy Cruz", tables["h2h"].get_text(" "))
        self.assertIn("Racing", tables["home"].get_text(" "))
        self.assertIn("Sarmiento", tables["away"].get_text(" "))

    def test_assigns_women_recent_tables_to_home_and_away(self):
        html = """
        <h3>Confronto Direto</h3>
        <table><tr><td><a href="/soccer/r/1/palmeiras-v-taubate">Palmeiras (F) v Taubaté (F)</a></td><td>5-0</td></tr></table>
        <h3>Taubaté (F) - Partidas Recentes</h3>
        <table><tr><td><a href="/soccer/r/2/taubate-v-itabirito">Taubaté (F) v Itabirito (F)</a></td><td>0-1</td></tr></table>
        <h3>Palmeiras (F) - Partidas Recentes</h3>
        <table><tr><td><a href="/soccer/r/3/palmeiras-v-corinthians">Palmeiras (F) v Corinthians (F)</a></td><td>1-0</td></tr></table>
        """
        tables = _find_history_tables(
            BeautifulSoup(html, "html.parser"),
            home_team="Taubate Women",
            away_team="Palmeiras Women",
        )
        self.assertIn("Itabirito", tables["home"].get_text(" "))
        self.assertIn("Corinthians", tables["away"].get_text(" "))

    def test_uses_official_team_link_to_match_short_display_name(self):
        html = """
        <a href="/soccer/t/6562/columbus-crew">[5] Columbus Crew</a>
        <a href="/soccer/t/44097/unam-pumas">Pumas [16]</a>
        <h3>Columbus Crew - Partidas Recentes</h3>
        <table><tr><td><a href="/soccer/r/1/columbus-v-atlas">Columbus Crew v Atlas</a></td><td>3-1</td></tr></table>
        <h3>Pumas - Partidas Recentes</h3>
        <table><tr><td><a href="/soccer/r/2/cincinnati-v-pumas">FC Cincinnati v Pumas</a></td><td>2-0</td></tr></table>
        """
        tables = _find_history_tables(
            BeautifulSoup(html, "html.parser"),
            home_team="Columbus Crew",
            away_team="UNAM Pumas",
        )
        self.assertIn("Columbus Crew", tables["home"].get_text(" "))
        self.assertIn("Pumas", tables["away"].get_text(" "))


if __name__ == "__main__":
    unittest.main()
