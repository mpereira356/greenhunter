import unittest

from app.services.matchday import _is_excluded_youth_match, parse_matchday_html
from app.services.match_analysis import _phase_metrics
from app.services.scraper import _find_history_tables, _scheduled_time_from_history_page, _team_name_in_text
from bs4 import BeautifulSoup


class MatchdayParserTest(unittest.TestCase):
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

    def test_excludes_requested_youth_age_variations(self):
        for name in ("Brasil Sub-20", "Portugal Sub19", "Premier League U18", "England U-17", "Under 20"):
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
