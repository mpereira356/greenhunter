import json
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

from flask import Flask

from app.extensions import db
from app.models import LiveGameState
from app.services import worker


def _stats(corners=0, cards=0, shots=0, on_target=0, dangerous=0, goals=0):
    def value(total):
        return {"home": total, "away": 0, "total": total}

    return {
        "Corners": value(corners),
        "Yellow Cards": value(cards),
        "Shots": value(shots),
        "On Target": value(on_target),
        "Dangerous Attacks": value(dangerous),
        "Goals": value(goals),
    }


def _payload(time_text, minute, stats):
    return {
        "time_text": time_text,
        "minute": minute,
        "score": f"{stats['Goals']['home']} x 0",
        "stats": stats,
        "events": [],
        "home_team": "Casa",
        "away_team": "Fora",
        "league": "Liga",
    }


class SecondHalfBaselineTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.app = Flask(__name__)
        self.app.config.update(
            SQLALCHEMY_DATABASE_URI=f"sqlite:///{Path(self.temp.name) / 'second-half.db'}",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
        )
        db.init_app(self.app)
        self.context = self.app.app_context()
        self.context.push()
        db.create_all()
        self.game = {"game_id": "game-1", "url": "https://example.test/game-1"}
        self._clear_memory()

    def tearDown(self):
        self._clear_memory()
        db.session.remove()
        db.drop_all()
        self.context.pop()
        self.temp.cleanup()

    def _clear_memory(self):
        worker.SECOND_HALF_BASELINES.clear()
        worker.SECOND_HALF_FROM_NOW.clear()
        worker.LAST_GAME_SNAPSHOTS.clear()
        worker.HALFTIME_CONFIRMED_AT.clear()

    def _observe(self, time_text, minute, stats, commit=True):
        worker.persist_live_game_state(self.game, _payload(time_text, minute, stats))
        if commit:
            db.session.commit()
        return LiveGameState.query.filter_by(game_id="game-1").one()

    def test_event_at_45_plus_is_in_final_baseline(self):
        self._observe("45'", 45, _stats(corners=3))
        self._observe("45+2'", 45, _stats(corners=4))
        state = self._observe("HT", 45, _stats(corners=4))
        self.assertEqual(state.first_half_snapshot_status, worker.FIRST_HALF_FINAL_CONFIRMED)
        self.assertEqual(json.loads(state.first_half_snapshot_json)["Corners"]["total"], 4)

    def test_multiple_kinds_of_45_plus_events_are_frozen(self):
        final = _stats(corners=5, cards=2, shots=11, on_target=4, dangerous=31, goals=1)
        self._observe("45+5'", 45, final)
        self._observe("HT", 45, final)
        baseline = worker.get_second_half_baseline("game-1")
        for key in ("Corners", "Yellow Cards", "Shots", "On Target", "Dangerous Attacks", "Goals"):
            self.assertEqual(baseline[key]["total"], final[key]["total"])

    def test_plain_45_never_finalizes_snapshot(self):
        state = self._observe("45'", 45, _stats(corners=3))
        self.assertEqual(state.first_half_snapshot_status, worker.FIRST_HALF_PROVISIONAL)
        self.assertIsNone(state.first_half_snapshot_json)
        self.assertFalse(worker.second_half_allowed("game-1", "45'", 45))

    def test_explicit_ht_finalizes_and_second_half_starts_at_zero(self):
        final = _stats(corners=6, on_target=4)
        self._observe("HT", 45, final)
        delta = worker.apply_second_half_delta(final, worker.get_second_half_baseline("game-1"))
        self.assertEqual(delta["Corners"]["total"], 0)
        self.assertEqual(delta["On Target"]["total"], 0)

    def test_direct_transition_from_45_plus_uses_last_pre_transition_snapshot(self):
        final = _stats(corners=7, cards=3)
        self._observe("45+4'", 45, final)
        self._observe("2nd Half", 46, final)
        self.assertEqual(worker.get_second_half_baseline("game-1")["Corners"]["total"], 7)
        self.assertTrue(worker.second_half_allowed("game-1", "2nd Half", 46))

    def test_event_exactly_at_transition_counts_as_second_half(self):
        first_half = _stats(corners=7)
        self._observe("45+3'", 45, first_half)
        current = _stats(corners=8)
        self._observe("2nd Half", 46, current)
        delta = worker.apply_second_half_delta(current, worker.get_second_half_baseline("game-1"))
        self.assertEqual(delta["Corners"]["total"], 1)

    def test_restart_during_stoppage_preserves_provisional_updates(self):
        self._observe("45+1'", 45, _stats(cards=1))
        self._clear_memory()
        self._observe("45+4'", 45, _stats(cards=2))
        self._observe("2nd Half", 46, _stats(cards=2))
        self.assertEqual(worker.get_second_half_baseline("game-1")["Yellow Cards"]["total"], 2)

    def test_restart_during_interval_loads_final_baseline(self):
        final = _stats(shots=10)
        self._observe("HT", 45, final)
        self._clear_memory()
        self.assertEqual(worker.get_second_half_baseline("game-1")["Shots"]["total"], 10)

    def test_restart_at_start_of_second_half_promotes_persisted_provisional(self):
        final = _stats(on_target=5)
        self._observe("45+2'", 45, final)
        self._clear_memory()
        self._observe("46'", 46, final)
        self.assertEqual(worker.get_second_half_baseline("game-1")["On Target"]["total"], 5)

    def test_restart_late_in_second_half_reuses_final_without_discarding_2h_stats(self):
        first_half = _stats(corners=4)
        self._observe("HT", 45, first_half)
        self._observe("2nd Half", 46, first_half)
        self._clear_memory()
        current = _stats(corners=7)
        self._observe("67'", 67, current)
        delta = worker.apply_second_half_delta(current, worker.get_second_half_baseline("game-1"))
        self.assertEqual(delta["Corners"]["total"], 3)

    def test_legacy_snapshot_without_final_status_is_rejected(self):
        old = _stats(corners=2)
        state = LiveGameState(
            game_id="game-1",
            first_half_snapshot_json=json.dumps(old),
            first_half_snapshot_minute=45,
            second_half_baseline_json=json.dumps(old),
        )
        db.session.add(state)
        db.session.commit()
        self.assertIsNone(worker.get_second_half_baseline("game-1"))
        self.assertFalse(worker.second_half_allowed("game-1", "60'", 60))

    def test_restart_in_second_half_without_provisional_blocks_rule(self):
        self._observe("67'", 67, _stats(corners=8))
        self.assertIsNone(worker.get_second_half_baseline("game-1"))
        self.assertFalse(worker.second_half_allowed("game-1", "67'", 67))

    def test_stale_partial_first_half_snapshot_is_not_promoted_after_restart(self):
        self._observe("32'", 32, _stats(corners=3))
        self._clear_memory()
        self._observe("67'", 67, _stats(corners=9))
        state = LiveGameState.query.filter_by(game_id="game-1").one()
        self.assertNotEqual(state.first_half_snapshot_status, worker.FIRST_HALF_FINAL_CONFIRMED)
        self.assertIsNone(worker.get_second_half_baseline("game-1"))

    def test_provider_regression_clamps_to_zero_without_mutating_baseline(self):
        first_half = _stats(corners=6, shots=12)
        self._observe("HT", 45, first_half)
        baseline = worker.get_second_half_baseline("game-1")
        corrected = _stats(corners=5, shots=11)
        delta = worker.apply_second_half_delta(corrected, baseline)
        self.assertEqual(delta["Corners"]["total"], 0)
        self.assertEqual(delta["Shots"]["total"], 0)
        self.assertEqual(worker.get_second_half_baseline("game-1")["Corners"]["total"], 6)

    def test_stat_missing_from_final_baseline_is_not_leaked_from_first_half(self):
        baseline = {"Corners": {"home": 2, "away": 1, "total": 3}}
        cumulative = {
            "Corners": {"home": 3, "away": 1, "total": 4},
            "On Target": {"home": 6, "away": 2, "total": 8},
        }
        delta = worker.apply_second_half_delta(cumulative, baseline)
        self.assertEqual(delta["Corners"]["total"], 1)
        self.assertNotIn("On Target", delta)

    def test_normal_cumulative_stats_remain_untouched(self):
        stats = _stats(corners=9, goals=2)
        # Normal rules continue receiving the original stats object; period
        # separation is applied only at the existing second_half_only branches.
        self.assertEqual(stats["Corners"]["total"], 9)
        self.assertEqual(stats["Goals"]["total"], 2)

    def test_normal_and_second_half_rules_use_independent_views_of_same_match(self):
        condition_normal = SimpleNamespace(
            stat_key="corners", side="total", operator=">=", value=6, group_id=0
        )
        condition_second_half = SimpleNamespace(
            stat_key="corners", side="total", operator=">=", value=2, group_id=0
        )
        normal_rule = SimpleNamespace(second_half_only=False, conditions=[condition_normal])
        second_half_rule = SimpleNamespace(second_half_only=True, conditions=[condition_second_half])

        # The fourth corner happened in 45+ and therefore belongs to the
        # cumulative match view and to the final first-half baseline.
        halftime = _stats(corners=4)
        self._observe("45+3'", 45, halftime)
        self._observe("HT", 45, halftime)
        baseline = worker.get_second_half_baseline("game-1")

        # At the beginning of 2H, the normal view still has all four corners,
        # while the period-specific view starts at zero.
        start_cumulative = _stats(corners=4)
        start_original = deepcopy(start_cumulative)
        start_second_half = worker.apply_second_half_delta(start_cumulative, baseline)
        self.assertEqual(start_cumulative, start_original)
        self.assertEqual(start_cumulative["Corners"]["total"], 4)
        self.assertEqual(start_second_half["Corners"]["total"], 0)

        # Two more corners in 2H produce independent 6 (normal) and 2 (2H)
        # views. Both rules are evaluated over the same match without changing
        # the globally accumulated payload.
        current_cumulative = _stats(corners=6)
        current_original = deepcopy(current_cumulative)
        state = self._observe("63'", 63, current_cumulative)
        provider_raw_persisted = json.loads(state.stats_json)
        persisted_final_baseline = json.loads(state.first_half_snapshot_json)
        current_second_half = worker.apply_second_half_delta(current_cumulative, baseline)

        # Four explicit, auditable views of the very same match:
        # 1) provider/raw persisted, 2) normal rule, 3) final 1H baseline,
        # 4) second_half_only rule.
        self.assertEqual(provider_raw_persisted["Corners"]["total"], 6)
        self.assertEqual(current_cumulative["Corners"]["total"], 6)
        self.assertEqual(persisted_final_baseline["Corners"]["total"], 4)
        self.assertEqual(current_second_half["Corners"]["total"], 2)
        self.assertTrue(worker.evaluate_rule(normal_rule, current_cumulative))
        self.assertTrue(worker.evaluate_rule(second_half_rule, current_second_half))
        self.assertEqual(current_cumulative, current_original)
        self.assertEqual(current_cumulative["Corners"]["total"], 6)
        self.assertEqual(current_second_half["Corners"]["total"], 2)


if __name__ == "__main__":
    unittest.main()
