import unittest
from datetime import timedelta
from types import SimpleNamespace

from app.services.sharing import ticket_is_shareable, ticket_snapshot_is_pregame
from app.utils.time import now_sp


class SharingEligibilityTest(unittest.TestCase):
    def _ticket(self, kickoff, ticket_status="pending", leg_status="pending"):
        return SimpleNamespace(status=ticket_status, legs=[SimpleNamespace(
            status=leg_status, game_day=kickoff.strftime("%Y-%m-%d"),
            game_time=kickoff.strftime("%H:%M"),
        )])

    def test_ticket_only_shareable_while_every_leg_is_pending_and_pregame(self):
        now = now_sp().replace(second=0, microsecond=0)
        self.assertTrue(ticket_is_shareable(self._ticket(now + timedelta(hours=2)), now))
        self.assertFalse(ticket_is_shareable(self._ticket(now - timedelta(minutes=1)), now))
        self.assertFalse(ticket_is_shareable(self._ticket(now + timedelta(hours=2), leg_status="green"), now))
        self.assertFalse(ticket_is_shareable(self._ticket(now + timedelta(hours=2), ticket_status="red"), now))

    def test_snapshot_expires_when_a_single_game_has_started(self):
        now = now_sp().replace(second=0, microsecond=0)
        future = now + timedelta(hours=2)
        past = now - timedelta(minutes=1)
        payload = {"legs": [
            {"game_day": future.strftime("%Y-%m-%d"), "game_time": future.strftime("%H:%M")},
            {"game_day": past.strftime("%Y-%m-%d"), "game_time": past.strftime("%H:%M")},
        ]}
        self.assertFalse(ticket_snapshot_is_pregame(payload, now))


if __name__ == "__main__":
    unittest.main()
