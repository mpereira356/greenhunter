import unittest

from app.services.worker import _telegram_reply_text, g2_bet_outcome, parse_telegram_bet_reply


class TelegramBetReplyTest(unittest.TestCase):
    def test_accepts_comma_or_dot(self):
        self.assertEqual(parse_telegram_bet_reply("15/3,42"), (15.0, 3.42, None))
        self.assertEqual(parse_telegram_bet_reply(" 15,50 / 1.875 "), (15.5, 1.875, None))
        self.assertEqual(parse_telegram_bet_reply("15/3,42/G2"), (15.0, 3.42, "G2"))

    def test_rejects_invalid_or_non_positive_values(self):
        for value in ("15", "texto", "0/2,00", "15/1", "-5/2"):
            self.assertIsNone(parse_telegram_bet_reply(value))

    def test_confirmation_has_financial_summary(self):
        message = _telegram_reply_text(15, 3.42)
        self.assertIn("R$ 51,30", message)
        self.assertIn("R$ 36,30", message)

    def test_g2_settlement(self):
        self.assertEqual(g2_bet_outcome(0, 15, 3.42), ("red", -15))
        self.assertEqual(g2_bet_outcome(1, 15, 3.42), ("push", 0.0))
        self.assertEqual(g2_bet_outcome(2, 15, 3.42), ("green", 36.3))


if __name__ == "__main__":
    unittest.main()
