import tempfile
import unittest
from pathlib import Path

from flask import Flask

from app.extensions import db
from app.models import Rule, RuleCondition, RuleOutcomeCondition, User
from app.rules.routes import _duplicate_rule


class RuleCopyTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.app = Flask(__name__)
        self.app.config.update(
            SQLALCHEMY_DATABASE_URI=f"sqlite:///{Path(self.temp.name) / 'rules.db'}",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
        )
        db.init_app(self.app)
        self.context = self.app.app_context()
        self.context.push()
        db.create_all()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        self.context.pop()
        self.temp.cleanup()

    def test_duplicate_copies_configuration_but_not_runtime_state(self):
        user = User(id=1, username="copy-test", password_hash="test")
        db.session.add(user)
        source = Rule(user_id=1, name="Escanteios", is_active=True, time_limit_min=35,
                      last_alert_desc="partida antiga", allowed_leagues_json='["Liga A"]')
        db.session.add(source)
        db.session.flush()
        db.session.add(RuleCondition(rule_id=source.id, stat_key="corners", side="total",
                                     operator=">=", value=4, group_id=2))
        db.session.add(RuleOutcomeCondition(rule_id=source.id, outcome_type="green",
                                            stat_key="corners", side="total",
                                            operator=">=", value=8, group_id=1))
        db.session.commit()

        duplicate = _duplicate_rule(source)
        db.session.commit()

        self.assertEqual(duplicate.name, "Escanteios (cópia)")
        self.assertFalse(duplicate.is_active)
        self.assertIsNone(duplicate.last_alert_desc)
        self.assertEqual(duplicate.allowed_leagues_json, '["Liga A"]')
        self.assertEqual([(c.stat_key, c.value, c.group_id) for c in duplicate.conditions],
                         [("corners", 4, 2)])
        self.assertEqual([(c.outcome_type, c.value) for c in duplicate.outcome_conditions],
                         [("green", 8)])

        second = _duplicate_rule(source)
        db.session.commit()
        self.assertEqual(second.name, "Escanteios (cópia 2)")


if __name__ == "__main__":
    unittest.main()
