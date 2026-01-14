from datetime import datetime

from flask_login import UserMixin
from werkzeug.security import check_password_hash, generate_password_hash

from .extensions import db


class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True)
    password_hash = db.Column(db.String(255), nullable=False)
    telegram_token = db.Column(db.String(255))
    telegram_chat_id = db.Column(db.String(64))
    telegram_verified = db.Column(db.Boolean, default=False, nullable=False)
    is_admin = db.Column(db.Boolean, default=False, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    rules = db.relationship("Rule", backref="user", cascade="all, delete-orphan")
    alerts = db.relationship("MatchAlert", backref="user", cascade="all, delete-orphan")

    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    @property
    def is_admin_user(self) -> bool:
        if self.is_admin:
            return True
        return (self.username or "").lower() == "admin"


class Rule(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    name = db.Column(db.String(120), nullable=False)
    time_limit_min = db.Column(db.Integer, nullable=False, default=30)
    message_template = db.Column(db.Text)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    second_half_only = db.Column(db.Boolean, default=False, nullable=False)
    follow_ht = db.Column(db.Boolean, default=True, nullable=False)
    follow_ft = db.Column(db.Boolean, default=True, nullable=False)
    outcome_green_stage = db.Column(db.String(5), default="HT", nullable=False)
    outcome_red_stage = db.Column(db.String(5), default="HT", nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    last_checked_at = db.Column(db.DateTime)
    last_match_desc = db.Column(db.String(255))
    last_alert_at = db.Column(db.DateTime)
    last_alert_desc = db.Column(db.String(255))

    conditions = db.relationship(
        "RuleCondition", backref="rule", cascade="all, delete-orphan", order_by="RuleCondition.id"
    )
    outcome_conditions = db.relationship(
        "RuleOutcomeCondition",
        backref="rule",
        cascade="all, delete-orphan",
        order_by="RuleOutcomeCondition.id",
    )
    alerts = db.relationship("MatchAlert", backref="rule", cascade="all, delete-orphan")


class RuleCondition(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    rule_id = db.Column(db.Integer, db.ForeignKey("rule.id"), nullable=False)
    stat_key = db.Column(db.String(120), nullable=False)
    side = db.Column(db.String(10), nullable=False)
    operator = db.Column(db.String(4), nullable=False)
    value = db.Column(db.Integer, nullable=False)
    group_id = db.Column(db.Integer, default=0, nullable=False)


class RuleOutcomeCondition(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    rule_id = db.Column(db.Integer, db.ForeignKey("rule.id"), nullable=False)
    outcome_type = db.Column(db.String(10), nullable=False)
    stat_key = db.Column(db.String(120), nullable=False)
    side = db.Column(db.String(10), nullable=False)
    operator = db.Column(db.String(4), nullable=False)
    value = db.Column(db.Integer, nullable=False)


class MatchAlert(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    rule_id = db.Column(db.Integer, db.ForeignKey("rule.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    game_id = db.Column(db.String(32), nullable=False)
    url = db.Column(db.String(255), nullable=False)
    status = db.Column(db.String(20), default="pending", nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    alert_minute = db.Column(db.Integer)
    initial_score = db.Column(db.String(20))
    ht_score = db.Column(db.String(20))
    ft_score = db.Column(db.String(20))
    initial_stats_json = db.Column(db.Text)
    ht_stats_json = db.Column(db.Text)
    ft_stats_json = db.Column(db.Text)
    league = db.Column(db.String(120))
    home_team = db.Column(db.String(120))
    away_team = db.Column(db.String(120))
    ft_completed = db.Column(db.Boolean, default=False, nullable=False)

    __table_args__ = (db.UniqueConstraint("rule_id", "game_id", name="uix_rule_game"),)


class LoginAttempt(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80))
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"))
    ip_address = db.Column(db.String(64))
    success = db.Column(db.Boolean, default=False, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class AdminBroadcast(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    message = db.Column(db.Text, nullable=False)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
