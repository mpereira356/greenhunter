from datetime import datetime

from flask_login import UserMixin
from werkzeug.security import check_password_hash, generate_password_hash

from .extensions import db
from .utils.time import now_sp


class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True)
    password_hash = db.Column(db.String(255), nullable=False)
    telegram_token = db.Column(db.String(255))
    telegram_chat_id = db.Column(db.String(64))
    telegram_verified = db.Column(db.Boolean, default=False, nullable=False)
    telegram_update_offset = db.Column(db.Integer)
    mercadopago_subscription_id = db.Column(db.String(80), index=True)
    mercadopago_subscription_status = db.Column(db.String(30))
    mercadopago_checkout_url = db.Column(db.Text)
    is_admin = db.Column(db.Boolean, default=False, nullable=False)
    subscription_plan = db.Column(db.String(20), default="starter", nullable=False)
    premium_granted_by_admin = db.Column(db.Boolean, default=False, nullable=False)
    rule_limit = db.Column(db.Integer, default=2, nullable=False)
    paid_until = db.Column(db.DateTime)
    trial_until = db.Column(db.DateTime)
    favorite_live_leagues_json = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)

    rules = db.relationship("Rule", backref="user", cascade="all, delete-orphan")
    alerts = db.relationship("MatchAlert", backref="user", cascade="all, delete-orphan")
    saved_tickets = db.relationship("SavedTicket", backref="user", cascade="all, delete-orphan")

    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password, method="pbkdf2:sha256", salt_length=16)

    def check_password(self, password: str) -> bool:
        try:
            return check_password_hash(self.password_hash, password)
        except ValueError:
            return False

    @property
    def is_admin_user(self) -> bool:
        if self.is_admin:
            return True
        return (self.username or "").lower() == "admin"

    @property
    def has_premium_analysis(self) -> bool:
        if self.is_admin_user:
            return True
        return self.has_paid_access

    @property
    def is_premium_user(self) -> bool:
        return self.is_admin_user or self.has_paid_access

    @property
    def matchday_sample_limit(self) -> int:
        return 10 if self.is_premium_user else 3

    @property
    def generated_ticket_game_limit(self) -> int:
        if self.is_admin_user:
            return 500
        return 10 if self.is_premium_user else 5

    @property
    def saved_ticket_limit(self) -> int:
        if self.is_admin_user:
            return 10000
        return 200 if self.is_premium_user else 5

    @property
    def trial_active(self) -> bool:
        return bool(self.trial_until and self.trial_until > now_sp())

    @property
    def paid_active(self) -> bool:
        plan = (self.subscription_plan or "starter").lower()
        return bool(plan in ("pro", "custom") and self.paid_until and self.paid_until > now_sp())

    @property
    def paid_days_left(self) -> int:
        if not self.paid_active:
            return 0
        delta = self.paid_until - now_sp()
        return max(1, int((delta.total_seconds() + 86399) // 86400))

    @property
    def trial_days_left(self) -> int:
        if not self.trial_active:
            return 0
        delta = self.trial_until - now_sp()
        return max(1, int((delta.total_seconds() + 86399) // 86400))

    @property
    def has_paid_access(self) -> bool:
        if self.premium_granted_by_admin:
            return True
        if self.paid_active:
            return True
        return self.trial_active

    @property
    def plan_label(self) -> str:
        plan = (self.subscription_plan or "starter").lower()
        if self.premium_granted_by_admin:
            return "Pro (liberado pelo admin)"
        if self.paid_active:
            labels = {"pro": "Pro", "custom": "Custom"}
            return f"{labels.get(plan, 'Pago')} ({self.paid_days_left}d)"
        if self.trial_active and (self.subscription_plan or "starter").lower() == "starter":
            return f"Trial Pro ({self.trial_days_left}d)"
        labels = {
            "starter": "Starter",
            "pro": "Pro",
            "custom": "Custom",
        }
        return labels.get((self.subscription_plan or "starter").lower(), "Starter")

    @property
    def effective_rule_limit(self) -> int:
        if self.is_admin_user:
            return 1000000
        if not self.is_admin_user and not self.has_paid_access:
            return 2
        if self.trial_active and (self.subscription_plan or "starter").lower() == "starter":
            return 20
        try:
            limit = int(self.rule_limit or 0)
        except (TypeError, ValueError):
            limit = 0
        return max(0, limit)


class MatchdayLeaguePreference(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    normalized_name = db.Column(db.String(160), unique=True, nullable=False, index=True)
    display_name = db.Column(db.String(160), nullable=False)
    is_relevant = db.Column(db.Boolean, default=True, nullable=False)
    updated_at = db.Column(db.DateTime, default=now_sp, onupdate=now_sp, nullable=False)


class UserMatchdayPreference(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), unique=True, nullable=False, index=True)
    relevant_leagues_json = db.Column(db.Text)
    market_settings_json = db.Column(db.Text)
    updated_at = db.Column(db.DateTime, default=now_sp, onupdate=now_sp, nullable=False)

    user = db.relationship("User", backref=db.backref("matchday_preference", uselist=False, cascade="all, delete-orphan"))


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
    outcome_green_minute = db.Column(db.Integer)
    outcome_red_minute = db.Column(db.Integer)
    outcome_red_if_no_green = db.Column(db.Boolean, default=False, nullable=False)
    notify_telegram = db.Column(db.Boolean, default=True, nullable=False)
    alert_on_penalty = db.Column(db.Boolean, default=False, nullable=False)
    score_home = db.Column(db.Integer)
    score_away = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)
    last_checked_at = db.Column(db.DateTime)
    last_match_desc = db.Column(db.String(255))
    last_alert_at = db.Column(db.DateTime)
    last_alert_desc = db.Column(db.String(255))
    # Optional league filter (JSON list of league names). If set, alerts are emitted only for matching leagues.
    allowed_leagues_json = db.Column(db.Text)

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


class MercadoPagoWebhookEvent(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    event_key = db.Column(db.String(180), unique=True, nullable=False, index=True)
    event_type = db.Column(db.String(80))
    resource_id = db.Column(db.String(100))
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)


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
    group_id = db.Column(db.Integer, default=0, nullable=False)


class MatchAlert(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    rule_id = db.Column(db.Integer, db.ForeignKey("rule.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    game_id = db.Column(db.String(32), nullable=False)
    url = db.Column(db.String(255), nullable=False)
    status = db.Column(db.String(20), default="pending", nullable=False)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)
    alert_minute = db.Column(db.Integer)
    result_minute = db.Column(db.Integer)
    result_time_hhmm = db.Column(db.String(8))
    initial_score = db.Column(db.String(20))
    last_score = db.Column(db.String(20))
    last_score_minute = db.Column(db.Integer)
    ht_score = db.Column(db.String(20))
    ft_score = db.Column(db.String(20))
    initial_stats_json = db.Column(db.Text)
    ht_stats_json = db.Column(db.Text)
    ft_stats_json = db.Column(db.Text)
    penalty_last_total = db.Column(db.Integer, default=0, nullable=False)
    penalty_notified = db.Column(db.Boolean, default=False, nullable=False)
    penalty_baseline_set = db.Column(db.Boolean, default=False, nullable=False)
    league = db.Column(db.String(120))
    home_team = db.Column(db.String(120))
    away_team = db.Column(db.String(120))
    ai_score = db.Column(db.Integer)
    ai_verdict = db.Column(db.String(40))
    ai_commentary = db.Column(db.Text)
    ml_pred_score = db.Column(db.Integer)
    ml_pred_verdict = db.Column(db.String(40))
    ml_pred_prob_green = db.Column(db.Float)
    ml_model_samples = db.Column(db.Integer)
    ml_model_trained_at = db.Column(db.String(32))
    market_key = db.Column(db.String(64))
    market_label = db.Column(db.String(120))
    outcome_signature = db.Column(db.Text)
    target_side = db.Column(db.String(20))
    target_operator = db.Column(db.String(8))
    target_value = db.Column(db.Integer)
    target_text = db.Column(db.String(255))
    initial_events_json = db.Column(db.Text)
    result_events_json = db.Column(db.Text)
    ft_events_json = db.Column(db.Text)
    initial_event_metrics_json = db.Column(db.Text)
    result_event_metrics_json = db.Column(db.Text)
    ft_event_metrics_json = db.Column(db.Text)
    ft_completed = db.Column(db.Boolean, default=False, nullable=False)
    telegram_entry_message_id = db.Column(db.Integer)
    telegram_entry_enriched = db.Column(db.Boolean, default=False, nullable=False)
    stake_amount = db.Column(db.Float)
    stake_odd = db.Column(db.Float)
    bet_note = db.Column(db.Text)
    bet_recorded_at = db.Column(db.DateTime)
    bet_tracking_type = db.Column(db.String(20))
    bet_initial_goal_total = db.Column(db.Integer)
    bet_settlement_stage = db.Column(db.String(5))
    bet_status = db.Column(db.String(20))
    bet_profit = db.Column(db.Float)
    bet_settled_at = db.Column(db.DateTime)

    __table_args__ = (db.UniqueConstraint("rule_id", "game_id", name="uix_rule_game"),)


class LiveGameState(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    game_id = db.Column(db.String(32), nullable=False, unique=True, index=True)
    url = db.Column(db.String(255))
    league = db.Column(db.String(120))
    home_team = db.Column(db.String(120))
    away_team = db.Column(db.String(120))
    time_text = db.Column(db.String(40))
    minute = db.Column(db.Integer)
    score = db.Column(db.String(20))
    stats_json = db.Column(db.Text)
    events_json = db.Column(db.Text)
    first_half_provisional_json = db.Column(db.Text)
    first_half_provisional_minute = db.Column(db.Integer)
    first_half_provisional_time_text = db.Column(db.String(40))
    first_half_snapshot_json = db.Column(db.Text)
    first_half_snapshot_minute = db.Column(db.Integer)
    first_half_snapshot_status = db.Column(db.String(24))
    first_half_snapshot_confirmed_at = db.Column(db.DateTime)
    ht_seen_at = db.Column(db.DateTime)
    second_half_baseline_json = db.Column(db.Text)
    second_half_started = db.Column(db.Boolean, default=False, nullable=False)
    second_half_started_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)
    updated_at = db.Column(db.DateTime, default=now_sp, onupdate=now_sp, nullable=False)


class SavedTicket(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    name = db.Column(db.String(80), nullable=False)
    total_odd = db.Column(db.Float, nullable=False)
    stake_amount = db.Column(db.Float, nullable=False)
    status = db.Column(db.String(20), default="pending", nullable=False, index=True)
    profit = db.Column(db.Float, default=0, nullable=False)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)
    resolved_at = db.Column(db.DateTime)
    telegram_notified_at = db.Column(db.DateTime)

    legs = db.relationship(
        "SavedTicketLeg", backref="ticket", cascade="all, delete-orphan", order_by="SavedTicketLeg.id"
    )


class SavedTicketLeg(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    ticket_id = db.Column(db.Integer, db.ForeignKey("saved_ticket.id"), nullable=False, index=True)
    game_id = db.Column(db.String(32), nullable=False, index=True)
    game_day = db.Column(db.String(10))
    game_time = db.Column(db.String(40))
    league = db.Column(db.String(120))
    home_team = db.Column(db.String(120), nullable=False)
    away_team = db.Column(db.String(120), nullable=False)
    market_key = db.Column(db.String(64), nullable=False)
    market_label = db.Column(db.String(160), nullable=False)
    target_side = db.Column(db.String(20), default="total", nullable=False)
    target_line = db.Column(db.Float)
    status = db.Column(db.String(20), default="pending", nullable=False)
    result_value = db.Column(db.Float)
    samples = db.Column(db.Integer)
    source_group = db.Column(db.String(120))
    predicted_probability = db.Column(db.Float)
    confidence_score = db.Column(db.Float)
    context_score = db.Column(db.Float)
    data_quality_score = db.Column(db.Float)
    consistency_score = db.Column(db.Float)
    prediction_json = db.Column(db.Text)
    result_margin = db.Column(db.Float)
    individual_odd = db.Column(db.Float)
    checked_at = db.Column(db.DateTime)


class SharedInvitation(db.Model):
    __tablename__ = "shared_invitation"

    id = db.Column(db.Integer, primary_key=True)
    sender_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    recipient_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    item_type = db.Column(db.String(20), nullable=False, index=True)
    source_id = db.Column(db.Integer, nullable=False)
    item_name = db.Column(db.String(160), nullable=False)
    payload_json = db.Column(db.Text, nullable=False)
    status = db.Column(db.String(20), nullable=False, default="pending", index=True)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)
    responded_at = db.Column(db.DateTime)

    sender = db.relationship("User", foreign_keys=[sender_id])
    recipient = db.relationship("User", foreign_keys=[recipient_id])

    __table_args__ = (
        db.Index("ix_shared_invitation_recipient_status", "recipient_id", "status"),
    )


class ModelVersion(db.Model):
    __tablename__ = "model_version"

    id = db.Column(db.Integer, primary_key=True)
    version = db.Column(db.String(80), unique=True, nullable=False, index=True)
    algorithm_family = db.Column(db.String(80), nullable=False)
    mode = db.Column(db.String(30), nullable=False, default="production")
    status = db.Column(db.String(30), nullable=False, default="baseline")
    config_json = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)


class PredictionRun(db.Model):
    __tablename__ = "prediction_run"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), index=True)
    algorithm = db.Column(db.String(80), nullable=False)
    model_version = db.Column(db.String(80), nullable=False, index=True)
    mode = db.Column(db.String(30), nullable=False, default="production")
    run_type = db.Column(db.String(30), nullable=False, default="PROSPECTIVE_SHADOW", index=True)
    target_date = db.Column(db.String(10), nullable=False, index=True)
    started_at = db.Column(db.DateTime, default=now_sp, nullable=False)
    finished_at = db.Column(db.DateTime)
    duration_ms = db.Column(db.Integer)
    status = db.Column(db.String(30), nullable=False, default="running", index=True)
    parameters_json = db.Column(db.Text)
    fixture_count = db.Column(db.Integer, default=0, nullable=False)
    candidate_count = db.Column(db.Integer, default=0, nullable=False)
    approved_count = db.Column(db.Integer, default=0, nullable=False)
    rejected_count = db.Column(db.Integer, default=0, nullable=False)
    fallback_used = db.Column(db.String(120))
    errors_json = db.Column(db.Text)
    operational_metrics_json = db.Column(db.Text)
    alert_status = db.Column(db.String(40))
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)

    predictions = db.relationship(
        "MarketPrediction", backref="run", cascade="all, delete-orphan", lazy="select"
    )


class PredictionFixtureSnapshot(db.Model):
    __tablename__ = "prediction_fixture_snapshot"

    id = db.Column(db.Integer, primary_key=True)
    run_id = db.Column(db.Integer, db.ForeignKey("prediction_run.id"), nullable=False, index=True)
    fixture_id = db.Column(db.String(64), nullable=False)
    snapshot_json = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)

    __table_args__ = (
        db.UniqueConstraint("run_id", "fixture_id", name="uix_prediction_fixture_snapshot"),
    )


class MarketPrediction(db.Model):
    __tablename__ = "market_prediction"

    id = db.Column(db.Integer, primary_key=True)
    run_id = db.Column(db.Integer, db.ForeignKey("prediction_run.id"), nullable=False, index=True)
    fixture_snapshot_id = db.Column(db.Integer, db.ForeignKey("prediction_fixture_snapshot.id"), index=True)
    model_version = db.Column(db.String(80), nullable=False, index=True)
    configuration_version = db.Column(db.String(120), index=True)
    fixture_id = db.Column(db.String(64), nullable=False, index=True)
    target_date = db.Column(db.String(10), nullable=False, index=True)
    kickoff_at = db.Column(db.String(40))
    competition = db.Column(db.String(160))
    home_team = db.Column(db.String(160))
    away_team = db.Column(db.String(160))
    market_type = db.Column(db.String(100), nullable=False)
    market_group = db.Column(db.String(100))
    scope = db.Column(db.String(20))
    direction = db.Column(db.String(10))
    line = db.Column(db.Float)
    raw_frequency = db.Column(db.Float)
    recent_frequency = db.Column(db.Float)
    adjusted_frequency = db.Column(db.Float)
    consistency_score = db.Column(db.Float)
    data_quality_score = db.Column(db.Float)
    context_score = db.Column(db.Float)
    confidence_score = db.Column(db.Float)
    sample_size = db.Column(db.Integer)
    individual_odd = db.Column(db.Float)
    status = db.Column(db.String(50), nullable=False, index=True)
    rejection_reason = db.Column(db.String(100), index=True)
    rejection_reasons_json = db.Column(db.Text)
    strengths_json = db.Column(db.Text)
    weaknesses_json = db.Column(db.Text)
    snapshot_json = db.Column(db.Text, nullable=False)
    feature_snapshot_json = db.Column(db.Text)
    v2_statistical_score = db.Column(db.Float)
    v2_calibrated_probability = db.Column(db.Float)
    calibration_source = db.Column(db.String(80))
    uncertainty_score = db.Column(db.Float)
    data_quality_v2 = db.Column(db.Float)
    temporal_reliability = db.Column(db.String(30))
    legacy_prediction_id = db.Column(db.Integer, db.ForeignKey("market_prediction.id"), index=True)
    settlement_status = db.Column(db.String(30), nullable=False, default="PENDING", index=True)
    actual_value = db.Column(db.Float)
    settled_at = db.Column(db.DateTime)
    selected_conservative = db.Column(db.Boolean, nullable=False, default=False)
    selected_balanced = db.Column(db.Boolean, nullable=False, default=False)
    prediction_timestamp = db.Column(db.DateTime)
    official_pre_match_snapshot = db.Column(db.Boolean, nullable=False, default=False, index=True)
    prospective_validity = db.Column(db.String(40), nullable=False, default="VALID", index=True)
    settlement_attempts = db.Column(db.Integer, nullable=False, default=0)
    settlement_first_attempt_at = db.Column(db.DateTime)
    settlement_last_attempt_at = db.Column(db.DateTime)
    settlement_failure_reason = db.Column(db.String(60), index=True)
    settlement_source = db.Column(db.String(80))
    settlement_audit_json = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)

    __table_args__ = (
        db.Index("ix_market_prediction_model_date_status", "model_version", "target_date", "status"),
    )


class CalibrationArtifact(db.Model):
    __tablename__ = "calibration_artifact"

    id = db.Column(db.Integer, primary_key=True)
    model_version = db.Column(db.String(80), nullable=False, index=True)
    method = db.Column(db.String(40), nullable=False)
    scope = db.Column(db.String(80), nullable=False, default="GLOBAL")
    trained_through = db.Column(db.DateTime, nullable=False)
    minimum_family_sample = db.Column(db.Integer, nullable=False)
    sample_count = db.Column(db.Integer, nullable=False)
    config_json = db.Column(db.Text, nullable=False)
    validation_metrics_json = db.Column(db.Text, nullable=False)
    is_active = db.Column(db.Boolean, nullable=False, default=False, index=True)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)


class MarketOddsSnapshot(db.Model):
    __tablename__ = "market_odds_snapshot"

    id = db.Column(db.Integer, primary_key=True)
    market_prediction_id = db.Column(db.Integer, db.ForeignKey("market_prediction.id"), index=True)
    fixture_id = db.Column(db.String(64), nullable=False, index=True)
    market_type = db.Column(db.String(100), nullable=False)
    line = db.Column(db.Float)
    side = db.Column(db.String(20))
    bookmaker = db.Column(db.String(120), nullable=False)
    odds_value = db.Column(db.Float, nullable=False)
    captured_at = db.Column(db.DateTime, nullable=False, index=True)
    fixture_kickoff = db.Column(db.DateTime, nullable=False)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)

    __table_args__ = (
        db.UniqueConstraint(
            "market_prediction_id", "bookmaker", "captured_at",
            name="uix_market_odds_prediction_bookmaker_capture",
        ),
        db.Index("ix_market_odds_fixture_market", "fixture_id", "market_type"),
    )


class TeamIdentity(db.Model):
    __tablename__ = "team_identity"

    id = db.Column(db.Integer, primary_key=True)
    canonical_name = db.Column(db.String(180), nullable=False)
    source = db.Column(db.String(40))
    external_id = db.Column(db.String(80))
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)

    __table_args__ = (
        db.UniqueConstraint("source", "external_id", name="uix_team_identity_source_external"),
    )


class TeamAlias(db.Model):
    __tablename__ = "team_alias"

    id = db.Column(db.Integer, primary_key=True)
    team_identity_id = db.Column(db.Integer, db.ForeignKey("team_identity.id"), nullable=False, index=True)
    source = db.Column(db.String(40), nullable=False)
    alias = db.Column(db.String(180), nullable=False)
    normalized_alias = db.Column(db.String(180), nullable=False)
    confirmed = db.Column(db.Boolean, default=False, nullable=False)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)

    team = db.relationship("TeamIdentity", backref=db.backref("aliases", cascade="all, delete-orphan"))

    __table_args__ = (
        db.UniqueConstraint("source", "normalized_alias", name="uix_team_alias_source_name"),
    )


class HistoricalMatch(db.Model):
    __tablename__ = "historical_match"

    id = db.Column(db.Integer, primary_key=True)
    source = db.Column(db.String(40), nullable=False)
    external_id = db.Column(db.String(80), nullable=False)
    source_url = db.Column(db.Text)
    kickoff_at = db.Column(db.DateTime)
    kickoff_original = db.Column(db.String(80))
    kickoff_timezone = db.Column(db.String(80))
    historical_date_available = db.Column(db.Boolean, default=False, nullable=False, index=True)
    league = db.Column(db.String(180))
    season = db.Column(db.String(80))
    home_team = db.Column(db.String(180))
    away_team = db.Column(db.String(180))
    home_team_identity_id = db.Column(db.Integer, db.ForeignKey("team_identity.id"))
    away_team_identity_id = db.Column(db.Integer, db.ForeignKey("team_identity.id"))
    home_score = db.Column(db.Integer)
    away_score = db.Column(db.Integer)
    status = db.Column(db.String(30))
    collected_at = db.Column(db.DateTime, default=now_sp, nullable=False)
    updated_at = db.Column(db.DateTime, default=now_sp, onupdate=now_sp, nullable=False)

    __table_args__ = (
        db.UniqueConstraint("source", "external_id", name="uix_historical_match_source_external"),
    )


class HistoricalMatchStat(db.Model):
    __tablename__ = "historical_match_stat"

    id = db.Column(db.Integer, primary_key=True)
    historical_match_id = db.Column(db.Integer, db.ForeignKey("historical_match.id"), nullable=False, index=True)
    period = db.Column(db.String(20), nullable=False, default="full_time")
    side = db.Column(db.String(20), nullable=False, default="total")
    stat_key = db.Column(db.String(80), nullable=False)
    value = db.Column(db.Float)
    available = db.Column(db.Boolean, default=False, nullable=False)
    source = db.Column(db.String(40), nullable=False, default="betsapi")
    collected_at = db.Column(db.DateTime, default=now_sp, nullable=False)

    match = db.relationship("HistoricalMatch", backref=db.backref("stats", cascade="all, delete-orphan"))

    __table_args__ = (
        db.UniqueConstraint(
            "historical_match_id", "period", "side", "stat_key", "source",
            name="uix_historical_match_stat_observation",
        ),
    )


class TeamStrengthSnapshot(db.Model):
    __tablename__ = "team_strength_snapshot"

    id = db.Column(db.Integer, primary_key=True)
    historical_match_id = db.Column(db.Integer, db.ForeignKey("historical_match.id"), nullable=False, index=True)
    team_identity_id = db.Column(db.Integer, db.ForeignKey("team_identity.id"), nullable=False, index=True)
    opponent_identity_id = db.Column(db.Integer, db.ForeignKey("team_identity.id"), nullable=False)
    competition_key = db.Column(db.String(180), nullable=False, index=True)
    model_version = db.Column(db.String(80), nullable=False, default="elo_v1")
    is_home = db.Column(db.Boolean, nullable=False)
    kickoff_at = db.Column(db.DateTime, nullable=False, index=True)
    pre_rating = db.Column(db.Float, nullable=False)
    opponent_pre_rating = db.Column(db.Float, nullable=False)
    expected_score = db.Column(db.Float, nullable=False)
    actual_score = db.Column(db.Float, nullable=False)
    post_rating = db.Column(db.Float, nullable=False)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)

    __table_args__ = (
        db.UniqueConstraint(
            "historical_match_id", "team_identity_id", "competition_key", "model_version",
            name="uix_team_strength_match_team_model",
        ),
    )


class BacktestRun(db.Model):
    __tablename__ = "backtest_run"

    id = db.Column(db.Integer, primary_key=True)
    run_type = db.Column(db.String(30), nullable=False, default="BACKTEST", index=True)
    legacy_model_version = db.Column(db.String(80), nullable=False, default="legacy_v1")
    shadow_model_version = db.Column(db.String(80), nullable=False, default="greenhunter_v2_shadow")
    methodology_version = db.Column(db.String(80), nullable=False, default="walk_forward_v1")
    development_end = db.Column(db.DateTime)
    evaluation_start = db.Column(db.DateTime)
    period_start = db.Column(db.DateTime)
    period_end = db.Column(db.DateTime)
    status = db.Column(db.String(30), nullable=False, default="running", index=True)
    parameters_json = db.Column(db.Text)
    metrics_json = db.Column(db.Text)
    fixture_count = db.Column(db.Integer, default=0, nullable=False)
    candidate_count = db.Column(db.Integer, default=0, nullable=False)
    resolved_count = db.Column(db.Integer, default=0, nullable=False)
    unresolved_count = db.Column(db.Integer, default=0, nullable=False)
    leakage_blocked_count = db.Column(db.Integer, default=0, nullable=False)
    duration_ms = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)
    finished_at = db.Column(db.DateTime)


class BacktestObservation(db.Model):
    __tablename__ = "backtest_observation"

    id = db.Column(db.Integer, primary_key=True)
    backtest_run_id = db.Column(db.Integer, db.ForeignKey("backtest_run.id"), nullable=False, index=True)
    logical_key = db.Column(db.String(255), nullable=False)
    historical_match_id = db.Column(db.Integer, db.ForeignKey("historical_match.id"), nullable=False, index=True)
    fixture_external_id = db.Column(db.String(80), nullable=False, index=True)
    kickoff_at = db.Column(db.DateTime, nullable=False, index=True)
    prediction_time = db.Column(db.DateTime, nullable=False)
    league = db.Column(db.String(180))
    market_family = db.Column(db.String(80), nullable=False, index=True)
    market_type = db.Column(db.String(100), nullable=False)
    line = db.Column(db.Float, nullable=False)
    scope = db.Column(db.String(20), nullable=False)
    period = db.Column(db.String(20), nullable=False, default="full_time")
    legacy_score = db.Column(db.Float)
    legacy_adjusted_probability = db.Column(db.Float)
    legacy_status = db.Column(db.String(50))
    v2_score = db.Column(db.Float)
    v2_calibrated_probability = db.Column(db.Float)
    calibration_source = db.Column(db.String(80))
    uncertainty_score = db.Column(db.Float)
    data_quality_v2 = db.Column(db.Float)
    raw_frequency = db.Column(db.Float)
    recent_frequency = db.Column(db.Float)
    effective_sample_size = db.Column(db.Float)
    team_strength = db.Column(db.Float)
    opponent_strength = db.Column(db.Float)
    strength_delta = db.Column(db.Float)
    production_avg = db.Column(db.Float)
    concession_avg = db.Column(db.Float)
    matchup_available = db.Column(db.Boolean, default=False, nullable=False)
    strength_available = db.Column(db.Boolean, default=False, nullable=False)
    temporal_confidence = db.Column(db.String(20), nullable=False)
    max_source_timestamp = db.Column(db.DateTime)
    settlement = db.Column(db.String(30), nullable=False, index=True)
    actual_value = db.Column(db.Float)
    target = db.Column(db.Integer)
    feature_snapshot_json = db.Column(db.Text, nullable=False)
    ablation_scores_json = db.Column(db.Text, nullable=False)
    leakage_reason = db.Column(db.String(180))
    prospective_shadow = db.Column(db.Boolean, default=False, nullable=False)
    dataset_partition = db.Column(db.String(20), nullable=False, default="evaluation", index=True)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)

    __table_args__ = (
        db.UniqueConstraint("backtest_run_id", "logical_key", name="uix_backtest_run_logical_key"),
        db.Index("ix_backtest_market_settlement", "market_family", "settlement"),
    )


class LoginAttempt(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80))
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"))
    ip_address = db.Column(db.String(64))
    success = db.Column(db.Boolean, default=False, nullable=False)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)

    __table_args__ = (
        db.Index("ix_login_attempt_ip_success_created", "ip_address", "success", "created_at"),
    )


class AdminBroadcast(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    message = db.Column(db.Text, nullable=False)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)


class AdminBroadcastView(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    broadcast_id = db.Column(db.Integer, db.ForeignKey("admin_broadcast.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    seen_at = db.Column(db.DateTime, default=now_sp, nullable=False)

    __table_args__ = (
        db.UniqueConstraint("broadcast_id", "user_id", name="uix_broadcast_user"),
    )


class UndoAction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    token = db.Column(db.String(64), unique=True, nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    action_type = db.Column(db.String(64), nullable=False)
    payload_json = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=now_sp, nullable=False)
    expires_at = db.Column(db.DateTime, nullable=False, index=True)
    used_at = db.Column(db.DateTime)
