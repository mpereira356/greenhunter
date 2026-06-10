import os
import resource
import sqlite3

from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
from flask import Flask
from flask_login import current_user

from app.extensions import db, login_manager
from app.models import AdminBroadcast, AdminBroadcastView, User
from app.services.worker import start_worker
from app.security import init_security
from app.utils.db import commit_with_retry


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    if not isinstance(dbapi_connection, sqlite3.Connection):
        return
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute(f"PRAGMA busy_timeout={int(os.environ.get('SQLITE_BUSY_TIMEOUT_MS', '30000'))}")
    cursor.close()


def create_app():
    # =========================
    # Carrega variáveis do .env
    # =========================
    load_dotenv()

    # =========================
    # Limite de memória para evitar travamento
    # =========================
    # Limita a ~1GB por processo sem reduzir o hard limit herdado por subprocessos.
    mem_limit = 1024 * 1024 * 1024
    _, hard_limit = resource.getrlimit(resource.RLIMIT_AS)
    soft_limit = min(mem_limit, hard_limit) if hard_limit != resource.RLIM_INFINITY else mem_limit
    resource.setrlimit(resource.RLIMIT_AS, (soft_limit, hard_limit))

    app = Flask(__name__)

    # =========================
    # Configurações principais
    # =========================
    secret_key = os.environ.get("SECRET_KEY", "")
    if len(secret_key) < 32 or secret_key in {"change-me", "dev-secret"}:
        raise RuntimeError("SECRET_KEY deve ser aleatoria e possuir pelo menos 32 caracteres.")
    app.config["SECRET_KEY"] = secret_key
    app.config["MAX_CONTENT_LENGTH"] = int(os.environ.get("MAX_CONTENT_LENGTH", str(160 * 1024 * 1024)))
    app.config["DEFAULT_REQUEST_CONTENT_LENGTH"] = int(
        os.environ.get("DEFAULT_REQUEST_CONTENT_LENGTH", str(1024 * 1024))
    )
    app.config["LOGIN_MAX_CONTENT_LENGTH"] = int(os.environ.get("LOGIN_MAX_CONTENT_LENGTH", str(64 * 1024)))
    app.config["LOGIN_MAX_ATTEMPTS"] = int(os.environ.get("LOGIN_MAX_ATTEMPTS", "8"))
    app.config["LOGIN_WINDOW_SECONDS"] = int(os.environ.get("LOGIN_WINDOW_SECONDS", "300"))
    app.config["LOGIN_BLOCK_SECONDS"] = int(os.environ.get("LOGIN_BLOCK_SECONDS", "900"))
    app.config["TRUSTED_PROXY_IPS"] = {
        value.strip()
        for value in os.environ.get("TRUSTED_PROXY_IPS", "127.0.0.1,::1").split(",")
        if value.strip()
    }
    app.config["GLOBAL_RATE_LIMIT"] = int(os.environ.get("GLOBAL_RATE_LIMIT", "240"))
    app.config["GLOBAL_RATE_WINDOW_SECONDS"] = int(os.environ.get("GLOBAL_RATE_WINDOW_SECONDS", "60"))
    app.config["ALLOWED_HOSTS"] = {
        value.strip().lower()
        for value in os.environ.get("ALLOWED_HOSTS", "").split(",")
        if value.strip()
    }
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = os.environ.get("SESSION_COOKIE_SECURE", "0") == "1"
    app.config["PERMANENT_SESSION_LIFETIME"] = int(os.environ.get("SESSION_LIFETIME_SECONDS", "43200"))

    # Diretório base do projeto (Windows e Linux)
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    # Pastas de dados
    DATA_DIR = os.path.join(BASE_DIR, "data")
    EXPORTS_DIR = os.path.join(DATA_DIR, "exports")

    # Garante que as pastas existam
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(EXPORTS_DIR, exist_ok=True)

    # =========================
    # Banco de dados SQLite
    # =========================
    default_db_path = os.path.join(DATA_DIR, "app.db")

    # Corrige caminho do Windows para formato SQLite
    db_path = default_db_path.replace("\\", "/")

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        database_url = f"sqlite:///{db_path}"

    app.config["SQLALCHEMY_DATABASE_URI"] = database_url
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    engine_options = {"pool_pre_ping": True}
    if str(database_url).startswith("sqlite"):
        engine_options["connect_args"] = {
            "timeout": float(os.environ.get("SQLITE_CONNECT_TIMEOUT_SECONDS", "30")),
            "check_same_thread": False,
        }
    app.config["SQLALCHEMY_ENGINE_OPTIONS"] = engine_options

    # =========================
    # Inicializações
    # =========================
    db.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = "auth.login"
    login_manager.session_protection = "strong"
    init_security(app)

    # =========================
    # Blueprints
    # =========================
    from app.auth.routes import auth_bp
    from app.rules.routes import rules_bp
    from app.history.routes import history_bp
    from app.settings.routes import settings_bp
    from app.main.routes import main_bp
    from app.admin.routes import admin_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(rules_bp)
    app.register_blueprint(history_bp)
    app.register_blueprint(settings_bp)
    app.register_blueprint(main_bp)
    app.register_blueprint(admin_bp)

    # =========================
    # Banco de dados
    # =========================
    with app.app_context():
        db.create_all()
        _ensure_user_columns()
        _ensure_rule_columns()
        _ensure_rule_condition_columns()
        _ensure_rule_outcome_condition_columns()
        _ensure_alert_columns()
        _ensure_live_game_state_columns()
        _ensure_login_attempt_indexes()

    # =========================
    # Worker (opcional)
    # =========================
    if os.environ.get("DISABLE_WORKER") != "1":
        is_reloader_main = os.environ.get("WERKZEUG_RUN_MAIN") == "true"
        if not app.debug or is_reloader_main:
            start_worker(app)

    # =========================
    # Broadcast para usuários
    # =========================
    @app.context_processor
    def inject_broadcast():
        if not getattr(current_user, "is_authenticated", False):
            return {"active_broadcast": None}

        broadcast = (
            AdminBroadcast.query.filter_by(is_active=True)
            .order_by(AdminBroadcast.created_at.desc())
            .first()
        )

        if not broadcast:
            return {"active_broadcast": None}

        seen = AdminBroadcastView.query.filter_by(
            broadcast_id=broadcast.id,
            user_id=current_user.id
        ).first()

        if seen:
            return {"active_broadcast": None}

        db.session.add(AdminBroadcastView(broadcast_id=broadcast.id, user_id=current_user.id))
        commit_with_retry()

        return {"active_broadcast": broadcast}

    return app


# =========================
# Login Loader
# =========================
@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


# =========================
# Migração leve (SQLite)
# =========================
def _ensure_rule_columns():
    columns = {
        "last_checked_at": "DATETIME",
        "last_match_desc": "VARCHAR(255)",
        "last_alert_at": "DATETIME",
        "last_alert_desc": "VARCHAR(255)",
        "outcome_green_stage": "VARCHAR(5)",
        "outcome_red_stage": "VARCHAR(5)",
        "outcome_green_minute": "INTEGER",
        "outcome_red_minute": "INTEGER",
        "outcome_red_if_no_green": "BOOLEAN DEFAULT 0",
        "notify_telegram": "BOOLEAN DEFAULT 1",
        "alert_on_penalty": "BOOLEAN DEFAULT 0",
        "score_home": "INTEGER",
        "score_away": "INTEGER",
        "second_half_only": "BOOLEAN DEFAULT 0",
        "allowed_leagues_json": "TEXT",
    }

    with db.engine.connect() as conn:
        result = conn.execute(text("PRAGMA table_info('rule')"))
        existing = {row[1] for row in result}

        for col, col_type in columns.items():
            if col not in existing:
                conn.execute(text(f"ALTER TABLE rule ADD COLUMN {col} {col_type}"))

        conn.commit()


def _ensure_rule_condition_columns():
    with db.engine.connect() as conn:
        result = conn.execute(text("PRAGMA table_info('rule_condition')"))
        existing = {row[1] for row in result}

        if "group_id" not in existing:
            conn.execute(text("ALTER TABLE rule_condition ADD COLUMN group_id INTEGER DEFAULT 0"))

        conn.commit()


def _ensure_rule_outcome_condition_columns():
    with db.engine.connect() as conn:
        result = conn.execute(text("PRAGMA table_info('rule_outcome_condition')"))
        existing = {row[1] for row in result}

        if "group_id" not in existing:
            conn.execute(text("ALTER TABLE rule_outcome_condition ADD COLUMN group_id INTEGER DEFAULT 0"))

        conn.commit()


def _ensure_user_columns():
    columns = {
        "email": "VARCHAR(120)",
        "is_admin": "BOOLEAN DEFAULT 0",
        "telegram_verified": "BOOLEAN DEFAULT 0",
        "subscription_plan": "VARCHAR(20) DEFAULT 'starter'",
        "rule_limit": "INTEGER DEFAULT 2",
        "paid_until": "DATETIME",
        "trial_until": "DATETIME",
        "favorite_live_leagues_json": "TEXT",
    }

    with db.engine.connect() as conn:
        result = conn.execute(text("PRAGMA table_info('user')"))
        existing = {row[1] for row in result}

        for col, col_type in columns.items():
            if col not in existing:
                conn.execute(text(f"ALTER TABLE user ADD COLUMN {col} {col_type}"))

        conn.commit()


def _ensure_alert_columns():
    columns = {
        "result_minute": "INTEGER",
        "result_time_hhmm": "VARCHAR(8)",
        "last_score": "VARCHAR(20)",
        "last_score_minute": "INTEGER",
        "penalty_last_total": "INTEGER DEFAULT 0",
        "penalty_notified": "BOOLEAN DEFAULT 0",
        "penalty_baseline_set": "BOOLEAN DEFAULT 0",
        "ml_pred_score": "INTEGER",
        "ml_pred_verdict": "VARCHAR(40)",
        "ml_pred_prob_green": "FLOAT",
        "ml_model_samples": "INTEGER",
        "ml_model_trained_at": "VARCHAR(32)",
        "ai_score": "INTEGER",
        "ai_verdict": "VARCHAR(40)",
        "ai_commentary": "TEXT",
        "market_key": "VARCHAR(64)",
        "market_label": "VARCHAR(120)",
        "outcome_signature": "TEXT",
        "target_side": "VARCHAR(20)",
        "target_operator": "VARCHAR(8)",
        "target_value": "INTEGER",
        "target_text": "VARCHAR(255)",
        "initial_events_json": "TEXT",
        "result_events_json": "TEXT",
        "ft_events_json": "TEXT",
        "initial_event_metrics_json": "TEXT",
        "result_event_metrics_json": "TEXT",
        "ft_event_metrics_json": "TEXT",
        "telegram_entry_message_id": "INTEGER",
        "telegram_entry_enriched": "BOOLEAN DEFAULT 0",
        "stake_amount": "FLOAT",
        "stake_odd": "FLOAT",
        "bet_note": "TEXT",
        "bet_recorded_at": "DATETIME",
    }

    with db.engine.connect() as conn:
        result = conn.execute(text("PRAGMA table_info('match_alert')"))
        existing = {row[1] for row in result}

        for col, col_type in columns.items():
            if col not in existing:
                conn.execute(text(f"ALTER TABLE match_alert ADD COLUMN {col} {col_type}"))

        conn.commit()


def _ensure_live_game_state_columns():
    columns = {
        "first_half_snapshot_json": "TEXT",
        "first_half_snapshot_minute": "INTEGER",
        "events_json": "TEXT",
    }

    with db.engine.connect() as conn:
        result = conn.execute(text("PRAGMA table_info('live_game_state')"))
        existing = {row[1] for row in result}

        for col, col_type in columns.items():
            if col not in existing:
                conn.execute(text(f"ALTER TABLE live_game_state ADD COLUMN {col} {col_type}"))

        conn.commit()


def _ensure_login_attempt_indexes():
    with db.engine.connect() as conn:
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_login_attempt_ip_success_created "
            "ON login_attempt (ip_address, success, created_at)"
        ))
        conn.commit()
