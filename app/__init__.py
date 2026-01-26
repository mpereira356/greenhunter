import os
from sqlalchemy import text
from dotenv import load_dotenv
from flask import Flask
from flask_login import current_user

from app.extensions import db, login_manager
from app.models import AdminBroadcast, AdminBroadcastView, User
from app.services.worker import start_worker


def create_app():
    # =========================
    # Carrega variáveis do .env
    # =========================
    load_dotenv()

    app = Flask(__name__)

    # =========================
    # Configurações principais
    # =========================
    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret")

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

    # =========================
    # Inicializações
    # =========================
    db.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = "auth.login"

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
        _ensure_alert_columns()

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
        db.session.commit()

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
        "score_home": "INTEGER",
        "score_away": "INTEGER",
        "second_half_only": "BOOLEAN DEFAULT 0",
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


def _ensure_user_columns():
    columns = {
        "email": "VARCHAR(120)",
        "is_admin": "BOOLEAN DEFAULT 0",
        "telegram_verified": "BOOLEAN DEFAULT 0",
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
    }

    with db.engine.connect() as conn:
        result = conn.execute(text("PRAGMA table_info('match_alert')"))
        existing = {row[1] for row in result}

        for col, col_type in columns.items():
            if col not in existing:
                conn.execute(text(f"ALTER TABLE match_alert ADD COLUMN {col} {col_type}"))

        conn.commit()
