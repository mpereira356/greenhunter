import os

from sqlalchemy import text

from dotenv import load_dotenv
from flask import Flask
from flask_login import current_user

from .extensions import db, login_manager
from .models import AdminBroadcast, AdminBroadcastView, User
from .services.worker import start_worker


def create_app():
    load_dotenv()
    app = Flask(__name__)

    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret")
    default_db_path = os.path.abspath(os.path.join(app.root_path, "..", "data", "app.db"))
    app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get(
        "DATABASE_URL", f"sqlite:///{default_db_path}"
    )
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    db.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = "auth.login"

    from .auth.routes import auth_bp
    from .rules.routes import rules_bp
    from .history.routes import history_bp
    from .settings.routes import settings_bp
    from .main.routes import main_bp
    from .admin.routes import admin_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(rules_bp)
    app.register_blueprint(history_bp)
    app.register_blueprint(settings_bp)
    app.register_blueprint(main_bp)
    app.register_blueprint(admin_bp)

    with app.app_context():
        os.makedirs("data", exist_ok=True)
        os.makedirs("data/exports", exist_ok=True)
        db.create_all()
        _ensure_user_columns()
        _ensure_rule_columns()
        _ensure_rule_condition_columns()
        _ensure_alert_columns()

    if os.environ.get("DISABLE_WORKER") != "1":
        start_worker(app)

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
            broadcast_id=broadcast.id, user_id=current_user.id
        ).first()
        if seen:
            return {"active_broadcast": None}
        db.session.add(AdminBroadcastView(broadcast_id=broadcast.id, user_id=current_user.id))
        db.session.commit()
        return {"active_broadcast": broadcast}

    return app


@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


def _ensure_rule_columns():
    columns = {
        "last_checked_at": "DATETIME",
        "last_match_desc": "VARCHAR(255)",
        "last_alert_at": "DATETIME",
        "last_alert_desc": "VARCHAR(255)",
        "outcome_green_stage": "VARCHAR(5)",
        "outcome_red_stage": "VARCHAR(5)",
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
