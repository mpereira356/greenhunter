from datetime import datetime, timedelta
from glob import glob
import json
import os
import sqlite3
import tempfile

from flask import Blueprint, abort, after_this_request, current_app, flash, jsonify, redirect, render_template, request, send_file, url_for
from flask_login import current_user, login_required
from sqlalchemy import case, func, or_
from sqlalchemy.engine.url import make_url

from ..extensions import db
from ..models import AdminBroadcast, LiveGameState, LoginAttempt, MatchAlert, Rule, RuleCondition, User
from ..security import safe_redirect_target
from ..services.scraper import is_first_half_extra_time
from ..services.telegram import send_message
from ..services.worker import get_api_status
from ..utils.time import now_sp

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")

ALERTS_PER_HOUR_THRESHOLD = 20
PLAN_RULE_LIMITS = {
    "starter": 2,
    "pro": 10,
    "custom": 50,
}


def _format_age(value: datetime | None, now: datetime) -> str:
    if not value:
        return "-"
    delta = now - value
    total_seconds = max(0, int(delta.total_seconds()))
    if total_seconds < 60:
        return f"{total_seconds}s"
    minutes = total_seconds // 60
    if minutes < 60:
        return f"{minutes}min"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h"
    return f"{hours // 24}d"


def _last_successful_logins() -> dict[int, datetime]:
    return {
        row.user_id: row.last_login
        for row in db.session.query(
            LoginAttempt.user_id,
            func.max(LoginAttempt.created_at).label("last_login"),
        )
        .filter(LoginAttempt.success.is_(True), LoginAttempt.user_id.isnot(None))
        .group_by(LoginAttempt.user_id)
        .all()
    }


def _database_info() -> dict:
    try:
        db_path = _sqlite_db_path()
    except RuntimeError:
        return {"available": False, "size_mb": "-", "backup_count": 0, "path": "-"}

    size_mb = "-"
    if os.path.exists(db_path):
        size_mb = round(os.path.getsize(db_path) / (1024 * 1024), 2)
    backup_count = len(glob(f"{db_path}.backup_*"))
    return {
        "available": os.path.exists(db_path),
        "size_mb": size_mb,
        "backup_count": backup_count,
        "path": db_path,
    }


def _sqlite_db_path() -> str:
    database_url = current_app.config.get("SQLALCHEMY_DATABASE_URI", "")
    url = make_url(database_url)
    if url.drivername not in {"sqlite", "sqlite+pysqlite"}:
        raise RuntimeError("Backup de DB disponivel apenas para SQLite.")
    if not url.database:
        raise RuntimeError("Caminho do banco SQLite nao encontrado.")
    return os.path.abspath(url.database)


def _validate_sqlite_file(path: str) -> None:
    try:
        conn = sqlite3.connect(path)
        try:
            result = conn.execute("PRAGMA integrity_check").fetchone()
            if not result or result[0] != "ok":
                raise ValueError("integrity_check falhou.")
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            required_tables = {"user", "rule", "match_alert"}
            missing = sorted(required_tables - tables)
            if missing:
                raise ValueError(f"tabelas obrigatorias ausentes: {', '.join(missing)}")
        finally:
            conn.close()
    except sqlite3.DatabaseError as exc:
        raise ValueError("arquivo enviado nao e um SQLite valido.") from exc


def _snapshot_sqlite_db(source_path: str, destination_path: str) -> None:
    source = sqlite3.connect(source_path)
    try:
        destination = sqlite3.connect(destination_path)
        try:
            source.backup(destination)
        finally:
            destination.close()
    finally:
        source.close()


def _replace_sqlite_db(upload_path: str, db_path: str) -> str:
    backup_path = f"{db_path}.backup_{now_sp().strftime('%Y%m%d_%H%M%S')}"

    if os.path.exists(db_path):
        _snapshot_sqlite_db(db_path, backup_path)

    db.session.remove()
    db.engine.dispose()
    os.replace(upload_path, db_path)
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = f"{db_path}{suffix}"
        if os.path.exists(sidecar):
            os.remove(sidecar)
    return backup_path


def _require_admin():
    if not current_user.is_authenticated or not current_user.is_admin_user:
        abort(403)


def _stat_total(payload: str | None, key: str) -> int | None:
    if not payload:
        return None
    try:
        data = json.loads(payload)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    value = data.get(key)
    if not isinstance(value, dict):
        return None
    total = value.get("total")
    return total if isinstance(total, int) else None


def _stat_pair(payload: str | None, key: str) -> str:
    if not payload:
        return "- x -"
    try:
        data = json.loads(payload)
    except Exception:
        return "- x -"
    value = data.get(key) if isinstance(data, dict) else None
    if not isinstance(value, dict):
        return "- x -"
    home = value.get("home")
    away = value.get("away")
    home_txt = str(home) if isinstance(home, int) else "-"
    away_txt = str(away) if isinstance(away, int) else "-"
    return f"{home_txt} x {away_txt}"


def _stat_delta_pair(
    current_payload: str | None,
    baseline_payload: str | None,
    key: str,
    show_total_when_zero: bool = False,
    show_total_first: bool = False,
) -> str:
    def _pair(payload: str | None):
        if not payload:
            return None, None
        try:
            data = json.loads(payload)
        except Exception:
            return None, None
        value = data.get(key) if isinstance(data, dict) else None
        if not isinstance(value, dict):
            return None, None
        home = value.get("home")
        away = value.get("away")
        return (home if isinstance(home, int) else None, away if isinstance(away, int) else None)

    cur_home, cur_away = _pair(current_payload)
    base_home, base_away = _pair(baseline_payload)
    if cur_home is None or cur_away is None:
        return "- x -"
    if base_home is None or base_away is None:
        return f"{cur_home} x {cur_away}"

    # If provider resets stats at HT, values may drop below baseline; in that case use current.
    home_delta = cur_home - base_home if cur_home >= base_home else cur_home
    away_delta = cur_away - base_away if cur_away >= base_away else cur_away
    home_delta = max(0, home_delta)
    away_delta = max(0, away_delta)
    if show_total_first and (cur_home is not None or cur_away is not None):
        if home_delta == 0 and away_delta == 0:
            return f"T {cur_home} x {cur_away}"
        return f"T {cur_home} x {cur_away} (Δ {home_delta} x {away_delta})"
    if show_total_when_zero and home_delta == 0 and away_delta == 0 and (cur_home or cur_away):
        return f"{home_delta} x {away_delta} (T {cur_home} x {cur_away})"
    return f"{home_delta} x {away_delta}"


def _build_tracked_games(now: datetime) -> list[dict]:
    tracked_games = []
    recent_window = now - timedelta(minutes=20)
    live_rows = (
        LiveGameState.query.filter(LiveGameState.updated_at >= recent_window)
        .order_by(LiveGameState.minute.desc(), LiveGameState.updated_at.desc())
        .all()
    )
    for row in live_rows:
        minute = row.minute if isinstance(row.minute, int) else None
        if minute is None or minute < 46:
            continue
        if is_first_half_extra_time(row.time_text or ""):
            continue

        tracked_games.append(
            {
                "game_id": row.game_id,
                "teams": f"{row.home_team} vs {row.away_team}",
                "url": row.url or (f"https://betsapi.com/r/{row.game_id}" if row.game_id else ""),
                "minute": minute,
                "time_text": row.time_text,
                "on_target_live": _stat_delta_pair(
                    row.stats_json,
                    row.first_half_snapshot_json or row.second_half_baseline_json,
                    "On Target",
                    show_total_first=True,
                ),
                "corners_live": _stat_delta_pair(
                    row.stats_json,
                    row.first_half_snapshot_json or row.second_half_baseline_json,
                    "Corners",
                    show_total_first=True,
                ),
                "dangerous_live": _stat_delta_pair(
                    row.stats_json,
                    row.first_half_snapshot_json or row.second_half_baseline_json,
                    "Dangerous Attacks",
                    show_total_first=True,
                ),
                "updated_at": row.updated_at,
                "updated_at_fmt": row.updated_at.strftime("%d/%m %H:%M:%S") if row.updated_at else "-",
            }
        )
    return tracked_games


@admin_bp.route("/")
@login_required
def dashboard():
    _require_admin()
    now = now_sp()
    start_day = datetime(now.year, now.month, now.day)
    end_day = start_day + timedelta(days=1)

    total_users = User.query.count()
    total_rules = Rule.query.count()
    active_rules = Rule.query.filter_by(is_active=True).count()
    alerts_today = (
        MatchAlert.query.filter(MatchAlert.created_at >= start_day, MatchAlert.created_at < end_day)
        .count()
    )
    greens_today = (
        MatchAlert.query.filter(MatchAlert.status == "green")
        .filter(MatchAlert.created_at >= start_day, MatchAlert.created_at < end_day)
        .count()
    )
    reds_today = (
        MatchAlert.query.filter(MatchAlert.status == "red")
        .filter(MatchAlert.created_at >= start_day, MatchAlert.created_at < end_day)
        .count()
    )
    pending_alerts = MatchAlert.query.filter_by(status="pending").count()
    settled_today = greens_today + reds_today
    win_rate_today = round((greens_today / settled_today) * 100, 1) if settled_today else 0

    latest_live_at = db.session.query(func.max(LiveGameState.updated_at)).scalar()
    latest_alert_at = db.session.query(func.max(MatchAlert.created_at)).scalar()
    fresh_live_since = now - timedelta(minutes=5)
    live_games_fresh = LiveGameState.query.filter(LiveGameState.updated_at >= fresh_live_since).count()
    live_games_total = LiveGameState.query.count()
    stale_live_games = max(0, live_games_total - live_games_fresh)
    exact_score_rules = (
        Rule.query.filter(Rule.is_active == True)
        .filter((Rule.score_home.isnot(None)) | (Rule.score_away.isnot(None)))
        .count()
    )
    system_health = {
        "api_status": get_api_status(),
        "latest_live_at": latest_live_at,
        "latest_live_age": _format_age(latest_live_at, now),
        "latest_alert_at": latest_alert_at,
        "latest_alert_age": _format_age(latest_alert_at, now),
        "live_games_fresh": live_games_fresh,
        "live_games_total": live_games_total,
        "stale_live_games": stale_live_games,
        "pending_alerts": pending_alerts,
        "exact_score_rules": exact_score_rules,
        "win_rate_today": win_rate_today,
    }

    plan_stats = {
        "starter": 0,
        "pro": 0,
        "custom": 0,
    }
    for plan, total in (
        db.session.query(User.subscription_plan, func.count(User.id))
        .group_by(User.subscription_plan)
        .all()
    ):
        plan_key = (plan or "starter").lower()
        plan_stats[plan_key] = plan_stats.get(plan_key, 0) + total

    rule_counts = {
        row.user_id: {"rules": row.rules, "active_rules": row.active_rules}
        for row in db.session.query(
            Rule.user_id,
            func.count(Rule.id).label("rules"),
            func.sum(case((Rule.is_active == True, 1), else_=0)).label("active_rules"),
        )
        .group_by(Rule.user_id)
        .all()
    }

    alert_counts = {
        row.user_id: {"alerts": row.alerts, "last_alert": row.last_alert}
        for row in db.session.query(
            MatchAlert.user_id,
            func.count(MatchAlert.id).label("alerts"),
            func.max(MatchAlert.created_at).label("last_alert"),
        )
        .group_by(MatchAlert.user_id)
        .all()
    }
    last_logins = _last_successful_logins()

    users = []
    for user in User.query.order_by(User.created_at.desc()).all():
        counts = rule_counts.get(user.id, {"rules": 0, "active_rules": 0})
        alerts = alert_counts.get(user.id, {"alerts": 0, "last_alert": None})
        users.append(
            {
                "user": user,
                "rules": counts["rules"] or 0,
                "active_rules": counts["active_rules"] or 0,
                "alerts": alerts["alerts"] or 0,
                "last_alert": alerts["last_alert"],
                "last_login": last_logins.get(user.id),
            }
        )

    rule_stats = {}
    recent_all = (
        MatchAlert.query.filter(MatchAlert.status.in_(["green", "red"]))
        .order_by(MatchAlert.created_at.desc())
        .limit(500)
        .all()
    )
    for alert in recent_all:
        rule_stats.setdefault(alert.rule_id, {"rule": alert.rule, "green": 0, "red": 0})
        if alert.status == "green":
            rule_stats[alert.rule_id]["green"] += 1
        elif alert.status == "red":
            rule_stats[alert.rule_id]["red"] += 1

    top_rules = []
    for stats in rule_stats.values():
        total = stats["green"] + stats["red"]
        if total == 0:
            continue
        win_rate = round((stats["green"] / total) * 100, 1)
        top_rules.append(
            {
                "rule": stats["rule"],
                "win_rate": win_rate,
                "total": total,
                "green": stats["green"],
                "red": stats["red"],
            }
        )
    top_rules.sort(key=lambda x: x["win_rate"], reverse=True)
    top_rules = top_rules[:8]

    since_hour = now - timedelta(hours=1)
    risk_rows = (
        db.session.query(
            MatchAlert.user_id,
            func.count(MatchAlert.id).label("alerts"),
        )
        .filter(MatchAlert.created_at >= since_hour)
        .group_by(MatchAlert.user_id)
        .having(func.count(MatchAlert.id) >= ALERTS_PER_HOUR_THRESHOLD)
        .all()
    )
    risk_users = []
    for row in risk_rows:
        user = User.query.get(row.user_id)
        if not user:
            continue
        risk_users.append({"user": user, "alerts": row.alerts})

    tracked_games = _build_tracked_games(now)
    recent_alerts = (
        MatchAlert.query.order_by(MatchAlert.created_at.desc())
        .limit(10)
        .all()
    )

    return render_template(
        "admin/dashboard.html",
        total_users=total_users,
        total_rules=total_rules,
        active_rules=active_rules,
        alerts_today=alerts_today,
        greens_today=greens_today,
        reds_today=reds_today,
        pending_alerts=pending_alerts,
        win_rate_today=win_rate_today,
        users=users,
        top_rules=top_rules,
        risk_users=risk_users,
        tracked_games=tracked_games,
        recent_alerts=recent_alerts,
        system_health=system_health,
        plan_stats=plan_stats,
        database_info=_database_info(),
        alerts_per_hour_threshold=ALERTS_PER_HOUR_THRESHOLD,
        login_attempts=LoginAttempt.query.order_by(LoginAttempt.created_at.desc()).limit(20).all(),
        broadcasts=AdminBroadcast.query.order_by(AdminBroadcast.created_at.desc()).limit(5).all(),
        api_status=system_health["api_status"],
    )


@admin_bp.route("/live-monitor")
@login_required
def live_monitor():
    _require_admin()
    now = now_sp()
    return jsonify(
        {
            "ok": True,
            "server_time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "games": _build_tracked_games(now),
        }
    )


@admin_bp.route("/database/download")
@login_required
def download_database():
    _require_admin()
    try:
        db_path = _sqlite_db_path()
    except RuntimeError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("admin.dashboard"))

    if not os.path.exists(db_path):
        flash("Banco de dados nao encontrado.", "danger")
        return redirect(url_for("admin.dashboard"))

    db.session.remove()
    filename = f"greenhunter-db-{now_sp().strftime('%Y%m%d-%H%M%S')}.db"
    fd, snapshot_path = tempfile.mkstemp(prefix="greenhunter_download_", suffix=".db")
    os.close(fd)
    try:
        _snapshot_sqlite_db(db_path, snapshot_path)
    except Exception:
        if os.path.exists(snapshot_path):
            os.remove(snapshot_path)
        current_app.logger.exception("Falha ao gerar snapshot do banco de dados.")
        flash("Nao foi possivel gerar o download do banco de dados.", "danger")
        return redirect(url_for("admin.dashboard"))

    @after_this_request
    def _cleanup_snapshot(response):
        try:
            if os.path.exists(snapshot_path):
                os.remove(snapshot_path)
        except OSError:
            current_app.logger.exception("Falha ao remover snapshot temporario do banco.")
        return response

    return send_file(
        snapshot_path,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.sqlite3",
        max_age=0,
    )


@admin_bp.route("/database/import", methods=["POST"])
@login_required
def import_database():
    _require_admin()
    uploaded = request.files.get("database_file")
    if not uploaded or not uploaded.filename:
        flash("Selecione um arquivo de banco de dados para importar.", "warning")
        return redirect(url_for("admin.dashboard"))

    try:
        db_path = _sqlite_db_path()
    except RuntimeError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("admin.dashboard"))

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix="db_import_", suffix=".db", dir=os.path.dirname(db_path))
    os.close(fd)
    try:
        uploaded.save(temp_path)
        _validate_sqlite_file(temp_path)
        backup_path = _replace_sqlite_db(temp_path, db_path)
    except ValueError as exc:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        flash(f"Importacao cancelada: {exc}", "danger")
        return redirect(url_for("admin.dashboard"))
    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        current_app.logger.exception("Falha ao importar banco de dados.")
        flash("Nao foi possivel importar o banco de dados.", "danger")
        return redirect(url_for("admin.dashboard"))

    flash(f"Banco de dados importado. Backup anterior salvo em {backup_path}.", "success")
    return redirect(url_for("admin.dashboard"))


@admin_bp.route("/users")
@login_required
def users_list():
    _require_admin()
    search_query = (request.args.get("q") or "").strip()
    rule_counts = {
        row.user_id: row.rules
        for row in db.session.query(
            Rule.user_id, func.count(Rule.id).label("rules")
        )
        .group_by(Rule.user_id)
        .all()
    }
    alert_counts = {
        row.user_id: row.alerts
        for row in db.session.query(
            MatchAlert.user_id, func.count(MatchAlert.id).label("alerts")
        )
        .group_by(MatchAlert.user_id)
        .all()
    }
    last_logins = _last_successful_logins()
    user_query = User.query
    if search_query:
        filters = [User.username.ilike(f"%{search_query}%")]
        if search_query.isdigit():
            filters.append(User.id == int(search_query))
        user_query = user_query.filter(or_(*filters))

    users = []
    for user in user_query.order_by(User.created_at.desc()).all():
        users.append(
            {
                "user": user,
                "rules": rule_counts.get(user.id, 0),
                "alerts": alert_counts.get(user.id, 0),
                "last_login": last_logins.get(user.id),
            }
        )
    return render_template(
        "admin/users.html",
        users=users,
        search_query=search_query,
    )


@admin_bp.route("/users/<int:user_id>")
@login_required
def user_detail(user_id):
    _require_admin()
    user = User.query.get_or_404(user_id)
    rules = Rule.query.filter_by(user_id=user.id).order_by(Rule.id.desc()).all()

    rule_stats = {rule.id: {"green": 0, "red": 0} for rule in rules}
    counts = (
        db.session.query(MatchAlert.rule_id, MatchAlert.status, func.count(MatchAlert.id))
        .filter(MatchAlert.user_id == user.id)
        .group_by(MatchAlert.rule_id, MatchAlert.status)
        .all()
    )
    for rule_id, status, total in counts:
        if rule_id in rule_stats and status in ("green", "red"):
            rule_stats[rule_id][status] = total

    recent_alerts = (
        MatchAlert.query.filter_by(user_id=user.id)
        .order_by(MatchAlert.created_at.desc())
        .limit(20)
        .all()
    )
    return render_template(
        "admin/user_detail.html",
        user=user,
        rules=rules,
        rule_stats=rule_stats,
        recent_alerts=recent_alerts,
    )


@admin_bp.route("/users/<int:user_id>/edit", methods=["GET", "POST"])
@login_required
def edit_user(user_id):
    _require_admin()
    user = User.query.get_or_404(user_id)
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        username_normalized = username.lower()
        email = request.form.get("email", "").strip()
        is_admin = bool(request.form.get("is_admin"))
        new_password = request.form.get("new_password", "").strip()
        subscription_plan = (request.form.get("subscription_plan") or "starter").strip().lower()
        rule_limit_raw = (request.form.get("rule_limit") or "").strip()
        paid_days_raw = (request.form.get("paid_days") or "").strip()
        trial_days_raw = (request.form.get("trial_days") or "").strip()
        clear_paid = bool(request.form.get("clear_paid"))
        clear_trial = bool(request.form.get("clear_trial"))

        if not username_normalized:
            flash("Usuario e obrigatorio.", "warning")
            return render_template("admin/user_edit.html", user=user)
        if subscription_plan not in PLAN_RULE_LIMITS:
            subscription_plan = "starter"
        if rule_limit_raw.isdigit():
            rule_limit = max(0, int(rule_limit_raw))
        else:
            rule_limit = PLAN_RULE_LIMITS[subscription_plan]
        existing = User.query.filter(
            func.lower(User.username) == username_normalized, User.id != user.id
        ).first()
        if existing:
            flash("Usuario ja existe.", "warning")
            return render_template("admin/user_edit.html", user=user)
        if email:
            email_existing = User.query.filter(User.email == email, User.id != user.id).first()
            if email_existing:
                flash("Email ja cadastrado.", "warning")
                return render_template("admin/user_edit.html", user=user)

        user.username = username_normalized
        user.email = email or None
        user.is_admin = is_admin
        user.subscription_plan = subscription_plan
        user.rule_limit = rule_limit
        if clear_paid:
            user.paid_until = None
        elif paid_days_raw:
            try:
                paid_days = int(paid_days_raw)
            except ValueError:
                paid_days = 0
            if paid_days > 0:
                user.paid_until = now_sp() + timedelta(days=paid_days)
        if clear_trial:
            user.trial_until = None
        elif trial_days_raw:
            try:
                trial_days = int(trial_days_raw)
            except ValueError:
                trial_days = 0
            if trial_days > 0:
                user.trial_until = now_sp() + timedelta(days=trial_days)
        if new_password:
            user.set_password(new_password)
        db.session.commit()
        flash("Usuario atualizado.", "success")
        return redirect(url_for("admin.user_detail", user_id=user.id))
    return render_template("admin/user_edit.html", user=user)


@admin_bp.route("/users/<int:user_id>/reset-telegram", methods=["POST"])
@login_required
def reset_telegram(user_id):
    _require_admin()
    user = User.query.get_or_404(user_id)
    user.telegram_token = None
    user.telegram_chat_id = None
    db.session.commit()
    flash("Telegram resetado para este usuario.", "success")
    return redirect(url_for("admin.user_detail", user_id=user.id))


@admin_bp.route("/rules/<int:rule_id>/toggle", methods=["POST"])
@login_required
def toggle_rule(rule_id):
    _require_admin()
    rule = Rule.query.get_or_404(rule_id)
    rule.is_active = not rule.is_active
    db.session.commit()
    flash("Status da regra atualizado.", "success")
    return redirect(safe_redirect_target(request.referrer, url_for("admin.dashboard")))


@admin_bp.route("/broadcast", methods=["POST"])
@login_required
def broadcast():
    _require_admin()
    message = (request.form.get("message") or "").strip()
    send_telegram = bool(request.form.get("send_telegram"))
    if not message:
        flash("Mensagem obrigatoria.", "warning")
        return redirect(url_for("admin.dashboard"))
    AdminBroadcast.query.update({"is_active": False})
    db.session.add(AdminBroadcast(message=message, is_active=True))
    db.session.commit()
    if send_telegram:
        users = User.query.filter_by(telegram_verified=True).all()
        for user in users:
            if user.telegram_token and user.telegram_chat_id:
                send_message(user.telegram_token, user.telegram_chat_id, message)
    flash("Mensagem enviada para o painel.", "success")
    return redirect(url_for("admin.dashboard"))
