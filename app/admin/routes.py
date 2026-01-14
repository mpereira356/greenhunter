from datetime import datetime, timedelta

from flask import Blueprint, abort, flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required
from sqlalchemy import case, func

from ..extensions import db
from ..models import AdminBroadcast, LoginAttempt, MatchAlert, Rule, RuleCondition, User
from ..services.telegram import send_message
from ..services.worker import get_api_status

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")

ALERTS_PER_HOUR_THRESHOLD = 20


def _require_admin():
    if not current_user.is_authenticated or not current_user.is_admin_user:
        abort(403)


@admin_bp.route("/")
@login_required
def dashboard():
    _require_admin()
    now = datetime.utcnow()
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

    return render_template(
        "admin/dashboard.html",
        total_users=total_users,
        total_rules=total_rules,
        active_rules=active_rules,
        alerts_today=alerts_today,
        greens_today=greens_today,
        reds_today=reds_today,
        users=users,
        top_rules=top_rules,
        risk_users=risk_users,
        alerts_per_hour_threshold=ALERTS_PER_HOUR_THRESHOLD,
        login_attempts=LoginAttempt.query.order_by(LoginAttempt.created_at.desc()).limit(20).all(),
        broadcasts=AdminBroadcast.query.order_by(AdminBroadcast.created_at.desc()).limit(5).all(),
        api_status=get_api_status(),
    )


@admin_bp.route("/users")
@login_required
def users_list():
    _require_admin()
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
    users = []
    for user in User.query.order_by(User.created_at.desc()).all():
        users.append(
            {
                "user": user,
                "rules": rule_counts.get(user.id, 0),
                "alerts": alert_counts.get(user.id, 0),
            }
        )
    return render_template("admin/users.html", users=users)


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
        email = request.form.get("email", "").strip()
        is_admin = bool(request.form.get("is_admin"))
        new_password = request.form.get("new_password", "").strip()

        if not username:
            flash("Usuario e obrigatorio.", "warning")
            return render_template("admin/user_edit.html", user=user)
        existing = User.query.filter(User.username == username, User.id != user.id).first()
        if existing:
            flash("Usuario ja existe.", "warning")
            return render_template("admin/user_edit.html", user=user)
        if email:
            email_existing = User.query.filter(User.email == email, User.id != user.id).first()
            if email_existing:
                flash("Email ja cadastrado.", "warning")
                return render_template("admin/user_edit.html", user=user)

        user.username = username
        user.email = email or None
        user.is_admin = is_admin
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
    return redirect(request.referrer or url_for("admin.dashboard"))


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
