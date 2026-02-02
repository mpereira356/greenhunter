from datetime import datetime

import os

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required

from ..extensions import db
from ..models import MatchAlert, Rule, User
from ..services.telegram import send_document

history_bp = Blueprint("history", __name__, url_prefix="/history")


@history_bp.route("/")
@login_required
def history():
    rule_id = request.args.get("rule_id", type=int)
    user_id = request.args.get("user_id", type=int)
    status = request.args.get("status", "").strip()
    date_from = request.args.get("from", "").strip()
    date_to = request.args.get("to", "").strip()
    page = request.args.get("page", 1, type=int)
    per_page = 20

    is_admin = current_user.is_admin_user
    query = MatchAlert.query
    if not is_admin:
        query = query.filter_by(user_id=current_user.id)

    if rule_id:
        query = query.filter(MatchAlert.rule_id == rule_id)
    if is_admin and user_id:
        query = query.filter(MatchAlert.user_id == user_id)
    if status:
        query = query.filter(MatchAlert.status == status)
    if date_from:
        try:
            dt_from = datetime.strptime(date_from, "%Y-%m-%d")
            query = query.filter(MatchAlert.created_at >= dt_from)
        except ValueError:
            pass
    if date_to:
        try:
            dt_to = datetime.strptime(date_to, "%Y-%m-%d")
            query = query.filter(MatchAlert.created_at < dt_to.replace(hour=23, minute=59, second=59))
        except ValueError:
            pass

    pagination = query.order_by(MatchAlert.created_at.desc()).paginate(page=page, per_page=per_page)
    total_count = query.count()
    green_count = query.filter(MatchAlert.status == "green").count()
    red_count = query.filter(MatchAlert.status == "red").count()
    pending_count = query.filter(MatchAlert.status == "pending").count()
    win_rate = 0
    if green_count + red_count > 0:
        win_rate = round((green_count / (green_count + red_count)) * 100, 1)
    if is_admin:
        rules = Rule.query.order_by(Rule.name).all()
        users = User.query.order_by(User.username.asc()).all()
    else:
        rules = Rule.query.filter_by(user_id=current_user.id).order_by(Rule.name).all()
        users = []
    query_args = request.args.to_dict()
    query_args.pop("page", None)
    return render_template(
        "history/list.html",
        pagination=pagination,
        is_admin=is_admin,
        rules=rules,
        users=users,
        query_args=query_args,
        total_count=total_count,
        green_count=green_count,
        red_count=red_count,
        pending_count=pending_count,
        win_rate=win_rate,
    )


@history_bp.route("/<int:alert_id>/delete", methods=["POST"])
@login_required
def delete_alert(alert_id):
    if not current_user.is_admin_user:
        flash("Apenas administradores podem excluir historicos.", "danger")
        return redirect(url_for("history.history"))

    alert = MatchAlert.query.get_or_404(alert_id)
    db.session.delete(alert)
    db.session.commit()
    flash("Historico excluido com sucesso.", "success")
    return redirect(url_for("history.history", **request.args.to_dict()))


@history_bp.route("/delete-selected", methods=["POST"])
@login_required
def delete_selected_alerts():
    if not current_user.is_admin_user:
        flash("Apenas administradores podem excluir historicos.", "danger")
        return redirect(url_for("history.history"))

    selected_ids = request.form.getlist("selected_alert_ids")
    ids = []
    for raw in selected_ids:
        try:
            ids.append(int(raw))
        except (TypeError, ValueError):
            continue

    if not ids:
        flash("Nenhum historico selecionado.", "warning")
        return redirect(url_for("history.history", **request.args.to_dict()))

    deleted = MatchAlert.query.filter(MatchAlert.id.in_(ids)).delete(synchronize_session=False)
    db.session.commit()
    flash(f"{deleted} historico(s) excluido(s) com sucesso.", "success")
    return redirect(url_for("history.history", **request.args.to_dict()))


@history_bp.route("/send-report", methods=["POST"])
@login_required
def send_report():
    if not current_user.telegram_token or not current_user.telegram_chat_id:
        flash("Configure o Telegram antes de enviar.", "warning")
        return redirect(url_for("history.history"))
    report_path = os.path.join("data", "exports", "historico_geral.xlsx")
    if not os.path.exists(report_path):
        flash("Nenhum relatorio encontrado ainda.", "warning")
        return redirect(url_for("history.history"))
    ok, message = send_document(
        current_user.telegram_token,
        current_user.telegram_chat_id,
        report_path,
        caption="Relatorio geral do historico",
    )
    if ok:
        flash("Relatorio enviado para o Telegram.", "success")
    else:
        flash(f"Falha ao enviar relatorio: {message}", "danger")
    return redirect(url_for("history.history"))
