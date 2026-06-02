from datetime import datetime

import os

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required
from markupsafe import Markup, escape

from ..extensions import db
from ..models import MatchAlert, Rule, User
from ..services.match_analysis import build_alert_analysis
from ..services.telegram import send_document
from ..services.undo import create_undo_action, snapshot_alert
from ..utils.time import now_sp

history_bp = Blueprint("history", __name__, url_prefix="/history")


def _can_manage_alert(alert) -> bool:
    return current_user.is_admin_user or alert.user_id == current_user.id


@history_bp.route("/<int:alert_id>/analysis")
@login_required
def alert_analysis(alert_id):
    alert = MatchAlert.query.get_or_404(alert_id)
    if not _can_manage_alert(alert):
        flash("Voce nao tem permissao para ver esta analise.", "danger")
        return redirect(url_for("history.history"))
    if not current_user.has_premium_analysis:
        return render_template("history/upgrade_analysis.html", alert=alert)

    try:
        analysis = build_alert_analysis(
            alert,
            force_refresh=request.args.get("refresh") == "1",
            include_details=request.args.get("details", "1") != "0",
        )
    except Exception as exc:
        analysis = None
        flash(f"Nao foi possivel montar a analise agora: {exc}", "warning")
    return render_template("history/analysis.html", alert=alert, analysis=analysis)


@history_bp.route("/")
@login_required
def history():
    rule_id = request.args.get("rule_id", type=int)
    user_id = request.args.get("user_id", type=int)
    status = request.args.get("status", "").strip()
    league = request.args.get("league", "").strip()
    date_from = request.args.get("from", "").strip()
    date_to = request.args.get("to", "").strip()
    page = request.args.get("page", 1, type=int)
    per_page = 20

    is_admin = current_user.is_admin_user
    query = MatchAlert.query
    leagues_query = db.session.query(MatchAlert.league).filter(
        MatchAlert.league.isnot(None),
        MatchAlert.league != "",
    )
    if not is_admin:
        query = query.filter_by(user_id=current_user.id)
        leagues_query = leagues_query.filter(MatchAlert.user_id == current_user.id)

    if rule_id:
        query = query.filter(MatchAlert.rule_id == rule_id)
    if is_admin and user_id:
        query = query.filter(MatchAlert.user_id == user_id)
        leagues_query = leagues_query.filter(MatchAlert.user_id == user_id)
    if status:
        query = query.filter(MatchAlert.status == status)
    if league:
        query = query.filter(MatchAlert.league == league)
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
    leagues = [
        row[0]
        for row in leagues_query.distinct().order_by(MatchAlert.league.asc()).all()
        if row[0]
    ]
    query_args = request.args.to_dict()
    query_args.pop("page", None)
    return render_template(
        "history/list.html",
        pagination=pagination,
        is_admin=is_admin,
        rules=rules,
        users=users,
        leagues=leagues,
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
    undo_token = create_undo_action(
        user_id=current_user.id,
        action_type="delete_alert",
        payload={"alerts": [snapshot_alert(alert)]},
    )
    db.session.delete(alert)
    db.session.commit()
    target_url = url_for("history.history", **request.args.to_dict())
    undo_url = url_for("main.undo_action", token=undo_token, next=target_url)
    flash(
        Markup(f"Historico excluido com sucesso. <a class='alert-link' href='{escape(undo_url)}'>Desfazer</a>"),
        "success",
    )
    return redirect(target_url)


@history_bp.route("/<int:alert_id>/bet", methods=["POST"])
@login_required
def save_bet(alert_id):
    alert = MatchAlert.query.get_or_404(alert_id)
    if not _can_manage_alert(alert):
        flash("Voce nao tem permissao para editar esta aposta.", "danger")
        return redirect(url_for("history.history"))

    amount_raw = (request.form.get("stake_amount") or "").strip().replace(",", ".")
    odd_raw = (request.form.get("stake_odd") or "").strip().replace(",", ".")
    note = (request.form.get("bet_note") or "").strip()

    def _money(raw):
        if not raw:
            return None
        try:
            value = float(raw)
        except ValueError:
            return None
        return value if value >= 0 else None

    stake_amount = _money(amount_raw)
    stake_odd = _money(odd_raw)
    if stake_amount is None and amount_raw:
        flash("Valor apostado invalido.", "warning")
        return redirect(url_for("history.history", **request.args.to_dict()))
    if stake_odd is None and odd_raw:
        flash("Odd invalida.", "warning")
        return redirect(url_for("history.history", **request.args.to_dict()))

    alert.stake_amount = stake_amount
    alert.stake_odd = stake_odd
    alert.bet_note = note or None
    alert.bet_recorded_at = now_sp() if stake_amount is not None or stake_odd is not None or note else None
    db.session.commit()
    flash("Aposta salva no historico.", "success")
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

    selected_alerts = MatchAlert.query.filter(MatchAlert.id.in_(ids)).all()
    snapshots = [snapshot_alert(alert) for alert in selected_alerts]
    undo_token = create_undo_action(
        user_id=current_user.id,
        action_type="delete_selected_alerts",
        payload={"alerts": snapshots},
    )
    deleted = MatchAlert.query.filter(MatchAlert.id.in_(ids)).delete(synchronize_session=False)
    db.session.commit()
    target_url = url_for("history.history", **request.args.to_dict())
    undo_url = url_for("main.undo_action", token=undo_token, next=target_url)
    flash(
        Markup(f"{deleted} historico(s) excluido(s) com sucesso. <a class='alert-link' href='{escape(undo_url)}'>Desfazer</a>"),
        "success",
    )
    return redirect(target_url)


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
