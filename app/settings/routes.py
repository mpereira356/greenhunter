from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required

from ..extensions import db
from ..services.telegram import send_message

settings_bp = Blueprint("settings", __name__, url_prefix="/settings")


@settings_bp.route("/", methods=["GET", "POST"])
@login_required
def settings():
    if request.method == "POST":
        current_user.telegram_token = request.form.get("telegram_token", "").strip()
        current_user.telegram_chat_id = request.form.get("telegram_chat_id", "").strip()
        db.session.commit()
        flash("Configuracoes atualizadas.", "success")
        return redirect(url_for("settings.settings"))
    return render_template("settings/index.html")


@settings_bp.route("/test", methods=["POST"])
@login_required
def test_telegram():
    token = current_user.telegram_token
    chat_id = current_user.telegram_chat_id
    if not token or not chat_id:
        flash("Preencha o token e o chat_id antes de testar.", "warning")
        return redirect(url_for("settings.settings"))
    ok, message = send_message(token, chat_id, "Teste de notificacao do sistema.")
    if ok:
        flash("Mensagem enviada com sucesso.", "success")
    else:
        flash(f"Falha ao enviar: {message}", "danger")
    return redirect(url_for("settings.settings"))
