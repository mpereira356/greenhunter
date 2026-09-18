from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required
from ..extensions import db
from ..services.telegram import send_message

settings_bp = Blueprint("settings", __name__, url_prefix="/settings")


@settings_bp.route("/", methods=["GET", "POST"])
@login_required
def settings():
    if request.method == "POST":
        form_type = request.form.get("form_type", "telegram")
        if form_type == "profile":
            current_password = request.form.get("current_password", "")
            new_password = request.form.get("new_password", "")
            confirm_password = request.form.get("confirm_password", "")

            if new_password or confirm_password:
                if not current_password or not current_user.check_password(current_password):
                    flash("Senha atual incorreta.", "warning")
                    return redirect(url_for("settings.settings"))
                if len(new_password) < 10:
                    flash("A nova senha deve possuir pelo menos 10 caracteres.", "warning")
                    return redirect(url_for("settings.settings"))
                if new_password != confirm_password:
                    flash("As senhas nao conferem.", "warning")
                    return redirect(url_for("settings.settings"))
                current_user.set_password(new_password)

            db.session.commit()
            flash("Senha atualizada.", "success")
            return redirect(url_for("settings.settings"))

        new_token = request.form.get("telegram_token", "").strip()
        new_chat = request.form.get("telegram_chat_id", "").strip()
        if new_token != (current_user.telegram_token or "") or new_chat != (current_user.telegram_chat_id or ""):
            current_user.telegram_verified = False
            current_user.telegram_update_offset = None
        current_user.telegram_token = new_token
        current_user.telegram_chat_id = new_chat
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
        current_user.telegram_verified = True
        db.session.commit()
        flash("Mensagem enviada com sucesso.", "success")
    else:
        flash(f"Falha ao enviar: {message}", "danger")
    return redirect(url_for("settings.settings"))
