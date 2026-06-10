from datetime import timedelta

from flask import Blueprint, current_app, flash, make_response, redirect, render_template, request, url_for
from flask_login import login_user, logout_user, login_required
from sqlalchemy import func

from ..extensions import db
from ..models import LoginAttempt, User
from ..utils.db import commit_with_retry
from ..utils.time import now_sp
from .security import LoginRateLimiter, build_login_rate_limiter, get_client_ip

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")
login_rate_limiter = build_login_rate_limiter()
registration_rate_limiter = LoginRateLimiter(max_attempts=5, window_seconds=3600, block_seconds=3600)


def _login_blocked_response(retry_after):
    flash("Muitas tentativas de login. Aguarde alguns minutos e tente novamente.", "danger")
    response = make_response(render_template("auth/login.html"), 429)
    response.headers["Retry-After"] = str(retry_after)
    return response


def _record_login_attempt(username, ip_address, success, user=None):
    db.session.add(
        LoginAttempt(
            username=(username or "")[:80],
            user_id=user.id if user else None,
            ip_address=ip_address[:64],
            success=success,
        )
    )
    commit_with_retry()


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.content_length and request.content_length > current_app.config["LOGIN_MAX_CONTENT_LENGTH"]:
            return _login_blocked_response(login_rate_limiter.block(get_client_ip()))
        ip_address = get_client_ip()
        retry_after = login_rate_limiter.retry_after(ip_address)
        if retry_after:
            return _login_blocked_response(retry_after)

        username = request.form.get("username", "").strip()
        username_normalized = username.lower()
        password = request.form.get("password", "")
        if not username_normalized or len(username_normalized) > 80 or len(password) > 256:
            _record_login_attempt(username_normalized, ip_address, False)
            retry_after = login_rate_limiter.record_failure(ip_address)
            if retry_after:
                return _login_blocked_response(retry_after)
            flash("Credenciais invalidas.", "danger")
            return render_template("auth/login.html")

        window_seconds = current_app.config["LOGIN_WINDOW_SECONDS"]
        recent_failures = LoginAttempt.query.filter(
            LoginAttempt.ip_address == ip_address,
            LoginAttempt.success.is_(False),
            LoginAttempt.created_at >= now_sp() - timedelta(seconds=window_seconds),
        ).count()
        if recent_failures >= current_app.config["LOGIN_MAX_ATTEMPTS"]:
            return _login_blocked_response(login_rate_limiter.block(ip_address))

        user = User.query.filter(func.lower(User.username) == username_normalized).first()
        if user and user.check_password(password):
            login_user(user)
            login_rate_limiter.reset(ip_address)
            _record_login_attempt(username_normalized, ip_address, True, user)
            return redirect(url_for("main.dashboard"))
        _record_login_attempt(username_normalized, ip_address, False, user)
        retry_after = login_rate_limiter.record_failure(ip_address)
        if retry_after:
            return _login_blocked_response(retry_after)
        flash("Credenciais invalidas.", "danger")
    return render_template("auth/login.html")


@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        ip_address = get_client_ip()
        retry_after = registration_rate_limiter.retry_after(ip_address)
        if retry_after:
            return _login_blocked_response(retry_after)
        registration_rate_limiter.record_failure(ip_address)

        username = request.form.get("username", "").strip()
        username_normalized = username.lower()
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "")
        if (
            not username_normalized
            or not password
            or not email
            or len(username_normalized) > 80
            or len(email) > 120
            or len(password) > 256
        ):
            flash("Preencha todos os campos.", "warning")
            return render_template("auth/register.html")
        if len(password) < 10:
            flash("Use uma senha com pelo menos 10 caracteres.", "warning")
            return render_template("auth/register.html")
        existing = User.query.filter(func.lower(User.username) == username_normalized).first()
        if existing:
            flash("Usuario ja existe.", "warning")
            return render_template("auth/register.html")
        email_exists = User.query.filter_by(email=email).first()
        if email_exists:
            flash("Email ja cadastrado.", "warning")
            return render_template("auth/register.html")
        user = User(username=username_normalized, email=email)
        user.set_password(password)
        db.session.add(user)
        commit_with_retry()
        flash("Cadastro criado. Faça login.", "success")
        return redirect(url_for("auth.login"))
    return render_template("auth/register.html")


@auth_bp.route("/logout", methods=["POST"])
@login_required
def logout():
    logout_user()
    return redirect(url_for("auth.login"))
