from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import login_user, logout_user, login_required

from ..extensions import db
from ..models import LoginAttempt, User

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = User.query.filter_by(username=username).first()
        ip_address = (request.headers.get("X-Forwarded-For", "") or request.remote_addr or "").split(",")[0].strip()
        if user and user.check_password(password):
            login_user(user)
            db.session.add(
                LoginAttempt(
                    username=username,
                    user_id=user.id,
                    ip_address=ip_address,
                    success=True,
                )
            )
            db.session.commit()
            return redirect(url_for("main.dashboard"))
        db.session.add(
            LoginAttempt(
                username=username,
                user_id=user.id if user else None,
                ip_address=ip_address,
                success=False,
            )
        )
        db.session.commit()
        flash("Credenciais invalidas.", "danger")
    return render_template("auth/login.html")


@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "")
        if not username or not password or not email:
            flash("Preencha todos os campos.", "warning")
            return render_template("auth/register.html")
        existing = User.query.filter_by(username=username).first()
        if existing:
            flash("Usuario ja existe.", "warning")
            return render_template("auth/register.html")
        email_exists = User.query.filter_by(email=email).first()
        if email_exists:
            flash("Email ja cadastrado.", "warning")
            return render_template("auth/register.html")
        user = User(username=username, email=email)
        user.set_password(password)
        db.session.add(user)
        db.session.commit()
        flash("Cadastro criado. Faça login.", "success")
        return redirect(url_for("auth.login"))
    return render_template("auth/register.html")


@auth_bp.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("auth.login"))
