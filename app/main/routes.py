import json
import math
import re
import threading
import unicodedata
from datetime import datetime, timedelta
from urllib.parse import urlparse

from flask import Blueprint, Response, abort, current_app, flash, jsonify, redirect, render_template, request, session, url_for
from flask_login import login_required, current_user
from sqlalchemy import func
from sqlalchemy.orm import joinedload, load_only

from ..extensions import db
from ..models import LiveGameState, MatchAlert, MatchdayLeaguePreference, MercadoPagoWebhookEvent, PredictionRun, Rule, SavedTicket, SavedTicketLeg, SharedInvitation, User, UserMatchdayPreference
from ..services.sharing import (accept_rule_snapshot, accept_ticket_snapshot, encode_snapshot,
                                find_recipient, ticket_is_shareable, ticket_snapshot,
                                ticket_snapshot_is_pregame)
from ..services.mercadopago import cancel_subscription, create_subscription, get_authorized_payment, get_payment, get_subscription, user_id_from_reference, valid_webhook_signature
from ..services.worker import get_api_status
from ..services.undo import apply_undo
from ..services.qualplacar_odds import attach_qualplacar_odds, bookmaker_selection_odds
from ..services.predictions import (
    MAX_CANDIDATES_PER_BATCH,
    append_prediction_candidates,
    finish_prediction_run,
    mark_prediction_run_failed,
    start_prediction_run,
)
from ..services.matchday import (
    _is_excluded_match,
    analyze_upcoming_match,
    find_match,
    get_matchday,
    known_matchday_leagues,
    load_matchday_summary_cache,
    save_matchday_summary_cache,
    load_matchday_trend_index,
    trend_groups_for_match,
)
from ..security import safe_redirect_target
from ..utils.time import now_sp

main_bp = Blueprint("main", __name__)
_matchday_analysis_slots = threading.BoundedSemaphore(2)


@main_bp.route("/premium")
@login_required
def premium():
    pending_checkout_url = None
    checkout_url = str(current_user.mercadopago_checkout_url or "").strip()
    checkout_host = (urlparse(checkout_url).hostname or "").lower()
    if (
        current_user.mercadopago_subscription_status == "pending"
        and checkout_url
        and (checkout_host == "mercadopago.com.br" or checkout_host.endswith(".mercadopago.com.br"))
    ):
        pending_checkout_url = checkout_url
    return render_template("premium.html", pending_checkout_url=pending_checkout_url)


def _activate_mercadopago_user(user, status="authorized", extend=False):
    now = now_sp()
    user.subscription_plan = "pro"
    user.rule_limit = max(int(user.rule_limit or 0), 20)
    user.mercadopago_subscription_status = status
    if extend:
        candidate = now + timedelta(days=31)
        if not user.paid_until or user.paid_until < candidate:
            user.paid_until = candidate
    elif not user.paid_until or user.paid_until <= now:
        user.paid_until = now + timedelta(days=31)


def _sync_subscription(payload, extend=False):
    subscription_id = str(payload.get("id") or payload.get("preapproval_id") or "")
    reference = payload.get("external_reference")
    user = User.query.get(user_id_from_reference(reference)) if user_id_from_reference(reference) else None
    if not user and subscription_id:
        user = User.query.filter_by(mercadopago_subscription_id=subscription_id).first()
    if not user:
        return None
    if subscription_id:
        user.mercadopago_subscription_id = subscription_id
    status = str(payload.get("status") or "").lower()
    user.mercadopago_subscription_status = status or user.mercadopago_subscription_status
    if status in {"authorized", "approved"}:
        _activate_mercadopago_user(user, status=status, extend=extend)
    elif status in {"cancelled", "canceled"}:
        user.mercadopago_checkout_url = None
        if not user.paid_until or user.paid_until <= now_sp():
            user.subscription_plan = "starter"
            user.rule_limit = 2
    elif status == "rejected":
        user.subscription_plan = "starter"
        user.rule_limit = 2
        user.paid_until = now_sp()
    return user


@main_bp.route("/premium/assinar", methods=["POST"])
@login_required
def premium_checkout():
    if current_user.is_admin_user:
        flash("A conta de administrador já possui acesso completo.", "info")
        return redirect(url_for("main.premium"))
    payer_email = str(request.form.get("payer_email") or current_user.email or "").strip().lower()
    if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", payer_email):
        flash("Informe um e-mail válido da conta Mercado Pago.", "warning")
        return redirect(url_for("main.premium"))
    replace_pending = request.form.get("replace_pending") == "1"
    existing_checkout = str(current_user.mercadopago_checkout_url or "").strip()
    existing_host = (urlparse(existing_checkout).hostname or "").lower()
    if (
        not replace_pending
        and current_user.mercadopago_subscription_status == "pending"
        and existing_checkout
        and (existing_host == "mercadopago.com.br" or existing_host.endswith(".mercadopago.com.br"))
    ):
        return redirect(existing_checkout, code=303)
    try:
        if replace_pending and current_user.mercadopago_subscription_status == "pending" and current_user.mercadopago_subscription_id:
            previous = get_subscription(current_user.mercadopago_subscription_id)
            previous_status = str(previous.get("status") or "").lower()
            if previous_status not in {"cancelled", "canceled"}:
                cancel_subscription(current_user.mercadopago_subscription_id)
            current_user.mercadopago_subscription_id = None
            current_user.mercadopago_subscription_status = "cancelled"
            current_user.mercadopago_checkout_url = None
            db.session.commit()
        payload = create_subscription(
            current_user,
            url_for("main.premium_return", _external=True, _scheme="https"),
            payer_email=payer_email,
        )
        current_user.mercadopago_subscription_id = str(payload.get("id") or "") or None
        current_user.mercadopago_subscription_status = str(payload.get("status") or "pending")
        current_user.mercadopago_checkout_url = payload.get("init_point") or payload.get("sandbox_init_point")
        db.session.commit()
        if not current_user.mercadopago_checkout_url:
            raise RuntimeError("O checkout não foi retornado pelo Mercado Pago.")
        return redirect(current_user.mercadopago_checkout_url, code=303)
    except Exception as exc:
        db.session.rollback()
        current_app.logger.warning("Falha ao criar assinatura Mercado Pago: %s", exc)
        flash(str(exc), "warning")
        return redirect(url_for("main.premium"))


@main_bp.route("/premium/retorno")
@login_required
def premium_return():
    if current_user.mercadopago_subscription_id:
        try:
            payload = get_subscription(current_user.mercadopago_subscription_id)
            _sync_subscription(payload)
            db.session.commit()
        except Exception as exc:
            db.session.rollback()
            current_app.logger.warning("Falha ao conferir retorno Mercado Pago: %s", exc)
    if current_user.is_premium_user:
        flash("Pagamento confirmado. Sua conta Premium está ativa!", "success")
    else:
        flash("Assinatura recebida e aguardando confirmação do pagamento.", "info")
    return redirect(url_for("main.premium"))


@main_bp.route("/premium/cancelar", methods=["POST"])
@login_required
def premium_cancel():
    if current_user.is_admin_user:
        flash("A conta de administrador não possui assinatura para cancelar.", "info")
        return redirect(url_for("main.premium"))
    subscription_id = str(current_user.mercadopago_subscription_id or "").strip()
    if not subscription_id:
        flash("Nenhuma assinatura do Mercado Pago foi encontrada.", "warning")
        return redirect(url_for("main.premium"))
    try:
        verified = cancel_subscription(subscription_id)
        current_user.mercadopago_subscription_status = str(verified.get("status") or "cancelled").lower()
        current_user.mercadopago_checkout_url = None
        db.session.commit()
        if current_user.paid_until and current_user.paid_until > now_sp():
            flash(f"Renovação cancelada. Seu Premium continua até {current_user.paid_until.strftime('%d/%m/%Y')}.", "success")
        else:
            current_user.subscription_plan = "starter"
            current_user.rule_limit = 2
            db.session.commit()
            flash("Assinatura cancelada. Não haverá novas cobranças.", "success")
    except Exception as exc:
        db.session.rollback()
        current_app.logger.warning("Falha ao cancelar assinatura Mercado Pago: %s", exc)
        flash("Não foi possível cancelar agora. Tente novamente em alguns instantes.", "warning")
    return redirect(url_for("main.premium"))


@main_bp.route("/api/payments/mercadopago/webhook", methods=["POST"])
def mercadopago_webhook():
    payload = request.get_json(silent=True) or {}
    signature_data_id = request.args.get("data.id")
    if not valid_webhook_signature(
        request.headers.get("X-Signature"),
        request.headers.get("X-Request-Id"),
        signature_data_id,
    ):
        current_app.logger.warning("Webhook Mercado Pago rejeitado: assinatura invalida.")
        return jsonify({"ok": False}), 401
    event_type = str(payload.get("type") or request.args.get("type") or "")
    resource_id = str((payload.get("data") or {}).get("id") or request.args.get("data.id") or "")
    action = str(payload.get("action") or "")
    if not event_type or not resource_id:
        return jsonify({"ok": True}), 200
    event_key = f"{event_type}:{resource_id}:{action}"
    if MercadoPagoWebhookEvent.query.filter_by(event_key=event_key).first():
        return jsonify({"ok": True, "duplicate": True}), 200
    try:
        if event_type == "subscription_preapproval":
            verified = get_subscription(resource_id)
            _sync_subscription(verified)
        elif event_type == "subscription_authorized_payment":
            verified = get_authorized_payment(resource_id)
            subscription_id = str(verified.get("preapproval_id") or "")
            user = User.query.filter_by(mercadopago_subscription_id=subscription_id).first()
            if user and str(verified.get("status") or "").lower() == "approved":
                _activate_mercadopago_user(user, status="authorized", extend=True)
        elif event_type == "payment":
            verified = get_payment(resource_id)
            user_id = user_id_from_reference(verified.get("external_reference"))
            user = User.query.get(user_id) if user_id else None
            if user and str(verified.get("status") or "").lower() == "approved":
                _activate_mercadopago_user(user, status="authorized", extend=True)
        else:
            return jsonify({"ok": True, "ignored": True}), 200
        db.session.add(MercadoPagoWebhookEvent(event_key=event_key, event_type=event_type, resource_id=resource_id))
        db.session.commit()
        return jsonify({"ok": True}), 200
    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception("Falha no webhook Mercado Pago: %s", exc)
        return jsonify({"ok": False}), 503

RELEVANT_MATCHDAY_LEAGUES = {
    "uefa champions league", "uefa champions league qualifying",
    "uefa europa league", "uefa europa league qualifying",
    "uefa conference league", "uefa conference league qualifying",
    "england premier league", "english premier league",
    "england championship", "english championship", "england league 1", "england league one",
    "england league 2", "england league two",
    "spain la liga", "spain segunda", "spain segunda division",
    "italy serie a", "italy serie b", "coppa italia", "italy coppa italia",
    "germany bundesliga", "germany bundesliga ii", "germany bundesliga 2",
    "germany 2 bundesliga", "germany dfb pokal", "dfb pokal", "france ligue 1",
    "portugal primeira liga", "netherlands eredivisie",
    "belgium first division a", "turkey super lig", "turkiye super lig", "scotland premiership",
    "austria bundesliga", "denmark superligaen", "norway eliteserien",
    "switzerland challenge league", "switzerland super league",
    "bulgaria first league", "sweden allsvenskan", "greece super league 1",
    "czechia first league", "czech republic first league", "poland ekstraklasa",
    "romania liga i", "romania liga 1", "saudi arabia pro league", "saudi pro league",
    "brazil serie a", "brazil serie b", "brazil cup", "copa do brasil",
    "copa libertadores", "copa sudamericana",
    "argentina liga profesional", "argentina cup", "copa argentina",
    "colombia primera a", "paraguay division profesional", "peru liga 1",
    "ecuador ligapro serie a", "ecuador liga pro serie a",
    "chile primera division", "chile liga de primera", "uruguay primera division",
    "usa mls", "mexico liga mx", "leagues cup", "concacaf champions cup",
    "fifa world cup", "world cup qualifying", "uefa nations league",
    "uefa european championship", "european championship", "copa america",
}

DEFAULT_MATCHDAY_MARKET_SETTINGS = {
    "minimum_samples": 3,
    "max_markets_per_game": 3,
    "goals": {"goal_ht": True, "over15": True, "over25": True, "btts": True},
    "team_goals": {"enabled": True, "total_enabled": False, "home_enabled": True, "away_enabled": True, "total": .5, "home": .5, "away": .5},
    "corners": {"enabled": True, "total_enabled": True, "home_enabled": True, "away_enabled": True, "total": 8.5, "home": 2.5, "away": 2.5},
    "cards": {"enabled": True, "total_enabled": True, "home_enabled": True, "away_enabled": True, "total": 4.5, "home": 1.5, "away": 1.5},
    "shots": {"enabled": True, "total_enabled": True, "home_enabled": True, "away_enabled": True, "total": 19.5, "home": 10.5, "away": 10.5},
    "shots_on_target": {"enabled": True, "total_enabled": True, "home_enabled": True, "away_enabled": True, "total": 7.5, "home": 3.5, "away": 3.5},
    "fouls": {"enabled": True, "total_enabled": True, "home_enabled": True, "away_enabled": True, "total": 15.5, "home": 7.5, "away": 7.5},
    "offsides": {"enabled": True, "total_enabled": True, "home_enabled": True, "away_enabled": True, "total": 1.5, "home": .5, "away": .5},
    "corners_1h": {"enabled": True, "total_enabled": True, "home_enabled": True, "away_enabled": True, "total": 2.5, "home": 2.5, "away": 2.5},
    "cards_1h": {"enabled": True, "total_enabled": True, "home_enabled": True, "away_enabled": True, "total": 1.5, "home": .5, "away": .5},
}


def _normalized_league_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    return " ".join(
        "".join(char for char in normalized if not unicodedata.combining(char))
        .casefold()
        .replace("-", " ")
        .split()
    )


def _is_relevant_matchday_league(value: str, preferences: dict | None = None) -> bool:
    normalized = _normalized_league_name(value)
    if preferences is not None and normalized in preferences:
        return bool(preferences[normalized])
    return normalized in RELEVANT_MATCHDAY_LEAGUES


def _relevant_league_preferences() -> dict[str, bool]:
    return {
        item.normalized_name: bool(item.is_relevant)
        for item in MatchdayLeaguePreference.query.all()
    }


def _user_matchday_preference() -> UserMatchdayPreference | None:
    return UserMatchdayPreference.query.filter_by(user_id=current_user.id).first()


def _market_settings(preference: UserMatchdayPreference | None) -> dict:
    settings = json.loads(json.dumps(DEFAULT_MATCHDAY_MARKET_SETTINGS))
    custom = _safe_json_dict(preference.market_settings_json) if preference else {}
    for key, value in custom.items():
        if isinstance(value, dict) and isinstance(settings.get(key), dict):
            settings[key].update(value)
        elif key in {"minimum_samples", "max_markets_per_game"}:
            settings[key] = value
    return settings


def _global_relevant_names(all_leagues: list[str], overrides: dict[str, bool]) -> list[str]:
    return [league for league in all_leagues if _is_relevant_matchday_league(league, overrides)]


def _site_url(path: str = "") -> str:
    base = (current_app.config.get("SITE_URL") or "https://greenhunter.com.br").rstrip("/")
    return f"{base}{path}"


def _safe_json_dict(raw):
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _safe_match_url(raw):
    try:
        parsed = urlparse(str(raw or ""))
    except ValueError:
        return "#"
    if parsed.scheme not in {"http", "https"}:
        return "#"
    if parsed.hostname not in {"betsapi.com", "pt.betsapi.com", "www.betsapi.com"}:
        return "#"
    return parsed.geturl()


def _live_match_from_state(state):
    stats = _safe_json_dict(state.stats_json)
    stats_list = []
    for key, value in sorted(stats.items()):
        if not isinstance(value, dict):
            continue
        stats_list.append(
            {
                "key": key,
                "home": value.get("home", "-") or "-",
                "away": value.get("away", "-") or "-",
            }
        )
    minute = f"{state.minute}'" if isinstance(state.minute, int) else (state.time_text or "-")
    return {
        "league": state.league or "",
        "home_team": state.home_team or "",
        "away_team": state.away_team or "",
        "minute": minute,
        "score": state.score or "0 x 0",
        "url": _safe_match_url(state.url),
        "on_target_home": stats.get("On Target", {}).get("home", "-"),
        "on_target_away": stats.get("On Target", {}).get("away", "-"),
        "corners_home": stats.get("Corners", {}).get("home", "-"),
        "corners_away": stats.get("Corners", {}).get("away", "-"),
        "dangerous_home": stats.get("Dangerous Attacks", {}).get("home", "-"),
        "dangerous_away": stats.get("Dangerous Attacks", {}).get("away", "-"),
        "stats_list": stats_list,
    }


def _parse_score_pair(text: str) -> tuple[int, int]:
    try:
        left, right = str(text or "0 x 0").replace("-", "x").replace(":", "x").split("x", 1)
        return int(left.strip()), int(right.strip())
    except Exception:
        return 0, 0


def _is_upcoming_match_today(match: dict, current_time: datetime) -> bool:
    """Hide past scheduled fixtures without dropping live/unknown-time games."""
    if match.get("is_live"):
        return True
    clock = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*", str(match.get("time") or ""))
    if not clock:
        return True
    hour, minute = int(clock.group(1)), int(clock.group(2))
    if hour > 23 or minute > 59:
        return True
    return (hour, minute) >= (current_time.hour, current_time.minute)


def _favorite_live_leagues() -> list[str]:
    try:
        data = json.loads(current_user.favorite_live_leagues_json or "[]")
    except Exception:
        data = []
    return [str(item) for item in data if str(item).strip()]


def _alert_profit(alert) -> float:
    if alert.stake_amount is None or alert.stake_odd is None:
        return 0.0
    if alert.bet_tracking_type:
        return float(alert.bet_profit or 0)
    if alert.status == "green":
        return float(alert.stake_amount) * (float(alert.stake_odd) - 1)
    if alert.status == "red":
        return -float(alert.stake_amount)
    return 0.0


def _ticket_profit(ticket) -> float:
    if ticket.status == "green":
        return float(ticket.stake_amount) * (float(ticket.total_odd) - 1)
    if ticket.status == "red":
        return -float(ticket.stake_amount)
    return 0.0


def _optional_float(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _current_matchday_live_matches() -> list[dict]:
    cutoff = now_sp() - timedelta(minutes=15)
    states = (
        LiveGameState.query.filter(LiveGameState.updated_at >= cutoff)
        .order_by(LiveGameState.updated_at.desc())
        .limit(600)
        .all()
    )
    matches = {}
    for state in states:
        time_text = (state.time_text or "").strip()
        if time_text.casefold() in {"ft", "finished", "ended", "encerrado"}:
            continue
        if _is_excluded_match(state.league, state.home_team, state.away_team):
            continue
        match = {
            "game_id": str(state.game_id),
            "url": _safe_match_url(state.url),
            "time": time_text or (f"{state.minute}'" if state.minute is not None else "Ao vivo"),
            "day": now_sp().strftime("%Y-%m-%d"),
            "league": state.league or "",
            "home_team": state.home_team or "",
            "away_team": state.away_team or "",
            "is_live": True,
            "score": state.score or "0 x 0",
        }
        matches[match["game_id"]] = match
    return list(matches.values())


def _find_matchday_match(day: str, game_id: str):
    match = find_match(day, game_id)
    if match:
        return match
    if day == now_sp().strftime("%Y-%m-%d"):
        return next(
            (item for item in _current_matchday_live_matches() if item["game_id"] == str(game_id)),
            None,
        )
    return None


@main_bp.route("/")
def dashboard():
    if not current_user.is_authenticated:
        return render_template("landing.html")
    dashboard_view = request.args.get("view", "rules")
    if dashboard_view not in {"rules", "finance"}:
        dashboard_view = "rules"
    now = now_sp()
    start_day = datetime(now.year, now.month, now.day)
    end_day = start_day + timedelta(days=1)

    def percentage(part, total, digits=1):
        """Return a database-backed rate that is always a valid percentage."""
        if not total:
            return 0
        return round(max(0.0, min(100.0, float(part) / float(total) * 100)), digits)

    total_rules = 0
    active_rules = 0
    today_rows = db.session.query(MatchAlert.status, func.count(MatchAlert.id)).filter(
        MatchAlert.user_id == current_user.id,
        MatchAlert.created_at >= start_day,
        MatchAlert.created_at < end_day,
    ).group_by(MatchAlert.status).all()
    today_counts = {status: count for status, count in today_rows}
    alerts_today = sum(today_counts.values())
    pending_alerts = today_counts.get("pending", 0)
    last_alert = last_green = last_red = None
    greens = today_counts.get("green", 0)
    reds = today_counts.get("red", 0)
    yesterday_start = start_day - timedelta(days=1)
    yesterday_rows = db.session.query(MatchAlert.status, func.count(MatchAlert.id)).filter(
        MatchAlert.user_id == current_user.id,
        MatchAlert.created_at >= yesterday_start,
        MatchAlert.created_at < start_day,
    ).group_by(MatchAlert.status).all()
    yesterday_counts = {status: count for status, count in yesterday_rows}
    alerts_yesterday = sum(yesterday_counts.values())
    greens_yesterday = yesterday_counts.get("green", 0)

    green_source = request.args.get("green_source", "alerts")
    if green_source not in {"alerts", "tickets"}:
        green_source = "alerts"
    ticket_green_today = SavedTicket.query.filter(
        SavedTicket.user_id == current_user.id,
        SavedTicket.status == "green",
        SavedTicket.created_at >= start_day,
        SavedTicket.created_at < end_day,
    ).count()
    ticket_green_yesterday = SavedTicket.query.filter(
        SavedTicket.user_id == current_user.id,
        SavedTicket.status == "green",
        SavedTicket.created_at >= yesterday_start,
        SavedTicket.created_at < start_day,
    ).count()
    if green_source == "tickets":
        green_kpi_value = ticket_green_today
        green_kpi_delta = ticket_green_today - ticket_green_yesterday
    else:
        green_kpi_value = greens
        green_kpi_delta = greens - greens_yesterday

    recent_alerts = (
        MatchAlert.query.filter_by(user_id=current_user.id)
        .order_by(MatchAlert.created_at.desc())
        .limit(10)
        .all()
    )

    since = start_day - timedelta(days=6)
    daily_rows = db.session.query(
        func.date(MatchAlert.created_at), MatchAlert.status, func.count(MatchAlert.id)
    ).filter(
        MatchAlert.user_id == current_user.id, MatchAlert.created_at >= since
    ).group_by(func.date(MatchAlert.created_at), MatchAlert.status).all()
    daily = {}
    for key, status, row_count in daily_rows:
        daily.setdefault(key, {"green": 0, "red": 0, "pending": 0})
        daily[key][status] = row_count
    chart_days = []
    max_count = 1
    for i in range(6, -1, -1):
        day = (now - timedelta(days=i)).strftime("%Y-%m-%d")
        counts = daily.get(day, {"green": 0, "red": 0, "pending": 0})
        total = counts["green"] + counts["red"] + counts["pending"]
        max_count = max(max_count, total)
        chart_days.append({"day": day[5:], "counts": counts, "total": total})
        resolved_day = counts["green"] + counts["red"]
        chart_days[-1]["win_rate"] = percentage(counts["green"], resolved_day)

    finance_period = request.args.get("finance_period", "today")
    finance_periods = {
        "today": (start_day, end_day, "resultado financeiro de hoje"),
        "7d": (start_day - timedelta(days=6), end_day, "resultado financeiro dos últimos 7 dias"),
        "15d": (start_day - timedelta(days=14), end_day, "resultado financeiro dos últimos 15 dias"),
        "30d": (start_day - timedelta(days=29), end_day, "resultado financeiro dos últimos 30 dias"),
        "all": (None, None, "resultado financeiro desde o começo"),
    }
    if finance_period not in finance_periods:
        finance_period = "today"
    finance_start, finance_end, finance_period_caption = finance_periods[finance_period]

    finance_alert_query = MatchAlert.query.filter(
        MatchAlert.user_id == current_user.id,
        MatchAlert.stake_amount.isnot(None),
        MatchAlert.stake_odd.isnot(None),
    )
    finance_ticket_query = SavedTicket.query.filter(SavedTicket.user_id == current_user.id)
    if finance_start is not None:
        finance_alert_query = finance_alert_query.filter(
            MatchAlert.created_at >= finance_start,
            MatchAlert.created_at < finance_end,
        )
        finance_ticket_query = finance_ticket_query.filter(
            SavedTicket.created_at >= finance_start,
            SavedTicket.created_at < finance_end,
        )
    finance_alerts = finance_alert_query.options(
        load_only(MatchAlert.status, MatchAlert.stake_amount, MatchAlert.stake_odd)
    ).all()
    finance_tickets = finance_ticket_query.all()
    financial_profit = round(sum(_alert_profit(alert) for alert in finance_alerts) + sum(_ticket_profit(ticket) for ticket in finance_tickets), 2)
    financial_staked = round(sum(float(alert.stake_amount or 0) for alert in finance_alerts) + sum(float(ticket.stake_amount or 0) for ticket in finance_tickets), 2)
    financial_bets = len(finance_alerts) + len(finance_tickets)
    finance_days = []
    max_finance_abs = 1

    top_rules = []
    finished_labels = {"ft", "finished", "ended", "encerrado"}
    ticket_rows = db.session.query(SavedTicket.status, func.count(SavedTicket.id)).filter(
        SavedTicket.user_id == current_user.id
    ).group_by(SavedTicket.status).all()
    ticket_counts = {(status or "").casefold(): count for status, count in ticket_rows}
    total_tickets = sum(ticket_counts.values())
    green_tickets = ticket_counts.get("green", 0)
    normalized_live_time = func.lower(func.trim(func.coalesce(LiveGameState.time_text, "")))
    live_games = LiveGameState.query.filter(
        LiveGameState.updated_at >= now - timedelta(minutes=15),
        ~normalized_live_time.in_(finished_labels),
    ).count()
    league_rows = db.session.query(MatchAlert.league, func.count(MatchAlert.id)).filter(
        MatchAlert.user_id == current_user.id, MatchAlert.created_at >= since
    ).group_by(MatchAlert.league).all()
    league_totals = {(league or "Sem liga").strip(): count for league, count in league_rows}
    active_leagues = len(league_totals)
    sorted_leagues = sorted(league_totals.items(), key=lambda item: (-item[1], item[0].casefold()))[:5]
    largest_league = max((count for _, count in sorted_leagues), default=1)
    top_leagues = [
        {"name": name, "count": count, "width": round(count / largest_league * 100)}
        for name, count in sorted_leagues
    ]
    market_definitions = [
        ("Gols", ("goal", "over", "score"), "#00e5a8"),
        ("Escanteios", ("corner",), "#3b82f6"),
        ("Cartões", ("card",), "#f6b817"),
        ("Chutes", ("shot", "on_target"), "#8b5cf6"),
        ("Faltas", ("foul",), "#20c997"),
        ("Outros", (), "#ff4057"),
    ]
    market_rows = db.session.query(
        MatchAlert.status,
        MatchAlert.market_key,
        func.count(MatchAlert.id),
    ).filter(
        MatchAlert.user_id == current_user.id,
        MatchAlert.created_at >= now - timedelta(days=30),
        MatchAlert.status.in_(("green", "red")),
    ).group_by(MatchAlert.status, MatchAlert.market_key).all()
    market_buckets = {label: {"green": 0, "resolved": 0} for label, _, _ in market_definitions}
    for status, market_key, row_count in market_rows:
        key = (market_key or "").casefold()
        label = next((name for name, terms, _ in market_definitions[:-1] if any(term in key for term in terms)), "Outros")
        market_buckets[label]["resolved"] += row_count
        if status == "green":
            market_buckets[label]["green"] += row_count
    market_rates = []
    for label, _, color in market_definitions:
        bucket = market_buckets[label]
        rate = percentage(bucket["green"], bucket["resolved"])
        market_rates.append((label, rate, color))
    market_resolved = sum(row_count for _, _, row_count in market_rows)
    market_greens = sum(row_count for status, _, row_count in market_rows if status == "green")
    market_win_rate = percentage(market_greens, market_resolved)
    recent_green = sum(1 for alert in recent_alerts if alert.status == "green")
    recent_red = sum(1 for alert in recent_alerts if alert.status == "red")
    recent_pending = sum(1 for alert in recent_alerts if alert.status == "pending")
    status_totals = {status: count for _, status, count in daily_rows}
    recent_greens = sum(count for _, status, count in daily_rows if status == "green")
    recent_resolved = recent_greens + sum(count for _, status, count in daily_rows if status == "red")
    dashboard_win_rate = percentage(recent_greens, recent_resolved)
    win_rate_source = request.args.get("win_rate_source", "alerts")
    if win_rate_source not in {"alerts", "tickets"}:
        win_rate_source = "alerts"
    ticket_rate_rows = db.session.query(SavedTicket.status, func.count(SavedTicket.id)).filter(
        SavedTicket.user_id == current_user.id,
        SavedTicket.created_at >= since,
        SavedTicket.status.in_(("green", "red")),
    ).group_by(SavedTicket.status).all()
    ticket_rate_counts = {(status or "").casefold(): count for status, count in ticket_rate_rows}
    ticket_rate_greens = ticket_rate_counts.get("green", 0)
    ticket_rate_resolved = ticket_rate_greens + ticket_rate_counts.get("red", 0)
    if win_rate_source == "tickets":
        win_rate_kpi = percentage(ticket_rate_greens, ticket_rate_resolved)
        win_rate_kpi_resolved = ticket_rate_resolved
    else:
        win_rate_kpi = dashboard_win_rate
        win_rate_kpi_resolved = recent_resolved

    dashboard_changes = {
        # These are count differences, not success rates. Showing them as a
        # percentage produced misleading values such as 300% and 400%.
        "alerts": alerts_today - alerts_yesterday,
        "greens": greens - greens_yesterday,
    }
    worker_status = get_api_status()

    def fresh_status(timestamp, maximum_age_seconds):
        if not timestamp:
            return False
        try:
            checked = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        except (TypeError, ValueError):
            return False
        return 0 <= (now.replace(tzinfo=None) - checked).total_seconds() <= maximum_age_seconds

    api_ok = worker_status.get("ok") is True and fresh_status(worker_status.get("checked_at"), 300)
    collector_ok = fresh_status(worker_status.get("last_cycle"), 300)
    telegram_ok = bool(current_user.telegram_verified and current_user.telegram_token and current_user.telegram_chat_id)
    system_services = [
        {
            "name": "API Odds",
            "ok": api_ok,
            "detail": f"HTTP {worker_status.get('code')}" if worker_status.get("code") else "Sem resposta",
            "checked": worker_status.get("checked_at") or "Ainda não verificada",
        },
        {
            "name": "Coleta de Dados",
            "ok": collector_ok,
            "detail": "Worker em execução" if collector_ok else "Aguardando primeiro ciclo",
            "checked": worker_status.get("last_cycle") or "Sem ciclo registrado",
        },
        {
            "name": "Configuração Telegram",
            "ok": telegram_ok,
            "detail": "Verificado" if telegram_ok else "Não configurado",
            "checked": "Conta do usuário",
        },
        {"name": "Banco de Dados", "ok": True, "detail": "Conectado", "checked": "Consulta concluída"},
    ]
    system_all_ok = all(item["ok"] for item in system_services)

    return render_template(
        "dashboard.html",
        active_rules=active_rules,
        total_rules=total_rules,
        alerts_today=alerts_today,
        greens=greens,
        green_source=green_source,
        green_kpi_value=green_kpi_value,
        green_kpi_delta=green_kpi_delta,
        reds=reds,
        pending_alerts=pending_alerts,
        last_alert=last_alert,
        last_green=last_green,
        last_red=last_red,
        recent_alerts=recent_alerts,
        chart_days=chart_days,
        max_count=max_count,
        top_rules=top_rules,
        worker_status=worker_status,
        dashboard_view=dashboard_view,
        financial_profit=financial_profit,
        finance_period=finance_period,
        finance_period_caption=finance_period_caption,
        financial_staked=financial_staked,
        financial_bets=financial_bets,
        finance_days=finance_days,
        max_finance_abs=max_finance_abs,
        total_tickets=total_tickets,
        green_tickets=green_tickets,
        live_games=live_games,
        active_leagues=active_leagues,
        top_leagues=top_leagues,
        market_rates=market_rates,
        market_win_rate=market_win_rate,
        recent_green=recent_green,
        recent_red=recent_red,
        recent_pending=recent_pending,
        recent_resolved=recent_resolved,
        dashboard_win_rate=dashboard_win_rate,
        win_rate_source=win_rate_source,
        win_rate_kpi=win_rate_kpi,
        win_rate_kpi_resolved=win_rate_kpi_resolved,
        dashboard_changes=dashboard_changes,
        system_services=system_services,
        system_all_ok=system_all_ok,
    )


@main_bp.route("/robots.txt")
def robots_txt():
    body = "\n".join(
        [
            "User-agent: *",
            "Allow: /",
            "",
            "Disallow: /admin",
            "Disallow: /dashboard",
            "Disallow: /auth/login",
            "Disallow: /logout",
            "Disallow: /api",
            "Disallow: /__pycache__/",
            "Disallow: /static/uploads/",
            "",
            "Sitemap: https://greenhunter.com.br/sitemap.xml",
            "",
        ]
    )
    return Response(body, mimetype="text/plain")


@main_bp.route("/sitemap.xml")
def sitemap_xml():
    now_iso = now_sp().date().isoformat()
    site_url = "https://greenhunter.com.br"
    urls = [
        {"loc": f"{site_url}/", "priority": "1.0", "changefreq": "daily"},
        {"loc": f"{site_url}/auth/register", "priority": "0.8", "changefreq": "weekly"},
    ]
    entries = []
    for item in urls:
        entries.append(
            "  <url>\n"
            f"    <loc>{item['loc']}</loc>\n"
            f"    <lastmod>{now_iso}</lastmod>\n"
            f"    <changefreq>{item['changefreq']}</changefreq>\n"
            f"    <priority>{item['priority']}</priority>\n"
            "  </url>"
        )
    body = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(entries)
        + "\n</urlset>\n"
    )
    return Response(body, mimetype="application/xml")


@main_bp.route("/api/status")
@login_required
def api_status():
    return jsonify(get_api_status())


@main_bp.route("/bilhetes")
@login_required
def saved_tickets():
    tickets = (
        SavedTicket.query.filter_by(user_id=current_user.id)
        .order_by(SavedTicket.created_at.desc()).limit(current_user.saved_ticket_limit).all()
    )
    shareable_ticket_ids = {ticket.id for ticket in tickets if ticket_is_shareable(ticket)}
    return render_template("tickets/list.html", tickets=tickets, shareable_ticket_ids=shareable_ticket_ids)


@main_bp.post("/bilhetes/<int:ticket_id>/compartilhar")
@login_required
def share_saved_ticket(ticket_id):
    ticket = SavedTicket.query.filter_by(id=ticket_id, user_id=current_user.id).first_or_404()
    if not ticket_is_shareable(ticket):
        flash("Este bilhete não pode mais ser compartilhado: alguma seleção iniciou ou deixou de estar pendente.", "warning")
        return redirect(url_for("main.saved_tickets"))
    recipient = find_recipient(request.form.get("recipient"), current_user.id)
    if recipient is None:
        flash("Usuário não encontrado. Confira o ID ou nome informado.", "warning")
        return redirect(url_for("main.saved_tickets"))
    existing = SharedInvitation.query.filter_by(
        sender_id=current_user.id, recipient_id=recipient.id, item_type="ticket",
        source_id=ticket.id, status="pending",
    ).first()
    if existing:
        flash(f"{recipient.username} já possui um convite pendente para este bilhete.", "info")
        return redirect(url_for("main.saved_tickets"))
    db.session.add(SharedInvitation(
        sender_id=current_user.id, recipient_id=recipient.id, item_type="ticket",
        source_id=ticket.id, item_name=ticket.name, payload_json=encode_snapshot(ticket_snapshot(ticket)),
    ))
    db.session.commit()
    flash(f"Convite do bilhete enviado para {recipient.username}.", "success")
    return redirect(url_for("main.saved_tickets"))


@main_bp.post("/compartilhamentos/<int:invitation_id>/<decision>")
@login_required
def respond_shared_invitation(invitation_id, decision):
    invitation = SharedInvitation.query.filter_by(
        id=invitation_id, recipient_id=current_user.id, status="pending"
    ).first_or_404()
    if decision not in {"accept", "reject"}:
        abort(404)
    if decision == "reject":
        invitation.status = "rejected"; invitation.responded_at = now_sp(); db.session.commit()
        flash("Convite recusado.", "info")
        return redirect(request.referrer or url_for("main.dashboard"))
    try:
        payload = json.loads(invitation.payload_json)
        if invitation.item_type == "rule":
            visible_rules = Rule.query.filter_by(user_id=current_user.id).count()
            if not current_user.is_admin_user and visible_rules >= current_user.effective_rule_limit:
                flash("Seu limite de regras foi atingido. Exclua uma regra antes de aceitar.", "warning")
                return redirect(url_for("rules.list_rules"))
            created = accept_rule_snapshot(payload, current_user.id)
            destination = url_for("rules.edit_rule", rule_id=created.id)
        elif invitation.item_type == "ticket":
            if not ticket_snapshot_is_pregame(payload):
                invitation.status = "expired"; invitation.responded_at = now_sp(); db.session.commit()
                flash("O convite expirou porque um dos jogos já começou.", "warning")
                return redirect(url_for("main.saved_tickets"))
            if SavedTicket.query.filter_by(user_id=current_user.id).count() >= current_user.saved_ticket_limit:
                flash("Seu limite de bilhetes foi atingido.", "warning")
                return redirect(url_for("main.saved_tickets"))
            created = accept_ticket_snapshot(payload, current_user.id)
            destination = url_for("main.saved_tickets")
        else:
            abort(400)
        invitation.status = "accepted"; invitation.responded_at = now_sp(); db.session.commit()
        flash(f"{invitation.item_name} foi adicionado à sua conta.", "success")
        return redirect(destination)
    except (TypeError, ValueError, json.JSONDecodeError):
        db.session.rollback()
        flash("Não foi possível aceitar este compartilhamento.", "danger")
        return redirect(request.referrer or url_for("main.dashboard"))


@main_bp.route("/bilhetes/<int:ticket_id>/editar", methods=["GET", "POST"])
@login_required
def edit_saved_ticket(ticket_id):
    ticket = SavedTicket.query.filter_by(id=ticket_id, user_id=current_user.id).first_or_404()
    if ticket.status != "pending" and not current_user.is_admin_user:
        flash("Bilhetes já finalizados não podem ser alterados.", "warning")
        return redirect(url_for("main.saved_tickets"))
    if request.method == "GET":
        line_options = {}
        for leg in ticket.legs:
            key = (leg.market_key or "").casefold()
            maximum = 20.5 if "corner" in key else 12.5 if "card" in key else 8.5 if key in {"over05", "over15", "over25"} or key.startswith("goals_") else 30.5
            options = [index + 0.5 for index in range(int(maximum + 0.5))]
            if leg.target_line is not None and float(leg.target_line) not in options:
                options.append(float(leg.target_line))
                options.sort()
            line_options[leg.id] = options
        return render_template("tickets/edit.html", ticket=ticket, line_options=line_options)

    try:
        odd = float((request.form.get("total_odd") or "").replace(",", "."))
        stake = float((request.form.get("stake_amount") or "").replace(",", "."))
    except ValueError:
        flash("Informe uma odd e um valor apostado válidos.", "warning")
        return redirect(url_for("main.edit_saved_ticket", ticket_id=ticket.id))
    if odd <= 1 or stake <= 0:
        flash("A odd deve ser maior que 1 e o valor deve ser maior que zero.", "warning")
        return redirect(url_for("main.edit_saved_ticket", ticket_id=ticket.id))

    kept = []
    line_changed = False
    for leg in ticket.legs:
        if request.form.get(f"remove_leg_{leg.id}"):
            db.session.delete(leg)
            continue
        raw_line = (request.form.get(f"line_{leg.id}") or "").strip().replace(",", ".")
        if leg.market_key != "goal_ht":
            try:
                line = float(raw_line)
            except ValueError:
                flash(f"Linha inválida em {leg.market_label}.", "warning")
                db.session.rollback()
                return redirect(url_for("main.edit_saved_ticket", ticket_id=ticket.id))
            if line < 0:
                flash("A linha do mercado não pode ser negativa.", "warning")
                db.session.rollback()
                return redirect(url_for("main.edit_saved_ticket", ticket_id=ticket.id))
            if leg.target_line is None or abs(float(leg.target_line) - line) > .0001:
                line_changed = True
                leg.status = "pending"
                leg.checked_at = None
            leg.target_line = line
            formatted = f"{line:.1f}".replace(".", ",")
            if re.search(r"(?:mais de|acima de)\s*\d+(?:[.,]\d+)?", leg.market_label, re.I):
                leg.market_label = re.sub(
                    r"((?:mais de|acima de)\s*)\d+(?:[.,]\d+)?",
                    rf"\g<1>{formatted}", leg.market_label, count=1, flags=re.I,
                )
        kept.append(leg)
    if not kept:
        db.session.rollback()
        flash("O bilhete precisa manter pelo menos uma opção.", "warning")
        return redirect(url_for("main.edit_saved_ticket", ticket_id=ticket.id))
    ticket.total_odd = round(odd, 3)
    ticket.stake_amount = round(stake, 2)
    statuses = [leg.status for leg in kept]
    if "red" in statuses:
        ticket.status = "red"
        ticket.profit = -ticket.stake_amount
    elif statuses and all(status == "green" for status in statuses):
        ticket.status = "green"
        ticket.profit = round(ticket.stake_amount * (ticket.total_odd - 1), 2)
    else:
        ticket.status = "pending"
        ticket.profit = 0
    ticket.resolved_at = now_sp() if ticket.status != "pending" else None
    db.session.commit()
    suffix = " A linha alterada será revalidada." if line_changed else ""
    flash(f"{ticket.name} atualizado com sucesso.{suffix}", "success")
    return redirect(url_for("main.saved_tickets"))


@main_bp.route("/api/bilhetes", methods=["POST"])
@login_required
def save_ticket():
    payload = request.get_json(silent=True) or {}
    if not current_user.is_premium_user and SavedTicket.query.filter_by(user_id=current_user.id).count() >= current_user.saved_ticket_limit:
        return jsonify(ok=False, message="O plano Free permite até 5 bilhetes salvos. Assine o Pro para continuar."), 403
    try:
        odd = float(str(payload.get("odd", "")).replace(",", "."))
        stake = float(str(payload.get("stake", "")).replace(",", "."))
    except (TypeError, ValueError):
        return jsonify(ok=False, message="Informe uma odd e um valor apostado válidos."), 400
    if odd <= 1 or stake <= 0:
        return jsonify(ok=False, message="A odd deve ser maior que 1 e o valor deve ser maior que zero."), 400
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        return jsonify(ok=False, message="O bilhete está vazio."), 400
    game_count = len({str(item.get("gameId") or "") for item in items})
    if game_count > current_user.generated_ticket_game_limit:
        return jsonify(ok=False, message=f"Seu plano permite até {current_user.generated_ticket_game_limit} jogos por bilhete."), 403

    ticket = SavedTicket(
        user_id=current_user.id,
        name=f"Bilhete #{SavedTicket.query.filter_by(user_id=current_user.id).count() + 1}",
        total_odd=round(odd, 3), stake_amount=round(stake, 2), status="pending",
    )
    db.session.add(ticket)
    db.session.flush()
    item_limit = 500 if current_user.is_admin_user else 100
    for item in items[:item_limit]:
        market_key = str(item.get("generatedMarket") or item.get("marketKey") or "").strip()
        label = str(item.get("market") or "Mercado").strip()[:160]
        line = item.get("selectedLine")
        if line is None:
            found = re.search(r"(?:mais de|acima de)\s*(\d+(?:[.,]\d+)?)", label, re.I)
            line = found.group(1).replace(",", ".") if found else None
        try:
            line = float(line) if line is not None else None
        except (TypeError, ValueError):
            line = None
        if not market_key:
            lower = label.casefold()
            market_key = "over05" if "0,5" in lower else "over15" if "1,5" in lower else "over25" if "2,5" in lower else "goal_ht" if "1º tempo" in lower else ""
        if market_key.endswith("_home"):
            side = "home"
        elif market_key.endswith("_away"):
            side = "away"
        else:
            group = str(item.get("group") or "").casefold()
            side = "home" if "casa" in group and "fora" not in group else "away" if "fora" in group and "casa" not in group else "total"
        if not market_key or (market_key not in {"goal_ht", "over05", "over15", "over25", "under15", "under25", "under35", "btts", "btts_yes"} and line is None):
            db.session.rollback()
            return jsonify(ok=False, message=f"Defina uma linha de aposta para “{label}” antes de salvar."), 400
        db.session.add(SavedTicketLeg(
            ticket_id=ticket.id, game_id=str(item.get("gameId") or "")[:32],
            game_day=str(item.get("day") or "")[:10], game_time=str(item.get("time") or "")[:40],
            league=str(item.get("league") or "")[:120], home_team=str(item.get("home") or "")[:120],
            away_team=str(item.get("away") or "")[:120], market_key=market_key[:64],
            market_label=label, target_side=side, target_line=line,
            samples=int(item.get("samples") or 0), source_group=str(item.get("group") or "")[:120],
            predicted_probability=_optional_float(item.get("adjustedProbability") or item.get("historicalFrequency")),
            confidence_score=_optional_float(item.get("confidenceScore")),
            context_score=_optional_float(item.get("contextScore")),
            data_quality_score=_optional_float(item.get("dataQualityScore")),
            consistency_score=_optional_float(item.get("consistencyScore")),
            individual_odd=_optional_float(item.get("bet365Odd")),
            prediction_json=json.dumps({
                "strengths": item.get("strengths") or [],
                "weaknesses": item.get("weaknesses") or [],
                "source": item.get("group") or "",
                "sample_score": item.get("sampleScore"),
                "supporting_score": item.get("supportingScore"),
            }, ensure_ascii=False),
        ))
    db.session.commit()
    return jsonify(ok=True, ticket_id=ticket.id, name=ticket.name, message=f"{ticket.name} salvo e em acompanhamento.")


@main_bp.post("/api/prediction-runs")
@login_required
def create_prediction_run():
    """Start best-effort telemetry without participating in ticket generation."""
    try:
        run = start_prediction_run(current_user.id, request.get_json(silent=True) or {})
        return jsonify(ok=True, run_id=run.id, max_batch=MAX_CANDIDATES_PER_BATCH)
    except (TypeError, ValueError):
        db.session.rollback()
        return jsonify(ok=False), 400
    except Exception:
        db.session.rollback()
        current_app.logger.exception("Falha ao iniciar registro do gerador Legacy")
        return jsonify(ok=False), 503


def _owned_prediction_run(run_id: int):
    return PredictionRun.query.filter_by(id=run_id, user_id=current_user.id).first()


@main_bp.post("/api/prediction-runs/<int:run_id>/candidates")
@login_required
def append_prediction_run_candidates(run_id):
    run = _owned_prediction_run(run_id)
    if run is None or run.status != "running":
        return jsonify(ok=False), 404
    try:
        count = append_prediction_candidates(run, request.get_json(silent=True) or {})
        return jsonify(ok=True, stored=count)
    except (TypeError, ValueError):
        db.session.rollback()
        return jsonify(ok=False), 400
    except Exception as exc:
        current_app.logger.exception("Falha ao registrar candidatos do gerador Legacy")
        mark_prediction_run_failed(run_id, exc)
        return jsonify(ok=False), 503


@main_bp.post("/api/prediction-runs/<int:run_id>/finish")
@login_required
def finish_prediction_run_route(run_id):
    run = _owned_prediction_run(run_id)
    if run is None or run.status != "running":
        return jsonify(ok=False), 404
    try:
        finish_prediction_run(run, request.get_json(silent=True) or {})
        return jsonify(ok=True, run_id=run.id, candidates=run.candidate_count)
    except Exception as exc:
        current_app.logger.exception("Falha ao finalizar registro do gerador Legacy")
        mark_prediction_run_failed(run_id, exc)
        return jsonify(ok=False), 503


@main_bp.route("/undo/<token>", methods=["GET"])
@login_required
def undo_action(token):
    next_url = safe_redirect_target(
        request.args.get("next") or request.referrer,
        url_for("main.dashboard"),
    )
    ok, message = apply_undo(token, current_user.id)
    flash(message, "success" if ok else "warning")
    return redirect(next_url)


@main_bp.route("/live")
@login_required
def live():
    query = (request.args.get("q") or "").strip().lower()
    score_filter = (request.args.get("score") or "").strip()
    min_minute = request.args.get("min_minute", type=int)
    max_minute = request.args.get("max_minute", type=int)
    favorites_only = request.args.get("favorites") == "1"
    page = max(request.args.get("page", 1, type=int), 1)
    per_page = 24
    start_index = (page - 1) * per_page
    favorite_leagues = _favorite_live_leagues()
    favorite_set = {league.casefold() for league in favorite_leagues}

    recent_cutoff = now_sp() - timedelta(minutes=8)
    live_states = (
        LiveGameState.query.filter(LiveGameState.updated_at >= recent_cutoff)
        .order_by(LiveGameState.minute.desc(), LiveGameState.updated_at.desc())
        .limit(600)
        .all()
    )
    filtered_states = []
    for state in live_states:
        state_minute = state.minute if isinstance(state.minute, int) else None
        if min_minute is not None and (state_minute is None or state_minute < min_minute):
            continue
        if max_minute is not None and (state_minute is None or state_minute > max_minute):
            continue
        if score_filter and (state.score or "") != score_filter:
            continue
        if favorites_only and (state.league or "").casefold() not in favorite_set:
            continue
        hay = " ".join(
            [
                state.league or "",
                state.home_team or "",
                state.away_team or "",
            ]
        ).lower()
        if query and query not in hay:
            continue
        filtered_states.append(state)

    page_states = filtered_states[start_index : start_index + per_page]
    available_leagues = sorted({state.league for state in live_states if state.league})
    query_args = request.args.to_dict()
    query_args.pop("page", None)
    return render_template(
        "live/list.html",
        matches=[_live_match_from_state(state) for state in page_states],
        query=query,
        score_filter=score_filter,
        min_minute=min_minute,
        max_minute=max_minute,
        favorites_only=favorites_only,
        favorite_leagues=favorite_leagues,
        available_leagues=available_leagues,
        query_args=query_args,
        status_code=200,
        page=page,
        has_prev=page > 1,
        has_next=len(filtered_states) > start_index + per_page,
    )


@main_bp.route("/jogos-do-dia")
@login_required
def matchday():
    current_time = now_sp()
    today = current_time.strftime("%Y-%m-%d")
    day = (request.args.get("day") or today).strip()
    try:
        datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        day = today
    payload = get_matchday(day, force_refresh=request.args.get("refresh") == "1")
    include_live = request.args.get("live") == "1"
    all_matches = list(payload["matches"])
    if day == today:
        all_matches = [match for match in all_matches if _is_upcoming_match_today(match, current_time)]
    if include_live and day == today:
        merged = {match["game_id"]: dict(match) for match in all_matches}
        for live_match in _current_matchday_live_matches():
            merged[live_match["game_id"]] = live_match
        all_matches = sorted(
            merged.values(),
            key=lambda match: (not match.get("is_live", False), match.get("time") or "", match.get("league") or ""),
        )
    attach_qualplacar_odds(all_matches, day)
    available_leagues = list(
        dict.fromkeys(match["league"] for match in all_matches if match.get("league"))
    )
    league_preferences = _relevant_league_preferences()
    global_relevant_leagues = _global_relevant_names(available_leagues, league_preferences)
    user_preference = _user_matchday_preference()
    personal_leagues = None
    decoded_leagues = []
    if user_preference and user_preference.relevant_leagues_json:
        try:
            decoded_leagues = json.loads(user_preference.relevant_leagues_json)
            if isinstance(decoded_leagues, list):
                personal_leagues = {_normalized_league_name(value) for value in decoded_leagues}
        except (TypeError, ValueError):
            personal_leagues = None
    relevant_leagues = global_relevant_leagues if personal_leagues is None else [
        league for league in available_leagues if _normalized_league_name(league) in personal_leagues
    ]
    all_known_leagues = list(dict.fromkeys([
        *known_matchday_leagues(), *available_leagues,
        *[str(value) for value in decoded_leagues]
    ]))
    configured_relevant_leagues = _global_relevant_names(all_known_leagues, league_preferences) if personal_leagues is None else [
        league for league in all_known_leagues if _normalized_league_name(league) in personal_leagues
    ]
    global_configured_relevant_leagues = _global_relevant_names(all_known_leagues, league_preferences)
    team_leagues = {}
    for match in all_matches:
        league = match.get("league") or ""
        for team in (match.get("home_team"), match.get("away_team")):
            if team:
                team_leagues.setdefault(team, set()).add(league)
    available_teams = list(team_leagues)
    selected_leagues = [value for value in request.args.getlist("league") if value in available_leagues]
    selected_teams = [value for value in request.args.getlist("team") if value in available_teams]
    relevant_selection_active = bool(selected_leagues) and bool(relevant_leagues) and set(relevant_leagues).issubset(selected_leagues)
    selected_league_keys = {value.casefold() for value in selected_leagues}
    selected_team_keys = {value.casefold() for value in selected_teams}
    matches = all_matches
    if selected_league_keys:
        matches = [match for match in matches if (match.get("league") or "").casefold() in selected_league_keys]
    if selected_team_keys:
        matches = [
            match for match in matches
            if (match.get("home_team") or "").casefold() in selected_team_keys
            or (match.get("away_team") or "").casefold() in selected_team_keys
        ]
    trend_market = (request.args.get("trend_market") or "").strip()
    if trend_market not in {
        "goal_ht", "over05", "over15", "over25", "corners_10_over1", "corners_avg",
        "cards_avg", "offsides_avg", "shots_avg", "shots_on_target_avg", "fouls_avg",
    }:
        trend_market = ""
    trend_group = (request.args.get("trend_group") or "best").strip()
    if trend_group not in {"best", "H2H", "Mandante", "Visitante"}:
        trend_group = "best"
    trend_min = max(0, min(100, request.args.get("trend_min", 0, type=int)))
    trend_max = max(trend_min, min(100, request.args.get("trend_max", 100, type=int)))
    sample_cap = current_user.matchday_sample_limit
    trend_limit = max(1, min(sample_cap, request.args.get("trend_limit", sample_cap, type=int)))
    trend_active = bool(trend_market)
    ticket_generator_active = request.args.get("ticket_generator") == "1"
    if ticket_generator_active and not current_user.is_premium_user:
        generation_key = f"free_ticket_generation:{current_user.id}"
        usage = session.get(generation_key)
        used_today = int(usage.get("count") or 0) if isinstance(usage, dict) and usage.get("day") == today else 0
        request_id = str(request.args.get("generator_request_id") or "").strip()[:100]
        previous_ids = list(usage.get("request_ids") or []) if isinstance(usage, dict) and usage.get("day") == today else []
        repeated_request = bool(request_id and request_id in previous_ids)
        if used_today >= 2 and not repeated_request:
            flash("O plano Free permite duas gerações de bilhete por dia.", "warning")
            return redirect(url_for("main.matchday", day=day))
        if not repeated_request:
            if request_id:
                previous_ids = [*previous_ids[-4:], request_id]
            session[generation_key] = {"day": today, "count": used_today + 1, "request_ids": previous_ids}
    trend_prefetch = {}
    generated_goal_suggestions = {}
    if ticket_generator_active:
        generator_samples = max(3, min(sample_cap, request.args.get("generator_samples", sample_cap, type=int)))
        generator_count = max(1, min(current_user.generated_ticket_game_limit, request.args.get("generator_count", 3, type=int)))
        generator_markets = set(request.args.getlist("generator_market"))
        goal_markets = generator_markets & {"over05", "over15", "over25"}
        trend_index = load_matchday_trend_index(day)
        # O ranking global precisa receber toda a agenda elegível. O índice
        # diário continua sendo reutilizado como prefetch, mas nunca elimina um
        # jogo antes que os BetCandidates sejam avaliados.
        matches = [match for match in matches if not match.get("is_live")]
        if trend_index.get("complete") and goal_markets:
            trend_prefetch = {
                str(match.get("game_id")): trend_groups_for_match(trend_index, match.get("game_id"), generator_samples)
                for match in matches
            }
    if trend_active and trend_market in {"over05", "over15", "over25"}:
        trend_index = load_matchday_trend_index(day)
        if trend_index.get("complete"):
            qualifying = []
            for match in matches:
                groups = trend_groups_for_match(trend_index, match.get("game_id"), trend_limit)
                candidates = ("H2H", "Mandante", "Visitante") if trend_group == "best" else (trend_group,)
                values = [
                    groups[key].get(trend_market)
                    for key in candidates
                    if int(groups[key].get("count") or 0) >= trend_limit
                    and groups[key].get(trend_market) is not None
                ]
                if any(trend_min <= int(value) <= trend_max for value in values):
                    qualifying.append(match)
                    trend_prefetch[str(match.get("game_id"))] = groups
            matches = qualifying
    page = max(request.args.get("page", 1, type=int), 1)
    per_page = 12
    start = (page - 1) * per_page
    total_matches = len(matches)
    # O filtro estatístico precisa avaliar a agenda completa da data, não
    # apenas os 12 cards da paginação comum.
    if not trend_active and not ticket_generator_active:
        matches = matches[start : start + per_page]
    card_sample_limit = (
        generator_samples if ticket_generator_active
        else trend_limit if trend_active
        else min(6, sample_cap)
    )
    # Entrega no HTML os resumos que o servidor já calculou. O cache é
    # compartilhado entre contas, portanto outro usuário não precisa iniciar
    # uma nova requisição/animação para cada card pronto.
    summary_prefetch = {}
    if not ticket_generator_active and not trend_active:
        for match in matches:
            cached = load_matchday_summary_cache(day, str(match.get("game_id")), card_sample_limit)
            if cached:
                summary_prefetch[str(match.get("game_id"))] = cached
    pagination_args = request.args.to_dict(flat=False)
    pagination_args.pop("page", None)
    trend_clear_args = request.args.to_dict(flat=False)
    for key in ("trend_market", "trend_group", "trend_min", "trend_max", "trend_limit", "page"):
        trend_clear_args.pop(key, None)
    prev_args = {**pagination_args, "page": page - 1}
    next_args = {**pagination_args, "page": page + 1}
    return render_template(
        "matchday/list.html",
        payload=payload,
        matches=matches,
        total_matches=total_matches,
        available_leagues=available_leagues,
        relevant_leagues=relevant_leagues,
        global_relevant_leagues=global_relevant_leagues,
        global_configured_relevant_leagues=global_configured_relevant_leagues,
        all_known_leagues=sorted(all_known_leagues, key=str.casefold),
        configured_relevant_leagues=configured_relevant_leagues,
        personal_relevant_active=personal_leagues is not None,
        matchday_market_settings=_market_settings(user_preference),
        default_matchday_market_settings=DEFAULT_MATCHDAY_MARKET_SETTINGS,
        relevant_selection_active=relevant_selection_active,
        available_teams=available_teams,
        team_leagues={
            team: [league for league in available_leagues if league in leagues]
            for team, leagues in team_leagues.items()
        },
        selected_leagues=selected_leagues,
        selected_teams=selected_teams,
        include_live=include_live,
        trend_active=trend_active,
        trend_market=trend_market,
        trend_group=trend_group,
        trend_min=trend_min,
        trend_max=trend_max,
        trend_limit=trend_limit,
        trend_prefetch=trend_prefetch,
        ticket_generator_active=ticket_generator_active,
        generated_goal_suggestions=generated_goal_suggestions,
        summary_prefetch=summary_prefetch,
        card_sample_limit=card_sample_limit,
        premium_access=current_user.is_premium_user,
        admin_access=current_user.is_admin_user,
        ticket_game_limit=current_user.generated_ticket_game_limit,
        day=day,
        page=page,
        has_prev=not trend_active and not ticket_generator_active and page > 1,
        has_next=not trend_active and not ticket_generator_active and total_matches > start + per_page,
        prev_args=prev_args,
        next_args=next_args,
        trend_clear_args=trend_clear_args,
    )


@main_bp.post("/jogos-do-dia/campeonato-principal")
@login_required
def toggle_matchday_relevant_league():
    if not current_user.is_admin_user:
        return jsonify(ok=False, message="Apenas administradores podem alterar os campeonatos principais."), 403
    payload = request.get_json(silent=True) or {}
    league = " ".join(str(payload.get("league") or "").strip().split())[:160]
    normalized = _normalized_league_name(league)
    if not normalized:
        return jsonify(ok=False, message="Campeonato inválido."), 400
    preference = MatchdayLeaguePreference.query.filter_by(normalized_name=normalized).first()
    current = preference.is_relevant if preference is not None else normalized in RELEVANT_MATCHDAY_LEAGUES
    if preference is None:
        preference = MatchdayLeaguePreference(
            normalized_name=normalized, display_name=league, is_relevant=not current
        )
        db.session.add(preference)
    else:
        preference.display_name = league
        preference.is_relevant = not current
    db.session.commit()
    return jsonify(ok=True, league=league, relevant=bool(preference.is_relevant))


@main_bp.post("/jogos-do-dia/preferencias")
@login_required
def save_matchday_preferences():
    payload = request.get_json(silent=True) or {}
    preference = _user_matchday_preference()
    if preference is None:
        preference = UserMatchdayPreference(user_id=current_user.id)
        db.session.add(preference)

    if payload.get("use_default_leagues") is True:
        preference.relevant_leagues_json = None
    else:
        known = {_normalized_league_name(value): value for value in known_matchday_leagues()}
        selected = []
        for raw in payload.get("leagues") or []:
            normalized = _normalized_league_name(str(raw))
            if normalized in known and known[normalized] not in selected:
                selected.append(known[normalized])
        preference.relevant_leagues_json = json.dumps(selected, ensure_ascii=False)

    submitted = payload.get("market_settings") if isinstance(payload.get("market_settings"), dict) else {}
    settings = json.loads(json.dumps(DEFAULT_MATCHDAY_MARKET_SETTINGS))
    try:
        minimum_samples = int(submitted.get("minimum_samples") or 3)
    except (TypeError, ValueError):
        minimum_samples = 3
    try:
        max_markets = int(submitted.get("max_markets_per_game") or 3)
    except (TypeError, ValueError):
        max_markets = 3
    settings["minimum_samples"] = max(3, min(10, minimum_samples))
    settings["max_markets_per_game"] = max(1, min(6, max_markets))
    for market, defaults in DEFAULT_MATCHDAY_MARKET_SETTINGS.items():
        if not isinstance(defaults, dict):
            continue
        incoming = submitted.get(market) if isinstance(submitted.get(market), dict) else {}
        if market == "goals":
            for goal_market in ("goal_ht", "over15", "over25", "btts"):
                settings[market][goal_market] = bool(incoming.get(goal_market, defaults[goal_market]))
            continue
        settings[market]["enabled"] = bool(incoming.get("enabled", defaults["enabled"]))
        for scope in ("total", "home", "away"):
            settings[market][f"{scope}_enabled"] = bool(incoming.get(f"{scope}_enabled", defaults[f"{scope}_enabled"]))
            try:
                value = float(incoming.get(scope, defaults[scope]))
            except (TypeError, ValueError):
                value = defaults[scope]
            settings[market][scope] = max(.5, min(99.5, round(value * 2) / 2))
    preference.market_settings_json = json.dumps(settings, ensure_ascii=False)
    db.session.commit()
    return jsonify(ok=True, message="Preferências dos Jogos do Dia salvas.", settings=settings)


@main_bp.route("/jogos-do-dia/<game_id>")
@login_required
def matchday_analysis(game_id):
    day = (request.args.get("day") or now_sp().strftime("%Y-%m-%d")).strip()
    sample_limit = max(1, min(current_user.matchday_sample_limit, request.args.get("limit", current_user.matchday_sample_limit, type=int)))
    match = _find_matchday_match(day, game_id)
    if not match:
        flash("Jogo não encontrado na agenda selecionada.", "warning")
        return redirect(url_for("main.matchday", day=day))
    analysis = analyze_upcoming_match(
        match,
        force_refresh=request.args.get("refresh") == "1",
        detail_limit=sample_limit,
        cache_variant=f"detail-v16-archived-halftime-{sample_limit}",
    )
    return render_template(
        "matchday/analysis.html",
        match=match,
        analysis=analysis,
        day=day,
        sample_limit=sample_limit,
    )


@main_bp.route("/jogos-do-dia/<game_id>/carregando")
@login_required
def matchday_analysis_loading(game_id):
    day = (request.args.get("day") or now_sp().strftime("%Y-%m-%d")).strip()
    sample_limit = max(1, min(current_user.matchday_sample_limit, request.args.get("limit", current_user.matchday_sample_limit, type=int)))
    match = _find_matchday_match(day, game_id)
    if not match:
        flash("Jogo não encontrado na agenda selecionada.", "warning")
        return redirect(url_for("main.matchday", day=day))
    analysis_url = url_for(
        "main.matchday_analysis",
        game_id=game_id,
        day=day,
        limit=sample_limit,
    )
    return render_template(
        "matchday/loading.html",
        match=match,
        analysis_url=analysis_url,
    )


@main_bp.route("/jogos-do-dia/<game_id>/resumo")
@login_required
def matchday_summary(game_id):
    day = (request.args.get("day") or now_sp().strftime("%Y-%m-%d")).strip()
    match = _find_matchday_match(day, game_id)
    if not match:
        return jsonify({"ok": False, "error": "Jogo não encontrado."}), 404
    sample_limit = max(1, min(current_user.matchday_sample_limit, request.args.get("limit", current_user.matchday_sample_limit, type=int)))
    force_refresh = request.args.get("refresh") in {"1", "true", "yes"}
    cached_summary = None if force_refresh else load_matchday_summary_cache(day, str(game_id), sample_limit)
    if cached_summary and all(
        "btts" in group and (
            int(group.get("count") or 0) == 0
            or bool((group.get("history_values") or {}).get("btts"))
        )
        for group in (cached_summary.get("groups") or {}).values()
    ):
        cached_summary["shared_cache"] = True
        return jsonify(cached_summary)
    if not _matchday_analysis_slots.acquire(blocking=False):
        response = jsonify({"ok": False, "busy": True, "error": "Análises em processamento. Aguarde."})
        response.status_code = 429
        response.headers["Retry-After"] = "3"
        return response
    try:
        try:
            analysis = analyze_upcoming_match(
                match,
                force_refresh=force_refresh,
                detail_limit=sample_limit,
                cache_variant=f"card-v30-periods-{sample_limit}",
            )
            # O scraper pode receber uma página intermediária durante a
            # liberação do desafio da fonte. Uma segunda leitura já usa a
            # sessão liberada e evita exibir um falso "sem histórico".
            if not any(int(group.get("count") or 0) > 0 for group in (analysis.get("groups") or [])):
                analysis = analyze_upcoming_match(
                    match,
                    force_refresh=True,
                    detail_limit=sample_limit,
                    cache_variant=f"card-v30-periods-{sample_limit}",
                )
        except Exception:
            current_app.logger.exception("Falha ao montar resumo pré-jogo %s", game_id)
            return jsonify({"ok": False, "error": "Análise indisponível."}), 503
    finally:
        _matchday_analysis_slots.release()
    groups = analysis.get("groups") or []
    usable = [group for group in groups if int(group.get("count") or 0) > 0]
    if not usable:
        payload = {
            "ok": True,
            "status": "empty",
            "scheduled_time": analysis.get("scheduled_time"),
            "message": "Não há partidas anteriores disponíveis para este confronto.",
            "groups": {},
        }
        # Ausência de histórico também é um resultado válido. Compartilhá-lo
        # evita consultar novamente a mesma partida a cada abertura da página.
        save_matchday_summary_cache(day, str(game_id), sample_limit, payload)
        return jsonify(payload)
    summaries = {}
    for group in groups:
        phase = group.get("phase") or {}
        group_key = str(group.get("key") or "")
        team_only = group_key in {"Mandante", "Visitante"}
        count = int(group.get("count") or 0)
        corners_1h = phase.get("avg_corners_1h")
        corners_2h = phase.get("avg_corners_2h")
        corners_samples = int(phase.get("corners_samples") or 0)
        cards_samples = int(phase.get("cards_samples") or 0)
        offsides_samples = int(phase.get("offsides_samples") or 0)
        shots_samples = int(phase.get("shots_samples") or 0)
        shots_on_target_samples = int(phase.get("shots_on_target_samples") or 0)
        fouls_samples = int(phase.get("fouls_samples") or 0)
        corners_avg = None
        if corners_samples >= 3 and corners_1h is not None and corners_2h is not None:
            corners_avg = round(float(corners_1h) + float(corners_2h), 2)
        team_goal_samples = int(phase.get("team_goals_samples") or 0)
        team_goal_ht_samples = int(phase.get("team_goal_ht_samples") or 0)
        total_history = phase.get("history_values") or {}
        btts_history = list(total_history.get("btts") or [])
        if not btts_history:
            for row in total_history.get("over15") or []:
                if not isinstance(row, dict):
                    continue
                try:
                    btts_value = int(float(row.get("home_value")) > 0 and float(row.get("away_value")) > 0)
                except (TypeError, ValueError):
                    continue
                btts_history.append({**row, "value": btts_value})

        def period_summary(period: str) -> dict:
            history = phase.get("history_values") or {}
            suffix = "1h" if period == "first" else "2h"
            goal_key = ("team_goal_ht" if period == "first" else "team_goals_2h") if team_only else ("goal_ht" if period == "first" else "goals_2h")
            corner_key = f"team_corners_{suffix}" if team_only else f"corners_{suffix}"
            card_key = f"team_cards_{suffix}" if team_only else f"cards_{suffix}"
            shots_key = f"team_shots_{suffix}" if team_only else f"shots_{suffix}"
            target_key = f"team_on_target_{suffix}" if team_only else f"on_target_{suffix}"

            def rows(key):
                return [row for row in (history.get(key) or []) if isinstance(row, dict) and row.get("value") is not None]

            def average(key):
                values = [float(row["value"]) for row in rows(key)]
                return round(sum(values) / len(values), 2) if values else None

            def percentage(key, threshold):
                values = [float(row["value"]) for row in rows(key)]
                return round(sum(1 for value in values if value > threshold) / len(values) * 100) if values else None

            goal_rows = rows(goal_key)
            corner_rows = rows(corner_key)
            card_rows = rows(card_key)
            shots_rows = rows(shots_key)
            target_rows = rows(target_key)
            return {
                "count": len(goal_rows),
                "samples": len(goal_rows),
                "goal_ht": percentage(goal_key, 0),
                "over15": percentage(goal_key, 1),
                "over25": percentage(goal_key, 2),
                "corners_avg": average(corner_key),
                "corners_samples": len(corner_rows),
                "cards_avg": average(card_key),
                "cards_samples": len(card_rows),
                "shots_avg": average(shots_key),
                "shots_samples": len(shots_rows),
                "shots_on_target_avg": average(target_key),
                "shots_on_target_samples": len(target_rows),
                "corners_10_over1": phase.get("corners_10_over1_pct") if period == "first" else None,
                "corners_10_samples": int(phase.get("corners_10_samples") or 0) if period == "first" else 0,
                "offsides_avg": None,
                "offsides_samples": 0,
                "fouls_avg": None,
                "fouls_samples": 0,
                "history_values": {
                    "goal_ht": goal_rows,
                    "over15": goal_rows,
                    "over25": goal_rows,
                    "corners_avg": corner_rows,
                    "cards_avg": card_rows,
                    "shots_avg": shots_rows,
                    "shots_on_target_avg": target_rows,
                    "corners_10_over1": (history.get("corners_10_over1") or []) if period == "first" else [],
                },
            }

        summaries[group_key] = {
            "count": team_goal_samples if team_only else count,
            "samples": team_goal_ht_samples if team_only else (phase.get("samples") or 0),
            "goal_ht": (phase.get("team_goal_ht_pct") if team_goal_ht_samples else None) if team_only else (phase.get("goal_1h_pct") if phase.get("samples") else None),
            "over15": (phase.get("team_over15_pct") if team_goal_samples else None) if team_only else (round((int(group.get("over15") or 0) / count) * 100) if count else None),
            "over25": (phase.get("team_over25_pct") if team_goal_samples else None) if team_only else (round((int(group.get("over25") or 0) / count) * 100) if count else None),
            "btts": round((int(group.get("btts") or 0) / count) * 100) if count else None,
            "corners_avg": corners_avg,
            "corners_samples": corners_samples,
            "corner_lines": phase.get("corner_lines") or {},
            "corners_10_over0": phase.get("corners_10_over0_pct"),
            "corners_10_over1": phase.get("corners_10_over1_pct"),
            "corners_10_samples": int(phase.get("corners_10_samples") or 0),
            "team_corners_avg": phase.get("avg_team_corners") if int(phase.get("team_corners_samples") or 0) >= 3 else None,
            "team_corners_samples": int(phase.get("team_corners_samples") or 0),
            "team_corner_lines": phase.get("team_corner_lines") or {},
            "cards_avg": phase.get("avg_team_cards") if team_only else (phase.get("avg_cards_total") if cards_samples >= 3 else None),
            "cards_samples": int(phase.get("team_cards_samples") or 0) if team_only else cards_samples,
            "card_lines": phase.get("card_lines") or {},
            "offsides_avg": phase.get("avg_team_offsides") if team_only else (phase.get("avg_offsides") if count > 0 and offsides_samples == count else None),
            "offsides_samples": int(phase.get("team_offsides_samples") or 0) if team_only else offsides_samples,
            "shots_avg": phase.get("avg_team_shots") if team_only else (phase.get("avg_shots") if shots_samples >= 1 else None),
            "shots_samples": int(phase.get("team_shots_samples") or 0) if team_only else shots_samples,
            "shots_on_target_avg": phase.get("avg_team_shots_on_target") if team_only else (phase.get("avg_shots_on_target") if shots_on_target_samples >= 1 else None),
            "shots_on_target_samples": int(phase.get("team_shots_on_target_samples") or 0) if team_only else shots_on_target_samples,
            "fouls_avg": phase.get("avg_team_fouls") if team_only else (phase.get("avg_fouls") if fouls_samples >= 1 else None),
            "fouls_samples": int(phase.get("team_fouls_samples") or 0) if team_only else fouls_samples,
            "history_values": {**total_history, "btts": btts_history},
            "periods": {
                "first": period_summary("first"),
                "second": period_summary("second"),
            },
        }
    payload = {
        "ok": True,
        "status": "ready",
        "scheduled_time": analysis.get("scheduled_time"),
        "sample_limit": sample_limit,
        "groups": summaries,
    }
    save_matchday_summary_cache(day, str(game_id), sample_limit, payload)
    return jsonify(payload)


@main_bp.post("/jogos-do-dia/<game_id>/odds-bet365")
@login_required
def matchday_bet365_odds(game_id):
    payload = request.get_json(silent=True) or {}
    day = str(payload.get("day") or now_sp().strftime("%Y-%m-%d")).strip()
    try:
        datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        return jsonify(ok=False, odds={}), 400
    match = _find_matchday_match(day, game_id)
    selections = payload.get("selections")
    supplied_match = payload.get("match") if isinstance(payload.get("match"), dict) else {}
    if not match and supplied_match:
        match = {
            "game_id": str(game_id),
            "league": str(supplied_match.get("league") or "")[:160],
            "home_team": str(supplied_match.get("home") or "")[:160],
            "away_team": str(supplied_match.get("away") or "")[:160],
            "time": str(supplied_match.get("time") or "")[:40],
        }
    if not match or not isinstance(selections, list):
        return jsonify(ok=False, odds={}), 404
    bookmaker_odds = bookmaker_selection_odds(match, day, selections)
    primary_odds = {}
    for selection_id, prices in bookmaker_odds.items():
        bet365 = next((price for price in prices if price["name"].casefold() == "bet365"), None)
        primary = bet365 or (prices[0] if prices else None)
        if primary:
            primary_odds[selection_id] = primary["odd"]
    return jsonify(ok=True, odds=primary_odds, bookmakers=bookmaker_odds)


@main_bp.route("/live/favorite-league", methods=["POST"])
@login_required
def toggle_live_favorite_league():
    league = (request.form.get("league") or "").strip()
    if not league:
        return redirect(url_for("main.live", **request.args.to_dict()))
    favorites = _favorite_live_leagues()
    favorite_map = {item.casefold(): item for item in favorites}
    key = league.casefold()
    if key in favorite_map:
        favorites = [item for item in favorites if item.casefold() != key]
        flash("Liga removida dos favoritos.", "success")
    else:
        favorites.append(league)
        flash("Liga adicionada aos favoritos.", "success")
    current_user.favorite_live_leagues_json = json.dumps(sorted(favorites), ensure_ascii=False)
    db.session.commit()
    return redirect(url_for("main.live", **request.args.to_dict()))
