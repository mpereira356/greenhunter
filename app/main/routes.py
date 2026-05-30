import json
from datetime import datetime, timedelta

from flask import Blueprint, flash, jsonify, redirect, render_template, request, url_for
from flask_login import login_required, current_user

from ..extensions import db
from ..models import LiveGameState, MatchAlert, Rule
from ..services.worker import get_api_status
from ..services.undo import apply_undo
from ..utils.time import now_sp

main_bp = Blueprint("main", __name__)


def _safe_json_dict(raw):
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


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
        "url": state.url or "#",
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


def _favorite_live_leagues() -> list[str]:
    try:
        data = json.loads(current_user.favorite_live_leagues_json or "[]")
    except Exception:
        data = []
    return [str(item) for item in data if str(item).strip()]


def _alert_profit(alert) -> float:
    if alert.stake_amount is None or alert.stake_odd is None:
        return 0.0
    if alert.status == "green":
        return float(alert.stake_amount) * (float(alert.stake_odd) - 1)
    if alert.status == "red":
        return -float(alert.stake_amount)
    return 0.0


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

    total_rules = Rule.query.filter_by(user_id=current_user.id).count()
    active_rules = Rule.query.filter_by(user_id=current_user.id, is_active=True).count()
    alerts_today = (
        MatchAlert.query.filter(
            MatchAlert.user_id == current_user.id,
            MatchAlert.created_at >= start_day,
            MatchAlert.created_at < end_day,
        ).count()
    )
    pending_alerts = MatchAlert.query.filter_by(user_id=current_user.id, status="pending").count()
    last_alert = (
        MatchAlert.query.filter_by(user_id=current_user.id)
        .order_by(MatchAlert.created_at.desc())
        .first()
    )
    last_green = (
        MatchAlert.query.filter_by(user_id=current_user.id, status="green")
        .order_by(MatchAlert.created_at.desc())
        .first()
    )
    last_red = (
        MatchAlert.query.filter_by(user_id=current_user.id, status="red")
        .order_by(MatchAlert.created_at.desc())
        .first()
    )
    greens = (
        MatchAlert.query.filter_by(user_id=current_user.id, status="green")
        .filter(MatchAlert.created_at >= start_day, MatchAlert.created_at < end_day)
        .count()
    )
    reds = (
        MatchAlert.query.filter_by(user_id=current_user.id, status="red")
        .filter(MatchAlert.created_at >= start_day, MatchAlert.created_at < end_day)
        .count()
    )

    recent_alerts = (
        MatchAlert.query.filter_by(user_id=current_user.id)
        .order_by(MatchAlert.created_at.desc())
        .limit(10)
        .all()
    )

    since = now - timedelta(days=6)
    recent_all = (
        MatchAlert.query.filter(
            MatchAlert.user_id == current_user.id, MatchAlert.created_at >= since
        )
        .order_by(MatchAlert.created_at.desc())
        .all()
    )
    daily = {}
    for alert in recent_all:
        key = alert.created_at.strftime("%Y-%m-%d")
        daily.setdefault(key, {"green": 0, "red": 0, "pending": 0})
        daily[key][alert.status] = daily[key].get(alert.status, 0) + 1
    chart_days = []
    max_count = 1
    for i in range(6, -1, -1):
        day = (now - timedelta(days=i)).strftime("%Y-%m-%d")
        counts = daily.get(day, {"green": 0, "red": 0, "pending": 0})
        total = counts["green"] + counts["red"] + counts["pending"]
        max_count = max(max_count, total)
        chart_days.append({"day": day[5:], "counts": counts, "total": total})

    finance_alerts = [
        alert
        for alert in recent_all
        if alert.stake_amount is not None and alert.stake_odd is not None
    ]
    finance_today_alerts = [
        alert
        for alert in finance_alerts
        if start_day <= alert.created_at < end_day
    ]
    financial_profit = round(sum(_alert_profit(alert) for alert in finance_alerts), 2)
    financial_profit_today = round(sum(_alert_profit(alert) for alert in finance_today_alerts), 2)
    financial_staked = round(sum(float(alert.stake_amount or 0) for alert in finance_alerts), 2)
    financial_bets = len(finance_alerts)
    finance_daily = {}
    for alert in finance_alerts:
        key = alert.created_at.strftime("%Y-%m-%d")
        finance_daily.setdefault(key, {"profit": 0.0, "staked": 0.0, "bets": 0})
        finance_daily[key]["profit"] += _alert_profit(alert)
        finance_daily[key]["staked"] += float(alert.stake_amount or 0)
        finance_daily[key]["bets"] += 1
    finance_days = []
    max_finance_abs = 1
    for i in range(6, -1, -1):
        day = (now - timedelta(days=i)).strftime("%Y-%m-%d")
        row = finance_daily.get(day, {"profit": 0.0, "staked": 0.0, "bets": 0})
        row = {**row, "day": day[5:], "profit": round(row["profit"], 2), "staked": round(row["staked"], 2)}
        max_finance_abs = max(max_finance_abs, abs(row["profit"]))
        finance_days.append(row)

    rule_stats = {}
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
        top_rules.append({"rule": stats["rule"], "win_rate": win_rate, "total": total})
    top_rules.sort(key=lambda x: x["win_rate"], reverse=True)
    top_rules = top_rules[:5]

    return render_template(
        "dashboard.html",
        active_rules=active_rules,
        total_rules=total_rules,
        alerts_today=alerts_today,
        greens=greens,
        reds=reds,
        pending_alerts=pending_alerts,
        last_alert=last_alert,
        last_green=last_green,
        last_red=last_red,
        recent_alerts=recent_alerts,
        chart_days=chart_days,
        max_count=max_count,
        top_rules=top_rules,
        worker_status=get_api_status(),
        dashboard_view=dashboard_view,
        financial_profit=financial_profit,
        financial_profit_today=financial_profit_today,
        financial_staked=financial_staked,
        financial_bets=financial_bets,
        finance_days=finance_days,
        max_finance_abs=max_finance_abs,
    )


@main_bp.route("/api/status")
def api_status():
    return jsonify(get_api_status())


@main_bp.route("/undo/<token>", methods=["GET"])
@login_required
def undo_action(token):
    next_url = request.args.get("next") or request.referrer or url_for("main.dashboard")
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
