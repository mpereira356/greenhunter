from datetime import datetime, timedelta

from flask import Blueprint, jsonify, render_template, request
from flask_login import login_required, current_user

from ..extensions import db
from ..models import MatchAlert, Rule
from ..services.worker import get_api_status
from ..services.scraper import fetch_live_games, fetch_match_stats, make_session

main_bp = Blueprint("main", __name__)


@main_bp.route("/")
@login_required
def dashboard():
    now = datetime.utcnow()
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
    )


@main_bp.route("/api/status")
def api_status():
    return jsonify(get_api_status())


@main_bp.route("/live")
@login_required
def live():
    query = (request.args.get("q") or "").strip().lower()
    limit = 12
    session = make_session()
    games, status_code = fetch_live_games(session)
    matches = []
    if status_code != 200:
        return render_template("live/list.html", matches=[], query=query, status_code=status_code)
    for game in games:
        if len(matches) >= limit:
            break
        stats_payload = fetch_match_stats(session, game["url"])
        if not stats_payload:
            continue
        hay = " ".join(
            [
                stats_payload.get("league", ""),
                stats_payload.get("home_team", ""),
                stats_payload.get("away_team", ""),
            ]
        ).lower()
        if query and query not in hay:
            continue
        stats = stats_payload.get("stats", {})
        raw_stats = stats_payload.get("raw_stats", {})
        raw_dangerous = raw_stats.get("Dangerous Attacks", ("-", "-"))
        raw_on_target = raw_stats.get("On Target", ("-", "-"))
        raw_corners = raw_stats.get("Corners", ("-", "-"))
        stats_list = []
        for key, values in sorted(raw_stats.items()):
            home_val, away_val = values
            stats_list.append(
                {
                    "key": key,
                    "home": home_val or "-",
                    "away": away_val or "-",
                }
            )
        matches.append(
            {
                "league": stats_payload.get("league"),
                "home_team": stats_payload.get("home_team"),
                "away_team": stats_payload.get("away_team"),
                "minute": stats_payload.get("minute"),
                "score": stats_payload.get("score"),
                "url": game["url"],
                "on_target_home": stats.get("On Target", {}).get("home", raw_on_target[0] or "-"),
                "on_target_away": stats.get("On Target", {}).get("away", raw_on_target[1] or "-"),
                "corners_home": stats.get("Corners", {}).get("home", raw_corners[0] or "-"),
                "corners_away": stats.get("Corners", {}).get("away", raw_corners[1] or "-"),
                "dangerous_home": stats.get("Dangerous Attacks", {}).get("home", raw_dangerous[0] or "-"),
                "dangerous_away": stats.get("Dangerous Attacks", {}).get("away", raw_dangerous[1] or "-"),
                "stats_list": stats_list,
            }
        )
    return render_template("live/list.html", matches=matches, query=query, status_code=200)
