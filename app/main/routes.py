import json
import re
import threading
import unicodedata
from datetime import datetime, timedelta
from urllib.parse import urlparse

from flask import Blueprint, Response, current_app, flash, jsonify, redirect, render_template, request, url_for
from flask_login import login_required, current_user
from sqlalchemy.orm import joinedload, load_only

from ..extensions import db
from ..models import LiveGameState, MatchAlert, Rule, SavedTicket, SavedTicketLeg
from ..services.worker import get_api_status
from ..services.undo import apply_undo
from ..services.matchday import (
    _is_excluded_match,
    analyze_upcoming_match,
    find_match,
    get_matchday,
    load_matchday_trend_index,
    trend_groups_for_match,
)
from ..security import safe_redirect_target
from ..utils.time import now_sp

main_bp = Blueprint("main", __name__)
_matchday_analysis_slots = threading.BoundedSemaphore(2)

RELEVANT_MATCHDAY_LEAGUES = {
    "uefa champions league", "uefa champions league qualifying",
    "uefa europa league", "uefa europa league qualifying",
    "uefa conference league", "uefa conference league qualifying",
    "england premier league", "english premier league",
    "spain la liga", "italy serie a", "germany bundesliga", "france ligue 1",
    "portugal primeira liga", "netherlands eredivisie",
    "belgium first division a", "turkey super lig", "scotland premiership",
    "brazil serie a", "brazil serie b", "brazil cup", "copa do brasil",
    "copa libertadores", "copa sudamericana",
    "argentina liga profesional", "argentina cup", "copa argentina",
    "colombia primera a", "chile primera division", "uruguay primera division",
    "usa mls", "mexico liga mx", "leagues cup", "concacaf champions cup",
    "fifa world cup", "world cup qualifying", "uefa nations league",
    "uefa european championship", "european championship", "copa america",
}


def _normalized_league_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    return " ".join(
        "".join(char for char in normalized if not unicodedata.combining(char))
        .casefold()
        .replace("-", " ")
        .split()
    )


def _is_relevant_matchday_league(value: str) -> bool:
    return _normalized_league_name(value) in RELEVANT_MATCHDAY_LEAGUES


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


def _ticket_profit(ticket) -> float:
    if ticket.status == "green":
        return float(ticket.stake_amount) * (float(ticket.total_odd) - 1)
    if ticket.status == "red":
        return -float(ticket.stake_amount)
    return 0.0


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
        .options(
            load_only(
                MatchAlert.created_at,
                MatchAlert.status,
                MatchAlert.rule_id,
                MatchAlert.stake_amount,
                MatchAlert.stake_odd,
            ),
            joinedload(MatchAlert.rule).load_only(Rule.id, Rule.name),
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
    finance_tickets = SavedTicket.query.filter(
        SavedTicket.user_id == current_user.id, SavedTicket.created_at >= since
    ).all()
    finance_today_alerts = [
        alert
        for alert in finance_alerts
        if start_day <= alert.created_at < end_day
    ]
    today_tickets = [ticket for ticket in finance_tickets if start_day <= ticket.created_at < end_day]
    financial_profit = round(sum(_alert_profit(alert) for alert in finance_alerts) + sum(_ticket_profit(ticket) for ticket in finance_tickets), 2)
    financial_profit_today = round(sum(_alert_profit(alert) for alert in finance_today_alerts) + sum(_ticket_profit(ticket) for ticket in today_tickets), 2)
    financial_staked = round(sum(float(alert.stake_amount or 0) for alert in finance_alerts) + sum(float(ticket.stake_amount or 0) for ticket in finance_tickets), 2)
    financial_bets = len(finance_alerts) + len(finance_tickets)
    finance_daily = {}
    for alert in finance_alerts:
        key = alert.created_at.strftime("%Y-%m-%d")
        finance_daily.setdefault(key, {"profit": 0.0, "staked": 0.0, "bets": 0})
        finance_daily[key]["profit"] += _alert_profit(alert)
        finance_daily[key]["staked"] += float(alert.stake_amount or 0)
        finance_daily[key]["bets"] += 1
    for ticket in finance_tickets:
        key = ticket.created_at.strftime("%Y-%m-%d")
        finance_daily.setdefault(key, {"profit": 0.0, "staked": 0.0, "bets": 0})
        finance_daily[key]["profit"] += _ticket_profit(ticket)
        finance_daily[key]["staked"] += float(ticket.stake_amount or 0)
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
        .order_by(SavedTicket.created_at.desc()).limit(200).all()
    )
    return render_template("tickets/list.html", tickets=tickets)


@main_bp.route("/bilhetes/<int:ticket_id>/editar", methods=["GET", "POST"])
@login_required
def edit_saved_ticket(ticket_id):
    ticket = SavedTicket.query.filter_by(id=ticket_id, user_id=current_user.id).first_or_404()
    if ticket.status != "pending":
        flash("Bilhetes já finalizados não podem ser alterados.", "warning")
        return redirect(url_for("main.saved_tickets"))
    if request.method == "GET":
        line_options = {}
        for leg in ticket.legs:
            key = (leg.market_key or "").casefold()
            maximum = 20.5 if "corner" in key else 12.5 if "card" in key else 8.5 if key in {"over15", "over25"} or key.startswith("goals_") else 30.5
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
    db.session.commit()
    flash(f"{ticket.name} atualizado com sucesso.", "success")
    return redirect(url_for("main.saved_tickets"))


@main_bp.route("/api/bilhetes", methods=["POST"])
@login_required
def save_ticket():
    payload = request.get_json(silent=True) or {}
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

    ticket = SavedTicket(
        user_id=current_user.id,
        name=f"Bilhete #{SavedTicket.query.filter_by(user_id=current_user.id).count() + 1}",
        total_odd=round(odd, 3), stake_amount=round(stake, 2), status="pending",
    )
    db.session.add(ticket)
    db.session.flush()
    for item in items[:100]:
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
            market_key = "over15" if "1,5" in lower else "over25" if "2,5" in lower else "goal_ht" if "1º tempo" in lower else ""
        if market_key.endswith("_home"):
            side = "home"
        elif market_key.endswith("_away"):
            side = "away"
        else:
            group = str(item.get("group") or "").casefold()
            side = "home" if "casa" in group and "fora" not in group else "away" if "fora" in group and "casa" not in group else "total"
        if not market_key or (market_key not in {"goal_ht", "over15", "over25"} and line is None):
            db.session.rollback()
            return jsonify(ok=False, message=f"Defina uma linha de aposta para “{label}” antes de salvar."), 400
        db.session.add(SavedTicketLeg(
            ticket_id=ticket.id, game_id=str(item.get("gameId") or "")[:32],
            game_day=str(item.get("day") or "")[:10], game_time=str(item.get("time") or "")[:40],
            league=str(item.get("league") or "")[:120], home_team=str(item.get("home") or "")[:120],
            away_team=str(item.get("away") or "")[:120], market_key=market_key[:64],
            market_label=label, target_side=side, target_line=line,
            samples=int(item.get("samples") or 0), source_group=str(item.get("group") or "")[:120],
        ))
    db.session.commit()
    return jsonify(ok=True, ticket_id=ticket.id, name=ticket.name, message=f"{ticket.name} salvo e em acompanhamento.")


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
    day = (request.args.get("day") or now_sp().strftime("%Y-%m-%d")).strip()
    try:
        datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        day = now_sp().strftime("%Y-%m-%d")
    payload = get_matchday(day, force_refresh=request.args.get("refresh") == "1")
    include_live = request.args.get("live") == "1"
    all_matches = list(payload["matches"])
    if include_live and day == now_sp().strftime("%Y-%m-%d"):
        merged = {match["game_id"]: dict(match) for match in all_matches}
        for live_match in _current_matchday_live_matches():
            merged[live_match["game_id"]] = live_match
        all_matches = sorted(
            merged.values(),
            key=lambda match: (not match.get("is_live", False), match.get("time") or "", match.get("league") or ""),
        )
    available_leagues = list(
        dict.fromkeys(match["league"] for match in all_matches if match.get("league"))
    )
    relevant_leagues = [league for league in available_leagues if _is_relevant_matchday_league(league)]
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
        "goal_ht", "over15", "over25", "corners_10_over1", "corners_avg",
        "cards_avg", "offsides_avg", "shots_avg", "shots_on_target_avg", "fouls_avg",
    }:
        trend_market = ""
    trend_group = (request.args.get("trend_group") or "best").strip()
    if trend_group not in {"best", "H2H", "Mandante", "Visitante"}:
        trend_group = "best"
    trend_min = max(0, min(100, request.args.get("trend_min", 0, type=int)))
    trend_max = max(trend_min, min(100, request.args.get("trend_max", 100, type=int)))
    trend_limit = max(4, min(10, request.args.get("trend_limit", 6, type=int)))
    trend_active = bool(trend_market)
    ticket_generator_active = request.args.get("ticket_generator") == "1"
    trend_prefetch = {}
    generated_goal_suggestions = {}
    if ticket_generator_active:
        generator_samples = max(3, min(10, request.args.get("generator_samples", 6, type=int)))
        generator_count = max(1, min(500, request.args.get("generator_count", 3, type=int)))
        generator_markets = set(request.args.getlist("generator_market"))
        goal_markets = generator_markets & {"over15", "over25"}
        trend_index = load_matchday_trend_index(day)
        ranked_matches = []
        # O atalho do índice de gols só pode limitar a agenda quando apenas
        # mercados de gols foram pedidos. Com cartões/escanteios, todos os
        # próximos jogos precisam chegar ao cliente para formar e repor o bilhete.
        only_goal_markets = bool(generator_markets) and generator_markets <= {"over15", "over25"}
        if trend_index.get("complete") and goal_markets and only_goal_markets:
            for match in matches:
                if match.get("is_live"):
                    continue
                groups = trend_groups_for_match(trend_index, match.get("game_id"), generator_samples)
                market_scores = []
                for market in goal_markets:
                    rows = [
                        (key, int(group.get(market)), int(group.get("count") or 0))
                        for key, group in groups.items()
                        if int(group.get("count") or 0) >= 3 and group.get(market) is not None
                    ]
                    values = [value for _, value, _ in rows]
                    if len(values) < 2:
                        continue
                    average = sum(values) / len(values)
                    spread = max(values) - min(values)
                    if average >= 80 and spread <= 25:
                        labels = {"H2H": "H2H", "Mandante": "Casa", "Visitante": "Fora"}
                        market_scores.append({
                            "market": market,
                            "score": average - spread * 0.35 + len(values) * 2 + min(samples for _, _, samples in rows) * 0.5,
                            "confidence": round(average),
                            "samples": min(samples for _, _, samples in rows),
                            "group": " + ".join(labels[key] for key, _, _ in rows),
                        })
                if market_scores:
                    best = max(market_scores, key=lambda item: item["score"])
                    ranked_matches.append((best["score"], match, best))
            ranked_matches.sort(key=lambda item: item[0], reverse=True)
            candidate_limit = min(len(ranked_matches), max(24, generator_count * 2))
            selected_ranked = ranked_matches[:candidate_limit]
            matches = [match for _, match, _ in selected_ranked]
            generated_goal_suggestions = {
                str(match.get("game_id")): suggestion
                for _, match, suggestion in selected_ranked
            }
            trend_prefetch = {
                str(match.get("game_id")): trend_groups_for_match(trend_index, match.get("game_id"), generator_samples)
                for match in matches
            }
        else:
            matches = [match for match in matches if not match.get("is_live")][:max(60, generator_count * 8)]
    if trend_active and trend_market in {"over15", "over25"}:
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
        day=day,
        page=page,
        has_prev=not trend_active and not ticket_generator_active and page > 1,
        has_next=not trend_active and not ticket_generator_active and total_matches > start + per_page,
        prev_args=prev_args,
        next_args=next_args,
        trend_clear_args=trend_clear_args,
    )


@main_bp.route("/jogos-do-dia/<game_id>")
@login_required
def matchday_analysis(game_id):
    day = (request.args.get("day") or now_sp().strftime("%Y-%m-%d")).strip()
    sample_limit = max(1, min(10, request.args.get("limit", 6, type=int)))
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
    sample_limit = max(1, min(10, request.args.get("limit", 6, type=int)))
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
    sample_limit = max(1, min(10, request.args.get("limit", 6, type=int)))
    if not _matchday_analysis_slots.acquire(blocking=False):
        response = jsonify({"ok": False, "busy": True, "error": "Análises em processamento. Aguarde."})
        response.status_code = 429
        response.headers["Retry-After"] = "3"
        return response
    try:
        try:
            analysis = analyze_upcoming_match(
                match,
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
        return jsonify({
            "ok": True,
            "status": "empty",
            "scheduled_time": analysis.get("scheduled_time"),
            "message": "O BetsAPI não disponibilizou partidas anteriores para este confronto.",
            "groups": {},
        })
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
            "history_values": phase.get("history_values") or {},
            "periods": {
                "first": period_summary("first"),
                "second": period_summary("second"),
            },
        }
    return jsonify(
        {
            "ok": True,
            "status": "ready",
            "scheduled_time": analysis.get("scheduled_time"),
            "sample_limit": sample_limit,
            "groups": summaries,
        }
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
