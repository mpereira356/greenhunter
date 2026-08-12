import json
import threading
from datetime import datetime, timedelta
from urllib.parse import urlparse

from flask import Blueprint, Response, current_app, flash, jsonify, redirect, render_template, request, url_for
from flask_login import login_required, current_user
from sqlalchemy.orm import joinedload, load_only

from ..extensions import db
from ..models import LiveGameState, MatchAlert, Rule
from ..services.worker import get_api_status
from ..services.undo import apply_undo
from ..services.matchday import (
    _is_excluded_youth_match,
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
        if _is_excluded_youth_match(state.league, state.home_team, state.away_team):
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
    team_leagues = {}
    for match in all_matches:
        league = match.get("league") or ""
        for team in (match.get("home_team"), match.get("away_team")):
            if team:
                team_leagues.setdefault(team, set()).add(league)
    available_teams = list(team_leagues)
    selected_leagues = [value for value in request.args.getlist("league") if value in available_leagues]
    selected_teams = [value for value in request.args.getlist("team") if value in available_teams]
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
    if trend_market not in {"goal_ht", "over15", "over25"}:
        trend_market = ""
    trend_group = (request.args.get("trend_group") or "best").strip()
    if trend_group not in {"best", "H2H", "Mandante", "Visitante"}:
        trend_group = "best"
    trend_min = max(0, min(100, request.args.get("trend_min", 0, type=int)))
    trend_max = max(trend_min, min(100, request.args.get("trend_max", 100, type=int)))
    trend_limit = max(4, min(10, request.args.get("trend_limit", 6, type=int)))
    trend_active = bool(trend_market)
    trend_prefetch = {}
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
    if not trend_active:
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
        day=day,
        page=page,
        has_prev=not trend_active and page > 1,
        has_next=not trend_active and total_matches > start + per_page,
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
                cache_variant=f"card-v18-exact-sample-{sample_limit}",
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
        count = int(group.get("count") or 0)
        corners_1h = phase.get("avg_corners_1h")
        corners_2h = phase.get("avg_corners_2h")
        corners_samples = int(phase.get("corners_samples") or 0)
        cards_samples = int(phase.get("cards_samples") or 0)
        offsides_samples = int(phase.get("offsides_samples") or 0)
        corners_avg = None
        if count > 0 and corners_samples == count and corners_1h is not None and corners_2h is not None:
            corners_avg = round(float(corners_1h) + float(corners_2h), 2)
        summaries[str(group.get("key") or "")] = {
            "count": count,
            "samples": phase.get("samples") or 0,
            "goal_ht": phase.get("goal_1h_pct") if phase.get("samples") else None,
            "over15": round((int(group.get("over15") or 0) / count) * 100) if count else None,
            "over25": round((int(group.get("over25") or 0) / count) * 100) if count else None,
            "corners_avg": corners_avg,
            "corners_samples": corners_samples,
            "cards_avg": phase.get("avg_cards_total") if count > 0 and cards_samples == count else None,
            "cards_samples": cards_samples,
            "offsides_avg": phase.get("avg_offsides") if count > 0 and offsides_samples == count else None,
            "offsides_samples": offsides_samples,
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
