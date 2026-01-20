import json
import os
import re
import threading
import time
from datetime import datetime

from sqlalchemy.exc import IntegrityError

from app.extensions import db
from app.models import MatchAlert, Rule, User
from app.services.evaluator import compare, evaluate_rule, history_confidence, render_message, stats_to_json
from app.services.exporter import export_alert
from app.services.scraper import (
    fetch_live_games,
    fetch_match_history,
    fetch_match_stats,
    format_history_summary,
    make_session,
    normalize_stat_key,
    summarize_history,
)
from app.services.telegram import send_message

POLL_INTERVAL = int(os.environ.get("WORKER_INTERVAL", "15"))
GAME_DELAY = float(os.environ.get("WORKER_GAME_DELAY", "1.5"))
EXPORT_DIR = os.environ.get("EXPORT_DIR", "data/exports")

API_STATUS = {"ok": None, "code": None, "checked_at": None, "last_cycle": None}
API_ALERT_STATE = {"last_ok": None}
SECOND_HALF_BASELINES = {}
NON_DELTA_KEYS = {"Minute", "Possession"}
YOUTH_TOKENS = (
    "u19", "u-19", "u 19", "sub19", "sub-19", "sub 19", "under 19",
    "u20", "u-20", "u 20", "sub20", "sub-20", "sub 20", "under 20",
)

def get_api_status() -> dict:
    return {
        "ok": API_STATUS.get("ok"),
        "code": API_STATUS.get("code"),
        "checked_at": API_STATUS.get("checked_at"),
        "last_cycle": API_STATUS.get("last_cycle"),
    }

def update_api_status(ok: bool, code: int | None):
    API_STATUS["ok"] = ok
    API_STATUS["code"] = code
    API_STATUS["checked_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    notify_api_status(ok, code)

def notify_api_status(ok: bool, code: int | None):
    last_ok = API_ALERT_STATE.get("last_ok")
    if last_ok is None:
        API_ALERT_STATE["last_ok"] = ok
        if ok: return
        reason = f"HTTP {code}" if code else "erro de conexao/anti-bot"
        message = f"API OFF: possivel anti-bot ativo ({reason})."
        for user in User.query.filter_by(telegram_verified=True).all():
            if user.telegram_token and user.telegram_chat_id:
                send_message(user.telegram_token, user.telegram_chat_id, message)
        return

    if last_ok == ok: return
    API_ALERT_STATE["last_ok"] = ok
    users = User.query.filter_by(telegram_verified=True).all()
    if not users: return

    message = "API voltou ao normal (status 200)." if ok else f"API OFF: possivel anti-bot ativo ({'HTTP ' + str(code) if code else 'erro de conexao/anti-bot'})."
    for user in users:
        if user.telegram_token and user.telegram_chat_id:
            send_message(user.telegram_token, user.telegram_chat_id, message)

def is_half_time(time_text: str, minute: int) -> bool:
    text = (time_text or "").lower()
    return "ht" in text or "half time" in text or "interval" in text or 45 <= minute <= 47

def is_first_half_goal(time_text: str, minute: int) -> bool:
    text = (time_text or "").lower()
    if any(x in text for x in ["2nd", "2o", "2h"]): return False
    return 0 <= minute <= 47

def is_full_time(time_text: str, minute: int) -> bool:
    text = (time_text or "").lower()
    if any(x in text for x in ["ft", "full time", "finished", "ended", "fim", "encerrado", "final"]): return True
    return 90 <= minute <= 130

def parse_score(score_text: str):
    if not score_text: return 0, 0
    nums = re.findall(r"\d+", score_text)
    return (int(nums[0]), int(nums[1])) if len(nums) >= 2 else (0, 0)

def is_youth_match(stats_payload: dict) -> bool:
    if not stats_payload: return False
    hay = f"{stats_payload.get('league', '')} {stats_payload.get('home_team', '')} {stats_payload.get('away_team', '')}".lower()
    return any(token in hay for token in YOUTH_TOKENS)

def copy_stats(stats):
    return {key: value.copy() if isinstance(value, dict) else value for key, value in stats.items()}

def ensure_second_half_baseline(game_id: str, stats_payload) -> None:
    if not stats_payload or not game_id or game_id in SECOND_HALF_BASELINES: return
    minute = stats_payload.get("minute") or 0
    if is_half_time(stats_payload.get("time_text", ""), minute) or minute >= 46:
        SECOND_HALF_BASELINES[game_id] = copy_stats(stats_payload["stats"])

def apply_second_half_delta(stats, baseline):
    adjusted = {}
    for key, value in stats.items():
        if not isinstance(value, dict): continue
        if key in NON_DELTA_KEYS or key not in baseline:
            adjusted[key] = value.copy()
            continue
        base = baseline[key]
        adjusted[key] = {
            "home": max(0, value.get("home", 0) - base.get("home", 0)),
            "away": max(0, value.get("away", 0) - base.get("away", 0)),
            "total": max(0, value.get("total", 0) - base.get("total", 0)),
        }
    return adjusted

def _num(value):
    return value if isinstance(value, (int, float)) else 0

def apply_alert_delta(stats, baseline, minute: int | None, alert_minute: int | None):
    if not stats or not baseline:
        return stats
    adjusted = {}
    for key, value in stats.items():
        if not isinstance(value, dict):
            continue
        if key == "Possession":
            adjusted[key] = value.copy()
            continue
        base = baseline.get(key)
        if not isinstance(base, dict):
            adjusted[key] = value.copy()
            continue
        adjusted[key] = {
            "home": max(0, _num(value.get("home")) - _num(base.get("home"))),
            "away": max(0, _num(value.get("away")) - _num(base.get("away"))),
            "total": max(0, _num(value.get("total")) - _num(base.get("total"))),
        }
    if minute is not None:
        start = alert_minute if alert_minute is not None else minute
        m_delta = max(0, minute - start)
        adjusted["Minute"] = {"home": m_delta, "away": m_delta, "total": m_delta}
    return adjusted

def evaluate_outcome_conditions(conditions, stats: dict) -> bool:
    if not conditions: return False
    for cond in conditions:
        key = normalize_stat_key(cond.stat_key)
        if key not in stats: return False
        side_values = stats[key]
        if cond.side not in side_values: return False
        value = side_values[cond.side]
        if value is None or not compare(cond.operator, value, cond.value): return False
    return True

def start_worker(app):
    threading.Thread(target=run_worker, args=(app,), daemon=True).start()

def run_worker(app):
    with app.app_context():
        session = make_session()
        while True:
            try:
                process_live_games(session)
                follow_alerts(session)
                finalize_full_time(session)
            except Exception as exc:
                db.session.rollback()
                print(f"[worker] erro: {exc}")
            API_STATUS["last_cycle"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            time.sleep(POLL_INTERVAL)

def process_live_games(session):
    games, status_code = fetch_live_games(session)
    update_api_status(status_code == 200, status_code)
    if not games: return

    active_rules = Rule.query.filter_by(is_active=True).all()
    for game in games:
        stats_payload = fetch_match_stats(session, game["url"])
        if not stats_payload or is_youth_match(stats_payload): continue
        
        minute = stats_payload.get("minute")
        if minute is None: continue

        ensure_second_half_baseline(game["game_id"], stats_payload)
        
        for rule in active_rules:
            existing = MatchAlert.query.filter_by(game_id=game["game_id"], rule_id=rule.id).first()
            if existing: continue

            stats_for_rule = stats_payload["stats"]
            h_score, a_score = parse_score(stats_payload.get("score", ""))
            if (rule.score_home is not None and h_score != rule.score_home) or \
               (rule.score_away is not None and a_score != rule.score_away):
                continue

            if rule.second_half_only:
                if minute < 46: continue
                baseline = SECOND_HALF_BASELINES.get(game["game_id"])
                if not baseline: continue
                stats_for_rule = apply_second_half_delta(stats_payload["stats"], baseline)
                m2h = max(0, minute - 45)
                stats_for_rule["Minute"] = {"home": m2h, "away": m2h, "total": m2h}

            if evaluate_rule(rule, stats_for_rule):
                user = rule.user
                if not user or not user.telegram_token or not user.telegram_chat_id: continue
                
                alert = MatchAlert(
                    rule_id=rule.id, user_id=user.id, game_id=game["game_id"], url=game["url"],
                    status="pending", alert_minute=minute, initial_score=stats_payload["score"],
                    initial_stats_json=stats_to_json(stats_for_rule),
                    league=stats_payload.get("league"), home_team=stats_payload.get("home_team"),
                    away_team=stats_payload.get("away_team")
                )
                db.session.add(alert)
                try:
                    db.session.commit()
                    rule.last_alert_at = datetime.utcnow()
                    rule.last_alert_desc = f"{alert.home_team} vs {alert.away_team}"
                    db.session.commit()
                    
                    history_meta = {}
                    try:
                        history = fetch_match_history(session, game["url"])
                        h2h_summary = summarize_history(history.get("h2h", []))
                        home_summary = summarize_history(history.get("home", []))
                        away_summary = summarize_history(history.get("away", []))
                        history_meta = {
                            "history_h2h": format_history_summary("H2H", h2h_summary),
                            "history_home": format_history_summary("Home", home_summary),
                            "history_away": format_history_summary("Away", away_summary),
                        }
                        conf_conds = [c for c in rule.outcome_conditions if c.outcome_type == "green"] or rule.conditions
                        confidence = history_confidence(conf_conds, history.get("h2h", []))
                        if confidence is not None:
                            history_meta["history_confidence"] = f"{confidence}%"
                    except Exception:
                        history_meta = {}
                    meta = build_message_meta(rule, stats_payload, game, history_meta)
                    send_message(user.telegram_token, user.telegram_chat_id, render_message(rule, meta))
                except IntegrityError:
                    db.session.rollback()
        time.sleep(GAME_DELAY)

def build_message_meta(rule, stats_payload, game, history_meta=None):
    stats = stats_payload.get("stats", {})
    def sv(k, s): return stats.get(k, {}).get(s, "")
    meta = {
        "rule": rule.name, "home_team": stats_payload.get("home_team"), "away_team": stats_payload.get("away_team"),
        "minute": stats_payload.get("minute"), "score": stats_payload.get("score"), "url": game.get("url"),
        "league": stats_payload.get("league"), "time_limit": rule.time_limit_min,
        "goals_home": sv("Goals", "home"), "goals_away": sv("Goals", "away"), "goals_total": sv("Goals", "total"),
        "corners_home": sv("Corners", "home"), "corners_away": sv("Corners", "away"), "corners_total": sv("Corners", "total"),
        "on_target_home": sv("On Target", "home"), "on_target_away": sv("On Target", "away"), "on_target_total": sv("On Target", "total"),
        "dangerous_attacks_home": sv("Dangerous Attacks", "home"), "dangerous_attacks_away": sv("Dangerous Attacks", "away"), "dangerous_attacks_total": sv("Dangerous Attacks", "total"),
    }
    if history_meta:
        meta.update(history_meta)
    return meta

def follow_alerts(session):
    pending_alerts = MatchAlert.query.filter_by(status="pending").all()
    for alert in pending_alerts:
        rule = alert.rule
        stats_payload = fetch_match_stats(session, alert.url)
        if not stats_payload: continue

        ensure_second_half_baseline(alert.game_id, stats_payload)
        minute = stats_payload.get("minute") or 0
        current_score = stats_payload.get("score")
        stats = stats_payload.get("stats", {})

        if rule and rule.second_half_only:
            baseline = SECOND_HALF_BASELINES.get(alert.game_id)
            if baseline: stats = apply_second_half_delta(stats_payload["stats"], baseline)
            m2h = max(0, minute - 45)
            stats["Minute"] = {"home": m2h, "away": m2h, "total": m2h}

        green_conds = [c for c in rule.outcome_conditions if c.outcome_type == "green"] if rule else []
        red_conds = [c for c in rule.outcome_conditions if c.outcome_type == "red"] if rule else []

        base_stats = None
        if alert.initial_stats_json:
            try:
                base_stats = json.loads(alert.initial_stats_json)
            except Exception:
                base_stats = None
        stats_for_outcome = apply_alert_delta(stats, base_stats, minute, alert.alert_minute) if base_stats else stats
        
        # 1. Verificar GREEN customizado
        if green_conds and evaluate_outcome_conditions(green_conds, stats_for_outcome):
            update_alert_status(alert, "green", minute, current_score, stats, "✅ GREEN - condições atingidas")
            continue

        # 2. Verificar RED customizado
        if red_conds and evaluate_outcome_conditions(red_conds, stats_for_outcome):
            update_alert_status(alert, "red", minute, current_score, stats, "❌ RED - condições de RED atingidas")
            continue

        # 3. Verificar RED por tempo (se habilitado)
        if rule and rule.outcome_red_if_no_green and rule.outcome_red_minute is not None:
            if minute >= rule.outcome_red_minute:
                update_alert_status(alert, "red", minute, current_score, stats, "❌ RED - prazo do GREEN expirou")
                continue

        # 4. Lógica padrão (se não houver condições customizadas)
        if not green_conds and not red_conds:
            if alert.initial_score and current_score != alert.initial_score and is_first_half_goal(stats_payload.get("time_text", ""), minute):
                update_alert_status(alert, "green", minute, current_score, stats, "✅ GREEN - gol no 1o tempo")
            elif is_half_time(stats_payload.get("time_text", ""), minute):
                update_alert_status(alert, "red", minute, current_score, stats, "❌ RED - fim do 1o tempo sem gol")

def update_alert_status(alert, status, minute, score, stats, msg_prefix):
    alert.status = status
    alert.result_minute = minute
    alert.result_time_hhmm = datetime.utcnow().strftime("%H:%M")
    alert.ht_score = score
    alert.ht_stats_json = stats_to_json(stats)
    db.session.commit()
    export_alert(alert, alert.rule.name, EXPORT_DIR)
    send_message(alert.user.telegram_token, alert.user.telegram_chat_id, 
                 f"{msg_prefix}\n{alert.home_team} vs {alert.away_team}\nTempo: {minute}'\nPlacar: {score}\nLink: {alert.url}")

def finalize_full_time(session):
    for alert in MatchAlert.query.filter_by(ft_completed=False).all():
        stats_payload = fetch_match_stats(session, alert.url)
        if not stats_payload: continue
        minute = stats_payload.get("minute") or 0
        if is_full_time(stats_payload.get("time_text", ""), minute):
            alert.ft_score = stats_payload.get("score")
            alert.ft_stats_json = stats_to_json(stats_payload["stats"])
            alert.ft_completed = True
            db.session.commit()
            export_alert(alert, alert.rule.name, EXPORT_DIR)
            SECOND_HALF_BASELINES.pop(alert.game_id, None)
        time.sleep(0.4)
