import os
import re
import threading
import time
from datetime import datetime

from sqlalchemy.exc import IntegrityError

from ..extensions import db
from ..models import MatchAlert, Rule, User
from .evaluator import compare, evaluate_rule, render_message, stats_to_json
from .exporter import export_alert
from .scraper import fetch_live_games, fetch_match_stats, make_session, normalize_stat_key
from .telegram import send_message

POLL_INTERVAL = int(os.environ.get("WORKER_INTERVAL", "15"))
GAME_DELAY = float(os.environ.get("WORKER_GAME_DELAY", "1.5"))
EXPORT_DIR = os.environ.get("EXPORT_DIR", "data/exports")

API_STATUS = {"ok": None, "code": None, "checked_at": None, "last_cycle": None}
API_ALERT_STATE = {"last_ok": None}
SECOND_HALF_BASELINES = {}
NON_DELTA_KEYS = {"Minute", "Possession"}
YOUTH_TOKENS = ("u19", "u-19", "u 19", "sub19", "sub-19", "sub 19", "under 19")


def update_api_status(ok: bool, code: int | None):
    API_STATUS["ok"] = ok
    API_STATUS["code"] = code
    API_STATUS["checked_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    notify_api_status(ok, code)


def notify_api_status(ok: bool, code: int | None):
    last_ok = API_ALERT_STATE.get("last_ok")
    if last_ok is None:
        API_ALERT_STATE["last_ok"] = ok
        if ok:
            return
        reason = f"HTTP {code}" if code else "erro de conexao/anti-bot"
        message = f"API OFF: possivel anti-bot ativo ({reason})."
        for user in User.query.filter_by(telegram_verified=True).all():
            if user.telegram_token and user.telegram_chat_id:
                send_message(user.telegram_token, user.telegram_chat_id, message)
        return

    if last_ok == ok:
        return

    API_ALERT_STATE["last_ok"] = ok
    users = User.query.filter_by(telegram_verified=True).all()
    if not users:
        return

    if ok:
        message = "API voltou ao normal (status 200)."
    else:
        reason = f"HTTP {code}" if code else "erro de conexao/anti-bot"
        message = f"API OFF: possivel anti-bot ativo ({reason})."

    for user in users:
        if not user.telegram_token or not user.telegram_chat_id:
            continue
        send_message(user.telegram_token, user.telegram_chat_id, message)


def get_api_status():
    return API_STATUS


def is_half_time(time_text: str, minute: int) -> bool:
    text = (time_text or "").lower()
    if "ht" in text or "half time" in text or "interval" in text:
        return True
    return 45 <= minute <= 47


def is_first_half_goal(time_text: str, minute: int) -> bool:
    text = (time_text or "").lower()
    if "2nd" in text or "2o" in text or "2o tempo" in text or "2h" in text:
        return False
    return 0 <= minute <= 47


def is_full_time(time_text: str, minute: int) -> bool:
    text = (time_text or "").lower()
    if "ft" in text or "full time" in text or "finished" in text or "ended" in text:
        return True
    if "fim" in text or "encerrado" in text or "final" in text:
        return True
    return 90 <= minute <= 130


def parse_score(score_text: str):
    if not score_text:
        return 0, 0
    nums = re.findall(r"\d+", score_text)
    if len(nums) >= 2:
        return int(nums[0]), int(nums[1])
    return 0, 0


def is_youth_match(stats_payload: dict) -> bool:
    if not stats_payload:
        return False
    league = stats_payload.get("league") or ""
    home_team = stats_payload.get("home_team") or ""
    away_team = stats_payload.get("away_team") or ""
    hay = f"{league} {home_team} {away_team}".lower()
    return any(token in hay for token in YOUTH_TOKENS)


def copy_stats(stats):
    return {key: value.copy() if isinstance(value, dict) else value for key, value in stats.items()}


def ensure_second_half_baseline(game_id: str, stats_payload) -> None:
    if not stats_payload or not game_id:
        return
    if game_id in SECOND_HALF_BASELINES:
        return

    minute = stats_payload.get("minute") or 0
    time_text = stats_payload.get("time_text", "")
    if is_half_time(time_text, minute) or minute >= 46:
        SECOND_HALF_BASELINES[game_id] = copy_stats(stats_payload["stats"])


def apply_second_half_delta(stats, baseline):
    adjusted = {}
    for key, value in stats.items():
        if not isinstance(value, dict):
            continue
        if key in NON_DELTA_KEYS:
            adjusted[key] = value.copy()
            continue

        base = baseline.get(key)
        if not base:
            adjusted[key] = value.copy()
            continue

        adjusted[key] = {
            "home": max(0, value.get("home", 0) - base.get("home", 0)),
            "away": max(0, value.get("away", 0) - base.get("away", 0)),
            "total": max(0, value.get("total", 0) - base.get("total", 0)),
        }
    return adjusted


def rule_has_custom_outcomes(rule):
    return len(rule.outcome_conditions) > 0


def start_worker(app):
    thread = threading.Thread(target=run_worker, args=(app,), daemon=True)
    thread.start()


def run_worker(app):
    with app.app_context():
        session = make_session()
        while True:
            try:
                process_live_games(session)
                follow_alerts(session)
                finalize_full_time(session)
            except Exception as exc:
                print(f"[worker] erro: {exc}")

            API_STATUS["last_cycle"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            time.sleep(POLL_INTERVAL)


def process_live_games(session):
    games, status_code = fetch_live_games(session)
    update_api_status(status_code == 200, status_code)

    if not games:
        return

    active_rules = Rule.query.filter_by(is_active=True).all()

    for game in games:
        minute = game.get("minute")
        if minute is None:
            continue

        candidate_rules = [r for r in active_rules if minute <= r.time_limit_min]
        if not candidate_rules:
            continue

        candidate_ids = [r.id for r in candidate_rules]

        existing = (
            MatchAlert.query.filter(
                MatchAlert.game_id == game["game_id"],
                MatchAlert.rule_id.in_(candidate_ids),
            )
            .with_entities(MatchAlert.rule_id)
            .all()
        )

        existing_ids = {row.rule_id for row in existing}

        stats_payload = fetch_match_stats(session, game["url"])
        if not stats_payload:
            continue

        if is_youth_match(stats_payload):
            continue

        ensure_second_half_baseline(game["game_id"], stats_payload)

        match_desc = f"{stats_payload.get('home_team')} vs {stats_payload.get('away_team')}"
        now = datetime.utcnow()

        for rule in candidate_rules:
            rule.last_checked_at = now
            rule.last_match_desc = match_desc

        db.session.commit()

        for rule in candidate_rules:
            if rule.id in existing_ids:
                continue

            stats_for_rule = stats_payload["stats"]

            if rule.second_half_only:
                if minute < 46:
                    continue
                baseline = SECOND_HALF_BASELINES.get(game["game_id"])
                if not baseline:
                    continue
                stats_for_rule = apply_second_half_delta(stats_payload["stats"], baseline)

            if not evaluate_rule(rule, stats_for_rule):
                continue

            user = rule.user
            if not user or not user.telegram_token or not user.telegram_chat_id:
                continue

            alert = MatchAlert(
                rule_id=rule.id,
                user_id=user.id,
                game_id=game["game_id"],
                url=game["url"],
                status="pending",
                alert_minute=minute,
                initial_score=stats_payload["score"],
                initial_stats_json=stats_to_json(stats_payload["stats"]),
                league=stats_payload.get("league"),
                home_team=stats_payload.get("home_team"),
                away_team=stats_payload.get("away_team"),
            )

            db.session.add(alert)

            try:
                db.session.commit()
            except IntegrityError:
                db.session.rollback()
                continue

            rule.last_alert_at = datetime.utcnow()
            rule.last_alert_desc = match_desc
            db.session.commit()

            meta = build_message_meta(rule, stats_payload, game)
            message = render_message(rule, meta)
            send_message(user.telegram_token, user.telegram_chat_id, message)
            export_alert(alert, rule.name, EXPORT_DIR)

        time.sleep(GAME_DELAY)


def build_message_meta(rule, stats_payload, game):
    stats = stats_payload.get("stats", {})

    def stat_value(key, side):
        value = stats.get(key)
        if not value:
            return ""
        return value.get(side, "")

    meta = {
        "rule": rule.name,
        "home_team": stats_payload.get("home_team"),
        "away_team": stats_payload.get("away_team"),
        "minute": stats_payload.get("minute"),
        "score": stats_payload.get("score"),
        "url": game.get("url"),
        "league": stats_payload.get("league"),
        "time_limit": rule.time_limit_min,
        "goals_home": stat_value("Goals", "home"),
        "goals_away": stat_value("Goals", "away"),
        "goals_total": stat_value("Goals", "total"),
        "corners_home": stat_value("Corners", "home"),
        "corners_away": stat_value("Corners", "away"),
        "corners_total": stat_value("Corners", "total"),
        "on_target_home": stat_value("On Target", "home"),
        "on_target_away": stat_value("On Target", "away"),
        "on_target_total": stat_value("On Target", "total"),
        "dangerous_attacks_home": stat_value("Dangerous Attacks", "home"),
        "dangerous_attacks_away": stat_value("Dangerous Attacks", "away"),
        "dangerous_attacks_total": stat_value("Dangerous Attacks", "total"),
    }

    return meta


def follow_alerts(session):
    pending_alerts = MatchAlert.query.filter_by(status="pending").all()

    for alert in pending_alerts:
        rule = alert.rule
        if rule and not rule.follow_ht:
            continue

        stats_payload = fetch_match_stats(session, alert.url)
        if not stats_payload:
            continue

        ensure_second_half_baseline(alert.game_id, stats_payload)

        time_text = stats_payload.get("time_text", "")
        minute = stats_payload.get("minute") or 0
        current_score = stats_payload.get("score")

        # GREEN se saiu gol no 1º tempo
        if (
            alert.initial_score
            and current_score != alert.initial_score
            and is_first_half_goal(time_text, minute)
        ):
            alert.status = "green"
            alert.result_minute = minute
            alert.result_time_hhmm = datetime.utcnow().strftime("%H:%M")
            alert.ht_score = current_score
            alert.ht_stats_json = stats_to_json(stats_payload["stats"])
            db.session.commit()

            export_alert(alert, alert.rule.name, EXPORT_DIR)

            send_message(
                alert.user.telegram_token,
                alert.user.telegram_chat_id,
                f"✅ GREEN - gol no 1o tempo\n"
                f"{alert.home_team} vs {alert.away_team}\n"
                f"Tempo: {minute}'\n"
                f"Placar: {current_score}\n"
                f"Link: {alert.url}",
            )
            continue

        # RED no intervalo
        if is_half_time(time_text, minute):
            alert.status = "red"
            alert.result_minute = minute
            alert.result_time_hhmm = datetime.utcnow().strftime("%H:%M")
            alert.ht_score = current_score or alert.initial_score
            alert.ht_stats_json = stats_to_json(stats_payload["stats"])
            db.session.commit()

            export_alert(alert, alert.rule.name, EXPORT_DIR)

            send_message(
                alert.user.telegram_token,
                alert.user.telegram_chat_id,
                f"❌ RED - fim do 1o tempo sem gol\n"
                f"{alert.home_team} vs {alert.away_team}\n"
                f"Tempo: HT\n"
                f"Placar: {alert.ht_score}\n"
                f"Link: {alert.url}",
            )

        time.sleep(0.4)


def finalize_full_time(session):
    candidates = MatchAlert.query.filter_by(ft_completed=False).all()

    for alert in candidates:
        rule = alert.rule
        if rule and not rule.follow_ft:
            continue

        stats_payload = fetch_match_stats(session, alert.url)
        if not stats_payload:
            continue

        ensure_second_half_baseline(alert.game_id, stats_payload)

        time_text = stats_payload.get("time_text", "")
        minute = stats_payload.get("minute") or 0

        if not is_full_time(time_text, minute):
            continue

        alert.ft_score = stats_payload.get("score")
        alert.ft_stats_json = stats_to_json(stats_payload["stats"])
        alert.ft_completed = True
        db.session.commit()

        export_alert(alert, alert.rule.name, EXPORT_DIR)

        SECOND_HALF_BASELINES.pop(alert.game_id, None)

        time.sleep(0.4)
