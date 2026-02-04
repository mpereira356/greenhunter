import json
import os
import re
import threading
import time
from datetime import datetime

from sqlalchemy.exc import IntegrityError

from app.extensions import db
from app.models import LiveGameState, MatchAlert, Rule, User
from app.services.evaluator import compare, evaluate_rule, history_confidence, render_message, stats_to_json
from app.services.exporter import export_alert
from app.services.scraper import (
    fetch_live_games,
    fetch_match_history,
    fetch_match_stats,
    format_history_summary,
    is_first_half_extra_time,
    make_session,
    normalize_stat_key,
    summarize_history,
)
from app.services.telegram import send_message
from app.utils.time import now_sp

POLL_INTERVAL = int(os.environ.get("WORKER_INTERVAL", "15"))
GAME_DELAY = float(os.environ.get("WORKER_GAME_DELAY", "1.5"))
EXPORT_DIR = os.environ.get("EXPORT_DIR", "data/exports")
RULE_CONF_SAMPLE = int(os.environ.get("RULE_CONF_SAMPLE", "50"))
RULE_CONF_MIN = int(os.environ.get("RULE_CONF_MIN", "10"))

API_STATUS = {"ok": None, "code": None, "checked_at": None, "last_cycle": None}
API_ALERT_STATE = {"last_ok": None}
SECOND_HALF_BASELINES = {}
LAST_GAME_SNAPSHOTS = {}
HALFTIME_SEEN_AT = {}
HALFTIME_CONFIRMED_AT = {}
HALFTIME_CONFIRM_SECONDS = int(os.environ.get("HALFTIME_CONFIRM_SECONDS", "120"))
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
    API_STATUS["checked_at"] = now_sp().strftime("%Y-%m-%d %H:%M:%S")
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

def is_half_time_text(time_text: str) -> bool:
    text = (time_text or "").lower()
    return "ht" in text or "half time" in text or "interval" in text

def is_first_half_goal(time_text: str, minute: int) -> bool:
    text = (time_text or "").lower()
    if any(x in text for x in ["2nd", "2o", "2h", "2º"]): return False
    return 0 <= minute <= 47

def is_full_time(time_text: str, minute: int) -> bool:
    text = (time_text or "").lower()
    if any(x in text for x in ["ft", "full time", "finished", "ended", "fim", "encerrado", "final"]): return True
    return 90 <= minute <= 130

def is_second_half(time_text: str, minute: int) -> bool:
    text = (time_text or "").lower()
    if any(x in text for x in ["2nd", "2o", "2h", "2Âº", "2º", "second", "segundo"]): return True
    return False

def parse_score(score_text: str):
    if not score_text: return 0, 0
    nums = re.findall(r"\d+", score_text)
    return (int(nums[0]), int(nums[1])) if len(nums) >= 2 else (0, 0)

def rule_confidence_text(rule_id: int | None, user_id: int | None) -> str | None:
    if not rule_id or not user_id:
        return None
    alerts = (
        MatchAlert.query.filter_by(rule_id=rule_id, user_id=user_id)
        .filter(MatchAlert.status.in_(("green", "red")))
        .order_by(MatchAlert.created_at.desc())
        .limit(RULE_CONF_SAMPLE)
        .all()
    )
    total = len(alerts)
    if total < RULE_CONF_MIN:
        return None
    greens = sum(1 for alert in alerts if alert.status == "green")
    pct = round((greens / total) * 100)
    return f"{pct}% ({greens}/{total})"

def is_youth_match(stats_payload: dict) -> bool:
    if not stats_payload: return False
    hay = f"{stats_payload.get('league', '')} {stats_payload.get('home_team', '')} {stats_payload.get('away_team', '')}".lower()
    return any(token in hay for token in YOUTH_TOKENS)

def copy_stats(stats):
    return {key: value.copy() if isinstance(value, dict) else value for key, value in stats.items()}

def remember_game_snapshot(game_id: str, stats_payload) -> None:
    if not game_id or not stats_payload:
        return
    stats = stats_payload.get("stats")
    if not isinstance(stats, dict) or not stats:
        return
    LAST_GAME_SNAPSHOTS[game_id] = {
        "minute": stats_payload.get("minute"),
        "time_text": stats_payload.get("time_text", ""),
        "stats": copy_stats(stats),
    }

def _load_persisted_game_snapshot(game_id: str):
    if not game_id:
        return None
    state = LiveGameState.query.filter_by(game_id=game_id).first()
    if not state or not state.stats_json:
        return None
    try:
        stats = json.loads(state.stats_json)
    except Exception:
        return None
    if not isinstance(stats, dict) or not stats:
        return None
    return {
        "minute": state.minute,
        "time_text": state.time_text or "",
        "stats": copy_stats(stats),
    }


def _load_persisted_first_half_snapshot(game_id: str):
    if not game_id:
        return None
    state = LiveGameState.query.filter_by(game_id=game_id).first()
    if not state or not state.first_half_snapshot_json:
        return None
    try:
        stats = json.loads(state.first_half_snapshot_json)
    except Exception:
        return None
    if not isinstance(stats, dict) or not stats:
        return None
    return {
        "minute": state.first_half_snapshot_minute,
        "time_text": "HT",
        "stats": copy_stats(stats),
    }

def _baseline_source_for_second_half(game_id: str, stats_payload):
    current_stats = stats_payload.get("stats") if stats_payload else None
    if not isinstance(current_stats, dict):
        return current_stats
    snap = (
        LAST_GAME_SNAPSHOTS.get(game_id)
        or _load_persisted_game_snapshot(game_id)
        or _load_persisted_first_half_snapshot(game_id)
        or {}
    )
    prev_stats = snap.get("stats")
    prev_minute = snap.get("minute")
    prev_time = snap.get("time_text", "")
    if not isinstance(prev_stats, dict):
        return current_stats
    # If we just crossed to 2nd half, prefer the last first-half snapshot as baseline.
    if is_half_time_text(prev_time) or is_first_half_extra_time(prev_time):
        return prev_stats
    if isinstance(prev_minute, int) and prev_minute <= 45 and not is_second_half(prev_time, prev_minute):
        return prev_stats
    return current_stats


def _has_first_half_context(game_id: str) -> bool:
    snap = LAST_GAME_SNAPSHOTS.get(game_id) or {}
    prev_minute = snap.get("minute")
    prev_time = snap.get("time_text", "")
    if isinstance(prev_minute, int) and prev_minute <= 45 and not is_second_half(prev_time, prev_minute):
        return True
    persisted = _load_persisted_first_half_snapshot(game_id)
    return bool(persisted and isinstance(persisted.get("stats"), dict))

def ensure_second_half_baseline(game_id: str, stats_payload) -> None:
    if not stats_payload or not game_id or game_id in SECOND_HALF_BASELINES: return
    minute = stats_payload.get("minute") or 0
    time_text = stats_payload.get("time_text", "")
    if is_first_half_extra_time(time_text):
        return
    if minute < 45:
        HALFTIME_SEEN_AT.pop(game_id, None)
        HALFTIME_CONFIRMED_AT.pop(game_id, None)
        return

    # Consider 45/HT for >= HALFTIME_CONFIRM_SECONDS as confirmed interval.
    if minute == 45 or is_half_time_text(time_text):
        seen_at = HALFTIME_SEEN_AT.get(game_id)
        if not seen_at:
            HALFTIME_SEEN_AT[game_id] = now_sp()
            return
        if (now_sp() - seen_at).total_seconds() >= HALFTIME_CONFIRM_SECONDS:
            HALFTIME_CONFIRMED_AT[game_id] = now_sp()
            SECOND_HALF_BASELINES[game_id] = copy_stats(stats_payload.get("stats", {}))
            HALFTIME_SEEN_AT.pop(game_id, None)
            return
        return

    # From 46+ onward: if we have first-half context use it, otherwise reset from current minute
    # (startup/restart mid-match).
    if HALFTIME_CONFIRMED_AT.get(game_id) or _has_first_half_context(game_id):
        SECOND_HALF_BASELINES[game_id] = copy_stats(_baseline_source_for_second_half(game_id, stats_payload))
    else:
        SECOND_HALF_BASELINES[game_id] = copy_stats(stats_payload.get("stats", {}))
    HALFTIME_SEEN_AT.pop(game_id, None)
    HALFTIME_CONFIRMED_AT.pop(game_id, None)


def _load_persisted_second_half_baseline(game_id: str):
    if not game_id:
        return None
    state = LiveGameState.query.filter_by(game_id=game_id).first()
    if not state or not state.second_half_baseline_json:
        return None
    try:
        baseline = json.loads(state.second_half_baseline_json)
    except Exception:
        return None
    if isinstance(baseline, dict) and baseline:
        return baseline
    return None


def get_second_half_baseline(game_id: str):
    baseline = SECOND_HALF_BASELINES.get(game_id)
    if baseline:
        return baseline
    persisted = _load_persisted_second_half_baseline(game_id)
    if persisted:
        SECOND_HALF_BASELINES[game_id] = copy_stats(persisted)
        return SECOND_HALF_BASELINES[game_id]
    return None


def persist_live_game_state(game: dict, stats_payload: dict) -> bool:
    if not game or not stats_payload:
        return False
    game_id = game.get("game_id")
    if not game_id:
        return False
    state = LiveGameState.query.filter_by(game_id=game_id).first()
    if state is None:
        state = LiveGameState(game_id=game_id)

    state.url = game.get("url")
    state.league = stats_payload.get("league")
    state.home_team = stats_payload.get("home_team")
    state.away_team = stats_payload.get("away_team")
    state.time_text = stats_payload.get("time_text", "")
    state.minute = stats_payload.get("minute")
    state.score = stats_payload.get("score")
    state.stats_json = json.dumps(stats_payload.get("stats", {}), ensure_ascii=False)
    minute = stats_payload.get("minute") or 0
    time_text = stats_payload.get("time_text", "")
    if minute <= 45 and not is_first_half_extra_time(time_text):
        first_half_stats = copy_stats(stats_payload.get("stats", {}))
        if first_half_stats:
            state.first_half_snapshot_json = json.dumps(first_half_stats, ensure_ascii=False)
            state.first_half_snapshot_minute = minute

    baseline = SECOND_HALF_BASELINES.get(game_id)
    if baseline:
        state.second_half_baseline_json = json.dumps(baseline, ensure_ascii=False)
        if not state.second_half_started:
            state.second_half_started = True
            state.second_half_started_at = now_sp()
    else:
        if not state.second_half_baseline_json and minute >= 45:
            snapshot = copy_stats(stats_payload.get("stats", {}))
            if snapshot:
                state.second_half_baseline_json = json.dumps(snapshot, ensure_ascii=False)

    db.session.add(state)
    return True

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

def effective_minute(rule, minute: int | None) -> int | None:
    if minute is None:
        return None
    if rule and rule.second_half_only:
        return max(0, minute - 45)
    return minute

def should_time_red(rule, alert, minute: int | None) -> bool:
    if not rule or not rule.outcome_red_if_no_green or rule.outcome_red_minute is None:
        return False
    eff_minute = effective_minute(rule, minute)
    if eff_minute is not None and eff_minute >= rule.outcome_red_minute:
        return True
    if eff_minute is not None and eff_minute <= 1:
        return False
    # Fallback: use wall-clock if o minuto do jogo nao avança
    if alert and alert.created_at and alert.alert_minute is not None:
        alert_eff = effective_minute(rule, alert.alert_minute)
        if alert_eff is None:
            return False
        remaining = rule.outcome_red_minute - alert_eff
        if remaining <= 0:
            return True
        elapsed = (now_sp() - alert.created_at).total_seconds()
        if elapsed >= remaining * 60:
            return True
    return False

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

def maybe_notify_penalty(alert, stats, minute, score, time_text=None):
    if not alert:
        return
    rule = alert.rule
    user = alert.user
    if not rule or not rule.alert_on_penalty:
        return
    if not rule.notify_telegram:
        return
    if not user or not user.telegram_token or not user.telegram_chat_id:
        return
    if minute is None or minute <= 0:
        return
    if is_full_time(time_text or "", minute):
        return
    penalties_total = stats.get("Penalties", {}).get("total", 0) if isinstance(stats, dict) else 0
    if penalties_total <= 0:
        return
    if rule.time_limit_min and minute is not None and minute > rule.time_limit_min:
        return
    if not alert.penalty_baseline_set:
        alert.penalty_last_total = penalties_total if isinstance(penalties_total, int) else 0
        alert.penalty_notified = False
        alert.penalty_baseline_set = True
        db.session.commit()
        return
    last_total = alert.penalty_last_total if alert.penalty_last_total is not None else 0
    if penalties_total <= last_total:
        return
    alert.penalty_last_total = penalties_total
    if alert.penalty_notified:
        db.session.commit()
        return
    ok, _ = send_message(
        user.telegram_token,
        user.telegram_chat_id,
        f"Penalti agora!\nRegra: {rule.name}\n{alert.home_team} vs {alert.away_team}\nTempo: {minute}'\nPlacar: {score}\nLink: {alert.url}",
    )
    if ok:
        alert.penalty_notified = True
    db.session.commit()

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

def maybe_notify_penalty_for_game(game_id: str, stats_payload: dict):
    if not game_id or not stats_payload:
        return
    minute = stats_payload.get("minute")
    if minute is None:
        return
    score = stats_payload.get("score")
    stats = stats_payload.get("stats", {})
    time_text = stats_payload.get("time_text", "")
    home_team = stats_payload.get("home_team", "")
    away_team = stats_payload.get("away_team", "")
    # Keep behavior aligned with rule flow: only alerts that were triggered and are still pending.
    alerts = MatchAlert.query.filter(
        MatchAlert.game_id == game_id,
        MatchAlert.status == "pending",
    ).all()
    for alert in alerts:
        if home_team:
            alert.home_team = home_team
        if away_team:
            alert.away_team = away_team
        maybe_notify_penalty(alert, stats, minute, score, time_text=time_text)

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
            API_STATUS["last_cycle"] = now_sp().strftime("%Y-%m-%d %H:%M:%S")
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
        remember_game_snapshot(game["game_id"], stats_payload)
        if persist_live_game_state(game, stats_payload):
            try:
                db.session.commit()
            except Exception:
                db.session.rollback()
        # Check penalty notifications as soon as this game's stats are fetched.
        maybe_notify_penalty_for_game(game["game_id"], stats_payload)
        
        for rule in active_rules:
            user = rule.user
            existing = MatchAlert.query.filter_by(game_id=game["game_id"], rule_id=rule.id).first()
            if existing: continue

            stats_for_rule = stats_payload["stats"]
            h_score, a_score = parse_score(stats_payload.get("score", ""))
            if (rule.score_home is not None and h_score != rule.score_home) or \
               (rule.score_away is not None and a_score != rule.score_away):
                continue

            if rule.second_half_only:
                if is_first_half_extra_time(stats_payload.get("time_text", "")):
                    continue
                if minute < 46:
                    continue
                baseline = get_second_half_baseline(game["game_id"])
                if not baseline: continue
                stats_for_rule = apply_second_half_delta(stats_payload["stats"], baseline)
                m2h = max(0, minute - 45)
                stats_for_rule["Minute"] = {"home": m2h, "away": m2h, "total": m2h}

            if evaluate_rule(rule, stats_for_rule):
                if not user:
                    continue
                if rule.notify_telegram and (not user.telegram_token or not user.telegram_chat_id):
                    continue
                
                alert = MatchAlert(
                    rule_id=rule.id, user_id=user.id, game_id=game["game_id"], url=game["url"],
                    status="pending", alert_minute=minute, initial_score=stats_payload["score"],
                    last_score=stats_payload["score"], last_score_minute=minute,
                    initial_stats_json=stats_to_json(stats_for_rule),
                    league=stats_payload.get("league"), home_team=stats_payload.get("home_team"),
                    away_team=stats_payload.get("away_team")
                )
                db.session.add(alert)
                try:
                    db.session.commit()
                    rule.last_alert_at = now_sp()
                    rule.last_alert_desc = f"{alert.home_team} vs {alert.away_team}"
                    db.session.commit()

                    if rule.alert_on_penalty:
                        penalties_total = stats_payload.get("stats", {}).get("Penalties", {}).get("total", 0)
                        alert.penalty_last_total = penalties_total if isinstance(penalties_total, int) else 0
                        alert.penalty_notified = False
                        alert.penalty_baseline_set = True
                        db.session.commit()
                    
                    history_meta = {}
                    try:
                        history = fetch_match_history(session, game["url"])
                        h2h_summary = summarize_history(history.get("h2h", []))
                        home_summary = summarize_history(history.get("home", []))
                        away_summary = summarize_history(history.get("away", []))
                        h2h_items = history.get("h2h", [])
                        history_meta = {
                            "history_h2h": format_history_summary("H2H", h2h_summary) if h2h_summary else "Sem historico de um contra o outro",
                            "history_home": format_history_summary("Home", home_summary),
                            "history_away": format_history_summary("Away", away_summary),
                        }
                        conf_conds = [c for c in rule.outcome_conditions if c.outcome_type == "green"] or rule.conditions
                        confidence = history_confidence(conf_conds, h2h_items)
                        history_meta["history_confidence"] = f"{confidence}%" if confidence is not None else "Sem historico de um contra o outro"
                    except Exception:
                        history_meta = {}
                    meta = build_message_meta(rule, stats_payload, game, history_meta, stats_override=stats_for_rule)
                    if rule.notify_telegram and user.telegram_token and user.telegram_chat_id:
                        send_message(user.telegram_token, user.telegram_chat_id, render_message(rule, meta))
                except IntegrityError:
                    db.session.rollback()
        time.sleep(GAME_DELAY)
def build_message_meta(rule, stats_payload, game, history_meta=None, stats_override=None):
    stats = stats_override if isinstance(stats_override, dict) else stats_payload.get("stats", {})
    def sv(k, s): return stats.get(k, {}).get(s, "")
    meta = {
        "rule": rule.name, "home_team": stats_payload.get("home_team"), "away_team": stats_payload.get("away_team"),
        "minute": stats_payload.get("minute"), "score": stats_payload.get("score"), "url": game.get("url"),
        "league": stats_payload.get("league"), "time_limit": rule.time_limit_min,
        "goals_home": sv("Goals", "home"), "goals_away": sv("Goals", "away"), "goals_total": sv("Goals", "total"),
        "corners_home": sv("Corners", "home"), "corners_away": sv("Corners", "away"), "corners_total": sv("Corners", "total"),
        "on_target_home": sv("On Target", "home"), "on_target_away": sv("On Target", "away"), "on_target_total": sv("On Target", "total"),
        "off_target_home": sv("Off Target", "home"), "off_target_away": sv("Off Target", "away"), "off_target_total": sv("Off Target", "total"),
        "dangerous_attacks_home": sv("Dangerous Attacks", "home"), "dangerous_attacks_away": sv("Dangerous Attacks", "away"), "dangerous_attacks_total": sv("Dangerous Attacks", "total"),
    }
    if rule:
        meta["rule_confidence"] = rule_confidence_text(rule.id, rule.user_id)
    if history_meta:
        meta.update(history_meta)
    return meta

def follow_alerts(session):
    active_alerts = MatchAlert.query.filter(MatchAlert.status.in_(("pending", "green", "red"))).all()
    for alert in active_alerts:
        rule = alert.rule
        stats_payload = fetch_match_stats(session, alert.url)
        if not stats_payload: continue

        ensure_second_half_baseline(alert.game_id, stats_payload)
        remember_game_snapshot(alert.game_id, stats_payload)
        minute = stats_payload.get("minute") or 0
        current_score = stats_payload.get("score")
        stats = stats_payload.get("stats", {})
        prev_score = alert.last_score or alert.initial_score
        prev_minute = alert.last_score_minute if alert.last_score_minute is not None else alert.alert_minute
        if alert.status != "pending" and prev_score and current_score and minute:
            prev_home, prev_away = parse_score(prev_score)
            curr_home, curr_away = parse_score(current_score)
            prev_total = prev_home + prev_away
            curr_total = curr_home + curr_away
            if prev_minute is not None and minute < prev_minute:
                pass
            elif curr_total < prev_total or curr_home < prev_home or curr_away < prev_away:
                penalties_total = stats.get("Penalties", {}).get("total", 0) if isinstance(stats, dict) else 0
                alert.status = "pending"
                alert.result_minute = None
                alert.result_time_hhmm = None
                alert.ht_score = None
                alert.ht_stats_json = None
                alert.last_score = current_score
                alert.last_score_minute = minute
                alert.penalty_last_total = penalties_total if isinstance(penalties_total, int) else 0
                alert.penalty_notified = False
                alert.penalty_baseline_set = True
                db.session.commit()
                if rule and rule.notify_telegram and alert.user.telegram_token and alert.user.telegram_chat_id:
                    send_message(
                        alert.user.telegram_token,
                        alert.user.telegram_chat_id,
                        f"⚠️ Gol anulado detectado. Status voltou para pendente.\nRegra: {alert.rule.name}\n{alert.home_team} vs {alert.away_team}\nTempo: {minute}'\nPlacar: {current_score}\nLink: {alert.url}",
                    )
                continue

        if current_score:
            alert.last_score = current_score
            alert.last_score_minute = minute
            db.session.commit()

        if alert.status != "pending":
            continue
        maybe_notify_penalty(alert, stats, minute, current_score, time_text=stats_payload.get("time_text"))

        if rule and rule.second_half_only:
            baseline = get_second_half_baseline(alert.game_id)
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
        if should_time_red(rule, alert, minute):
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
    alert.result_time_hhmm = now_sp().strftime("%H:%M")
    alert.ht_score = score
    alert.ht_stats_json = stats_to_json(stats)
    alert.last_score = score
    alert.last_score_minute = minute
    db.session.commit()
    export_alert(alert, alert.rule.name, EXPORT_DIR)
    if alert.rule and alert.rule.notify_telegram and alert.user.telegram_token and alert.user.telegram_chat_id:
        send_message(
            alert.user.telegram_token,
            alert.user.telegram_chat_id,
            f"{msg_prefix}\nRegra: {alert.rule.name}\n{alert.home_team} vs {alert.away_team}\nTempo: {minute}'\nPlacar: {score}\nLink: {alert.url}",
        )

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
            LAST_GAME_SNAPSHOTS.pop(alert.game_id, None)
            HALFTIME_SEEN_AT.pop(alert.game_id, None)
        time.sleep(0.4)
