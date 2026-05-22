import json
import os
import re
import resource
import threading
import time
import unicodedata
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from statistics import mean
from types import SimpleNamespace
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import ObjectDeletedError, StaleDataError

from app.extensions import db
from app.models import LiveGameState, MatchAlert, Rule, User
from app.services.evaluator import _build_ai_assessment, _build_ai_commentary, compare, evaluate_rule, history_confidence, render_message, stats_to_json
from app.services.exporter import export_alert
from app.services.ml_engine import infer_green_profile, maybe_retrain_model, predict_alert_ml
from app.services.scraper import (
    fetch_live_games,
    fetch_match_history,
    fetch_match_stats,
    format_history_summary,
    is_first_half_extra_time,
    make_session,
    normalize_stat_key,
    summarize_history,
    is_second_half as scraper_is_second_half,
)
from app.services.telegram import edit_message_text, send_message
from app.utils.time import now_sp

POLL_INTERVAL = int(os.environ.get("WORKER_INTERVAL", "1"))
ALERTS_POLL_INTERVAL = float(os.environ.get("ALERTS_POLL_INTERVAL", "1"))
GAME_DELAY = float(os.environ.get("WORKER_GAME_DELAY", "1.5"))
FETCH_STATS_ATTEMPTS = max(1, int(os.environ.get("FETCH_STATS_ATTEMPTS", "2")))
FETCH_STATS_DELAY = max(0.0, float(os.environ.get("FETCH_STATS_DELAY", "0.25")))
ALERT_FETCH_STATS_ATTEMPTS = max(1, int(os.environ.get("ALERT_FETCH_STATS_ATTEMPTS", str(FETCH_STATS_ATTEMPTS))))
ALERT_FETCH_STATS_DELAY = max(0.0, float(os.environ.get("ALERT_FETCH_STATS_DELAY", str(FETCH_STATS_DELAY))))
FINALIZE_POLL_INTERVAL = max(5.0, float(os.environ.get("FINALIZE_POLL_INTERVAL", "60")))
FOLLOW_SETTLED_WINDOW_MINUTES = max(5, int(os.environ.get("FOLLOW_SETTLED_WINDOW_MINUTES", "20")))
FINALIZE_LOOKBACK_HOURS = max(1, int(os.environ.get("FINALIZE_LOOKBACK_HOURS", "12")))
EXPORT_DIR = os.environ.get("EXPORT_DIR", "data/exports")
RULE_CONF_SAMPLE = int(os.environ.get("RULE_CONF_SAMPLE", "50"))
RULE_CONF_MIN = int(os.environ.get("RULE_CONF_MIN", "10"))
LEAGUE_CONF_SAMPLE = int(os.environ.get("LEAGUE_CONF_SAMPLE", "80"))
LEAGUE_CONF_MIN = int(os.environ.get("LEAGUE_CONF_MIN", "8"))

API_STATUS = {"ok": None, "code": None, "checked_at": None, "last_cycle": None}
API_ALERT_STATE = {"last_ok": None}
BOT_STARTED_AT = None
LAST_FINALIZE_RUN_AT = None
SECOND_HALF_BASELINES = {}
SECOND_HALF_FROM_NOW = {}
GAME_FIRST_SEEN_AT = {}
LAST_GAME_SNAPSHOTS = {}
HALFTIME_SEEN_AT = {}
HALFTIME_CONFIRMED_AT = {}
HALFTIME_CONFIRM_SECONDS = int(os.environ.get("HALFTIME_CONFIRM_SECONDS", "180"))
RED_CONFIRM_PENDING = {}
RED_CONFIRM_SECONDS = int(os.environ.get("RED_CONFIRM_SECONDS", "15"))
FORCE_SECOND_HALF_FROM_FIRST_HALF = os.environ.get("FORCE_SECOND_HALF_FROM_FIRST_HALF", "0").strip().lower() in ("1", "true", "yes")
NON_DELTA_KEYS = {"Minute", "Possession"}
ALERT_FRESH_SECONDS = int(os.environ.get("ALERT_FRESH_SECONDS", "180"))
RED_CORRECTION_SECONDS = int(os.environ.get("RED_CORRECTION_SECONDS", "300"))
ANALYSIS_THREADS = max(1, int(os.environ.get("WORKER_ANALYSIS_THREADS", "4")))
USE_SERIAL_PREFETCH = os.environ.get("WORKER_SERIAL_PREFETCH", "0").strip().lower() in ("1", "true", "yes")
IA_SHADOW_ENABLED = os.environ.get("IA_SHADOW_ENABLED", "1").strip().lower() in ("1", "true", "yes")
IA_SHADOW_NOTIFY = os.environ.get("IA_SHADOW_NOTIFY", "1").strip().lower() in ("1", "true", "yes")
IA_SHADOW_ENFORCE_MAIN_RULES = os.environ.get("IA_SHADOW_ENFORCE_MAIN_RULES", "0").strip().lower() in ("1", "true", "yes")
IA_SHADOW_PROFILE_REFRESH_SECONDS = int(os.environ.get("IA_SHADOW_PROFILE_REFRESH_SECONDS", "600"))
IA_SHADOW_MIN_SAMPLES = int(os.environ.get("IA_SHADOW_MIN_SAMPLES", "120"))
IA_SHADOW_RULE_MIN_SAMPLES = int(os.environ.get("IA_SHADOW_RULE_MIN_SAMPLES", "24"))
IA_SHADOW_COOLDOWN_SECONDS = int(os.environ.get("IA_SHADOW_COOLDOWN_SECONDS", "900"))
IA_SHADOW_LOG_PATH = os.environ.get("IA_SHADOW_LOG_PATH", "data/exports/ia_shadow_events.jsonl")
IA_SHADOW_1H_MIN_GREEN_RATE = float(os.environ.get("IA_SHADOW_1H_MIN_GREEN_RATE", "52"))
IA_SHADOW_2H_MIN_GREEN_RATE = float(os.environ.get("IA_SHADOW_2H_MIN_GREEN_RATE", "55"))
IA_SHADOW_2H_MIN_SAMPLES = int(os.environ.get("IA_SHADOW_2H_MIN_SAMPLES", "120"))
IA_SHADOW_CONTROL_RULE_NAME = os.environ.get("IA_SHADOW_CONTROL_RULE_NAME", "REGRA IA SOMBRA (Sistema)")
YOUTH_TOKENS = (
    "u19", "u-19", "u 19", "sub19", "sub-19", "sub 19", "under 19",
    "u20", "u-20", "u 20", "sub20", "sub-20", "sub 20", "under 20",
)
IA_SHADOW_LAST_SENT = {}
IA_SHADOW_PROFILE_CACHE = None
IA_SHADOW_PROFILE_UPDATED_AT = None
IA_SHADOW_LOCK = threading.Lock()
GREEN_GROUP_WINDOW_SECONDS = max(1, int(os.environ.get("GREEN_GROUP_WINDOW_SECONDS", "8")))
GREEN_NOTIFICATION_PENDING = {}
RED_GROUP_WINDOW_SECONDS = max(1, int(os.environ.get("RED_GROUP_WINDOW_SECONDS", "180")))
RED_NOTIFICATION_PENDING = {}
ENTRY_GROUP_WINDOW_SECONDS = max(0, int(os.environ.get("ENTRY_GROUP_WINDOW_SECONDS", "0")))
REALTIME_ENTRY_ALERTS = os.environ.get("REALTIME_ENTRY_ALERTS", "1").strip().lower() in ("1", "true", "yes")
REALTIME_SKIP_ENTRY_HISTORY = os.environ.get("REALTIME_SKIP_ENTRY_HISTORY", "1").strip().lower() in ("1", "true", "yes")
ML_AUTOTRAIN_ENABLED = os.environ.get("ML_AUTOTRAIN_ENABLED", "0").strip().lower() in ("1", "true", "yes")
ENTRY_NOTIFICATION_PENDING = {}
ENTRY_ENRICHMENT_QUEUE = deque()
ENTRY_ENRICHMENT_QUEUED = set()
ENTRY_ENRICHMENT_LOCK = threading.Lock()

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
                _send_message_safe(user.telegram_token, user.telegram_chat_id, message, context="api_off")
        return

    if last_ok == ok: return
    API_ALERT_STATE["last_ok"] = ok
    users = User.query.filter_by(telegram_verified=True).all()
    if not users: return

    message = "API voltou ao normal (status 200)." if ok else f"API OFF: possivel anti-bot ativo ({'HTTP ' + str(code) if code else 'erro de conexao/anti-bot'})."
    for user in users:
        if user.telegram_token and user.telegram_chat_id:
            _send_message_safe(user.telegram_token, user.telegram_chat_id, message, context="api_status")


def _send_message_safe(token: str, chat_id: str, text: str, context: str = "") -> bool:
    ok, _detail, _message_id = _send_message_result(token, chat_id, text, context=context)
    return ok


def _send_message_result(token: str, chat_id: str, text: str, context: str = ""):
    try:
        result = send_message(token, chat_id, text)
        message_id = None
        if isinstance(result, tuple):
            ok, detail = bool(result[0]), result[1] if len(result) > 1 else ""
            if len(result) > 2:
                message_id = result[2]
        else:
            ok, detail = bool(result), ""
        if not ok:
            print(f"[telegram] envio falhou ({context}): {detail}")
        return ok, detail, message_id
    except Exception as exc:
        print(f"[telegram] excecao ao enviar ({context}): {exc}")
        return False, str(exc), None


def _edit_message_safe(token: str, chat_id: str, message_id: int, text: str, context: str = "") -> bool:
    try:
        ok, detail = edit_message_text(token, chat_id, message_id, text)
        if not ok:
            print(f"[telegram] edicao falhou ({context}): {detail}")
        return ok
    except Exception as exc:
        print(f"[telegram] excecao ao editar ({context}): {exc}")
        return False


def _stats_total_from_json(stats_json: str | None, key: str) -> int:
    if not stats_json:
        return 0
    try:
        payload = json.loads(stats_json)
    except Exception:
        return 0
    if not isinstance(payload, dict):
        return 0
    bucket = payload.get(key)
    if not isinstance(bucket, dict):
        return 0
    try:
        return int(bucket.get("total", 0) or 0)
    except Exception:
        return 0


def _stats_total_live(stats: dict | None, key: str) -> int:
    if not isinstance(stats, dict):
        return 0
    bucket = stats.get(key)
    if not isinstance(bucket, dict):
        return 0
    try:
        return int(bucket.get("total", 0) or 0)
    except Exception:
        return 0


def _ia_shadow_assessment_for_phase(phase: str, minute: int, score: str | None, stats: dict | None) -> dict:
    home, away = parse_score(score or "0 x 0")
    fake_name = "REGRA IA 1T HT" if phase == "1H" else "REGRA IA 2T"
    fake_rule = SimpleNamespace(name=fake_name, message_template="")
    meta = {
        "minute": minute,
        "goals_home": home,
        "goals_away": away,
        "goals_total": home + away,
        "on_target_total": _stats_total_live(stats, "On Target"),
        "dangerous_attacks_total": _stats_total_live(stats, "Dangerous Attacks"),
        "corners_total": _stats_total_live(stats, "Corners"),
        "rule_confidence": None,
        "league_rule_confidence": None,
        "history_confidence": None,
    }
    return _build_ai_assessment(fake_rule, meta)


def _phase_from_alert(alert) -> str:
    minute = alert.alert_minute or 0
    if alert.rule and alert.rule.second_half_only:
        return "2H"
    return "1H" if minute <= 47 else "2H"


def _build_phase_profile(phase: str, rows: list[dict], min_samples: int, rule_id: int | None = None) -> dict | None:
    if len(rows) < min_samples:
        return None
    greens = [r for r in rows if r.get("status") == "green"]
    reds = [r for r in rows if r.get("status") == "red"]
    if len(greens) < max(6, min_samples // 4):
        return None

    def _vals(items, key):
        out = [r.get(key, 0) for r in items]
        return [int(v) for v in out if isinstance(v, (int, float))]

    def _threshold(key, default):
        gv = _vals(greens, key)
        rv = _vals(reds, key)
        if not gv:
            return default
        g_avg = mean(gv)
        r_avg = mean(rv) if rv else (g_avg * 0.8)
        if g_avg <= r_avg:
            return default
        return int(round((g_avg + r_avg) / 2.0))

    min_minute_default = 16 if phase == "1H" else 50
    max_minute_default = 45 if phase == "1H" else 88
    minute_values = _vals(greens, "minute")
    if minute_values:
        minute_values = sorted(minute_values)
        lo_idx = max(0, int(len(minute_values) * 0.2) - 1)
        hi_idx = min(len(minute_values) - 1, int(len(minute_values) * 0.85))
        min_minute = minute_values[lo_idx]
        max_minute = minute_values[hi_idx]
    else:
        min_minute = min_minute_default
        max_minute = max_minute_default
    if phase == "1H":
        min_minute = max(8, min(min_minute, 42))
        max_minute = max(min_minute + 1, min(47, max_minute))
    else:
        min_minute = max(46, min(min_minute, 80))
        max_minute = max(min_minute + 1, min(92, max_minute))

    return {
        "phase": phase,
        "rule_id": rule_id,
        "samples": len(rows),
        "greens": len(greens),
        "reds": len(reds),
        "green_rate": round((len(greens) / len(rows)) * 100.0, 2),
        "min_minute": int(min_minute),
        "max_minute": int(max_minute),
        "min_ai_score": _threshold("ai_score", 64 if phase == "1H" else 66),
        "min_on_target_total": max(2, _threshold("on_target_total", 4 if phase == "1H" else 5)),
        "min_dangerous_total": max(12, _threshold("dangerous_total", 34 if phase == "1H" else 48)),
        "min_corners_total": max(0, _threshold("corners_total", 3 if phase == "1H" else 4)),
    }


def _min_green_rate_for_phase(phase: str) -> float:
    return IA_SHADOW_1H_MIN_GREEN_RATE if phase == "1H" else IA_SHADOW_2H_MIN_GREEN_RATE


def _profile_has_enough_samples(profile: dict | None, phase: str, is_rule_specific: bool) -> bool:
    if not profile:
        return False
    minimum = IA_SHADOW_RULE_MIN_SAMPLES if is_rule_specific else IA_SHADOW_MIN_SAMPLES
    if phase == "2H":
        minimum = max(minimum, IA_SHADOW_2H_MIN_SAMPLES)
    return int(profile.get("samples") or 0) >= minimum


def _profile_is_actionable(profile: dict | None, phase: str, is_rule_specific: bool) -> bool:
    if not _profile_has_enough_samples(profile, phase, is_rule_specific):
        return False
    return float(profile.get("green_rate") or 0.0) >= _min_green_rate_for_phase(phase)


def _select_ia_shadow_profile(profiles: dict | None, rule_id: int | None, phase: str) -> tuple[dict | None, str | None]:
    profiles = profiles or {}
    if rule_id is not None:
        rule_profiles = (profiles.get("rules") or {}).get(str(rule_id), {})
        rule_profile = rule_profiles.get(phase)
        if _profile_is_actionable(rule_profile, phase, is_rule_specific=True):
            return rule_profile, "rule"
        if _profile_has_enough_samples(rule_profile, phase, is_rule_specific=True):
            return None, "rule_blocked"

    phase_profile = (profiles.get("phases") or {}).get(phase)
    if _profile_is_actionable(phase_profile, phase, is_rule_specific=False):
        return phase_profile, "phase"
    return None, None


def _profile_accepts_snapshot(profile: dict | None, minute: int, ai_score: int, on_target_total: int, dangerous_total: int, corners_total: int) -> bool:
    if not profile:
        return True
    if minute < int(profile.get("min_minute") or 0) or minute > int(profile.get("max_minute") or 999):
        return False
    if ai_score < int(profile.get("min_ai_score") or 0):
        return False
    if on_target_total < int(profile.get("min_on_target_total") or 0):
        return False
    if dangerous_total < int(profile.get("min_dangerous_total") or 0):
        return False
    if corners_total < int(profile.get("min_corners_total") or 0):
        return False
    return True


def _resolve_ia_shadow_gate(rule_id: int | None, phase: str, minute: int, score_text: str | None, stats: dict | None, profiles: dict | None) -> dict:
    assessment = _ia_shadow_assessment_for_phase(phase, minute, score_text, stats)
    ai_score = int(assessment.get("score") or 0)
    on_target_total = _stats_total_live(stats, "On Target")
    dangerous_total = _stats_total_live(stats, "Dangerous Attacks")
    corners_total = _stats_total_live(stats, "Corners")
    profile, source = _select_ia_shadow_profile(profiles, rule_id, phase)
    accepted = _profile_accepts_snapshot(profile, minute, ai_score, on_target_total, dangerous_total, corners_total)
    if source == "rule_blocked":
        accepted = False
    return {
        "accepted": accepted,
        "profile": profile,
        "profile_source": source,
        "ai_score": ai_score,
        "on_target_total": on_target_total,
        "dangerous_total": dangerous_total,
        "corners_total": corners_total,
    }


def _compute_ai_shadow_profiles() -> dict:
    alerts = (
        MatchAlert.query.filter(MatchAlert.status.in_(("green", "red")))
        .order_by(MatchAlert.created_at.desc())
        .limit(5000)
        .all()
    )
    grouped = {"1H": [], "2H": []}
    grouped_by_rule = {}
    for alert in alerts:
        minute = alert.alert_minute or 0
        phase = _phase_from_alert(alert)
        try:
            base_stats = json.loads(alert.initial_stats_json) if alert.initial_stats_json else {}
        except Exception:
            base_stats = {}
        ai_assessment = _ia_shadow_assessment_for_phase(
            phase=phase,
            minute=minute,
            score=alert.initial_score,
            stats=base_stats,
        )
        row = {
            "status": alert.status,
            "minute": minute,
            "ai_score": int(ai_assessment.get("score") or 0),
            "on_target_total": _stats_total_from_json(alert.initial_stats_json, "On Target"),
            "dangerous_total": _stats_total_from_json(alert.initial_stats_json, "Dangerous Attacks"),
            "corners_total": _stats_total_from_json(alert.initial_stats_json, "Corners"),
        }
        grouped[phase].append(row)
        grouped_by_rule.setdefault(str(alert.rule_id), {"1H": [], "2H": []})[phase].append(row)

    profiles = {"phases": {}, "rules": {}}
    for phase in ("1H", "2H"):
        profile = _build_phase_profile(phase, grouped.get(phase, []), IA_SHADOW_MIN_SAMPLES)
        if profile:
            profiles["phases"][phase] = profile
    for rule_id, phase_rows in grouped_by_rule.items():
        for phase in ("1H", "2H"):
            profile = _build_phase_profile(
                phase,
                phase_rows.get(phase, []),
                IA_SHADOW_RULE_MIN_SAMPLES,
                rule_id=int(rule_id),
            )
            if profile:
                profiles["rules"].setdefault(rule_id, {})[phase] = profile
    return profiles


def _get_ai_shadow_profiles(force: bool = False) -> dict:
    global IA_SHADOW_PROFILE_CACHE, IA_SHADOW_PROFILE_UPDATED_AT
    with IA_SHADOW_LOCK:
        now = now_sp()
        if not force and IA_SHADOW_PROFILE_CACHE and IA_SHADOW_PROFILE_UPDATED_AT:
            if (now - IA_SHADOW_PROFILE_UPDATED_AT).total_seconds() < IA_SHADOW_PROFILE_REFRESH_SECONDS:
                return IA_SHADOW_PROFILE_CACHE
        IA_SHADOW_PROFILE_CACHE = _compute_ai_shadow_profiles()
        IA_SHADOW_PROFILE_UPDATED_AT = now
        return IA_SHADOW_PROFILE_CACHE or {}


def _is_ia_shadow_enabled_for_user(user_id: int) -> bool:
    rule = (
        Rule.query.filter_by(user_id=user_id, name=IA_SHADOW_CONTROL_RULE_NAME)
        .order_by(Rule.id.asc())
        .first()
    )
    if not rule:
        # Backward-compatible default: if control rule does not exist yet, keep IA shadow on.
        return True
    return bool(rule.is_active)


def _ia_shadow_recipients() -> list[User]:
    admins = User.query.filter_by(telegram_verified=True, is_admin=True).all()
    if admins:
        return [u for u in admins if u.telegram_token and u.telegram_chat_id]
    all_verified = User.query.filter_by(telegram_verified=True).all()
    return [u for u in all_verified if u.telegram_token and u.telegram_chat_id]


def _ia_shadow_log_event(event: dict):
    if not IA_SHADOW_LOG_PATH:
        return
    try:
        folder = os.path.dirname(IA_SHADOW_LOG_PATH)
        if folder:
            os.makedirs(folder, exist_ok=True)
        with open(IA_SHADOW_LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, ensure_ascii=False) + "\n")
    except Exception as exc:
        print(f"[ia-shadow] falha ao gravar log: {exc}")


def _maybe_emit_ia_shadow_signal(game: dict, stats_payload: dict, profiles: dict):
    if not IA_SHADOW_ENABLED:
        return
    minute = stats_payload.get("minute")
    if not isinstance(minute, int):
        return
    phase = "1H" if minute <= 47 else "2H"
    profile, profile_source = _select_ia_shadow_profile(profiles, None, phase)
    if not profile:
        return

    game_id = str(game.get("game_id") or "")
    if not game_id:
        return
    dedupe_key = f"{game_id}:{phase}"
    last_sent = IA_SHADOW_LAST_SENT.get(dedupe_key)
    if last_sent and (now_sp() - last_sent).total_seconds() < IA_SHADOW_COOLDOWN_SECONDS:
        return

    score_text = stats_payload.get("score") or "0 x 0"
    stats = stats_payload.get("stats", {})
    gate = _resolve_ia_shadow_gate(None, phase, minute, score_text, stats, profiles)
    if not gate["accepted"]:
        return
    ai_score = gate["ai_score"]
    on_target_total = gate["on_target_total"]
    dangerous_total = gate["dangerous_total"]
    corners_total = gate["corners_total"]

    msg = (
        f"REGRA IA ({phase})\n"
        f"Jogo: {stats_payload.get('home_team')} vs {stats_payload.get('away_team')}\n"
        f"Liga: {stats_payload.get('league')}\n"
        f"Tempo: {minute}' | Placar: {score_text}\n"
        f"AI Score: {ai_score}/100\n"
        f"On Target: {on_target_total} | Dangerous: {dangerous_total} | Corners: {corners_total}\n"
        f"Perfil {phase} [{profile_source or 'phase'}]: score>={profile['min_ai_score']} | OT>={profile['min_on_target_total']} | DA>={profile['min_dangerous_total']} | C>={profile['min_corners_total']} | min {profile['min_minute']}-{profile['max_minute']}\n"
        f"Link: {game.get('url')}\n"
        f"Modo: sombra (sem aposta automatica)"
    )

    if IA_SHADOW_NOTIFY:
        for user in _ia_shadow_recipients():
            if not _is_ia_shadow_enabled_for_user(user.id):
                continue
            _send_message_safe(
                user.telegram_token,
                user.telegram_chat_id,
                msg,
                context=f"ia_shadow_{phase}",
            )
    IA_SHADOW_LAST_SENT[dedupe_key] = now_sp()
    _ia_shadow_log_event(
        {
            "at": now_sp().strftime("%Y-%m-%d %H:%M:%S"),
            "phase": phase,
            "game_id": game_id,
            "league": stats_payload.get("league"),
            "home_team": stats_payload.get("home_team"),
            "away_team": stats_payload.get("away_team"),
            "minute": minute,
            "score": score_text,
            "ai_score": ai_score,
            "on_target_total": on_target_total,
            "dangerous_total": dangerous_total,
            "corners_total": corners_total,
            "profile": profile,
            "profile_source": profile_source,
            "url": game.get("url"),
        }
    )


def _commit_allow_missing_alert(alert_id=None, context: str = "") -> bool:
    try:
        db.session.commit()
        return True
    except (StaleDataError, ObjectDeletedError) as exc:
        db.session.rollback()
        print(f"[worker] stale alert ignored (alert_id={alert_id}, context={context}): {exc}")
        return False


def _cache_bust_url(url: str) -> str:
    if not url:
        return url
    try:
        parsed = urlparse(url)
        params = parse_qsl(parsed.query, keep_blank_values=True)
        params = [(k, v) for k, v in params if k != "_"]
        params.append(("_", str(int(time.time()))))
        new_query = urlencode(params)
        return urlunparse(parsed._replace(query=new_query))
    except Exception:
        return url

def fetch_match_stats_fresh(session, url: str, attempts: int = 3, delay: float = 1.0):
    best = None
    for _ in range(max(1, attempts)):
        payload = fetch_match_stats(session, _cache_bust_url(url))
        if payload:
            if not best:
                best = payload
            else:
                ma = best.get("minute") or 0
                mb = payload.get("minute") or 0
                sa = parse_score(best.get("score", ""))
                sb = parse_score(payload.get("score", ""))
                if mb > ma or sum(sb) > sum(sa):
                    best = payload
        if delay:
            time.sleep(delay)
    return best


def _recently_settled_alerts_query():
    cutoff = now_sp() - timedelta(minutes=FOLLOW_SETTLED_WINDOW_MINUTES)
    return (
        MatchAlert.query.filter_by(ft_completed=False)
        .filter(MatchAlert.status.in_(("green", "red")))
        .filter(MatchAlert.created_at >= cutoff)
        .all()
    )

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
    return scraper_is_second_half(time_text, minute)

def second_half_allowed(game_id: str, time_text: str, minute: int | None) -> bool:
    if minute is None:
        return False
    if is_half_time_text(time_text):
        return False
    if is_first_half_extra_time(time_text):
        return False
    # Only allow 2H alerts after HT confirmed or 50+ to avoid 45+ stoppage false positives.
    if not HALFTIME_CONFIRMED_AT.get(game_id) and minute < 50:
        return False
    if minute < 46:
        return False
    return True

def _is_recent_game_update(game_id: str) -> bool:
    if not game_id:
        return False
    state = LiveGameState.query.filter_by(game_id=game_id).first()
    if not state or not state.updated_at:
        return False
    return (now_sp() - state.updated_at).total_seconds() <= ALERT_FRESH_SECONDS


def _last_live_update_at(game_id: str):
    if not game_id:
        return None
    state = LiveGameState.query.filter_by(game_id=game_id).first()
    if not state:
        return None
    return state.updated_at

def _result_time_to_dt(result_time_hhmm: str | None):
    if not result_time_hhmm:
        return None
    try:
        hh, mm = result_time_hhmm.split(":")
        dt = now_sp().replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
        # handle cross-midnight edge
        if dt > now_sp():
            dt = dt.replace(day=dt.day - 1)
        return dt
    except Exception:
        return None

def parse_score(score_text: str):
    if not score_text: return 0, 0
    nums = re.findall(r"\d+", score_text)
    return (int(nums[0]), int(nums[1])) if len(nums) >= 2 else (0, 0)


def _events_to_json(events) -> str | None:
    if not events:
        return None
    try:
        return json.dumps(events, ensure_ascii=False)
    except Exception:
        return None


def _normalize_team_token(value: str) -> str:
    text = str(value or "").strip().casefold()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "", text)


def _event_in_first_half(event: dict) -> bool:
    if not isinstance(event, dict):
        return False
    time_text = str(event.get("time_text") or "")
    if time_text.startswith("45+"):
        return True
    minute = event.get("minute")
    return isinstance(minute, int) and minute <= 45


def _build_event_metrics(events, alert_minute: int | None = None) -> dict:
    if not isinstance(events, list):
        return {}
    timeline = [item for item in events if isinstance(item, dict)]
    metrics = {
        "total_events": len(timeline),
        "goals_total": 0,
        "corners_total": 0,
        "yellow_cards_total": 0,
        "red_cards_total": 0,
        "first_goal_minute": None,
        "first_goal_team": None,
        "goal_before_ht": False,
        "goals_before_alert": 0,
        "corners_before_alert": 0,
        "yellow_cards_before_alert": 0,
        "red_cards_before_alert": 0,
        "events_after_alert_count": 0,
        "goals_after_alert": 0,
        "corners_after_alert": 0,
        "yellow_cards_after_alert": 0,
        "red_cards_after_alert": 0,
        "time_to_first_goal_after_alert": None,
    }
    first_goal_after_alert = None
    for event in timeline:
        kind = event.get("kind")
        minute = event.get("minute")
        if kind == "goal":
            metrics["goals_total"] += 1
            if metrics["first_goal_minute"] is None:
                metrics["first_goal_minute"] = minute
                metrics["first_goal_team"] = event.get("team")
            if _event_in_first_half(event):
                metrics["goal_before_ht"] = True
        elif kind == "corner":
            metrics["corners_total"] += 1
        elif kind == "yellow_card":
            metrics["yellow_cards_total"] += 1
        elif kind == "red_card":
            metrics["red_cards_total"] += 1

        if alert_minute is None or not isinstance(minute, int):
            continue
        if minute <= alert_minute:
            if kind == "goal":
                metrics["goals_before_alert"] += 1
            elif kind == "corner":
                metrics["corners_before_alert"] += 1
            elif kind == "yellow_card":
                metrics["yellow_cards_before_alert"] += 1
            elif kind == "red_card":
                metrics["red_cards_before_alert"] += 1
        if minute >= alert_minute:
            metrics["events_after_alert_count"] += 1
            if kind == "goal":
                metrics["goals_after_alert"] += 1
                if first_goal_after_alert is None:
                    first_goal_after_alert = minute
            elif kind == "corner":
                metrics["corners_after_alert"] += 1
            elif kind == "yellow_card":
                metrics["yellow_cards_after_alert"] += 1
            elif kind == "red_card":
                metrics["red_cards_after_alert"] += 1
    if first_goal_after_alert is not None and isinstance(alert_minute, int):
        metrics["time_to_first_goal_after_alert"] = max(0, first_goal_after_alert - alert_minute)
    return metrics


def _event_metrics_json(events, alert_minute: int | None = None) -> str | None:
    metrics = _build_event_metrics(events, alert_minute=alert_minute)
    if not metrics:
        return None
    return json.dumps(metrics, ensure_ascii=False)


def _ml_feedback_for_result(alert, final_status: str) -> str:
    score = alert.ml_pred_score
    verdict = alert.ml_pred_verdict
    samples = alert.ml_model_samples
    if score is None:
        return "ML: sem previsao registrada para este alerta."

    predicted_green = score >= 50
    actual_green = final_status == "green"
    correct = predicted_green == actual_green

    predicted_text = "entrada" if predicted_green else "evitar"
    if correct:
        msg = f"ML: acertou esta leitura (indicacao: {predicted_text}, score {score}/100)"
        msg += " e segue melhorando com novos resultados."
    else:
        msg = f"ML: errou esta leitura (indicacao: {predicted_text}, score {score}/100)."
        msg += " Vai ajustar no proximo ciclo de treino."

    if verdict:
        msg += f" Veredito: {verdict}."
    if samples:
        msg += f" Base atual: {samples} amostras."
    return msg


def _normalized_market_target(rule, market_ctx: dict) -> tuple[str, str, str, str]:
    market_key = str(market_ctx.get("market_key") or "")
    side = str(market_ctx.get("target_side") or "")
    operator = str(market_ctx.get("target_operator") or "")
    target_value = market_ctx.get("target_value")
    normalized_value = str(target_value) if target_value not in (None, "", "*") else ""

    if market_key == "goals" and side == "total" and operator in ("", ">="):
        if not normalized_value:
            hay = f"{getattr(rule, 'name', '')} {getattr(rule, 'message_template', '')}".lower()
            score_pairs = re.findall(r"(\d+)\s*x\s*(\d+)", hay)
            if score_pairs:
                normalized_value = score_pairs[0][0]
            else:
                goal_mentions = re.findall(r"(\d+)\s*gols?", hay)
                normalized_value = goal_mentions[0] if goal_mentions else "1"
            operator = ">="

    signature_family = "|".join(part for part in (market_key, side, operator, normalized_value) if part)
    return market_key, side, operator, signature_family


def _notification_group_key(alert, status: str) -> tuple:
    rule = getattr(alert, "rule", None)
    market_ctx = infer_green_profile(rule)
    stage_attr = "outcome_green_stage" if status in ("green", "entry") else "outcome_red_stage"
    stage = (getattr(rule, stage_attr, None) or "HT").upper() if rule else "HT"
    market_key, side, operator, signature_family = _normalized_market_target(rule, market_ctx)
    return (
        getattr(alert, "user_id", None),
        str(getattr(alert, "game_id", "") or ""),
        market_key,
        side,
        operator,
        signature_family,
        stage,
    )


def _notification_pending_store(status: str) -> dict:
    if status == "green":
        return GREEN_NOTIFICATION_PENDING
    if status == "red":
        return RED_NOTIFICATION_PENDING
    return ENTRY_NOTIFICATION_PENDING


def _notification_group_window_seconds(status: str) -> int:
    if status == "green":
        return GREEN_GROUP_WINDOW_SECONDS
    if status == "red":
        return RED_GROUP_WINDOW_SECONDS
    return ENTRY_GROUP_WINDOW_SECONDS


def _queue_grouped_notification(alert, minute, score, msg_prefix, status: str):
    if not alert or not getattr(alert, "rule", None):
        return
    user = getattr(alert, "user", None)
    rule = getattr(alert, "rule", None)
    if not user or not rule or not rule.notify_telegram or not user.telegram_token or not user.telegram_chat_id:
        return

    now = now_sp()
    key = _notification_group_key(alert, status)
    market_ctx = infer_green_profile(rule)
    stage_attr = "outcome_green_stage" if status == "green" else "outcome_red_stage"
    entry = _notification_pending_store(status).get(key)
    if not entry:
        entry = {
            "created_at": now,
            "last_updated_at": now,
            "token": user.telegram_token,
            "chat_id": user.telegram_chat_id,
            "home_team": alert.home_team,
            "away_team": alert.away_team,
            "url": alert.url,
            "market_label": market_ctx.get("market_label") or "Mercado",
            "target_text": market_ctx.get("target_text") or "",
            "stage": (getattr(rule, stage_attr, None) or "HT").upper(),
            "msg_prefix": msg_prefix,
            "status": status,
            "alerts": {},
        }
        _notification_pending_store(status)[key] = entry

    entry["last_updated_at"] = now
    entry["home_team"] = alert.home_team or entry.get("home_team")
    entry["away_team"] = alert.away_team or entry.get("away_team")
    entry["url"] = alert.url or entry.get("url")
    entry["market_label"] = market_ctx.get("market_label") or entry.get("market_label")
    entry["target_text"] = market_ctx.get("target_text") or entry.get("target_text")
    entry["stage"] = (getattr(rule, stage_attr, None) or entry.get("stage") or "HT").upper()
    entry["alerts"][alert.id] = {
        "rule_name": rule.name,
        "minute": minute,
        "score": score,
        "ml_score": alert.ml_pred_score,
        "ml_verdict": alert.ml_pred_verdict,
        "ml_model_samples": alert.ml_model_samples,
        "result_time_hhmm": alert.result_time_hhmm,
    }


def _flush_grouped_notification_queue(status: str, force: bool = False):
    pending_store = _notification_pending_store(status)
    if not pending_store:
        return
    now = now_sp()
    to_delete = []
    for key, entry in list(pending_store.items()):
        last_updated_at = entry.get("last_updated_at") or entry.get("created_at") or now
        if not force and (now - last_updated_at).total_seconds() < _notification_group_window_seconds(status):
            continue

        alerts = list((entry.get("alerts") or {}).values())
        if not alerts:
            to_delete.append(key)
            continue
        alerts.sort(key=lambda item: ((item.get("minute") or 0), item.get("rule_name") or ""))
        latest = max(alerts, key=lambda item: (item.get("minute") or 0, item.get("rule_name") or ""))
        latest_minute = latest.get("minute")
        latest_score = latest.get("score")
        rules_sorted = sorted({item.get("rule_name") for item in alerts if item.get("rule_name")})

        if len(rules_sorted) == 1:
            alert_info = alerts[0]
            temp_alert = SimpleNamespace(
                ml_pred_score=alert_info.get("ml_score"),
                ml_pred_verdict=alert_info.get("ml_verdict"),
                ml_model_samples=alert_info.get("ml_model_samples"),
            )
            ml_feedback = _ml_feedback_for_result(temp_alert, status)
            message = (
                f"{entry.get('msg_prefix')}\n"
                f"Regra: {rules_sorted[0]}\n"
                f"{entry.get('home_team')} vs {entry.get('away_team')}\n"
                f"Tempo: {latest_minute}'\n"
                f"Placar: {latest_score}\n"
                f"{ml_feedback}\n"
                f"Link: {entry.get('url')}"
            )
        else:
            target_text = entry.get("target_text") or entry.get("market_label") or "Mercado"
            stage_label = "HT" if entry.get("stage") == "HT" else entry.get("stage")
            rules_block = "\n".join(f"- {name}" for name in rules_sorted)
            status_title = "GREEN AGRUPADO" if status == "green" else "RED AGRUPADO"
            message = (
                f"{'✅' if status == 'green' else '❌'} {status_title} - {target_text} ({stage_label})\n"
                f"{entry.get('home_team')} vs {entry.get('away_team')}\n"
                f"Tempo: {latest_minute}'\n"
                f"Placar: {latest_score}\n"
                f"Regras que bateram ({len(rules_sorted)}):\n"
                f"{rules_block}\n"
                f"Link: {entry.get('url')}"
            )

        _send_message_safe(
            entry.get("token"),
            entry.get("chat_id"),
            message,
            context=f"green_group_{entry.get('home_team')}_{entry.get('away_team')}",
        )
        to_delete.append(key)

    for key in to_delete:
        pending_store.pop(key, None)


def _queue_entry_notification(alert, meta: dict, rendered_message: str):
    if not alert or not getattr(alert, "rule", None):
        return
    user = getattr(alert, "user", None)
    rule = getattr(alert, "rule", None)
    if not user or not rule or not rule.notify_telegram or not user.telegram_token or not user.telegram_chat_id:
        return

    now = now_sp()
    key = _notification_group_key(alert, "entry")
    market_ctx = infer_green_profile(rule)
    entry = ENTRY_NOTIFICATION_PENDING.get(key)
    if not entry:
        entry = {
            "created_at": now,
            "last_updated_at": now,
            "token": user.telegram_token,
            "chat_id": user.telegram_chat_id,
            "home_team": alert.home_team,
            "away_team": alert.away_team,
            "url": alert.url,
            "market_label": market_ctx.get("market_label") or "Mercado",
            "target_text": market_ctx.get("target_text") or "",
            "stage": (getattr(rule, "outcome_green_stage", None) or "HT").upper(),
            "meta": dict(meta or {}),
            "alerts": {},
        }
        ENTRY_NOTIFICATION_PENDING[key] = entry

    entry["last_updated_at"] = now
    entry["home_team"] = alert.home_team or entry.get("home_team")
    entry["away_team"] = alert.away_team or entry.get("away_team")
    entry["url"] = alert.url or entry.get("url")
    entry["meta"] = dict(meta or entry.get("meta") or {})
    entry["alerts"][alert.id] = {
        "rule_name": rule.name,
        "minute": alert.alert_minute,
        "score": alert.initial_score,
        "ml_score": alert.ml_pred_score,
        "ai_score": alert.ai_score,
        "rendered_message": rendered_message,
    }


def _flush_entry_notification_queue(force: bool = False):
    if not ENTRY_NOTIFICATION_PENDING:
        return
    now = now_sp()
    to_delete = []
    for key, entry in list(ENTRY_NOTIFICATION_PENDING.items()):
        last_updated_at = entry.get("last_updated_at") or entry.get("created_at") or now
        if not force and (now - last_updated_at).total_seconds() < ENTRY_GROUP_WINDOW_SECONDS:
            continue

        alerts = list((entry.get("alerts") or {}).values())
        if not alerts:
            to_delete.append(key)
            continue
        alerts.sort(key=lambda item: ((item.get("minute") or 0), item.get("rule_name") or ""))

        if len(alerts) == 1:
            message = alerts[0].get("rendered_message") or ""
            if message:
                _send_message_safe(
                    entry.get("token"),
                    entry.get("chat_id"),
                    message,
                    context=f"entry_single_{entry.get('home_team')}_{entry.get('away_team')}",
                )
            to_delete.append(key)
            continue

        latest = max(alerts, key=lambda item: (item.get("minute") or 0, item.get("rule_name") or ""))
        rules_sorted = sorted({item.get("rule_name") for item in alerts if item.get("rule_name")})
        meta = entry.get("meta") or {}
        ai_score = meta.get("ai_score", "N/A")
        ai_verdict = meta.get("ai_verdict", "N/A")
        ai_commentary = meta.get("ai_commentary", "N/A")
        ml_best = max((item.get("ml_score") for item in alerts if isinstance(item.get("ml_score"), int)), default=None)
        stage_label = "HT" if entry.get("stage") == "HT" else entry.get("stage")
        message = (
            f"📢 ALERTA AGRUPADO - {entry.get('target_text') or entry.get('market_label') or 'Mercado'} ({stage_label})\n"
            f"{entry.get('home_team')} vs {entry.get('away_team')}\n"
            f"Min: {latest.get('minute')} | Placar: {latest.get('score')}\n"
            f"Regras que bateram ({len(rules_sorted)}):\n"
            f"{chr(10).join(f'- {name}' for name in rules_sorted)}\n"
            f"IA: {ai_verdict} ({ai_score}/100)\n"
            f"Leitura IA: {ai_commentary}\n"
            f"Melhor ML do grupo: {ml_best if ml_best is not None else 'N/A'}/100\n"
            f"Link: {entry.get('url')}"
        )
        _send_message_safe(
            entry.get("token"),
            entry.get("chat_id"),
            message,
            context=f"entry_group_{entry.get('home_team')}_{entry.get('away_team')}",
        )
        to_delete.append(key)

    for key in to_delete:
        ENTRY_NOTIFICATION_PENDING.pop(key, None)


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


def league_rule_confidence_text(rule_id: int | None, user_id: int | None, league: str | None) -> str | None:
    if not rule_id or not user_id or not league:
        return None
    alerts = (
        MatchAlert.query.filter_by(rule_id=rule_id, user_id=user_id, league=league)
        .filter(MatchAlert.status.in_(("green", "red")))
        .order_by(MatchAlert.created_at.desc())
        .limit(LEAGUE_CONF_SAMPLE)
        .all()
    )
    total = len(alerts)
    if total < LEAGUE_CONF_MIN:
        return None
    greens = sum(1 for alert in alerts if alert.status == "green")
    pct = round((greens / total) * 100)
    return f"{pct}% ({greens}/{total})"


def build_message_meta(rule, stats_payload, game, history_meta=None, stats_override=None):
    stats = stats_override if isinstance(stats_override, dict) else stats_payload.get("stats", {})
    def sv(k, s, default=0):
        bucket = stats.get(k, {})
        if not isinstance(bucket, dict):
            return default
        value = bucket.get(s, default)
        if value in (None, "", "-"):
            return default
        return value
    meta = {
        "rule": rule.name if rule else "",
        "home_team": stats_payload.get("home_team") if stats_payload else "",
        "away_team": stats_payload.get("away_team") if stats_payload else "",
        "minute": stats_payload.get("minute") if stats_payload else None,
        "score": stats_payload.get("score") if stats_payload else "",
        "url": game.get("url") if game else "",
        "league": stats_payload.get("league") if stats_payload else "",
        "time_limit": rule.time_limit_min if rule else None,
        "goals_home": sv("Goals", "home"), "goals_away": sv("Goals", "away"), "goals_total": sv("Goals", "total"),
        "corners_home": sv("Corners", "home"), "corners_away": sv("Corners", "away"), "corners_total": sv("Corners", "total"),
        "on_target_home": sv("On Target", "home"), "on_target_away": sv("On Target", "away"), "on_target_total": sv("On Target", "total"),
        "off_target_home": sv("Off Target", "home"), "off_target_away": sv("Off Target", "away"), "off_target_total": sv("Off Target", "total"),
        "dangerous_attacks_home": sv("Dangerous Attacks", "home"), "dangerous_attacks_away": sv("Dangerous Attacks", "away"), "dangerous_attacks_total": sv("Dangerous Attacks", "total"),
        # Avoid blank placeholders in templates when history is unavailable.
        "history_h2h": "Sem historico disponivel",
        "history_home": "Sem historico do mandante",
        "history_away": "Sem historico do visitante",
        "history_confidence": "N/A",
    }
    if rule:
        market_ctx = infer_green_profile(rule)
        meta["green_market_key"] = market_ctx.get("market_key")
        meta["green_market_label"] = market_ctx.get("market_label")
        meta["green_market_target"] = market_ctx.get("target_text")
        meta["rule_confidence"] = rule_confidence_text(rule.id, rule.user_id)
        meta["league_rule_confidence"] = league_rule_confidence_text(
            rule.id,
            rule.user_id,
            meta.get("league"),
        )
        ml_prediction = predict_alert_ml(
            rule_id=rule.id,
            league=meta.get("league"),
            minute=meta.get("minute"),
            stats=stats,
            score_text=meta.get("score"),
            outcome_signature=market_ctx.get("outcome_signature"),
            market_keys=market_ctx.get("market_keys"),
            target_count=market_ctx.get("target_count"),
            target_sides=market_ctx.get("target_sides"),
            target_operators=market_ctx.get("target_operators"),
            market_key=market_ctx.get("market_key"),
            target_side=market_ctx.get("target_side"),
            target_operator=market_ctx.get("target_operator"),
            target_value=market_ctx.get("target_value"),
        )
        if ml_prediction:
            meta["ml_score"] = ml_prediction.get("score")
            meta["ml_verdict"] = ml_prediction.get("verdict")
            meta["ml_prob_green"] = ml_prediction.get("prob_green")
            meta["ml_samples"] = ml_prediction.get("samples")
            meta["ml_trained_at"] = ml_prediction.get("trained_at")
            meta["ml_feature_count"] = ml_prediction.get("feature_count")
    if history_meta:
        meta.update(history_meta)
    if "history_confidence" in meta and not meta["history_confidence"]:
        meta["history_confidence"] = "Sem historico de um contra o outro"
    return meta


def render_fast_entry_message(rule, stats_payload: dict, game: dict, stats_for_rule: dict) -> str:
    stats = stats_for_rule if isinstance(stats_for_rule, dict) else (stats_payload or {}).get("stats", {})

    def total(key: str):
        bucket = stats.get(key, {})
        if not isinstance(bucket, dict):
            return 0
        value = bucket.get("total", 0)
        return value if value not in (None, "", "-") else 0

    market_ctx = infer_green_profile(rule)
    return (
        f"ALERTA AO VIVO\n"
        f"Regra: {getattr(rule, 'name', '')}\n"
        f"Liga: {(stats_payload or {}).get('league') or (game or {}).get('league') or ''}\n"
        f"Jogo: {(stats_payload or {}).get('home_team') or (game or {}).get('home_team')} vs "
        f"{(stats_payload or {}).get('away_team') or (game or {}).get('away_team')}\n"
        f"Min: {(stats_payload or {}).get('minute')} | Placar: {(stats_payload or {}).get('score')}\n"
        f"Mercado: {market_ctx.get('target_text') or market_ctx.get('market_label') or 'N/A'}\n"
        f"On Target: {total('On Target')} | Dangerous: {total('Dangerous Attacks')} | Corners: {total('Corners')}\n"
        f"Status: analisando historico e IA...\n"
        f"Link: {(game or {}).get('url') or (stats_payload or {}).get('url') or ''}"
    )


def _queue_entry_enrichment(alert_id: int | None):
    if not alert_id:
        return
    with ENTRY_ENRICHMENT_LOCK:
        if alert_id in ENTRY_ENRICHMENT_QUEUED:
            return
        ENTRY_ENRICHMENT_QUEUED.add(alert_id)
        ENTRY_ENRICHMENT_QUEUE.append(alert_id)


def _pop_entry_enrichment_id():
    with ENTRY_ENRICHMENT_LOCK:
        if not ENTRY_ENRICHMENT_QUEUE:
            return None
        return ENTRY_ENRICHMENT_QUEUE.popleft()


def _finish_entry_enrichment_id(alert_id: int | None):
    if not alert_id:
        return
    with ENTRY_ENRICHMENT_LOCK:
        ENTRY_ENRICHMENT_QUEUED.discard(alert_id)


def _build_history_meta(session, rule, url: str):
    history_meta = {}
    history_data = fetch_match_history(session, url)
    h2h_items = (history_data or {}).get("h2h", [])
    home_items = (history_data or {}).get("home", [])
    away_items = (history_data or {}).get("away", [])
    h2h_summary = summarize_history(h2h_items)
    home_summary = summarize_history(home_items)
    away_summary = summarize_history(away_items)
    conf_pct = history_confidence((rule.conditions or []) if rule else [], h2h_items)
    if h2h_summary:
        history_meta["history_h2h"] = format_history_summary("H2H:", h2h_summary)
    if home_summary:
        history_meta["history_home"] = format_history_summary("Mandante:", home_summary)
    if away_summary:
        history_meta["history_away"] = format_history_summary("Visitante:", away_summary)
    if conf_pct is not None:
        history_meta["history_confidence"] = f"{conf_pct}%"
    return history_meta


def enrich_entry_alert(session, alert_id: int):
    alert = MatchAlert.query.get(alert_id)
    if not alert or alert.telegram_entry_enriched or not alert.telegram_entry_message_id:
        return
    rule = alert.rule
    user = alert.user
    if not rule or not user or not user.telegram_token or not user.telegram_chat_id:
        return

    try:
        stats_override = json.loads(alert.initial_stats_json or "{}")
    except Exception:
        stats_override = {}
    stats_payload = {
        "league": alert.league,
        "home_team": alert.home_team,
        "away_team": alert.away_team,
        "minute": alert.alert_minute,
        "score": alert.initial_score,
        "stats": stats_override,
        "events": [],
    }
    game = {"url": alert.url, "game_id": alert.game_id}

    try:
        history_meta = _build_history_meta(session, rule, alert.url)
    except Exception as exc:
        history_meta = {}
        print(f"[worker] falha ao enriquecer historico do alerta {alert.id}: {exc}")

    meta = build_message_meta(rule, stats_payload, game, history_meta, stats_override=stats_override)
    ai_live_assessment = _build_ai_assessment(rule, meta)
    alert.ai_score = int(ai_live_assessment.get("score") or alert.ai_score or 0)
    alert.ai_verdict = ai_live_assessment.get("verdict") or alert.ai_verdict
    alert.ai_commentary = _build_ai_commentary(
        rule,
        {
            **meta,
            "ai_score": alert.ai_score,
            "ai_verdict": alert.ai_verdict,
        },
    )
    meta["ai_score"] = alert.ai_score
    meta["ai_verdict"] = alert.ai_verdict
    meta["ai_commentary"] = alert.ai_commentary
    message = render_message(rule, meta)
    if _edit_message_safe(
        user.telegram_token,
        user.telegram_chat_id,
        alert.telegram_entry_message_id,
        message,
        context=f"entry_enrich_{alert.id}",
    ):
        alert.telegram_entry_enriched = True
        try:
            db.session.commit()
        except Exception:
            db.session.rollback()

def is_youth_match(stats_payload: dict) -> bool:
    if not stats_payload: return False
    hay = f"{stats_payload.get('league', '')} {stats_payload.get('home_team', '')} {stats_payload.get('away_team', '')}".lower()
    return any(token in hay for token in YOUTH_TOKENS)


def _valid_team_name(value: str | None) -> bool:
    text = str(value or "").strip()
    return bool(text and not text.isdigit() and text.lower() not in ("none", "null", "x"))


def _apply_live_row_identity(game: dict, stats_payload: dict | None) -> dict | None:
    if not game or not stats_payload:
        return stats_payload
    home_team = game.get("home_team")
    away_team = game.get("away_team")
    # The live row is tied to the game_id being processed. Prefer it over
    # names parsed from stats tables, which can be stale or ambiguous.
    if _valid_team_name(home_team) and _valid_team_name(away_team):
        stats_payload["home_team"] = home_team
        stats_payload["away_team"] = away_team
    if not stats_payload.get("league") and game.get("league"):
        stats_payload["league"] = game.get("league")
    return stats_payload


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


def _ht_confirmed_with_state(state: LiveGameState | None, time_text: str, minute: int | None) -> bool:
    if state is None:
        return False
    if minute is None:
        return False
    # Modificação solicitada: Se o jogo ficou nos 45 por 3 min, salva o HT
    if minute != 45 and not is_half_time_text(time_text):
        state.ht_seen_at = None
        return False
    seen_at = state.ht_seen_at
    if not seen_at:
        state.ht_seen_at = now_sp()
        return False
    if (now_sp() - seen_at).total_seconds() >= HALFTIME_CONFIRM_SECONDS:
        HALFTIME_CONFIRMED_AT[state.game_id] = now_sp()
        state.ht_seen_at = None
        return True
    return False


def _ht_confirmed(game_id: str, time_text: str, minute: int | None) -> bool:
    state = LiveGameState.query.filter_by(game_id=game_id).first()
    return _ht_confirmed_with_state(state, time_text, minute)

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
    if game_id in SECOND_HALF_FROM_NOW:
        return
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
        if _ht_confirmed(game_id, time_text, minute):
            return
        return

    # From 46+ onward:
    # Modificação solicitada: Se o bot está ligado e o jogo já está no 2º tempo, ele cria baseline imediatamente na hora e começa a contar a partir dali.
    if is_second_half(time_text, minute) or minute >= 46:
        if _has_first_half_context(game_id):
            SECOND_HALF_BASELINES[game_id] = copy_stats(_baseline_source_for_second_half(game_id, stats_payload))
        else:
            # Se não tem contexto do 1º tempo, cria baseline imediatamente com os stats atuais
            SECOND_HALF_BASELINES[game_id] = copy_stats(stats_payload.get("stats", {}))
            SECOND_HALF_FROM_NOW[game_id] = True
        
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
        minute_total = (baseline.get("Minute") or {}).get("total") if isinstance(baseline.get("Minute"), dict) else None
        if isinstance(minute_total, int) and minute_total > 45 and state.first_half_snapshot_json:
            try:
                first_half = json.loads(state.first_half_snapshot_json)
                if isinstance(first_half, dict) and first_half:
                    return first_half
            except Exception:
                pass
        return baseline
    return None


def get_second_half_baseline(game_id: str):
    if FORCE_SECOND_HALF_FROM_FIRST_HALF:
        persisted_first_half = _load_persisted_first_half_snapshot(game_id)
        if persisted_first_half and isinstance(persisted_first_half.get("stats"), dict):
            SECOND_HALF_BASELINES[game_id] = copy_stats(persisted_first_half["stats"])
            return SECOND_HALF_BASELINES[game_id]
    if game_id in SECOND_HALF_FROM_NOW:
        baseline = SECOND_HALF_BASELINES.get(game_id)
        if baseline:
            return baseline
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
    state.events_json = _events_to_json(stats_payload.get("events"))
    minute = stats_payload.get("minute") or 0
    time_text = stats_payload.get("time_text", "")
    
    # Se o jogo ficou nos 45 por 3 min, salva o HT
    if minute == 45 and not is_first_half_extra_time(time_text):
        if game_id not in SECOND_HALF_FROM_NOW and _ht_confirmed_with_state(state, time_text, minute):
            first_half_stats = copy_stats(stats_payload.get("stats", {}))
            if first_half_stats:
                state.first_half_snapshot_json = json.dumps(first_half_stats, ensure_ascii=False)
                state.first_half_snapshot_minute = minute

    # If a previous run saved a late baseline but we have first-half snapshot,
    # repair baseline to the first-half reference so 2nd-half deltas are correct.
    if state.second_half_baseline_json and state.first_half_snapshot_json and game_id not in SECOND_HALF_FROM_NOW:
        try:
            persisted_base = json.loads(state.second_half_baseline_json)
        except Exception:
            persisted_base = None
        base_minute = (
            (persisted_base.get("Minute") or {}).get("total")
            if isinstance(persisted_base, dict) and isinstance(persisted_base.get("Minute"), dict)
            else None
        )
        if isinstance(base_minute, int) and base_minute > 45:
            state.second_half_baseline_json = state.first_half_snapshot_json
            try:
                fixed_base = json.loads(state.first_half_snapshot_json)
            except Exception:
                fixed_base = None
            if isinstance(fixed_base, dict) and fixed_base:
                SECOND_HALF_BASELINES[game_id] = copy_stats(fixed_base)

    baseline = SECOND_HALF_BASELINES.get(game_id)
    if baseline:
        # If provider corrects stats downward, adjust baseline to avoid negative/locked deltas.
        for key, value in stats_payload.get("stats", {}).items():
            if key in NON_DELTA_KEYS or not isinstance(value, dict):
                continue
            base_val = baseline.get(key)
            if not isinstance(base_val, dict):
                continue
            for side in ("home", "away", "total"):
                curr = _num(value.get(side, 0))
                base = _num(base_val.get(side, 0))
                if curr < base:
                    base_val[side] = curr
        SECOND_HALF_BASELINES[game_id] = baseline
        # If baseline equals current for a while after 50', start counting from "now".
        if minute >= 50:
            zero_delta = True
            for key in ("On Target", "Corners", "Dangerous Attacks"):
                cur_val = stats_payload.get("stats", {}).get(key)
                base_val = baseline.get(key)
                if not isinstance(cur_val, dict) or not isinstance(base_val, dict):
                    continue
                for side in ("home", "away"):
                    if _num(cur_val.get(side, 0)) != _num(base_val.get(side, 0)):
                        zero_delta = False
                        break
                if not zero_delta:
                    break
            started_at = state.second_half_started_at
            if zero_delta and (started_at is None or (now_sp() - started_at).total_seconds() > 120):
                SECOND_HALF_BASELINES[game_id] = copy_stats(stats_payload.get("stats", {}))
                baseline = SECOND_HALF_BASELINES[game_id]
                state.second_half_started_at = now_sp()
                SECOND_HALF_FROM_NOW[game_id] = True
                # Drop HT snapshot so 2H deltas reflect from now on.
                state.first_half_snapshot_json = None
                state.first_half_snapshot_minute = None
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
    def _delta(curr, base):
        curr_n = _num(curr)
        base_n = _num(base)
        # Some providers reset 2H stats to zero; when that happens use current as 2H total.
        return curr_n - base_n if curr_n >= base_n else curr_n

    adjusted = {}
    for key, value in stats.items():
        if not isinstance(value, dict): continue
        if key in NON_DELTA_KEYS or key not in baseline:
            adjusted[key] = value.copy()
            continue
        base = baseline[key]
        adjusted[key] = {
            "home": max(0, _delta(value.get("home", 0), base.get("home", 0))),
            "away": max(0, _delta(value.get("away", 0), base.get("away", 0))),
            "total": max(0, _delta(value.get("total", 0), base.get("total", 0))),
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

def should_time_red(rule, alert, minute: int | None, last_update_at=None, allow_wall_clock: bool = False) -> bool:
    if not rule or not rule.outcome_red_if_no_green or rule.outcome_red_minute is None:
        return False
    eff_minute = effective_minute(rule, minute)
    if eff_minute is not None and eff_minute >= rule.outcome_red_minute:
        return True
    if eff_minute is not None and eff_minute <= 1:
        return False
    # Wall-clock fallback is only for missing/stale live payloads. When we have
    # a fresh BetsAPI minute, trust that minute instead of double-counting time.
    if not allow_wall_clock or not alert:
        return False

    reference_minute = alert.last_score_minute if last_update_at else alert.alert_minute
    if reference_minute is None:
        return False
    reference_time = last_update_at or alert.created_at
    if not reference_time:
        return False
    elapsed_minutes = max(0.0, (now_sp() - reference_time).total_seconds() / 60.0)
    estimated_minute = int(reference_minute + elapsed_minutes)
    estimated_eff = effective_minute(rule, estimated_minute)
    if estimated_eff is None:
        return False
    if estimated_eff <= 1:
        return False
    if estimated_eff >= rule.outcome_red_minute:
        return True
    return False

def _stage_window_open_for_annulment(rule, time_text: str, minute: int | None) -> bool:
    if minute is None:
        return False
    if not rule:
        return True
    if rule.second_half_only:
        return True
    green_stage = (rule.outcome_green_stage or "HT").upper()
    red_stage = (rule.outcome_red_stage or "HT").upper()
    if green_stage != "HT" and red_stage != "HT":
        return True
    return is_first_half_goal(time_text, minute) or is_half_time(time_text, minute)

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


def merge_score_delta_into_stats(stats_for_outcome: dict, initial_score: str | None, current_score: str | None):
    if not isinstance(stats_for_outcome, dict):
        return stats_for_outcome
    if not initial_score or not current_score:
        return stats_for_outcome
    init_home, init_away = parse_score(initial_score)
    curr_home, curr_away = parse_score(current_score)
    score_delta = {
        "home": max(0, curr_home - init_home),
        "away": max(0, curr_away - init_away),
        "total": max(0, (curr_home + curr_away) - (init_home + init_away)),
    }
    stats_for_outcome["Goals"] = score_delta
    return stats_for_outcome

def maybe_notify_penalty(alert, stats, minute, score, time_text=None):
    if not alert:
        return
    rule = alert.rule
    user = alert.user
    if not rule or not rule.alert_on_penalty or not user:
        return
    if not alert.penalty_baseline_set:
        return
    if minute is None or minute <= 0:
        return
    if is_full_time(time_text or "", minute):
        return
    
    penalties_total = stats.get("Penalties", {}).get("total", 0)
    if not isinstance(penalties_total, int):
        return
        
    if penalties_total > alert.penalty_last_total:
        alert.penalty_last_total = penalties_total
        alert.penalty_notified = True
        db.session.commit()
        
        if rule.notify_telegram and user.telegram_token and user.telegram_chat_id:
            msg = (
                f"🚨 PÊNALTI DETECTADO!\n"
                f"Jogo: {alert.home_team} vs {alert.away_team}\n"
                f"Tempo: {minute}'\n"
                f"Placar: {score}\n"
                f"Regra: {rule.name}\n"
                f"Link: {alert.url}"
            )
            _send_message_safe(user.telegram_token, user.telegram_chat_id, msg, context="penalty")

def maybe_notify_penalty_for_game(game_id, stats_payload):
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
        if not _is_recent_game_update(game_id):
            continue
        if home_team:
            alert.home_team = home_team
        if away_team:
            alert.away_team = away_team
        maybe_notify_penalty(alert, stats, minute, score, time_text=time_text)

def start_worker(app):
    # Limite de memória para threads do worker
    mem_limit = 1024 * 1024 * 1024
    _, hard_limit = resource.getrlimit(resource.RLIMIT_AS)
    soft_limit = min(mem_limit, hard_limit) if hard_limit != resource.RLIM_INFINITY else mem_limit
    resource.setrlimit(resource.RLIMIT_AS, (soft_limit, hard_limit))
    threading.Thread(target=run_worker, args=(app,), daemon=True).start()
    threading.Thread(target=run_alerts_worker, args=(app,), daemon=True).start()
    threading.Thread(target=run_entry_enrichment_worker, args=(app,), daemon=True).start()

def run_worker(app):
    with app.app_context():
        global BOT_STARTED_AT
        global LAST_FINALIZE_RUN_AT
        BOT_STARTED_AT = now_sp()
        if ML_AUTOTRAIN_ENABLED:
            maybe_retrain_model(force=False)
        session = make_session()
        while True:
            try:
                if ML_AUTOTRAIN_ENABLED:
                    maybe_retrain_model(force=False)
                process_live_games(session)
                _flush_entry_notification_queue(force=False)
                now = now_sp()
                if (
                    LAST_FINALIZE_RUN_AT is None
                    or (now - LAST_FINALIZE_RUN_AT).total_seconds() >= FINALIZE_POLL_INTERVAL
                ):
                    finalize_full_time(session)
                    LAST_FINALIZE_RUN_AT = now
            except Exception as exc:
                db.session.rollback()
                print(f"[worker] erro: {exc}")
            API_STATUS["last_cycle"] = now_sp().strftime("%Y-%m-%d %H:%M:%S")
            time.sleep(POLL_INTERVAL)

def run_alerts_worker(app):
    with app.app_context():
        session = make_session()
        while True:
            try:
                follow_alerts(session)
                _flush_grouped_notification_queue("green", force=False)
                _flush_grouped_notification_queue("red", force=False)
            except Exception as exc:
                db.session.rollback()
                print(f"[alerts] erro: {exc}")
            time.sleep(ALERTS_POLL_INTERVAL)


def run_entry_enrichment_worker(app):
    with app.app_context():
        session = make_session()
        while True:
            alert_id = _pop_entry_enrichment_id()
            if not alert_id:
                time.sleep(1)
                continue
            try:
                enrich_entry_alert(session, alert_id)
            except Exception as exc:
                db.session.rollback()
                print(f"[entry_enrich] erro alert_id={alert_id}: {exc}")
            finally:
                _finish_entry_enrichment_id(alert_id)

def _game_key(game: dict) -> str:
    return str(game.get("game_id") or game.get("url") or "")


def _normalize_text(value: str) -> str:
    text = str(value or "").strip().casefold()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.split())


def _is_women_league(league_name: str) -> bool:
    text = _normalize_text(league_name)
    if not text:
        return False
    return bool(
        re.search(
            r"\b(women|woman|feminino|feminina|femenil|femenina|feminil)\b",
            text,
            flags=re.IGNORECASE,
        )
    )


def _league_allowed(league_name: str, allowed_items: list[str]) -> bool:
    league_norm = _normalize_text(league_name)
    if not league_norm:
        return False
    allowed_norm = [_normalize_text(item) for item in allowed_items if str(item).strip()]
    if not allowed_norm:
        return True
    if "others" in allowed_norm and not _is_women_league(league_name):
        # "Others" is treated as dynamic wildcard for non-women competitions.
        # This keeps new leagues enabled without needing to manually re-mark them.
        return True

    def _variants(text: str) -> set[str]:
        variants = {text}
        # Many feeds prefix country before " - ".
        # Match both full name and the competition-only tail.
        if " - " in text:
            parts = [p.strip() for p in text.split(" - ") if p.strip()]
            if len(parts) >= 2:
                variants.add(" - ".join(parts[1:]))
                variants.update(parts[1:])
        return {v for v in variants if v}

    league_variants = _variants(league_norm)
    allowed_variants = set()
    for item in allowed_norm:
        allowed_variants.update(_variants(item))

    for lv in league_variants:
        for av in allowed_variants:
            if lv == av:
                return True
            if lv.startswith(av) or av.startswith(lv):
                return True
            if lv in av or av in lv:
                return True
    return False


def _rule_trigger_distance(rule, stats: dict, minute: int | None) -> float:
    conditions = list(rule.conditions or [])
    if not conditions:
        return 9999.0

    grouped = {}
    for cond in conditions:
        gid = getattr(cond, "group_id", 0)
        grouped.setdefault(gid if gid is not None else 0, []).append(cond)

    best = 9999.0
    for conds in grouped.values():
        ot_needed = None
        min_floor = None
        min_cap = None
        impossible = False
        for cond in conds:
            key = normalize_stat_key(getattr(cond, "stat_key", ""))
            side = getattr(cond, "side", "total")
            op = getattr(cond, "operator", "")
            val = getattr(cond, "value", None)
            if not isinstance(val, (int, float)):
                continue

            if key == "On Target" and side == "total":
                if op in (">=", ">"):
                    target = int(val) if op == ">=" else int(val) + 1
                    if ot_needed is None or target > ot_needed:
                        ot_needed = target
                elif op in ("<=", "<", "=="):
                    # These constraints are not "near trigger" friendly for ranking.
                    continue

            if key == "Minute" and side == "total":
                if op in (">=", ">"):
                    floor = int(val) if op == ">=" else int(val) + 1
                    if min_floor is None or floor > min_floor:
                        min_floor = floor
                elif op in ("<=", "<"):
                    cap = int(val) if op == "<=" else int(val) - 1
                    if min_cap is None or cap < min_cap:
                        min_cap = cap
                elif op == "==":
                    floor = int(val)
                    cap = int(val)
                    min_floor = floor if min_floor is None else max(min_floor, floor)
                    min_cap = cap if min_cap is None else min(min_cap, cap)

        if ot_needed is None:
            continue

        curr_ot = 0
        if isinstance(stats, dict):
            curr_ot = int((stats.get("On Target", {}) or {}).get("total") or 0)
        curr_min = minute if isinstance(minute, int) else 0

        if min_cap is not None and curr_min > min_cap:
            impossible = True

        d_ot = max(0, ot_needed - curr_ot)
        d_floor = max(0, (min_floor or 0) - curr_min)
        d_cap = 0 if min_cap is None else max(0, curr_min - min_cap)

        # Lower score = closer to trigger. Penalize expired windows heavily.
        score = (d_ot * 6) + d_floor + (d_cap * 20)
        if impossible:
            score += 10000
        if score < best:
            best = float(score)

    return best


def _prioritize_games(games: list[dict], prefetched: dict[str, dict], active_rules: list) -> list[dict]:
    if not games or not prefetched or not active_rules:
        return games

    def rank(game: dict):
        key = _game_key(game)
        item = prefetched.get(key) if key else None
        if not item:
            return (99999.0, 99999)
        payload = item.get("stats_payload") or {}
        stats = payload.get("stats", {}) or {}
        minute = item.get("minute")

        best = 99999.0
        for rule in active_rules:
            dist = _rule_trigger_distance(rule, stats, minute)
            if dist < best:
                best = dist

        # Tie-breaker: later minutes first (usually closer to decision windows).
        minute_rank = -(minute if isinstance(minute, int) else 0)
        return (best, minute_rank)

    return sorted(games, key=rank)


def _rule_minute_window(rule) -> tuple[int | None, int | None]:
    min_minute = None
    max_minute = None
    for cond in (rule.conditions or []):
        if normalize_stat_key(getattr(cond, "stat_key", "")) != "Minute":
            continue
        value = getattr(cond, "value", None)
        try:
            value = int(value)
        except Exception:
            continue
        op = getattr(cond, "operator", "")
        if op in (">=", ">"):
            lower = value if op == ">=" else value + 1
            min_minute = lower if min_minute is None else max(min_minute, lower)
        elif op in ("<=", "<"):
            upper = value if op == "<=" else value - 1
            max_minute = upper if max_minute is None else min(max_minute, upper)
        elif op == "==":
            min_minute = value
            max_minute = value
    return min_minute, max_minute


def _game_maybe_relevant_for_rule(game: dict, rule) -> bool:
    minute = game.get("minute")
    if not isinstance(minute, int):
        return True
    if getattr(rule, "second_half_only", False) and minute < 46:
        return False
    min_minute, max_minute = _rule_minute_window(rule)
    if min_minute is not None and minute < max(0, min_minute - 2):
        return False
    if max_minute is not None and minute > (max_minute + 2):
        return False
    return True


def _filter_candidate_games(games: list[dict], active_rules: list) -> list[dict]:
    if not games or not active_rules:
        return games
    filtered = []
    for game in games:
        if any(_game_maybe_relevant_for_rule(game, rule) for rule in active_rules):
            filtered.append(game)
    return filtered or games


def _prefetch_game_stats(game: dict):
    key = _game_key(game)
    if not key:
        return key, None, None
    session = make_session()
    try:
        stats_payload = fetch_match_stats_fresh(
            session,
            game["url"],
            attempts=FETCH_STATS_ATTEMPTS,
            delay=FETCH_STATS_DELAY,
        )
        if not stats_payload:
            stats_payload = fetch_match_stats(session, game["url"])
        if not stats_payload or is_youth_match(stats_payload):
            return key, None, None
        minute = stats_payload.get("minute")
        if minute is None:
            return key, None, None
        return key, stats_payload, minute
    except Exception as exc:
        print(f"[worker] erro no prefetch game={game.get('game_id')}: {exc}")
        return key, None, None
    finally:
        try:
            session.close()
        except Exception:
            pass


def _prefetch_live_stats(games: list[dict]) -> dict[str, dict]:
    if not games:
        return {}
    max_workers = 1 if USE_SERIAL_PREFETCH else min(len(games), ANALYSIS_THREADS)
    if max_workers <= 1:
        prefetched = {}
        for game in games:
            key, stats_payload, minute = _prefetch_game_stats(game)
            if key and stats_payload and minute is not None:
                prefetched[key] = {"stats_payload": stats_payload, "minute": minute}
        return prefetched

    prefetched = {}
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="analysis") as executor:
        futures = [executor.submit(_prefetch_game_stats, game) for game in games]
        for future in as_completed(futures):
            key, stats_payload, minute = future.result()
            if key and stats_payload and minute is not None:
                prefetched[key] = {"stats_payload": stats_payload, "minute": minute}
    return prefetched


def process_live_games(session):
    games, status_code = fetch_live_games(session)
    update_api_status(status_code == 200, status_code)
    if not games: return

    active_rules = Rule.query.filter_by(is_active=True).all()
    games = _filter_candidate_games(games, active_rules)
    rule_ids = [rule.id for rule in active_rules]
    game_ids = [str(game.get("game_id")) for game in games if game.get("game_id") is not None]
    existing_pairs = set()
    if rule_ids and game_ids:
        rows = (
            db.session.query(MatchAlert.game_id, MatchAlert.rule_id)
            .filter(MatchAlert.game_id.in_(game_ids), MatchAlert.rule_id.in_(rule_ids))
            .all()
        )
        existing_pairs = {(str(gid), rid) for gid, rid in rows}

    # Parse allowed leagues once per cycle instead of per game/rule.
    rule_allowed_items = {}
    for rule in active_rules:
        allowed_json = getattr(rule, "allowed_leagues_json", None)
        if not allowed_json:
            rule_allowed_items[rule.id] = None
            continue
        try:
            items = json.loads(allowed_json)
        except Exception:
            items = []
        rule_allowed_items[rule.id] = items if isinstance(items, list) and items else None

    ia_profiles = _get_ai_shadow_profiles() if IA_SHADOW_ENABLED else {}
    prefetched = _prefetch_live_stats(games)
    games = _prioritize_games(games, prefetched, active_rules)
    for game in games:
        game_key = _game_key(game)
        prefetched_item = prefetched.get(game_key) if game_key else None
        if not prefetched_item:
            continue
        stats_payload = _apply_live_row_identity(game, prefetched_item["stats_payload"])
        minute = prefetched_item["minute"]
        _maybe_emit_ia_shadow_signal(game, stats_payload, ia_profiles)

        game_id = game.get("game_id")
        if game_id and game_id not in GAME_FIRST_SEEN_AT:
            GAME_FIRST_SEEN_AT[game_id] = now_sp()
            # If bot starts mid-2H, count from now for this game.
            if isinstance(minute, int) and minute >= 46:
                SECOND_HALF_FROM_NOW[game_id] = True
                SECOND_HALF_BASELINES[game_id] = copy_stats(stats_payload.get("stats", {}))

        # If minute advanced but score/stats are stuck, force a cache-busted re-fetch.
        state = LiveGameState.query.filter_by(game_id=game.get("game_id")).first()
        if state:
            prev_minute = state.minute
            if isinstance(prev_minute, int) and isinstance(minute, int) and minute >= prev_minute + 2:
                same_score = state.score and stats_payload.get("score") == state.score
                same_on_target = False
                if state.stats_json:
                    try:
                        prev_stats = json.loads(state.stats_json)
                    except Exception:
                        prev_stats = {}
                    curr_stats = stats_payload.get("stats", {})
                    prev_ot = (prev_stats.get("On Target") or {}).get("total")
                    curr_ot = (curr_stats.get("On Target") or {}).get("total")
                    if isinstance(prev_ot, int) and isinstance(curr_ot, int) and prev_ot == curr_ot:
                        same_on_target = True
                if same_score and same_on_target:
                    forced_url = _cache_bust_url(game["url"])
                    forced_payload = fetch_match_stats(session, forced_url)
                    if forced_payload and forced_payload.get("score") and forced_payload.get("score") != stats_payload.get("score"):
                        stats_payload = _apply_live_row_identity(game, forced_payload)
                        minute = stats_payload.get("minute") or minute

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
            pair_key = (str(game["game_id"]), rule.id)
            if pair_key in existing_pairs:
                continue
            # Optional league filter: if configured, only emit alerts for matching leagues.
            allowed_items = rule_allowed_items.get(rule.id)
            if allowed_items:
                league_name = str(stats_payload.get("league") or "")
                if not _league_allowed(league_name, allowed_items):
                    continue

            stats_for_rule = stats_payload["stats"]
            h_score, a_score = parse_score(stats_payload.get("score", ""))
            if (rule.score_home is not None and h_score != rule.score_home) or \
               (rule.score_away is not None and a_score != rule.score_away):
                continue

            if rule.second_half_only:
                if not second_half_allowed(game["game_id"], stats_payload.get("time_text", ""), minute):
                    continue
                
                baseline = get_second_half_baseline(game["game_id"])
                if not baseline: continue
                stats_for_rule = apply_second_half_delta(stats_payload["stats"], baseline)
                m2h = max(0, minute - 45)
                stats_for_rule["Minute"] = {"home": m2h, "away": m2h, "total": m2h}
                # 2H rules must be evaluated strictly on second-half delta.

            if evaluate_rule(rule, stats_for_rule):
                # If rule depends on mutable stats, revalidar para evitar feed atrasado.
                has_stat_cond = any(
                    normalize_stat_key(getattr(cond, "stat_key", "")).lower() not in ("minute", "")
                    for cond in (rule.conditions or [])
                )
                if has_stat_cond or rule.score_home is not None or rule.score_away is not None:
                    latest_payload = fetch_match_stats(session, game["url"])
                    if latest_payload:
                        latest_payload = _apply_live_row_identity(game, latest_payload)
                        latest_stats_for_rule = latest_payload.get("stats", {})
                        latest_minute = latest_payload.get("minute")
                        latest_score = latest_payload.get("score", "")
                        lh_score, la_score = parse_score(latest_score)
                        latest_score_matches = not (
                            (rule.score_home is not None and lh_score != rule.score_home) or
                            (rule.score_away is not None and la_score != rule.score_away)
                        )
                        if rule.second_half_only:
                            if not second_half_allowed(game["game_id"], latest_payload.get("time_text", ""), latest_minute):
                                latest_stats_for_rule = None
                            baseline = get_second_half_baseline(game["game_id"])
                            if not baseline:
                                latest_stats_for_rule = None
                            if latest_stats_for_rule is not None:
                                latest_stats_for_rule = apply_second_half_delta(latest_payload.get("stats", {}), baseline)
                                m2h_latest = max(0, (latest_minute or 0) - 45)
                                latest_stats_for_rule["Minute"] = {"home": m2h_latest, "away": m2h_latest, "total": m2h_latest}

                        latest_ok = bool(latest_score_matches and latest_stats_for_rule is not None and evaluate_rule(rule, latest_stats_for_rule))
                        if latest_ok:
                            stats_for_rule = latest_stats_for_rule
                            stats_payload = latest_payload
                        else:
                            # Retry once with cache-busted URL. If still false, keep initial match
                            # to avoid missing valid entries due feed jitter between reads.
                            forced_payload = fetch_match_stats(session, _cache_bust_url(game["url"]))
                            forced_ok = False
                            if forced_payload:
                                forced_payload = _apply_live_row_identity(game, forced_payload)
                                forced_stats_for_rule = forced_payload.get("stats", {})
                                forced_minute = forced_payload.get("minute")
                                fs_home, fs_away = parse_score(forced_payload.get("score", ""))
                                forced_score_matches = not (
                                    (rule.score_home is not None and fs_home != rule.score_home) or
                                    (rule.score_away is not None and fs_away != rule.score_away)
                                )
                                if rule.second_half_only:
                                    if second_half_allowed(game["game_id"], forced_payload.get("time_text", ""), forced_minute):
                                        baseline = get_second_half_baseline(game["game_id"])
                                        if baseline:
                                            forced_stats_for_rule = apply_second_half_delta(forced_payload.get("stats", {}), baseline)
                                            m2h_forced = max(0, (forced_minute or 0) - 45)
                                            forced_stats_for_rule["Minute"] = {"home": m2h_forced, "away": m2h_forced, "total": m2h_forced}
                                        else:
                                            forced_stats_for_rule = None
                                    else:
                                        forced_stats_for_rule = None
                                forced_ok = bool(
                                    forced_score_matches and
                                    forced_stats_for_rule is not None and
                                    evaluate_rule(rule, forced_stats_for_rule)
                                )
                                if forced_ok:
                                    stats_for_rule = forced_stats_for_rule
                                    stats_payload = forced_payload
                            if not forced_ok:
                                print(f"[worker] regra {rule.id} manteve match inicial; revalidacao divergente game={game.get('game_id')} url={game.get('url')}")
                if not user:
                    continue
                if rule.notify_telegram and (not user.telegram_token or not user.telegram_chat_id):
                    print(f"[worker] regra {rule.id} sem token/chat_id para envio telegram (user={getattr(user, 'id', None)})")
                    continue

                effective_minute = stats_payload.get("minute") if stats_payload else minute
                if effective_minute is None:
                    effective_minute = minute
                phase = "2H" if rule.second_half_only or int(effective_minute or 0) > 47 else "1H"
                shadow_gate = _resolve_ia_shadow_gate(
                    rule.id,
                    phase,
                    int(effective_minute or 0),
                    stats_payload.get("score") if stats_payload else "",
                    stats_for_rule,
                    ia_profiles,
                )
                # Shadow mode should observe the live flow by default, not suppress it.
                if IA_SHADOW_ENFORCE_MAIN_RULES and not shadow_gate["accepted"]:
                    continue
                market_ctx = infer_green_profile(rule)
                ai_assessment = _ia_shadow_assessment_for_phase(
                    phase,
                    int(effective_minute or 0),
                    stats_payload.get("score") if stats_payload else "",
                    stats_for_rule,
                )
                event_timeline = (stats_payload or {}).get("events") or []
                ml_prediction = predict_alert_ml(
                    rule_id=rule.id,
                    league=stats_payload.get("league"),
                    minute=effective_minute,
                    stats=stats_for_rule,
                    score_text=stats_payload.get("score"),
                    outcome_signature=market_ctx.get("outcome_signature"),
                    market_keys=market_ctx.get("market_keys"),
                    target_count=market_ctx.get("target_count"),
                    target_sides=market_ctx.get("target_sides"),
                    target_operators=market_ctx.get("target_operators"),
                    market_key=market_ctx.get("market_key"),
                    target_side=market_ctx.get("target_side"),
                    target_operator=market_ctx.get("target_operator"),
                    target_value=market_ctx.get("target_value"),
                )
                alert = MatchAlert(
                    rule_id=rule.id, user_id=user.id, game_id=game["game_id"], url=game["url"],
                    status="pending", alert_minute=effective_minute, initial_score=stats_payload["score"],
                    last_score=stats_payload["score"], last_score_minute=effective_minute,
                    initial_stats_json=stats_to_json(stats_for_rule),
                    league=stats_payload.get("league"), home_team=stats_payload.get("home_team"),
                    away_team=stats_payload.get("away_team"),
                    ai_score=int((ai_assessment or {}).get("score") or 0),
                    ai_verdict=(ai_assessment or {}).get("verdict"),
                    ml_pred_score=(ml_prediction or {}).get("score"),
                    ml_pred_verdict=(ml_prediction or {}).get("verdict"),
                    ml_pred_prob_green=(ml_prediction or {}).get("prob_green"),
                    ml_model_samples=(ml_prediction or {}).get("samples"),
                    ml_model_trained_at=(ml_prediction or {}).get("trained_at"),
                    market_key=market_ctx.get("market_key"),
                    market_label=market_ctx.get("market_label"),
                    outcome_signature=market_ctx.get("outcome_signature"),
                    target_side=market_ctx.get("target_side"),
                    target_operator=market_ctx.get("target_operator"),
                    target_value=market_ctx.get("target_value"),
                    target_text=market_ctx.get("target_text"),
                    initial_events_json=_events_to_json(event_timeline),
                    initial_event_metrics_json=_event_metrics_json(event_timeline, effective_minute),
                )
                db.session.add(alert)
                try:
                    rule.last_alert_at = now_sp()
                    rule.last_alert_desc = f"{alert.home_team} vs {alert.away_team}"
                    if rule.alert_on_penalty:
                        penalties_total = stats_payload.get("stats", {}).get("Penalties", {}).get("total", 0)
                        alert.penalty_last_total = penalties_total if isinstance(penalties_total, int) else 0
                        alert.penalty_notified = False
                        alert.penalty_baseline_set = True
                    db.session.commit()
                    existing_pairs.add(pair_key)
                    
                    # Send entry alert immediately after rule hit to reduce latency.
                    if rule.notify_telegram:
                        if REALTIME_ENTRY_ALERTS and ENTRY_GROUP_WINDOW_SECONDS <= 0:
                            message = render_fast_entry_message(rule, stats_payload, game, stats_for_rule)
                            ok, _detail, message_id = _send_message_result(
                                user.telegram_token,
                                user.telegram_chat_id,
                                message,
                                context=f"entry_realtime_{alert.home_team}_{alert.away_team}",
                            )
                            if ok and message_id:
                                alert.telegram_entry_message_id = int(message_id)
                                try:
                                    db.session.commit()
                                except Exception:
                                    db.session.rollback()
                                _queue_entry_enrichment(alert.id)
                        else:
                            history_meta = {}
                            if not REALTIME_SKIP_ENTRY_HISTORY:
                                try:
                                    history_meta = _build_history_meta(session, rule, game.get("url"))
                                except Exception as exc:
                                    print(f"[worker] falha ao coletar historico do jogo {game.get('game_id')}: {exc}")
                            meta = build_message_meta(rule, stats_payload, game, history_meta, stats_override=stats_for_rule)
                            message = render_message(rule, meta)
                            _queue_entry_notification(alert, meta, message)

                    # Refresh once without blocking the cycle with multiple sleeps.
                    latest_payload = fetch_match_stats(session, _cache_bust_url(alert.url))
                    if latest_payload:
                        latest_payload = _apply_live_row_identity(game, latest_payload)
                        stats_payload = latest_payload
                        if rule.second_half_only:
                            baseline = get_second_half_baseline(game["game_id"])
                            if baseline:
                                stats_for_rule = apply_second_half_delta(latest_payload.get("stats", {}), baseline)
                                m2h_latest = max(0, (latest_payload.get("minute") or 0) - 45)
                                stats_for_rule["Minute"] = {"home": m2h_latest, "away": m2h_latest, "total": m2h_latest}
                        else:
                            stats_for_rule = latest_payload.get("stats", {})
                        alert.initial_score = stats_payload.get("score")
                        alert.last_score = stats_payload.get("score")
                        alert.last_score_minute = stats_payload.get("minute")
                        alert.initial_stats_json = stats_to_json(stats_for_rule)
                        alert.initial_events_json = _events_to_json(stats_payload.get("events"))
                        alert.initial_event_metrics_json = _event_metrics_json(
                            stats_payload.get("events"),
                            alert.alert_minute,
                        )
                        db.session.commit()
                except IntegrityError:
                    db.session.rollback()
                except Exception as e:
                    print(f"[worker] erro ao criar alerta: {e}")
                    db.session.rollback()

def evaluate_outcome_conditions(conditions, stats: dict) -> bool:
    if not conditions:
        return False
    # Conditions inside the same group are AND; different groups are OR.
    grouped = {}
    for cond in conditions:
        gid = getattr(cond, "group_id", 0)
        if gid is None:
            gid = 0
        grouped.setdefault(gid, []).append(cond)

    for group_conditions in grouped.values():
        group_ok = True
        for cond in group_conditions:
            key = normalize_stat_key(cond.stat_key)
            if key not in stats:
                group_ok = False
                break
            side_values = stats[key]
            if cond.side not in side_values:
                group_ok = False
                break
            value = side_values[cond.side]
            if value is None:
                group_ok = False
                break
            if not compare(cond.operator, value, cond.value):
                group_ok = False
                break
        if group_ok:
            return True
    return False

def _close_pending_without_live_payload(alert, rule) -> bool:
    if alert.status != "pending" or not rule:
        return False
    fallback_minute = alert.last_score_minute if alert.last_score_minute is not None else alert.alert_minute
    last_update_at = _last_live_update_at(alert.game_id)
    if not should_time_red(rule, alert, fallback_minute, last_update_at=last_update_at, allow_wall_clock=True):
        return False

    fallback_score = alert.last_score or alert.initial_score or "0 x 0"
    fallback_stats = {}
    source_json = alert.ht_stats_json or alert.initial_stats_json
    if source_json:
        try:
            parsed = json.loads(source_json)
            if isinstance(parsed, dict):
                fallback_stats = parsed
        except Exception:
            fallback_stats = {}

    result_minute = fallback_minute if fallback_minute is not None else rule.outcome_red_minute
    update_alert_status(
        alert,
        "red",
        result_minute,
        fallback_score,
        fallback_stats,
        "❌ RED - prazo do GREEN expirou (sem atualização ao vivo)",
    )
    return True

def follow_alerts(session):
    pending_alerts = MatchAlert.query.filter_by(status="pending", ft_completed=False).all()
    active_alerts = pending_alerts + _recently_settled_alerts_query()
    stats_cache = {}
    for alert in active_alerts:
        rule = alert.rule
        # Pending alerts must keep being checked even if live_state became stale,
        # otherwise 2H entries can get stuck without GREEN/RED resolution.
        if alert.status != "pending" and not _is_recent_game_update(alert.game_id):
            continue
        cache_key = alert.url
        if alert.status == "pending":
            # Double-check with multiple reads to catch recent score changes.
            stats_payload = fetch_match_stats_fresh(
                session,
                alert.url,
                attempts=ALERT_FETCH_STATS_ATTEMPTS,
                delay=ALERT_FETCH_STATS_DELAY,
            )
        else:
            if cache_key in stats_cache:
                stats_payload = stats_cache[cache_key]
            else:
                stats_payload = fetch_match_stats(session, alert.url)
                stats_cache[cache_key] = stats_payload
        if not stats_payload:
            _close_pending_without_live_payload(alert, rule)
            continue

        ensure_second_half_baseline(alert.game_id, stats_payload)
        remember_game_snapshot(alert.game_id, stats_payload)
        if persist_live_game_state({"game_id": alert.game_id, "url": alert.url}, stats_payload):
            try:
                db.session.commit()
            except Exception:
                db.session.rollback()
        minute = stats_payload.get("minute") or 0
        current_score = stats_payload.get("score")
        stats = stats_payload.get("stats", {})
        if minute <= 0:
            _close_pending_without_live_payload(alert, rule)
            continue
        prev_minute = alert.last_score_minute if alert.last_score_minute is not None else alert.alert_minute
        if isinstance(prev_minute, int) and isinstance(minute, int) and minute >= prev_minute + 2:
            if current_score and current_score == (alert.last_score or alert.initial_score):
                forced_payload = fetch_match_stats(session, _cache_bust_url(alert.url))
                if forced_payload and forced_payload.get("score") and forced_payload.get("score") != current_score:
                    stats_payload = forced_payload
                    minute = stats_payload.get("minute") or minute
                    current_score = stats_payload.get("score") or current_score
                    stats = stats_payload.get("stats", {}) or stats
        prev_score = alert.last_score or alert.initial_score
        prev_minute = alert.last_score_minute if alert.last_score_minute is not None else alert.alert_minute
        if alert.status != "pending" and prev_score and current_score and minute:
            prev_home, prev_away = parse_score(prev_score)
            curr_home, curr_away = parse_score(current_score)
            prev_total = prev_home + prev_away
            curr_total = curr_home + curr_away
            annulment_window_open = _stage_window_open_for_annulment(
                rule,
                stats_payload.get("time_text", ""),
                minute,
            )
            if prev_minute is not None and minute < prev_minute:
                pass
            elif annulment_window_open and (curr_total < prev_total or curr_home < prev_home or curr_away < prev_away):
                penalties_total = stats.get("Penalties", {}).get("total", 0) if isinstance(stats, dict) else 0
                alert.status = "pending"
                alert.result_minute = None
                alert.result_time_hhmm = None
                alert.ht_score = None
                alert.ht_stats_json = None
                alert.result_events_json = None
                alert.result_event_metrics_json = None
                alert.last_score = current_score
                alert.last_score_minute = minute
                alert.penalty_last_total = penalties_total if isinstance(penalties_total, int) else 0
                alert.penalty_notified = False
                alert.penalty_baseline_set = True
                if not _commit_allow_missing_alert(alert.id, "undo_goal_pending"):
                    continue
                if rule and rule.notify_telegram and alert.user.telegram_token and alert.user.telegram_chat_id:
                    _send_message_safe(
                        alert.user.telegram_token,
                        alert.user.telegram_chat_id,
                        f"Alerta: gol anulado detectado. Status voltou para pendente.\nRegra: {alert.rule.name}\n{alert.home_team} vs {alert.away_team}\nTempo: {minute}'\nPlacar: {current_score}\nLink: {alert.url}",
                        context=f"undo_goal_rule_{rule.id if rule else 'na'}",
                    )
                continue

        if current_score:
            alert.last_score = current_score
            alert.last_score_minute = minute
            if not _commit_allow_missing_alert(alert.id, "follow_score_update"):
                continue

        if alert.status != "pending":
            # Allow a short correction window after RED if a late goal appears.
            if alert.status == "red" and rule and rule.outcome_red_if_no_green:
                red_time = _result_time_to_dt(alert.result_time_hhmm)
                if red_time and (now_sp() - red_time).total_seconds() <= RED_CORRECTION_SECONDS:
                    latest_payload = fetch_match_stats_fresh(
                        session,
                        alert.url,
                        attempts=ALERT_FETCH_STATS_ATTEMPTS,
                        delay=ALERT_FETCH_STATS_DELAY,
                    )
                    if latest_payload:
                        latest_score = latest_payload.get("score") or alert.last_score
                        latest_minute = latest_payload.get("minute") or alert.result_minute
                        # Only allow correction if there was a new goal after RED.
                        red_score = alert.ht_score or alert.last_score or alert.initial_score
                        if red_score and latest_score:
                            red_home, red_away = parse_score(red_score)
                            latest_home, latest_away = parse_score(latest_score)
                            if (latest_home + latest_away) <= (red_home + red_away) and latest_home <= red_home and latest_away <= red_away:
                                continue
                        if alert.result_minute is not None and latest_minute is not None and latest_minute <= alert.result_minute:
                            continue
                        green_conds = [c for c in rule.outcome_conditions if c.outcome_type == "green"]
                        allow_green_eval = True
                        if rule and not rule.second_half_only:
                            green_stage = (rule.outcome_green_stage or "HT").upper()
                            in_first_half_window = is_first_half_goal(
                                latest_payload.get("time_text", ""),
                                latest_minute or 0,
                            ) or is_half_time(
                                latest_payload.get("time_text", ""),
                                latest_minute or 0,
                            )
                            if green_stage == "HT" and not in_first_half_window:
                                allow_green_eval = False
                        base_stats = None
                        if alert.initial_stats_json:
                            try:
                                base_stats = json.loads(alert.initial_stats_json)
                            except Exception:
                                base_stats = None
                        latest_stats = latest_payload.get("stats", {}) or stats
                        eval_stats = apply_alert_delta(latest_stats, base_stats, latest_minute, alert.alert_minute) if base_stats else latest_stats
                        eval_stats = merge_score_delta_into_stats(eval_stats, alert.initial_score, latest_score)
                        if allow_green_eval and green_conds and evaluate_outcome_conditions(green_conds, eval_stats):
                            update_alert_status(
                                alert,
                                "green",
                                latest_minute,
                                latest_score,
                                latest_stats,
                                "✅ GREEN - correção pós-RED",
                                events=latest_payload.get("events"),
                            )
            continue
        maybe_notify_penalty(alert, stats, minute, current_score, time_text=stats_payload.get("time_text"))

        if rule and rule.second_half_only:
            baseline = get_second_half_baseline(alert.game_id)
            if baseline: stats = apply_second_half_delta(stats_payload["stats"], baseline)
            m2h = max(0, minute - 45)
            stats["Minute"] = {"home": m2h, "away": m2h, "total": m2h}

        green_conds = [c for c in rule.outcome_conditions if c.outcome_type == "green"] if rule else []
        red_conds = [c for c in rule.outcome_conditions if c.outcome_type == "red"] if rule else []
        allow_green_eval = True
        allow_red_eval = True
        if rule and not rule.second_half_only:
            green_stage = (rule.outcome_green_stage or "HT").upper()
            red_stage = (rule.outcome_red_stage or "HT").upper()
            # Guard against misconfigured stage/minute combos:
            # if minute cap is after HT, evaluate on FT window.
            if green_stage == "HT" and (rule.outcome_green_minute or 0) > 45:
                green_stage = "FT"
            if red_stage == "HT" and (rule.outcome_red_minute or 0) > 45:
                red_stage = "FT"
            in_first_half_window = is_first_half_goal(stats_payload.get("time_text", ""), minute) or is_half_time(stats_payload.get("time_text", ""), minute)
            if green_stage == "HT" and not in_first_half_window:
                allow_green_eval = False
            if red_stage == "HT" and not in_first_half_window:
                allow_red_eval = False

        base_stats = None
        if alert.initial_stats_json:
            try:
                base_stats = json.loads(alert.initial_stats_json)
            except Exception:
                base_stats = None
        stats_for_outcome = apply_alert_delta(stats, base_stats, minute, alert.alert_minute) if base_stats else stats
        stats_for_outcome = merge_score_delta_into_stats(stats_for_outcome, alert.initial_score, current_score)
        
        # 1. Verificar GREEN customizado
        if allow_green_eval and green_conds and evaluate_outcome_conditions(green_conds, stats_for_outcome):
            latest_payload = fetch_match_stats_fresh(
                session,
                alert.url,
                attempts=ALERT_FETCH_STATS_ATTEMPTS,
                delay=ALERT_FETCH_STATS_DELAY,
            )
            if latest_payload:
                minute = latest_payload.get("minute") or minute
                current_score = latest_payload.get("score") or current_score
                stats = latest_payload.get("stats", {}) or stats
            update_alert_status(
                alert,
                "green",
                minute,
                current_score,
                stats,
                "✅ GREEN - condições atingidas",
                events=latest_payload.get("events") if latest_payload else stats_payload.get("events"),
            )
            continue

        # 2. Verificar RED customizado
        if allow_red_eval and red_conds and evaluate_outcome_conditions(red_conds, stats_for_outcome):
            update_alert_status(
                alert,
                "red",
                minute,
                current_score,
                stats,
                "❌ RED - condições de RED atingidas",
                events=stats_payload.get("events"),
            )
            continue

        # 3. Verificar RED por tempo (se habilitado)
        time_red_due = should_time_red(rule, alert, minute)
        if not time_red_due and alert.id in RED_CONFIRM_PENDING:
            RED_CONFIRM_PENDING.pop(alert.id, None)
        if time_red_due:
            # Dupla verificacao antes do RED: reler o jogo para evitar atraso de feed.
            latest_payload = fetch_match_stats(session, _cache_bust_url(alert.url))
            latest_minute = minute
            latest_score = current_score
            latest_stats = stats
            if latest_payload:
                latest_minute = latest_payload.get("minute") or minute
                latest_score = latest_payload.get("score") or current_score
                latest_stats = latest_payload.get("stats", {}) or stats
                if latest_score:
                    alert.last_score = latest_score
                    alert.last_score_minute = latest_minute
                    if not _commit_allow_missing_alert(alert.id, "time_red_latest_score"):
                        continue

                latest_eval_stats = latest_stats
                if rule and rule.second_half_only:
                    latest_baseline = get_second_half_baseline(alert.game_id)
                    if latest_baseline:
                        latest_eval_stats = apply_second_half_delta(latest_stats, latest_baseline)
                    m2h_latest = max(0, (latest_minute or 0) - 45)
                    latest_eval_stats["Minute"] = {"home": m2h_latest, "away": m2h_latest, "total": m2h_latest}

                latest_outcome_stats = (
                    apply_alert_delta(latest_eval_stats, base_stats, latest_minute, alert.alert_minute)
                    if base_stats else latest_eval_stats
                )
                latest_outcome_stats = merge_score_delta_into_stats(latest_outcome_stats, alert.initial_score, latest_score)
                if allow_green_eval and green_conds and evaluate_outcome_conditions(green_conds, latest_outcome_stats):
                    latest_payload = fetch_match_stats_fresh(
                        session,
                        alert.url,
                        attempts=ALERT_FETCH_STATS_ATTEMPTS,
                        delay=ALERT_FETCH_STATS_DELAY,
                    )
                    if latest_payload:
                        latest_minute = latest_payload.get("minute") or latest_minute
                        latest_score = latest_payload.get("score") or latest_score
                        latest_stats = latest_payload.get("stats", {}) or latest_stats
                    update_alert_status(
                        alert,
                        "green",
                        latest_minute,
                        latest_score,
                        latest_stats,
                        "GREEN - condicoes atingidas",
                        events=latest_payload.get("events") if latest_payload else None,
                    )
                    RED_CONFIRM_PENDING.pop(alert.id, None)
                    continue

            pending = RED_CONFIRM_PENDING.get(alert.id)
            if not pending:
                RED_CONFIRM_PENDING[alert.id] = {"seen_at": now_sp()}
                continue
            if (now_sp() - pending["seen_at"]).total_seconds() < RED_CONFIRM_SECONDS:
                continue

            # Final refresh to avoid stale score in RED message.
            final_payload = fetch_match_stats_fresh(
                session,
                alert.url,
                attempts=max(1, ALERT_FETCH_STATS_ATTEMPTS),
                delay=ALERT_FETCH_STATS_DELAY,
            )
            if final_payload:
                latest_minute = final_payload.get("minute") or latest_minute
                latest_score = final_payload.get("score") or latest_score
                latest_stats = final_payload.get("stats", {}) or latest_stats
            RED_CONFIRM_PENDING.pop(alert.id, None)
            update_alert_status(
                alert,
                "red",
                latest_minute,
                latest_score,
                latest_stats,
                "❌ RED - prazo do GREEN expirou",
                events=final_payload.get("events") if final_payload else None,
            )
            continue

        # 4. Lógica padrão (se não houver condições customizadas)
        if not green_conds and not red_conds:
            if alert.initial_score and current_score != alert.initial_score and is_first_half_goal(stats_payload.get("time_text", ""), minute):
                update_alert_status(
                    alert,
                    "green",
                    minute,
                    current_score,
                    stats,
                    "✅ GREEN - gol no 1o tempo",
                    events=stats_payload.get("events"),
                )
            elif is_half_time(stats_payload.get("time_text", ""), minute):
                update_alert_status(
                    alert,
                    "red",
                    minute,
                    current_score,
                    stats,
                    "❌ RED - fim do 1o tempo sem gol",
                    events=stats_payload.get("events"),
                )

def update_alert_status(alert, status, minute, score, stats, msg_prefix, events=None):
    RED_CONFIRM_PENDING.pop(alert.id, None)
    alert.status = status
    alert.result_minute = minute
    alert.result_time_hhmm = now_sp().strftime("%H:%M")
    alert.ht_score = score
    alert.ht_stats_json = stats_to_json(stats)
    alert.result_events_json = _events_to_json(events)
    alert.result_event_metrics_json = _event_metrics_json(events, alert.alert_minute)
    alert.last_score = score
    alert.last_score_minute = minute
    if not _commit_allow_missing_alert(alert.id, f"update_status_{status}"):
        return
    export_alert(alert, alert.rule.name, EXPORT_DIR)
    if alert.rule and alert.rule.notify_telegram and alert.user.telegram_token and alert.user.telegram_chat_id:
        if status == "green":
            _queue_grouped_notification(alert, minute, score, msg_prefix, "green")
        else:
            _queue_grouped_notification(alert, minute, score, msg_prefix, "red")

def finalize_full_time(session):
    cutoff = now_sp() - timedelta(hours=FINALIZE_LOOKBACK_HOURS)
    alerts = (
        MatchAlert.query.filter_by(ft_completed=False)
        .filter(MatchAlert.status.in_(("green", "red")))
        .filter(MatchAlert.created_at >= cutoff)
        .all()
    )
    for alert in alerts:
        stats_payload = fetch_match_stats(session, alert.url)
        if not stats_payload:
            continue
        minute = stats_payload.get("minute") or 0
        if is_full_time(stats_payload.get("time_text", ""), minute):
            alert.ft_score = stats_payload.get("score")
            alert.ft_stats_json = stats_to_json(stats_payload["stats"])
            alert.ft_events_json = _events_to_json(stats_payload.get("events"))
            alert.ft_event_metrics_json = _event_metrics_json(stats_payload.get("events"), alert.alert_minute)
            alert.ft_completed = True
            db.session.commit()
            export_alert(alert, alert.rule.name, EXPORT_DIR)
            SECOND_HALF_BASELINES.pop(alert.game_id, None)
            LAST_GAME_SNAPSHOTS.pop(alert.game_id, None)
            HALFTIME_SEEN_AT.pop(alert.game_id, None)
