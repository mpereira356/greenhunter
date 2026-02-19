import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import ObjectDeletedError, StaleDataError

from app.extensions import db
from app.models import LiveGameState, MatchAlert, Rule, User
from app.services.evaluator import compare, evaluate_rule, history_confidence, render_message, stats_to_json
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
from app.services.telegram import send_message
from app.utils.time import now_sp

POLL_INTERVAL = int(os.environ.get("WORKER_INTERVAL", "15"))
ALERTS_POLL_INTERVAL = float(os.environ.get("ALERTS_POLL_INTERVAL", "5"))
GAME_DELAY = float(os.environ.get("WORKER_GAME_DELAY", "1.5"))
EXPORT_DIR = os.environ.get("EXPORT_DIR", "data/exports")
RULE_CONF_SAMPLE = int(os.environ.get("RULE_CONF_SAMPLE", "50"))
RULE_CONF_MIN = int(os.environ.get("RULE_CONF_MIN", "10"))
LEAGUE_CONF_SAMPLE = int(os.environ.get("LEAGUE_CONF_SAMPLE", "80"))
LEAGUE_CONF_MIN = int(os.environ.get("LEAGUE_CONF_MIN", "8"))

API_STATUS = {"ok": None, "code": None, "checked_at": None, "last_cycle": None}
API_ALERT_STATE = {"last_ok": None}
BOT_STARTED_AT = None
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
ANALYSIS_THREADS = max(1, int(os.environ.get("WORKER_ANALYSIS_THREADS", "6")))
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
    try:
        result = send_message(token, chat_id, text)
        if isinstance(result, tuple):
            ok, detail = bool(result[0]), result[1] if len(result) > 1 else ""
        else:
            ok, detail = bool(result), ""
        if not ok:
            print(f"[telegram] envio falhou ({context}): {detail}")
        return ok
    except Exception as exc:
        print(f"[telegram] excecao ao enviar ({context}): {exc}")
        return False


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
    threading.Thread(target=run_worker, args=(app,), daemon=True).start()
    threading.Thread(target=run_alerts_worker, args=(app,), daemon=True).start()

def run_worker(app):
    with app.app_context():
        global BOT_STARTED_AT
        BOT_STARTED_AT = now_sp()
        maybe_retrain_model(force=False)
        session = make_session()
        while True:
            try:
                maybe_retrain_model(force=False)
                process_live_games(session)
                finalize_full_time(session)
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
            except Exception as exc:
                db.session.rollback()
                print(f"[alerts] erro: {exc}")
            time.sleep(ALERTS_POLL_INTERVAL)

def _game_key(game: dict) -> str:
    return str(game.get("game_id") or game.get("url") or "")


def _prefetch_game_stats(game: dict):
    key = _game_key(game)
    if not key:
        return key, None, None
    session = make_session()
    try:
        stats_payload = fetch_match_stats_fresh(session, game["url"], attempts=3, delay=1)
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
    max_workers = min(len(games), ANALYSIS_THREADS)
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
    prefetched = _prefetch_live_stats(games)
    for game in games:
        game_key = _game_key(game)
        prefetched_item = prefetched.get(game_key) if game_key else None
        if not prefetched_item:
            continue
        stats_payload = prefetched_item["stats_payload"]
        minute = prefetched_item["minute"]

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
                        stats_payload = forced_payload
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
            existing = MatchAlert.query.filter_by(game_id=game["game_id"], rule_id=rule.id).first()
            if existing: continue
            # Optional league filter: if configured, only emit alerts for matching leagues.
            allowed_json = getattr(rule, "allowed_leagues_json", None)
            if allowed_json:
                try:
                    allowed_items = json.loads(allowed_json)
                except Exception:
                    allowed_items = []
                if isinstance(allowed_items, list) and allowed_items:
                    allowed = {str(x).strip().casefold() for x in allowed_items if str(x).strip()}
                    league_name = str(stats_payload.get("league") or "").strip().casefold()
                    if allowed and league_name not in allowed:
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
                market_ctx = infer_green_profile(rule)
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
                    ml_pred_score=(ml_prediction or {}).get("score"),
                    ml_pred_verdict=(ml_prediction or {}).get("verdict"),
                    ml_pred_prob_green=(ml_prediction or {}).get("prob_green"),
                    ml_model_samples=(ml_prediction or {}).get("samples"),
                    ml_model_trained_at=(ml_prediction or {}).get("trained_at"),
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
                        conf_conds = [c for c in rule.outcome_conditions if c.outcome_type == "green"] or rule.conditions
                        confidence = history_confidence(conf_conds, h2h_items)
                        history_meta = {
                            "history_h2h": format_history_summary("H2H", h2h_summary) if h2h_summary else "Sem historico de um contra o outro",
                            "history_home": format_history_summary("Home", home_summary),
                            "history_away": format_history_summary("Away", away_summary),
                            "history_confidence": f"{confidence}%" if confidence is not None else "N/A",
                        }
                    except Exception:
                        pass
                    
                    # Re-fetch latest stats for accurate initial alert message.
                    latest_payload = fetch_match_stats_fresh(session, alert.url, attempts=3, delay=1)
                    if latest_payload:
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
                        db.session.commit()
                    meta = build_message_meta(rule, stats_payload, game, history_meta, stats_override=stats_for_rule)
                    message = render_message(rule, meta)
                    if rule.notify_telegram:
                        _send_message_safe(user.telegram_token, user.telegram_chat_id, message, context=f"new_alert_rule_{rule.id}")
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
    if not should_time_red(rule, alert, fallback_minute):
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
    active_alerts = MatchAlert.query.filter(MatchAlert.status.in_(("pending", "green", "red"))).all()
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
            stats_payload = fetch_match_stats_fresh(session, alert.url, attempts=3, delay=1)
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
                    latest_payload = fetch_match_stats_fresh(session, alert.url, attempts=3, delay=1)
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
                        base_stats = None
                        if alert.initial_stats_json:
                            try:
                                base_stats = json.loads(alert.initial_stats_json)
                            except Exception:
                                base_stats = None
                        latest_stats = latest_payload.get("stats", {}) or stats
                        eval_stats = apply_alert_delta(latest_stats, base_stats, latest_minute, alert.alert_minute) if base_stats else latest_stats
                        eval_stats = merge_score_delta_into_stats(eval_stats, alert.initial_score, latest_score)
                        if green_conds and evaluate_outcome_conditions(green_conds, eval_stats):
                            update_alert_status(
                                alert,
                                "green",
                                latest_minute,
                                latest_score,
                                latest_stats,
                                "✅ GREEN - correção pós-RED",
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
            latest_payload = fetch_match_stats_fresh(session, alert.url, attempts=3, delay=1)
            if latest_payload:
                minute = latest_payload.get("minute") or minute
                current_score = latest_payload.get("score") or current_score
                stats = latest_payload.get("stats", {}) or stats
            update_alert_status(alert, "green", minute, current_score, stats, "✅ GREEN - condições atingidas")
            continue

        # 2. Verificar RED customizado
        if allow_red_eval and red_conds and evaluate_outcome_conditions(red_conds, stats_for_outcome):
            update_alert_status(alert, "red", minute, current_score, stats, "❌ RED - condições de RED atingidas")
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
                    latest_payload = fetch_match_stats_fresh(session, alert.url, attempts=3, delay=1)
                    if latest_payload:
                        latest_minute = latest_payload.get("minute") or latest_minute
                        latest_score = latest_payload.get("score") or latest_score
                        latest_stats = latest_payload.get("stats", {}) or latest_stats
                    update_alert_status(alert, "green", latest_minute, latest_score, latest_stats, "GREEN - condicoes atingidas")
                    RED_CONFIRM_PENDING.pop(alert.id, None)
                    continue

            pending = RED_CONFIRM_PENDING.get(alert.id)
            if not pending:
                RED_CONFIRM_PENDING[alert.id] = {"seen_at": now_sp()}
                continue
            if (now_sp() - pending["seen_at"]).total_seconds() < RED_CONFIRM_SECONDS:
                continue

            # Final refresh to avoid stale score in RED message.
            final_payload = fetch_match_stats_fresh(session, alert.url, attempts=2, delay=1)
            if final_payload:
                latest_minute = final_payload.get("minute") or latest_minute
                latest_score = final_payload.get("score") or latest_score
                latest_stats = final_payload.get("stats", {}) or latest_stats
            RED_CONFIRM_PENDING.pop(alert.id, None)
            update_alert_status(alert, "red", latest_minute, latest_score, latest_stats, "❌ RED - prazo do GREEN expirou")
            continue

        # 4. Lógica padrão (se não houver condições customizadas)
        if not green_conds and not red_conds:
            if alert.initial_score and current_score != alert.initial_score and is_first_half_goal(stats_payload.get("time_text", ""), minute):
                update_alert_status(alert, "green", minute, current_score, stats, "✅ GREEN - gol no 1o tempo")
            elif is_half_time(stats_payload.get("time_text", ""), minute):
                update_alert_status(alert, "red", minute, current_score, stats, "❌ RED - fim do 1o tempo sem gol")

def update_alert_status(alert, status, minute, score, stats, msg_prefix):
    RED_CONFIRM_PENDING.pop(alert.id, None)
    alert.status = status
    alert.result_minute = minute
    alert.result_time_hhmm = now_sp().strftime("%H:%M")
    alert.ht_score = score
    alert.ht_stats_json = stats_to_json(stats)
    alert.last_score = score
    alert.last_score_minute = minute
    if not _commit_allow_missing_alert(alert.id, f"update_status_{status}"):
        return
    export_alert(alert, alert.rule.name, EXPORT_DIR)
    if alert.rule and alert.rule.notify_telegram and alert.user.telegram_token and alert.user.telegram_chat_id:
        ml_feedback = _ml_feedback_for_result(alert, status)
        _send_message_safe(
            alert.user.telegram_token,
            alert.user.telegram_chat_id,
            (
                f"{msg_prefix}\n"
                f"Regra: {alert.rule.name}\n"
                f"{alert.home_team} vs {alert.away_team}\n"
                f"Tempo: {minute}'\n"
                f"Placar: {score}\n"
                f"{ml_feedback}\n"
                f"Link: {alert.url}"
            ),
            context=f"status_{status}_rule_{alert.rule.id if alert.rule else 'na'}",
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
