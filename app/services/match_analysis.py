import json
import os
import time
from hashlib import sha1
from statistics import mean

import requests

from app.models import LiveGameState
from app.services.scraper import (
    enrich_history_with_ht_goals,
    fetch_match_history,
    fetch_match_stats,
    make_session,
    summarize_history,
)

DETAIL_LIMITS = {
    "h2h": int(os.environ.get("ANALYSIS_H2H_DETAIL_LIMIT", "2")),
    "home": int(os.environ.get("ANALYSIS_HOME_DETAIL_LIMIT", "1")),
    "away": int(os.environ.get("ANALYSIS_AWAY_DETAIL_LIMIT", "1")),
}
HISTORY_LIMITS = {
    "h2h": int(os.environ.get("ANALYSIS_H2H_LIMIT", "8")),
    "home": int(os.environ.get("ANALYSIS_HOME_LIMIT", "6")),
    "away": int(os.environ.get("ANALYSIS_AWAY_LIMIT", "6")),
}
ANALYSIS_CACHE_DIR = os.environ.get("ANALYSIS_CACHE_DIR", os.path.join("data", "analysis_cache"))
ANALYSIS_CACHE_TTL_SECONDS = int(os.environ.get("ANALYSIS_CACHE_TTL_SECONDS", str(7 * 24 * 60 * 60)))
ANALYSIS_PENDING_CACHE_TTL_SECONDS = int(os.environ.get("ANALYSIS_PENDING_CACHE_TTL_SECONDS", "600"))
ANALYSIS_HISTORY_TIMEOUT_SECONDS = int(os.environ.get("ANALYSIS_HISTORY_TIMEOUT_SECONDS", "4"))


def _safe_json(raw):
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _cache_path(alert) -> str:
    raw = f"{getattr(alert, 'id', '')}:{getattr(alert, 'game_id', '')}:{getattr(alert, 'url', '')}"
    digest = sha1(raw.encode("utf-8", "ignore")).hexdigest()
    return os.path.join(ANALYSIS_CACHE_DIR, f"{digest}.json")


def _cache_ttl(alert) -> int:
    return ANALYSIS_PENDING_CACHE_TTL_SECONDS if getattr(alert, "status", "") == "pending" else ANALYSIS_CACHE_TTL_SECONDS


def _load_cached_analysis(alert):
    path = _cache_path(alert)
    try:
        stat = os.stat(path)
    except OSError:
        return None
    if time.time() - stat.st_mtime > _cache_ttl(alert):
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            cached = json.load(fh)
    except Exception:
        return None
    if not isinstance(cached, dict):
        return None
    groups = cached.get("groups") or []
    if not any(isinstance(group, dict) and int(group.get("count") or 0) > 0 for group in groups):
        return None
    cached["cached"] = True
    return cached


def _save_cached_analysis(alert, analysis: dict) -> None:
    if not isinstance(analysis, dict):
        return
    groups = analysis.get("groups") or []
    if not any(isinstance(group, dict) and int(group.get("count") or 0) > 0 for group in groups):
        return
    try:
        os.makedirs(ANALYSIS_CACHE_DIR, exist_ok=True)
        payload = {k: v for k, v in analysis.items() if k != "alert"}
        payload["cached"] = False
        with open(_cache_path(alert), "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=True)
    except Exception:
        pass


def _make_quick_session():
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    return session


def _stat(stats: dict, key: str, side: str = "total"):
    bucket = stats.get(key) if isinstance(stats, dict) else None
    if not isinstance(bucket, dict):
        return None
    value = bucket.get(side)
    return value if value not in (None, "", "-") else None


def _avg(items, key: str):
    values = [float(item.get(key) or 0) for item in items if item.get(key) is not None]
    return round(mean(values), 2) if values else None


def _pct(part: int, total: int):
    return round((part / total) * 100) if total else 0


def _phase_metrics(items: list[dict]) -> dict:
    detailed = [item for item in items if item.get("goals_ht") is not None]
    total = len(detailed)
    if not total:
        return {
            "samples": 0,
            "goal_1h_games": 0,
            "goal_2h_games": 0,
            "goal_1h_pct": 0,
            "goal_2h_pct": 0,
            "avg_goals_1h": None,
            "avg_goals_2h": None,
            "avg_corners_1h": None,
            "avg_corners_2h": None,
            "avg_on_target_1h": None,
            "avg_on_target_2h": None,
            "avg_cards_1h": None,
            "avg_cards_2h": None,
            "corner_lines": {},
            "corners_samples": 0,
            "avg_cards_total": None,
            "card_lines": {},
            "cards_samples": 0,
            "throw_ins_samples": 0,
            "avg_throw_ins": None,
            "throw_in_lines": {},
            "offsides_samples": 0,
            "avg_offsides": None,
        }
    goal_1h = sum(1 for item in detailed if int(item.get("goals_ht") or 0) > 0)
    goal_2h = sum(1 for item in detailed if int(item.get("goals_2h") or 0) > 0)
    card_detailed = [item for item in detailed if item.get("yellow_cards_ht") is not None]
    corner_detailed = [item for item in detailed if item.get("corners_ht") is not None]
    cards_1h = [
        int(item.get("yellow_cards_ht") or 0) + int(item.get("red_cards_ht") or 0)
        for item in card_detailed
    ]
    cards_2h = [
        int(item.get("yellow_cards_2h") or 0) + int(item.get("red_cards_2h") or 0)
        for item in card_detailed
    ]
    corner_totals = [
        int(item.get("corners_ht") or 0) + int(item.get("corners_2h") or 0)
        for item in corner_detailed
    ]
    card_totals = [cards_1h[index] + cards_2h[index] for index in range(len(card_detailed))]
    throw_ins = [int(item["throw_ins_total"]) for item in detailed if item.get("throw_ins_total") is not None]
    offsides = [int(item["offsides_total"]) for item in detailed if item.get("offsides_total") is not None]
    return {
        "samples": total,
        "goal_1h_games": goal_1h,
        "goal_2h_games": goal_2h,
        "goal_1h_pct": _pct(goal_1h, total),
        "goal_2h_pct": _pct(goal_2h, total),
        "avg_goals_1h": _avg(detailed, "goals_ht"),
        "avg_goals_2h": _avg(detailed, "goals_2h"),
        "avg_corners_1h": _avg(corner_detailed, "corners_ht"),
        "avg_corners_2h": _avg(corner_detailed, "corners_2h"),
        "avg_on_target_1h": _avg(detailed, "on_target_events_ht"),
        "avg_on_target_2h": _avg(detailed, "on_target_events_2h"),
        "avg_cards_1h": round(mean(cards_1h), 2) if cards_1h else None,
        "avg_cards_2h": round(mean(cards_2h), 2) if cards_2h else None,
        "avg_cards_total": round(mean(card_totals), 2) if card_totals else None,
        "cards_samples": len(card_detailed),
        "card_lines": {
            str(line): _pct(sum(1 for total_cards in card_totals if total_cards > line), len(card_totals))
            for line in (2.5, 3.5, 4.5, 5.5)
        },
        "corner_lines": {
            str(line): _pct(sum(1 for total_corners in corner_totals if total_corners > line), len(corner_totals))
            for line in (4.5, 7.5, 8.5, 9.5)
        },
        "corners_samples": len(corner_detailed),
        "throw_ins_samples": len(throw_ins),
        "avg_throw_ins": round(mean(throw_ins), 2) if throw_ins else None,
        "throw_in_lines": {
            str(line): _pct(sum(1 for total_throw_ins in throw_ins if total_throw_ins > line), len(throw_ins))
            for line in (29.5, 34.5, 39.5)
        } if throw_ins else {},
        "offsides_samples": len(offsides),
        "avg_offsides": round(mean(offsides), 2) if offsides else None,
    }


def _group_analysis(label: str, items: list[dict]) -> dict:
    summary = summarize_history(items) or {}
    phase = _phase_metrics(items)
    return {
        "key": label,
        "count": summary.get("count", 0),
        "avg_goals": summary.get("avg_goals"),
        "over15": summary.get("over15", 0),
        "over25": summary.get("over25", 0),
        "btts": summary.get("btts", 0),
        "phase": phase,
        "items": items,
    }


def _current_snapshot(alert, session):
    state = LiveGameState.query.filter_by(game_id=alert.game_id).first()
    if state:
        stats = _safe_json(state.stats_json)
        return {
            "source": "live_state",
            "minute": state.minute,
            "time_text": state.time_text,
            "score": state.score,
            "stats": stats,
            "on_target": _stat(stats, "On Target"),
            "corners": _stat(stats, "Corners"),
            "dangerous_attacks": _stat(stats, "Dangerous Attacks"),
            "attacks": _stat(stats, "Attacks"),
            "possession_home": _stat(stats, "Possession", "home"),
            "possession_away": _stat(stats, "Possession", "away"),
        }
    if getattr(alert, "status", "") in ("green", "red"):
        stats = _safe_json(alert.ft_stats_json) or _safe_json(alert.ht_stats_json) or _safe_json(alert.initial_stats_json)
        return {
            "source": "alert",
            "minute": alert.result_minute or alert.last_score_minute or alert.alert_minute,
            "time_text": alert.result_time_hhmm,
            "score": alert.ft_score or alert.last_score or alert.ht_score or alert.initial_score,
            "stats": stats,
            "on_target": _stat(stats, "On Target"),
            "corners": _stat(stats, "Corners"),
            "dangerous_attacks": _stat(stats, "Dangerous Attacks"),
            "attacks": _stat(stats, "Attacks"),
            "possession_home": _stat(stats, "Possession", "home"),
            "possession_away": _stat(stats, "Possession", "away"),
        }
    stats = _safe_json(alert.ft_stats_json) or _safe_json(alert.ht_stats_json) or _safe_json(alert.initial_stats_json)
    return {
        "source": "alert",
        "minute": alert.result_minute or alert.last_score_minute or alert.alert_minute,
        "time_text": alert.result_time_hhmm,
        "score": alert.ft_score or alert.last_score or alert.ht_score or alert.initial_score,
        "stats": stats,
        "on_target": _stat(stats, "On Target"),
        "corners": _stat(stats, "Corners"),
        "dangerous_attacks": _stat(stats, "Dangerous Attacks"),
        "attacks": _stat(stats, "Attacks"),
        "possession_home": _stat(stats, "Possession", "home"),
        "possession_away": _stat(stats, "Possession", "away"),
    }


def build_alert_analysis(
    alert,
    force_refresh: bool = False,
    include_details: bool = True,
    detail_limits: dict | None = None,
    history_limits: dict | None = None,
) -> dict:
    if not force_refresh:
        cached = _load_cached_analysis(alert)
        if cached:
            return cached

    session = make_session()
    history_data = fetch_match_history(
        session,
        alert.url,
        limits=history_limits or HISTORY_LIMITS,
        use_fallback=True,
        timeout=ANALYSIS_HISTORY_TIMEOUT_SECONDS,
    )
    selected_detail_limits = detail_limits or DETAIL_LIMITS
    if include_details and any((limit or 0) > 0 for limit in selected_detail_limits.values()):
        history_data = enrich_history_with_ht_goals(session, history_data, selected_detail_limits)
    groups = [
        _group_analysis("H2H", history_data.get("h2h", [])),
        _group_analysis("Mandante", history_data.get("home", [])),
        _group_analysis("Visitante", history_data.get("away", [])),
    ]
    current = _current_snapshot(alert, session)
    best_signal = max(
        groups,
        key=lambda group: (
            group["phase"]["samples"],
            group["phase"]["goal_1h_pct"] + group["phase"]["goal_2h_pct"],
            group["count"],
        ),
    )
    analysis = {
        "alert": alert,
        "current": current,
        "groups": groups,
        "best_signal": best_signal,
        "scheduled_time": history_data.get("scheduled_time"),
        "cached": False,
    }
    _save_cached_analysis(alert, analysis)
    return analysis
