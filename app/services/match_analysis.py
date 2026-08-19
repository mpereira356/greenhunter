import json
import os
import time
from hashlib import sha1
from statistics import mean

import requests
import re
import unicodedata

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


def _team_key(value) -> str:
    text = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode().casefold()
    return re.sub(r"[^a-z0-9]+", "", text)


def _same_team(left, right) -> bool:
    left_key, right_key = _team_key(left), _team_key(right)
    if not left_key or not right_key:
        return False
    if left_key == right_key:
        return True
    # A fonte alterna nomes como "Orlando City" / "Orlando City SC" e
    # "Pumas" / "UNAM Pumas". Só aceite contenção para nomes significativos.
    return min(len(left_key), len(right_key)) >= 6 and (left_key in right_key or right_key in left_key)


def _phase_metrics(items: list[dict], target_team: str | None = None) -> dict:
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
            "corners_10_samples": 0,
            "corners_10_over0_pct": None,
            "corners_10_over1_pct": None,
            "team_corners_samples": 0,
            "avg_team_corners": None,
            "team_corner_lines": {},
            "avg_cards_total": None,
            "card_lines": {},
            "cards_samples": 0,
            "throw_ins_samples": 0,
            "avg_throw_ins": None,
            "throw_in_lines": {},
            "offsides_samples": 0,
            "avg_offsides": None,
            "shots_samples": 0,
            "avg_shots": None,
            "shots_on_target_samples": 0,
            "avg_shots_on_target": None,
            "fouls_samples": 0,
            "avg_fouls": None,
            "history_values": {},
        }
    goal_1h = sum(1 for item in detailed if int(item.get("goals_ht") or 0) > 0)
    goal_2h = sum(1 for item in detailed if int(item.get("goals_2h") or 0) > 0)
    card_detailed = [item for item in detailed if item.get("yellow_cards_ht") is not None]
    corner_detailed = [item for item in detailed if item.get("corners_ht") is not None]
    corners_10_detailed = [item for item in detailed if item.get("corners_10") is not None]
    corners_10_values = [int(item.get("corners_10") or 0) for item in corners_10_detailed]
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
    target_key = _team_key(target_team)

    def team_side_value(item, home_field, away_field):
        if not target_key:
            return None
        if _same_team(item.get("history_home_team"), target_team):
            return item.get(home_field)
        if _same_team(item.get("history_away_team"), target_team):
            return item.get(away_field)
        return None

    def team_values(home_field, away_field):
        return [int(value) for item in detailed if (value := team_side_value(item, home_field, away_field)) is not None]

    team_goals = team_values("home", "away")
    team_goals_ht = team_values("goals_ht_home", "goals_ht_away")
    team_goals_2h = team_values("goals_2h_home", "goals_2h_away")
    team_cards = team_values("cards_home", "cards_away")
    team_cards_1h = team_values("cards_ht_home", "cards_ht_away")
    team_cards_2h = team_values("cards_2h_home", "cards_2h_away")
    team_offsides = team_values("offsides_home", "offsides_away")
    team_shots = team_values("shots_home", "shots_away")
    team_shots_on_target = team_values("shots_on_target_home", "shots_on_target_away")
    team_shots_1h = team_values("shots_ht_home", "shots_ht_away")
    team_shots_2h = team_values("shots_2h_home", "shots_2h_away")
    team_shots_on_target_1h = team_values("on_target_ht_home", "on_target_ht_away")
    team_shots_on_target_2h = team_values("on_target_2h_home", "on_target_2h_away")
    team_fouls = team_values("fouls_home", "fouls_away")
    team_corner_values = []
    team_corners_1h = team_values("corners_ht_home", "corners_ht_away")
    team_corners_2h = team_values("corners_2h_home", "corners_2h_away")
    if target_key:
        for item in corner_detailed:
            value = None
            if _same_team(item.get("history_home_team"), target_team):
                value = item.get("corners_home")
            elif _same_team(item.get("history_away_team"), target_team):
                value = item.get("corners_away")
            if value is not None:
                team_corner_values.append(int(value))
    card_totals = [cards_1h[index] + cards_2h[index] for index in range(len(card_detailed))]
    throw_ins = [int(item["throw_ins_total"]) for item in detailed if item.get("throw_ins_total") is not None]
    offsides = [int(item["offsides_total"]) for item in detailed if item.get("offsides_total") is not None]
    shots = [int(item["shots_total"]) for item in detailed if item.get("shots_total") is not None]
    shots_on_target = [int(item["shots_on_target_total"]) for item in detailed if item.get("shots_on_target_total") is not None]
    fouls = [int(item["fouls_total"]) for item in detailed if item.get("fouls_total") is not None]

    def history_rows(value_getter, sides_getter=None):
        rows = []
        for item in detailed:
            value = value_getter(item)
            if value is None:
                continue
            home_name = str(item.get("history_home_team") or "Time da casa")
            away_name = str(item.get("history_away_team") or "Time visitante")
            row = {"match": f"{home_name} x {away_name}", "value": int(value)}
            sides = sides_getter(item) if sides_getter else None
            if sides and sides[0] is not None and sides[1] is not None:
                row["home_name"], row["away_name"] = home_name, away_name
                row["home_value"], row["away_value"] = int(sides[0]), int(sides[1])
            rows.append(row)
        return rows

    def team_corner_value(item):
        if _same_team(item.get("history_home_team"), target_team):
            return item.get("corners_home")
        if _same_team(item.get("history_away_team"), target_team):
            return item.get("corners_away")
        return None

    def card_total(item):
        if item.get("yellow_cards_ht") is None:
            return None
        return sum(int(item.get(key) or 0) for key in ("yellow_cards_ht", "yellow_cards_2h", "red_cards_ht", "red_cards_2h"))
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
        "corners_10_samples": len(corners_10_values),
        "corners_10_over0_pct": _pct(sum(1 for value in corners_10_values if value >= 1), len(corners_10_values)) if corners_10_values else None,
        "corners_10_over1_pct": _pct(sum(1 for value in corners_10_values if value >= 2), len(corners_10_values)) if corners_10_values else None,
        "team_corners_samples": len(team_corner_values),
        "avg_team_corners": round(mean(team_corner_values), 2) if team_corner_values else None,
        "team_corner_lines": {
            str(line): _pct(sum(1 for corners in team_corner_values if corners > line), len(team_corner_values))
            for line in (1.5, 2.5, 3.5, 4.5, 5.5, 6.5)
        } if team_corner_values else {},
        "team_goals_samples": len(team_goals),
        "team_goal_ht_samples": len(team_goals_ht),
        "team_goal_ht_pct": _pct(sum(1 for value in team_goals_ht if value > 0), len(team_goals_ht)),
        "team_goal_2h_samples": len(team_goals_2h),
        "team_goal_2h_pct": _pct(sum(1 for value in team_goals_2h if value > 0), len(team_goals_2h)),
        "team_over15_pct": _pct(sum(1 for value in team_goals if value > 1), len(team_goals)),
        "team_over25_pct": _pct(sum(1 for value in team_goals if value > 2), len(team_goals)),
        "team_cards_samples": len(team_cards),
        "avg_team_cards": round(mean(team_cards), 2) if team_cards else None,
        "avg_team_cards_1h": round(mean(team_cards_1h), 2) if team_cards_1h else None,
        "avg_team_cards_2h": round(mean(team_cards_2h), 2) if team_cards_2h else None,
        "team_cards_1h_samples": len(team_cards_1h),
        "team_cards_2h_samples": len(team_cards_2h),
        "avg_team_corners_1h": round(mean(team_corners_1h), 2) if team_corners_1h else None,
        "avg_team_corners_2h": round(mean(team_corners_2h), 2) if team_corners_2h else None,
        "team_corners_1h_samples": len(team_corners_1h),
        "team_corners_2h_samples": len(team_corners_2h),
        "team_offsides_samples": len(team_offsides),
        "avg_team_offsides": round(mean(team_offsides), 2) if team_offsides else None,
        "team_shots_samples": len(team_shots),
        "avg_team_shots": round(mean(team_shots), 2) if team_shots else None,
        "avg_team_shots_1h": round(mean(team_shots_1h), 2) if team_shots_1h else None,
        "avg_team_shots_2h": round(mean(team_shots_2h), 2) if team_shots_2h else None,
        "team_shots_1h_samples": len(team_shots_1h),
        "team_shots_2h_samples": len(team_shots_2h),
        "team_shots_on_target_samples": len(team_shots_on_target),
        "avg_team_shots_on_target": round(mean(team_shots_on_target), 2) if team_shots_on_target else None,
        "avg_team_shots_on_target_1h": round(mean(team_shots_on_target_1h), 2) if team_shots_on_target_1h else None,
        "avg_team_shots_on_target_2h": round(mean(team_shots_on_target_2h), 2) if team_shots_on_target_2h else None,
        "team_shots_on_target_1h_samples": len(team_shots_on_target_1h),
        "team_shots_on_target_2h_samples": len(team_shots_on_target_2h),
        "team_fouls_samples": len(team_fouls),
        "avg_team_fouls": round(mean(team_fouls), 2) if team_fouls else None,
        "throw_ins_samples": len(throw_ins),
        "avg_throw_ins": round(mean(throw_ins), 2) if throw_ins else None,
        "throw_in_lines": {
            str(line): _pct(sum(1 for total_throw_ins in throw_ins if total_throw_ins > line), len(throw_ins))
            for line in (29.5, 34.5, 39.5)
        } if throw_ins else {},
        "offsides_samples": len(offsides),
        "avg_offsides": round(mean(offsides), 2) if offsides else None,
        "shots_samples": len(shots),
        "avg_shots": round(mean(shots), 2) if shots else None,
        "shots_on_target_samples": len(shots_on_target),
        "avg_shots_on_target": round(mean(shots_on_target), 2) if shots_on_target else None,
        "fouls_samples": len(fouls),
        "avg_fouls": round(mean(fouls), 2) if fouls else None,
        "history_values": {
            "goal_ht": history_rows(lambda item: item.get("goals_ht")),
            "goals_2h": history_rows(lambda item: item.get("goals_2h")),
            "corners_1h": history_rows(lambda item: item.get("corners_ht")),
            "corners_2h": history_rows(lambda item: item.get("corners_2h")),
            "cards_1h": history_rows(lambda item: None if item.get("yellow_cards_ht") is None else int(item.get("yellow_cards_ht") or 0) + int(item.get("red_cards_ht") or 0)),
            "cards_2h": history_rows(lambda item: None if item.get("yellow_cards_2h") is None else int(item.get("yellow_cards_2h") or 0) + int(item.get("red_cards_2h") or 0)),
            "shots_1h": history_rows(lambda item: int(item.get("on_target_events_ht") or 0) + int(item.get("off_target_events_ht") or 0) if item.get("on_target_events_ht") is not None else None),
            "shots_2h": history_rows(lambda item: int(item.get("on_target_events_2h") or 0) + int(item.get("off_target_events_2h") or 0) if item.get("on_target_events_2h") is not None else None),
            "on_target_1h": history_rows(lambda item: item.get("on_target_events_ht")),
            "on_target_2h": history_rows(lambda item: item.get("on_target_events_2h")),
            "over15": history_rows(lambda item: item.get("total"), lambda item: (item.get("home"), item.get("away"))),
            "over25": history_rows(lambda item: item.get("total"), lambda item: (item.get("home"), item.get("away"))),
            "corners_avg": history_rows(lambda item: None if item.get("corners_ht") is None else int(item.get("corners_ht") or 0) + int(item.get("corners_2h") or 0), lambda item: (item.get("corners_home"), item.get("corners_away"))),
            "corners_10_over1": history_rows(lambda item: item.get("corners_10")),
            "team_corners_avg": history_rows(team_corner_value),
            "cards_avg": history_rows(card_total, lambda item: (item.get("cards_home"), item.get("cards_away"))),
            "offsides_avg": history_rows(lambda item: item.get("offsides_total"), lambda item: (item.get("offsides_home"), item.get("offsides_away"))),
            "shots_avg": history_rows(lambda item: item.get("shots_total"), lambda item: (item.get("shots_home"), item.get("shots_away"))),
            "shots_on_target_avg": history_rows(lambda item: item.get("shots_on_target_total"), lambda item: (item.get("shots_on_target_home"), item.get("shots_on_target_away"))),
            "fouls_avg": history_rows(lambda item: item.get("fouls_total"), lambda item: (item.get("fouls_home"), item.get("fouls_away"))),
            "team_goal_ht": history_rows(lambda item: team_side_value(item, "goals_ht_home", "goals_ht_away")),
            "team_goals_2h": history_rows(lambda item: team_side_value(item, "goals_2h_home", "goals_2h_away")),
            "team_corners_1h": history_rows(lambda item: team_side_value(item, "corners_ht_home", "corners_ht_away")),
            "team_corners_2h": history_rows(lambda item: team_side_value(item, "corners_2h_home", "corners_2h_away")),
            "team_cards_1h": history_rows(lambda item: team_side_value(item, "cards_ht_home", "cards_ht_away")),
            "team_cards_2h": history_rows(lambda item: team_side_value(item, "cards_2h_home", "cards_2h_away")),
            "team_shots_1h": history_rows(lambda item: team_side_value(item, "shots_ht_home", "shots_ht_away")),
            "team_shots_2h": history_rows(lambda item: team_side_value(item, "shots_2h_home", "shots_2h_away")),
            "team_on_target_1h": history_rows(lambda item: team_side_value(item, "on_target_ht_home", "on_target_ht_away")),
            "team_on_target_2h": history_rows(lambda item: team_side_value(item, "on_target_2h_home", "on_target_2h_away")),
            "team_goals": history_rows(lambda item: team_side_value(item, "home", "away")),
            "team_cards": history_rows(lambda item: team_side_value(item, "cards_home", "cards_away")),
            "team_offsides": history_rows(lambda item: team_side_value(item, "offsides_home", "offsides_away")),
            "team_shots": history_rows(lambda item: team_side_value(item, "shots_home", "shots_away")),
            "team_shots_on_target": history_rows(lambda item: team_side_value(item, "shots_on_target_home", "shots_on_target_away")),
            "team_fouls": history_rows(lambda item: team_side_value(item, "fouls_home", "fouls_away")),
        },
    }


def _group_analysis(label: str, items: list[dict], target_team: str | None = None) -> dict:
    summary = summarize_history(items) or {}
    phase = _phase_metrics(items, target_team)
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
        _group_analysis("Mandante", history_data.get("home", []), getattr(alert, "home_team", None)),
        _group_analysis("Visitante", history_data.get("away", []), getattr(alert, "away_team", None)),
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
