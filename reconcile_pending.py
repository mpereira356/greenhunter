import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

os.environ.setdefault("DISABLE_WORKER", "1")

from app import create_app
from app.extensions import db
from app.models import LiveGameState, MatchAlert
from app.services.scraper import (
    _archived_full_time_snapshot,
    _event_timeline_snapshot,
    _normalize_team_name,
    fetch_match_stats,
    make_session,
)
from app.services.worker import (
    _event_metrics_json,
    _events_to_json,
    apply_alert_delta,
    evaluate_outcome_conditions,
    merge_score_delta_into_stats,
    parse_score,
    stats_to_json,
)
from app.utils.time import now_sp


def _fetch(item):
    payload = fetch_match_stats(make_session(), item["url"])
    return item["id"], payload


def _local_payload(alert):
    state = LiveGameState.query.filter_by(game_id=alert.game_id).first()
    if not state or not state.events_json:
        return None
    try:
        events = json.loads(state.events_json)
    except (TypeError, ValueError):
        return None
    snapshot = _archived_full_time_snapshot(events, alert.home_team, alert.away_team)
    if not snapshot and isinstance(alert.last_score_minute, int) and alert.last_score_minute >= 90:
        snapshot = _event_timeline_snapshot(events, alert.home_team, alert.away_team)
        snapshot["minute"] = alert.last_score_minute
        snapshot["time_text"] = "FT"
    elif not snapshot:
        return None
    stats = snapshot["stats"]
    stats["Minute"] = {"home": 90, "away": 90, "total": 90}
    return {
        "score": snapshot["score"],
        "time_text": "FT",
        "minute": 90,
        "stats": stats,
        "events": events,
    }


def _event_result_minute(alert, payload, green_conditions):
    baseline = json.loads(alert.initial_stats_json or "{}")
    cumulative = {
        "Goals": {"home": 0, "away": 0, "total": 0},
        "Corners": {"home": 0, "away": 0, "total": 0},
    }
    home_norm = _normalize_team_name(alert.home_team)
    away_norm = _normalize_team_name(alert.away_team)
    for event in payload.get("events") or []:
        kind = event.get("kind")
        team = event.get("team")
        if kind == "goal":
            key = "Goals"
        elif kind == "corner":
            key = "Corners"
        else:
            continue
        team_norm = _normalize_team_name(team)
        if team_norm and team_norm == home_norm:
            cumulative[key]["home"] += 1
        elif team_norm and team_norm == away_norm:
            cumulative[key]["away"] += 1
        cumulative[key]["total"] = cumulative[key]["home"] + cumulative[key]["away"]

        minute = event.get("minute")
        if not isinstance(minute, int) or minute < (alert.alert_minute or 0):
            continue
        delta = apply_alert_delta(cumulative, baseline, minute, alert.alert_minute)
        current_score = f"{cumulative['Goals']['home']} x {cumulative['Goals']['away']}"
        delta = merge_score_delta_into_stats(delta, alert.initial_score, current_score)
        if evaluate_outcome_conditions(green_conditions, delta):
            return minute, current_score
    return 90, payload.get("score") or alert.last_score or alert.initial_score


def _resolve(alert, payload):
    if not payload or payload.get("time_text") != "FT" or payload.get("minute") != 90:
        return None
    final_score = payload.get("score") or alert.last_score or alert.initial_score
    final_stats = payload.get("stats") or {}
    baseline = json.loads(alert.initial_stats_json or "{}")
    outcome_stats = apply_alert_delta(final_stats, baseline, 90, alert.alert_minute)
    outcome_stats = merge_score_delta_into_stats(outcome_stats, alert.initial_score, final_score)
    green_conditions = [c for c in alert.rule.outcome_conditions if c.outcome_type == "green"]
    is_green = evaluate_outcome_conditions(green_conditions, outcome_stats)
    result_minute, result_score = (
        _event_result_minute(alert, payload, green_conditions) if is_green else (90, final_score)
    )
    return {
        "status": "green" if is_green else "red",
        "result_minute": result_minute,
        "result_score": result_score,
        "final_score": final_score,
        "stats": final_stats,
        "events": payload.get("events"),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--network", action="store_true")
    args = parser.parse_args()

    app = create_app()
    payloads = {}
    with app.app_context():
        pending = MatchAlert.query.filter_by(status="pending").order_by(MatchAlert.id).all()
        items = [{"id": alert.id, "url": alert.url} for alert in pending]
        if not args.network:
            payloads = {alert.id: _local_payload(alert) for alert in pending}

    if args.network:
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
            futures = {executor.submit(_fetch, item): item for item in items}
            for index, future in enumerate(as_completed(futures), 1):
                item = futures[future]
                try:
                    alert_id, payload = future.result()
                    payloads[alert_id] = payload
                except Exception as exc:
                    print(f"fetch_error id={item['id']} error={exc}")
                if index % 25 == 0 or index == len(items):
                    print(f"fetched={index}/{len(items)}")

    counts = {"green": 0, "red": 0, "unresolved": 0}
    unresolved = []
    with app.app_context():
        for index, alert in enumerate(
            MatchAlert.query.filter_by(status="pending").order_by(MatchAlert.id).all(), 1
        ):
            result = _resolve(alert, payloads.get(alert.id))
            if not result:
                counts["unresolved"] += 1
                unresolved.append(alert.id)
                continue
            counts[result["status"]] += 1
            if not args.apply:
                continue
            alert.status = result["status"]
            alert.result_minute = result["result_minute"]
            alert.result_time_hhmm = now_sp().strftime("%H:%M")
            alert.ht_score = result["result_score"]
            alert.ht_stats_json = stats_to_json(result["stats"])
            alert.result_events_json = _events_to_json(result["events"])
            alert.result_event_metrics_json = _event_metrics_json(result["events"], alert.alert_minute)
            alert.last_score = result["final_score"]
            alert.last_score_minute = 90
            alert.ft_score = result["final_score"]
            alert.ft_stats_json = stats_to_json(result["stats"])
            alert.ft_events_json = _events_to_json(result["events"])
            alert.ft_event_metrics_json = _event_metrics_json(result["events"], alert.alert_minute)
            alert.ft_completed = True
            if index % 25 == 0:
                db.session.commit()
        if args.apply:
            db.session.commit()

    print(json.dumps({"counts": counts, "unresolved_ids": unresolved}, ensure_ascii=False))


if __name__ == "__main__":
    main()
