"""Daily prospective telemetry built only from already cached pre-match data."""

import json
import time
from datetime import datetime

from app.extensions import db
from app.models import PredictionRun
from app.services.backtesting.legacy_replay import evaluate_legacy
from app.services.backtesting.walk_forward import MARKETS, _bases
from app.services.matchday import get_matchday, load_matchday_summary_cache
from app.services.predictions import (append_prediction_candidates,
                                      finish_prediction_run,
                                      start_prediction_run)
from app.services.qualplacar_odds import attach_qualplacar_odds, bookmaker_selection_odds
from app.utils.time import now_sp


def _compact_groups(groups):
    compact = {}
    allowed_history = {
        "over15", "over25", "goal_ht", "corners_avg", "team_corners_avg", "corners_1h",
        "cards_avg", "team_cards", "shots_avg", "team_shots", "shots_on_target_avg",
        "team_shots_on_target", "offsides_avg", "team_offsides", "fouls_avg", "team_fouls",
        "team_goals", "team_goals_home", "team_goals_away", "team_corners_home", "team_corners_away",
    }
    allowed_row = {
        "value", "home_value", "away_value", "home_external_id", "away_external_id",
        "external_id", "kickoff_at", "kickoff_timezone", "historical_date_available", "league", "source",
    }
    for name in ("H2H", "Mandante", "Visitante"):
        source = groups.get(name) or {}
        history = {}
        for key, rows in (source.get("history_values") or {}).items():
            if key in allowed_history and isinstance(rows, list):
                history[key] = [
                    {field: row.get(field) for field in allowed_row if field in row}
                    if isinstance(row, dict) else row for row in rows[:6]
                ]
        compact[name] = {"count": source.get("count"), "samples": source.get("samples"),
                         "history_values": history}
    return compact


def _candidate(match, spec, snapshot):
    bases = _bases(snapshot, spec)
    legacy = evaluate_legacy(bases, spec.line, spec.scope)
    source_stats = {}
    for key, values in bases.items():
        hits = sum(value > spec.line for value in values)
        source_stats[key] = {"samples": len(values), "hits": hits,
                             "raw": hits / len(values) * 100 if values else None}
    return {
        "fixtureId": str(match["game_id"]), "competitionName": match.get("league") or "",
        "homeTeam": match.get("home_team") or "", "awayTeam": match.get("away_team") or "",
        "kickoffAt": f"{match['day']}T{match.get('time') or '00:00'}",
        "marketType": spec.market_type, "marketGroup": spec.market_type,
        "scope": spec.scope, "direction": "over", "line": spec.line,
        "status": legacy["status"], "rejectionReasons": [],
        "rawProbability": legacy["raw_probability"],
        "adjustedProbability": legacy["adjusted_probability"],
        "confidenceScore": legacy["confidence_score"], "sourceStats": source_stats,
    }


def _odds_key(candidate):
    market = candidate.get("marketType")
    if market in {"over15", "over25"}:
        return market
    if market == "goal_ht":
        return "goals_1h_total"
    if market == "corners" and candidate.get("scope") == "total":
        return "corners_avg"
    return None


def collect_cached_prospective_shadow(day, sample_limit=6):
    started = time.perf_counter()
    existing = PredictionRun.query.filter_by(
        algorithm="daily_shadow_observer", run_type="PROSPECTIVE_SHADOW", target_date=day,
    ).order_by(PredictionRun.id.desc()).first()
    if existing and existing.status == "completed":
        return existing
    agenda = get_matchday(day, force_refresh=False)
    matches = list(agenda.get("matches") or [])
    odds_fixture_matches = attach_qualplacar_odds(matches, day)
    now = now_sp()
    eligible = []
    snapshots = {}
    for match in matches:
        try:
            kickoff = datetime.fromisoformat(f"{match['day']}T{match.get('time') or '00:00'}")
        except (TypeError, ValueError, KeyError):
            continue
        if kickoff <= now:
            continue
        summary = load_matchday_summary_cache(day, str(match.get("game_id") or ""), sample_limit)
        groups = (summary or {}).get("groups")
        if not isinstance(groups, dict):
            continue
        snapshot = {
            "cache_schema_version": 2, "captured_at": now.isoformat(),
            "fixture": {"fixture_id": str(match["game_id"]), "target_date": day,
                        "competition": match.get("league"), "home_team": match.get("home_team"),
                        "away_team": match.get("away_team")},
            "groups": _compact_groups(groups),
        }
        eligible.append(match); snapshots[str(match["game_id"])] = snapshot
    run = existing
    if run is None:
        run = start_prediction_run(None, {
            "target_date": day, "fixture_count": len(eligible),
            "parameters": {"analysis_metadata": {"prospective_daily": True, "sample_limit": sample_limit}},
        })
        run.algorithm = "daily_shadow_observer"
    run.fixture_count = len(eligible)
    db.session.commit()
    candidates = [
        _candidate(match, spec, snapshots[str(match["game_id"])])
        for match in eligible for spec in MARKETS
    ]
    candidates_by_fixture = {}
    for candidate in candidates:
        candidates_by_fixture.setdefault(candidate["fixtureId"], []).append(candidate)
    odds_supported = odds_mapped = odds_rejected = 0
    match_by_id = {str(match["game_id"]): match for match in eligible}
    for fixture_id, fixture_candidates in candidates_by_fixture.items():
        selections = []
        by_selection = {}
        for index, candidate in enumerate(fixture_candidates):
            market_key = _odds_key(candidate)
            if not market_key:
                continue
            selection_id = f"{fixture_id}:{index}"
            selections.append({"id": selection_id, "marketKey": market_key,
                               "selectedLine": candidate.get("line"),
                               "direction": candidate.get("direction")})
            by_selection[selection_id] = candidate
        odds_supported += len(selections)
        if not selections:
            continue
        prices = bookmaker_selection_odds(match_by_id[fixture_id], day, selections)
        for selection in selections:
            candidate_prices = prices.get(selection["id"]) or []
            if candidate_prices:
                by_selection[selection["id"]]["bookmakerOdds"] = candidate_prices
                odds_mapped += 1
            else:
                odds_rejected += 1
    for offset in range(0, len(candidates), 200):
        batch = candidates[offset:offset + 200]
        fixture_ids = {candidate["fixtureId"] for candidate in batch}
        append_prediction_candidates(run, {
            "candidates": batch,
            "fixture_snapshots": {fixture_id: snapshots[fixture_id] for fixture_id in fixture_ids},
        })
    run = finish_prediction_run(run)
    run.operational_metrics_json = json.dumps({
        "fixtures_found": len(matches), "fixtures_analyzed": len(eligible),
        "candidates_created": len(candidates),
        "candidates_without_v2": None,
        "source_errors": int(bool(agenda.get("error"))),
        "odds_fixture_matches": odds_fixture_matches,
        "odds_supported_candidates": odds_supported,
        "odds_mapped": odds_mapped, "odds_rejected": odds_rejected,
        "duration_ms": int((time.perf_counter() - started) * 1000),
        "official_snapshot_policy": "FIRST_COMPLETE_DAILY_CACHE_AFTER_03H_PREWARM_BEFORE_KICKOFF",
    }, ensure_ascii=False, separators=(",", ":"))
    shadow_total = run.predictions and sum(row.model_version == "greenhunter_v2_shadow" for row in run.predictions)
    shadow_missing = sum(row.model_version == "greenhunter_v2_shadow" and row.v2_statistical_score is None
                         for row in run.predictions)
    metrics = json.loads(run.operational_metrics_json)
    metrics["candidates_without_v2"] = shadow_missing
    run.operational_metrics_json = json.dumps(metrics, ensure_ascii=False, separators=(",", ":"))
    run.alert_status = "ZERO_FIXTURES" if not eligible else "PARTIAL" if len(eligible) < len(matches) else "OK"
    db.session.commit()
    return run
