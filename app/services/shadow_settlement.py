"""Auditable settlement of frozen prospective predictions from local evidence."""
import json
import re
from datetime import datetime, timedelta

from app.extensions import db
from app.models import HistoricalMatch, LiveGameState, MarketPrediction, PredictionRun
from app.services.backtesting.walk_forward import _metric, _stat_maps
from app.utils.time import now_sp

FINAL_STATES = {"GREEN", "RED", "VOID"}


def _family(p):
    market, group = str(p.market_type or "").casefold(), str(p.market_group or p.market_type or "").casefold()
    if market == "goal_ht": return "goals_first_half", "first_half"
    if market.startswith("over") or group.startswith("team_goals"): return "goals", "full_time"
    for family in ("shots_on_target", "corners", "cards", "shots", "offsides", "fouls"):
        if family in group or family in market:
            return family, "first_half" if "_1h" in group or "_1h" in market else "full_time"
    return None, "full_time"


def _kickoff(value):
    try: return datetime.fromisoformat(str(value or "").replace("Z", "+00:00")).replace(tzinfo=None)
    except (TypeError, ValueError): return None


def _json(value, fallback):
    try:
        parsed = json.loads(value or "")
        return parsed if isinstance(parsed, type(fallback)) else fallback
    except (TypeError, ValueError): return fallback


def _score(value):
    found = re.search(r"(?<!\d)(\d+)\s*(?:x|[-:])\s*(\d+)(?!\d)", str(value or ""), re.I)
    return (int(found.group(1)), int(found.group(2))) if found else (None, None)


def _finished(state, prediction, checked_at):
    text = str(state.time_text or "").strip().casefold()
    if text in {"ft", "finished", "ended", "encerrado", "ap"} or "full time" in text: return True
    kickoff = _kickoff(prediction.kickoff_at)
    return bool(kickoff and checked_at.date() > kickoff.date() and int(state.minute or 0) >= 90)


def _stat(stats, aliases, side):
    for alias in aliases:
        row = stats.get(alias)
        if not isinstance(row, dict): continue
        try:
            if row.get(side) is not None and row.get(side) != "-": return float(row[side])
            if side == "total" and row.get("home") is not None and row.get("away") is not None:
                return float(row["home"]) + float(row["away"])
        except (TypeError, ValueError): pass
    return None


def _ht_goals(events):
    for event in events:
        if not isinstance(event, dict) or event.get("kind") != "score_after_ht": continue
        found = re.search(r"first half\s*-\s*(\d+)\s*[-:]\s*(\d+)", str(event.get("text") or ""), re.I)
        if found: return float(int(found.group(1)) + int(found.group(2)))
    goals = [e for e in events if isinstance(e, dict) and e.get("kind") == "goal"
             and isinstance(e.get("minute"), int) and int(e["minute"]) <= 45]
    return float(len(goals)) if goals else None


def _live_metric(p, state, checked_at):
    family, period = _family(p)
    if not family: return None, "UNSUPPORTED_MARKET", None
    side = p.scope if p.scope in {"home", "away", "total"} else "total"
    if not state: return None, "FIXTURE_RESULT_NOT_FOUND", None
    if not _finished(state, p, checked_at): return None, "SOURCE_NOT_FINAL", "live_game_state"
    home, away = _score(state.score)
    stats, events, first = _json(state.stats_json, {}), _json(state.events_json, []), _json(state.first_half_snapshot_json, {})
    if family == "goals":
        if home is None: return None, "FINAL_SCORE_MISSING", "live_game_state"
        return float({"home": home, "away": away, "total": home + away}[side]), None, "live_game_state.final_score"
    if family == "goals_first_half":
        value = _stat(first, ("Goals",), side)
        if value is None and side == "total": value = _ht_goals(events)
        return (value, None, "live_game_state.first_half") if value is not None else (None, "FIRST_HALF_DATA_MISSING", "live_game_state")
    if family == "corners" and period == "first_half":
        value = _stat(stats, ("Corners (Half)",), side)
        if value is None: value = _stat(first, ("Corners", "Corners (Half)"), side)
        return (value, None, "live_game_state.first_half") if value is not None else (None, "FIRST_HALF_DATA_MISSING", "live_game_state")
    aliases = {
        "corners": ("Corners", "Corner Kicks", "Escanteios"),
        "shots_on_target": ("On Target", "Shots on Target", "Chutes ao Gol"),
        "fouls": ("Fouls", "Fouls Committed", "Faltas"),
        "offsides": ("Offsides", "Offside", "Impedimentos", "Foras de Jogo"),
        "shots": ("Shots", "Total Shots", "Finalizações", "Goal attempts", "Tentativas de Golo"),
    }
    if family == "cards":
        value = _stat(stats, ("Cards", "Total Cards", "Cartões"), side)
        if value is None:
            yellow = _stat(stats, ("Yellow Card", "Yellow Cards", "Cartões Amarelos"), side)
            red = _stat(stats, ("Red Card", "Red Cards", "Cartões Vermelhos"), side)
            value = None if yellow is None and red is None else (yellow or 0) + (red or 0)
    else:
        value = _stat(stats, aliases.get(family, ()), side)
        if family == "shots" and value is None:
            on = _stat(stats, ("On Target", "Shots on Target", "Chutes ao Gol"), side)
            off = _stat(stats, ("Off Target", "Shots off Target", "Chutes para Fora"), side)
            value = None if on is None or off is None else on + off
    reason = {"corners":"CORNERS_MISSING", "cards":"CARDS_MISSING", "fouls":"FOULS_MISSING",
              "offsides":"OFFSIDES_MISSING", "shots":"SHOTS_MISSING",
              "shots_on_target":"SHOTS_ON_TARGET_MISSING"}.get(family, "OTHER")
    return (value, None, "live_game_state.stats") if value is not None else (None, reason, "live_game_state")


def _audit(p, at, before, old_reason, result, reason, source):
    history = _json(p.settlement_audit_json, [])
    history.append({"attempt": p.settlement_attempts, "at": at.isoformat(), "previous_status": before,
                    "previous_reason": old_reason, "result_status": result,
                    "failure_reason": reason, "source": source})
    p.settlement_audit_json = json.dumps(history[-100:], ensure_ascii=False)


def settle_one(p, match, stats, checked_at=None, live_state=None):
    """Absence of trustworthy evidence always remains UNRESOLVED, never RED."""
    if p.settlement_status in FINAL_STATES: return p.settlement_status
    at, before, old_reason = checked_at or now_sp(), p.settlement_status, p.settlement_failure_reason
    p.settlement_attempts = int(p.settlement_attempts or 0) + 1
    p.settlement_first_attempt_at = p.settlement_first_attempt_at or at
    p.settlement_last_attempt_at = at
    match_status = str(match.status or "").casefold() if match else ""
    value = reason = source = None
    if match_status in {"cancelled", "canceled", "postponed", "abandoned", "void"}:
        result, source = "VOID", "historical_match.status"
    else:
        family, period = _family(p); side = p.scope if p.scope in {"home", "away"} else "total"
        if match and match.home_score is not None and match.away_score is not None and family:
            value = _metric(match, stats, family, side, period)
            if value is not None: source = "historical_match"
        if value is None: value, reason, source = _live_metric(p, live_state, at)
        if value is None: result = "UNRESOLVED"
        elif p.line is None: result, reason = "UNRESOLVED", "UNSUPPORTED_MARKET"
        else:
            hit = value < p.line if str(p.direction or "over").casefold() == "under" else value > p.line
            p.actual_value, result = value, "GREEN" if hit else "RED"
    p.settlement_status, p.settlement_failure_reason = result, reason if result == "UNRESOLVED" else None
    p.settlement_source, p.settled_at = source, at if result != "PENDING" else None
    _audit(p, at, before, old_reason, result, p.settlement_failure_reason, source)
    return result


def settle_prospective_predictions(target_date=None, checked_at=None):
    at = checked_at or now_sp()
    query = MarketPrediction.query.join(MarketPrediction.run).filter(
        MarketPrediction.settlement_status.in_(("PENDING", "UNRESOLVED")),
        PredictionRun.run_type == "PROSPECTIVE_SHADOW",
        MarketPrediction.model_version == "greenhunter_v2_shadow",
        MarketPrediction.prospective_validity == "VALID")
    if target_date: query = query.filter(MarketPrediction.target_date == target_date)
    predictions = query.all(); fixture_ids = {p.fixture_id for p in predictions}
    matches = HistoricalMatch.query.filter(HistoricalMatch.source == "betsapi", HistoricalMatch.external_id.in_(fixture_ids)).all() if fixture_ids else []
    states = LiveGameState.query.filter(LiveGameState.game_id.in_(fixture_ids)).all() if fixture_ids else []
    by_match, by_state = {m.external_id:m for m in matches}, {s.game_id:s for s in states}
    stats = _stat_maps([m.id for m in matches]); counts = {s:0 for s in ("PENDING","GREEN","RED","VOID","UNRESOLVED")}
    for p in predictions:
        state = settle_one(p, by_match.get(p.fixture_id), stats, at, by_state.get(p.fixture_id))
        kickoff = _kickoff(p.kickoff_at)
        if state == "UNRESOLVED" and p.settlement_failure_reason == "FIXTURE_RESULT_NOT_FOUND" and (not kickoff or at <= kickoff + timedelta(hours=36)):
            p.settlement_status, p.settled_at, state = "PENDING", None, "PENDING"
        counts[state] += 1
    db.session.commit(); return counts


def daily_shadow_report(target_date):
    rows = MarketPrediction.query.join(MarketPrediction.run).filter(MarketPrediction.target_date == target_date,
        PredictionRun.run_type == "PROSPECTIVE_SHADOW").all()
    shadow = [r for r in rows if r.model_version == "greenhunter_v2_shadow"]
    def profile(field):
        chosen = [r for r in shadow if getattr(r, field)]; counts = {s:sum(r.settlement_status == s for r in chosen) for s in ("GREEN","RED","PENDING","VOID","UNRESOLVED")}
        resolved = counts["GREEN"] + counts["RED"]
        return {"selected":len(chosen), **{k.lower():v for k,v in counts.items()}, "hit_rate":round(counts["GREEN"]/resolved*100,2) if resolved else None}
    return {"date":target_date, "legacy_evaluated":sum(r.model_version == "legacy_v1" for r in rows), "v2_evaluated":len(shadow),
            "pending":sum(r.settlement_status == "PENDING" for r in shadow), "settled":sum(r.settlement_status in FINAL_STATES|{"UNRESOLVED"} for r in shadow),
            "SHADOW_CONSERVATIVE":profile("selected_conservative"), "SHADOW_BALANCED":profile("selected_balanced")}
