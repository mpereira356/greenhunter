"""Idempotent settlement and reporting for prospective shadow observations."""

from datetime import datetime, timedelta

from app.extensions import db
from app.models import HistoricalMatch, MarketPrediction, PredictionRun
from app.services.backtesting.walk_forward import _metric, _stat_maps
from app.utils.time import now_sp


FINAL_STATES = {"GREEN", "RED", "VOID"}


def _family(prediction):
    market = str(prediction.market_type or "").casefold()
    group = str(prediction.market_group or market).casefold()
    if market == "goal_ht": return "goals_first_half", "first_half"
    if market.startswith("over") or group.startswith("team_goals"): return "goals", "full_time"
    for family in ("shots_on_target", "corners", "cards", "shots", "offsides", "fouls"):
        if family in group or family in market:
            return family, "first_half" if "_1h" in group else "full_time"
    return None, "full_time"


def _kickoff(value):
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00")).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


def settle_one(prediction, match, stats, checked_at=None):
    """Settle once; absence of a trustworthy value is never RED."""
    if prediction.settlement_status in FINAL_STATES:
        return prediction.settlement_status
    prediction.settlement_attempts = int(prediction.settlement_attempts or 0) + 1
    status = str(match.status or "").casefold() if match else ""
    if status in {"cancelled", "canceled", "postponed", "abandoned", "void"}:
        prediction.settlement_status = "VOID"
    elif not match or match.home_score is None or match.away_score is None:
        return "PENDING"
    else:
        family, period = _family(prediction)
        side = prediction.scope if prediction.scope in {"home", "away"} else "total"
        value = _metric(match, stats, family, side, period) if family else None
        if value is None or prediction.line is None:
            prediction.settlement_status = "UNRESOLVED"
        else:
            direction = str(prediction.direction or "over").casefold()
            hit = value < prediction.line if direction == "under" else value > prediction.line
            prediction.actual_value = value
            prediction.settlement_status = "GREEN" if hit else "RED"
    prediction.settled_at = checked_at or now_sp()
    return prediction.settlement_status


def settle_prospective_predictions(target_date=None, checked_at=None):
    checked_at = checked_at or now_sp()
    query = MarketPrediction.query.join(MarketPrediction.run).filter(
        MarketPrediction.settlement_status.in_(("PENDING", "UNRESOLVED")),
        PredictionRun.run_type == "PROSPECTIVE_SHADOW",
        MarketPrediction.prospective_validity == "VALID",
    )
    if target_date:
        query = query.filter(MarketPrediction.target_date == target_date)
    predictions = query.all()
    fixture_ids = {row.fixture_id for row in predictions}
    matches = HistoricalMatch.query.filter(
        HistoricalMatch.source == "betsapi", HistoricalMatch.external_id.in_(fixture_ids)
    ).all() if fixture_ids else []
    by_fixture = {row.external_id: row for row in matches}
    stats = _stat_maps([row.id for row in matches])
    counts = {state: 0 for state in ("PENDING", "GREEN", "RED", "VOID", "UNRESOLVED")}
    for prediction in predictions:
        match = by_fixture.get(prediction.fixture_id)
        state = settle_one(prediction, match, stats, checked_at)
        # A fixture absent long after kickoff is unresolved, never a loss.
        kickoff = _kickoff(prediction.kickoff_at)
        if state == "PENDING" and not match and kickoff and checked_at > kickoff + timedelta(hours=36):
            prediction.settlement_status = "UNRESOLVED"
            prediction.settled_at = checked_at
            state = "UNRESOLVED"
        counts[state] += 1
    db.session.commit()
    return counts


def daily_shadow_report(target_date):
    rows = MarketPrediction.query.join(MarketPrediction.run).filter(
        MarketPrediction.target_date == target_date,
        PredictionRun.run_type == "PROSPECTIVE_SHADOW",
    ).all()
    legacy = [row for row in rows if row.model_version == "legacy_v1"]
    shadow = [row for row in rows if row.model_version == "greenhunter_v2_shadow"]

    def profile(name):
        selected = [row for row in shadow if getattr(row, name)]
        green = sum(row.settlement_status == "GREEN" for row in selected)
        red = sum(row.settlement_status == "RED" for row in selected)
        resolved = green + red
        return {"selected": len(selected), "green": green, "red": red,
                "hit_rate": round(green / resolved * 100, 2) if resolved else None}

    return {
        "date": target_date, "legacy_evaluated": len(legacy), "v2_evaluated": len(shadow),
        "pending": sum(row.settlement_status == "PENDING" for row in shadow),
        "settled": sum(row.settlement_status in FINAL_STATES | {"UNRESOLVED"} for row in shadow),
        "SHADOW_CONSERVATIVE": profile("selected_conservative"),
        "SHADOW_BALANCED": profile("selected_balanced"),
    }
