import json
from datetime import datetime

from sqlalchemy import func, or_

from ..extensions import db
from ..models import Rule, RuleCondition, RuleOutcomeCondition, SavedTicket, SavedTicketLeg, User
from ..utils.time import now_sp


RULE_FIELDS = (
    "name", "time_limit_min", "message_template", "second_half_only", "follow_ht", "follow_ft",
    "outcome_green_stage", "outcome_red_stage", "outcome_green_minute", "outcome_red_minute",
    "outcome_red_if_no_green", "notify_telegram", "alert_on_penalty", "score_home", "score_away",
    "allowed_leagues_json",
)
LEG_FIELDS = (
    "game_id", "game_day", "game_time", "league", "home_team", "away_team", "market_key",
    "market_label", "target_side", "target_line", "samples", "source_group", "predicted_probability",
    "confidence_score", "context_score", "data_quality_score", "consistency_score", "prediction_json",
    "individual_odd",
)


def find_recipient(raw, sender_id):
    value = str(raw or "").strip()
    if not value:
        return None
    query = User.query
    if value.isdigit():
        query = query.filter(or_(User.id == int(value), func.lower(User.username) == value.casefold()))
    else:
        query = query.filter(func.lower(User.username) == value.casefold())
    return query.filter(User.id != sender_id).first()


def rule_snapshot(rule):
    return {
        "rule": {field: getattr(rule, field) for field in RULE_FIELDS},
        "conditions": [{"stat_key": c.stat_key, "side": c.side, "operator": c.operator,
                        "value": c.value, "group_id": c.group_id} for c in rule.conditions],
        "outcomes": [{"outcome_type": c.outcome_type, "stat_key": c.stat_key, "side": c.side,
                      "operator": c.operator, "value": c.value, "group_id": c.group_id}
                     for c in rule.outcome_conditions],
    }


def accept_rule_snapshot(payload, user_id):
    values = {field: (payload.get("rule") or {}).get(field) for field in RULE_FIELDS}
    values["name"] = f"{str(values.get('name') or 'Regra')[:100]} (compartilhada)"
    rule = Rule(user_id=user_id, is_active=False, **values)
    db.session.add(rule); db.session.flush()
    for row in payload.get("conditions") or []:
        db.session.add(RuleCondition(rule_id=rule.id, **row))
    for row in payload.get("outcomes") or []:
        db.session.add(RuleOutcomeCondition(rule_id=rule.id, **row))
    return rule


def _kickoff(leg):
    day, clock = str(leg.game_day or ""), str(leg.game_time or "")[:5]
    try:
        return datetime.fromisoformat(f"{day}T{clock}")
    except (TypeError, ValueError):
        return None


def ticket_is_editable(ticket, now=None):
    now = (now or now_sp()).replace(tzinfo=None)
    return bool(ticket.status == "pending" and ticket.legs and all(
        leg.status == "pending" and _kickoff(leg) is not None and _kickoff(leg) > now
        for leg in ticket.legs
    ))


def ticket_is_shareable(ticket, now=None):
    return ticket_is_editable(ticket, now)


def ticket_snapshot(ticket):
    return {"ticket": {"name": ticket.name, "total_odd": ticket.total_odd,
                       "stake_amount": ticket.stake_amount},
            "legs": [{field: getattr(leg, field) for field in LEG_FIELDS} for leg in ticket.legs]}


def ticket_snapshot_is_pregame(payload, now=None):
    now = (now or now_sp()).replace(tzinfo=None)
    rows = payload.get("legs") or []
    for row in rows:
        try: kickoff = datetime.fromisoformat(f"{row.get('game_day')}T{str(row.get('game_time') or '')[:5]}")
        except (TypeError, ValueError): return False
        if kickoff <= now: return False
    return bool(rows)


def accept_ticket_snapshot(payload, user_id):
    header = payload.get("ticket") or {}
    ticket = SavedTicket(user_id=user_id,
        name=f"{str(header.get('name') or 'Bilhete')[:60]} (compartilhado)",
        total_odd=float(header.get("total_odd") or 1), stake_amount=float(header.get("stake_amount") or 0),
        status="pending", profit=0)
    db.session.add(ticket); db.session.flush()
    for row in payload.get("legs") or []:
        db.session.add(SavedTicketLeg(ticket_id=ticket.id, status="pending",
                                     **{field: row.get(field) for field in LEG_FIELDS}))
    return ticket


def encode_snapshot(payload):
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
