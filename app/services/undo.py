import json
import os
import secrets
from datetime import datetime, timedelta

from sqlalchemy import and_

from app.extensions import db
from app.models import MatchAlert, Rule, RuleCondition, RuleOutcomeCondition, UndoAction
from app.utils.time import now_sp

UNDO_TTL_SECONDS = int(os.environ.get("UNDO_TTL_SECONDS", "300"))


def _serialize_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _column_data(instance, skip: set[str] | None = None) -> dict:
    skip = skip or set()
    data = {}
    for col in instance.__table__.columns:
        if col.name in skip:
            continue
        data[col.name] = _serialize_value(getattr(instance, col.name))
    return data


def _to_datetime_if_needed(model_cls, col_name: str, value):
    if value is None:
        return None
    col = model_cls.__table__.columns.get(col_name)
    if col is None:
        return value
    if col.type.__class__.__name__ == "DateTime" and isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


def _build_instance(model_cls, raw_data: dict):
    payload = {}
    for col in model_cls.__table__.columns:
        name = col.name
        if name not in raw_data:
            continue
        payload[name] = _to_datetime_if_needed(model_cls, name, raw_data[name])
    return model_cls(**payload)


def snapshot_alert(alert: MatchAlert) -> dict:
    return _column_data(alert)


def snapshot_rule(rule: Rule) -> dict:
    return {
        "rule": _column_data(rule),
        "conditions": [_column_data(c) for c in (rule.conditions or [])],
        "outcome_conditions": [_column_data(c) for c in (rule.outcome_conditions or [])],
        "alerts": [_column_data(a) for a in (rule.alerts or [])],
    }


def create_undo_action(user_id: int, action_type: str, payload: dict, ttl_seconds: int | None = None) -> str:
    now = now_sp()
    ttl = ttl_seconds if isinstance(ttl_seconds, int) and ttl_seconds > 0 else UNDO_TTL_SECONDS
    token = secrets.token_urlsafe(24)
    action = UndoAction(
        token=token,
        user_id=user_id,
        action_type=action_type,
        payload_json=json.dumps(payload, ensure_ascii=False),
        created_at=now,
        expires_at=now + timedelta(seconds=ttl),
    )
    db.session.add(action)
    _purge_old_actions(user_id)
    return token


def _purge_old_actions(user_id: int):
    cutoff = now_sp() - timedelta(days=2)
    UndoAction.query.filter(
        and_(
            UndoAction.user_id == user_id,
            (UndoAction.expires_at < now_sp()) | (UndoAction.created_at < cutoff),
        )
    ).delete(synchronize_session=False)


def apply_undo(token: str, user_id: int) -> tuple[bool, str]:
    action = (
        UndoAction.query.filter_by(token=token, user_id=user_id)
        .filter(UndoAction.used_at.is_(None))
        .first()
    )
    if not action:
        return False, "Undo invalido ou ja utilizado."
    if action.expires_at < now_sp():
        return False, "Undo expirado."

    try:
        payload = json.loads(action.payload_json or "{}")
    except Exception:
        payload = {}

    try:
        restored = 0
        if action.action_type == "delete_rule":
            restored = _restore_rule(payload.get("rule"))
        elif action.action_type in ("delete_alert", "delete_selected_alerts"):
            restored = _restore_alerts(payload.get("alerts") or [])
        else:
            return False, "Tipo de undo nao suportado."
        action.used_at = now_sp()
        db.session.commit()
        if restored <= 0:
            return False, "Nada para restaurar."
        return True, "Acao desfeita com sucesso."
    except Exception:
        db.session.rollback()
        return False, "Nao foi possivel desfazer a acao."


def _restore_rule(rule_snapshot: dict | None) -> int:
    if not isinstance(rule_snapshot, dict):
        return 0
    rule_data = rule_snapshot.get("rule") or {}
    if not isinstance(rule_data, dict):
        return 0

    existing = Rule.query.get(rule_data.get("id"))
    if existing:
        return 0

    rule = _build_instance(Rule, rule_data)
    db.session.add(rule)
    db.session.flush()

    for cond_data in (rule_snapshot.get("conditions") or []):
        if not isinstance(cond_data, dict):
            continue
        cond_data = dict(cond_data)
        cond_data["rule_id"] = rule.id
        cond = _build_instance(RuleCondition, cond_data)
        cond.id = None
        db.session.add(cond)

    for cond_data in (rule_snapshot.get("outcome_conditions") or []):
        if not isinstance(cond_data, dict):
            continue
        cond_data = dict(cond_data)
        cond_data["rule_id"] = rule.id
        cond = _build_instance(RuleOutcomeCondition, cond_data)
        cond.id = None
        db.session.add(cond)

    _restore_alerts(rule_snapshot.get("alerts") or [], forced_rule_id=rule.id)
    return 1


def _restore_alerts(alert_snapshots: list[dict], forced_rule_id: int | None = None) -> int:
    restored = 0
    for alert_data in alert_snapshots:
        if not isinstance(alert_data, dict):
            continue
        payload = dict(alert_data)
        if forced_rule_id is not None:
            payload["rule_id"] = forced_rule_id
        existing = MatchAlert.query.get(payload.get("id"))
        if existing:
            continue
        alert = _build_instance(MatchAlert, payload)
        db.session.add(alert)
        restored += 1
    return restored
