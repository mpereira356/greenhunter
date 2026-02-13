import json
from datetime import datetime

from flask import Blueprint, Response, flash, jsonify, redirect, render_template, request, url_for
from flask_login import current_user, login_required
from sqlalchemy import func

from ..extensions import db
from ..models import LiveGameState, MatchAlert, Rule, RuleCondition, RuleOutcomeCondition
from ..services.evaluator import evaluate_rule, history_confidence
from ..services.scraper import (
    fetch_live_games,
    fetch_match_history,
    fetch_match_stats,
    format_history_summary,
    is_first_half_extra_time,
    make_session,
    summarize_history,
)
from ..services.worker import parse_score
from ..utils.time import now_sp

rules_bp = Blueprint("rules", __name__, url_prefix="/rules")


def _clean_league_name(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def _parse_allowed_leagues_json(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        data = json.loads(value)
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    leagues = []
    seen = set()
    for item in data:
        name = _clean_league_name(item)
        if not name:
            continue
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        leagues.append(name)
    return leagues


def _available_leagues(limit: int = 250) -> list[str]:
    leagues = set()
    for col in (LiveGameState.league, MatchAlert.league):
        try:
            rows = (
                db.session.query(col)
                .filter(col.isnot(None))
                .distinct()
                .limit(limit)
                .all()
            )
        except Exception:
            rows = []
        for (val,) in rows:
            name = _clean_league_name(val)
            if name:
                leagues.add(name)
    return sorted(leagues, key=lambda s: s.casefold())


def _parse_conditions(form):
    conditions = []
    def _normalize_operator(value: str) -> str:
        value = (value or "").strip()
        if value == "≥":
            return ">="
        if value == "≤":
            return "<="
        if value == "=":
            return "=="
        return value

    # New grouped format: group-<g>-cond-<i>-stat_key
    grouped_keys = [k for k in form.keys() if k.startswith("group-") and k.endswith("-stat_key")]
    if grouped_keys:
        for key in grouped_keys:
            parts = key.split("-")
            if len(parts) < 4:
                continue
            group_id = int(parts[1])
            index = int(parts[3])
            stat_key = form.get(f"group-{group_id}-cond-{index}-stat_key", "").strip()
            if stat_key.casefold() in ("league", "liga"):
                # League filter is stored at the Rule level (allowed_leagues_json), not as a numeric stat condition.
                continue
            side = form.get(f"group-{group_id}-cond-{index}-side", "").strip()
            if not side and stat_key.lower() in ("minute", "minutos", "minuto", "min"):
                side = "total"
            operator = _normalize_operator(form.get(f"group-{group_id}-cond-{index}-operator", ""))
            value_raw = form.get(f"group-{group_id}-cond-{index}-value", "").strip()
            if stat_key and side and operator and value_raw.isdigit():
                conditions.append(
                    RuleCondition(
                        stat_key=stat_key,
                        side=side,
                        operator=operator,
                        value=int(value_raw),
                        group_id=group_id,
                    )
                )
        return conditions

    # Legacy flat format
    index = 0
    while True:
        stat_key = form.get(f"conditions-{index}-stat_key")
        if stat_key is None:
            break
        stat_key = stat_key.strip()
        if stat_key.casefold() in ("league", "liga"):
            # League filter is stored at the Rule level (allowed_leagues_json), not as a numeric stat condition.
            index += 1
            continue
        side = form.get(f"conditions-{index}-side", "").strip()
        if not side and stat_key.lower() in ("minute", "minutos", "minuto", "min"):
            side = "total"
        operator = _normalize_operator(form.get(f"conditions-{index}-operator", ""))
        value_raw = form.get(f"conditions-{index}-value", "").strip()
        if stat_key and side and operator and value_raw.isdigit():
            conditions.append(
                RuleCondition(
                    stat_key=stat_key,
                    side=side,
                    operator=operator,
                    value=int(value_raw),
                    group_id=0,
                )
            )
        index += 1
    return conditions


def _parse_outcome_conditions(form, prefix):
    conditions = []
    def _normalize_operator(value: str) -> str:
        value = (value or "").strip()
        if value == "≥":
            return ">="
        if value == "≤":
            return "<="
        if value == "=":
            return "=="
        return value

    outcome_type = prefix.split("-")[-1]

    # New grouped format: outcome-<type>-group-<g>-cond-<i>-stat_key
    grouped_keys = [k for k in form.keys() if k.startswith(f"{prefix}-group-") and k.endswith("-stat_key")]
    if grouped_keys:
        for key in grouped_keys:
            parts = key.split("-")
            if len(parts) < 7:
                continue
            group_id = _coerce_int(parts[3], default=0)
            index = _coerce_int(parts[5], default=0)
            stat_key = form.get(f"{prefix}-group-{group_id}-cond-{index}-stat_key", "").strip()
            side = form.get(f"{prefix}-group-{group_id}-cond-{index}-side", "").strip()
            if not side and stat_key.lower() in ("minute", "minutos", "minuto", "min"):
                side = "total"
            operator = _normalize_operator(form.get(f"{prefix}-group-{group_id}-cond-{index}-operator", ""))
            value_raw = form.get(f"{prefix}-group-{group_id}-cond-{index}-value", "").strip()
            if stat_key and side and operator and value_raw.isdigit():
                conditions.append(
                    RuleOutcomeCondition(
                        outcome_type=outcome_type,
                        stat_key=stat_key,
                        side=side,
                        operator=operator,
                        value=int(value_raw),
                        group_id=group_id,
                    )
                )
        return conditions

    # Legacy flat format
    index = 0
    while True:
        stat_key = form.get(f"{prefix}-{index}-stat_key")
        if stat_key is None:
            break
        stat_key = stat_key.strip()
        side = form.get(f"{prefix}-{index}-side", "").strip()
        if not side and stat_key.lower() in ("minute", "minutos", "minuto", "min"):
            side = "total"
        operator = _normalize_operator(form.get(f"{prefix}-{index}-operator", ""))
        value_raw = form.get(f"{prefix}-{index}-value", "").strip()
        if stat_key and side and operator and value_raw.isdigit():
            conditions.append(
                RuleOutcomeCondition(
                    outcome_type=outcome_type,
                    stat_key=stat_key,
                    side=side,
                    operator=operator,
                    value=int(value_raw),
                    group_id=0,
                )
            )
        index += 1
    return conditions


def _coerce_int(value, default=None):
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _normalize_import_operator(value: str) -> str:
    value = (value or "").strip()
    if value in ("?", "???"):
        return ">="
    if value in ("?", "???"):
        return "<="
    if value == "=":
        return "=="
    if value not in (">", ">=", "<", "<=", "==", "!="):
        return ">="
    return value


def _serialize_rule(rule: Rule) -> dict:
    return {
        "name": rule.name,
        "time_limit_min": rule.time_limit_min,
        "message_template": rule.message_template,
        "is_active": rule.is_active,
        "second_half_only": rule.second_half_only,
        "notify_telegram": rule.notify_telegram,
        "alert_on_penalty": rule.alert_on_penalty,
        "follow_ht": rule.follow_ht,
        "follow_ft": rule.follow_ft,
        "outcome_green_stage": rule.outcome_green_stage,
        "outcome_red_stage": rule.outcome_red_stage,
        "outcome_green_minute": rule.outcome_green_minute,
        "outcome_red_minute": rule.outcome_red_minute,
        "outcome_red_if_no_green": rule.outcome_red_if_no_green,
        "green_allow_score_swap": bool(getattr(rule, "green_allow_score_swap", False)),
        "score_home": rule.score_home,
        "score_away": rule.score_away,
        "allowed_leagues": _parse_allowed_leagues_json(getattr(rule, "allowed_leagues_json", None)),
        "conditions": [_condition_dict(c) for c in (rule.conditions or [])],
        "outcome_conditions": [_condition_dict(c) | {"outcome_type": c.outcome_type} for c in (rule.outcome_conditions or [])],
    }


def _deserialize_condition(item: dict):
    stat_key = str(item.get("stat_key", "")).strip()
    side = str(item.get("side", "")).strip()
    operator = _normalize_import_operator(item.get("operator", ""))
    value = _coerce_int(item.get("value"))
    group_id = _coerce_int(item.get("group_id"), default=0)
    if not stat_key or not side or value is None:
        return None
    return RuleCondition(
        stat_key=stat_key,
        side=side,
        operator=operator,
        value=value,
        group_id=group_id,
    )


def _deserialize_outcome_condition(item: dict):
    outcome_type = str(item.get("outcome_type", "")).strip().lower()
    stat_key = str(item.get("stat_key", "")).strip()
    side = str(item.get("side", "")).strip()
    operator = _normalize_import_operator(item.get("operator", ""))
    value = _coerce_int(item.get("value"))
    group_id = _coerce_int(item.get("group_id"), default=0)
    if outcome_type not in ("green", "red") or not stat_key or not side or value is None:
        return None
    return RuleOutcomeCondition(
        outcome_type=outcome_type,
        stat_key=stat_key,
        side=side,
        operator=operator,
        value=value,
        group_id=group_id,
    )


def _condition_dict(cond):
    data = {
        "stat_key": cond.stat_key,
        "side": cond.side,
        "operator": cond.operator,
        "value": cond.value,
    }
    if hasattr(cond, "group_id"):
        data["group_id"] = cond.group_id
    return data


def _normalize_text(value) -> str:
    return " ".join(str(value or "").strip().split())


def _name_key(value) -> str:
    return _normalize_text(value).casefold()


def _condition_signature(cond, include_outcome_type: bool = False):
    base = (
        _normalize_text(getattr(cond, "stat_key", "")).casefold(),
        _normalize_text(getattr(cond, "side", "")).lower(),
        _normalize_import_operator(getattr(cond, "operator", "")),
        _coerce_int(getattr(cond, "value", None), 0),
        _coerce_int(getattr(cond, "group_id", 0), 0),
    )
    if include_outcome_type:
        return (_normalize_text(getattr(cond, "outcome_type", "")).lower(),) + base
    return base


def _rule_signature(
    *,
    name,
    time_limit_min,
    message_template,
    is_active,
    second_half_only,
    notify_telegram,
    alert_on_penalty,
    follow_ht,
    follow_ft,
    outcome_green_stage,
    outcome_red_stage,
    outcome_green_minute,
    outcome_red_minute,
    outcome_red_if_no_green,
    green_allow_score_swap,
    score_home,
    score_away,
    allowed_leagues,
    conditions,
    outcome_conditions,
):
    return (
        _name_key(name),
        _coerce_int(time_limit_min, 90),
        _normalize_text(message_template) or None,
        _coerce_bool(is_active, True),
        _coerce_bool(second_half_only, False),
        _coerce_bool(notify_telegram, True),
        _coerce_bool(alert_on_penalty, False),
        _coerce_bool(follow_ht, True),
        _coerce_bool(follow_ft, True),
        _normalize_text(outcome_green_stage or "HT").upper(),
        _normalize_text(outcome_red_stage or "HT").upper(),
        _coerce_int(outcome_green_minute),
        _coerce_int(outcome_red_minute),
        _coerce_bool(outcome_red_if_no_green, False),
        _coerce_bool(green_allow_score_swap, False),
        _coerce_int(score_home),
        _coerce_int(score_away),
        tuple(sorted(_clean_league_name(x).casefold() for x in (allowed_leagues or []) if _clean_league_name(x))),
        tuple(sorted(_condition_signature(c) for c in (conditions or []))),
        tuple(sorted(_condition_signature(c, include_outcome_type=True) for c in (outcome_conditions or []))),
    )


def _rule_model_signature(rule: Rule):
    return _rule_signature(
        name=rule.name,
        time_limit_min=rule.time_limit_min,
        message_template=rule.message_template,
        is_active=rule.is_active,
        second_half_only=rule.second_half_only,
        notify_telegram=rule.notify_telegram,
        alert_on_penalty=rule.alert_on_penalty,
        follow_ht=rule.follow_ht,
        follow_ft=rule.follow_ft,
        outcome_green_stage=rule.outcome_green_stage,
        outcome_red_stage=rule.outcome_red_stage,
        outcome_green_minute=rule.outcome_green_minute,
        outcome_red_minute=rule.outcome_red_minute,
        outcome_red_if_no_green=rule.outcome_red_if_no_green,
        green_allow_score_swap=bool(getattr(rule, "green_allow_score_swap", False)),
        score_home=rule.score_home,
        score_away=rule.score_away,
        allowed_leagues=_parse_allowed_leagues_json(getattr(rule, "allowed_leagues_json", None)),
        conditions=rule.conditions or [],
        outcome_conditions=rule.outcome_conditions or [],
    )


def _build_form_context(form):
    allowed_leagues = _parse_allowed_leagues_json(form.get("allowed_leagues_json"))
    form_data = {
        "name": form.get("name", "").strip(),
        "message_template": form.get("message_template", "").strip(),
        "is_active": bool(form.get("is_active")),
        "second_half_only": bool(form.get("second_half_only")),
        "notify_telegram": bool(form.get("notify_telegram")),
        "alert_on_penalty": bool(form.get("alert_on_penalty")),
        "score_home": form.get("score_home", "").strip(),
        "score_away": form.get("score_away", "").strip(),
        "outcome_green_minute": form.get("outcome_green_minute", "").strip(),
        "outcome_red_minute": form.get("outcome_red_minute", "").strip(),
        "outcome_red_if_no_green": bool(form.get("outcome_red_if_no_green")),
        "green_allow_score_swap": bool(form.get("green_allow_score_swap")),
    }
    conditions = [_condition_dict(c) for c in _parse_conditions(form)]
    outcome_green = [_condition_dict(c) for c in _parse_outcome_conditions(form, "outcome-green")]
    outcome_red = [_condition_dict(c) for c in _parse_outcome_conditions(form, "outcome-red")]
    return {
        "form_data": form_data,
        "form_conditions": conditions,
        "form_outcome_green": outcome_green,
        "form_outcome_red": outcome_red,
        "allowed_leagues": allowed_leagues,
    }


def _build_rule_context(rule):
    return {
        "form_conditions": [_condition_dict(c) for c in (rule.conditions or [])],
        "form_outcome_green": [
            _condition_dict(c)
            for c in (rule.outcome_conditions or [])
            if c.outcome_type == "green"
        ],
        "form_outcome_red": [
            _condition_dict(c)
            for c in (rule.outcome_conditions or [])
            if c.outcome_type == "red"
        ],
        "allowed_leagues": _parse_allowed_leagues_json(getattr(rule, "allowed_leagues_json", None)),
    }


@rules_bp.route("/")
@login_required
def list_rules():
    stage_filter = (request.args.get("stage") or "all").strip().lower()
    if stage_filter not in ("all", "ht", "ft"):
        stage_filter = "all"
    rules_query = Rule.query.filter_by(user_id=current_user.id)
    if stage_filter == "ht":
        rules_query = rules_query.filter(Rule.second_half_only.is_(False))
    elif stage_filter == "ft":
        rules_query = rules_query.filter(Rule.second_half_only.is_(True))
    rules = rules_query.all()
    has_any_rules = Rule.query.filter_by(user_id=current_user.id).count() > 0
    rule_stats = {rule.id: {"green": 0, "red": 0} for rule in rules}
    rule_alert_counts = {rule.id: 0 for rule in rules}
    rule_rankings = {rule.id: {"leagues": {"green": [], "red": []}, "teams": {"green": [], "red": []}} for rule in rules}
    rule_ids = [rule.id for rule in rules]
    counts = (
        db.session.query(MatchAlert.rule_id, MatchAlert.status, func.count(MatchAlert.id))
        .filter(MatchAlert.user_id == current_user.id)
        .group_by(MatchAlert.rule_id, MatchAlert.status)
        .all()
    )
    for rule_id, status, total in counts:
        if rule_id in rule_stats and status in ("green", "red"):
            rule_stats[rule_id][status] = total
        if rule_id in rule_alert_counts:
            rule_alert_counts[rule_id] += total

    if rule_ids:
        alerts = (
            db.session.query(
                MatchAlert.rule_id,
                MatchAlert.status,
                MatchAlert.league,
                MatchAlert.home_team,
                MatchAlert.away_team,
            )
            .filter(MatchAlert.user_id == current_user.id)
            .filter(MatchAlert.rule_id.in_(rule_ids))
            .filter(MatchAlert.status.in_(("green", "red")))
            .all()
        )
        league_counts = {rid: {"green": {}, "red": {}} for rid in rule_ids}
        team_counts = {rid: {"green": {}, "red": {}} for rid in rule_ids}
        for rule_id, status, league, home_team, away_team in alerts:
            if rule_id not in league_counts:
                continue
            if league:
                league_counts[rule_id][status][league] = league_counts[rule_id][status].get(league, 0) + 1
            for team in (home_team, away_team):
                if team:
                    team_counts[rule_id][status][team] = team_counts[rule_id][status].get(team, 0) + 1

        def _top5(d):
            items = sorted(d.items(), key=lambda item: (-item[1], item[0]))
            return [{"name": name, "count": count} for name, count in items[:5]]

        for rid in rule_ids:
            rule_rankings[rid]["leagues"]["green"] = _top5(league_counts[rid]["green"])
            rule_rankings[rid]["leagues"]["red"] = _top5(league_counts[rid]["red"])
            rule_rankings[rid]["teams"]["green"] = _top5(team_counts[rid]["green"])
            rule_rankings[rid]["teams"]["red"] = _top5(team_counts[rid]["red"])
    rules = sorted(
        rules,
        key=lambda rule: (
            -(rule_stats.get(rule.id, {}).get("green", 0) - rule_stats.get(rule.id, {}).get("red", 0)),
            -rule_stats.get(rule.id, {}).get("green", 0),
            rule_stats.get(rule.id, {}).get("red", 0),
            -rule.id,
        ),
    )
    return render_template(
        "rules/list.html",
        rules=rules,
        rule_stats=rule_stats,
        rule_alert_counts=rule_alert_counts,
        rule_rankings=rule_rankings,
        stage_filter=stage_filter,
        has_any_rules=has_any_rules,
    )




@rules_bp.route("/export", methods=["GET"])
@login_required
def export_rules():
    rules = Rule.query.filter_by(user_id=current_user.id).order_by(Rule.id.asc()).all()
    payload = {
        "version": 1,
        "exported_at": now_sp().isoformat(),
        "user": current_user.username,
        "rules": [_serialize_rule(rule) for rule in rules],
    }
    data = json.dumps(payload, ensure_ascii=False, indent=2)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"regras_{current_user.username}_{stamp}.json"
    return Response(
        data,
        mimetype="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@rules_bp.route("/import", methods=["POST"])
@login_required
def import_rules():
    file = request.files.get("rules_file")
    if not file or not file.filename:
        flash("Selecione um arquivo JSON para importar.", "warning")
        return redirect(url_for("rules.list_rules"))

    try:
        payload = json.loads(file.read().decode("utf-8-sig"))
    except Exception:
        flash("Arquivo invalido. Envie um JSON exportado pelo sistema.", "danger")
        return redirect(url_for("rules.list_rules"))

    rule_items = payload.get("rules") if isinstance(payload, dict) else None
    if not isinstance(rule_items, list):
        flash("Formato invalido: campo 'rules' nao encontrado.", "danger")
        return redirect(url_for("rules.list_rules"))

    imported = 0
    skipped = 0
    duplicated = 0
    existing_name_keys = {
        _name_key(rule.name)
        for rule in Rule.query.filter_by(user_id=current_user.id).all()
    }
    existing_signatures = {
        _rule_model_signature(rule)
        for rule in Rule.query.filter_by(user_id=current_user.id).all()
    }

    for item in rule_items:
        if not isinstance(item, dict):
            skipped += 1
            continue

        name = str(item.get("name", "")).strip()
        if not name:
            skipped += 1
            continue
        if _name_key(name) in existing_name_keys:
            duplicated += 1
            continue

        conditions_raw = item.get("conditions") or []
        conditions = [_deserialize_condition(c) for c in conditions_raw if isinstance(c, dict)]
        conditions = [c for c in conditions if c]
        if not conditions:
            skipped += 1
            continue

        outcome_conditions_raw = item.get("outcome_conditions") or []
        outcome_conditions = [_deserialize_outcome_condition(c) for c in outcome_conditions_raw if isinstance(c, dict)]
        outcome_conditions = [c for c in outcome_conditions if c]

        allowed_leagues_raw = item.get("allowed_leagues") or []
        allowed_leagues = []
        if isinstance(allowed_leagues_raw, list):
            for x in allowed_leagues_raw:
                name = _clean_league_name(x)
                if name:
                    allowed_leagues.append(name)

        rule_data = {
            "time_limit_min": _coerce_int(item.get("time_limit_min"), 90),
            "message_template": item.get("message_template") or None,
            "is_active": _coerce_bool(item.get("is_active"), True),
            "second_half_only": _coerce_bool(item.get("second_half_only"), False),
            "notify_telegram": _coerce_bool(item.get("notify_telegram"), True),
            "alert_on_penalty": _coerce_bool(item.get("alert_on_penalty"), False),
            "follow_ht": _coerce_bool(item.get("follow_ht"), True),
            "follow_ft": _coerce_bool(item.get("follow_ft"), True),
            "outcome_green_stage": str(item.get("outcome_green_stage") or "HT"),
            "outcome_red_stage": str(item.get("outcome_red_stage") or "HT"),
            "outcome_green_minute": _coerce_int(item.get("outcome_green_minute")),
            "outcome_red_minute": _coerce_int(item.get("outcome_red_minute")),
            "outcome_red_if_no_green": _coerce_bool(item.get("outcome_red_if_no_green"), False),
            "green_allow_score_swap": _coerce_bool(item.get("green_allow_score_swap"), False),
            "score_home": _coerce_int(item.get("score_home")),
            "score_away": _coerce_int(item.get("score_away")),
            "allowed_leagues_json": json.dumps(allowed_leagues, ensure_ascii=False) if allowed_leagues else None,
        }
        signature = _rule_signature(
            name=name,
            allowed_leagues=allowed_leagues,
            conditions=conditions,
            outcome_conditions=outcome_conditions,
            **rule_data,
        )
        if signature in existing_signatures:
            duplicated += 1
            continue

        rule = Rule(
            user_id=current_user.id,
            name=name,
            **rule_data,
        )

        db.session.add(rule)
        db.session.flush()
        for cond in conditions:
            cond.rule_id = rule.id
            db.session.add(cond)
        for cond in outcome_conditions:
            cond.rule_id = rule.id
            db.session.add(cond)

        try:
            db.session.commit()
            imported += 1
            existing_name_keys.add(_name_key(name))
            existing_signatures.add(signature)
        except Exception:
            db.session.rollback()
            skipped += 1

    if imported:
        flash(f"Importacao concluida: {imported} regra(s) importada(s).", "success")
    if duplicated:
        flash(f"{duplicated} regra(s) duplicada(s) foram ignoradas.", "warning")
    if skipped:
        flash(f"{skipped} item(ns) foram ignorados por formato invalido.", "warning")
    if not imported and not skipped and not duplicated:
        flash("Nenhuma regra encontrada para importar.", "warning")
    return redirect(url_for("rules.list_rules"))


@rules_bp.route("/new", methods=["GET", "POST"])
@login_required
def create_rule():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        time_limit_raw = request.form.get("time_limit_min", "").strip()
        message_template = request.form.get("message_template", "").strip()
        is_active = bool(request.form.get("is_active"))
        second_half_only = bool(request.form.get("second_half_only"))
        notify_telegram = bool(request.form.get("notify_telegram"))
        alert_on_penalty = bool(request.form.get("alert_on_penalty"))
        follow_ht = bool(request.form.get("follow_ht"))
        follow_ft = bool(request.form.get("follow_ft"))
        outcome_green_stage = request.form.get("outcome_green_stage", "HT")
        outcome_red_stage = request.form.get("outcome_red_stage", "HT")
        outcome_green_minute_raw = request.form.get("outcome_green_minute", "").strip()
        outcome_red_minute_raw = request.form.get("outcome_red_minute", "").strip()
        outcome_red_if_no_green = bool(request.form.get("outcome_red_if_no_green"))
        green_allow_score_swap = bool(request.form.get("green_allow_score_swap"))
        score_home_raw = request.form.get("score_home", "").strip()
        score_away_raw = request.form.get("score_away", "").strip()
        allowed_leagues = _parse_allowed_leagues_json(request.form.get("allowed_leagues_json"))

        if not name:
            flash("Nome e obrigatorio.", "warning")
            return render_template(
                "rules/form.html",
                rule=None,
                available_leagues=_available_leagues(),
                **_build_form_context(request.form),
            )
        if notify_telegram and (not current_user.telegram_token or not current_user.telegram_chat_id or not current_user.telegram_verified):
            flash("Configure e teste o Telegram antes de criar regras que avisam.", "warning")
            return redirect(url_for("settings.settings"))
        time_limit_min = int(time_limit_raw) if time_limit_raw.isdigit() else 90
        outcome_green_minute = int(outcome_green_minute_raw) if outcome_green_minute_raw.isdigit() else None
        outcome_red_minute = int(outcome_red_minute_raw) if outcome_red_minute_raw.isdigit() else None
        score_home = int(score_home_raw) if score_home_raw.isdigit() else None
        score_away = int(score_away_raw) if score_away_raw.isdigit() else None

        conditions = _parse_conditions(request.form)
        if not conditions:
            flash("Adicione ao menos uma condicao.", "warning")
            return render_template(
                "rules/form.html",
                rule=None,
                available_leagues=_available_leagues(),
                **_build_form_context(request.form),
            )

        outcome_green = _parse_outcome_conditions(request.form, "outcome-green")
        outcome_red = _parse_outcome_conditions(request.form, "outcome-red")
        if outcome_red_if_no_green and not outcome_green:
            flash("Adicione ao menos uma condicao de GREEN para usar o RED por tempo.", "warning")
            return render_template(
                "rules/form.html",
                rule=None,
                available_leagues=_available_leagues(),
                **_build_form_context(request.form),
            )
        if outcome_red_if_no_green and outcome_red_minute is None:
            flash("Defina o minuto limite para virar RED quando o GREEN nao ocorrer.", "warning")
            return render_template(
                "rules/form.html",
                rule=None,
                available_leagues=_available_leagues(),
                **_build_form_context(request.form),
            )

        rule = Rule(
            user_id=current_user.id,
            name=name,
            time_limit_min=time_limit_min,
            message_template=message_template or None,
            is_active=is_active,
            second_half_only=second_half_only,
            notify_telegram=notify_telegram,
            alert_on_penalty=alert_on_penalty,
            follow_ht=follow_ht,
            follow_ft=follow_ft,
            outcome_green_stage=outcome_green_stage,
            outcome_red_stage=outcome_red_stage,
            outcome_green_minute=outcome_green_minute,
            outcome_red_minute=outcome_red_minute,
            outcome_red_if_no_green=outcome_red_if_no_green,
            green_allow_score_swap=green_allow_score_swap,
            score_home=score_home,
            score_away=score_away,
            allowed_leagues_json=json.dumps(allowed_leagues, ensure_ascii=False) if allowed_leagues else None,
        )
        db.session.add(rule)
        db.session.flush()

        for cond in conditions:
            cond.rule_id = rule.id
            db.session.add(cond)
        for cond in outcome_green + outcome_red:
            cond.rule_id = rule.id
            db.session.add(cond)
        db.session.commit()
        flash("Regra criada.", "success")
        return redirect(url_for("rules.list_rules"))
    return render_template(
        "rules/form.html",
        rule=None,
        available_leagues=_available_leagues(),
        allowed_leagues=[],
    )


@rules_bp.route("/<int:rule_id>/edit", methods=["GET", "POST"])
@login_required
def edit_rule(rule_id):
    rule = Rule.query.filter_by(id=rule_id, user_id=current_user.id).first_or_404()
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        time_limit_raw = request.form.get("time_limit_min", "").strip()
        message_template = request.form.get("message_template", "").strip()
        is_active = bool(request.form.get("is_active"))
        second_half_only = bool(request.form.get("second_half_only"))
        notify_telegram = bool(request.form.get("notify_telegram"))
        alert_on_penalty = bool(request.form.get("alert_on_penalty"))
        follow_ht = bool(request.form.get("follow_ht"))
        follow_ft = bool(request.form.get("follow_ft"))
        outcome_green_stage = request.form.get("outcome_green_stage", "HT")
        outcome_red_stage = request.form.get("outcome_red_stage", "HT")
        outcome_green_minute_raw = request.form.get("outcome_green_minute", "").strip()
        outcome_red_minute_raw = request.form.get("outcome_red_minute", "").strip()
        outcome_red_if_no_green = bool(request.form.get("outcome_red_if_no_green"))
        green_allow_score_swap = bool(request.form.get("green_allow_score_swap"))
        score_home_raw = request.form.get("score_home", "").strip()
        score_away_raw = request.form.get("score_away", "").strip()
        allowed_leagues = _parse_allowed_leagues_json(request.form.get("allowed_leagues_json"))

        if not name:
            flash("Nome e obrigatorio.", "warning")
            return render_template(
                "rules/form.html",
                rule=rule,
                available_leagues=_available_leagues(),
                **_build_form_context(request.form),
            )
        if notify_telegram and (not current_user.telegram_token or not current_user.telegram_chat_id or not current_user.telegram_verified):
            flash("Configure e teste o Telegram antes de ativar avisos.", "warning")
            return redirect(url_for("settings.settings"))

        time_limit_min = int(time_limit_raw) if time_limit_raw.isdigit() else rule.time_limit_min
        outcome_green_minute = int(outcome_green_minute_raw) if outcome_green_minute_raw.isdigit() else None
        outcome_red_minute = int(outcome_red_minute_raw) if outcome_red_minute_raw.isdigit() else None
        score_home = int(score_home_raw) if score_home_raw.isdigit() else None
        score_away = int(score_away_raw) if score_away_raw.isdigit() else None
        rule.name = name
        rule.time_limit_min = time_limit_min
        rule.message_template = message_template or None
        rule.is_active = is_active
        rule.second_half_only = second_half_only
        rule.notify_telegram = notify_telegram
        rule.alert_on_penalty = alert_on_penalty
        rule.follow_ht = follow_ht
        rule.follow_ft = follow_ft
        rule.outcome_green_stage = outcome_green_stage
        rule.outcome_red_stage = outcome_red_stage
        rule.outcome_green_minute = outcome_green_minute
        rule.outcome_red_minute = outcome_red_minute
        rule.outcome_red_if_no_green = outcome_red_if_no_green
        rule.green_allow_score_swap = green_allow_score_swap
        rule.score_home = score_home
        rule.score_away = score_away
        rule.allowed_leagues_json = json.dumps(allowed_leagues, ensure_ascii=False) if allowed_leagues else None

        conditions = _parse_conditions(request.form)
        if not conditions:
            flash("Adicione ao menos uma condicao.", "warning")
            db.session.rollback()
            return render_template(
                "rules/form.html",
                rule=rule,
                available_leagues=_available_leagues(),
                **_build_form_context(request.form),
            )
        outcome_green = _parse_outcome_conditions(request.form, "outcome-green")
        outcome_red = _parse_outcome_conditions(request.form, "outcome-red")
        if outcome_red_if_no_green and not outcome_green:
            flash("Adicione ao menos uma condicao de GREEN para usar o RED por tempo.", "warning")
            return render_template(
                "rules/form.html",
                rule=rule,
                available_leagues=_available_leagues(),
                **_build_form_context(request.form),
            )
        if outcome_red_if_no_green and outcome_red_minute is None:
            flash("Defina o minuto limite para virar RED quando o GREEN nao ocorrer.", "warning")
            return render_template(
                "rules/form.html",
                rule=rule,
                available_leagues=_available_leagues(),
                **_build_form_context(request.form),
            )

        RuleCondition.query.filter_by(rule_id=rule.id).delete()
        RuleOutcomeCondition.query.filter_by(rule_id=rule.id).delete()
        for cond in conditions:
            cond.rule_id = rule.id
            db.session.add(cond)
        for cond in outcome_green + outcome_red:
            cond.rule_id = rule.id
            db.session.add(cond)
        db.session.commit()
        flash("Regra atualizada.", "success")
        return redirect(url_for("rules.list_rules"))
    return render_template(
        "rules/form.html",
        rule=rule,
        available_leagues=_available_leagues(),
        **_build_rule_context(rule),
    )


@rules_bp.route("/<int:rule_id>/delete", methods=["POST"])
@login_required
def delete_rule(rule_id):
    rule = Rule.query.filter_by(id=rule_id, user_id=current_user.id).first_or_404()
    alert_count = MatchAlert.query.filter_by(rule_id=rule.id).count()
    if alert_count > 20 and not current_user.is_admin_user:
        password = request.form.get("confirm_password", "").strip()
        if not current_user.check_password(password):
            flash("Regra com muitos jogos no histórico. Informe sua senha para confirmar.", "warning")
            return redirect(url_for("rules.list_rules"))
    db.session.delete(rule)
    db.session.commit()
    flash("Regra removida.", "success")
    return redirect(url_for("rules.list_rules"))


@rules_bp.route("/<int:rule_id>/toggle", methods=["POST"])
@login_required
def toggle_rule(rule_id):
    rule = Rule.query.filter_by(id=rule_id, user_id=current_user.id).first_or_404()
    rule.is_active = not rule.is_active
    db.session.commit()
    return redirect(url_for("rules.list_rules"))


@rules_bp.route("/test", methods=["POST"])
@login_required
def test_rule():
    def is_youth_match(payload):
        if not payload:
            return False
        hay = f"{payload.get('league', '')} {payload.get('home_team', '')} {payload.get('away_team', '')}".lower()
        return any(
            token in hay
            for token in (
                "u19",
                "u-19",
                "u 19",
                "sub19",
                "sub-19",
                "sub 19",
                "under 19",
                "u20",
                "u-20",
                "u 20",
                "sub20",
                "sub-20",
                "sub 20",
                "under 20",
            )
        )

    conditions = _parse_conditions(request.form)
    if not conditions:
        return jsonify({"ok": False, "message": "Adicione condicoes antes de testar."}), 400
    allowed_leagues = _parse_allowed_leagues_json(request.form.get("allowed_leagues_json"))
    allowed_leagues_set = {name.casefold() for name in allowed_leagues}
    temp_rule = Rule(
        user_id=current_user.id,
        name=request.form.get("name", "Regra teste"),
        time_limit_min=90,
    )
    temp_rule.second_half_only = bool(request.form.get("second_half_only"))
    temp_rule.alert_on_penalty = bool(request.form.get("alert_on_penalty"))
    score_home_raw = request.form.get("score_home", "").strip()
    score_away_raw = request.form.get("score_away", "").strip()
    temp_rule.score_home = int(score_home_raw) if score_home_raw.isdigit() else None
    temp_rule.score_away = int(score_away_raw) if score_away_raw.isdigit() else None
    temp_rule.conditions = conditions

    session = make_session()
    games, status_code = fetch_live_games(session)
    if status_code != 200:
        return jsonify({"ok": False, "message": f"API OFF (HTTP {status_code})"}), 503
    matches = []
    for game in games:
        stats_payload = fetch_match_stats(session, game["url"])
        if not stats_payload:
            continue
        if is_youth_match(stats_payload):
            continue
        if allowed_leagues_set:
            league_name = _clean_league_name(stats_payload.get("league"))
            if league_name.casefold() not in allowed_leagues_set:
                continue
        minute = stats_payload.get("minute") or game["minute"]
        home_score, away_score = parse_score(stats_payload.get("score", ""))
        if temp_rule.score_home is not None and home_score != temp_rule.score_home:
            continue
        if temp_rule.score_away is not None and away_score != temp_rule.score_away:
            continue
        stats_for_rule = stats_payload.get("stats", {})
        if temp_rule.second_half_only:
            if is_first_half_extra_time(stats_payload.get("time_text", "")):
                continue
            if (minute or 0) < 46:
                continue
            stats_for_rule = {
                key: value.copy() if isinstance(value, dict) else value
                for key, value in stats_for_rule.items()
            }
            if minute is not None:
                minute_2h = max(0, minute - 45)
                stats_for_rule["Minute"] = {"home": minute_2h, "away": minute_2h, "total": minute_2h}
        # alert_on_penalty nao deve bloquear a regra no teste
        if evaluate_rule(temp_rule, stats_for_rule):
            history_meta = {}
            try:
                history = fetch_match_history(session, game["url"])
                h2h_summary = summarize_history(history.get("h2h", []))
                home_summary = summarize_history(history.get("home", []))
                away_summary = summarize_history(history.get("away", []))
                h2h_items = history.get("h2h", [])
                history_meta = {
                    "history_h2h": format_history_summary("H2H", h2h_summary) if h2h_summary else "Sem historico de um contra o outro",
                    "history_home": format_history_summary("Home", home_summary),
                    "history_away": format_history_summary("Away", away_summary),
                }
                confidence = history_confidence(conditions, h2h_items)
                history_meta["history_confidence"] = f"{confidence}%" if confidence is not None else "Sem historico de um contra o outro"
            except Exception:
                history_meta = {}
            matches.append(
                {
                    "league": stats_payload.get("league"),
                    "home_team": stats_payload.get("home_team"),
                    "away_team": stats_payload.get("away_team"),
                    "minute": stats_payload.get("minute"),
                    "score": stats_payload.get("score"),
                    "url": game["url"],
                    **history_meta,
                }
            )
        if len(matches) >= 5:
            break
    return jsonify({"ok": True, "matches": matches})
