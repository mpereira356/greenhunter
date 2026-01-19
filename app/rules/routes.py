from flask import Blueprint, flash, jsonify, redirect, render_template, request, url_for
from flask_login import current_user, login_required
from sqlalchemy import func

from ..extensions import db
from ..models import MatchAlert, Rule, RuleCondition, RuleOutcomeCondition
from ..services.evaluator import evaluate_rule
from ..services.scraper import fetch_live_games, fetch_match_stats, make_session
from ..services.worker import parse_score

rules_bp = Blueprint("rules", __name__, url_prefix="/rules")


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
            side = form.get(f"group-{group_id}-cond-{index}-side", "").strip()
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
        side = form.get(f"conditions-{index}-side", "").strip()
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

    index = 0
    while True:
        stat_key = form.get(f"{prefix}-{index}-stat_key")
        if stat_key is None:
            break
        stat_key = stat_key.strip()
        side = form.get(f"{prefix}-{index}-side", "").strip()
        operator = _normalize_operator(form.get(f"{prefix}-{index}-operator", ""))
        value_raw = form.get(f"{prefix}-{index}-value", "").strip()
        if stat_key and side and operator and value_raw.isdigit():
            conditions.append(
                RuleOutcomeCondition(
                    outcome_type=prefix.split("-")[-1],
                    stat_key=stat_key,
                    side=side,
                    operator=operator,
                    value=int(value_raw),
                )
            )
        index += 1
    return conditions


def _condition_dict(cond):
    return {
        "stat_key": cond.stat_key,
        "side": cond.side,
        "operator": cond.operator,
        "value": cond.value,
    }


def _build_form_context(form):
    form_data = {
        "name": form.get("name", "").strip(),
        "message_template": form.get("message_template", "").strip(),
        "is_active": bool(form.get("is_active")),
        "second_half_only": bool(form.get("second_half_only")),
        "score_home": form.get("score_home", "").strip(),
        "score_away": form.get("score_away", "").strip(),
        "outcome_green_minute": form.get("outcome_green_minute", "").strip(),
        "outcome_red_minute": form.get("outcome_red_minute", "").strip(),
        "outcome_red_if_no_green": bool(form.get("outcome_red_if_no_green")),
    }
    conditions = [_condition_dict(c) for c in _parse_conditions(form)]
    outcome_green = [_condition_dict(c) for c in _parse_outcome_conditions(form, "outcome-green")]
    outcome_red = [_condition_dict(c) for c in _parse_outcome_conditions(form, "outcome-red")]
    return {
        "form_data": form_data,
        "form_conditions": conditions,
        "form_outcome_green": outcome_green,
        "form_outcome_red": outcome_red,
    }


@rules_bp.route("/")
@login_required
def list_rules():
    rules = Rule.query.filter_by(user_id=current_user.id).order_by(Rule.id.desc()).all()
    rule_stats = {rule.id: {"green": 0, "red": 0} for rule in rules}
    counts = (
        db.session.query(MatchAlert.rule_id, MatchAlert.status, func.count(MatchAlert.id))
        .filter(MatchAlert.user_id == current_user.id)
        .group_by(MatchAlert.rule_id, MatchAlert.status)
        .all()
    )
    for rule_id, status, total in counts:
        if rule_id in rule_stats and status in ("green", "red"):
            rule_stats[rule_id][status] = total
    return render_template("rules/list.html", rules=rules, rule_stats=rule_stats)


@rules_bp.route("/new", methods=["GET", "POST"])
@login_required
def create_rule():
    if not current_user.telegram_token or not current_user.telegram_chat_id or not current_user.telegram_verified:
        flash("Configure e teste o Telegram antes de criar regras.", "warning")
        return redirect(url_for("settings.settings"))
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        time_limit_raw = request.form.get("time_limit_min", "").strip()
        message_template = request.form.get("message_template", "").strip()
        is_active = bool(request.form.get("is_active"))
        second_half_only = bool(request.form.get("second_half_only"))
        follow_ht = bool(request.form.get("follow_ht"))
        follow_ft = bool(request.form.get("follow_ft"))
        outcome_green_stage = request.form.get("outcome_green_stage", "HT")
        outcome_red_stage = request.form.get("outcome_red_stage", "HT")
        outcome_green_minute_raw = request.form.get("outcome_green_minute", "").strip()
        outcome_red_minute_raw = request.form.get("outcome_red_minute", "").strip()
        outcome_red_if_no_green = bool(request.form.get("outcome_red_if_no_green"))
        score_home_raw = request.form.get("score_home", "").strip()
        score_away_raw = request.form.get("score_away", "").strip()

        if not name:
            flash("Nome e obrigatorio.", "warning")
            return render_template("rules/form.html", rule=None, **_build_form_context(request.form))
        time_limit_min = int(time_limit_raw) if time_limit_raw.isdigit() else 90
        outcome_green_minute = int(outcome_green_minute_raw) if outcome_green_minute_raw.isdigit() else None
        outcome_red_minute = int(outcome_red_minute_raw) if outcome_red_minute_raw.isdigit() else None
        score_home = int(score_home_raw) if score_home_raw.isdigit() else None
        score_away = int(score_away_raw) if score_away_raw.isdigit() else None

        conditions = _parse_conditions(request.form)
        if not conditions:
            flash("Adicione ao menos uma condicao.", "warning")
            return render_template("rules/form.html", rule=None, **_build_form_context(request.form))

        outcome_green = _parse_outcome_conditions(request.form, "outcome-green")
        outcome_red = _parse_outcome_conditions(request.form, "outcome-red")
        if outcome_red_if_no_green and not outcome_green:
            flash("Adicione ao menos uma condicao de GREEN para usar o RED por tempo.", "warning")
            return render_template("rules/form.html", rule=None, **_build_form_context(request.form))
        if outcome_red_if_no_green and outcome_red_minute is None:
            flash("Defina o minuto limite para virar RED quando o GREEN nao ocorrer.", "warning")
            return render_template("rules/form.html", rule=None, **_build_form_context(request.form))

        rule = Rule(
            user_id=current_user.id,
            name=name,
            time_limit_min=time_limit_min,
            message_template=message_template or None,
            is_active=is_active,
            second_half_only=second_half_only,
            follow_ht=follow_ht,
            follow_ft=follow_ft,
            outcome_green_stage=outcome_green_stage,
            outcome_red_stage=outcome_red_stage,
            outcome_green_minute=outcome_green_minute,
            outcome_red_minute=outcome_red_minute,
            outcome_red_if_no_green=outcome_red_if_no_green,
            score_home=score_home,
            score_away=score_away,
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
    return render_template("rules/form.html", rule=None)


@rules_bp.route("/<int:rule_id>/edit", methods=["GET", "POST"])
@login_required
def edit_rule(rule_id):
    if not current_user.telegram_token or not current_user.telegram_chat_id or not current_user.telegram_verified:
        flash("Configure e teste o Telegram antes de editar regras.", "warning")
        return redirect(url_for("settings.settings"))
    rule = Rule.query.filter_by(id=rule_id, user_id=current_user.id).first_or_404()
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        time_limit_raw = request.form.get("time_limit_min", "").strip()
        message_template = request.form.get("message_template", "").strip()
        is_active = bool(request.form.get("is_active"))
        second_half_only = bool(request.form.get("second_half_only"))
        follow_ht = bool(request.form.get("follow_ht"))
        follow_ft = bool(request.form.get("follow_ft"))
        outcome_green_stage = request.form.get("outcome_green_stage", "HT")
        outcome_red_stage = request.form.get("outcome_red_stage", "HT")
        outcome_green_minute_raw = request.form.get("outcome_green_minute", "").strip()
        outcome_red_minute_raw = request.form.get("outcome_red_minute", "").strip()
        outcome_red_if_no_green = bool(request.form.get("outcome_red_if_no_green"))
        score_home_raw = request.form.get("score_home", "").strip()
        score_away_raw = request.form.get("score_away", "").strip()

        if not name:
            flash("Nome e obrigatorio.", "warning")
            return render_template("rules/form.html", rule=rule, **_build_form_context(request.form))

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
        rule.follow_ht = follow_ht
        rule.follow_ft = follow_ft
        rule.outcome_green_stage = outcome_green_stage
        rule.outcome_red_stage = outcome_red_stage
        rule.outcome_green_minute = outcome_green_minute
        rule.outcome_red_minute = outcome_red_minute
        rule.outcome_red_if_no_green = outcome_red_if_no_green
        rule.score_home = score_home
        rule.score_away = score_away

        conditions = _parse_conditions(request.form)
        if not conditions:
            flash("Adicione ao menos uma condicao.", "warning")
            db.session.rollback()
            return render_template("rules/form.html", rule=rule, **_build_form_context(request.form))
        outcome_green = _parse_outcome_conditions(request.form, "outcome-green")
        outcome_red = _parse_outcome_conditions(request.form, "outcome-red")
        if outcome_red_if_no_green and not outcome_green:
            flash("Adicione ao menos uma condicao de GREEN para usar o RED por tempo.", "warning")
            return render_template("rules/form.html", rule=rule, **_build_form_context(request.form))
        if outcome_red_if_no_green and outcome_red_minute is None:
            flash("Defina o minuto limite para virar RED quando o GREEN nao ocorrer.", "warning")
            return render_template("rules/form.html", rule=rule, **_build_form_context(request.form))

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
    return render_template("rules/form.html", rule=rule)


@rules_bp.route("/<int:rule_id>/delete", methods=["POST"])
@login_required
def delete_rule(rule_id):
    rule = Rule.query.filter_by(id=rule_id, user_id=current_user.id).first_or_404()
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
        return any(token in hay for token in ("u19", "u-19", "u 19", "sub19", "sub-19", "sub 19", "under 19"))

    conditions = _parse_conditions(request.form)
    if not conditions:
        return jsonify({"ok": False, "message": "Adicione condicoes antes de testar."}), 400
    temp_rule = Rule(
        user_id=current_user.id,
        name=request.form.get("name", "Regra teste"),
        time_limit_min=90,
    )
    temp_rule.second_half_only = bool(request.form.get("second_half_only"))
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
        minute = stats_payload.get("minute") or game["minute"]
        home_score, away_score = parse_score(stats_payload.get("score", ""))
        if temp_rule.score_home is not None and home_score != temp_rule.score_home:
            continue
        if temp_rule.score_away is not None and away_score != temp_rule.score_away:
            continue
        if temp_rule.second_half_only and (minute or 0) < 46:
            continue
        if evaluate_rule(temp_rule, stats_payload["stats"]):
            matches.append(
                {
                    "league": stats_payload.get("league"),
                    "home_team": stats_payload.get("home_team"),
                    "away_team": stats_payload.get("away_team"),
                    "minute": stats_payload.get("minute"),
                    "score": stats_payload.get("score"),
                    "url": game["url"],
                }
            )
        if len(matches) >= 5:
            break
    return jsonify({"ok": True, "matches": matches})
