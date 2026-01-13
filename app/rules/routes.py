from flask import Blueprint, flash, jsonify, redirect, render_template, request, url_for
from flask_login import current_user, login_required
from sqlalchemy import func

from ..extensions import db
from ..models import MatchAlert, Rule, RuleCondition, RuleOutcomeCondition
from ..services.evaluator import evaluate_rule
from ..services.scraper import fetch_live_games, fetch_match_stats, make_session

rules_bp = Blueprint("rules", __name__, url_prefix="/rules")


def _parse_conditions(form):
    conditions = []
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
            operator = form.get(f"group-{group_id}-cond-{index}-operator", "").strip()
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
        operator = form.get(f"conditions-{index}-operator", "").strip()
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
    index = 0
    while True:
        stat_key = form.get(f"{prefix}-{index}-stat_key")
        if stat_key is None:
            break
        stat_key = stat_key.strip()
        side = form.get(f"{prefix}-{index}-side", "").strip()
        operator = form.get(f"{prefix}-{index}-operator", "").strip()
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
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        time_limit_min = request.form.get("time_limit_min", "30").strip()
        message_template = request.form.get("message_template", "").strip()
        is_active = bool(request.form.get("is_active"))
        second_half_only = bool(request.form.get("second_half_only"))
        follow_ht = bool(request.form.get("follow_ht"))
        follow_ft = bool(request.form.get("follow_ft"))
        outcome_green_stage = request.form.get("outcome_green_stage", "HT")
        outcome_red_stage = request.form.get("outcome_red_stage", "HT")

        if not name or not time_limit_min.isdigit():
            flash("Nome e tempo limite sao obrigatorios.", "warning")
            return render_template("rules/form.html", rule=None)

        rule = Rule(
            user_id=current_user.id,
            name=name,
            time_limit_min=int(time_limit_min),
            message_template=message_template or None,
            is_active=is_active,
            second_half_only=second_half_only,
            follow_ht=follow_ht,
            follow_ft=follow_ft,
            outcome_green_stage=outcome_green_stage,
            outcome_red_stage=outcome_red_stage,
        )
        db.session.add(rule)
        db.session.flush()

        conditions = _parse_conditions(request.form)
        if not conditions:
            flash("Adicione ao menos uma condicao.", "warning")
            db.session.rollback()
            return render_template("rules/form.html", rule=None)

        for cond in conditions:
            cond.rule_id = rule.id
            db.session.add(cond)
        outcome_green = _parse_outcome_conditions(request.form, "outcome-green")
        outcome_red = _parse_outcome_conditions(request.form, "outcome-red")
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
    rule = Rule.query.filter_by(id=rule_id, user_id=current_user.id).first_or_404()
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        time_limit_min = request.form.get("time_limit_min", "30").strip()
        message_template = request.form.get("message_template", "").strip()
        is_active = bool(request.form.get("is_active"))
        second_half_only = bool(request.form.get("second_half_only"))
        follow_ht = bool(request.form.get("follow_ht"))
        follow_ft = bool(request.form.get("follow_ft"))
        outcome_green_stage = request.form.get("outcome_green_stage", "HT")
        outcome_red_stage = request.form.get("outcome_red_stage", "HT")

        if not name or not time_limit_min.isdigit():
            flash("Nome e tempo limite sao obrigatorios.", "warning")
            return render_template("rules/form.html", rule=rule)

        rule.name = name
        rule.time_limit_min = int(time_limit_min)
        rule.message_template = message_template or None
        rule.is_active = is_active
        rule.second_half_only = second_half_only
        rule.follow_ht = follow_ht
        rule.follow_ft = follow_ft
        rule.outcome_green_stage = outcome_green_stage
        rule.outcome_red_stage = outcome_red_stage

        RuleCondition.query.filter_by(rule_id=rule.id).delete()
        RuleOutcomeCondition.query.filter_by(rule_id=rule.id).delete()
        conditions = _parse_conditions(request.form)
        if not conditions:
            flash("Adicione ao menos uma condicao.", "warning")
            db.session.rollback()
            return render_template("rules/form.html", rule=rule)
        for cond in conditions:
            cond.rule_id = rule.id
            db.session.add(cond)
        outcome_green = _parse_outcome_conditions(request.form, "outcome-green")
        outcome_red = _parse_outcome_conditions(request.form, "outcome-red")
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
    conditions = _parse_conditions(request.form)
    if not conditions:
        return jsonify({"ok": False, "message": "Adicione condicoes antes de testar."}), 400
    temp_rule = Rule(
        user_id=current_user.id,
        name=request.form.get("name", "Regra teste"),
        time_limit_min=int(request.form.get("time_limit_min", "30") or 30),
    )
    temp_rule.second_half_only = bool(request.form.get("second_half_only"))
    temp_rule.conditions = conditions

    session = make_session()
    games, status_code = fetch_live_games(session)
    if status_code != 200:
        return jsonify({"ok": False, "message": f"API OFF (HTTP {status_code})"}), 503
    matches = []
    for game in games[:10]:
        stats_payload = fetch_match_stats(session, game["url"])
        if not stats_payload:
            continue
        if game["minute"] and game["minute"] > temp_rule.time_limit_min:
            continue
        if temp_rule.second_half_only and (game["minute"] or 0) < 46:
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
