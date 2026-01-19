import os
import json

import pandas as pd


def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def _upsert_excel(path: str, row: dict, key_field: str):
    if os.path.exists(path):
        try:
            df = pd.read_excel(path)
        except Exception:
            df = pd.DataFrame()
    else:
        df = pd.DataFrame()
    if key_field in df.columns and row.get(key_field) in df[key_field].values:
        df.loc[df[key_field] == row[key_field], list(row.keys())] = list(row.values())
    else:
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True, sort=False)
    df.to_excel(path, index=False)


def _flatten_stats(prefix: str, stats_json: str | None) -> dict:
    if not stats_json:
        return {}
    try:
        stats = json.loads(stats_json)
    except Exception:
        return {}
    flat = {}
    for key, value in stats.items():
        if isinstance(value, dict):
            for side, side_val in value.items():
                flat[f"{prefix}{key} ({side})"] = side_val
        else:
            flat[f"{prefix}{key}"] = value
    return flat


def export_alert(alert, rule_name: str, base_dir: str):
    _ensure_dir(base_dir)
    rule = alert.rule
    conditions_payload = []
    outcome_payload = []
    if rule:
        for cond in rule.conditions:
            conditions_payload.append(
                {
                    "stat_key": cond.stat_key,
                    "side": cond.side,
                    "operator": cond.operator,
                    "value": cond.value,
                    "group_id": cond.group_id,
                }
            )
        for cond in rule.outcome_conditions:
            outcome_payload.append(
                {
                    "stat_key": cond.stat_key,
                    "side": cond.side,
                    "operator": cond.operator,
                    "value": cond.value,
                    "outcome_type": cond.outcome_type,
                }
            )
    row = {
        "alert_id": alert.id,
        "rule_id": alert.rule_id,
        "rule_name": rule_name,
        "user_id": alert.user_id,
        "created_at": alert.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "alert_time_hhmm": alert.created_at.strftime("%H:%M"),
        "rule_time_limit_min": rule.time_limit_min if rule else None,
        "rule_second_half_only": rule.second_half_only if rule else None,
        "rule_follow_ht": rule.follow_ht if rule else None,
        "rule_follow_ft": rule.follow_ft if rule else None,
        "rule_is_active": rule.is_active if rule else None,
        "rule_message_template": rule.message_template if rule else None,
        "rule_outcome_green_stage": rule.outcome_green_stage if rule else None,
        "rule_outcome_red_stage": rule.outcome_red_stage if rule else None,
        "rule_outcome_green_minute": rule.outcome_green_minute if rule else None,
        "rule_outcome_red_minute": rule.outcome_red_minute if rule else None,
        "rule_outcome_red_if_no_green": rule.outcome_red_if_no_green if rule else None,
        "rule_conditions_json": json.dumps(conditions_payload, ensure_ascii=False),
        "rule_outcome_conditions_json": json.dumps(outcome_payload, ensure_ascii=False),
        "game_id": alert.game_id,
        "league": alert.league,
        "home_team": alert.home_team,
        "away_team": alert.away_team,
        "alert_minute": alert.alert_minute,
        "result_minute": alert.result_minute,
        "result_time_hhmm": alert.result_time_hhmm,
        "initial_score": alert.initial_score,
        "status": alert.status,
        "ht_score": alert.ht_score,
        "ft_score": alert.ft_score,
        "url": alert.url,
        "initial_stats_json": alert.initial_stats_json,
        "ht_stats_json": alert.ht_stats_json,
        "ft_stats_json": alert.ft_stats_json,
    }
    row.update(_flatten_stats("alert_", alert.initial_stats_json))
    row.update(_flatten_stats("ht_", alert.ht_stats_json))
    row.update(_flatten_stats("ft_", alert.ft_stats_json))
    general_path = os.path.join(base_dir, "historico_geral.xlsx")
    rule_path = os.path.join(base_dir, f"regra_{alert.rule_id}.xlsx")
    _upsert_excel(general_path, row, "alert_id")
    _upsert_excel(rule_path, row, "alert_id")
