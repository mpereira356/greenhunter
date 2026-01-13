import os

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


def export_alert(alert, rule_name: str, base_dir: str):
    _ensure_dir(base_dir)
    row = {
        "alert_id": alert.id,
        "rule_id": alert.rule_id,
        "rule_name": rule_name,
        "user_id": alert.user_id,
        "created_at": alert.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "league": alert.league,
        "home_team": alert.home_team,
        "away_team": alert.away_team,
        "alert_minute": alert.alert_minute,
        "initial_score": alert.initial_score,
        "status": alert.status,
        "ht_score": alert.ht_score,
        "ft_score": alert.ft_score,
        "url": alert.url,
        "initial_stats_json": alert.initial_stats_json,
        "ht_stats_json": alert.ht_stats_json,
        "ft_stats_json": alert.ft_stats_json,
    }
    general_path = os.path.join(base_dir, "historico_geral.xlsx")
    rule_path = os.path.join(base_dir, f"regra_{alert.rule_id}.xlsx")
    _upsert_excel(general_path, row, "alert_id")
    _upsert_excel(rule_path, row, "alert_id")
