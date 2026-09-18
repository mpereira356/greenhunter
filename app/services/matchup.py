import statistics

from app.services.prediction_features import MIN_MATCHUP_SAMPLE


PAIR_KEYS = {
    "team_goals": ("team_goals", "opponent_goals"),
    "corners": ("team_corners_avg", "opponent_corners"),
    "cards": ("team_cards", "opponent_cards"),
    "shots": ("team_shots", "opponent_shots"),
    "shots_on_target": ("team_shots_on_target", "opponent_shots_on_target"),
    "offsides": ("team_offsides", "opponent_offsides"),
    "fouls": ("team_fouls", "opponent_fouls"),
}


def _values(group, key):
    result = []
    for row in ((group or {}).get("history_values") or {}).get(key) or []:
        raw = row.get("value") if isinstance(row, dict) else row
        try:
            result.append(float(raw))
        except (TypeError, ValueError):
            continue
    return result


def build_matchup_features(candidate, fixture_snapshot):
    scope = candidate.get("scope") or "total"
    group_name = str(candidate.get("marketGroup") or candidate.get("marketType") or "")
    base = next((key for key in PAIR_KEYS if group_name.startswith(key)), None)
    if scope not in {"home", "away"} or not base:
        return {"matchup_available": False, "reason": "NOT_A_TEAM_MARKET", "production_sample": 0, "concession_sample": 0}
    groups = (fixture_snapshot or {}).get("groups") or {}
    team = groups.get("Mandante" if scope == "home" else "Visitante") or {}
    opponent = groups.get("Visitante" if scope == "home" else "Mandante") or {}
    production_key, concession_key = PAIR_KEYS[base]
    production = _values(team, production_key)
    concession = _values(opponent, concession_key)
    production_expected = int(team.get("count") or len(production))
    concession_expected = int(opponent.get("count") or len(concession))
    available = len(production) >= MIN_MATCHUP_SAMPLE and len(concession) >= MIN_MATCHUP_SAMPLE
    return {
        "matchup_available": available, "market_family": base,
        "production_avg": round(statistics.fmean(production), 3) if production else None,
        "opponent_concession_avg": round(statistics.fmean(concession), 3) if concession else None,
        "production_sample": len(production), "concession_sample": len(concession),
        "production_coverage": round(len(production) / production_expected, 4) if production_expected else 0,
        "concession_coverage": round(len(concession) / concession_expected, 4) if concession_expected else 0,
        "combined_expected_value": round((statistics.fmean(production) + statistics.fmean(concession)) / 2, 3) if available else None,
        "reason": None if available else "INSUFFICIENT_CONCESSION_OR_PRODUCTION",
    }
