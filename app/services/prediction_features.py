import math
import statistics
from datetime import datetime


RECENCY_HALF_LIFE_DAYS = 90.0
RECENCY_MIN_WEIGHT = 0.25
MIN_MATCHUP_SAMPLE = 3


def _number(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _date(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
    except (TypeError, ValueError):
        return None


def effective_sample_size(weights):
    valid = [float(weight) for weight in weights if _number(weight) is not None and float(weight) > 0]
    denominator = sum(weight * weight for weight in valid)
    return (sum(valid) ** 2 / denominator) if denominator else 0.0


def recency_weight(age_days, half_life_days=RECENCY_HALF_LIFE_DAYS):
    if age_days is None or age_days < 0:
        return None
    decay = math.log(2) / max(1.0, float(half_life_days))
    return max(RECENCY_MIN_WEIGHT, math.exp(-decay * age_days))


def _hit(value, line, direction):
    return value < line if direction == "under" else value > line


def _streak(hits, wanted):
    count = 0
    for hit in hits:
        if hit is wanted:
            count += 1
        else:
            break
    return count


def series_features(rows, line, direction, target_at, expected_count=None):
    observations = []
    future_excluded = 0
    for position, raw in enumerate(rows or []):
        row = raw if isinstance(raw, dict) else {"value": raw}
        value = _number(row.get("value"))
        if value is None:
            continue
        kickoff = _date(row.get("kickoff_at")) if row.get("historical_date_available") else None
        if kickoff and target_at and kickoff >= target_at:
            future_excluded += 1
            continue
        observations.append((position, value, kickoff, row))
    values = [item[1] for item in observations]
    known = len(values)
    expected = max(known, int(expected_count or known))
    unknown = max(0, expected - known)
    if not values:
        return {
            "raw_sample_size": 0, "effective_sample_size": 0, "known_count": 0,
            "unknown_count": unknown, "coverage": 0, "temporal_count": 0,
            "raw_frequency": None, "recent_weighted_frequency": None,
            "future_rows_excluded": future_excluded,
            "max_source_timestamp": None,
        }
    hits = [_hit(value, line, direction) for value in values]
    dated = [(value, kickoff) for _, value, kickoff, _ in observations if kickoff is not None]
    weights = []
    weighted_hits = []
    if target_at:
        for value, kickoff in dated:
            weight = recency_weight((target_at - kickoff).total_seconds() / 86400)
            if weight is not None:
                weights.append(weight)
                weighted_hits.append(weight * int(_hit(value, line, direction)))
    recent = sum(weighted_hits) / sum(weights) if weights else None
    average = statistics.fmean(values)
    variance = statistics.pvariance(values) if len(values) > 1 else 0.0
    stddev = math.sqrt(variance)
    chronological = [value for _, value, kickoff, _ in sorted(
        observations, key=lambda item: item[2] or datetime.min
    ) if kickoff is not None]
    trend = None
    if len(chronological) >= 2:
        x_mean = (len(chronological) - 1) / 2
        denominator = sum((index - x_mean) ** 2 for index in range(len(chronological)))
        trend = sum((index - x_mean) * (value - statistics.fmean(chronological)) for index, value in enumerate(chronological)) / denominator if denominator else 0
    return {
        "raw_sample_size": known,
        "effective_sample_size": round(effective_sample_size(weights), 3) if weights else None,
        "known_count": known, "unknown_count": unknown, "coverage": round(known / expected, 4) if expected else 0,
        "temporal_count": len(weights), "temporal_coverage": round(len(weights) / known, 4),
        "raw_frequency": round(sum(hits) / known, 4),
        "recent_weighted_frequency": round(recent, 4) if recent is not None else None,
        "mean": round(average, 4), "median": round(statistics.median(values), 4),
        "minimum": min(values), "maximum": max(values), "range": max(values) - min(values),
        "variance": round(variance, 4), "stddev": round(stddev, 4),
        "coefficient_of_variation": round(stddev / abs(average), 4) if average else None,
        "trend": round(trend, 4) if trend is not None else None,
        "hit_streak": _streak(hits, True), "miss_streak": _streak(hits, False),
        "nearby_lines": {
            str(round(line - 1, 2)): round(sum(_hit(value, line - 1, direction) for value in values) / known, 4),
            str(round(line, 2)): round(sum(hits) / known, 4),
            str(round(line + 1, 2)): round(sum(_hit(value, line + 1, direction) for value in values) / known, 4),
        },
        "future_rows_excluded": future_excluded,
        "max_source_timestamp": max((kickoff for _, _, kickoff, _ in observations if kickoff), default=None).isoformat()
        if any(kickoff for _, _, kickoff, _ in observations) else None,
    }


def market_history_key(candidate):
    market = str(candidate.get("marketType") or "")
    group = str(candidate.get("marketGroup") or market)
    scope = candidate.get("scope") or "total"
    if market in {"over05", "over15", "over25", "btts", "goal_ht"}:
        return {"btts": "btts", "goal_ht": "goal_ht"}.get(market, market)
    if group.startswith("team_goals"):
        return "team_goals"
    base = next((name for name in ("shots_on_target", "corners", "cards", "shots", "fouls", "offsides") if group.startswith(name)), market)
    period = "_1h" if "_1h" in group else "_2h" if "_2h" in group else ""
    if period:
        period_names = {
            "corners": "team_corners_1h" if scope != "total" else "corners_1h",
            "cards": "team_cards_1h" if scope != "total" else "cards_1h",
            "shots": "team_shots_1h" if scope != "total" else "shots_1h",
            "shots_on_target": "team_on_target_1h" if scope != "total" else "on_target_1h",
        }
        return period_names.get(base, f"{base}{period}")
    if scope != "total":
        return {"corners": "team_corners_avg"}.get(base, f"team_{base}")
    return f"{base}_avg"


def build_feature_snapshot(candidate, fixture_snapshot, strength=None, matchup=None):
    target_at = _date(candidate.get("kickoffAt"))
    groups = (fixture_snapshot or {}).get("groups") or {}
    history_key = market_history_key(candidate)
    line = _number(candidate.get("line")) or 0
    direction = candidate.get("direction") or "over"
    scope = candidate.get("scope") or "total"
    features = {}
    for label, key in (("h2h", "H2H"), ("home", "Mandante"), ("away", "Visitante")):
        group = groups.get(key) or {}
        history_values = group.get("history_values") or {}
        selected_key = history_key
        if label == "h2h" and scope in {"home", "away"}:
            selected_key = {
                "team_goals": f"team_goals_{scope}",
                "team_corners_avg": f"team_corners_{scope}",
            }.get(history_key, history_key)
        rows = history_values.get(selected_key) or []
        features[label] = series_features(rows, line, direction, target_at, group.get("count"))
    relevant = features["home"] if scope == "home" else features["away"] if scope == "away" else None
    if relevant is None:
        form = [features["home"], features["away"]]
        known = [item["raw_frequency"] for item in form if item.get("raw_frequency") is not None]
        relevant_frequency = statistics.fmean(known) if known else None
    else:
        relevant_frequency = relevant.get("raw_frequency")
    temporal_total = sum(item.get("temporal_count") or 0 for item in features.values())
    known_total = sum(item.get("known_count") or 0 for item in features.values())
    identity_rows = []
    for group in groups.values():
        for rows in ((group or {}).get("history_values") or {}).values():
            identity_rows.extend(row for row in (rows or []) if isinstance(row, dict) and row.get("external_id"))
    identity_rows = {str(row.get("external_id")): row for row in identity_rows}.values()
    identity_coverage = (
        sum(bool(row.get("home_external_id") and row.get("away_external_id")) for row in identity_rows)
        / len(identity_rows) if identity_rows else 0
    )
    temporal_reliability = "RELIABLE" if temporal_total >= 6 else "PARTIAL" if temporal_total else "UNAVAILABLE"
    divergence_values = [item["raw_frequency"] for item in features.values() if item.get("raw_frequency") is not None]
    divergence = max(divergence_values) - min(divergence_values) if len(divergence_values) > 1 else 0
    primary = relevant or features["home"]
    cv = primary.get("coefficient_of_variation")
    primary_sample = primary.get("effective_sample_size")
    if primary_sample is None:
        primary_sample = primary.get("raw_sample_size") or 0
    sample_uncertainty = max(0, 1 - min(1, primary_sample / 10))
    uncertainty_parts = {
        "sample": sample_uncertainty,
        "temporal": 1 - (temporal_total / known_total if known_total else 0),
        "coverage": 1 - statistics.fmean([item.get("coverage", 0) for item in features.values()]),
        "dispersion": min(1, (cv if cv is not None else 1) / 1.5),
        "divergence": min(1, divergence),
        "strength": 0 if (strength or {}).get("available") else 1,
        "matchup": 0 if (matchup or {}).get("matchup_available") else 1,
    }
    uncertainty = (
        uncertainty_parts["sample"] * .20 + uncertainty_parts["temporal"] * .20
        + uncertainty_parts["coverage"] * .15 + uncertainty_parts["dispersion"] * .15
        + uncertainty_parts["divergence"] * .10 + uncertainty_parts["strength"] * .10
        + uncertainty_parts["matchup"] * .10
    )
    quality = (
        (1 - uncertainty_parts["coverage"]) * .25 + min(1, known_total / 18) * .20
        + (1 - uncertainty_parts["temporal"]) * .20
        + (1 if (strength or {}).get("available") else 0) * .10
        + (1 if (matchup or {}).get("matchup_available") else 0) * .15
        + (1 - min(1, divergence)) * .05 + identity_coverage * .05
    )
    h2h = features["h2h"].get("raw_frequency")
    recent_values = [item.get("recent_weighted_frequency") for item in features.values() if item.get("recent_weighted_frequency") is not None]
    recent = statistics.fmean(recent_values) if recent_values else None
    evidence = [
        (relevant_frequency, .70), (recent, .20), (h2h, .10),
    ]
    evidence = [(value, weight) for value, weight in evidence if value is not None]
    preliminary = sum(value * weight for value, weight in evidence) / sum(weight for _, weight in evidence) if evidence else None
    # O score é experimental e deliberadamente não usa Elo como ajuste causal.
    # Incerteza reduz a força da evidência em direção ao ponto neutro de 50%.
    score = 0.5 + (preliminary - 0.5) * (1 - uncertainty * .45) if preliminary is not None else None
    source_timestamps = [item.get("max_source_timestamp") for item in features.values() if item.get("max_source_timestamp")]
    return {
        "schema_version": 1, "history_key": history_key, "line": line, "direction": direction,
        "h2h_frequency": h2h, "home_form_frequency": features["home"].get("raw_frequency"),
        "away_form_frequency": features["away"].get("raw_frequency"),
        "team_relevant_frequency": relevant_frequency, "series": features,
        "strength": strength or {"available": False}, "matchup": matchup or {"matchup_available": False},
        "uncertainty_components": uncertainty_parts, "uncertainty_score": round(uncertainty, 4),
        "data_quality_v2": round(quality, 4), "temporal_reliability": temporal_reliability,
        "v2_statistical_score": round(score * 100, 2) if score is not None else None,
        "provenance": {"future_rows_excluded": sum(item.get("future_rows_excluded") or 0 for item in features.values()),
                       "undated_rows_not_used_for_decay": known_total - temporal_total,
                       "identity_coverage": round(identity_coverage, 4),
                       "temporal_features_use_explicit_dates_only": True,
                       "max_source_timestamp": max(source_timestamps) if source_timestamps else None},
    }
