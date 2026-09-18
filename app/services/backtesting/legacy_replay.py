"""Frozen Python replay of legacy_v1's candidate evaluator for backtests."""

import math
import statistics


RECENCY_WEIGHTS = [1, .95, .90, .85, .80, .75, .70, .65, .60, .55]
SAMPLE_CONFIDENCE = [0, 15, 30, 45, 55, 65, 75, 82, 88, 94, 100]
BASE_WEIGHTS = {
    "total": {"home": .40, "away": .40, "h2h": .20},
    "home": {"home": .85, "h2h": .15},
    "away": {"away": .85, "h2h": .15},
    "h2h": {"h2h": 1},
}


def _clamp(value, low=0, high=100):
    return max(low, min(high, float(value or 0)))


def _round1(value):
    return round(float(value) + 1e-12, 1)


def sample_score(samples):
    return SAMPLE_CONFIDENCE[max(0, min(10, int(samples or 0)))]


def h2h_credibility(samples):
    return {0: 0, 1: .15, 2: .30, 3: .50, 4: .70, 5: .85, 6: 1}[min(6, max(0, int(samples or 0)))]


def series_stats(values, line, direction="over"):
    clean = []
    for value in values or []:
        try:
            value = float(value)
            if math.isfinite(value):
                clean.append(value)
        except (TypeError, ValueError):
            pass
    clean = clean[:10]
    if not clean:
        return None
    predicate = (lambda value: value < line) if direction == "under" else (lambda value: value > line)
    hits = sum(predicate(value) for value in clean)
    weighted_hits = sum((RECENCY_WEIGHTS[index] if predicate(value) else 0) for index, value in enumerate(clean))
    weight_total = sum(RECENCY_WEIGHTS[:len(clean)])
    return {
        "samples": len(clean), "hits": hits, "raw": hits / len(clean) * 100,
        "credible": (hits + 2) / (len(clean) + 4) * 100,
        "recent": weighted_hits / weight_total * 100,
    }


def _consistency(probabilities):
    if not probabilities:
        return 0
    if len(probabilities) == 1:
        return 70
    spread = max(probabilities) - min(probabilities)
    deviation = statistics.pstdev(probabilities)
    range_score = 100 if spread <= 10 else 90 if spread <= 20 else 75 if spread <= 30 else 55 if spread <= 40 else 35 if spread <= 50 else 10
    return _round1(_clamp(range_score - max(0, deviation - 8) * .6))


def _weighted(stats, scope, field):
    entries = []
    for key, base_weight in BASE_WEIGHTS.get(scope, BASE_WEIGHTS["total"]).items():
        stat = stats.get(key)
        if not stat:
            continue
        credibility = sample_score(stat["samples"]) / 100
        if key == "h2h":
            credibility *= h2h_credibility(stat["samples"])
        if credibility > 0:
            entries.append((stat[field], base_weight * credibility))
    total = sum(weight for _, weight in entries)
    return sum(value * weight for value, weight in entries) / total if total else None


def evaluate_legacy(bases, line, scope="total", direction="over", supporting_score=None, context_score=None):
    stats = {key: series_stats((bases or {}).get(key), line, direction) for key in ("h2h", "home", "away")}
    raw = _weighted(stats, scope, "raw")
    recent = _weighted(stats, scope, "recent")
    credible = _weighted(stats, scope, "credible")
    probabilities = [item["raw"] for item in stats.values() if item and item["samples"] >= 3]
    consistency = _consistency(probabilities)
    sample = sample_score(_weighted(stats, scope, "samples") or 0)
    adjusted = None if raw is None or recent is None or credible is None else _round1(
        (credible * .60 + raw * .25 + recent * .15) * (.92 + consistency / 1250)
    )
    primary = ["home", "away"] if scope == "total" else [scope]
    primary_coverage = sum(bool(stats.get(key) and stats[key]["samples"] >= 3) for key in primary) / len(primary)
    h2h = stats.get("h2h")
    all_stats = [item for item in stats.values() if item]
    sample_component = statistics.fmean(sample_score(item["samples"]) for item in all_stats) if all_stats else 0
    quality = _round1(_clamp(primary_coverage * 65 + (min(15, h2h_credibility(h2h["samples"]) * 15) if h2h else 0)
                            + sample_component * .15 + (20 if supporting_score is not None else 8)))
    support = 55 if supporting_score is None else _clamp(supporting_score)
    components = [(adjusted, .30), (consistency, .15), (sample, .10), (recent, .10), (support, .15), (context_score, .20)]
    components = [(value, weight) for value, weight in components if value is not None]
    total_weight = sum(weight for _, weight in components)
    confidence = sum(value * weight for value, weight in components) / total_weight if total_weight else 0
    confidence = _round1(_clamp(confidence * (.75 + quality / 400)))
    reasons = []
    if not any(stats.values()) or not any(stats.get(key) and stats[key]["samples"] >= 3 for key in primary):
        reasons.append("INSUFFICIENT_DATA")
    if raw is None or raw < 64:
        reasons.append("LOW_RAW_PROBABILITY")
    if adjusted is None or adjusted < 72:
        reasons.append("LOW_ADJUSTED_PROBABILITY")
    if quality < 55:
        reasons.append("LOW_DATA_QUALITY")
    if scope == "total":
        if stats.get("home") and stats["home"]["samples"] >= 3 and stats["home"]["raw"] < 60:
            reasons.append("WEAK_HOME_BASE")
        if stats.get("away") and stats["away"]["samples"] >= 3 and stats["away"]["raw"] < 60:
            reasons.append("WEAK_AWAY_BASE")
    if len(probabilities) > 1 and max(probabilities) - min(probabilities) > 50:
        reasons.append("HIGH_DIVERGENCE")
    if "HIGH_DIVERGENCE" in reasons:
        confidence -= 12
    if "WEAK_HOME_BASE" in reasons or "WEAK_AWAY_BASE" in reasons:
        confidence -= 10
    confidence = _round1(_clamp(confidence))
    if confidence < 75:
        reasons.append("LOW_CONFIDENCE")
    return {
        "raw_probability": _round1(raw or 0), "recent_score": _round1(recent or 0),
        "adjusted_probability": _round1(adjusted or 0), "confidence_score": confidence,
        "data_quality_score": quality, "consistency_score": consistency,
        "status": "REJECTED" if reasons else "APPROVED", "rejection_reasons": reasons,
    }
