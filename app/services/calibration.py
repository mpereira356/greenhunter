"""Leakage-safe probability calibration for the V2 shadow score."""

import math
import statistics
from collections import defaultdict


CALIBRATION_METHODS = ("platt", "isotonic", "bucket")
MIN_FAMILY_TRAIN = 300
MIN_FAMILY_VALIDATION = 100
BUCKET_PRIOR_STRENGTH = 12.0


def _clip(value):
    return max(0.000001, min(0.999999, float(value)))


def shrink_score(score, sample=None, uncertainty=None, data_quality=None, temporal_confidence=None):
    """Pull fragile extremes toward 50% once, before empirical calibration."""
    probability = _clip(float(score) / 100.0)
    sample = max(0.0, float(sample or 0))
    sample_reliability = sample / (sample + 6.0)
    uncertainty_reliability = 1.0 - max(0.0, min(1.0, float(uncertainty or 50) / 100.0))
    quality_reliability = max(0.0, min(1.0, float(data_quality or 0) / 100.0))
    temporal_reliability = {"HIGH": 1.0, "MEDIUM": .82, "LOW": .6}.get(
        str(temporal_confidence or "").upper(), .5
    )
    reliability = (.45 * sample_reliability + .25 * uncertainty_reliability
                   + .20 * quality_reliability + .10 * temporal_reliability)
    reliability = max(.2, min(1.0, reliability))
    return _clip(.5 + (probability - .5) * reliability)


def _input(row):
    return shrink_score(
        row["v2_score"], row.get("effective_sample_size"), row.get("uncertainty_score"),
        row.get("data_quality_v2"), row.get("temporal_confidence"),
    )


def _fit_platt(points):
    a, b = 1.0, 0.0
    for _ in range(80):
        g_a = g_b = 0.0
        h_aa = h_ab = h_bb = 0.0
        for x, y in points:
            z = max(-30.0, min(30.0, a * x + b))
            p = 1.0 / (1.0 + math.exp(-z))
            error = p - y
            weight = p * (1 - p)
            g_a += error * x; g_b += error
            h_aa += weight * x * x; h_ab += weight * x; h_bb += weight
        h_aa += .01; h_bb += .01
        determinant = h_aa * h_bb - h_ab * h_ab
        if determinant <= 1e-12:
            break
        delta_a = (h_bb * g_a - h_ab * g_b) / determinant
        delta_b = (-h_ab * g_a + h_aa * g_b) / determinant
        a -= delta_a; b -= delta_b
        if abs(delta_a) + abs(delta_b) < 1e-8:
            break
    return {"method": "platt", "a": a, "b": b}


def _fit_isotonic(points):
    ordered = sorted(points)
    blocks = []
    for x, y in ordered:
        blocks.append([x, x, float(y), 1])
        while len(blocks) > 1 and blocks[-2][2] / blocks[-2][3] > blocks[-1][2] / blocks[-1][3]:
            right = blocks.pop(); left = blocks.pop()
            blocks.append([left[0], right[1], left[2] + right[2], left[3] + right[3]])
    return {"method": "isotonic", "blocks": [
        {"low": low, "high": high, "probability": successes / count, "count": count}
        for low, high, successes, count in blocks
    ]}


def _fit_bucket(points):
    prior = statistics.fmean(y for _, y in points)
    buckets = {}
    for index in range(10):
        selected = [y for x, y in points if min(9, int(x * 10)) == index]
        probability = (sum(selected) + BUCKET_PRIOR_STRENGTH * prior) / (
            len(selected) + BUCKET_PRIOR_STRENGTH
        )
        buckets[str(index)] = {"probability": probability, "count": len(selected)}
    return {"method": "bucket", "prior": prior, "prior_strength": BUCKET_PRIOR_STRENGTH,
            "buckets": buckets}


def fit_calibrator(rows, method):
    points = [(_input(row), int(row["target"])) for row in rows
              if row.get("v2_score") is not None and row.get("target") in (0, 1)]
    if not points:
        raise ValueError("calibrador sem observações resolvidas")
    if method == "platt":
        model = _fit_platt(points)
    elif method == "isotonic":
        model = _fit_isotonic(points)
    elif method == "bucket":
        model = _fit_bucket(points)
    else:
        raise ValueError(f"método de calibração desconhecido: {method}")
    model.update({"sample_count": len(points), "uses_shrinkage": True})
    return model


def predict_calibrated(model, row):
    x = _input(row)
    method = model["method"]
    if method == "platt":
        z = max(-30.0, min(30.0, model["a"] * x + model["b"]))
        return _clip(1.0 / (1.0 + math.exp(-z))) * 100
    if method == "bucket":
        return _clip(model["buckets"][str(min(9, int(x * 10)))]["probability"]) * 100
    blocks = model["blocks"]
    block = min(blocks, key=lambda item: 0 if item["low"] <= x <= item["high"]
                else min(abs(x - item["low"]), abs(x - item["high"])))
    return _clip(block["probability"]) * 100


def calibration_metrics(rows, model):
    values = [(predict_calibrated(model, row) / 100, int(row["target"])) for row in rows
              if row.get("v2_score") is not None and row.get("target") in (0, 1)]
    if not values:
        return {"quantity": 0}
    brier = statistics.fmean((p - y) ** 2 for p, y in values)
    log_loss = statistics.fmean(-(y * math.log(_clip(p)) + (1-y) * math.log(_clip(1-p))) for p, y in values)
    return {"quantity": len(values), "brier": round(brier, 6), "log_loss": round(log_loss, 6)}


def _temporal_split(rows, fraction=.75):
    ordered = sorted(rows, key=lambda row: (row["kickoff_at"], row["fixture_external_id"]))
    fixtures = []
    for row in ordered:
        key = (row["kickoff_at"], row["fixture_external_id"])
        if not fixtures or fixtures[-1] != key:
            fixtures.append(key)
    cut = max(1, min(len(fixtures)-1, int(len(fixtures) * fraction))) if len(fixtures) > 1 else len(fixtures)
    boundary = fixtures[cut] if cut < len(fixtures) else None
    training = [row for row in ordered if boundary is None or (row["kickoff_at"], row["fixture_external_id"]) < boundary]
    validation = [row for row in ordered if boundary is not None and (row["kickoff_at"], row["fixture_external_id"]) >= boundary]
    return training, validation, boundary


def train_temporal_calibration(development_rows):
    """Choose on a later development slice, then refit without evaluation data."""
    training, validation, boundary = _temporal_split(development_rows)
    comparisons = {}
    for method in CALIBRATION_METHODS:
        model = fit_calibrator(training, method)
        comparisons[method] = calibration_metrics(validation, model)
    selected_method = min(
        CALIBRATION_METHODS,
        key=lambda method: (comparisons[method].get("brier", 9), comparisons[method].get("log_loss", 9)),
    )
    global_model = fit_calibrator(development_rows, selected_method)

    family_models = {}
    family_decisions = {}
    grouped = defaultdict(list)
    for row in development_rows:
        grouped[row.get("market_family") or "UNKNOWN"].append(row)
    for family, rows in grouped.items():
        family_train, family_validation, _ = _temporal_split(rows)
        if len(family_train) < MIN_FAMILY_TRAIN or len(family_validation) < MIN_FAMILY_VALIDATION:
            family_decisions[family] = {"source": "GLOBAL_FALLBACK", "reason": "MINIMUM_SAMPLE"}
            continue
        family_candidates = {}
        for method in CALIBRATION_METHODS:
            fitted = fit_calibrator(family_train, method)
            family_candidates[method] = (fitted, calibration_metrics(family_validation, fitted))
        method, (_, family_metrics) = min(
            family_candidates.items(), key=lambda item: (
                item[1][1].get("brier", 9), item[1][1].get("log_loss", 9)
            )
        )
        global_metrics = calibration_metrics(family_validation, global_model)
        if (family_metrics["brier"] <= global_metrics["brier"] - .002
                and family_metrics["log_loss"] <= global_metrics["log_loss"] + .005):
            family_models[family] = fit_calibrator(rows, method)
            family_decisions[family] = {"source": f"FAMILY_{family.upper()}", "method": method,
                                        "validation": family_metrics, "global": global_metrics}
        else:
            family_decisions[family] = {"source": "GLOBAL_FALLBACK", "reason": "NO_VALIDATED_GAIN",
                                        "validation": family_metrics, "global": global_metrics}
    return {
        "version": "calibration_v1", "selected_global_method": selected_method,
        "global_model": global_model, "family_models": family_models,
        "family_decisions": family_decisions, "method_comparison": comparisons,
        "internal_validation_start": boundary[0].isoformat() if boundary else None,
        "minimum_family_train": MIN_FAMILY_TRAIN,
        "minimum_family_validation": MIN_FAMILY_VALIDATION,
    }


def apply_calibration(artifact, row):
    family = row.get("market_family") or "UNKNOWN"
    model = (artifact.get("family_models") or {}).get(family)
    source = f"FAMILY_{family.upper()}" if model else "GLOBAL_FALLBACK"
    return predict_calibrated(model or artifact["global_model"], row), source
