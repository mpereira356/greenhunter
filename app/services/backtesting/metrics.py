import math
from collections import defaultdict


BUCKETS = ((50, 59), (60, 69), (70, 79), (80, 89), (90, 100))


def score_bucket(score):
    if score is None:
        return "UNKNOWN"
    value = float(score)
    for low, high in BUCKETS:
        if low <= value <= high + .999999:
            return f"{low}-{high}"
    return "<50" if value < 50 else ">100"


def _auc(points):
    positives = [score for score, target in points if target == 1]
    negatives = [score for score, target in points if target == 0]
    if not positives or not negatives:
        return None
    wins = sum(1 if positive > negative else .5 if positive == negative else 0
               for positive in positives for negative in negatives)
    return wins / (len(positives) * len(negatives))


def _pr_auc(points):
    positives = sum(target for _, target in points)
    if not positives:
        return None
    ordered = sorted(points, reverse=True)
    tp = fp = 0
    previous_recall = 0
    area = 0
    for _, target in ordered:
        tp += int(target == 1); fp += int(target == 0)
        recall = tp / positives
        precision = tp / (tp + fp)
        area += (recall - previous_recall) * precision
        previous_recall = recall
    return area


def binary_metrics(rows, score_key):
    usable = [(float(row[score_key]), int(row["target"])) for row in rows
              if row.get(score_key) is not None and row.get("target") in (0, 1)]
    if not usable:
        return {"quantity": 0}
    probabilities = [(max(.000001, min(.999999, score / 100)), target) for score, target in usable]
    brier = sum((probability - target) ** 2 for probability, target in probabilities) / len(probabilities)
    log_loss = -sum(target * math.log(probability) + (1 - target) * math.log(1 - probability)
                    for probability, target in probabilities) / len(probabilities)
    buckets = defaultdict(list)
    for score, target in usable:
        buckets[score_bucket(score)].append((score, target))
    bucket_report = {}
    weighted_error = 0
    for name, values in buckets.items():
        actual = sum(target for _, target in values) / len(values)
        predicted = sum(score for score, _ in values) / len(values) / 100
        weighted_error += len(values) / len(usable) * abs(predicted - actual)
        bucket_report[name] = {
            "quantity": len(values), "green": sum(target for _, target in values),
            "red": sum(1 - target for _, target in values), "actual_rate": round(actual * 100, 2),
            "mean_score": round(predicted * 100, 2),
        }
    return {
        "quantity": len(usable), "green": sum(target for _, target in usable),
        "red": sum(1 - target for _, target in usable),
        "hit_rate": round(sum(target for _, target in usable) / len(usable) * 100, 3),
        "brier": round(brier, 6), "log_loss": round(log_loss, 6),
        "calibration_error": round(weighted_error, 6),
        "roc_auc": round(_auc(usable), 6) if _auc(usable) is not None else None,
        "pr_auc": round(_pr_auc(usable), 6) if _pr_auc(usable) is not None else None,
        "buckets": dict(sorted(bucket_report.items())),
        "warning": "Scores avaliados experimentalmente como probabilidades não calibradas.",
    }


def grouped_rates(rows, key):
    groups = defaultdict(list)
    for row in rows:
        groups[str(row.get(key) if row.get(key) is not None else "UNKNOWN")].append(row)
    result = {}
    for name, values in groups.items():
        resolved = [row for row in values if row.get("target") in (0, 1)]
        result[name] = {
            "quantity": len(values), "resolved": len(resolved),
            "green": sum(row["target"] for row in resolved),
            "red": sum(1 - row["target"] for row in resolved),
            "hit_rate": round(sum(row["target"] for row in resolved) / len(resolved) * 100, 2) if resolved else None,
            "conclusion_eligible": len(resolved) >= 30,
        }
    return dict(sorted(result.items(), key=lambda item: (-item[1]["quantity"], item[0])))
