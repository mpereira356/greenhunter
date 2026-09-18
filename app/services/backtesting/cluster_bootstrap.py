"""Reproducible uncertainty intervals clustered by fixture."""

import math
import random
import statistics
from collections import defaultdict

DEFAULT_SEED = 20260918
DEFAULT_REPETITIONS = 2000


def _percentile(values, proportion):
    ordered = sorted(values)
    if not ordered:
        return None
    position = (len(ordered) - 1) * proportion
    low = int(position); high = min(len(ordered) - 1, low + 1)
    weight = position - low
    return ordered[low] * (1 - weight) + ordered[high] * weight


def _loss(score, target, kind):
    probability = max(.000001, min(.999999, float(score) / 100))
    if kind == "brier":
        return (probability - target) ** 2
    return -(target * math.log(probability) + (1-target) * math.log(1-probability))


def _ranking_metrics(rows, score_key):
    points = sorted((float(row[score_key]), int(row["target"])) for row in rows)
    positives = sum(target for _, target in points); negatives = len(points) - positives
    if not positives or not negatives:
        return None, None
    rank_sum = 0.0; index = 0
    while index < len(points):
        end = index + 1
        while end < len(points) and points[end][0] == points[index][0]:
            end += 1
        average_rank = ((index + 1) + end) / 2
        rank_sum += average_rank * sum(target for _, target in points[index:end])
        index = end
    auc = (rank_sum - positives * (positives + 1) / 2) / (positives * negatives)
    tp = 0; previous_recall = 0.0; pr_auc = 0.0
    for rank, (_, target) in enumerate(reversed(points), 1):
        tp += target
        recall = tp / positives
        pr_auc += (recall - previous_recall) * (tp / rank)
        previous_recall = recall
    return auc, pr_auc


def cluster_bootstrap(rows, challenger_key, baseline_key="legacy_score",
                      fixture_key="fixture_external_id", repetitions=DEFAULT_REPETITIONS,
                      seed=DEFAULT_SEED):
    eligible = [row for row in rows if row.get("target") in (0, 1)
                and row.get(baseline_key) is not None and row.get(challenger_key) is not None]
    grouped = defaultdict(list)
    for row in eligible:
        grouped[str(row[fixture_key])].append(row)
    fixtures = sorted(grouped)
    rng = random.Random(seed)
    deltas = {"brier": [], "log_loss": [], "roc_auc": [], "pr_auc": []}
    for _ in range(max(1, int(repetitions))):
        sampled = [row for _fixture in range(len(fixtures))
                   for row in grouped[rng.choice(fixtures)]]
        for kind in ("brier", "log_loss"):
            delta = statistics.fmean(
                _loss(row[challenger_key], row["target"], kind)
                - _loss(row[baseline_key], row["target"], kind) for row in sampled
            )
            deltas[kind].append(delta)
        baseline_auc, baseline_pr = _ranking_metrics(sampled, baseline_key)
        challenger_auc, challenger_pr = _ranking_metrics(sampled, challenger_key)
        if baseline_auc is not None and challenger_auc is not None:
            deltas["roc_auc"].append(challenger_auc - baseline_auc)
            deltas["pr_auc"].append(challenger_pr - baseline_pr)
    result = {
        "seed": seed, "repetitions": repetitions, "fixture_clusters": len(fixtures),
        "candidate_count": len(eligible), "comparison": f"{challenger_key} minus {baseline_key}",
    }
    for metric, values in deltas.items():
        result[metric] = {
            "mean_delta": round(statistics.fmean(values), 6) if values else None,
            "ci95_low": round(_percentile(values, .025), 6) if values else None,
            "ci95_high": round(_percentile(values, .975), 6) if values else None,
        }
    return result
