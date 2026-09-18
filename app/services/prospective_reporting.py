"""Administrative JSON reporting for the frozen C3 prospective window."""

import json
import os
import statistics
from collections import Counter, defaultdict
from datetime import timedelta
from pathlib import Path

from app.models import BacktestObservation, MarketOddsSnapshot, MarketPrediction, PredictionRun
from app.services.backtesting.cluster_bootstrap import cluster_bootstrap
from app.services.backtesting.metrics import binary_metrics, grouped_rates
from app.services.frozen_shadow import FROZEN_VERSION
from app.utils.time import now_sp


def _prediction_rows(start_date=None, end_date=None):
    query = MarketPrediction.query.join(PredictionRun, MarketPrediction.run_id == PredictionRun.id).filter(
        PredictionRun.run_type == "PROSPECTIVE_SHADOW",
        MarketPrediction.model_version == "greenhunter_v2_shadow",
        MarketPrediction.configuration_version == FROZEN_VERSION,
        MarketPrediction.official_pre_match_snapshot.is_(True),
        MarketPrediction.prospective_validity == "VALID",
    )
    if start_date: query = query.filter(MarketPrediction.target_date >= start_date)
    if end_date: query = query.filter(MarketPrediction.target_date <= end_date)
    predictions = query.order_by(MarketPrediction.target_date, MarketPrediction.id).all()
    legacy_ids = [row.legacy_prediction_id for row in predictions if row.legacy_prediction_id]
    legacy = {row.id: row for row in MarketPrediction.query.filter(MarketPrediction.id.in_(legacy_ids)).all()} if legacy_ids else {}
    rows = []
    for prediction in predictions:
        linked = legacy.get(prediction.legacy_prediction_id)
        target = 1 if prediction.settlement_status == "GREEN" else 0 if prediction.settlement_status == "RED" else None
        try: features = json.loads(prediction.feature_snapshot_json or "{}")
        except (TypeError, ValueError): features = {}
        rows.append({
            "fixture_external_id": prediction.fixture_id, "target_date": prediction.target_date,
            "league": prediction.competition, "market_family": _family(prediction),
            "settlement": prediction.settlement_status, "target": target,
            "legacy_score": linked.confidence_score if linked else None,
            "legacy_status": linked.status if linked else None,
            "v2_score": prediction.v2_statistical_score,
            "v2_calibrated_probability": prediction.v2_calibrated_probability,
            "uncertainty_score": prediction.uncertainty_score,
            "data_quality_v2": prediction.data_quality_v2,
            "selected_conservative": prediction.selected_conservative,
            "selected_balanced": prediction.selected_balanced,
            "strength_available": bool((features.get("strength") or {}).get("available")),
            "matchup_available": bool((features.get("matchup") or {}).get("matchup_available")),
        })
    return rows


def _family(prediction):
    market = str(prediction.market_type or "").casefold()
    group = str(prediction.market_group or market).casefold()
    if market == "goal_ht": return "goals_first_half"
    if market.startswith("over") or group.startswith("team_goals"): return "goals"
    return next((name for name in ("shots_on_target", "corners", "cards", "shots", "fouls", "offsides")
                 if name in group or name in market), group or market)


def _profile(rows, field):
    selected = [row for row in rows if row[field]]
    counts = Counter(row["settlement"] for row in selected)
    resolved = counts["GREEN"] + counts["RED"]
    return {"selected": len(selected), "green": counts["GREEN"], "red": counts["RED"],
            "pending": counts["PENDING"], "void": counts["VOID"], "unresolved": counts["UNRESOLVED"],
            "hit_rate": round(counts["GREEN"] / resolved * 100, 2) if resolved else None}


def _coverage(rows, field):
    return round(sum(bool(row[field]) for row in rows) / len(rows) * 100, 2) if rows else None


def _metrics(rows):
    counts = Counter(row["settlement"] for row in rows)
    return {
        "fixtures": len({row["fixture_external_id"] for row in rows}), "candidates": len(rows),
        "green": counts["GREEN"], "red": counts["RED"], "pending": counts["PENDING"],
        "void": counts["VOID"], "unresolved": counts["UNRESOLVED"],
        "legacy": binary_metrics(rows, "legacy_score"),
        "v2_raw": binary_metrics(rows, "v2_score"),
        "v2_calibrated": binary_metrics(rows, "v2_calibrated_probability"),
        "conservative": _profile(rows, "selected_conservative"),
        "balanced": _profile(rows, "selected_balanced"),
        "strength_coverage_percent": _coverage(rows, "strength_available"),
        "matchup_coverage_percent": _coverage(rows, "matchup_available"),
        "by_family": grouped_rates(rows, "market_family"),
        "by_league": {key: value for key, value in grouped_rates(rows, "league").items()
                      if value["resolved"] >= 30},
    }


def _drift(rows):
    baseline = BacktestObservation.query.filter_by(backtest_run_id=3, dataset_partition="evaluation").all()
    baseline_rows = [{"score": row.v2_calibrated_probability, "uncertainty": row.uncertainty_score,
                      "quality": row.data_quality_v2, "target": row.target,
                      "strength": row.strength_available, "matchup": row.matchup_available,
                      "family": row.market_family} for row in baseline]
    current = [{"score": row.get("v2_calibrated_probability"), "uncertainty": row.get("uncertainty_score"),
                "quality": row.get("data_quality_v2"), "target": row.get("target"),
                "strength": row.get("strength_available"), "matchup": row.get("matchup_available"),
                "family": row.get("market_family")} for row in rows]
    def mean(data, key):
        values = [float(row[key]) for row in data if row.get(key) is not None]
        return statistics.fmean(values) if values else None
    comparisons = {}
    thresholds = {"score": 10, "uncertainty": 10, "quality": 10, "target": .10,
                  "strength": .15, "matchup": .15}
    flags = []
    for key, threshold in thresholds.items():
        before, after = mean(baseline_rows, key), mean(current, key)
        delta = after - before if before is not None and after is not None else None
        comparisons[key] = {"development": before, "prospective": after, "delta": delta}
        if delta is not None and abs(delta) >= threshold: flags.append(key)
    return {"status": "POSSIBLE_DRIFT" if flags else "NO_DRIFT_SIGNAL",
            "signals": flags, "comparisons": comparisons,
            "note": "Monitoramento descritivo; nunca altera o modelo automaticamente."}


def report_for_period(days=None, end_date=None, include_bootstrap_if_checkpoint=True):
    end = end_date or now_sp().date().isoformat()
    start = (now_sp().date() - timedelta(days=days-1)).isoformat() if days else None
    rows = _prediction_rows(start, end)
    report = _metrics(rows)
    report.update({"start_date": start, "end_date": end, "window_days": days or "ALL",
                   "drift": _drift(rows)})
    complete_dates = 0
    for day in {row["target_date"] for row in rows}:
        day_rows = [row for row in rows if row["target_date"] == day]
        if day_rows and not any(row["settlement"] == "PENDING" for row in day_rows): complete_dates += 1
    resolved = report["green"] + report["red"]
    resolved_fixtures = len({row["fixture_external_id"] for row in rows if row["target"] in (0, 1)})
    checkpoint = complete_dates >= 14 or resolved_fixtures >= 500 or resolved >= 8000
    report["checkpoint"] = {"ready": checkpoint, "complete_days": complete_dates,
                            "resolved_fixtures": resolved_fixtures,
                            "resolved_candidates": resolved,
                            "conditions": {"days_14": complete_dates >= 14,
                                           "fixtures_500": resolved_fixtures >= 500,
                                           "candidates_8000": resolved >= 8000}}
    if checkpoint and include_bootstrap_if_checkpoint:
        report["checkpoint"]["cluster_bootstrap"] = cluster_bootstrap(
            rows, "v2_calibrated_probability", repetitions=2000, seed=20260918)
    odds = MarketOddsSnapshot.query.filter(MarketOddsSnapshot.captured_at <= now_sp()).all()
    report["odds"] = {"captured": len(odds), "bookmakers": dict(Counter(row.bookmaker for row in odds))}
    return report


def daily_report(day):
    rows = _prediction_rows(day, day)
    report = _metrics(rows)
    run = PredictionRun.query.filter_by(algorithm="daily_shadow_observer", target_date=day).order_by(
        PredictionRun.id.desc()).first()
    try: operations = json.loads(run.operational_metrics_json or "{}") if run else {}
    except (TypeError, ValueError): operations = {}
    return {"date": day, **report, "operations": operations,
            "collection_alert": run.alert_status if run else "DAILY_RUN_MISSING"}


def write_report_files(day, directory="data/prospective_reports"):
    root = Path(directory); root.mkdir(parents=True, exist_ok=True)
    reports = {f"daily-{day}.json": daily_report(day)}
    for days in (7, 14, 30):
        reports[f"rolling-{days}d.json"] = report_for_period(days, include_bootstrap_if_checkpoint=False)
    reports["all.json"] = report_for_period(None, include_bootstrap_if_checkpoint=False)
    for filename, payload in reports.items():
        target = root / filename
        temporary = root / f".{filename}.{os.getpid()}.tmp"
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        os.replace(temporary, target)
    return [str(root / filename) for filename in reports]
