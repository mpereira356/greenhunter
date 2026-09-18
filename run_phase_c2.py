#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path

os.environ.setdefault("DISABLE_WORKER", "1")

from app import create_app
from app.extensions import db
from app.models import BacktestObservation, BacktestRun, CalibrationArtifact
from app.services.backtesting.cluster_bootstrap import cluster_bootstrap
from app.services.backtesting.metrics import binary_metrics, grouped_rates
from app.services.calibration import apply_calibration, train_temporal_calibration


def _rows(observations):
    result = []
    for item in observations:
        ablations = json.loads(item.ablation_scores_json or "{}")
        def combined(*values):
            return max(0, min(100, sum(values))) if all(value is not None for value in values) else None
        base = ablations.get("A_base")
        recency = ablations.get("B_recency")
        strength = ablations.get("C_strength")
        matchup = ablations.get("D_matchup")
        uncertainty = ablations.get("E_dispersion_uncertainty")
        complete = ablations.get("G_v2_complete")
        # Controlled marginal ablations derived from the frozen A-G scores.
        # They do not tune new weights or overwrite the raw V2 score.
        ablations["H_recency_uncertainty"] = combined(recency, uncertainty, -base) if base is not None else None
        ablations["I_complete_without_strength"] = combined(complete, base, -strength) if base is not None else None
        ablations["J_complete_without_matchup"] = combined(complete, base, -matchup) if base is not None else None
        result.append({
            "id": item.id, "fixture_external_id": item.fixture_external_id,
            "kickoff_at": item.kickoff_at, "market_family": item.market_family,
            "legacy_score": item.legacy_score, "v2_score": item.v2_score,
            "uncertainty_score": item.uncertainty_score,
            "data_quality_v2": item.data_quality_v2,
            "effective_sample_size": item.effective_sample_size,
            "temporal_confidence": item.temporal_confidence,
            "target": item.target, "settlement": item.settlement,
            "dataset_partition": item.dataset_partition,
            "ablations": ablations,
        })
    return result


def _component_configuration(development_rows):
    families = sorted({row["market_family"] for row in development_rows})
    result = {}
    components = {
        "A_base": [], "B_recency": ["recency"], "C_strength": ["strength"],
        "D_matchup": ["matchup"], "E_dispersion_uncertainty": ["uncertainty"],
        "F_recency_strength_matchup": ["recency", "strength", "matchup"],
        "G_v2_complete": ["recency", "strength", "matchup", "uncertainty"],
        "H_recency_uncertainty": ["recency", "uncertainty"],
        "I_complete_without_strength": ["recency", "matchup", "uncertainty"],
        "J_complete_without_matchup": ["recency", "strength", "uncertainty"],
    }
    for family in families:
        selected = [row for row in development_rows if row["market_family"] == family
                    and row.get("target") in (0, 1)]
        metrics = {}
        for variant in components:
            scored = [{"score": row["ablations"].get(variant), "target": row["target"]} for row in selected]
            metrics[variant] = binary_metrics(scored, "score")
        eligible = [name for name, values in metrics.items() if values.get("quantity", 0) >= 100]
        chosen = min(eligible, key=lambda name: (
            metrics[name].get("brier", 9), metrics[name].get("log_loss", 9)
        )) if eligible else "A_base"
        enabled = components[chosen]
        result[family] = {
            "chosen_variant": chosen, "enabled_components": enabled,
            "zero_weight_components": sorted(set(("recency", "strength", "matchup", "uncertainty")) - set(enabled)),
            "development_metrics": metrics,
        }
    return result


def _metrics_by_family(rows):
    result = {}
    for family in sorted({row["market_family"] for row in rows}):
        selected = [row for row in rows if row["market_family"] == family]
        result[family] = {
            "quantity": len(selected),
            "legacy": binary_metrics(selected, "legacy_score"),
            "v2_raw": binary_metrics(selected, "v2_score"),
            "v2_calibrated": binary_metrics(selected, "v2_calibrated_probability"),
        }
    return result


def execute(backtest_run_id=None, repetitions=2000, seed=20260918):
    run = db.session.get(BacktestRun, backtest_run_id) if backtest_run_id else BacktestRun.query.filter_by(
        status="completed").order_by(BacktestRun.fixture_count.desc(), BacktestRun.id.desc()).first()
    if not run:
        raise RuntimeError("nenhum backtest concluído disponível")
    observations = BacktestObservation.query.filter_by(backtest_run_id=run.id).order_by(
        BacktestObservation.kickoff_at, BacktestObservation.id).all()
    rows = _rows(observations)
    development = [row for row in rows if row["dataset_partition"] == "development"
                   and row.get("target") in (0, 1) and row.get("v2_score") is not None]
    evaluation = [row for row in rows if row["dataset_partition"] == "evaluation"
                  and row.get("target") in (0, 1)]
    artifact_config = train_temporal_calibration(development)
    component_configuration = _component_configuration(development)
    artifact_config["component_configuration_by_family"] = component_configuration

    by_id = {item.id: item for item in observations}
    for row in evaluation:
        if row.get("v2_score") is None:
            continue
        probability, source = apply_calibration(artifact_config, row)
        row["v2_calibrated_probability"] = probability
        row["calibration_source"] = source
        stored = by_id[row["id"]]
        stored.v2_calibrated_probability = probability
        stored.calibration_source = source

    existing = CalibrationArtifact.query.filter_by(model_version="greenhunter_v2_shadow", is_active=True).all()
    for item in existing:
        item.is_active = False
    artifact = CalibrationArtifact(
        model_version="greenhunter_v2_shadow",
        method=artifact_config["selected_global_method"], scope="GLOBAL_WITH_FAMILY_FALLBACK",
        trained_through=run.development_end, minimum_family_sample=artifact_config["minimum_family_train"],
        sample_count=len(development), config_json=json.dumps(artifact_config, ensure_ascii=False, separators=(",", ":")),
        validation_metrics_json=json.dumps(artifact_config["method_comparison"], ensure_ascii=False, separators=(",", ":")),
        is_active=True,
    )
    db.session.add(artifact)
    db.session.commit()

    resolved_calibrated = [row for row in evaluation if row.get("v2_calibrated_probability") is not None]
    calibrated_metrics = binary_metrics(resolved_calibrated, "v2_calibrated_probability")
    calibrated_metrics["warning"] = "Probabilidades calibradas fora da amostra de ajuste."
    report = {
        "backtest_run_id": run.id, "calibration_artifact_id": artifact.id,
        "training": {
            "development_candidates": len(development), "trained_through": run.development_end.isoformat(),
            "evaluation_start": run.evaluation_start.isoformat(),
            "internal_validation_start": artifact_config["internal_validation_start"],
            "method_comparison": artifact_config["method_comparison"],
            "selected_global_method": artifact_config["selected_global_method"],
            "family_decisions": artifact_config["family_decisions"],
        },
        "evaluation": {
            "candidates": len(evaluation),
            "legacy": binary_metrics(evaluation, "legacy_score"),
            "v2_raw": binary_metrics(evaluation, "v2_score"),
            "v2_calibrated": calibrated_metrics,
            "calibration_source": grouped_rates(resolved_calibrated, "calibration_source"),
            "by_family": _metrics_by_family(evaluation),
        },
        "component_configuration_by_family": component_configuration,
        "bootstrap": {
            "v2_raw_vs_legacy": cluster_bootstrap(evaluation, "v2_score", repetitions=repetitions, seed=seed),
            "v2_calibrated_vs_legacy": cluster_bootstrap(
                resolved_calibrated, "v2_calibrated_probability", repetitions=repetitions, seed=seed
            ),
        },
        "experimental_selection_profiles": {
            "SHADOW_CONSERVATIVE": {"minimum_probability": 75, "maximum_uncertainty": 35,
                                    "minimum_data_quality": 65, "minimum_sample": 5},
            "SHADOW_BALANCED": {"minimum_probability": 65, "maximum_uncertainty": 55,
                                "minimum_data_quality": 45, "minimum_sample": 3},
        },
        "warnings": [
            "V2 permanece exclusivamente Shadow.",
            "Configurações foram escolhidas no desenvolvimento; avaliação permaneceu congelada.",
            "Odds e ROI não foram reconstruídos.",
        ],
    }
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backtest-run", type=int)
    parser.add_argument("--bootstrap-repetitions", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=20260918)
    parser.add_argument("--output", default="data/backtest/latest-phase-c2.json")
    args = parser.parse_args()
    application = create_app()
    with application.app_context():
        report = execute(args.backtest_run, args.bootstrap_repetitions, args.seed)
        output = Path(args.output); output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(json.dumps({"output": str(output), "artifact": report["calibration_artifact_id"],
                          "method": report["training"]["selected_global_method"]}, ensure_ascii=False))
