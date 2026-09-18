import json
import logging
from datetime import datetime

from sqlalchemy import update

from app.extensions import db
from app.models import (CalibrationArtifact, MarketOddsSnapshot, MarketPrediction,
                        ModelVersion, PredictionFixtureSnapshot, PredictionRun)
from app.services.calibration import apply_calibration
from app.services.frozen_shadow import FROZEN_VERSION, load_frozen_configuration
from app.services.matchup import build_matchup_features
from app.services.prediction_features import build_feature_snapshot
from app.services.strength import build_strength_features
from app.utils.time import now_sp


LOGGER = logging.getLogger(__name__)
LEGACY_VERSION = "legacy_v1"
SHADOW_VERSION = "greenhunter_v2_shadow"
MAX_CANDIDATES_PER_BATCH = 250
MAX_SNAPSHOT_BYTES = 96_000


def _json(value, maximum=MAX_SNAPSHOT_BYTES):
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
    if len(encoded.encode("utf-8")) > maximum:
        raise ValueError("snapshot excede o limite permitido")
    return encoded


def _text(value, maximum):
    return str(value or "").strip()[:maximum]


def _number(value):
    try:
        result = float(value)
        return result if result == result and abs(result) != float("inf") else None
    except (TypeError, ValueError, RuntimeError):
        return None


def _integer(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _market_family(candidate):
    market = str(candidate.get("marketType") or "").casefold()
    group = str(candidate.get("marketGroup") or market).casefold()
    if market == "goal_ht":
        return "goals_first_half"
    if market.startswith("over") or group.startswith("team_goals"):
        return "goals"
    for family in ("shots_on_target", "corners", "cards", "shots", "offsides", "fouls"):
        if family in group or family in market:
            return f"{family}_first_half" if "_1h" in group else family
    return group or market or "UNKNOWN"


def _active_calibration():
    try:
        return load_frozen_configuration().get("calibration_config")
    # Some isolated/legacy test databases intentionally do not seed the C2
    # artifact. Production still uses the immutable frozen version; these
    # databases simply continue without calibration, as they did before C3.
    except (RuntimeError, TypeError, ValueError):
        return None


def _selection_flags(probability, uncertainty, quality, sample):
    if probability is None:
        return False, False
    uncertainty = float(uncertainty if uncertainty is not None else 100)
    quality = float(quality if quality is not None else 0)
    sample = int(sample or 0)
    conservative = probability >= 75 and uncertainty <= 35 and quality >= 65 and sample >= 5
    balanced = probability >= 65 and uncertainty <= 55 and quality >= 45 and sample >= 3
    return conservative, balanced


def _safe_parameters(value):
    allowed = {
        "generator_count", "generator_samples", "enabled_markets", "max_markets_per_game",
        "minimum_samples", "profile", "early_mode", "engine_config", "analysis_metadata",
    }
    return {key: value[key] for key in allowed if isinstance(value, dict) and key in value}


def ensure_model_versions():
    definitions = (
        (LEGACY_VERSION, "statistical_candidate_engine", "production", "baseline"),
        (SHADOW_VERSION, "statistical_shadow", "shadow", "prepared"),
    )
    for version, family, mode, status in definitions:
        if db.session.query(ModelVersion.id).filter_by(version=version).first() is None:
            db.session.add(ModelVersion(
                version=version, algorithm_family=family, mode=mode, status=status, config_json="{}"
            ))


def start_prediction_run(user_id: int | None, payload: dict) -> PredictionRun:
    ensure_model_versions()
    target_date = _text(payload.get("target_date"), 10)
    datetime.strptime(target_date, "%Y-%m-%d")
    parameters = _safe_parameters(payload.get("parameters") or {})
    legacy = ModelVersion.query.filter_by(version=LEGACY_VERSION).first()
    if legacy and (not legacy.config_json or legacy.config_json == "{}") and parameters.get("engine_config"):
        legacy.config_json = _json(parameters["engine_config"], 160_000)
    run = PredictionRun(
        user_id=user_id,
        algorithm="statistical_candidate_engine",
        model_version=LEGACY_VERSION,
        mode="production_baseline",
        run_type="PROSPECTIVE_SHADOW",
        target_date=target_date,
        started_at=now_sp(),
        status="running",
        parameters_json=_json(parameters, 160_000),
        fixture_count=max(0, _integer(payload.get("fixture_count"))),
    )
    db.session.add(run)
    db.session.commit()
    return run


def _rejection_status(candidate: dict) -> tuple[str, str | None, list[str]]:
    raw_status = _text(candidate.get("status"), 30).upper()
    reasons = [
        _text(reason, 80).upper()
        for reason in (candidate.get("rejectionReasons") or [])
        if _text(reason, 80)
    ]
    if raw_status == "APPROVED":
        return "APPROVED", None, reasons
    reason = reasons[0] if reasons else "OTHER"
    aliases = {
        "LOW_CONFIDENCE": "LOW_CONFIDENCE", "LOW_SAMPLE": "LOW_SAMPLE",
        "LOW_DATA_QUALITY": "LOW_DATA_QUALITY", "HIGH_DIVERGENCE": "INCONSISTENCY",
        "REDUNDANT_MARKET": "REDUNDANCY", "HIGH_CORRELATION": "CORRELATION",
        "BETTER_LINE_AVAILABLE": "WEAKER_LINE", "INSUFFICIENT_DATA": "LOW_DATA_QUALITY",
    }
    normalized = aliases.get(reason, reason if reason else "OTHER")
    return f"REJECTED_{normalized}"[:50], reason, reasons


def append_prediction_candidates(run: PredictionRun, payload: dict) -> int:
    candidates = payload.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        return 0
    if len(candidates) > MAX_CANDIDATES_PER_BATCH:
        raise ValueError(f"máximo de {MAX_CANDIDATES_PER_BATCH} candidatos por lote")

    fixture_snapshots = payload.get("fixture_snapshots") or {}
    snapshot_ids = {}
    fixture_ids = {_text(candidate.get("fixtureId"), 64) for candidate in candidates}
    existing = PredictionFixtureSnapshot.query.filter(
        PredictionFixtureSnapshot.run_id == run.id,
        PredictionFixtureSnapshot.fixture_id.in_(fixture_ids),
    ).all() if fixture_ids else []
    snapshot_ids.update({item.fixture_id: item.id for item in existing})
    for fixture_id in fixture_ids:
        if not fixture_id or fixture_id in snapshot_ids:
            continue
        snapshot = fixture_snapshots.get(fixture_id)
        if not isinstance(snapshot, dict):
            snapshot = {"historical_data_available": False, "groups": {}}
        row = PredictionFixtureSnapshot(
            run_id=run.id, fixture_id=fixture_id, snapshot_json=_json(snapshot)
        )
        db.session.add(row)
        db.session.flush()
        snapshot_ids[fixture_id] = row.id

    rows = []
    shadow_inputs = []
    approved = 0
    for candidate in candidates:
        fixture_id = _text(candidate.get("fixtureId"), 64)
        if not fixture_id or not _text(candidate.get("marketType"), 100):
            continue
        status, primary_reason, reasons = _rejection_status(candidate)
        approved += int(status == "APPROVED")
        source_stats = candidate.get("sourceStats") if isinstance(candidate.get("sourceStats"), dict) else {}
        samples = max(
            [_integer((source_stats.get(key) or {}).get("samples")) for key in ("h2h", "home", "away")]
            or [0]
        )
        immutable = {
            "modelVersion": LEGACY_VERSION,
            "candidate": candidate,
            "fixtureSnapshotId": snapshot_ids.get(fixture_id),
        }
        legacy_row = MarketPrediction(
            run_id=run.id, fixture_snapshot_id=snapshot_ids.get(fixture_id), model_version=LEGACY_VERSION,
            fixture_id=fixture_id, target_date=run.target_date,
            kickoff_at=_text(candidate.get("kickoffAt"), 40) or None,
            competition=_text(candidate.get("competitionName"), 160) or None,
            home_team=_text(candidate.get("homeTeam"), 160) or None,
            away_team=_text(candidate.get("awayTeam"), 160) or None,
            market_type=_text(candidate.get("marketType"), 100),
            market_group=_text(candidate.get("marketGroup"), 100) or None,
            scope=_text(candidate.get("scope"), 20) or None,
            direction=_text(candidate.get("direction"), 10) or None,
            line=_number(candidate.get("line")), raw_frequency=_number(candidate.get("rawProbability")),
            recent_frequency=_number(candidate.get("recentScore")), adjusted_frequency=_number(candidate.get("adjustedProbability")),
            consistency_score=_number(candidate.get("consistencyScore")), data_quality_score=_number(candidate.get("dataQualityScore")),
            context_score=_number(candidate.get("contextScore")), confidence_score=_number(candidate.get("confidenceScore")),
            sample_size=samples, individual_odd=_number(candidate.get("individualOdd")),
            status=status, rejection_reason=primary_reason,
            rejection_reasons_json=_json(reasons, 16_000),
            strengths_json=_json(candidate.get("strengths") or [], 24_000),
            weaknesses_json=_json(candidate.get("weaknesses") or [], 24_000),
            snapshot_json=_json(immutable),
        )
        rows.append(legacy_row)
        shadow_inputs.append((legacy_row, candidate, fixture_snapshots.get(fixture_id) or {"groups": {}}))
    db.session.add_all(rows)
    db.session.flush()

    # Shadow is observational. Any unavailable feature remains UNKNOWN and no
    # assessment below is consulted by the Legacy or sent back to the browser.
    shadows = []
    shadow_candidates = []
    calibration = _active_calibration()
    prediction_timestamp = now_sp()
    is_daily_official = run.algorithm == "daily_shadow_observer"
    strength_cache = {}
    for legacy_row, candidate, fixture_snapshot in shadow_inputs:
        try:
            matchup = build_matchup_features(candidate, fixture_snapshot)
            strength_key = (legacy_row.fixture_id, candidate.get("scope") or "total")
            if strength_key not in strength_cache:
                strength_cache[strength_key] = build_strength_features(candidate, fixture_snapshot)
            strength = strength_cache[strength_key]
            features = build_feature_snapshot(candidate, fixture_snapshot, strength=strength, matchup=matchup)
            score = features.get("v2_statistical_score")
            uncertainty = (features.get("uncertainty_score") or 0) * 100
            quality = (features.get("data_quality_v2") or 0) * 100
            try:
                fixture_kickoff = datetime.fromisoformat(
                    str(legacy_row.kickoff_at or "").replace("Z", "+00:00")
                ).replace(tzinfo=None)
            except (TypeError, ValueError):
                fixture_kickoff = None
            is_pre_match = bool(fixture_kickoff and prediction_timestamp < fixture_kickoff)
            calibrated = calibration_source = None
            if is_pre_match and calibration is not None and score is not None:
                calibrated, calibration_source = apply_calibration(calibration, {
                    "v2_score": score, "market_family": _market_family(candidate),
                    "effective_sample_size": legacy_row.sample_size,
                    "uncertainty_score": uncertainty, "data_quality_v2": quality,
                    "temporal_confidence": features.get("temporal_reliability"),
                })
            conservative, balanced = _selection_flags(
                calibrated, uncertainty, quality, legacy_row.sample_size
            )
            if not is_pre_match:
                conservative = balanced = False
            assessment = (
                "SHADOW_UNKNOWN" if score is None else "SHADOW_STRONG" if score >= 75
                else "SHADOW_MODERATE" if score >= 60 else "SHADOW_WEAK"
            )
            immutable = {
                "modelVersion": SHADOW_VERSION,
                "legacyPredictionId": legacy_row.id,
                "candidate": candidate,
                "features": features,
            }
            shadow = MarketPrediction(
                run_id=run.id, fixture_snapshot_id=legacy_row.fixture_snapshot_id,
                model_version=SHADOW_VERSION, configuration_version=FROZEN_VERSION,
                legacy_prediction_id=legacy_row.id,
                fixture_id=legacy_row.fixture_id, target_date=legacy_row.target_date,
                kickoff_at=legacy_row.kickoff_at, competition=legacy_row.competition,
                home_team=legacy_row.home_team, away_team=legacy_row.away_team,
                market_type=legacy_row.market_type, market_group=legacy_row.market_group,
                scope=legacy_row.scope, direction=legacy_row.direction, line=legacy_row.line,
                raw_frequency=legacy_row.raw_frequency, recent_frequency=legacy_row.recent_frequency,
                adjusted_frequency=legacy_row.adjusted_frequency,
                consistency_score=legacy_row.consistency_score,
                data_quality_score=legacy_row.data_quality_score,
                context_score=legacy_row.context_score, confidence_score=legacy_row.confidence_score,
                sample_size=legacy_row.sample_size, individual_odd=legacy_row.individual_odd,
                status=assessment, rejection_reason=None, rejection_reasons_json="[]",
                strengths_json="[]", weaknesses_json="[]",
                feature_snapshot_json=_json(features),
                v2_statistical_score=score,
                v2_calibrated_probability=calibrated, calibration_source=calibration_source,
                uncertainty_score=uncertainty,
                data_quality_v2=quality,
                temporal_reliability=features.get("temporal_reliability"),
                settlement_status="PENDING", selected_conservative=conservative,
                selected_balanced=balanced,
                prediction_timestamp=prediction_timestamp,
                official_pre_match_snapshot=is_daily_official and is_pre_match,
                prospective_validity="VALID" if is_pre_match else "LATE_PREDICTION_REJECTED",
                snapshot_json=_json(immutable),
            )
            shadows.append(shadow)
            shadow_candidates.append(candidate)
        except Exception:
            LOGGER.exception("Falha isolada no V2 Shadow para candidato Legacy %s", legacy_row.id)
    db.session.add_all(shadows)
    db.session.flush()
    captured_at = prediction_timestamp
    for shadow, candidate in zip(shadows, shadow_candidates):
        prices = candidate.get("bookmakerOdds") if isinstance(candidate.get("bookmakerOdds"), list) else []
        fallback_odd = _number(candidate.get("individualOdd"))
        fallback_bookmaker = _text(candidate.get("bookmaker") or candidate.get("oddsBookmaker"), 120)
        if fallback_odd and fallback_bookmaker:
            prices = [*prices, {"name": fallback_bookmaker, "odd": fallback_odd}]
        try:
            kickoff = datetime.fromisoformat(str(shadow.kickoff_at or "").replace("Z", "+00:00")).replace(tzinfo=None)
        except (TypeError, ValueError):
            kickoff = None
        if kickoff and captured_at < kickoff:
            seen_bookmakers = set()
            for price in prices:
                odds = _number(price.get("odd")); bookmaker = _text(price.get("name"), 120)
                normalized = bookmaker.casefold()
                if not odds or odds <= 1 or not bookmaker or normalized in seen_bookmakers:
                    continue
                seen_bookmakers.add(normalized)
                db.session.add(MarketOddsSnapshot(
                    market_prediction_id=shadow.id, fixture_id=shadow.fixture_id,
                    market_type=shadow.market_type, line=shadow.line, side=shadow.scope,
                    bookmaker=bookmaker, odds_value=odds, captured_at=captured_at,
                    fixture_kickoff=kickoff,
                ))
    run.candidate_count += len(rows)
    run.approved_count += approved
    run.rejected_count += len(rows) - approved
    db.session.commit()
    return len(rows)


def finish_prediction_run(run: PredictionRun, payload: dict | None = None) -> PredictionRun:
    payload = payload or {}
    run.finished_at = now_sp()
    run.duration_ms = max(0, int((run.finished_at - run.started_at).total_seconds() * 1000))
    run.status = "completed" if not payload.get("error") else "completed_with_errors"
    run.fallback_used = _text(payload.get("fallback_used"), 120) or None
    run.errors_json = _json(payload.get("errors") or [], 32_000)
    db.session.commit()
    return run


def mark_prediction_run_failed(run_id: int, error: Exception) -> None:
    try:
        db.session.rollback()
        run = db.session.get(PredictionRun, run_id)
        if run:
            run.status = "telemetry_failed"
            run.finished_at = now_sp()
            run.duration_ms = max(0, int((run.finished_at - run.started_at).total_seconds() * 1000))
            run.errors_json = _json([{"type": type(error).__name__, "message": str(error)[:300]}])
            db.session.commit()
    except Exception:
        db.session.rollback()
        LOGGER.exception("Falha ao registrar erro da execução de previsões %s", run_id)
