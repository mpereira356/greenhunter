"""Immutable identity for the approved C2 shadow configuration."""

import hashlib
import json
from datetime import datetime

from app.extensions import db
from app.models import CalibrationArtifact, ModelVersion


FROZEN_VERSION = "greenhunter_v2_shadow_c2_20260918"
FROZEN_AT = datetime(2026, 9, 18, 7, 15)
CONSERVATIVE = {"minimum_probability": 75, "maximum_uncertainty": 35,
                "minimum_data_quality": 65, "minimum_sample": 5}
BALANCED = {"minimum_probability": 65, "maximum_uncertainty": 55,
            "minimum_data_quality": 45, "minimum_sample": 3}


def frozen_configuration():
    artifact = CalibrationArtifact.query.filter_by(is_active=True).order_by(
        CalibrationArtifact.id.desc()).first()
    if not artifact:
        raise RuntimeError("calibrador C2 ativo não encontrado")
    payload = {
        "version": FROZEN_VERSION, "frozen_at": FROZEN_AT.isoformat(),
        "calibration_artifact_id": artifact.id, "calibration_method": artifact.method,
        "calibration_trained_through": artifact.trained_through.isoformat(),
        "calibration_config": json.loads(artifact.config_json),
        "conservative": CONSERVATIVE, "balanced": BALANCED,
        "official_snapshot_policy": "FIRST_COMPLETE_DAILY_CACHE_AFTER_03H_PREWARM_BEFORE_KICKOFF",
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    payload["configuration_sha256"] = hashlib.sha256(canonical.encode()).hexdigest()
    return payload


def ensure_frozen_version():
    payload = frozen_configuration()
    existing = ModelVersion.query.filter_by(version=FROZEN_VERSION).first()
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if existing:
        if existing.config_json != encoded:
            raise RuntimeError("a configuração congelada existente não pode ser sobrescrita")
        return existing, payload
    row = ModelVersion(version=FROZEN_VERSION, algorithm_family="statistical_shadow_calibrated",
                       mode="shadow", status="frozen", config_json=encoded,
                       created_at=FROZEN_AT)
    db.session.add(row); db.session.commit()
    return row, payload


def load_frozen_configuration():
    row = ModelVersion.query.filter_by(version=FROZEN_VERSION).first()
    if not row:
        row, payload = ensure_frozen_version()
        return payload
    return json.loads(row.config_json)
