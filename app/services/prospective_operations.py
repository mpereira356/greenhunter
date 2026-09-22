"""Durable operational state for the prospective daily collector."""
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from app.utils.time import now_sp

STATE_PATH = Path(os.environ.get("PROSPECTIVE_HEALTH_PATH", "data/prospective-health.json"))


def _next_run(now=None):
    now = now or now_sp()
    target = now.replace(hour=3, minute=0, second=0, microsecond=0)
    if target <= now: target += timedelta(days=1)
    return target.isoformat()


def prospective_health():
    try: payload = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError): payload = {}
    payload.setdefault("status", "NEVER_RAN")
    payload["next_collection_at"] = _next_run()
    payload["timezone"] = "America/Sao_Paulo"
    return payload


def update_prospective_health(**values):
    payload = prospective_health(); payload.update(values); payload["updated_at"] = now_sp().isoformat()
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = STATE_PATH.with_name(f".{STATE_PATH.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(temporary, STATE_PATH)
    return payload
