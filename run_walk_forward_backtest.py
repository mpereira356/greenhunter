#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path

os.environ.setdefault("DISABLE_WORKER", "1")

from app import create_app
from app.services.backtesting.walk_forward import run_walk_forward


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-fixtures", type=int)
    parser.add_argument("--sample-limit", type=int, default=10)
    parser.add_argument("--output", default="data/backtest/latest-walk-forward.json")
    args = parser.parse_args()
    application = create_app()
    with application.app_context():
        run, report = run_walk_forward(args.max_fixtures, min(10, max(3, args.sample_limit)))
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        payload = {"run_id": run.id, "duration_ms": run.duration_ms, "report": report}
        output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"run_id": run.id, "fixtures": run.fixture_count,
                          "candidates": run.candidate_count, "resolved": run.resolved_count,
                          "unresolved": run.unresolved_count, "leakage_blocked": run.leakage_blocked_count,
                          "duration_ms": run.duration_ms, "output": str(output)}, ensure_ascii=False))
