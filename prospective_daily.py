#!/usr/bin/env python3
"""Persistent 03h job: warm full summaries and freeze today's pre-match snapshot."""
import fcntl
import os
import sys
from datetime import datetime

os.environ["DISABLE_WORKER"] = "1"

from app import create_app
from app.extensions import db
from app.services.matchday import analyze_upcoming_match, get_matchday, save_matchday_summary_cache
from app.services.prospective_operations import update_prospective_health
from app.services.prospective_reporting import write_report_files
from app.services.prospective_shadow import collect_cached_prospective_shadow
from app.utils.time import now_sp


def main():
    lock = open("/tmp/greenhunter-prospective-daily.lock", "w", encoding="utf-8")
    try: fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        update_prospective_health(status="ALREADY_RUNNING", last_error="Outra execução possui o lock.")
        return 0
    day = now_sp().strftime("%Y-%m-%d")
    update_prospective_health(status="RUNNING", last_attempt_at=now_sp().isoformat(),
                              target_date=day, last_error=None, fixtures_found=0,
                              fixtures_analyzed=0, candidates_created=0)
    try:
        app = create_app()
        with app.app_context():
            matches = list((get_matchday(day, force_refresh=True) or {}).get("matches") or [])
            analyzed = failures = 0
            for match in matches:
                # analyze_upcoming_match/append_prediction_candidates enforce the
                # final pre-kickoff boundary too; no past fixture is reconstructed.
                try:
                    kickoff = datetime.fromisoformat(f"{match['day']}T{match.get('time') or '00:00'}")
                    if kickoff <= now_sp():
                        continue
                    payload = analyze_upcoming_match(match, detail_limit=6, cache_variant="card-v30-periods-6")
                    save_matchday_summary_cache(day, str(match.get("game_id") or ""), 6, payload)
                    analyzed += 1
                except Exception:
                    failures += 1
            run = collect_cached_prospective_shadow(day, sample_limit=6)
            write_report_files(day)
            success = run.status == "completed" and int(run.candidate_count or 0) > 0
            health_values = dict(
                status="SUCCESS" if success else "FAILED", last_success_at=now_sp().isoformat() if success else None,
                last_error=None if success else (run.alert_status or "EMPTY_RUN"),
                fixtures_found=len(matches), fixtures_analyzed=analyzed,
                candidates_created=int(run.candidate_count or 0), summary_failures=failures, run_id=run.id)
            if success:
                health_values["last_official_date"] = day
            else:
                health_values.pop("last_success_at", None)
            update_prospective_health(**health_values)
            return 0 if success else 2
    except Exception as exc:
        db.session.rollback()
        update_prospective_health(status="FAILED", last_error=f"{type(exc).__name__}: {str(exc)[:300]}")
        print(f"[prospective_daily] falhou: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__": raise SystemExit(main())
