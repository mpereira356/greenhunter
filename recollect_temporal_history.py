#!/usr/bin/env python3
"""Controlled recollection for safely fixture-mappable legacy summaries.

Nothing runs automatically.  The command is resumable, rate-limited and only
accepts cache hashes that can be reproduced from an existing fixture agenda.
"""

import argparse
import glob
import hashlib
import json
import os
import time
from pathlib import Path

os.environ.setdefault("DISABLE_WORKER", "1")

from app import create_app
from app.extensions import db
from app.models import HistoricalMatch
from app.services.matchday import analyze_upcoming_match


ROOT = Path(__file__).resolve().parent
FIXTURE_GLOB = ROOT / "data" / "matchday_cache" / "fixtures-*.json"
SUMMARY_DIR = ROOT / "data" / "matchday_cache" / "summaries"
STATE_PATH = ROOT / "data" / "matchday_cache" / "temporal-recollection-state.json"
LOG_PATH = ROOT / "data" / "matchday_cache" / "temporal-recollection.jsonl"


def discover_mappable():
    existing = {path.stem for path in SUMMARY_DIR.glob("*.json")}
    found = {}
    for agenda_path in glob.glob(str(FIXTURE_GLOB)):
        try:
            agenda = json.loads(Path(agenda_path).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        day = str(agenda.get("day") or Path(agenda_path).stem.removeprefix("fixtures-"))
        for match in agenda.get("matches") or []:
            game_id = str(match.get("game_id") or "")
            if not game_id:
                continue
            # A auditoria B1 identificou exatamente os 488 resumos v4 como
            # associáveis com segurança. Outras versões ficam fora por desenho.
            for sample_limit in range(1, 11):
                raw = f"v4:{day}:{game_id}:{sample_limit}"
                digest = hashlib.sha1(raw.encode("utf-8", "ignore")).hexdigest()
                if digest in existing:
                    found.setdefault(game_id, {**match, "day": day, "sample_limit": sample_limit,
                                               "legacy_cache_hash": digest})
    return sorted(found.values(), key=lambda item: (item["day"], item["game_id"]))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--restart", action="store_true")
    args = parser.parse_args()
    delay = max(0.5, args.delay)
    state = {
        "processed": {}, "attempted": 0, "recovered": 0, "failed": 0,
        "skipped": 0, "duplicated": 0, "temporal_valid": 0,
        "team_ids_available": 0, "stats_available": 0,
    }
    if STATE_PATH.exists() and not args.restart:
        try:
            state.update(json.loads(STATE_PATH.read_text(encoding="utf-8")))
        except (OSError, ValueError):
            pass
    pending = [item for item in discover_mappable() if item["game_id"] not in state["processed"]]
    print(json.dumps({"mappable": len(pending) + len(state["processed"]), "pending": len(pending),
                      "selected": min(max(0, args.limit), len(pending)), "dry_run": args.dry_run}))
    if args.dry_run:
        return
    application = create_app()
    with application.app_context():
        consecutive_failures = 0
        for match in pending[:max(0, args.limit)]:
            record = {"game_id": match["game_id"], "day": match["day"], "ok": False}
            state["attempted"] += 1
            try:
                before = {row[0] for row in db.session.query(HistoricalMatch.external_id).all()}
                result = analyze_upcoming_match(match, force_refresh=True,
                                                detail_limit=min(10, match.get("sample_limit") or 6),
                                                cache_variant="b2-temporal-recollection")
                items = []
                for group in (result or {}).get("groups") or []:
                    items.extend(item for item in (group.get("items") or []) if isinstance(item, dict))
                unique = {str(item.get("external_id")): item for item in items if item.get("external_id")}
                temporal = [item for item in unique.values() if item.get("historical_date_available") and item.get("kickoff_at")]
                both_ids = [item for item in temporal if item.get("history_home_external_id") and item.get("history_away_external_id")]
                stats = [item for item in temporal if any(item.get(key) is not None for key in (
                    "corners_home", "corners_away", "cards_home", "cards_away",
                    "shots_home", "shots_away", "shots_on_target_home", "shots_on_target_away",
                    "fouls_home", "fouls_away", "offsides_home", "offsides_away",
                ))]
                duplicates = sum(external_id in before for external_id in unique)
                record.update({
                    "history_matches": len(unique), "temporal_valid": len(temporal),
                    "team_ids_available": len(both_ids), "stats_available": len(stats),
                    "duplicated": duplicates,
                })
                record["ok"] = bool(temporal)
                if not unique:
                    record["reason"] = "NO_HISTORY_RETURNED"
                elif not temporal:
                    record["reason"] = "NO_EXPLICIT_TEMPORAL_HISTORY"
            except Exception as exc:
                record["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
            state["processed"][match["game_id"]] = record
            if record["ok"]:
                state["recovered"] += 1
                consecutive_failures = 0
            elif record.get("history_matches"):
                state["skipped"] += 1
                consecutive_failures += 1
            else:
                state["failed"] += 1
                consecutive_failures += 1
            for key in ("duplicated", "temporal_valid", "team_ids_available", "stats_available"):
                state[key] += int(record.get(key) or 0)
            STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
            with LOG_PATH.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
            if consecutive_failures >= 3:
                state["aborted_reason"] = "THREE_CONSECUTIVE_SOURCE_FAILURES"
                STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
                break
            time.sleep(delay)


if __name__ == "__main__":
    main()
