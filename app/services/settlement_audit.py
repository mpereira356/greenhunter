"""Operational audit of prospective settlement coverage."""
import json
import os
from collections import Counter, defaultdict
from pathlib import Path

from app.models import LiveGameState, MarketPrediction, PredictionRun
from app.services.prospective_reporting import _family


def build_settlement_audit(day, before=None):
    rows = MarketPrediction.query.join(PredictionRun).filter(
        PredictionRun.run_type == "PROSPECTIVE_SHADOW",
        MarketPrediction.model_version == "greenhunter_v2_shadow",
        MarketPrediction.target_date == day,
        MarketPrediction.prospective_validity == "VALID",
    ).all()
    states = {s.game_id: s for s in LiveGameState.query.filter(
        LiveGameState.game_id.in_({r.fixture_id for r in rows})).all()}
    by_fixture = defaultdict(list)
    for row in rows: by_fixture[row.fixture_id].append(row)
    unresolved = [row for row in rows if row.settlement_status == "UNRESOLVED"]
    reasons = Counter(row.settlement_failure_reason or "OTHER" for row in unresolved)
    reasons_by_family = defaultdict(Counter)
    for row in unresolved: reasons_by_family[_family(row)][row.settlement_failure_reason or "OTHER"] += 1
    fixture_rows = []
    for fixture_id, candidates in sorted(by_fixture.items()):
        state = states.get(fixture_id)
        try: stats = json.loads(state.stats_json or "{}") if state else {}
        except (TypeError, ValueError): stats = {}
        score_present = bool(state and state.score and "-" not in str(state.score))
        normalized_keys = {str(key).casefold() for key in stats}
        complete_groups = {
            "corners": bool(normalized_keys & {"corners", "corner kicks", "escanteios"}),
            "cards": bool(normalized_keys & {"cards", "total cards", "cartões", "yellow card", "yellow cards"}),
            "fouls": bool(normalized_keys & {"fouls", "fouls committed", "faltas"}),
            "offsides": bool(normalized_keys & {"offsides", "offside", "impedimentos", "foras de jogo"}),
            "shots": bool(normalized_keys & {"shots", "total shots", "finalizações", "goal attempts", "tentativas de golo"})
                     or {"on target", "off target"}.issubset(normalized_keys),
            "shots_on_target": bool(normalized_keys & {"on target", "shots on target", "chutes ao gol"}),
        }
        resolved = sum(row.settlement_status in {"GREEN", "RED", "VOID"} for row in candidates)
        fixture_rows.append({
            "fixture_id": fixture_id, "league": candidates[0].competition,
            "home_team": candidates[0].home_team, "away_team": candidates[0].away_team,
            "kickoff_at": candidates[0].kickoff_at, "source_fixture_id": state.game_id if state else None,
            "result_found": score_present, "statistics_present": bool(stats),
            "statistics_complete": all(complete_groups.values()), "market_data_available": complete_groups,
            "statistics_keys": sorted(stats), "resolved_candidates": resolved,
            "unresolved_candidates": sum(row.settlement_status == "UNRESOLVED" for row in candidates),
        })
    counts = Counter(row.settlement_status for row in rows)
    eligible = counts["GREEN"] + counts["RED"] + counts["UNRESOLVED"]
    resolved = counts["GREEN"] + counts["RED"]
    profile = lambda field: dict(Counter(row.settlement_status for row in rows if getattr(row, field)))
    return {
        "date": day, "before": before,
        "after": {"fixtures": len(by_fixture), "candidates": len(rows), **{k.lower():v for k,v in counts.items()},
                  "resolved": resolved, "settlement_coverage": round(resolved / eligible * 100, 2) if eligible else None},
        "failure_reasons": {reason:{"count":count, "percent_of_unresolved":round(count/len(unresolved)*100,2)}
                            for reason,count in sorted(reasons.items())},
        "failure_reasons_by_family": {family:dict(sorted(values.items())) for family,values in sorted(reasons_by_family.items())},
        "fixtures": {
            "with_at_least_one_resolved": sum(row["resolved_candidates"] > 0 for row in fixture_rows),
            "with_zero_resolved": sum(row["resolved_candidates"] == 0 for row in fixture_rows),
            "with_final_result": sum(row["result_found"] for row in fixture_rows),
            "with_statistics": sum(row["statistics_present"] for row in fixture_rows),
            "with_complete_statistics": sum(row["statistics_complete"] for row in fixture_rows),
            "score_only": sum(row["result_found"] and not row["statistics_present"] for row in fixture_rows),
            "not_found_in_local_settlement_sources": sum(not row["result_found"] and not row["statistics_present"] for row in fixture_rows),
            "details": fixture_rows,
        },
        "conservative": profile("selected_conservative"), "balanced": profile("selected_balanced"),
    }


def write_settlement_audit(day, before=None, directory="data/prospective_reports"):
    root = Path(directory); root.mkdir(parents=True, exist_ok=True)
    target = root / f"settlement-audit-{day}.json"; temporary = root / f".{target.name}.{os.getpid()}.tmp"
    payload = build_settlement_audit(day, before)
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(temporary, target)
    return str(target), payload
