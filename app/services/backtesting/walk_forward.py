"""Deterministic walk-forward evaluation for GreenHunter Phase C.

Every target fixture is evaluated from a database slice strictly preceding its
kickoff. Settlement is read only after feature and score construction.
"""

import json
import math
import statistics
import time
from dataclasses import dataclass
from datetime import timedelta

from app.extensions import db
from app.models import (BacktestObservation, BacktestRun, HistoricalMatch,
                        HistoricalMatchStat, TeamIdentity)
from app.services.backtesting.leakage import audit_temporal_sources
from app.services.backtesting.legacy_replay import evaluate_legacy
from app.services.backtesting.metrics import binary_metrics, grouped_rates
from app.services.matchup import build_matchup_features
from app.services.prediction_features import build_feature_snapshot, recency_weight
from app.services.strength import build_strength_features
from app.utils.time import now_sp


@dataclass(frozen=True)
class MarketSpec:
    market_type: str
    family: str
    history_key: str
    line: float
    scope: str = "total"
    period: str = "full_time"


MARKETS = (
    MarketSpec("over15", "goals", "over15", 1.5),
    MarketSpec("over25", "goals", "over25", 2.5),
    MarketSpec("goal_ht", "goals_first_half", "goal_ht", .5, period="first_half"),
    MarketSpec("corners", "corners", "corners_avg", 8.5),
    MarketSpec("corners_home", "corners", "team_corners_avg", 2.5, "home"),
    MarketSpec("corners_away", "corners", "team_corners_avg", 2.5, "away"),
    MarketSpec("corners_1h", "corners_first_half", "corners_1h", 2.5, period="first_half"),
    MarketSpec("cards", "cards", "cards_avg", 4.5),
    MarketSpec("cards_home", "cards", "team_cards", 1.5, "home"),
    MarketSpec("cards_away", "cards", "team_cards", 1.5, "away"),
    MarketSpec("shots", "shots", "shots_avg", 19.5),
    MarketSpec("shots_home", "shots", "team_shots", 10.5, "home"),
    MarketSpec("shots_away", "shots", "team_shots", 10.5, "away"),
    MarketSpec("shots_on_target", "shots_on_target", "shots_on_target_avg", 7.5),
    MarketSpec("shots_on_target_home", "shots_on_target", "team_shots_on_target", 3.5, "home"),
    MarketSpec("shots_on_target_away", "shots_on_target", "team_shots_on_target", 3.5, "away"),
    MarketSpec("offsides", "offsides", "offsides_avg", 1.5),
    MarketSpec("offsides_home", "offsides", "team_offsides", .5, "home"),
    MarketSpec("offsides_away", "offsides", "team_offsides", .5, "away"),
    MarketSpec("fouls", "fouls", "fouls_avg", 15.5),
    MarketSpec("fouls_home", "fouls", "team_fouls", 7.5, "home"),
    MarketSpec("fouls_away", "fouls", "team_fouls", 7.5, "away"),
)


def logical_key(match, spec):
    return "|".join((str(match.source), str(match.external_id), spec.market_type,
                     f"{spec.line:g}", spec.scope, spec.period))


def settle_market(actual, line, temporal_audit):
    if not temporal_audit.allowed:
        return "LEAKAGE_BLOCKED", None
    if actual is None:
        return "UNRESOLVED", None
    return ("GREEN", 1) if actual > line else ("RED", 0)


def _stat_maps(match_ids):
    result = {}
    rows = HistoricalMatchStat.query.filter(HistoricalMatchStat.historical_match_id.in_(match_ids)).all() if match_ids else []
    for row in rows:
        if row.available and row.value is not None:
            result.setdefault(row.historical_match_id, {})[(row.period, row.side, row.stat_key)] = float(row.value)
    return result


def _side(match, team_id):
    return "home" if match.home_team_identity_id == team_id else "away" if match.away_team_identity_id == team_id else None


def _metric(match, stats, family, side="total", period="full_time"):
    values = stats.get(match.id, {})
    if family == "goals":
        if side == "home": return float(match.home_score) if match.home_score is not None else None
        if side == "away": return float(match.away_score) if match.away_score is not None else None
        return float(match.home_score + match.away_score) if match.home_score is not None and match.away_score is not None else None
    if family == "goals_first_half":
        return values.get(("first_half", "total", "goals_ht"))
    base = family.replace("_first_half", "")
    stat_key = {"corners": "corners", "cards": "cards", "shots": "shots",
                "shots_on_target": "shots_on_target", "offsides": "offsides", "fouls": "fouls"}.get(base)
    if not stat_key:
        return None
    selected_period = "first_half" if period == "first_half" else "full_time"
    if side == "total":
        direct = values.get((selected_period, "total", stat_key))
        if direct is not None:
            return direct
        home = values.get((selected_period, "home", f"{stat_key}_home"))
        away = values.get((selected_period, "away", f"{stat_key}_away"))
        return home + away if home is not None and away is not None else None
    return values.get((selected_period, side, f"{stat_key}_{side}"))


def _row(match, value, stats, target_team_id=None):
    side = _side(match, target_team_id) if target_team_id else None
    opposing_side = "away" if side == "home" else "home" if side == "away" else None
    base = {
        "match": f"{match.home_team} x {match.away_team}", "value": value,
        "external_id": match.external_id, "kickoff_at": match.kickoff_at.isoformat(),
        "kickoff_timezone": match.kickoff_timezone,
        "historical_date_available": bool(match.historical_date_available),
        "league": match.league, "source": match.source,
        "home_external_id": db.session.get(TeamIdentity, match.home_team_identity_id).external_id if match.home_team_identity_id else None,
        "away_external_id": db.session.get(TeamIdentity, match.away_team_identity_id).external_id if match.away_team_identity_id else None,
    }
    if side:
        for family, key in (("goals", "goals"), ("corners", "corners"), ("cards", "cards"),
                            ("shots", "shots"), ("shots_on_target", "shots_on_target"),
                            ("offsides", "offsides"), ("fouls", "fouls")):
            produced = _metric(match, stats, family, side)
            conceded = _metric(match, stats, family, opposing_side)
            if produced is not None:
                base["team_goals" if key == "goals" else f"team_{key}" + ("_avg" if key == "corners" else "")] = produced
            if conceded is not None:
                base[f"opponent_{key}"] = conceded
    return base


def _team_history(matches, team_id, before, maximum=10):
    return [match for match in reversed(matches) if match.kickoff_at < before
            and team_id in (match.home_team_identity_id, match.away_team_identity_id)][:maximum]


def _snapshot(target, all_matches, stats, maximum=10):
    home_history = _team_history(all_matches, target.home_team_identity_id, target.kickoff_at, maximum)
    away_history = _team_history(all_matches, target.away_team_identity_id, target.kickoff_at, maximum)
    h2h = [match for match in reversed(all_matches) if match.kickoff_at < target.kickoff_at and
           {match.home_team_identity_id, match.away_team_identity_id} ==
           {target.home_team_identity_id, target.away_team_identity_id}][:maximum]

    def group(history, team_id=None):
        history_values = {}
        for spec in MARKETS:
            values = []
            for match in history:
                if spec.scope == "total":
                    value = _metric(match, stats, spec.family, "total", spec.period)
                else:
                    side = _side(match, team_id)
                    value = _metric(match, stats, spec.family, side, spec.period) if side else None
                if value is not None:
                    values.append(_row(match, value, stats, team_id))
            history_values[spec.history_key] = values
        if team_id:
            for family, source_key, opponent_key in (
                ("goals", "team_goals", "opponent_goals"),
                ("corners", "team_corners_avg", "opponent_corners"),
                ("cards", "team_cards", "opponent_cards"),
                ("shots", "team_shots", "opponent_shots"),
                ("shots_on_target", "team_shots_on_target", "opponent_shots_on_target"),
                ("offsides", "team_offsides", "opponent_offsides"),
                ("fouls", "team_fouls", "opponent_fouls"),
            ):
                produced, conceded = [], []
                for match in history:
                    side = _side(match, team_id); other = "away" if side == "home" else "home"
                    prod = _metric(match, stats, family, side) if side else None
                    conc = _metric(match, stats, family, other) if side else None
                    if prod is not None: produced.append(_row(match, prod, stats, team_id))
                    if conc is not None: conceded.append(_row(match, conc, stats, team_id))
                history_values[source_key] = produced
                history_values[opponent_key] = conceded
        return {"count": len(history), "history_values": history_values}

    h2h_group = group(h2h)
    for scope, team_id in (("home", target.home_team_identity_id), ("away", target.away_team_identity_id)):
        for family, key in (("goals", f"team_goals_{scope}"), ("corners", f"team_corners_{scope}")):
            rows = []
            for match in h2h:
                side = _side(match, team_id)
                value = _metric(match, stats, family, side) if side else None
                if value is not None:
                    rows.append(_row(match, value, stats, team_id))
            h2h_group["history_values"][key] = rows
    home_identity = db.session.get(TeamIdentity, target.home_team_identity_id)
    away_identity = db.session.get(TeamIdentity, target.away_team_identity_id)
    return {
        "cache_schema_version": 2,
        "fixture": {
            "fixture_id": target.external_id, "competition": target.league,
            "home_team": target.home_team, "away_team": target.away_team,
            "home_external_id": home_identity.external_id if home_identity else None,
            "away_external_id": away_identity.external_id if away_identity else None,
        },
        "groups": {
            "H2H": h2h_group,
            "Mandante": group(home_history, target.home_team_identity_id),
            "Visitante": group(away_history, target.away_team_identity_id),
        },
        "source_matches": list({match.id: match for match in h2h + home_history + away_history}.values()),
    }


def _candidate(target, spec):
    group = spec.market_type
    if spec.family.startswith("goals") and spec.scope == "total":
        group = spec.market_type
    return {
        "fixtureId": target.external_id, "competitionName": target.league,
        "homeTeam": target.home_team, "awayTeam": target.away_team,
        "kickoffAt": target.kickoff_at.isoformat(), "marketType": spec.market_type,
        "marketGroup": group, "scope": spec.scope, "direction": "over", "line": spec.line,
    }


def _bases(snapshot, spec):
    result = {}
    for output, name in (("h2h", "H2H"), ("home", "Mandante"), ("away", "Visitante")):
        key = spec.history_key
        if output == "h2h" and spec.scope in {"home", "away"}:
            key = {"team_goals": f"team_goals_{spec.scope}",
                   "team_corners_avg": f"team_corners_{spec.scope}"}.get(key, key)
        result[output] = [row.get("value") for row in snapshot["groups"][name]["history_values"].get(key, [])]
    return result


def _ablation(features, spec):
    base = features.get("team_relevant_frequency")
    series = features.get("series") or {}
    recent_values = [item.get("recent_weighted_frequency") for item in series.values()
                     if item.get("recent_weighted_frequency") is not None]
    recent = statistics.fmean(recent_values) if recent_values else None
    matchup = features.get("matchup") or {}
    strength = features.get("strength") or {}
    uncertainty = features.get("uncertainty_score")

    def percent(value): return None if value is None else max(0, min(100, value * 100))
    a = percent(base)
    b = None if a is None else a if recent is None else .8 * a + .2 * percent(recent)
    # Strength adjustment is only directionally meaningful for a team's own
    # attacking production. It remains neutral for total/card/foul markets.
    strength_signal = None
    if spec.scope != "total" and spec.family in {"goals", "corners", "shots", "shots_on_target", "offsides"}:
        delta = strength.get("current_vs_historical_strength_delta")
        if delta is not None:
            strength_signal = max(-10, min(10, -delta / 40))
    c = None if a is None else max(0, min(100, a + (strength_signal or 0)))
    matchup_signal = None
    expected = matchup.get("combined_expected_value")
    if expected is not None:
        matchup_signal = 100 / (1 + math.exp(-(expected - spec.line) / max(1, abs(spec.line) * .25)))
    d = None if a is None else a if matchup_signal is None else .7 * a + .3 * matchup_signal
    e = None if a is None else 50 + (a - 50) * (1 - (uncertainty or 0) * .45)
    f = None if b is None else max(0, min(100, b + (strength_signal or 0)))
    if f is not None and matchup_signal is not None:
        f = .75 * f + .25 * matchup_signal
    return {"A_base": a, "B_recency": b, "C_strength": c, "D_matchup": d,
            "E_dispersion_uncertainty": e, "F_recency_strength_matchup": f,
            "G_v2_complete": features.get("v2_statistical_score")}


def _quality_band(value):
    return "UNKNOWN" if value is None else "LOW" if value < .5 else "MEDIUM" if value < .75 else "HIGH"


def _uncertainty_band(value):
    return "UNKNOWN" if value is None else "LOW" if value < .35 else "MEDIUM" if value < .65 else "HIGH"


def _compact_metrics(rows, score_key):
    metrics = binary_metrics(rows, score_key)
    return {key: metrics.get(key) for key in (
        "quantity", "green", "red", "hit_rate", "brier", "log_loss",
        "calibration_error", "roc_auc", "pr_auc",
    )}


def _model_comparison_by(rows, key):
    groups = {}
    values = sorted({str(row.get(key) if row.get(key) is not None else "UNKNOWN") for row in rows})
    for value in values:
        selected = [row for row in rows
                    if str(row.get(key) if row.get(key) is not None else "UNKNOWN") == value]
        resolved = [row for row in selected if row.get("target") in (0, 1)]
        groups[value] = {
            "quantity": len(selected), "resolved": len(resolved),
            "conclusion_eligible": len(resolved) >= 30,
            "legacy": _compact_metrics(resolved, "legacy_score"),
            "v2": _compact_metrics(resolved, "v2_score"),
        }
    return groups


def _ablation_by(rows, key):
    groups = {}
    values = sorted({str(row.get(key) if row.get(key) is not None else "UNKNOWN") for row in rows})
    for value in values:
        selected = [row for row in rows
                    if str(row.get(key) if row.get(key) is not None else "UNKNOWN") == value
                    and row.get("target") in (0, 1)]
        variants = sorted({name for row in selected for name in (row.get("ablations") or {})})
        groups[value] = {
            "quantity": len(selected), "conclusion_eligible": len(selected) >= 30,
            "variants": {
                variant: _compact_metrics(
                    [{**row, "score": (row.get("ablations") or {}).get(variant)} for row in selected],
                    "score",
                ) for variant in variants
            },
        }
    return groups


def _report(rows):
    resolved = [row for row in rows if row.get("target") in (0, 1)]
    report = {
        "coverage": {
            "candidates": len(rows), "resolved": len(resolved),
            "unresolved": sum(row.get("settlement") == "UNRESOLVED" for row in rows),
            "leakage_blocked": sum(row.get("settlement") == "LEAKAGE_BLOCKED" for row in rows),
            "strength_available": sum(bool(row.get("strength_available")) for row in rows),
            "matchup_available": sum(bool(row.get("matchup_available")) for row in rows),
        },
        "legacy": binary_metrics(resolved, "legacy_score"),
        "v2": binary_metrics(resolved, "v2_score"),
        "by_market": grouped_rates(rows, "market_family"),
        "by_league": grouped_rates(rows, "league"),
        "by_temporal_confidence": grouped_rates(rows, "temporal_confidence"),
        "by_data_quality": grouped_rates(rows, "data_quality_band"),
        "by_uncertainty": grouped_rates(rows, "uncertainty_band"),
        "by_matchup": grouped_rates(rows, "matchup_available"),
        "by_strength": grouped_rates(rows, "strength_available"),
        "by_historical_opponent_strength": grouped_rates(rows, "historical_strength_band"),
        "by_recency_shift": grouped_rates(rows, "recency_shift_band"),
        "by_legacy_status": grouped_rates(rows, "legacy_status"),
        "model_comparison_by_market": _model_comparison_by(rows, "market_family"),
        "model_comparison_by_league": _model_comparison_by(rows, "league"),
    }
    variants = sorted({key for row in rows for key in (row.get("ablations") or {})})
    report["ablation"] = {variant: binary_metrics(
        [{**row, "score": (row.get("ablations") or {}).get(variant)} for row in resolved], "score"
    ) for variant in variants}
    report["ablation_by_market"] = _ablation_by(rows, "market_family")
    raw100 = [row for row in resolved if row.get("raw_frequency") is not None and row["raw_frequency"] >= 99.999]
    for row in raw100:
        sample = row.get("effective_sample_size") or 0
        row["raw100_sample_band"] = "3" if sample < 4 else "4-5" if sample < 6 else "6-7" if sample < 8 else "8-10" if sample <= 10 else "10+"
        delta = row.get("historical_strength_delta")
        row["raw100_strength_band"] = "UNKNOWN" if delta is None else "WEAKER" if delta < -50 else "SIMILAR" if delta <= 50 else "STRONGER"
    report["raw_frequency_100"] = {
        "quantity": len(raw100), "by_sample": grouped_rates(raw100, "raw100_sample_band"),
        "by_strength": grouped_rates(raw100, "raw100_strength_band"),
    }
    divergences = [row for row in resolved if row.get("legacy_score") is not None and row.get("v2_score") is not None]
    divergences.sort(key=lambda row: abs(row["legacy_score"] - row["v2_score"]), reverse=True)
    report["largest_legacy_v2_divergences"] = [{
        key: row.get(key) for key in ("fixture_external_id", "league", "market_type", "line", "scope",
                                      "legacy_score", "v2_score", "settlement")
    } for row in divergences[:20]]
    return report


def run_walk_forward(max_fixtures=None, sample_limit=10):
    started = time.perf_counter()
    eligible = HistoricalMatch.query.filter(
        HistoricalMatch.historical_date_available.is_(True), HistoricalMatch.kickoff_at.isnot(None),
        HistoricalMatch.home_team_identity_id.isnot(None), HistoricalMatch.away_team_identity_id.isnot(None),
        HistoricalMatch.home_score.isnot(None), HistoricalMatch.away_score.isnot(None),
        HistoricalMatch.league.isnot(None),
    ).order_by(HistoricalMatch.kickoff_at.asc(), HistoricalMatch.id.asc()).all()
    if max_fixtures:
        eligible = eligible[-max(1, int(max_fixtures)):]
    split_index = max(1, int(len(eligible) * .7)) if eligible else 0
    evaluation_start = eligible[split_index].kickoff_at if len(eligible) > split_index else None
    run = BacktestRun(
        run_type="BACKTEST", methodology_version="walk_forward_v1",
        development_end=(eligible[split_index - 1].kickoff_at if split_index else None),
        evaluation_start=evaluation_start,
        period_start=eligible[0].kickoff_at if eligible else None,
        period_end=eligible[-1].kickoff_at if eligible else None,
        parameters_json=json.dumps({"sample_limit": sample_limit, "markets": len(MARKETS),
                                    "parameter_adjustments_after_evaluation": False}),
    )
    db.session.add(run); db.session.commit()
    # SQLite permits only one writer.  Persist the run header, then keep the
    # CPU-heavy walk-forward phase read-only.  Pending observations are flushed
    # in one short transaction at the end instead of holding a production write
    # lock for the whole backtest.
    original_autoflush = db.session.autoflush
    db.session.autoflush = False
    all_matches = HistoricalMatch.query.filter(
        HistoricalMatch.historical_date_available.is_(True), HistoricalMatch.kickoff_at.isnot(None),
        HistoricalMatch.home_team_identity_id.isnot(None), HistoricalMatch.away_team_identity_id.isnot(None),
    ).order_by(HistoricalMatch.kickoff_at.asc(), HistoricalMatch.id.asc()).all()
    stats = _stat_maps([match.id for match in all_matches])
    report_rows = []
    for target in eligible:
        snapshot = _snapshot(target, all_matches, stats, sample_limit)
        sources = [{"kickoff_at": match.kickoff_at, "kickoff_timezone": match.kickoff_timezone}
                   for match in snapshot.pop("source_matches")]
        audit = audit_temporal_sources(target.kickoff_at, sources)
        partition = "evaluation" if evaluation_start and target.kickoff_at >= evaluation_start else "development"
        strength_cache = {}
        for spec in MARKETS:
            candidate = _candidate(target, spec)
            legacy = evaluate_legacy(_bases(snapshot, spec), spec.line, spec.scope)
            matchup = build_matchup_features(candidate, snapshot)
            if spec.scope not in strength_cache:
                strength_cache[spec.scope] = build_strength_features(candidate, snapshot)
            strength = strength_cache[spec.scope]
            features = build_feature_snapshot(candidate, snapshot, strength=strength, matchup=matchup)
            actual = _metric(target, stats, spec.family,
                             "total" if spec.scope == "total" else spec.scope, spec.period)
            settlement, target_value = settle_market(actual, spec.line, audit)
            relevant_series = ([features["series"]["home"], features["series"]["away"]]
                               if spec.scope == "total" else
                               [features["series"]["home" if spec.scope == "home" else "away"]])
            recent_values = [item.get("recent_weighted_frequency") for item in relevant_series
                             if item.get("recent_weighted_frequency") is not None]
            effective_values = [item.get("effective_sample_size") for item in relevant_series
                                if item.get("effective_sample_size") is not None]
            ablations = _ablation(features, spec)
            row = {
                "fixture_external_id": target.external_id, "market_type": spec.market_type,
                "line": spec.line, "scope": spec.scope,
                "league": target.league, "market_family": spec.family,
                "target": target_value, "settlement": settlement,
                "legacy_score": legacy["confidence_score"], "v2_score": features.get("v2_statistical_score"),
                "legacy_status": legacy["status"],
                "raw_frequency": features.get("team_relevant_frequency") * 100 if features.get("team_relevant_frequency") is not None else None,
                "recent_frequency": statistics.fmean(recent_values) if recent_values else None,
                "effective_sample_size": statistics.fmean(effective_values) if effective_values else None,
                "strength_available": bool(strength.get("available")),
                "matchup_available": bool(matchup.get("matchup_available")),
                "strength_delta": strength.get("strength_difference"),
                "historical_strength_delta": strength.get("current_vs_historical_strength_delta"),
                "temporal_confidence": audit.confidence,
                "data_quality_band": _quality_band(features.get("data_quality_v2")),
                "uncertainty_band": _uncertainty_band(features.get("uncertainty_score")),
                "ablations": ablations,
                "dataset_partition": partition,
            }
            historical_delta = row["historical_strength_delta"]
            row["historical_strength_band"] = (
                "UNKNOWN" if historical_delta is None else "CURRENT_WEAKER" if historical_delta < -50
                else "SIMILAR" if historical_delta <= 50 else "CURRENT_STRONGER"
            )
            recent_percent = row["recent_frequency"] * 100 if row["recent_frequency"] is not None else None
            recency_shift = recent_percent - row["raw_frequency"] if recent_percent is not None and row["raw_frequency"] is not None else None
            row["recency_shift_band"] = (
                "UNKNOWN" if recency_shift is None else "NEGATIVE" if recency_shift < -5
                else "STABLE" if recency_shift <= 5 else "POSITIVE"
            )
            report_rows.append(row)
            db.session.add(BacktestObservation(
                backtest_run_id=run.id, logical_key=logical_key(target, spec), historical_match_id=target.id,
                fixture_external_id=target.external_id, kickoff_at=target.kickoff_at,
                prediction_time=target.kickoff_at - timedelta(minutes=1), league=target.league,
                market_family=spec.family, market_type=spec.market_type, line=spec.line,
                scope=spec.scope, period=spec.period, legacy_score=legacy["confidence_score"],
                legacy_adjusted_probability=legacy["adjusted_probability"], legacy_status=legacy["status"],
                v2_score=features.get("v2_statistical_score"),
                uncertainty_score=(features.get("uncertainty_score") or 0) * 100,
                data_quality_v2=(features.get("data_quality_v2") or 0) * 100,
                raw_frequency=row["raw_frequency"], recent_frequency=(row["recent_frequency"] * 100 if row["recent_frequency"] is not None else None),
                effective_sample_size=row["effective_sample_size"],
                team_strength=strength.get("team_strength"), opponent_strength=strength.get("current_opponent_strength"),
                strength_delta=strength.get("strength_difference"), production_avg=matchup.get("production_avg"),
                concession_avg=matchup.get("opponent_concession_avg"),
                matchup_available=row["matchup_available"], strength_available=row["strength_available"],
                temporal_confidence=audit.confidence, max_source_timestamp=audit.max_source_timestamp,
                settlement=settlement, actual_value=actual, target=target_value,
                feature_snapshot_json=json.dumps(features, ensure_ascii=False, separators=(",", ":")),
                ablation_scores_json=json.dumps(ablations, ensure_ascii=False, separators=(",", ":")),
                leakage_reason=audit.reason, prospective_shadow=False, dataset_partition=partition,
            ))
    report = _report(report_rows)
    report["development"] = _report([row for row in report_rows if row["dataset_partition"] == "development"])
    report["evaluation"] = _report([row for row in report_rows if row["dataset_partition"] == "evaluation"])
    run.fixture_count = len(eligible); run.candidate_count = len(report_rows)
    run.resolved_count = sum(row["target"] in (0, 1) for row in report_rows)
    run.unresolved_count = sum(row["settlement"] == "UNRESOLVED" for row in report_rows)
    run.leakage_blocked_count = sum(row["settlement"] == "LEAKAGE_BLOCKED" for row in report_rows)
    run.metrics_json = json.dumps(report, ensure_ascii=False, separators=(",", ":"))
    run.status = "completed"; run.finished_at = now_sp()
    run.duration_ms = int((time.perf_counter() - started) * 1000)
    db.session.autoflush = original_autoflush
    db.session.commit()
    return run, report
