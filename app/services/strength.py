"""Competition-scoped, chronological Elo features for the B2 shadow engine.

Elo is deliberately exposed as an auditable feature.  It is not converted into
market probability and it never changes the legacy generator's decision.
"""

import math
import os
import re
import statistics
import unicodedata
from collections import defaultdict
from datetime import datetime
from itertools import groupby

from app.extensions import db
from app.models import HistoricalMatch, TeamAlias, TeamIdentity, TeamStrengthSnapshot


ELO_MODEL_VERSION = "elo_v1"
ELO_BASE_RATING = float(os.environ.get("GREENHUNTER_ELO_BASE_RATING", "1500"))
ELO_K_FACTOR = float(os.environ.get("GREENHUNTER_ELO_K_FACTOR", "24"))
ELO_HOME_ADVANTAGE = float(os.environ.get("GREENHUNTER_ELO_HOME_ADVANTAGE", "65"))


def normalize_key(value):
    normalized = unicodedata.normalize("NFKD", str(value or "").casefold())
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", " ", normalized).strip()[:180]


def competition_key(value):
    """Return a controlled rating pool; missing leagues are not mixed globally."""
    return normalize_key(value) or None


def expected_score(rating, opponent_rating, home_advantage=0.0):
    return 1.0 / (1.0 + 10.0 ** ((opponent_rating - (rating + home_advantage)) / 400.0))


def actual_scores(home_score, away_score):
    if home_score > away_score:
        return 1.0, 0.0
    if home_score < away_score:
        return 0.0, 1.0
    return 0.5, 0.5


class StrengthEngine:
    def __init__(self, base_rating=ELO_BASE_RATING, k_factor=ELO_K_FACTOR,
                 home_advantage=ELO_HOME_ADVANTAGE, model_version=ELO_MODEL_VERSION):
        self.base_rating = float(base_rating)
        self.k_factor = float(k_factor)
        self.home_advantage = float(home_advantage)
        self.model_version = model_version

    def rebuild(self):
        """Rebuild all valid snapshots chronologically and return audit counts."""
        matches = HistoricalMatch.query.filter(
            HistoricalMatch.historical_date_available.is_(True),
            HistoricalMatch.kickoff_at.isnot(None),
            HistoricalMatch.home_team_identity_id.isnot(None),
            HistoricalMatch.away_team_identity_id.isnot(None),
            HistoricalMatch.home_score.isnot(None),
            HistoricalMatch.away_score.isnot(None),
            HistoricalMatch.league.isnot(None),
        ).order_by(HistoricalMatch.kickoff_at.asc(), HistoricalMatch.id.asc()).all()
        existing = {
            (row.historical_match_id, row.team_identity_id, row.competition_key): row
            for row in TeamStrengthSnapshot.query.filter_by(model_version=self.model_version).all()
        }
        ratings = defaultdict(lambda: self.base_rating)
        produced_keys = set()
        teams = set()
        used_matches = 0
        for _, simultaneous in groupby(matches, key=lambda item: item.kickoff_at):
            simultaneous = list(simultaneous)
            occurrence = defaultdict(int)
            for match in simultaneous:
                occurrence[match.home_team_identity_id] += 1
                occurrence[match.away_team_identity_id] += 1
            pending_updates = []
            for match in simultaneous:
                pool = competition_key(match.league)
                if (not pool or match.home_team_identity_id == match.away_team_identity_id
                        or occurrence[match.home_team_identity_id] > 1
                        or occurrence[match.away_team_identity_id] > 1):
                    continue
                home_key = (pool, match.home_team_identity_id)
                away_key = (pool, match.away_team_identity_id)
                home_pre, away_pre = ratings[home_key], ratings[away_key]
                home_expected = expected_score(home_pre, away_pre, self.home_advantage)
                away_expected = 1.0 - home_expected
                home_actual, away_actual = actual_scores(match.home_score, match.away_score)
                home_post = home_pre + self.k_factor * (home_actual - home_expected)
                away_post = away_pre + self.k_factor * (away_actual - away_expected)
                pending_updates.append((home_key, home_post, away_key, away_post))
                used_matches += 1
                for team_id, opponent_id, is_home, pre, opp_pre, expected, actual, post in (
                    (match.home_team_identity_id, match.away_team_identity_id, True, home_pre, away_pre,
                     home_expected, home_actual, home_post),
                    (match.away_team_identity_id, match.home_team_identity_id, False, away_pre, home_pre,
                     away_expected, away_actual, away_post),
                ):
                    key = (match.id, team_id, pool)
                    row = existing.get(key)
                    if row is None:
                        row = TeamStrengthSnapshot(
                            historical_match_id=match.id, team_identity_id=team_id,
                            competition_key=pool, model_version=self.model_version,
                        )
                        db.session.add(row)
                        existing[key] = row
                    row.opponent_identity_id = opponent_id
                    row.is_home = is_home
                    row.kickoff_at = match.kickoff_at
                    row.pre_rating = pre
                    row.opponent_pre_rating = opp_pre
                    row.expected_score = expected
                    row.actual_score = actual
                    row.post_rating = post
                    produced_keys.add(key)
                    teams.add(team_id)
            # No result at this timestamp can affect another simultaneous game.
            for home_key, home_post, away_key, away_post in pending_updates:
                ratings[home_key], ratings[away_key] = home_post, away_post
        db.session.flush()
        # Stale rows can otherwise expose ratings calculated from matches no longer valid.
        for row in TeamStrengthSnapshot.query.filter_by(model_version=self.model_version).all():
            if (row.historical_match_id, row.team_identity_id, row.competition_key) not in produced_keys:
                db.session.delete(row)
        db.session.commit()
        return {"matches": used_matches, "teams": len(teams), "snapshots": len(produced_keys)}


def _resolve_identity(external_id=None, name=None):
    if external_id:
        return TeamIdentity.query.filter_by(source="betsapi", external_id=str(external_id)).first()
    normalized = normalize_key(name)
    if not normalized:
        return None
    aliases = TeamAlias.query.filter_by(source="betsapi", normalized_alias=normalized).limit(2).all()
    return db.session.get(TeamIdentity, aliases[0].team_identity_id) if len(aliases) == 1 else None


def _latest_rating(team_id, pool, target_at):
    if not team_id or not pool or not target_at:
        return None
    row = TeamStrengthSnapshot.query.filter(
        TeamStrengthSnapshot.team_identity_id == team_id,
        TeamStrengthSnapshot.competition_key == pool,
        TeamStrengthSnapshot.model_version == ELO_MODEL_VERSION,
        TeamStrengthSnapshot.kickoff_at < target_at,
    ).order_by(TeamStrengthSnapshot.kickoff_at.desc(), TeamStrengthSnapshot.id.desc()).first()
    return row.post_rating if row else None


def _parse_date(value):
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00")).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


def build_strength_features(candidate, fixture_snapshot):
    """Build strength context without deriving a market probability from Elo."""
    target_at = _parse_date(candidate.get("kickoffAt"))
    pool = competition_key(candidate.get("competitionName"))
    fixture = (fixture_snapshot or {}).get("fixture") or {}
    home = _resolve_identity(fixture.get("home_external_id"), candidate.get("homeTeam"))
    away = _resolve_identity(fixture.get("away_external_id"), candidate.get("awayTeam"))
    home_rating = _latest_rating(home.id if home else None, pool, target_at)
    away_rating = _latest_rating(away.id if away else None, pool, target_at)

    scope = candidate.get("scope") or "total"
    team_rating = home_rating if scope in {"home", "total"} else away_rating
    opponent_rating = away_rating if scope in {"home", "total"} else home_rating
    relevant_group = "Mandante" if scope == "home" else "Visitante" if scope == "away" else None
    opponent_ratings = []
    weighted_opponents = []
    if relevant_group:
        rows_by_match = {}
        for rows in (((fixture_snapshot or {}).get("groups") or {}).get(relevant_group, {}).get("history_values") or {}).values():
            for row in rows or []:
                if isinstance(row, dict) and row.get("external_id") and row.get("historical_date_available"):
                    rows_by_match[str(row["external_id"])] = row
        historical = HistoricalMatch.query.filter(
            HistoricalMatch.source == "betsapi",
            HistoricalMatch.external_id.in_(list(rows_by_match)),
        ).all() if rows_by_match else []
        for match in historical:
            if not match.kickoff_at or (target_at and match.kickoff_at >= target_at):
                continue
            team_id = home.id if scope == "home" and home else away.id if scope == "away" and away else None
            if not team_id:
                continue
            snapshot = TeamStrengthSnapshot.query.filter_by(
                historical_match_id=match.id, team_identity_id=team_id,
                competition_key=competition_key(match.league), model_version=ELO_MODEL_VERSION,
            ).first()
            if snapshot:
                opponent_ratings.append(snapshot.opponent_pre_rating)
                age_days = max(0.0, (target_at - match.kickoff_at).total_seconds() / 86400) if target_at else 0
                weight = max(0.25, math.exp(-math.log(2) * age_days / 90.0))
                weighted_opponents.append((snapshot.opponent_pre_rating, weight))
    historical_average = statistics.fmean(opponent_ratings) if opponent_ratings else None
    historical_weighted = (
        sum(value * weight for value, weight in weighted_opponents) / sum(weight for _, weight in weighted_opponents)
        if weighted_opponents else None
    )
    available = team_rating is not None and opponent_rating is not None
    return {
        "available": available,
        "model_version": ELO_MODEL_VERSION,
        "competition_key": pool,
        "team_strength": round(team_rating, 3) if team_rating is not None else None,
        "current_opponent_strength": round(opponent_rating, 3) if opponent_rating is not None else None,
        "home_strength": round(home_rating, 3) if home_rating is not None else None,
        "away_strength": round(away_rating, 3) if away_rating is not None else None,
        "strength_difference": round(team_rating - opponent_rating, 3) if available else None,
        "historical_opponents_sample": len(opponent_ratings),
        "historical_opponents_avg": round(historical_average, 3) if historical_average is not None else None,
        "historical_opponents_weighted": round(historical_weighted, 3) if historical_weighted is not None else None,
        "current_vs_historical_strength_delta": round(opponent_rating - historical_average, 3)
        if opponent_rating is not None and historical_average is not None else None,
        "provenance": "chronological_pre_match_elo" if available else "unknown_insufficient_identity_or_snapshots",
    }
