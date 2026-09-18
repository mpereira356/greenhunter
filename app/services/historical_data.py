from datetime import datetime
from urllib.parse import urlparse

from sqlalchemy import tuple_

from app.extensions import db
from app.models import HistoricalMatch, HistoricalMatchStat, TeamAlias, TeamIdentity
from app.utils.time import now_sp


STAT_FIELDS = {
    "goals": ("full_time", "total", "total"),
    "goals_home": ("full_time", "home", "home"),
    "goals_away": ("full_time", "away", "away"),
    "goals_ht": ("first_half", "total", "goals_ht"),
    "goals_2h": ("second_half", "total", "goals_2h"),
    "corners": ("full_time", "total", "corners_ft_events"),
    "corners_home": ("full_time", "home", "corners_home"),
    "corners_away": ("full_time", "away", "corners_away"),
    "corners_ht": ("first_half", "total", "corners_ht"),
    "corners_2h": ("second_half", "total", "corners_2h"),
    "cards_home": ("full_time", "home", "cards_home"),
    "cards_away": ("full_time", "away", "cards_away"),
    "shots": ("full_time", "total", "shots_total"),
    "shots_home": ("full_time", "home", "shots_home"),
    "shots_away": ("full_time", "away", "shots_away"),
    "shots_on_target": ("full_time", "total", "shots_on_target_total"),
    "shots_on_target_home": ("full_time", "home", "shots_on_target_home"),
    "shots_on_target_away": ("full_time", "away", "shots_on_target_away"),
    "fouls": ("full_time", "total", "fouls_total"),
    "fouls_home": ("full_time", "home", "fouls_home"),
    "fouls_away": ("full_time", "away", "fouls_away"),
    "offsides": ("full_time", "total", "offsides_total"),
    "offsides_home": ("full_time", "home", "offsides_home"),
    "offsides_away": ("full_time", "away", "offsides_away"),
}


def _external_id(item: dict) -> str | None:
    value = str(item.get("external_id") or "").strip()
    if value:
        return value[:80]
    path = urlparse(str(item.get("url") or "")).path
    parts = path.split("/r/", 1)
    if len(parts) == 2:
        candidate = parts[1].split("/", 1)[0]
        return candidate[:80] if candidate.isdigit() else None
    return None


def _kickoff(item: dict):
    value = item.get("kickoff_at")
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _value(item: dict, field: str):
    if field not in item or item.get(field) is None:
        return None
    try:
        return float(item[field])
    except (TypeError, ValueError):
        return None


def _normalized_alias(value: str) -> str:
    import re
    import unicodedata
    normalized = unicodedata.normalize("NFKD", str(value or "").casefold())
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", " ", normalized).strip()[:180]


def persist_historical_data(history_data: dict) -> int:
    """Upsert observed history; missing fields remain explicitly unavailable."""
    unique = {}
    for group in ("h2h", "home", "away"):
        for item in history_data.get(group) or []:
            if not isinstance(item, dict):
                continue
            external_id = _external_id(item)
            if external_id:
                unique[(str(item.get("source") or "betsapi")[:40], external_id)] = item
    if not unique:
        return 0

    source_ids = list(unique)
    existing = {
        (row.source, row.external_id): row
        for row in HistoricalMatch.query.filter(
            tuple_(HistoricalMatch.source, HistoricalMatch.external_id).in_(source_ids)
        ).all()
    }
    team_external_ids = {
        str(item.get(field) or "").strip()[:80]
        for item in unique.values()
        for field in ("history_home_external_id", "history_away_external_id")
        if str(item.get(field) or "").strip()
    }
    identities = {
        row.external_id: row for row in TeamIdentity.query.filter(
            TeamIdentity.source == "betsapi", TeamIdentity.external_id.in_(team_external_ids)
        ).all()
    } if team_external_ids else {}
    alias_names = {
        _normalized_alias(item.get(field))
        for item in unique.values()
        for field in ("history_home_team", "history_away_team")
        if _normalized_alias(item.get(field))
    }
    existing_aliases = {
        row[0] for row in db.session.query(TeamAlias.normalized_alias).filter(
            TeamAlias.source == "betsapi", TeamAlias.normalized_alias.in_(alias_names)
        ).all()
    } if alias_names else set()

    def identity(external_id, name):
        external_id = str(external_id or "").strip()[:80]
        if not external_id:
            return None
        row = identities.get(external_id)
        if row is None:
            row = TeamIdentity(source="betsapi", external_id=external_id, canonical_name=str(name or external_id)[:180])
            db.session.add(row)
            db.session.flush()
            identities[external_id] = row
        normalized = _normalized_alias(name)
        if normalized and normalized not in existing_aliases:
            db.session.add(TeamAlias(
                team_identity_id=row.id, source="betsapi", alias=str(name)[:180],
                normalized_alias=normalized, confirmed=False,
            ))
            existing_aliases.add(normalized)
        return row
    stored = 0
    prepared = []
    for key, item in unique.items():
        row = existing.get(key)
        if row is None:
            row = HistoricalMatch(source=key[0], external_id=key[1], collected_at=now_sp(), updated_at=now_sp())
            db.session.add(row)
            db.session.flush()
            existing[key] = row
        row.source_url = str(item.get("url") or "")[:1000] or row.source_url
        row.kickoff_at = _kickoff(item)
        row.kickoff_original = str(item.get("kickoff_original") or "")[:80] or None
        row.kickoff_timezone = str(item.get("kickoff_timezone") or "")[:80] or None
        row.historical_date_available = bool(item.get("historical_date_available") and row.kickoff_at)
        row.league = str(item.get("league") or "")[:180] or None
        row.season = str(item.get("season") or "")[:80] or None
        row.home_team = str(item.get("history_home_team") or "")[:180] or None
        row.away_team = str(item.get("history_away_team") or "")[:180] or None
        home_identity = identity(item.get("history_home_external_id"), row.home_team)
        away_identity = identity(item.get("history_away_external_id"), row.away_team)
        row.home_team_identity_id = home_identity.id if home_identity else None
        row.away_team_identity_id = away_identity.id if away_identity else None
        row.home_score = int(item["home"]) if item.get("home") is not None else None
        row.away_score = int(item["away"]) if item.get("away") is not None else None
        row.status = "finished" if row.home_score is not None and row.away_score is not None else "unknown"
        row.updated_at = now_sp()

        prepared.append((key, item, row))
        stored += 1

    db.session.flush()
    match_ids = [row.id for _, _, row in prepared]
    all_observations = HistoricalMatchStat.query.filter(
        HistoricalMatchStat.historical_match_id.in_(match_ids)
    ).all() if match_ids else []
    observations_by_match = {}
    for stat in all_observations:
        observations_by_match.setdefault(stat.historical_match_id, {})[
            (stat.period, stat.side, stat.stat_key, stat.source)
        ] = stat

    for key, item, row in prepared:
        observations = observations_by_match.get(row.id, {})
        for stat_key, (period, side, field) in STAT_FIELDS.items():
            observation_key = (period, side, stat_key, key[0])
            observation = observations.get(observation_key)
            if observation is None:
                observation = HistoricalMatchStat(
                    historical_match_id=row.id, period=period, side=side,
                    stat_key=stat_key, source=key[0], collected_at=now_sp(),
                )
                db.session.add(observation)
            value = _value(item, field)
            # Uma nova coleta sem o campo não apaga uma observação real já
            # persistida. Ausência inicial continua registrada como UNKNOWN.
            if value is not None or not observation.available:
                observation.value = value
                observation.available = value is not None
    db.session.commit()
    return stored
