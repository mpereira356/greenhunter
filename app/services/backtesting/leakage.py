from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class TemporalAudit:
    allowed: bool
    confidence: str
    max_source_timestamp: datetime | None
    reason: str | None = None


def audit_temporal_sources(fixture_kickoff, sources):
    """Conservatively prove every source preceded the prediction fixture."""
    timestamps = []
    unknown_timezone = False
    for source in sources or []:
        timestamp = source.get("kickoff_at") if isinstance(source, dict) else None
        if not isinstance(timestamp, datetime):
            return TemporalAudit(False, "UNSAFE", None, "SOURCE_TIMESTAMP_MISSING")
        if timestamp >= fixture_kickoff:
            return TemporalAudit(False, "UNSAFE", timestamp, "SOURCE_NOT_BEFORE_FIXTURE")
        timestamps.append(timestamp)
        timezone_name = str(source.get("kickoff_timezone") or "")
        unknown_timezone = unknown_timezone or timezone_name in {"", "SOURCE_LOCAL_UNKNOWN"}
    if not timestamps:
        return TemporalAudit(False, "UNSAFE", None, "NO_TEMPORAL_SOURCES")
    maximum = max(timestamps)
    if unknown_timezone:
        # A 36-hour safety margin is deliberately wider than real football
        # timezone offsets and DST changes. Ambiguous observations closer than
        # this never enter the main evaluation.
        if fixture_kickoff - maximum <= timedelta(hours=36):
            return TemporalAudit(False, "UNSAFE", maximum, "UNKNOWN_TIMEZONE_ORDERING_AMBIGUOUS")
        confidence = "MEDIUM"
    else:
        confidence = "HIGH"
    return TemporalAudit(True, confidence, maximum)


def assert_no_leakage(fixture_kickoff, sources):
    audit = audit_temporal_sources(fixture_kickoff, sources)
    if not audit.allowed:
        raise ValueError(f"LEAKAGE_BLOCKED:{audit.reason}")
    return audit
