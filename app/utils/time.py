from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - fallback for older Python
    ZoneInfo = None


def now_sp() -> datetime:
    if ZoneInfo:
        return datetime.now(ZoneInfo("America/Sao_Paulo")).replace(tzinfo=None)
    return datetime.now()
