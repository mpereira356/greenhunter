import time

from sqlalchemy.exc import OperationalError

from app.extensions import db


def _is_locked_error(exc: OperationalError) -> bool:
    msg = str(getattr(exc, "orig", exc)).lower()
    return "database is locked" in msg or "database table is locked" in msg


def commit_with_retry(attempts: int = 5, delay_seconds: float = 0.15) -> None:
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            db.session.commit()
            return
        except OperationalError as exc:
            db.session.rollback()
            if not _is_locked_error(exc) or attempt >= attempts:
                raise
            last_exc = exc
            time.sleep(delay_seconds * attempt)
    if last_exc:
        raise last_exc
