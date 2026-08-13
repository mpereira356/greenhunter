import json
import os
import time
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from app.services.match_analysis import build_alert_analysis
from app.services.scraper import (
    BASE_URLS,
    _extract_row_teams,
    fetch_match_history,
    get_with_fallback,
    make_session,
)
from app.utils.time import now_sp


MATCHDAY_CACHE_DIR = os.environ.get("MATCHDAY_CACHE_DIR", os.path.join("data", "matchday_cache"))
MATCHDAY_CACHE_TTL_SECONDS = int(os.environ.get("MATCHDAY_CACHE_TTL_SECONDS", "900"))
MATCHDAY_CACHE_VERSION = 5
MATCHDAY_TREND_INDEX_VERSION = 1


def _is_excluded_youth_match(*values: str) -> bool:
    import re

    text = " ".join(str(value or "") for value in values).casefold()
    patterns = (
        r"\bsub[\s._-]?(?:17|18|19|20)\b",
        r"\bu[\s._-]?(?:17|18|19|20)\b",
        r"\bunder[\s._-]?(?:17|18|19|20)\b",
    )
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def _cache_path(day: str) -> str:
    return os.path.join(MATCHDAY_CACHE_DIR, f"fixtures-{day}.json")


def _load_cache(day: str):
    path = _cache_path(day)
    try:
        if time.time() - os.stat(path).st_mtime > MATCHDAY_CACHE_TTL_SECONDS:
            return None
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(payload, dict) or payload.get("cache_version") != MATCHDAY_CACHE_VERSION:
        return None
    return payload if isinstance(payload.get("matches"), list) else None


def _save_cache(day: str, payload: dict) -> None:
    if not payload.get("matches"):
        return
    try:
        os.makedirs(MATCHDAY_CACHE_DIR, exist_ok=True)
        with open(_cache_path(day), "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
    except OSError:
        pass


def _trend_index_path(day: str) -> str:
    return os.path.join(MATCHDAY_CACHE_DIR, f"trends-{day}-v{MATCHDAY_TREND_INDEX_VERSION}.json")


def load_matchday_trend_index(day: str) -> dict:
    try:
        with open(_trend_index_path(day), "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError, TypeError):
        return {}
    if not isinstance(payload, dict) or payload.get("version") != MATCHDAY_TREND_INDEX_VERSION:
        return {}
    return payload


def _save_matchday_trend_index(day: str, payload: dict) -> None:
    try:
        os.makedirs(MATCHDAY_CACHE_DIR, exist_ok=True)
        path = _trend_index_path(day)
        temp_path = f"{path}.tmp"
        with open(temp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
        os.replace(temp_path, path)
    except OSError:
        pass


def build_matchday_trend_index(day: str, force_refresh: bool = True, progress_callback=None) -> dict:
    """Collect score-only history once, enough for fast Over 1.5/2.5 filters."""
    agenda = get_matchday(day, force_refresh=force_refresh)
    matches = list(agenda.get("matches") or [])
    existing = load_matchday_trend_index(day)
    entries = {} if force_refresh else dict(existing.get("entries") or {})
    payload = {
        "version": MATCHDAY_TREND_INDEX_VERSION,
        "day": day,
        "complete": False,
        "total": len(matches),
        "entries": entries,
        "updated_at": now_sp().strftime("%Y-%m-%d %H:%M:%S"),
    }
    session = make_session()
    failures = 0
    for index, match in enumerate(matches, start=1):
        game_id = str(match.get("game_id") or "")
        if not game_id:
            continue
        if game_id in entries and not force_refresh:
            continue
        try:
            history = fetch_match_history(
                session,
                match.get("url") or "",
                limits={"h2h": 10, "home": 10, "away": 10},
                use_fallback=True,
                timeout=8,
            )
            entries[game_id] = {
                key: [
                    {"home": int(item.get("home") or 0), "away": int(item.get("away") or 0), "total": int(item.get("total") or 0)}
                    for item in history.get(source, [])[:10]
                ]
                for key, source in (("H2H", "h2h"), ("Mandante", "home"), ("Visitante", "away"))
            }
        except Exception:
            failures += 1
        if index % 10 == 0:
            payload["updated_at"] = now_sp().strftime("%Y-%m-%d %H:%M:%S")
            payload["processed"] = index
            payload["failures"] = failures
            _save_matchday_trend_index(day, payload)
            if progress_callback:
                progress_callback(index, len(matches), failures)
        time.sleep(1)
    payload.update({
        "complete": failures == 0 and all(str(match.get("game_id") or "") in entries for match in matches),
        "processed": len(matches),
        "failures": failures,
        "updated_at": now_sp().strftime("%Y-%m-%d %H:%M:%S"),
    })
    _save_matchday_trend_index(day, payload)
    return payload


def trend_groups_for_match(index_payload: dict, game_id: str, limit: int) -> dict:
    entry = (index_payload.get("entries") or {}).get(str(game_id)) or {}
    groups = {}
    for key in ("H2H", "Mandante", "Visitante"):
        items = list(entry.get(key) or [])[:limit]
        count = len(items)
        groups[key] = {
            "count": count,
            "samples": 0,
            "over15": round(sum(1 for item in items if int(item.get("total") or 0) > 1) / count * 100) if count else None,
            "over25": round(sum(1 for item in items if int(item.get("total") or 0) > 2) / count * 100) if count else None,
        }
    return groups


def _utc_schedule_from_row(row, reference_day: str | None = None):
    import re

    text = row.get_text(" ", strip=True)
    # Na página individual, o BetsAPI publica AAAA/MM/DD HH:MM já no
    # horário apresentado ao usuário. Não capture o trecho MM/DD desse
    # formato e não aplique a conversão UTC uma segunda vez.
    full_date = re.search(r"\b(\d{4})/(\d{1,2})/(\d{1,2})\s+([01]?\d|2[0-3]):([0-5]\d)\b", text)
    if full_date:
        try:
            return datetime(
                int(full_date.group(1)), int(full_date.group(2)), int(full_date.group(3)),
                int(full_date.group(4)), int(full_date.group(5)),
                tzinfo=ZoneInfo("America/Sao_Paulo"),
            )
        except ValueError:
            return None
    found = re.search(r"\b(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?\s+([01]?\d|2[0-3]):([0-5]\d)\b", text)
    if not found:
        return None
    month, day = int(found.group(1)), int(found.group(2))
    reference = datetime.strptime(reference_day, "%Y-%m-%d") if reference_day else now_sp()
    year = int(found.group(3)) if found.group(3) else reference.year
    if year < 100:
        year += 2000
    try:
        parsed = datetime(year, month, day, int(found.group(4)), int(found.group(5)), tzinfo=ZoneInfo("UTC"))
    except ValueError:
        return None
    if not found.group(3) and parsed.date() < reference.date() and (reference.date() - parsed.date()).days > 180:
        parsed = parsed.replace(year=year + 1)
    return parsed.astimezone(ZoneInfo("America/Sao_Paulo"))


def _clock_from_row(row, reference_day: str | None = None) -> str:
    scheduled = _utc_schedule_from_row(row, reference_day)
    return scheduled.strftime("%H:%M") if scheduled else "-"


def _day_from_row(row, reference_day: str | None = None) -> str:
    scheduled = _utc_schedule_from_row(row, reference_day)
    return scheduled.strftime("%Y-%m-%d") if scheduled else (reference_day or "")


def parse_matchday_html(
    html: str,
    base_url: str = "https://betsapi.com",
    reference_day: str | None = None,
) -> list[dict]:
    import re

    soup = BeautifulSoup(html or "", "html.parser")
    matches = {}
    for row in soup.find_all("tr"):
        link = row.find("a", href=re.compile(r"^(?:/[a-z]+)?/r/\d+"))
        if not link:
            continue
        href = link.get("href") or ""
        id_match = re.search(r"/r/(\d+)", href)
        if not id_match:
            continue
        game_id = id_match.group(1)
        url = f"{base_url.rstrip('/')}{href}"
        home, away = _extract_row_teams(row, url)
        if not home or not away:
            continue
        league_cell = row.find("td", class_="league_n")
        league_link = league_cell.find("a") if league_cell else None
        league = league_link.get_text(" ", strip=True) if league_link else ""
        if "esoccer" in league.casefold():
            continue
        if _is_excluded_youth_match(league, home, away):
            continue
        matches[game_id] = {
            "game_id": game_id,
            "url": url,
            "time": _clock_from_row(row, reference_day),
            "day": _day_from_row(row, reference_day),
            "league": league,
            "home_team": home,
            "away_team": away,
        }
    return list(matches.values())


def _fetch_from_api(day: str, token: str) -> list[dict]:
    matches = []
    page = 1
    while page <= int(os.environ.get("MATCHDAY_MAX_PAGES", "20")):
        response = requests.get(
            "https://api.b365api.com/v3/events/upcoming",
            params={
                "sport_id": 1,
                "day": day.replace("-", ""),
                "skip_esports": 1,
                "token": token,
                "page": page,
            },
            timeout=20,
        )
        response.raise_for_status()
        body = response.json()
        results = body.get("results") or []
        for event in results:
            game_id = str(event.get("id") or "")
            home = (event.get("home") or {}).get("name") or ""
            away = (event.get("away") or {}).get("name") or ""
            if not game_id or not home or not away:
                continue
            league = (event.get("league") or {}).get("name") or ""
            if _is_excluded_youth_match(league, home, away):
                continue
            timestamp = event.get("time")
            try:
                clock = datetime.fromtimestamp(int(timestamp), tz=ZoneInfo("America/Sao_Paulo")).strftime("%H:%M")
            except (TypeError, ValueError, OSError):
                clock = "-"
            matches.append(
                {
                    "game_id": game_id,
                    "url": f"https://betsapi.com/r/{game_id}",
                    "time": clock,
                    "day": day,
                    "league": league,
                    "home_team": home,
                    "away_team": away,
                }
            )
        pager = body.get("pager") or {}
        total = int(pager.get("total") or len(results))
        per_page = int(pager.get("per_page") or 50)
        if not results or page * per_page >= total:
            break
        page += 1
    return matches


def _fetch_from_public(day: str) -> list[dict]:
    session = make_session()
    maximum = max(1, int(os.environ.get("MATCHDAY_MAX_PAGES", "20")))
    found = {}
    for page in range(1, maximum + 1):
        page_matches = []
        for base in BASE_URLS:
            response = get_with_fallback(session, f"{base}/cf/soccer/p.{page}")
            if response.status_code == 200:
                page_matches = parse_matchday_html(response.text, base, reference_day=day)
            if page_matches:
                break
        if not page_matches:
            break
        dated = [item["day"] for item in page_matches if item.get("day")]
        for item in page_matches:
            if item.get("day") == day:
                found[item["game_id"]] = item
        if found and dated and min(dated) > day:
            break
        if not found and dated and min(dated) > day:
            break
    return list(found.values())


def get_matchday(day: str, force_refresh: bool = False) -> dict:
    if not force_refresh:
        cached = _load_cache(day)
        if cached:
            cached["cached"] = True
            return cached

    matches = []
    source = "BetsAPI público"
    error = ""
    token = os.environ.get("BETSAPI_TOKEN", "").strip()
    try:
        if token:
            matches = _fetch_from_api(day, token)
            source = "BetsAPI oficial"
        else:
            matches = _fetch_from_public(day)
    except Exception as exc:
        error = f"A agenda não respondeu neste momento ({type(exc).__name__})."

    matches.sort(key=lambda item: (item.get("time") == "-", item.get("time") or "", item.get("league") or ""))
    payload = {
        "cache_version": MATCHDAY_CACHE_VERSION,
        "day": day,
        "matches": matches,
        "source": source,
        "error": error,
        "cached": False,
    }
    _save_cache(day, payload)
    return payload


def find_match(day: str, game_id: str):
    payload = get_matchday(day)
    return next((item for item in payload["matches"] if item["game_id"] == str(game_id)), None)


def analyze_upcoming_match(
    match: dict,
    force_refresh: bool = False,
    detail_limit: int | None = None,
    cache_variant: str = "detail-v18-exact-sample",
) -> dict:
    placeholder = SimpleNamespace(
        id=f"upcoming-{cache_variant}-{match['game_id']}",
        game_id=match["game_id"],
        url=match["url"],
        status="upcoming",
        league=match.get("league"),
        home_team=match.get("home_team"),
        away_team=match.get("away_team"),
        ft_stats_json=None,
        ht_stats_json=None,
        initial_stats_json=None,
        result_minute=None,
        last_score_minute=None,
        alert_minute=None,
        result_time_hhmm=None,
        ft_score=None,
        last_score=None,
        ht_score=None,
        initial_score=None,
    )
    if detail_limit is None:
        detail_limit = max(1, int(os.environ.get("MATCHDAY_ANALYSIS_DETAIL_LIMIT", "6")))
    detail_limit = min(10, max(1, int(detail_limit)))
    matchday_limits = {"h2h": detail_limit, "home": detail_limit, "away": detail_limit}
    return build_alert_analysis(
        placeholder,
        force_refresh=force_refresh,
        include_details=True,
        detail_limits={"h2h": detail_limit, "home": detail_limit, "away": detail_limit},
        history_limits=matchday_limits,
    )
