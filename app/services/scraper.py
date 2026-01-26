import os
import re
import unicodedata

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://pt.betsapi.com"


def make_session():
    session = requests.Session()
    try:
        retries = Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=1.5,
            status_forcelist=(403, 429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
        )
    except TypeError:
        retries = Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=1.5,
            status_forcelist=(403, 429, 500, 502, 503, 504),
            method_whitelist=("GET", "POST"),
        )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    proxy = os.environ.get("PROXY_URL")
    if proxy:
        session.proxies.update({"http": proxy, "https": proxy})
    return session


def get_with_fallback(session, url):
    resp = session.get(url, timeout=15)
    if resp.status_code == 403:
        session.headers.update({"Referer": BASE_URL, "Cache-Control": "no-cache"})
        resp = session.get(url, timeout=15)
    return resp


def extrair_valor_td(td):
    span = td.find("span", class_="sr-only")
    return span.get_text(strip=True) if span else td.get_text(strip=True)


def _to_ascii(text: str) -> str:
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode("ascii", "ignore").decode("ascii")


def normalize_stat_key(name: str) -> str:
    raw = _to_ascii(name).strip().lower()
    raw = raw.replace("-", " ").replace("_", " ")
    raw = " ".join(raw.split())
    if raw in ("on target", "shots on target", "shot on target"):
        return "On Target"
    if raw in ("off target", "shots off target", "shot off target"):
        return "Off Target"
    if raw in ("dangerous attacks", "dangerous attack"):
        return "Dangerous Attacks"
    if raw in ("yellow cards", "yellow card"):
        return "Yellow Card"
    if raw in ("red cards", "red card"):
        return "Red Card"
    if (
        "on target" in raw
        or "on goal" in raw
        or "a baliza" in raw
        or "ao alvo" in raw
        or ("alvo" in raw and ("chute" in raw or "shot" in raw))
    ):
        return "On Target"
    if "off target" in raw:
        return "Off Target"
    if "fora" in raw:
        if "chute" in raw or "shot" in raw:
            return "Off Target"
    if "dangerous" in raw and "attack" in raw:
        return "Dangerous Attacks"
    if "ataques perigosos" in raw or "ataque perigoso" in raw:
        return "Dangerous Attacks"
    if "corners" in raw and "half" in raw:
        return "Corners (Half)"
    if raw == "corners" or "corner" in raw:
        return "Corners"
    if raw == "attacks" or "attack" in raw:
        return "Attacks"
    if raw in ("ataques", "ataque"):
        return "Attacks"
    if "possession" in raw:
        return "Possession"
    if raw in ("golos", "goals", "goal"):
        return "Goals"
    if "yellow/red" in raw or "yellow red" in raw or "amarelo/vermelho" in raw:
        return "Yellow/Red Card"
    if "yellow card" in raw or "amarelo" in raw:
        return "Yellow Card"
    if "red card" in raw or "vermelho" in raw:
        return "Red Card"
    if "penalt" in raw:
        return "Penalties"
    if "ball safe" in raw or "bola segura" in raw:
        return "Ball Safe"
    if "substitution" in raw or "substitu" in raw:
        return "Substitutions"
    if raw in ("minute", "minuto", "min"):
        return "Minute"
    return name.strip()


def parse_int(value: str):
    if value is None:
        return None
    digits = re.findall(r"\d+", str(value))
    if not digits:
        return None
    return int(digits[0])


def parse_minutes(time_text: str):
    if not time_text:
        return None
    text = time_text.strip().lower()
    text = text.replace("’", "").replace("'", "")
    text = text.replace("＋", "+").replace("﹢", "+").replace("⁺", "+")
    text = text.replace("﹣", "-").replace("−", "-")
    if text.startswith("+"):
        return None
    extra_match = re.search(r"(\d+)\s*\+\s*(\d+)", text)
    if extra_match:
        return int(extra_match.group(1))

    nums = [int(n) for n in re.findall(r"\d+", text)]
    if not nums:
        return None
    if "+" in text:
        return nums[0] if len(nums) >= 2 else None

    if len(nums) == 1 and nums[0] <= 2 and ("half" in text or "tempo" in text or "ht" in text):
        return None

    minute = nums[-1]
    if minute <= 45 and ("2h" in text or "2nd" in text or "2o" in text or "2o tempo" in text):
        return 45 + minute
    return minute


def fetch_live_games(session):
    try:
        resp = get_with_fallback(session, BASE_URL)
    except requests.RequestException:
        return [], None
    if resp.status_code != 200:
        return [], resp.status_code
    soup = BeautifulSoup(resp.text, "html.parser")
    trs = soup.find_all("tr", id=lambda x: x and x.startswith("r_"))
    games = []
    for tr in trs:
        sport_td = tr.find("td", class_="sport_n")
        league_td = tr.find("td", class_="league_n")
        time_span = tr.find("span", class_="race-time")

        sport_a = sport_td.find("a") if sport_td else None
        league_a = league_td.find("a") if league_td else None
        league_name = league_a.text.strip() if league_a else ""
        time_text = time_span.get_text(strip=True) if time_span else ""

        if not (sport_a and sport_a.get("href") == "/c/soccer"):
            continue
        if "esoccer" in league_name.lower():
            continue
        if not time_text or not time_text[0].isdigit():
            continue

        game_link_tag = tr.find("a", href=re.compile(r"^/r/\d+"))
        if not game_link_tag:
            continue
        game_href = game_link_tag["href"]
        match_id = re.search(r"/r/(\d+)", game_href)
        if not match_id:
            continue
        game_id = match_id.group(1)
        games.append(
            {
                "game_id": game_id,
                "url": BASE_URL + game_href,
                "minute": parse_minutes(time_text),
                "time_text": time_text,
                "league": league_name,
            }
        )
    return games, resp.status_code


def fetch_match_stats(session, url):
    resp = get_with_fallback(session, url)
    if resp.status_code != 200:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")

    league_tag = soup.select_one("ol.breadcrumb li:nth-of-type(2) a")
    league = league_tag.text.strip() if league_tag else ""

    time_tag = soup.find("span", class_="race-time")
    time_text = time_tag.get_text(strip=True) if time_tag else ""

    tables = soup.find_all("table", class_="table table-sm")
    stat_tables = tables if tables else soup.find_all("table")
    if not stat_tables:
        return None

    rows = stat_tables[0].find_all("tr")
    home_team = ""
    away_team = ""
    if rows:
        first = rows[0].find_all("td")
        if len(first) == 3:
            home_team = first[0].get_text(strip=True)
            away_team = first[2].get_text(strip=True)

    stats = {}
    raw_stats = {}
    score = "0 x 0"

    all_rows = []
    for table in stat_tables:
        all_rows.extend(table.find_all("tr"))

    def _is_missing_pair(values):
        if not values:
            return True
        return all(not v or str(v).strip() in ("-", "—") for v in values)

    for row in all_rows:
        cols = row.find_all("td")
        if len(cols) != 3:
            continue
        name_raw = cols[1].get_text(strip=True)
        if not name_raw or name_raw.isdigit():
            continue
        key = normalize_stat_key(name_raw)
        home_val = extrair_valor_td(cols[0])
        away_val = extrair_valor_td(cols[2])
        if key in raw_stats:
            if _is_missing_pair(raw_stats.get(key)) and not _is_missing_pair((home_val, away_val)):
                raw_stats[key] = (home_val, away_val)
        else:
            raw_stats[key] = (home_val, away_val)

        if key == "Goals":
            score = f"{home_val} x {away_val}"
        home_int = parse_int(home_val)
        away_int = parse_int(away_val)
        if home_int is not None and away_int is not None:
            stats[key] = {
                "home": home_int,
                "away": away_int,
                "total": home_int + away_int,
            }

    minute_value = parse_minutes(time_text)
    raw_minute = raw_stats.get("Minute")
    if raw_minute:
        candidates = []
        for candidate in raw_minute:
            parsed = parse_minutes(candidate)
            if parsed is not None:
                candidates.append(parsed)
        if candidates:
            best_minute = max(candidates)
            if minute_value is None or best_minute > minute_value:
                minute_value = best_minute
    if minute_value is not None:
        stats["Minute"] = {"home": minute_value, "away": minute_value, "total": minute_value}
    else:
        stats.pop("Minute", None)
    return {
        "league": league,
        "home_team": home_team,
        "away_team": away_team,
        "score": score,
        "time_text": time_text,
        "minute": minute_value,
        "stats": stats,
        "raw_stats": raw_stats,
    }


HISTORY_LIMITS = {"h2h": 8, "home": 6, "away": 6}
HISTORY_LABELS = {
    "head to head": "h2h",
    "home history": "home",
    "away history": "away",
}


def history_url_from_match(url: str) -> str:
    if not url:
        return ""
    return url.replace("/r/", "/rh/")


def _normalize_heading(text: str) -> str:
    return " ".join(_to_ascii(text).lower().split())


def _find_history_tables(soup):
    tables = {}
    used = set()
    headings = soup.find_all(["h2", "h3", "h4", "h5", "h6", "div", "span", "strong"])
    for tag in headings:
        text = tag.get_text(" ", strip=True)
        if not text:
            continue
        norm = _normalize_heading(text)
        for label, key in HISTORY_LABELS.items():
            if label in norm and key not in tables:
                table = None
                parent = tag.parent
                if parent:
                    table = parent.find("table")
                if not table:
                    table = tag.find_next("table")
                if table and id(table) in used:
                    table = table.find_next("table")
                if table:
                    tables[key] = table
                    used.add(id(table))
    return tables


def _parse_history_table(table):
    items = []
    if not table:
        return items
    for row in table.find_all("tr"):
        text = row.get_text(" ", strip=True)
        if not text:
            continue
        score_match = re.search(r"(\d+)\s*-\s*(\d+)", text)
        if not score_match:
            continue
        home_goals = int(score_match.group(1))
        away_goals = int(score_match.group(2))
        items.append(
            {
                "home": home_goals,
                "away": away_goals,
                "total": home_goals + away_goals,
            }
        )
    return items


def fetch_match_history(session, match_url: str, limits=None):
    history_url = history_url_from_match(match_url)
    if not history_url:
        return {"h2h": [], "home": [], "away": []}
    resp = get_with_fallback(session, history_url)
    if resp.status_code != 200:
        return {"h2h": [], "home": [], "away": []}
    soup = BeautifulSoup(resp.text, "html.parser")
    tables = _find_history_tables(soup)
    limits = limits or HISTORY_LIMITS
    result = {"h2h": [], "home": [], "away": []}
    for key in ("h2h", "home", "away"):
        items = _parse_history_table(tables.get(key))
        limit = limits.get(key) if isinstance(limits, dict) else None
        result[key] = items[:limit] if limit else items
    return result


def summarize_history(items):
    total = len(items)
    if total == 0:
        return None
    goals_sum = sum(item.get("total", 0) for item in items)
    over15 = sum(1 for item in items if item.get("total", 0) > 1)
    over25 = sum(1 for item in items if item.get("total", 0) > 2)
    btts = sum(1 for item in items if item.get("home", 0) > 0 and item.get("away", 0) > 0)
    avg_goals = round(goals_sum / total, 1)
    return {
        "count": total,
        "avg_goals": avg_goals,
        "over15": over15,
        "over25": over25,
        "btts": btts,
    }


def format_history_summary(label: str, summary):
    if not summary:
        return ""
    return (
        f"{label} {summary['count']}j | "
        f"Media gols {summary['avg_goals']} | "
        f"O1.5 {summary['over15']}/{summary['count']} | "
        f"O2.5 {summary['over25']}/{summary['count']} | "
        f"BTTS {summary['btts']}/{summary['count']}"
    )
