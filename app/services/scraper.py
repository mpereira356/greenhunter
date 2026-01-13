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
    if "on target" in raw or "a baliza" in raw or "ao alvo" in raw:
        return "On Target"
    if "off target" in raw or "fora" in raw:
        if "chute" in raw or "shot" in raw:
            return "Off Target"
    if "dangerous" in raw and "attack" in raw:
        return "Dangerous Attacks"
    if "corners" in raw and "half" in raw:
        return "Corners (Half)"
    if raw == "corners" or "corner" in raw:
        return "Corners"
    if raw == "attacks" or "attack" in raw:
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
    match = re.match(r"(\d+)", time_text)
    if match:
        return int(match.group(1))
    return None


def fetch_live_games(session):
    resp = get_with_fallback(session, BASE_URL)
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
    if not tables:
        return None

    rows = tables[0].find_all("tr")
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

    for row in rows:
        cols = row.find_all("td")
        if len(cols) != 3:
            continue
        name_raw = cols[1].get_text(strip=True)
        key = normalize_stat_key(name_raw)
        home_val = extrair_valor_td(cols[0])
        away_val = extrair_valor_td(cols[2])
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
    if minute_value is not None:
        stats["Minute"] = {"home": minute_value, "away": minute_value, "total": minute_value}
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
