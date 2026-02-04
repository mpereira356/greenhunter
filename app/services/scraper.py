import os
import re
import threading
import time
import unicodedata
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URLS = ("https://betsapi.com", "https://pt.betsapi.com")
SECOND_HALF_TOKENS = ("2nd", "2o", "2h", "2Âº", "2º", "second", "segundo")
CF_CHALLENGE_MARKERS = (
    "just a moment",
    "um momento",
    "cf-challenge",
    "checking your browser",
)
_CF_LOCK = threading.Lock()
_CF_LAST_SOLVED_AT = 0.0
_CF_ALERT_LAST_SENT_AT = 0.0
_BROWSER_DRIVER = None


def make_session():
    session = requests.Session()
    try:
        retries = Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=1.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
        )
    except TypeError:
        retries = Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=1.5,
            status_forcelist=(429, 500, 502, 503, 504),
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


def _cf_mode() -> str:
    return os.environ.get("BETSAPI_CF_MODE", "manual").strip().lower()


def _cf_alert_cooldown_seconds() -> int:
    raw = os.environ.get("BETSAPI_CF_ALERT_COOLDOWN_SECONDS", "300").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 300


def _send_cf_telegram_alert(url: str):
    token = (os.environ.get("BETSAPI_CF_ALERT_TELEGRAM_TOKEN") or "").strip()
    chat_id = (os.environ.get("BETSAPI_CF_ALERT_TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat_id:
        return

    global _CF_ALERT_LAST_SENT_AT
    now = time.time()
    cooldown = _cf_alert_cooldown_seconds()
    if cooldown > 0 and (now - _CF_ALERT_LAST_SENT_AT) < cooldown:
        return

    text = (
        "GreenHunter: Cloudflare pediu verificacao humana novamente.\n"
        "Abra o navegador do bot e resolva o challenge para continuar.\n"
        f"URL: {url}"
    )
    api_url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    try:
        resp = requests.post(api_url, data=payload, timeout=10)
        if resp.status_code == 200:
            _CF_ALERT_LAST_SENT_AT = now
    except requests.RequestException:
        pass


class _SimpleResponse:
    def __init__(self, url: str, status_code: int, text: str):
        self.url = url
        self.status_code = status_code
        self.text = text


def _is_cloudflare_content(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    return any(marker in lowered for marker in CF_CHALLENGE_MARKERS)


def _is_cloudflare_response(resp) -> bool:
    if resp is None:
        return False
    if resp.status_code in (403, 429, 503):
        return True
    return _is_cloudflare_content(getattr(resp, "text", ""))




def _has_cf_clearance_cookie(cookies) -> bool:
    if not cookies:
        return False
    return any((c.get("name") or "").lower() == "cf_clearance" for c in cookies)


def _apply_cookies_to_session(session, cookies):
    if not cookies:
        return
    for cookie in cookies:
        name = cookie.get("name")
        value = cookie.get("value")
        if not name:
            continue
        session.cookies.set(
            name,
            value,
            domain=cookie.get("domain") or ".betsapi.com",
            path=cookie.get("path") or "/",
        )


def _build_browser_driver():
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except ModuleNotFoundError:
        print("[scraper] selenium nao instalado. Instale com: pip install selenium")
        return None
    except Exception as exc:
        print(f"[scraper] falha ao carregar selenium: {exc}")
        return None

    profile_dir = os.environ.get("BETSAPI_BROWSER_PROFILE_DIR", "data/browser_profile")
    profile_dir = os.path.abspath(profile_dir)
    os.makedirs(profile_dir, exist_ok=True)

    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-notifications")
    options.add_argument(f"--user-data-dir={profile_dir}")

    headless = os.environ.get("BETSAPI_BROWSER_HEADLESS", "0").strip().lower() in ("1", "true", "yes")
    if headless:
        options.add_argument("--headless=new")

    return webdriver.Chrome(options=options)


def _ensure_browser_driver():
    global _BROWSER_DRIVER
    if _BROWSER_DRIVER is not None:
        return _BROWSER_DRIVER
    _BROWSER_DRIVER = _build_browser_driver()
    return _BROWSER_DRIVER


def _browser_snapshot(driver):
    cookies = driver.get_cookies() or []
    try:
        user_agent = driver.execute_script("return navigator.userAgent")
    except Exception:
        user_agent = None
    return cookies, user_agent


def _browser_fetch(url: str):
    wait_seconds = int(os.environ.get("BETSAPI_CF_WAIT_SECONDS", "180"))
    try:
        from selenium.common.exceptions import TimeoutException
        from selenium.webdriver.support.ui import WebDriverWait
    except Exception as exc:
        print(f"[scraper] falha no webdriver: {exc}")
        return None, None, None

    with _CF_LOCK:
        for attempt in (1, 2):
            driver = _ensure_browser_driver()
            if driver is None:
                return None, None, None
            try:
                driver.get(url)
                WebDriverWait(driver, 25).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                if _is_cloudflare_content(driver.title or "") or _is_cloudflare_content(driver.page_source or ""):
                    print("[scraper] Cloudflare detectado. Resolva manualmente no navegador...")
                    _send_cf_telegram_alert(url)
                    try:
                        WebDriverWait(driver, wait_seconds).until(
                            lambda d: not (
                                _is_cloudflare_content(d.title or "")
                                or _is_cloudflare_content(d.page_source or "")
                            )
                        )
                    except TimeoutException:
                        print("[scraper] timeout ao aguardar liberacao do challenge.")
                        return None, None, None

                html = driver.page_source or ""
                cookies, user_agent = _browser_snapshot(driver)
                # Some valid pages may still contain "cloudflare" references in scripts/CDN links.
                # If clearance cookie exists and title is no longer challenge, accept the page.
                title_blocked = _is_cloudflare_content(driver.title or "")
                html_blocked = _is_cloudflare_content(html)
                if title_blocked or (html_blocked and not _has_cf_clearance_cookie(cookies)):
                    return None, None, None
                print("[scraper] Challenge liberado. Sessao persistente ativa.")
                return _SimpleResponse(driver.current_url or url, 200, html), cookies, user_agent
            except Exception as exc:
                # Browser closed/invalid session: try recreate once
                if attempt == 1:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    globals()["_BROWSER_DRIVER"] = None
                    continue
                print(f"[scraper] erro no navegador de desafio: {exc}")
                return None, None, None


def _browser_fallback_enabled() -> bool:
    mode = _cf_mode()
    return mode not in ("off", "0", "false", "no")


def _swap_base(url: str) -> str:
    if "://pt.betsapi.com" in url:
        return url.replace("://pt.betsapi.com", "://betsapi.com", 1)
    if "://betsapi.com" in url:
        return url.replace("://betsapi.com", "://pt.betsapi.com", 1)
    return url


def get_with_fallback(session, url):
    last_resp = None
    last_exc = None
    blocked_urls = []
    candidates = [url]
    alt_url = _swap_base(url)
    if alt_url != url:
        candidates.append(alt_url)

    for candidate in candidates:
        try:
            resp = session.get(candidate, timeout=15)
        except requests.RequestException as exc:
            last_exc = exc
            continue

        if resp.status_code == 403:
            session.headers.update({"Referer": candidate.split("/r/")[0], "Cache-Control": "no-cache"})
            try:
                resp = session.get(candidate, timeout=15)
            except requests.RequestException as exc:
                last_exc = exc
                continue

        if resp.status_code == 200 and not _is_cloudflare_response(resp):
            return resp

        if _is_cloudflare_response(resp):
            blocked_urls.append(candidate)
        last_resp = resp

    if _browser_fallback_enabled():
        for candidate in blocked_urls or candidates:
            browser_resp, cookies, user_agent = _browser_fetch(candidate)
            if not browser_resp:
                continue
            _apply_cookies_to_session(session, cookies)
            if user_agent:
                session.headers["User-Agent"] = user_agent
            return browser_resp

    if last_resp is not None:
        return last_resp
    if last_exc:
        raise last_exc
    raise requests.RequestException(f"falha ao acessar {url}")


def extrair_valor_td(td):
    span = td.find("span", class_="sr-only")
    return span.get_text(strip=True) if span else td.get_text(strip=True)


def _to_ascii(text: str) -> str:
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode("ascii", "ignore").decode("ascii")


def _normalize_team_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", _to_ascii(name).lower())


def _team_names_match(a: str, b: str) -> bool:
    na = _normalize_team_name(a)
    nb = _normalize_team_name(b)
    if not na or not nb:
        return False
    return na in nb or nb in na


def _split_team_text(text: str):
    if not text:
        return "", ""
    cleaned = " ".join(text.split())
    match = re.search(r"(.+?)\s+vs\.?\s+(.+)", cleaned, re.IGNORECASE)
    if not match:
        match = re.search(r"(.+?)\s+v\s+(.+)", cleaned, re.IGNORECASE)
    if not match:
        return "", ""
    home = match.group(1).strip()
    away = match.group(2).strip()
    home = home.split(" - ")[0].strip()
    away = away.split(" - ")[0].strip()
    return home, away


def _teams_from_title(soup):
    if not soup:
        return "", ""
    for tag in (soup.find("h1"), soup.title):
        if not tag:
            continue
        home, away = _split_team_text(tag.get_text(" ", strip=True))
        if home and away:
            return home, away
    return "", ""


def _teams_from_url(url: str):
    if not url:
        return "", ""
    try:
        path = urlparse(url).path or ""
    except Exception:
        path = ""
    slug_match = re.search(r"/r/\d+/(.+)$", path)
    if not slug_match:
        return "", ""
    slug = slug_match.group(1).strip("/")
    if not slug:
        return "", ""
    slug = slug.replace("_", "-")
    parts = re.split(r"-(?:vs|v)-", slug, flags=re.IGNORECASE)
    if len(parts) != 2:
        return "", ""
    home = parts[0].replace("-", " ").strip()
    away = parts[1].replace("-", " ").strip()
    return home, away


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
    text = text.replace("º", "o")
    text = text.replace("â€™", "").replace("'", "")
    text = text.replace("ï¼‹", "+").replace("ï¹¢", "+").replace("âº", "+")
    text = text.replace("ï¹£", "-").replace("âˆ’", "-")
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


def is_first_half_extra_time(time_text: str) -> bool:
    if not time_text:
        return False
    text = time_text.strip().lower()
    text = text.replace("º", "o")
    if "+" not in text:
        return False
    if any(token in text for token in SECOND_HALF_TOKENS):
        return False
    nums = re.findall(r"\d+", text)
    if not nums:
        return False
    base_minute = int(nums[0])
    return base_minute <= 45


def fetch_live_games(session):
    last_status = None
    for base in BASE_URLS:
        try:
            resp = get_with_fallback(session, base)
        except requests.RequestException:
            last_status = None
            continue
        last_status = resp.status_code
        if resp.status_code != 200:
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        break
    else:
        return [], last_status
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
        if not time_text:
            continue
        time_norm = time_text.strip().lower()
        is_halftime = "ht" in time_norm or "half time" in time_norm or "interval" in time_norm
        if not time_text[0].isdigit() and not is_halftime:
            continue

        game_link_tag = tr.find("a", href=re.compile(r"^/r/\d+"))
        if not game_link_tag:
            continue
        game_href = game_link_tag["href"]
        match_id = re.search(r"/r/(\d+)", game_href)
        if not match_id:
            continue
        game_id = match_id.group(1)
        minute_value = parse_minutes(time_text)
        if minute_value is None and is_halftime:
            minute_value = 45
        games.append(
            {
                "game_id": game_id,
                "url": base + game_href,
                "minute": minute_value,
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

    title_home, title_away = _teams_from_title(soup)
    url_home, url_away = _teams_from_url(url)

    def _valid_team(name: str) -> bool:
        return bool(name and name.strip() and not name.strip().isdigit())

    if not _valid_team(home_team) or not _valid_team(away_team):
        if _valid_team(title_home) and _valid_team(title_away):
            home_team, away_team = title_home, title_away
        elif _valid_team(url_home) and _valid_team(url_away):
            home_team, away_team = url_home, url_away
    elif _valid_team(url_home) and _valid_team(url_away):
        home_ok = _team_names_match(home_team, url_home)
        away_ok = _team_names_match(away_team, url_away)
        if not (home_ok and away_ok):
            if _valid_team(title_home) and _valid_team(title_away) and _team_names_match(title_home, url_home) and _team_names_match(title_away, url_away):
                home_team, away_team = title_home, title_away
            else:
                home_team, away_team = url_home, url_away

    stats = {}
    raw_stats = {}
    score = "0 x 0"

    all_rows = []
    for table in stat_tables:
        all_rows.extend(table.find_all("tr"))

    def _is_missing_pair(values):
        if not values:
            return True
        return all(not v or str(v).strip() in ("-", "â€”") for v in values)

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
