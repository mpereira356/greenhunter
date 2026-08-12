import os
import re
import shutil
import threading
import time
import unicodedata
import json
import base64
import sqlite3
import ctypes
import subprocess
import resource
import signal
from urllib.parse import urljoin, urlparse
import tempfile
from urllib.parse import unquote

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URLS = ("https://betsapi.com", "https://pt.betsapi.com")
SECOND_HALF_TOKENS = ("2nd", "2o", "2h", "second", "segundo")
CF_CHALLENGE_MARKERS = (
    "just a moment",
    "um momento",
    "cf-challenge",
    "checking your browser",
)
_CF_LOCK = threading.Lock()
_CF_LAST_SOLVED_AT = 0.0
_CF_ALERT_LAST_SENT_AT = 0.0
_CF_CONSECUTIVE_CHALLENGES = 0
_CF_RESTART_SCHEDULED = False
_BROWSER_DRIVER = None
_BROWSER_PROFILE_DIR = None
_PROFILE_LAST_CLEANUP_AT = 0.0
_SELENIUM_CACHE_LAST_CLEANUP_AT = 0.0
_PROFILE_COOKIES_LOADED = False
_PROFILE_COOKIES_LAST_ERROR_AT = 0.0
_BROWSER_LAST_LAUNCHED_AT = 0.0
_BROWSER_TARGET_ID = None
_BROWSER_SESSION_COOKIES = []
_BROWSER_SESSION_USER_AGENT = None


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
    _apply_manual_betsapi_cookies(session)
    return session


def _cookie_domains():
    return (".betsapi.com", "betsapi.com", "pt.betsapi.com")


def _set_session_cookie(session, name: str, value: str, domain: str = ".betsapi.com"):
    if not name or not value:
        return
    session.cookies.set(name, value, domain=domain, path="/")


def _apply_manual_betsapi_cookies(session):
    cookie_string = (os.environ.get("BETSAPI_COOKIE_STRING") or "").strip()
    if cookie_string:
        for part in cookie_string.split(";"):
            if "=" not in part:
                continue
            name, value = part.split("=", 1)
            for domain in _cookie_domains():
                _set_session_cookie(session, name.strip(), value.strip(), domain=domain)

    cf_clearance = (os.environ.get("BETSAPI_CF_CLEARANCE") or "").strip()
    if cf_clearance:
        for domain in _cookie_domains():
            _set_session_cookie(session, "cf_clearance", cf_clearance, domain=domain)

    _load_chrome_profile_cookies(session)


class _DATA_BLOB(ctypes.Structure):
    _fields_ = [("cbData", ctypes.c_uint), ("pbData", ctypes.POINTER(ctypes.c_char))]


def _dpapi_unprotect(encrypted_bytes: bytes) -> bytes:
    if not encrypted_bytes or os.name != "nt":
        return b""
    blob_in = _DATA_BLOB(
        len(encrypted_bytes),
        ctypes.cast(ctypes.create_string_buffer(encrypted_bytes), ctypes.POINTER(ctypes.c_char)),
    )
    blob_out = _DATA_BLOB()
    if not ctypes.windll.crypt32.CryptUnprotectData(
        ctypes.byref(blob_in), None, None, None, None, 0, ctypes.byref(blob_out)
    ):
        raise ctypes.WinError()
    try:
        return ctypes.string_at(blob_out.pbData, blob_out.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(blob_out.pbData)


def _chrome_master_key(profile_root: str) -> bytes:
    local_state = os.path.join(profile_root, "Local State")
    if not os.path.exists(local_state):
        return b""
    with open(local_state, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    encrypted_key = payload.get("os_crypt", {}).get("encrypted_key")
    if not encrypted_key:
        return b""
    raw = base64.b64decode(encrypted_key)
    if raw.startswith(b"DPAPI"):
        raw = raw[5:]
    return _dpapi_unprotect(raw)


def _decrypt_chrome_cookie(encrypted_value: bytes, master_key: bytes) -> str:
    if not encrypted_value:
        return ""
    if encrypted_value.startswith(b"v10") or encrypted_value.startswith(b"v11"):
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except Exception:
            return ""
        nonce = encrypted_value[3:15]
        ciphertext = encrypted_value[15:]
        if not nonce or not ciphertext or not master_key:
            return ""
        try:
            decrypted = AESGCM(master_key).decrypt(nonce, ciphertext, None)
        except Exception:
            return ""
        try:
            return decrypted.decode("utf-8")
        except UnicodeDecodeError:
            return ""
    try:
        decrypted = _dpapi_unprotect(encrypted_value)
        return decrypted.decode("utf-8")
    except UnicodeDecodeError:
        return ""
    except Exception:
        return ""


def _load_chrome_profile_cookies(session):
    global _PROFILE_COOKIES_LOADED, _PROFILE_COOKIES_LAST_ERROR_AT
    if _PROFILE_COOKIES_LOADED or os.name != "nt":
        return
    if _browser_debug_running():
        return
    if _PROFILE_COOKIES_LAST_ERROR_AT and (time.time() - _PROFILE_COOKIES_LAST_ERROR_AT) < 300:
        return
    profile_root = os.path.abspath(os.environ.get("BETSAPI_BROWSER_PROFILE_DIR", "data/browser_profile"))
    cookies_path = os.path.join(profile_root, "Default", "Network", "Cookies")
    if not os.path.exists(cookies_path):
        return

    tmp_copy = os.path.join(os.path.abspath("data"), "cookies_profile_copy.db")
    try:
        shutil.copy2(cookies_path, tmp_copy)
        master_key = _chrome_master_key(profile_root)
        con = sqlite3.connect(tmp_copy)
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT host_key, name, value, encrypted_value
            FROM cookies
            WHERE host_key LIKE '%betsapi%' OR name = 'cf_clearance'
            """
        ).fetchall()
        con.close()
    except PermissionError:
        _PROFILE_COOKIES_LAST_ERROR_AT = time.time()
        return
    except Exception as exc:
        _PROFILE_COOKIES_LAST_ERROR_AT = time.time()
        print(f"[scraper] nao foi possivel carregar cookies do perfil do Chrome: {exc}")
        return
    finally:
        try:
            if os.path.exists(tmp_copy):
                os.remove(tmp_copy)
        except OSError:
            pass

    loaded = 0
    for host_key, name, value, encrypted_value in rows:
        try:
            cookie_value = value or _decrypt_chrome_cookie(encrypted_value, master_key)
        except Exception:
            continue
        if not cookie_value:
            continue
        _set_session_cookie(session, name, cookie_value, domain=host_key or ".betsapi.com")
        loaded += 1
    if loaded:
        _PROFILE_COOKIES_LOADED = True
        print(f"[scraper] cookies carregados do perfil Chrome: {loaded}")


def _cf_mode() -> str:
    return os.environ.get("BETSAPI_CF_MODE", "manual").strip().lower()


def _cf_alert_cooldown_seconds() -> int:
    raw = (
        os.environ.get("BETSAPI_CF_ALERT_COOLDOWN_SECONDS")
        or os.environ.get("BETSAPI_CF_COOLDOWN_SECONDS")
        or "300"
    ).strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 300


def _cf_alert_credentials():
    token = (os.environ.get("BETSAPI_CF_ALERT_TELEGRAM_TOKEN") or "").strip()
    chat_id = (os.environ.get("BETSAPI_CF_ALERT_TELEGRAM_CHAT_ID") or "").strip()
    if token and chat_id:
        return token, chat_id

    db_path = (os.environ.get("GREENHUNTER_DB_PATH") or "data/app.db").strip()
    if not db_path or not os.path.exists(db_path):
        return None, None
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                """
                select telegram_token, telegram_chat_id
                  from user
                 where telegram_verified = 1
                   and telegram_token is not null
                   and telegram_token <> ''
                   and telegram_chat_id is not null
                   and telegram_chat_id <> ''
                 order by case when lower(username) = 'admin' then 0 else 1 end, id
                 limit 1
                """
            ).fetchone()
    except sqlite3.Error:
        return None, None
    if not row:
        return None, None
    return (row[0] or "").strip(), (row[1] or "").strip()


def _send_cf_telegram_alert(url: str):
    token, chat_id = _cf_alert_credentials()
    if not token or not chat_id:
        print("[scraper] alerta Cloudflare nao enviado: Telegram nao configurado.")
        return

    global _CF_ALERT_LAST_SENT_AT
    now = time.time()
    cooldown = _cf_alert_cooldown_seconds()
    if cooldown > 0 and (now - _CF_ALERT_LAST_SENT_AT) < cooldown:
        return

    text = (
        "GreenHunter: Cloudflare pediu verificacao humana novamente.\n"
        "O bot esta tentando liberar o checkbox automaticamente. Se o desafio continuar preso, "
        "o Chrome e o worker serao reciclados sem intervencao manual.\n"
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


def _cf_auto_restart_threshold() -> int:
    raw = (os.environ.get("BETSAPI_CF_AUTO_RESTART_AFTER") or "3").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 3


def _send_cf_recovery_alert(url: str, count: int):
    token, chat_id = _cf_alert_credentials()
    if not token or not chat_id:
        return
    text = (
        f"GreenHunter: Cloudflare apareceu {count} vezes seguidas.\n"
        "Recuperacao automatica acionada: o worker e o Chrome serao reiniciados agora.\n"
        f"URL: {url}"
    )
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
            timeout=10,
        )
    except requests.RequestException:
        pass


def _schedule_worker_restart(delay_seconds: float):
    """Close the stale Chrome and let the Gunicorn master replace this worker."""
    def _restart():
        time.sleep(max(0.0, delay_seconds))
        _close_debug_browser()
        os.kill(os.getpid(), signal.SIGKILL)

    threading.Thread(target=_restart, name="cf-auto-restart", daemon=True).start()


def _close_debug_browser():
    try:
        urllib = __import__("urllib.request", fromlist=["request"])
        version = json.load(urllib.urlopen(f"{_browser_debug_base()}/json/version", timeout=3))
        ws_url = version.get("webSocketDebuggerUrl")
        if not ws_url:
            return
        ws = _cdp_connect(ws_url)
        try:
            ws.send(json.dumps({"id": 9000, "method": "Browser.close"}))
        finally:
            ws.close()
    except Exception as exc:
        print(f"[scraper] Chrome nao respondeu ao fechamento automatico: {exc}")


def _record_cf_challenge(url: str) -> bool:
    """Return True once when repeated challenges require a full clean restart."""
    global _CF_CONSECUTIVE_CHALLENGES, _CF_RESTART_SCHEDULED
    _CF_CONSECUTIVE_CHALLENGES += 1
    threshold = _cf_auto_restart_threshold()
    if threshold <= 0 or _CF_CONSECUTIVE_CHALLENGES < threshold or _CF_RESTART_SCHEDULED:
        return False

    _CF_RESTART_SCHEDULED = True
    count = _CF_CONSECUTIVE_CHALLENGES
    print(f"[scraper] Cloudflare recorrente ({count}x); reinicio automatico do worker agendado.")
    _send_cf_recovery_alert(url, count)
    delay = float(os.environ.get("BETSAPI_CF_AUTO_RESTART_DELAY_SECONDS", "2"))
    _schedule_worker_restart(delay)
    return True


def _force_cf_timeout_recovery(url: str) -> bool:
    """Recycle a worker after one challenge stays blocked beyond the safe wait."""
    global _CF_RESTART_SCHEDULED
    if _CF_RESTART_SCHEDULED:
        return False
    raw = (os.environ.get("BETSAPI_CF_RECYCLE_ON_TIMEOUT") or "1").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False

    _CF_RESTART_SCHEDULED = True
    count = max(1, _CF_CONSECUTIVE_CHALLENGES)
    print("[scraper] Cloudflare permaneceu bloqueado; reciclando Chrome e worker automaticamente.")
    _send_cf_recovery_alert(url, count)
    delay = float(os.environ.get("BETSAPI_CF_AUTO_RESTART_DELAY_SECONDS", "2"))
    _schedule_worker_restart(delay)
    return True


def _cf_auto_recovery_timeout_seconds(wait_seconds: int) -> int:
    raw = (os.environ.get("BETSAPI_CF_AUTO_RECOVERY_TIMEOUT_SECONDS") or "45").strip()
    try:
        configured = max(10, int(raw))
    except ValueError:
        configured = 45
    return min(wait_seconds, configured)


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


def _remember_browser_session(cookies, user_agent=None):
    """Share a browser-renewed session with every scraper Session in this process."""
    global _BROWSER_SESSION_COOKIES, _BROWSER_SESSION_USER_AGENT
    if cookies:
        _BROWSER_SESSION_COOKIES = [dict(cookie) for cookie in cookies]
    if user_agent:
        _BROWSER_SESSION_USER_AGENT = user_agent


def _apply_remembered_browser_session(session):
    if _BROWSER_SESSION_COOKIES:
        _apply_cookies_to_session(session, _BROWSER_SESSION_COOKIES)
    if _BROWSER_SESSION_USER_AGENT:
        session.headers["User-Agent"] = _BROWSER_SESSION_USER_AGENT


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

    # Selenium Manager may default to a sandboxed home/cache location that is not writable.
    selenium_cache_dir = os.path.abspath(os.environ.get("BETSAPI_SELENIUM_CACHE_DIR", "data/selenium_cache"))
    try:
        os.makedirs(selenium_cache_dir, exist_ok=True)
    except OSError as exc:
        print(f"[scraper] falha ao preparar cache do selenium em {selenium_cache_dir}: {exc}")
    else:
        _cleanup_selenium_cache(selenium_cache_dir)
        os.environ.setdefault("SE_CACHE_PATH", selenium_cache_dir)
        os.environ.setdefault("SE_DOWNLOAD_PATH", selenium_cache_dir)

    # Use a stable profile dir by default, but fall back to a temp dir in headless
    # or when explicitly requested to avoid profile locks on VPS.
    global _BROWSER_PROFILE_DIR
    profile_dir_env = os.environ.get("BETSAPI_BROWSER_PROFILE_DIR", "data/browser_profile").strip()
    force_temp = os.environ.get("BETSAPI_BROWSER_PROFILE_TEMP", "0").strip().lower() in ("1", "true", "yes")

    headless_env = os.environ.get("BETSAPI_BROWSER_HEADLESS", "0").strip().lower() in ("1", "true", "yes")
    forced_display = (os.environ.get("BETSAPI_DISPLAY") or "").strip()
    if forced_display:
        os.environ["DISPLAY"] = forced_display
    has_display = os.name == "nt" or bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))
    headless = headless_env or not has_display
    profile_dir = None
    lock_detected = False
    if profile_dir_env and not headless and not force_temp:
        candidate_dir = os.path.abspath(profile_dir_env)
        # Chrome creates these files when a profile is in use.
        lock_files = ("SingletonLock", "SingletonCookie", "SingletonSocket")
        lock_detected = any(os.path.exists(os.path.join(candidate_dir, lf)) for lf in lock_files)
        if not lock_detected:
            profile_dir = candidate_dir

    if force_temp or headless or not profile_dir_env or profile_dir is None:
        if _BROWSER_PROFILE_DIR is None:
            _BROWSER_PROFILE_DIR = tempfile.mkdtemp(prefix="gh_chrome_profile_")
        profile_dir = _BROWSER_PROFILE_DIR
    else:
        os.makedirs(profile_dir, exist_ok=True)

    _cleanup_browser_profile(profile_dir)

    options = Options()
    chrome_binary = (os.environ.get("BETSAPI_CHROME_BINARY") or os.environ.get("CHROME_BIN") or "").strip()
    if not chrome_binary and os.name == "nt":
        for candidate in (
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        ):
            if os.path.exists(candidate):
                chrome_binary = candidate
                break
    if chrome_binary:
        options.binary_location = chrome_binary
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-notifications")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-background-networking")
    options.add_argument("--remote-debugging-port=0")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-features=VizDisplayCompositor")
    if os.name == "nt":
        options.add_argument("--disable-features=RendererCodeIntegrity")
    options.add_argument(f"--user-data-dir={profile_dir}")

    print(
        f"[scraper] chrome display={'yes' if has_display else 'no'} "
        f"display_var={os.environ.get('DISPLAY') or 'unset'} "
        f"headless={'yes' if headless else 'no'} "
        f"profile={profile_dir} "
        f"lock={'yes' if lock_detected else 'no'} "
        f"binary={chrome_binary or 'auto'}"
    )

    if headless:
        runtime_root = os.path.abspath(os.environ.get("BETSAPI_BROWSER_RUNTIME_DIR", "data/browser_runtime"))
        os.makedirs(runtime_root, exist_ok=True)
        options.add_argument(f"--data-path={os.path.join(runtime_root, 'data')}")
        options.add_argument(f"--disk-cache-dir={os.path.join(runtime_root, 'cache')}")
        options.add_argument(f"--crash-dumps-dir={os.path.join(runtime_root, 'crash')}")
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1280,720")

    chromedriver_path = (os.environ.get("BETSAPI_CHROMEDRIVER") or "").strip()
    if chromedriver_path:
        from selenium.webdriver.chrome.service import Service
        service = Service(executable_path=chromedriver_path)
        return webdriver.Chrome(service=service, options=options)

    return webdriver.Chrome(options=options)


def _dir_size_bytes(path: str) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                continue
    return total


def _remove_path(target: str) -> int:
    if not target or not os.path.exists(target):
        return 0
    removed = 0
    try:
        if os.path.isdir(target):
            removed = _dir_size_bytes(target)
            shutil.rmtree(target, ignore_errors=True)
        else:
            removed = os.path.getsize(target)
            os.remove(target)
    except OSError:
        return 0
    return removed


def _cleanup_large_dir(
    path: str,
    *,
    max_mb: int,
    interval_seconds: int,
    removable_rel_paths: list[str],
    label: str,
    last_cleanup_attr: str,
    max_age_hours: int = 72,
):
    if not path or not os.path.isdir(path):
        return

    now_ts = time.time()
    last_cleanup_at = globals().get(last_cleanup_attr, 0.0) or 0.0
    if interval_seconds > 0 and (now_ts - last_cleanup_at) < interval_seconds:
        return
    globals()[last_cleanup_attr] = now_ts

    max_bytes = max_mb * 1024 * 1024 if max_mb > 0 else 0
    current_size = _dir_size_bytes(path)
    if max_bytes and current_size <= max_bytes:
        return

    removed = 0
    for rel in removable_rel_paths:
        removed += _remove_path(os.path.join(path, rel))

    after_size = _dir_size_bytes(path)
    if max_age_hours > 0 and (max_bytes == 0 or after_size > max_bytes):
        cutoff_ts = now_ts - (max_age_hours * 3600)
        stale_items = []
        for entry in os.scandir(path):
            if entry.name in {".", ".."}:
                continue
            try:
                mtime = entry.stat().st_mtime
            except OSError:
                continue
            if mtime <= cutoff_ts:
                stale_items.append((mtime, entry.path))
        for _, target in sorted(stale_items):
            removed += _remove_path(target)
            after_size = _dir_size_bytes(path)
            if max_bytes and after_size <= max_bytes:
                break

    if removed > 0:
        print(
            f"[scraper] limpeza {label}: "
            f"{round(removed / (1024*1024), 1)} MB removidos "
            f"(atual={round(after_size / (1024*1024), 1)} MB, limite={max_mb} MB)"
        )


def _cleanup_browser_profile(profile_dir: str):
    max_mb = int(os.environ.get("BROWSER_PROFILE_MAX_MB", "350"))
    interval = int(os.environ.get("BROWSER_PROFILE_CLEANUP_INTERVAL_SECONDS", "1800"))
    removable_rel_paths = [
        os.path.join("Default", "Cache"),
        os.path.join("Default", "Code Cache"),
        os.path.join("Default", "GPUCache"),
        os.path.join("Default", "WebStorage"),
        os.path.join("Default", "Service Worker", "CacheStorage"),
        os.path.join("Default", "History"),
        os.path.join("Default", "History-journal"),
        os.path.join("Default", "Favicons"),
        os.path.join("Default", "Favicons-journal"),
        "GrShaderCache",
        "DawnCache",
        "ShaderCache",
        "component_crx_cache",
        "optimization_guide_model_store",
        "segmentation_platform",
        "BrowserMetrics",
    ]
    _cleanup_large_dir(
        profile_dir,
        max_mb=max_mb,
        interval_seconds=interval,
        removable_rel_paths=removable_rel_paths,
        label="browser_profile",
        last_cleanup_attr="_PROFILE_LAST_CLEANUP_AT",
        max_age_hours=int(os.environ.get("BROWSER_PROFILE_MAX_AGE_HOURS", "72")),
    )


def _cleanup_selenium_cache(cache_dir: str):
    max_mb = int(os.environ.get("BETSAPI_SELENIUM_CACHE_MAX_MB", "250"))
    interval = int(os.environ.get("BETSAPI_SELENIUM_CACHE_CLEANUP_INTERVAL_SECONDS", "1800"))
    removable_rel_paths = [
        "se-metadata.json",
        "chrome",
        "chromedriver",
    ]
    _cleanup_large_dir(
        cache_dir,
        max_mb=max_mb,
        interval_seconds=interval,
        removable_rel_paths=removable_rel_paths,
        label="selenium_cache",
        last_cleanup_attr="_SELENIUM_CACHE_LAST_CLEANUP_AT",
        max_age_hours=int(os.environ.get("BETSAPI_SELENIUM_CACHE_MAX_AGE_HOURS", "72")),
    )


def _browser_debug_port() -> int:
    raw = (os.environ.get("BETSAPI_BROWSER_DEBUG_PORT") or "9222").strip()
    try:
        return int(raw)
    except ValueError:
        return 9222


def _browser_debug_base() -> str:
    return f"http://127.0.0.1:{_browser_debug_port()}"


def _browser_chrome_binary() -> str:
    chrome_binary = (os.environ.get("BETSAPI_CHROME_BINARY") or os.environ.get("CHROME_BIN") or "").strip()
    if not chrome_binary and os.name == "nt":
        for candidate in (
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        ):
            if os.path.exists(candidate):
                chrome_binary = candidate
                break
    if not chrome_binary and os.name != "nt":
        for candidate in (
            "/usr/bin/google-chrome-stable",
            "/usr/bin/google-chrome",
            "/usr/bin/chrome",
            "/usr/bin/chromium-browser",
            "/usr/bin/chromium",
            "/snap/bin/chromium",
        ):
            if os.path.exists(candidate):
                chrome_binary = candidate
                break
    return chrome_binary


def _browser_profile_dir() -> str:
    profile_dir = os.path.abspath(os.environ.get("BETSAPI_BROWSER_PROFILE_DIR", "data/browser_profile"))
    os.makedirs(profile_dir, exist_ok=True)
    _cleanup_browser_profile(profile_dir)
    return profile_dir


def _browser_debug_running() -> bool:
    try:
        urllib = __import__("urllib.request", fromlist=["request"])
        urllib.urlopen(f"{_browser_debug_base()}/json/version", timeout=2).read()
        return True
    except Exception:
        return False


def _launch_debug_browser(start_url: str):
    global _BROWSER_LAST_LAUNCHED_AT
    chrome_binary = _browser_chrome_binary()
    if not chrome_binary:
        raise RuntimeError("Chrome/Edge nao encontrado para o fallback do BetsAPI")
    profile_dir = _browser_profile_dir()
    env = os.environ.copy()
    forced_display = (env.get("BETSAPI_DISPLAY") or "").strip()
    if forced_display and not env.get("DISPLAY"):
        env["DISPLAY"] = forced_display
    if not env.get("XAUTHORITY"):
        xauthority = (env.get("BETSAPI_XAUTHORITY") or os.path.expanduser("~/.Xauthority")).strip()
        if xauthority and os.path.exists(xauthority):
            env["XAUTHORITY"] = xauthority
    args = [
        chrome_binary,
        f"--remote-debugging-port={_browser_debug_port()}",
        "--remote-allow-origins=*",
        f"--user-data-dir={profile_dir}",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-background-networking",
        start_url,
    ]
    kwargs = {
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "stdin": subprocess.DEVNULL,
        "env": env,
    }
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    else:
        def _reset_child_limits():
            _, hard_limit = resource.getrlimit(resource.RLIMIT_AS)
            if hard_limit == resource.RLIM_INFINITY:
                resource.setrlimit(resource.RLIMIT_AS, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))
            else:
                resource.setrlimit(resource.RLIMIT_AS, (hard_limit, hard_limit))

        kwargs["preexec_fn"] = _reset_child_limits
    subprocess.Popen(args, **kwargs)
    _BROWSER_LAST_LAUNCHED_AT = time.time()


def _cdp_connect(ws_url: str):
    import websocket
    return websocket.create_connection(ws_url, timeout=10, origin=_browser_debug_base())


def _cdp_recv_result(ws, command_id: int, timeout_seconds: float = 15.0):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        msg = json.loads(ws.recv())
        if msg.get("id") == command_id:
            if "error" in msg:
                raise RuntimeError(msg["error"])
            return msg.get("result", {})
    raise TimeoutError(f"timeout aguardando resposta do CDP para id={command_id}")


def _cdp_eval(ws, command_id: int, expression: str, timeout_seconds: float = 15.0):
    ws.send(json.dumps({
        "id": command_id,
        "method": "Runtime.evaluate",
        "params": {"expression": expression, "returnByValue": True},
    }))
    return _cdp_recv_result(ws, command_id, timeout_seconds).get("result", {}).get("value")


def _cf_auto_click_enabled() -> bool:
    raw = (os.environ.get("BETSAPI_CF_AUTO_CLICK") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _dispatch_cdp_click(ws, command_id: int, x: float, y: float):
    for offset, event_type in enumerate(("mouseMoved", "mousePressed", "mouseReleased")):
        params = {
            "type": event_type,
            "x": x,
            "y": y,
            "button": "left",
            "clickCount": 1,
        }
        if event_type == "mouseMoved":
            params.pop("button")
            params.pop("clickCount")
        ws.send(json.dumps({
            "id": command_id + offset,
            "method": "Input.dispatchMouseEvent",
            "params": params,
        }))
        _cdp_recv_result(ws, command_id + offset, timeout_seconds=5)


def _try_click_cloudflare_turnstile(ws) -> bool:
    try:
        ws.send(json.dumps({"id": 5000, "method": "Page.bringToFront"}))
        _cdp_recv_result(ws, 5000, timeout_seconds=5)
    except Exception:
        pass

    frame_info = _cdp_eval(ws, 5001, """
(() => {
  const frames = Array.from(document.querySelectorAll('iframe')).map((frame) => {
    const rect = frame.getBoundingClientRect();
    return {
      src: frame.src || '',
      title: frame.title || '',
      left: rect.left,
      top: rect.top,
      width: rect.width,
      height: rect.height,
      visible: rect.width > 0 && rect.height > 0
    };
  });
  const candidates = frames.filter((frame) => {
    const text = `${frame.src} ${frame.title}`.toLowerCase();
    return frame.visible && (
      text.includes('challenges.cloudflare.com') ||
      text.includes('turnstile') ||
      text.includes('challenge')
    );
  });
  return {
    innerWidth: window.innerWidth,
    innerHeight: window.innerHeight,
    frames: candidates.length ? candidates : frames.filter((frame) => frame.visible)
  };
})()
""", timeout_seconds=5) or {}

    frames = frame_info.get("frames") or []
    target = frames[0] if frames else None
    if target:
        left = float(target.get("left") or 0)
        top = float(target.get("top") or 0)
        width = max(1.0, float(target.get("width") or 1))
        height = max(1.0, float(target.get("height") or 1))
        x = left + min(35.0, max(12.0, width * 0.12))
        y = top + (height / 2.0)
    else:
        inner_width = float(frame_info.get("innerWidth") or 1280)
        inner_height = float(frame_info.get("innerHeight") or 720)
        x = min(max(45.0, inner_width * 0.035), inner_width - 20.0)
        y = min(max(335.0, inner_height * 0.46), inner_height - 20.0)

    _dispatch_cdp_click(ws, 5010, x, y)
    return True


def _cdp_get_page_target(url: str):
    urllib = __import__("urllib.request", fromlist=["request"])
    global _BROWSER_TARGET_ID
    try:
        targets = json.load(urllib.urlopen(f"{_browser_debug_base()}/json/list", timeout=10))
    except Exception:
        targets = []

    if _BROWSER_TARGET_ID:
        for target in targets:
            if target.get("id") == _BROWSER_TARGET_ID and target.get("type") == "page":
                return target

    for target in targets:
        if target.get("type") != "page":
            continue
        target_url = (target.get("url") or "").lower()
        target_title = (target.get("title") or "").lower()
        if "betsapi.com" in target_url or "betsapi" in target_title:
            _BROWSER_TARGET_ID = target.get("id")
            return target

    req = urllib.Request(f"{_browser_debug_base()}/json/new?about:blank", method="PUT")
    target = json.load(urllib.urlopen(req, timeout=10))
    _BROWSER_TARGET_ID = target.get("id")
    return target


def _browser_fetch(url: str):
    wait_seconds = int(os.environ.get("BETSAPI_CF_WAIT_SECONDS", "180"))
    urllib = __import__("urllib.request", fromlist=["request"])

    # Only one request may drive the shared Chrome window. Previously every
    # caller queued here for up to BETSAPI_CF_WAIT_SECONDS. Once the challenge
    # was released, the backlog resumed at once and caused SQLite lock storms.
    lock_wait_seconds = float(os.environ.get("BETSAPI_CF_LOCK_WAIT_SECONDS", "1"))
    if not _CF_LOCK.acquire(timeout=max(0.0, lock_wait_seconds)):
        print("[scraper] recuperacao Cloudflare ja em andamento; tentativa adiada.")
        return None, None, None
    try:
        if not _browser_debug_running():
            recently_launched = _BROWSER_LAST_LAUNCHED_AT and (time.time() - _BROWSER_LAST_LAUNCHED_AT) < 20
            if recently_launched:
                time.sleep(2)
            if not _browser_debug_running():
                try:
                    _launch_debug_browser(url)
                except Exception as exc:
                    print(f"[scraper] nao foi possivel iniciar navegador de fallback: {exc}")
                    return None, None, None
            for _ in range(30):
                if _browser_debug_running():
                    break
                time.sleep(1)
            else:
                print("[scraper] depuracao remota do Chrome nao respondeu a tempo.")
                return None, None, None

        try:
            target = _cdp_get_page_target(url)
            ws = _cdp_connect(target["webSocketDebuggerUrl"])
        except Exception as exc:
            print(f"[scraper] falha ao conectar no DevTools do Chrome: {exc}")
            return None, None, None

        try:
            ws.send(json.dumps({"id": 1, "method": "Page.enable"}))
            _cdp_recv_result(ws, 1)
            ws.send(json.dumps({"id": 2, "method": "Runtime.enable"}))
            _cdp_recv_result(ws, 2)
            ws.send(json.dumps({"id": 3, "method": "Network.enable"}))
            _cdp_recv_result(ws, 3)
            ws.send(json.dumps({"id": 4, "method": "Page.navigate", "params": {"url": url}}))
            _cdp_recv_result(ws, 4)

            ready_deadline = time.time() + 25
            while time.time() < ready_deadline:
                ws.send(json.dumps({"id": 10, "method": "Runtime.evaluate", "params": {"expression": "document.readyState", "returnByValue": True}}))
                state = _cdp_recv_result(ws, 10).get("result", {}).get("value")
                ws.send(json.dumps({"id": 18, "method": "Runtime.evaluate", "params": {"expression": "location.href", "returnByValue": True}}))
                current_url = _cdp_recv_result(ws, 18).get("result", {}).get("value") or ""
                ws.send(json.dumps({"id": 19, "method": "Runtime.evaluate", "params": {"expression": "document.documentElement.outerHTML", "returnByValue": True}}))
                current_html = _cdp_recv_result(ws, 19).get("result", {}).get("value") or ""
                if state == "complete" and current_url.startswith(("https://betsapi.com", "https://pt.betsapi.com")) and len(current_html) > 1000:
                    break
                time.sleep(0.5)

            ws.send(json.dumps({"id": 11, "method": "Runtime.evaluate", "params": {"expression": "document.title", "returnByValue": True}}))
            title = _cdp_recv_result(ws, 11).get("result", {}).get("value") or ""
            ws.send(json.dumps({"id": 12, "method": "Runtime.evaluate", "params": {"expression": "document.documentElement.outerHTML", "returnByValue": True}}))
            html = _cdp_recv_result(ws, 12).get("result", {}).get("value") or ""
            ws.send(json.dumps({"id": 13, "method": "Runtime.evaluate", "params": {"expression": "navigator.userAgent", "returnByValue": True}}))
            user_agent = _cdp_recv_result(ws, 13).get("result", {}).get("value")
            ws.send(json.dumps({"id": 14, "method": "Network.getCookies", "params": {"urls": ["https://betsapi.com/", "https://pt.betsapi.com/"]}}))
            cookies = _cdp_recv_result(ws, 14).get("cookies", [])

            if _is_cloudflare_content(title) or (_is_cloudflare_content(html) and not _has_cf_clearance_cookie(cookies)):
                print("[scraper] Cloudflare detectado. Tentando clicar no checkbox automaticamente...")
                auto_click = _cf_auto_click_enabled()
                last_click_at = 0.0
                if auto_click:
                    try:
                        if _try_click_cloudflare_turnstile(ws):
                            last_click_at = time.time()
                    except Exception as exc:
                        print(f"[scraper] clique automatico no Cloudflare falhou: {exc}")
                _send_cf_telegram_alert(url)
                if _record_cf_challenge(url):
                    return None, None, None
                deadline = time.time() + _cf_auto_recovery_timeout_seconds(wait_seconds)
                while time.time() < deadline:
                    time.sleep(2)
                    if auto_click and (time.time() - last_click_at) >= 8:
                        try:
                            if _try_click_cloudflare_turnstile(ws):
                                last_click_at = time.time()
                        except Exception as exc:
                            print(f"[scraper] nova tentativa de clique Cloudflare falhou: {exc}")
                            last_click_at = time.time()
                    ws.send(json.dumps({"id": 15, "method": "Runtime.evaluate", "params": {"expression": "document.documentElement.outerHTML", "returnByValue": True}}))
                    html = _cdp_recv_result(ws, 15).get("result", {}).get("value") or ""
                    ws.send(json.dumps({"id": 16, "method": "Runtime.evaluate", "params": {"expression": "document.title", "returnByValue": True}}))
                    title = _cdp_recv_result(ws, 16).get("result", {}).get("value") or ""
                    ws.send(json.dumps({"id": 17, "method": "Network.getCookies", "params": {"urls": ["https://betsapi.com/", "https://pt.betsapi.com/"]}}))
                    cookies = _cdp_recv_result(ws, 17).get("cookies", [])
                    if not (_is_cloudflare_content(title) or (_is_cloudflare_content(html) and not _has_cf_clearance_cookie(cookies))):
                        break
                else:
                    print("[scraper] timeout ao aguardar liberacao do challenge.")
                    _force_cf_timeout_recovery(url)
                    return None, None, None

            print("[scraper] Challenge liberado. Sessao persistente ativa.")
            _remember_browser_session(cookies, user_agent)
            return _SimpleResponse(url, 200, html), cookies, user_agent
        except Exception as exc:
            print(f"[scraper] erro no navegador de desafio: {exc}")
            return None, None, None
        finally:
            try:
                ws.close()
            except Exception:
                pass
    finally:
        _CF_LOCK.release()


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

    # A clearance obtained by either worker is immediately reused by the
    # others instead of waiting for each Session to hit Cloudflare itself.
    _apply_remembered_browser_session(session)

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
            try:
                browser_resp, cookies, user_agent = _browser_fetch(candidate)
            except Exception as exc:
                print(f"[scraper] browser fallback falhou para {candidate}: {exc}")
                continue
            if not browser_resp:
                continue
            _apply_cookies_to_session(session, cookies)
            if user_agent:
                session.headers["User-Agent"] = user_agent
            _remember_browser_session(cookies, user_agent)
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
    ascii_name = _to_ascii(name).lower()
    # A origem alterna nomes em inglês e português conforme o domínio.
    aliases = {
        "reserva": "reserves",
        "reservas": "reserves",
        "f": "women",
        "w": "women",
        "fem": "women",
        "feminina": "women",
        "feminino": "women",
        "mulheres": "women",
    }
    tokens = [aliases.get(token, token) for token in re.split(r"[^a-z0-9]+", ascii_name) if token]
    return "".join(tokens)


def _team_identity_name(name: str) -> str:
    ascii_name = _to_ascii(name).lower()
    aliases = {
        "reserva": "reserves", "reservas": "reserves",
        "f": "women", "w": "women", "fem": "women",
        "feminina": "women", "feminino": "women", "mulheres": "women",
    }
    ignored = {"fc", "cf", "sc", "ac", "afc", "ca", "club", "de", "da", "do", "del"}
    tokens = [aliases.get(token, token) for token in re.split(r"[^a-z0-9]+", ascii_name) if token]
    return "".join(token for token in tokens if token not in ignored)


def _team_names_match(a: str, b: str) -> bool:
    na = _normalize_team_name(a)
    nb = _normalize_team_name(b)
    if not na or not nb:
        return False
    if na in nb or nb in na:
        return True
    ia = _team_identity_name(a)
    ib = _team_identity_name(b)
    return bool(ia and ib and (ia in ib or ib in ia))


def _team_name_in_text(team_name: str, text: str) -> bool:
    if not team_name or not text:
        return False
    ascii_name = _to_ascii(team_name).lower()
    tokens = [token for token in re.split(r"[^a-z0-9]+", ascii_name) if token]
    if not tokens:
        return False
    club_markers = {"fc", "cf", "sc", "ac", "afc", "club"}
    variants = {_normalize_team_name(team_name)}
    if tokens and tokens[0] in club_markers:
        variants.add("".join(tokens[1:]))
    if tokens and tokens[-1] in club_markers:
        variants.add("".join(tokens[:-1]))
    if len(tokens) >= 2 and tokens[-2:] == ["football", "club"]:
        variants.add("".join(tokens[:-2]))
    text_norm = _normalize_team_name(text)
    if any(len(variant) >= 4 and variant in text_norm for variant in variants):
        return True
    identity_name = _team_identity_name(team_name)
    identity_text = _team_identity_name(text)
    if len(identity_name) >= 4 and identity_name in identity_text:
        return True
    # Siglas reais como CRB, ABC e PSG precisam de correspondência por
    # palavra completa; procurar apenas na string concatenada geraria falsos
    # positivos dentro de nomes maiores.
    text_tokens = {
        _normalize_team_name(token)
        for token in re.split(r"[^a-zA-Z0-9À-ÿ]+", text)
        if token
    }
    return any(len(variant) == 3 and variant in text_tokens for variant in variants)


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
    home = _clean_team_name(home)
    away = _clean_team_name(away)
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


def _match_time_from_text(text: str) -> str:
    if not text:
        return ""
    compact = " ".join(text.split())
    match = re.search(r"(\d+\s*(?:\+\s*\d+)?\s*')", compact)
    if match:
        return match.group(1).replace(" ", "")
    upper = compact.upper()
    if " HT " in f" {upper} ":
        return "HT"
    if " FT " in f" {upper} ":
        return "FT"
    return ""


def _extract_match_time_text(soup) -> str:
    if not soup:
        return ""
    time_tag = soup.find("span", class_="race-time")
    if time_tag:
        text = time_tag.get_text(strip=True)
        if text:
            return text
    badge_selectors = (
        "td[id$='T'] span.badge",
        "span.badge.bg-danger-lt",
        "span.badge.bg-warning-lt",
        "span.badge.bg-success-lt",
        "span.badge.bg-secondary-lt",
    )
    for selector in badge_selectors:
        time_tag = soup.select_one(selector)
        if not time_tag:
            continue
        text = time_tag.get_text(" ", strip=True)
        text = _match_time_from_text(text)
        if text:
            return text
    for tag in (soup.find("h1"), soup.title):
        if not tag:
            continue
        text = _match_time_from_text(tag.get_text(" ", strip=True))
        if text:
            return text
    return ""


def _teams_from_url(url: str):
    if not url:
        return "", ""
    try:
        path = urlparse(url).path or ""
    except Exception:
        path = ""
    slug_match = re.search(r"(?:/[a-z]+)?/r/\d+/(.+)$", path)
    if not slug_match:
        return "", ""
    slug = unquote(slug_match.group(1).strip("/"))
    if not slug:
        return "", ""
    slug = slug.replace("_", "-")
    parts = re.split(r"-(?:vs|v)-", slug, flags=re.IGNORECASE)
    if len(parts) != 2:
        return "", ""
    home = _clean_team_name(parts[0].replace("-", " ").strip())
    away = _clean_team_name(parts[1].replace("-", " ").strip())
    return home, away


def _extract_row_teams(tr, fallback_url: str):
    if tr:
        for anchor in tr.find_all("a", href=True):
            text = anchor.get_text(" ", strip=True)
            home, away = _split_team_text(text)
            if home and away:
                return home, away
    return _teams_from_url(fallback_url)


def _extract_row_score(tr):
    if not tr:
        return "0 x 0"
    for text in tr.stripped_strings:
        score = _extract_score_from_text(text)
        if score:
            return score
    return "0 x 0"


def _extract_score_from_text(text: str) -> str | None:
    if not text:
        return None
    compact = " ".join(str(text).split())
    match = re.search(r"(\d+)\s*[-:x]\s*(\d+)", compact)
    if not match:
        return None
    return f"{int(match.group(1))} x {int(match.group(2))}"


def _parse_score_pair(text: str) -> tuple[int, int]:
    score = _extract_score_from_text(text or "")
    if not score:
        return 0, 0
    home, away = score.split(" x ", 1)
    return int(home), int(away)


def _extract_match_score(soup, rows) -> str | None:
    if rows:
        first = rows[0].find_all("td")
        if len(first) == 3:
            header_score = _extract_score_from_text(first[1].get_text(" ", strip=True))
            if header_score:
                return header_score
    if not soup:
        return None
    for tag in (soup.find("h1"), soup.title):
        if not tag:
            continue
        title_score = _extract_score_from_text(tag.get_text(" ", strip=True))
        if title_score:
            return title_score
    return None


def _select_current_stats_table(soup):
    """Select the live/full-match stats table, never the half/history tables."""
    if not soup:
        return None

    candidates = soup.select("table.table.table-sm") or soup.find_all("table")
    best_table = None
    best_score = -1
    stat_keys = {
        "Goals",
        "Corners",
        "On Target",
        "Possession",
        "Dangerous Attacks",
        "Attacks",
        "Yellow Card",
        "Red Card",
        "Penalties",
        "Substitutions",
    }
    for position, table in enumerate(candidates):
        score = 0
        for row_index, row in enumerate(table.find_all("tr")):
            cols = row.find_all("td", recursive=False)
            if len(cols) != 3:
                continue
            middle = cols[1].get_text(" ", strip=True)
            middle_norm = _to_ascii(middle).strip().lower().rstrip(".")
            if row_index == 0 or not middle.strip():
                if middle_norm in {"stat", "stats", "estat", "estatistica", "estatisticas"}:
                    score += 100
                elif middle_norm in {"half", "ht", "tempo", "intervalo"}:
                    score -= 100
            if normalize_stat_key(middle) in stat_keys:
                score += 1

        # Keep document order as the tie-breaker; BetsAPI renders current stats first.
        score = (score * 1000) - position
        if score > best_score:
            best_score = score
            best_table = table
    return best_table


def _clean_team_name(name: str) -> str:
    if not name:
        return ""
    text = unquote(str(name)).strip()
    # Normalize common women's markers to a readable suffix/token.
    text = re.sub(r"[\(\[\{]\s*w\s*[\)\]\}]", "(Women)", text, flags=re.IGNORECASE)
    text = re.sub(r"\bwomen\b", "(Women)", text, flags=re.IGNORECASE)
    text = re.sub(r"\(\s*Women\s*\)", "(Women)", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"(?:\s*\(Women\)){2,}", " (Women)", text, flags=re.IGNORECASE)
    return text.strip()


def normalize_stat_key(name: str) -> str:
    raw = _to_ascii(name).strip().lower()
    raw = raw.replace("-", " ").replace("_", " ")
    raw = raw.replace("%", " ")
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
    if raw in ("ao lado", "fora", "fora do alvo"):
        return "Off Target"
    if ("fora" in raw or "lado" in raw) and ("chute" in raw or "shot" in raw):
        return "Off Target"
    if "fora" in raw:
        if "chute" in raw or "shot" in raw:
            return "Off Target"
    if "dangerous" in raw and "attack" in raw:
        return "Dangerous Attacks"
    if "ataques perigosos" in raw or "ataque perigoso" in raw:
        return "Dangerous Attacks"
    if raw in ("cantos", "escanteios", "escanteio"):
        return "Corners"
    if "corners" in raw and "half" in raw:
        return "Corners (Half)"
    if ("cantos" in raw or "escanteio" in raw) and ("tempo" in raw or "half" in raw):
        return "Corners (Half)"
    if raw == "corners" or "corner" in raw:
        return "Corners"
    if raw == "attacks" or "attack" in raw:
        return "Attacks"
    if raw in ("ataques", "ataque"):
        return "Attacks"
    if "possession" in raw:
        return "Possession"
    if "posse" in raw:
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
    if "penaliz" in raw:
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


def _looks_like_event_line(text: str) -> bool:
    if not text:
        return False
    compact = " ".join(str(text).split())
    if len(compact) < 6 or len(compact) > 180:
        return False
    lower = compact.lower()
    if lower.startswith("score after first half") or lower.startswith("score after full time"):
        return True
    if re.match(r"^\d+(?:\+\d+)?['’]", compact):
        return True
    return False


def _event_kind_from_text(text: str) -> str | None:
    if not text:
        return None
    lower = _to_ascii(str(text).lower())
    if "score after first half" in lower:
        return "score_after_ht"
    if "score after full time" in lower:
        return "score_after_ft"
    if "race to" in lower:
        return "race_to"
    if "yellow card" in lower:
        return "yellow_card"
    if "red card" in lower and "yellow/red" not in lower:
        return "red_card"
    if "corner" in lower:
        return "corner"
    if "shot on target" in lower or "on target" in lower or "chute ao alvo" in lower or "chutes ao alvo" in lower:
        return "on_target"
    if "shot off target" in lower or "off target" in lower or "chute fora" in lower or "chutes fora" in lower:
        return "off_target"
    if "goal" in lower:
        return "goal"
    return None


def _extract_event_team(text: str, home_team: str, away_team: str) -> str | None:
    if not text:
        return None
    candidates = []
    candidates.extend(re.findall(r"\(([^()]+)\)", text))
    candidates.extend(part.strip() for part in re.split(r"\s+-\s+", text) if part.strip())
    home_norm = _normalize_team_name(home_team)
    away_norm = _normalize_team_name(away_team)
    for candidate in candidates:
        cand_norm = _normalize_team_name(candidate)
        if not cand_norm:
            continue
        if home_norm and (cand_norm == home_norm or cand_norm in home_norm or home_norm in cand_norm):
            return home_team
        if away_norm and (cand_norm == away_norm or cand_norm in away_norm or away_norm in cand_norm):
            return away_team
    return None


def _build_event_entry(text: str, home_team: str, away_team: str) -> dict | None:
    compact = " ".join(str(text or "").split())
    if not _looks_like_event_line(compact):
        return None
    minute_match = re.match(r"^(\d+(?:\+\d+)?)\s*['’]", compact)
    minute = None
    time_text = ""
    if minute_match:
        time_text = f"{minute_match.group(1)}'"
        minute = parse_minutes(time_text)
    kind = _event_kind_from_text(compact)
    if not kind:
        return None
    return {
        "time_text": time_text,
        "minute": minute,
        "kind": kind,
        "team": _extract_event_team(compact, home_team, away_team),
        "text": compact,
    }


def _extract_events_from_container(container, home_team: str, away_team: str) -> list[dict]:
    if not container:
        return []
    items = []
    seen = set()
    for tag in container.find_all(["tr", "li", "div", "p"], recursive=True):
        text = tag.get_text(" ", strip=True)
        entry = _build_event_entry(text, home_team, away_team)
        if not entry:
            continue
        key = (entry.get("time_text"), entry.get("kind"), entry.get("text"))
        if key in seen:
            continue
        seen.add(key)
        items.append(entry)
    if items:
        return items
    for line in container.get_text("\n", strip=True).splitlines():
        entry = _build_event_entry(line, home_team, away_team)
        if not entry:
            continue
        key = (entry.get("time_text"), entry.get("kind"), entry.get("text"))
        if key in seen:
            continue
        seen.add(key)
        items.append(entry)
    return items


def _extract_match_events(soup, home_team: str, away_team: str) -> list[dict]:
    if not soup:
        return []
    headings = soup.find_all(["h2", "h3", "h4", "h5", "h6", "div", "span", "strong"])
    for tag in headings:
        heading = _normalize_heading(tag.get_text(" ", strip=True))
        if heading not in ("events", "eventos", "match events"):
            continue
        container = None
        parent = tag.parent
        if parent:
            container = parent.find("table") or parent.find("ul") or parent.find("div")
        if not container:
            container = tag.find_next(["table", "ul", "div"])
        events = _extract_events_from_container(container, home_team, away_team)
        if events:
            return events

    fallback_items = []
    seen = set()
    for tag in soup.find_all(["tr", "li", "div", "p"]):
        text = tag.get_text(" ", strip=True)
        entry = _build_event_entry(text, home_team, away_team)
        if not entry:
            continue
        key = (entry.get("time_text"), entry.get("kind"), entry.get("text"))
        if key in seen:
            continue
        seen.add(key)
        fallback_items.append(entry)
    return fallback_items


def _event_timeline_snapshot(events, home_team: str, away_team: str):
    max_minute = None
    home_norm = _normalize_team_name(home_team)
    away_norm = _normalize_team_name(away_team)
    event_stats = {}
    kind_to_stat = {
        "goal": "Goals",
        "corner": "Corners",
        "yellow_card": "Yellow Card",
        "red_card": "Red Card",
    }
    for event in events or []:
        minute = event.get("minute")
        if isinstance(minute, int):
            max_minute = minute if max_minute is None else max(max_minute, minute)
        stat_key = kind_to_stat.get(event.get("kind"))
        if not stat_key:
            continue
        values = event_stats.setdefault(stat_key, {"home": 0, "away": 0, "total": 0})
        team_norm = _normalize_team_name(event.get("team"))
        if team_norm and team_norm == home_norm:
            values["home"] += 1
        elif team_norm and team_norm == away_norm:
            values["away"] += 1
        values["total"] = values["home"] + values["away"]

    goals = event_stats.get("Goals", {"home": 0, "away": 0, "total": 0})
    return {
        "score": f"{goals['home']} x {goals['away']}",
        "minute": max_minute,
        "stats": event_stats,
    }


def _archived_full_time_snapshot(events, home_team: str, away_team: str):
    final_score = None
    for event in events or []:
        if event.get("kind") != "score_after_ft":
            continue
        match = re.search(
            r"score after full time\s*-\s*(\d+)\s*[-:x]\s*(\d+)",
            _to_ascii(event.get("text") or "").lower(),
        )
        if match:
            final_score = (int(match.group(1)), int(match.group(2)))

    if final_score is None:
        return None

    snapshot = _event_timeline_snapshot(events, home_team, away_team)
    snapshot["score"] = f"{final_score[0]} x {final_score[1]}"
    snapshot["minute"] = 90
    snapshot["time_text"] = "FT"
    snapshot["stats"]["Goals"] = {
        "home": final_score[0],
        "away": final_score[1],
        "total": sum(final_score),
    }
    return snapshot


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
    merged_games = {}
    got_success = False

    for base in BASE_URLS:
        try:
            resp = get_with_fallback(session, base)
        except requests.RequestException:
            continue
        last_status = resp.status_code
        if resp.status_code != 200:
            continue
        got_success = True

        soup = BeautifulSoup(resp.text, "html.parser")
        trs = soup.find_all("tr", id=lambda x: x and x.startswith("r_"))
        for tr in trs:
            sport_td = tr.find("td", class_="sport_n")
            league_td = tr.find("td", class_="league_n")
            time_span = tr.find("span", class_="race-time")
            time_td = None
            tr_id = tr.get("id") or ""
            if tr_id:
                time_td = tr.find("td", id=f"{tr_id}T")

            sport_a = sport_td.find("a") if sport_td else None
            league_a = league_td.find("a") if league_td else None
            league_name = league_a.text.strip() if league_a else ""
            time_text = ""
            if time_span:
                time_text = time_span.get_text(strip=True)
            elif time_td:
                time_text = _match_time_from_text(time_td.get_text(" ", strip=True))

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

            game_link_tag = tr.find("a", href=re.compile(r"^(?:/[a-z]+)?/r/\d+"))
            if not game_link_tag:
                continue
            game_href = game_link_tag["href"]
            match_id = re.search(r"/r/(\d+)", game_href)
            if not match_id:
                continue
            game_id = match_id.group(1)
            game_url = base + game_href
            minute_value = parse_minutes(time_text)
            if minute_value is None and is_halftime:
                minute_value = 45
            home_team, away_team = _extract_row_teams(tr, game_url)
            score = _extract_row_score(tr)

            candidate = {
                "game_id": game_id,
                "url": game_url,
                "minute": minute_value,
                "time_text": time_text,
                "league": league_name,
                "home_team": home_team,
                "away_team": away_team,
                "score": score,
            }
            current = merged_games.get(game_id)
            if not current:
                merged_games[game_id] = candidate
                continue
            curr_min = current.get("minute")
            cand_min = candidate.get("minute")
            if isinstance(cand_min, int) and (not isinstance(curr_min, int) or cand_min >= curr_min):
                merged_games[game_id] = candidate

    if not merged_games:
        return [], last_status
    return list(merged_games.values()), 200 if got_success else last_status


def fetch_match_stats(session, url):
    resp = get_with_fallback(session, url)
    if resp.status_code != 200:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")

    league_tag = soup.select_one("ol.breadcrumb li:nth-of-type(2) a")
    league = league_tag.text.strip() if league_tag else ""

    time_text = _extract_match_time_text(soup)

    stat_table = _select_current_stats_table(soup)
    if not stat_table:
        return None

    rows = stat_table.find_all("tr")
    home_team = ""
    away_team = ""
    header_score = None
    if rows:
        first = rows[0].find_all("td")
        if len(first) == 3:
            home_team = _clean_team_name(first[0].get_text(strip=True))
            away_team = _clean_team_name(first[2].get_text(strip=True))
    header_score = _extract_match_score(soup, rows)

    title_home, title_away = _teams_from_title(soup)
    url_home, url_away = _teams_from_url(url)

    def _valid_team(name: str) -> bool:
        return bool(name and name.strip() and not name.strip().isdigit())

    if _valid_team(url_home) and _valid_team(url_away):
        if (
            _valid_team(title_home)
            and _valid_team(title_away)
            and _team_names_match(title_home, url_home)
            and _team_names_match(title_away, url_away)
        ):
            home_team, away_team = title_home, title_away
        else:
            home_team, away_team = url_home, url_away
    elif not _valid_team(home_team) or not _valid_team(away_team):
        if _valid_team(title_home) and _valid_team(title_away):
            home_team, away_team = title_home, title_away

    home_team = _clean_team_name(home_team)
    away_team = _clean_team_name(away_team)
    events = _extract_match_events(soup, home_team, away_team)

    archived_ht_goals = None
    for table in soup.find_all("table"):
        table_rows = table.find_all("tr")
        if not table_rows or "ht" not in _to_ascii(table_rows[0].get_text(" ", strip=True)).lower():
            continue
        half_scores = []
        for table_row in table_rows[1:]:
            values = [parse_int(cell.get_text(" ", strip=True)) for cell in table_row.find_all(["td", "th"])]
            values = [value for value in values if value is not None]
            if values:
                half_scores.append(values[0])
        if len(half_scores) >= 2:
            archived_ht_goals = half_scores[0] + half_scores[1]
            break

    stats = {}
    raw_stats = {}
    score = header_score or "0 x 0"
    best_goals = (0, 0)

    # Only the current/full-match table is authoritative. The page also contains
    # a "Half" snapshot and historical averages with the same stat labels.
    all_rows = rows

    def _is_missing_pair(values):
        if not values:
            return True
        return all(not v or str(v).strip() in ("-", "â€”") for v in values)

    def _pair_total(values):
        if not values or len(values) < 2:
            return None
        a = parse_int(values[0])
        b = parse_int(values[1])
        if a is None or b is None:
            return None
        return a + b

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
            prev_pair = raw_stats.get(key)
            prev_total = _pair_total(prev_pair)
            new_total = _pair_total((home_val, away_val))
            if _is_missing_pair(prev_pair) and not _is_missing_pair((home_val, away_val)):
                raw_stats[key] = (home_val, away_val)
            elif new_total is not None and (prev_total is None or new_total >= prev_total):
                raw_stats[key] = (home_val, away_val)
        else:
            raw_stats[key] = (home_val, away_val)

        home_int = parse_int(home_val)
        away_int = parse_int(away_val)
        if key == "Goals":
            if home_int is not None and away_int is not None:
                if (home_int + away_int) >= sum(best_goals):
                    best_goals = (home_int, away_int)
                    if not header_score:
                        score = f"{home_int} x {away_int}"
        if home_int is not None and away_int is not None:
            new_total = home_int + away_int
            prev = stats.get(key)
            prev_total = prev.get("total") if isinstance(prev, dict) else None
            if prev_total is None or new_total >= prev_total:
                stats[key] = {
                    "home": home_int,
                    "away": away_int,
                    "total": new_total,
                }

    minute_value = parse_minutes(time_text)
    raw_minute = raw_stats.get("Minute")
    allow_raw_minute_override = not is_first_half_extra_time(time_text)
    if minute_value is not None and minute_value <= 45 and not is_second_half(time_text, minute_value):
        allow_raw_minute_override = False
    if raw_minute and allow_raw_minute_override:
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
    score_home, score_away = _parse_score_pair(score)
    if sum(best_goals) > (score_home + score_away):
        score = f"{best_goals[0]} x {best_goals[1]}"
    archived_snapshot = _archived_full_time_snapshot(events, home_team, away_team)
    if archived_snapshot:
        score = archived_snapshot["score"]
        time_text = archived_snapshot["time_text"]
        minute_value = archived_snapshot["minute"]
        for key, values in archived_snapshot["stats"].items():
            stats[key] = values
        stats["Minute"] = {"home": minute_value, "away": minute_value, "total": minute_value}
    else:
        timeline = _event_timeline_snapshot(events, home_team, away_team)
        if isinstance(timeline["minute"], int) and (
            minute_value is None or timeline["minute"] > minute_value
        ):
            minute_value = timeline["minute"]
            time_text = time_text or f"{minute_value}'"
            stats["Minute"] = {"home": minute_value, "away": minute_value, "total": minute_value}
        for key, values in timeline["stats"].items():
            current = stats.get(key)
            if not isinstance(current, dict) or values["total"] >= (current.get("total") or 0):
                stats[key] = values
        timeline_home, timeline_away = _parse_score_pair(timeline["score"])
        score_home, score_away = _parse_score_pair(score)
        if timeline_home + timeline_away >= score_home + score_away:
            score = timeline["score"]
    return {
        "league": league,
        "home_team": home_team,
        "away_team": away_team,
        "score": score,
        "time_text": time_text,
        "minute": minute_value,
        "stats": stats,
        "raw_stats": raw_stats,
        "events": events,
        "archived_ht_goals": archived_ht_goals,
    }


HISTORY_LIMITS = {"h2h": 8, "home": 6, "away": 6}
HISTORY_LABELS = {
    "head to head": "h2h",
    "h2h": "h2h",
    "historico h2h": "h2h",
    "historico de confrontos": "h2h",
    "confronto direto": "h2h",
    "confrontos diretos": "h2h",
    "head-to-head": "h2h",
    "home history": "home",
    "home form": "home",
    "historico em casa": "home",
    "historico do mandante": "home",
    "mandante": "home",
    "away history": "away",
    "away form": "away",
    "historico fora": "away",
    "historico do visitante": "away",
    "visitante": "away",
}


def history_url_from_match(url: str) -> str:
    if not url:
        return ""
    return url.replace("/r/", "/rh/")


def _normalize_heading(text: str) -> str:
    return " ".join(_to_ascii(text).lower().split())


def _find_history_tables(soup, home_team: str = "", away_team: str = ""):
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
                # O título e sua tabela são irmãos em diversas versões
                # do BetsAPI. Buscar no parent devolvia sempre a primeira
                # tabela da página e misturava H2H/Casa/Fora.
                table = tag.find_next("table")
                while table and id(table) in used:
                    table = table.find_next("table")
                if table:
                    tables[key] = table
                    used.add(id(table))
    scored_tables = []
    for table in soup.find_all("table"):
        parsed = _parse_history_table(table)
        if parsed:
            scored_tables.append((table, len(parsed)))
    scored_tables.sort(key=lambda item: item[1], reverse=True)

    home_norm = _normalize_team_name(home_team)
    away_norm = _normalize_team_name(away_team)
    team_aliases = {"home": [home_team], "away": [away_team]}
    # A página pode usar o nome completo na URL (UNAM Pumas) e um nome
    # curto no texto (Pumas). O link oficial do clube permite relacionar as
    # duas formas sem manter uma lista manual de apelidos.
    for link in soup.find_all("a", href=True):
        href = unquote((link.get("href") or "").strip())
        match = re.search(r"/soccer/t/\d+/([^/?#]+)", href, flags=re.IGNORECASE)
        if not match:
            continue
        slug_name = match.group(1).replace("-", " ")
        visible_name = re.sub(r"^\s*\[\d+\]\s*|\s*\[\d+\]\s*$", "", link.get_text(" ", strip=True)).strip()
        if not visible_name:
            continue
        if home_team and _team_names_match(slug_name, home_team):
            team_aliases["home"].append(visible_name)
        if away_team and _team_names_match(slug_name, away_team):
            team_aliases["away"].append(visible_name)
    if home_norm and away_norm and len(tables) < 3:
        team_scores = []
        for table, parsed_count in scored_tables:
            home_rows = 0
            away_rows = 0
            direct_rows = 0
            for row in table.find_all("tr"):
                if _history_match_url(row) is None:
                    continue
                row_text = row.get_text(" ", strip=True)
                has_home = any(_team_name_in_text(alias, row_text) for alias in team_aliases["home"])
                has_away = any(_team_name_in_text(alias, row_text) for alias in team_aliases["away"])
                home_rows += int(has_home)
                away_rows += int(has_away)
                direct_rows += int(has_home and has_away)
            team_scores.append((table, parsed_count, home_rows, away_rows, direct_rows))

        direct_candidates = [item for item in team_scores if item[4] and item[4] / max(item[1], 1) >= 0.5]
        if direct_candidates and "h2h" not in tables:
            direct_table = max(direct_candidates, key=lambda item: (item[4] / max(item[1], 1), item[4]))[0]
            tables["h2h"] = direct_table
            used.add(id(direct_table))

        for key, score_index in (("home", 2), ("away", 3)):
            if key in tables:
                continue
            candidates = [item for item in team_scores if id(item[0]) not in used and item[score_index] > 0]
            if candidates:
                selected = max(candidates, key=lambda item: (item[score_index], item[score_index] / max(item[1], 1)))[0]
                tables[key] = selected
                used.add(id(selected))

    if len(tables) < 3 and not (home_norm and away_norm):
        remaining_keys = [key for key in ("h2h", "home", "away") if key not in tables]
        for key in remaining_keys:
            for table, _ in scored_tables:
                if id(table) in used:
                    continue
                tables[key] = table
                used.add(id(table))
                break
    return tables


def _history_match_url(row) -> str | None:
    if not row:
        return None
    for link in row.find_all("a", href=True):
        href = (link.get("href") or "").strip()
        if not href:
            continue
        parsed_path = urlparse(href).path
        if "/r/" in parsed_path:
            return urljoin(BASE_URLS[0], href)
        if "/rh/" in parsed_path:
            return urljoin(BASE_URLS[0], href.replace("/rh/", "/r/", 1))
    return None


def _parse_history_table(table):
    items = []
    if not table:
        return items
    for row in table.find_all("tr"):
        home_goals, away_goals = _extract_history_score(row)
        if home_goals is None or away_goals is None:
            continue
        item = {
            "home": home_goals,
            "away": away_goals,
            "total": home_goals + away_goals,
        }
        match_url = _history_match_url(row)
        if match_url:
            item["url"] = match_url
        items.append(item)
    return items


def _scheduled_time_from_history_page(soup) -> str | None:
    if not soup:
        return None
    # Datas das partidas anteriores não são o horário do evento atual.
    # Considere apenas o cabeçalho/conteúdo fora das tabelas de histórico.
    header_soup = BeautifulSoup(str(soup), "html.parser")
    for table in header_soup.find_all("table"):
        table.decompose()
    page_text = header_soup.get_text(" ", strip=True)
    scheduled = re.search(r"\b\d{4}/\d{1,2}/\d{1,2}\s+([01]?\d|2[0-3]):([0-5]\d)\b", page_text)
    return f"{int(scheduled.group(1)):02d}:{scheduled.group(2)}" if scheduled else None


def _extract_history_score(row):
    if not row:
        return None, None

    candidates = []

    def collect_candidates(text: str):
        if not text:
            return
        for home_raw, away_raw in re.findall(r"(\d+)\s*(?:-|:|x)\s*(\d+)", text, flags=re.IGNORECASE):
            home_goals = int(home_raw)
            away_goals = int(away_raw)
            # Ignore date-like and obviously invalid score pairs.
            if home_goals > 20 or away_goals > 20:
                continue
            candidates.append((home_goals, away_goals))

    cols = row.find_all("td")
    for col in cols:
        collect_candidates(col.get_text(" ", strip=True))

    if not candidates:
        collect_candidates(row.get_text(" ", strip=True))

    if not candidates:
        return None, None

    # Prefer the last plausible pair because row text often starts with a date.
    return candidates[-1]


def fetch_match_history(session, match_url: str, limits=None, use_fallback: bool = True, timeout: int = 15):
    history_url = history_url_from_match(match_url)
    if not history_url:
        return {"h2h": [], "home": [], "away": [], "scheduled_time": None}
    if use_fallback:
        resp = get_with_fallback(session, history_url)
    else:
        try:
            resp = session.get(history_url, timeout=timeout)
        except Exception:
            return {"h2h": [], "home": [], "away": [], "scheduled_time": None}
    if resp.status_code != 200:
        return {"h2h": [], "home": [], "away": [], "scheduled_time": None}
    soup = BeautifulSoup(resp.text, "html.parser")
    home_team, away_team = _teams_from_url(match_url)
    tables = _find_history_tables(soup, home_team=home_team, away_team=away_team)
    limits = limits or HISTORY_LIMITS
    # A agenda e a página do evento podem divergir apó uma remarcação.
    # A hora exibida no cabeçalho do próprio evento é a referência final.
    scheduled_time = _scheduled_time_from_history_page(soup)
    if not scheduled_time and match_url and match_url != history_url:
        try:
            event_resp = get_with_fallback(session, match_url) if use_fallback else session.get(match_url, timeout=timeout)
            if event_resp.status_code == 200:
                scheduled_time = _scheduled_time_from_history_page(BeautifulSoup(event_resp.text, "html.parser"))
        except Exception:
            scheduled_time = None
    result = {
        "h2h": [],
        "home": [],
        "away": [],
        "scheduled_time": scheduled_time,
    }
    for key in ("h2h", "home", "away"):
        items = _parse_history_table(tables.get(key))
        limit = limits.get(key) if isinstance(limits, dict) else None
        result[key] = items[:limit] if limit else items
    return result


def _history_event_in_first_half(event: dict) -> bool:
    if not isinstance(event, dict):
        return False
    time_text = str(event.get("time_text") or "")
    if time_text.startswith("45+"):
        return True
    minute = event.get("minute")
    return isinstance(minute, int) and minute <= 45


def _history_event_in_second_half(event: dict) -> bool:
    if not isinstance(event, dict):
        return False
    time_text = str(event.get("time_text") or "")
    if time_text.startswith("45+"):
        return False
    minute = event.get("minute")
    return isinstance(minute, int) and minute > 45


def enrich_history_with_ht_goals(session, history_data, limits=None):
    if not isinstance(history_data, dict):
        return history_data
    limits = limits or {"h2h": 8, "home": 6, "away": 6}
    seen_urls = {}
    for key, items in history_data.items():
        if not isinstance(items, list):
            continue
        detail_limit = limits.get(key) if isinstance(limits, dict) else None
        checked = 0
        for item in items:
            if not isinstance(item, dict) or not item.get("url"):
                continue
            if detail_limit is not None and checked >= detail_limit:
                break
            checked += 1
            url = item.get("url")
            if url in seen_urls:
                item.update(seen_urls[url])
                continue
            # Uma partida arquivada indisponível não deve derrubar o
            # histórico inteiro do card.
            try:
                payload = fetch_match_stats(session, url)
            except Exception:
                payload = None
            events = (payload or {}).get("events") or []
            match_stats = (payload or {}).get("stats") or {}
            if not events and int(item.get("total") or 0) > 0:
                archived_ht_goals = (payload or {}).get("archived_ht_goals")
                if archived_ht_goals is None:
                    continue
                total_goals = int(item.get("total") or 0)
                item.update({
                    "goals_ht": int(archived_ht_goals),
                    "goals_2h": max(0, total_goals - int(archived_ht_goals)),
                    "goals_ft_events": total_goals,
                    "goal_before_ht": int(archived_ht_goals) > 0,
                    "goal_after_ht": total_goals - int(archived_ht_goals) > 0,
                    "corners_ht": None,
                    "corners_2h": None,
                    "yellow_cards_ht": None,
                    "yellow_cards_2h": None,
                    "red_cards_ht": None,
                    "red_cards_2h": None,
                    "throw_ins_total": None,
                    "offsides_total": None,
                })
                continue
            ht_events = [
                event
                for event in events
                if isinstance(event, dict) and _history_event_in_first_half(event)
            ]
            sh_events = [
                event
                for event in events
                if isinstance(event, dict) and _history_event_in_second_half(event)
            ]
            ft_events = [event for event in events if isinstance(event, dict)]
            goals_ht = sum(1 for event in ht_events if event.get("kind") == "goal")
            goals_2h = sum(1 for event in sh_events if event.get("kind") == "goal")
            goals_ft = sum(1 for event in ft_events if event.get("kind") == "goal")
            throw_ins_total = None
            offsides_total = None
            for stat_name, stat_values in match_stats.items():
                normalized_stat = _to_ascii(str(stat_name or "").lower()).replace("-", " ")
                is_throw_in = normalized_stat in {"throw in", "throw ins", "throwin", "throwins", "laterais", "lateral"}
                is_offside = normalized_stat in {"offside", "offsides", "impedimento", "impedimentos"}
                if not is_throw_in and not is_offside:
                    continue
                if isinstance(stat_values, dict):
                    raw_total = stat_values.get("total")
                    if raw_total is None:
                        try:
                            raw_total = int(stat_values.get("home") or 0) + int(stat_values.get("away") or 0)
                        except (TypeError, ValueError):
                            raw_total = None
                    try:
                        parsed_total = int(raw_total) if raw_total is not None else None
                    except (TypeError, ValueError):
                        parsed_total = None
                    if is_throw_in:
                        throw_ins_total = parsed_total
                    if is_offside:
                        offsides_total = parsed_total
            detail = {
                "goals_ht": goals_ht,
                "goals_2h": goals_2h,
                "goals_ft_events": goals_ft,
                "goal_before_ht": goals_ht > 0,
                "goal_after_ht": goals_2h > 0,
                "corners_ht": sum(1 for event in ht_events if event.get("kind") == "corner"),
                "corners_2h": sum(1 for event in sh_events if event.get("kind") == "corner"),
                "corners_ft_events": sum(1 for event in ft_events if event.get("kind") == "corner"),
                "yellow_cards_ht": sum(1 for event in ht_events if event.get("kind") == "yellow_card"),
                "yellow_cards_2h": sum(1 for event in sh_events if event.get("kind") == "yellow_card"),
                "yellow_cards_ft_events": sum(1 for event in ft_events if event.get("kind") == "yellow_card"),
                "red_cards_ht": sum(1 for event in ht_events if event.get("kind") == "red_card"),
                "red_cards_2h": sum(1 for event in sh_events if event.get("kind") == "red_card"),
                "red_cards_ft_events": sum(1 for event in ft_events if event.get("kind") == "red_card"),
                "on_target_events_ht": sum(1 for event in ht_events if event.get("kind") == "on_target"),
                "on_target_events_2h": sum(1 for event in sh_events if event.get("kind") == "on_target"),
                "on_target_events_ft": sum(1 for event in ft_events if event.get("kind") == "on_target"),
                "off_target_events_ht": sum(1 for event in ht_events if event.get("kind") == "off_target"),
                "off_target_events_2h": sum(1 for event in sh_events if event.get("kind") == "off_target"),
                "off_target_events_ft": sum(1 for event in ft_events if event.get("kind") == "off_target"),
                "throw_ins_total": throw_ins_total,
                "offsides_total": offsides_total,
            }
            seen_urls[url] = detail
            item.update(detail)
    return history_data


def summarize_history(items):
    total = len(items)
    if total == 0:
        return None
    goals_sum = sum(item.get("total", 0) for item in items)
    over15 = sum(1 for item in items if item.get("total", 0) > 1)
    over25 = sum(1 for item in items if item.get("total", 0) > 2)
    btts = sum(1 for item in items if item.get("home", 0) > 0 and item.get("away", 0) > 0)
    avg_goals = round(goals_sum / total, 1)
    ht_items = [item for item in items if item.get("goals_ht") is not None]
    summary = {
        "count": total,
        "avg_goals": avg_goals,
        "over15": over15,
        "over25": over25,
        "btts": btts,
    }
    if ht_items:
        ht_total = len(ht_items)
        ht_goals_sum = sum(int(item.get("goals_ht") or 0) for item in ht_items)
        ht_goal_games = sum(1 for item in ht_items if int(item.get("goals_ht") or 0) > 0)
        ht_corners_sum = sum(int(item.get("corners_ht") or 0) for item in ht_items)
        sh_goals_sum = sum(int(item.get("goals_2h") or 0) for item in ht_items)
        sh_goal_games = sum(1 for item in ht_items if int(item.get("goals_2h") or 0) > 0)
        sh_corners_sum = sum(int(item.get("corners_2h") or 0) for item in ht_items)
        ht_yellows_sum = sum(int(item.get("yellow_cards_ht") or 0) for item in ht_items)
        sh_yellows_sum = sum(int(item.get("yellow_cards_2h") or 0) for item in ht_items)
        ht_reds_sum = sum(int(item.get("red_cards_ht") or 0) for item in ht_items)
        sh_reds_sum = sum(int(item.get("red_cards_2h") or 0) for item in ht_items)
        ht_on_target_sum = sum(int(item.get("on_target_events_ht") or 0) for item in ht_items)
        sh_on_target_sum = sum(int(item.get("on_target_events_2h") or 0) for item in ht_items)
        ht_off_target_sum = sum(int(item.get("off_target_events_ht") or 0) for item in ht_items)
        sh_off_target_sum = sum(int(item.get("off_target_events_2h") or 0) for item in ht_items)
        summary.update(
            {
                "ht_count": ht_total,
                "avg_ht_goals": round(ht_goals_sum / ht_total, 2),
                "ht_goal_games": ht_goal_games,
                "ht_goal_pct": round((ht_goal_games / ht_total) * 100),
                "avg_2h_goals": round(sh_goals_sum / ht_total, 2),
                "2h_goal_games": sh_goal_games,
                "2h_goal_pct": round((sh_goal_games / ht_total) * 100),
                "avg_ht_corners": round(ht_corners_sum / ht_total, 2),
                "avg_2h_corners": round(sh_corners_sum / ht_total, 2),
                "avg_ht_yellow_cards": round(ht_yellows_sum / ht_total, 2),
                "avg_2h_yellow_cards": round(sh_yellows_sum / ht_total, 2),
                "avg_ht_red_cards": round(ht_reds_sum / ht_total, 2),
                "avg_2h_red_cards": round(sh_reds_sum / ht_total, 2),
                "avg_ht_on_target_events": round(ht_on_target_sum / ht_total, 2),
                "avg_2h_on_target_events": round(sh_on_target_sum / ht_total, 2),
                "avg_ht_off_target_events": round(ht_off_target_sum / ht_total, 2),
                "avg_2h_off_target_events": round(sh_off_target_sum / ht_total, 2),
            }
        )
    return summary


def format_history_summary(label: str, summary):
    if not summary:
        return ""
    text = (
        f"{label} {summary['count']}j | "
        f"Media gols {summary['avg_goals']} | "
        f"O1.5 {summary['over15']}/{summary['count']} | "
        f"O2.5 {summary['over25']}/{summary['count']} | "
        f"BTTS {summary['btts']}/{summary['count']}"
    )
    if summary.get("ht_count"):
        text += (
            f" | Gol 1T {summary['ht_goal_games']}/{summary['ht_count']} "
            f"({summary['ht_goal_pct']}%) | Gol 2T {summary['2h_goal_games']}/{summary['ht_count']} "
            f"({summary['2h_goal_pct']}%) | Media gols 1T/2T "
            f"{summary['avg_ht_goals']}/{summary['avg_2h_goals']}"
        )
        if summary.get("avg_ht_corners") or summary.get("avg_2h_corners"):
            text += f" | Esc 1T/2T {summary['avg_ht_corners']}/{summary['avg_2h_corners']}"
        if summary.get("avg_ht_on_target_events") or summary.get("avg_2h_on_target_events"):
            text += (
                f" | Chutes alvo 1T/2T "
                f"{summary['avg_ht_on_target_events']}/{summary['avg_2h_on_target_events']}"
            )
        if (
            summary.get("avg_ht_yellow_cards")
            or summary.get("avg_2h_yellow_cards")
            or summary.get("avg_ht_red_cards")
            or summary.get("avg_2h_red_cards")
        ):
            text += (
                f" | Cartoes A/V 1T "
                f"{summary['avg_ht_yellow_cards']}/{summary['avg_ht_red_cards']}"
                f" 2T {summary['avg_2h_yellow_cards']}/{summary['avg_2h_red_cards']}"
            )
    return text

def is_second_half(time_text: str, minute: int) -> bool:
    text = (time_text or "").lower()
    text = text.replace("º", "o").replace("ª", "a").replace("Âº", "o")
    if any(x in text for x in SECOND_HALF_TOKENS):
        return True
    return False
