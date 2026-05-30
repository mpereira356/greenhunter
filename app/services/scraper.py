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
_BROWSER_DRIVER = None
_BROWSER_PROFILE_DIR = None
_PROFILE_LAST_CLEANUP_AT = 0.0
_SELENIUM_CACHE_LAST_CLEANUP_AT = 0.0
_PROFILE_COOKIES_LOADED = False
_PROFILE_COOKIES_LAST_ERROR_AT = 0.0
_BROWSER_LAST_LAUNCHED_AT = 0.0
_BROWSER_TARGET_ID = None


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

    with _CF_LOCK:
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
                print("[scraper] Cloudflare detectado. Resolva manualmente no navegador...")
                _send_cf_telegram_alert(url)
                deadline = time.time() + wait_seconds
                while time.time() < deadline:
                    time.sleep(2)
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
                    return None, None, None

            print("[scraper] Challenge liberado. Sessao persistente ativa.")
            return _SimpleResponse(url, 200, html), cookies, user_agent
        except Exception as exc:
            print(f"[scraper] erro no navegador de desafio: {exc}")
            return None, None, None
        finally:
            try:
                ws.close()
            except Exception:
                pass


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

    tables = soup.find_all("table", class_="table table-sm")
    stat_tables = tables if tables else soup.find_all("table")
    if not stat_tables:
        return None

    rows = stat_tables[0].find_all("tr")
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

    stats = {}
    raw_stats = {}
    score = header_score or "0 x 0"
    best_goals = (0, 0)

    all_rows = []
    for table in stat_tables:
        all_rows.extend(table.find_all("tr"))

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
    }


HISTORY_LIMITS = {"h2h": 8, "home": 6, "away": 6}
HISTORY_LABELS = {
    "head to head": "h2h",
    "h2h": "h2h",
    "historico h2h": "h2h",
    "historico de confrontos": "h2h",
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
    if len(tables) < 3:
        scored_tables = []
        for table in soup.find_all("table"):
            parsed = _parse_history_table(table)
            if parsed:
                scored_tables.append((table, len(parsed)))
        scored_tables.sort(key=lambda item: item[1], reverse=True)
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
        return {"h2h": [], "home": [], "away": []}
    if use_fallback:
        resp = get_with_fallback(session, history_url)
    else:
        try:
            resp = session.get(history_url, timeout=timeout)
        except Exception:
            return {"h2h": [], "home": [], "away": []}
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
            payload = fetch_match_stats(session, url)
            events = (payload or {}).get("events") or []
            if not events and int(item.get("total") or 0) > 0:
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
