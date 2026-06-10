import hmac
import secrets
import threading
import time
from collections import defaultdict, deque
from urllib.parse import urljoin, urlparse

from flask import abort, current_app, request, session

import ipaddress


SAFE_METHODS = {"GET", "HEAD", "OPTIONS", "TRACE"}


class RequestRateLimiter:
    def __init__(self):
        self._requests = defaultdict(deque)
        self._lock = threading.Lock()
        self._last_cleanup = 0

    def exceeded(self, key, limit, window_seconds):
        now = time.monotonic()
        cutoff = now - window_seconds
        with self._lock:
            if now - self._last_cleanup >= window_seconds:
                stale = [item for item, values in self._requests.items() if not values or values[-1] <= cutoff]
                for item in stale:
                    self._requests.pop(item, None)
                self._last_cleanup = now
            requests = self._requests[key]
            while requests and requests[0] <= cutoff:
                requests.popleft()
            if len(requests) >= limit:
                return True
            requests.append(now)
            return False


request_rate_limiter = RequestRateLimiter()


def csrf_token():
    token = session.get("_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf_token"] = token
    return token


def get_client_ip():
    remote_ip = request.remote_addr or ""
    trusted_proxies = current_app.config.get("TRUSTED_PROXY_IPS", ())
    if remote_ip in trusted_proxies:
        candidate = request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        if candidate:
            remote_ip = candidate
    try:
        return str(ipaddress.ip_address(remote_ip))
    except ValueError:
        return "unknown"


def is_safe_redirect_target(target):
    if not target:
        return False
    host_url = request.host_url
    resolved = urlparse(urljoin(host_url, target))
    expected = urlparse(host_url)
    return resolved.scheme in {"http", "https"} and resolved.netloc == expected.netloc


def safe_redirect_target(target, fallback):
    return target if is_safe_redirect_target(target) else fallback


def init_security(app):
    app.jinja_env.globals["csrf_token"] = csrf_token

    @app.before_request
    def protect_request():
        allowed_hosts = current_app.config["ALLOWED_HOSTS"]
        if allowed_hosts and request.host.split(":", 1)[0].lower() not in allowed_hosts:
            abort(400, description="Host nao permitido.")

        ip_address = get_client_ip()
        if request_rate_limiter.exceeded(
            ip_address,
            current_app.config["GLOBAL_RATE_LIMIT"],
            current_app.config["GLOBAL_RATE_WINDOW_SECONDS"],
        ):
            abort(429)

        if request.method in SAFE_METHODS:
            return None

        if (
            request.endpoint != "admin.import_database"
            and request.content_length
            and request.content_length > current_app.config["DEFAULT_REQUEST_CONTENT_LENGTH"]
        ):
            abort(413)

        token = request.headers.get("X-CSRF-Token") or request.form.get("_csrf_token")
        expected = session.get("_csrf_token")
        if not token or not expected or not hmac.compare_digest(token, expected):
            abort(400, description="Token CSRF ausente ou invalido.")
        return None

    @app.after_request
    def add_security_headers(response):
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; base-uri 'self'; object-src 'none'; frame-ancestors 'none'; "
            "form-action 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline' "
            "https://cdn.jsdelivr.net https://fonts.googleapis.com; font-src 'self' "
            "https://fonts.gstatic.com; script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
            "connect-src 'self'",
        )
        if current_app.config.get("SESSION_COOKIE_SECURE"):
            response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
        if request.path.startswith(("/auth/", "/admin/", "/settings/")):
            response.headers.setdefault("Cache-Control", "no-store")
        return response
