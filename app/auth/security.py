import os
import threading
import time
from collections import defaultdict, deque

from app.security import get_client_ip


class LoginRateLimiter:
    def __init__(self, max_attempts=8, window_seconds=300, block_seconds=900):
        self.max_attempts = max_attempts
        self.window_seconds = window_seconds
        self.block_seconds = block_seconds
        self._attempts = defaultdict(deque)
        self._blocked_until = {}
        self._lock = threading.Lock()
        self._last_cleanup = 0

    def retry_after(self, key, now=None):
        now = time.monotonic() if now is None else now
        with self._lock:
            blocked_until = self._blocked_until.get(key, 0)
            if blocked_until <= now:
                self._blocked_until.pop(key, None)
                return 0
            return max(1, int(blocked_until - now))

    def record_failure(self, key, now=None):
        now = time.monotonic() if now is None else now
        with self._lock:
            if now - self._last_cleanup >= self.window_seconds:
                cutoff = now - self.window_seconds
                stale = [item for item, values in self._attempts.items() if not values or values[-1] <= cutoff]
                for item in stale:
                    self._attempts.pop(item, None)
                expired = [item for item, blocked_until in self._blocked_until.items() if blocked_until <= now]
                for item in expired:
                    self._blocked_until.pop(item, None)
                self._last_cleanup = now
            attempts = self._attempts[key]
            cutoff = now - self.window_seconds
            while attempts and attempts[0] <= cutoff:
                attempts.popleft()
            attempts.append(now)
            if len(attempts) < self.max_attempts:
                return 0
            attempts.clear()
            self._blocked_until[key] = now + self.block_seconds
            return self.block_seconds

    def block(self, key, now=None):
        now = time.monotonic() if now is None else now
        with self._lock:
            self._attempts.pop(key, None)
            self._blocked_until[key] = now + self.block_seconds
        return self.block_seconds

    def reset(self, key):
        with self._lock:
            self._attempts.pop(key, None)
            self._blocked_until.pop(key, None)


def build_login_rate_limiter():
    return LoginRateLimiter(
        max_attempts=int(os.environ.get("LOGIN_MAX_ATTEMPTS", "8")),
        window_seconds=int(os.environ.get("LOGIN_WINDOW_SECONDS", "300")),
        block_seconds=int(os.environ.get("LOGIN_BLOCK_SECONDS", "900")),
    )
