import unittest

from flask import Flask

from app.auth.security import LoginRateLimiter, get_client_ip


class LoginRateLimiterTest(unittest.TestCase):
    def test_blocks_after_limit_and_does_not_extend_block_on_checks(self):
        limiter = LoginRateLimiter(max_attempts=3, window_seconds=60, block_seconds=120)

        self.assertEqual(limiter.record_failure("ip", now=1), 0)
        self.assertEqual(limiter.record_failure("ip", now=2), 0)
        self.assertEqual(limiter.record_failure("ip", now=3), 120)
        self.assertEqual(limiter.retry_after("ip", now=4), 119)
        self.assertEqual(limiter.retry_after("ip", now=124), 0)

    def test_old_attempts_leave_window(self):
        limiter = LoginRateLimiter(max_attempts=2, window_seconds=10, block_seconds=60)

        self.assertEqual(limiter.record_failure("ip", now=1), 0)
        self.assertEqual(limiter.record_failure("ip", now=12), 0)

    def test_success_reset_clears_failures_and_block(self):
        limiter = LoginRateLimiter(max_attempts=1, window_seconds=60, block_seconds=120)
        limiter.record_failure("ip", now=1)

        limiter.reset("ip")

        self.assertEqual(limiter.retry_after("ip", now=2), 0)


class ClientIpTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["TRUSTED_PROXY_IPS"] = {"127.0.0.1"}

    def test_uses_forwarded_ip_only_from_trusted_proxy(self):
        with self.app.test_request_context(
            "/", environ_base={"REMOTE_ADDR": "127.0.0.1"}, headers={"X-Forwarded-For": "203.0.113.7"}
        ):
            self.assertEqual(get_client_ip(), "203.0.113.7")

    def test_ignores_spoofed_forwarded_ip_from_direct_client(self):
        with self.app.test_request_context(
            "/", environ_base={"REMOTE_ADDR": "198.51.100.9"}, headers={"X-Forwarded-For": "203.0.113.7"}
        ):
            self.assertEqual(get_client_ip(), "198.51.100.9")

    def test_invalid_address_is_not_used_as_rate_limit_key(self):
        with self.app.test_request_context("/", environ_base={"REMOTE_ADDR": "not-an-ip"}):
            self.assertEqual(get_client_ip(), "unknown")


if __name__ == "__main__":
    unittest.main()
