import os
import tempfile
import unittest

os.environ["DISABLE_WORKER"] = "1"
os.environ["DATABASE_URL"] = f"sqlite:///{tempfile.gettempdir()}/greenhunter_security_tests.db"
os.environ["SECRET_KEY"] = "test-secret-key-with-enough-length"

from app import create_app
from app.main.routes import _safe_match_url
from app.security import safe_redirect_target


class SecurityIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = create_app()
        cls.app.config.update(TESTING=True)

    def setUp(self):
        self.client = self.app.test_client()

    def _csrf_token(self):
        self.client.get("/auth/login")
        with self.client.session_transaction() as session:
            return session["_csrf_token"]

    def test_post_without_csrf_is_rejected(self):
        response = self.client.post("/auth/login", data={"username": "x", "password": "x"})
        self.assertEqual(response.status_code, 400)

    def test_post_with_csrf_reaches_route(self):
        token = self._csrf_token()
        response = self.client.post(
            "/auth/login",
            data={"username": "x", "password": "x", "_csrf_token": token},
        )
        self.assertEqual(response.status_code, 200)

    def test_security_headers_are_present(self):
        response = self.client.get("/auth/login")
        self.assertEqual(response.headers["X-Frame-Options"], "DENY")
        self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
        self.assertIn("frame-ancestors 'none'", response.headers["Content-Security-Policy"])
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    def test_external_redirect_target_is_rejected(self):
        with self.app.test_request_context("/", base_url="http://greenhunter.local"):
            self.assertEqual(
                safe_redirect_target("https://attacker.example/path", "/"),
                "/",
            )

    def test_untrusted_match_url_is_rejected(self):
        self.assertEqual(_safe_match_url("javascript:alert(1)"), "#")
        self.assertEqual(_safe_match_url("https://attacker.example/game"), "#")
        self.assertEqual(_safe_match_url("https://betsapi.com/game"), "https://betsapi.com/game")


if __name__ == "__main__":
    unittest.main()
