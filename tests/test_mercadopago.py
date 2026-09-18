import unittest
import hashlib
import hmac
from unittest.mock import patch

from app.services.mercadopago import cancel_subscription, create_subscription, user_id_from_reference, user_reference, valid_webhook_signature


class MercadoPagoReferenceTest(unittest.TestCase):
    def test_round_trip_user_reference(self):
        self.assertEqual(user_id_from_reference(user_reference(42)), 42)

    def test_rejects_unknown_reference(self):
        self.assertIsNone(user_id_from_reference("other-42"))
        self.assertIsNone(user_id_from_reference("greenhunter-user-x"))

    def test_valid_webhook_signature(self):
        secret = "test-secret"
        manifest = "id:abc123;request-id:req-1;ts:1704908010;"
        digest = hmac.new(secret.encode(), manifest.encode(), hashlib.sha256).hexdigest()
        signature = f"ts=1704908010,v1={digest}"
        self.assertTrue(valid_webhook_signature(signature, "req-1", "ABC123", secret))

    def test_rejects_invalid_or_missing_webhook_signature(self):
        self.assertFalse(valid_webhook_signature("ts=1,v1=invalid", "req", "1", "secret"))
        self.assertFalse(valid_webhook_signature(None, "req", "1", "secret"))
        self.assertFalse(valid_webhook_signature("ts=1,v1=invalid", "req", "1", ""))

    @patch("app.services.mercadopago._request")
    def test_cancel_subscription(self, request_mock):
        request_mock.return_value = {"id": "sub-1", "status": "cancelled"}
        self.assertEqual(cancel_subscription("sub-1")["status"], "cancelled")
        request_mock.assert_called_once_with("PUT", "/preapproval/sub-1", json={"status": "cancelled"})

    @patch("app.services.mercadopago._request")
    def test_subscription_accepts_a_different_payer_email(self, request_mock):
        request_mock.return_value = {"status": "pending"}
        user = type("User", (), {"id": 42, "email": "greenhunter@example.com"})()
        create_subscription(user, "https://greenhunter.com.br/premium/retorno", "payer@example.com")
        payload = request_mock.call_args.kwargs["json"]
        self.assertEqual(payload["payer_email"], "payer@example.com")
        self.assertEqual(payload["external_reference"], "greenhunter-user-42")


if __name__ == "__main__":
    unittest.main()
