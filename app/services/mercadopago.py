import hashlib
import hmac
import os
import uuid

import requests


API_ROOT = "https://api.mercadopago.com"


def valid_webhook_signature(x_signature, x_request_id, data_id, secret=None):
    """Validate Mercado Pago's HMAC-SHA256 webhook signature."""
    secret = (secret if secret is not None else os.environ.get("MERCADOPAGO_WEBHOOK_SECRET", "")).strip()
    if not secret or not x_signature:
        return False
    parts = {}
    for item in str(x_signature).split(","):
        key, separator, value = item.strip().partition("=")
        if separator and key and value:
            parts[key] = value
    timestamp = parts.get("ts")
    received_hash = parts.get("v1")
    if not timestamp or not received_hash:
        return False

    manifest_parts = []
    if data_id:
        manifest_parts.append(f"id:{str(data_id).lower()};")
    if x_request_id:
        manifest_parts.append(f"request-id:{x_request_id};")
    manifest_parts.append(f"ts:{timestamp};")
    expected_hash = hmac.new(
        secret.encode("utf-8"),
        "".join(manifest_parts).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected_hash, received_hash)


def _headers(idempotent=False):
    token = os.environ.get("MERCADOPAGO_ACCESS_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Mercado Pago ainda não foi configurado.")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if idempotent:
        headers["X-Idempotency-Key"] = str(uuid.uuid4())
    return headers


def _request(method, path, **kwargs):
    response = requests.request(method, f"{API_ROOT}{path}", headers=_headers(method == "POST"), timeout=20, **kwargs)
    try:
        payload = response.json()
    except ValueError:
        payload = {}
    if response.status_code < 200 or response.status_code >= 300:
        detail = payload.get("message") or payload.get("error") or f"HTTP {response.status_code}"
        raise RuntimeError(f"Mercado Pago: {detail}")
    return payload


def user_reference(user_id):
    return f"greenhunter-user-{int(user_id)}"


def user_id_from_reference(reference):
    prefix = "greenhunter-user-"
    value = str(reference or "")
    if not value.startswith(prefix):
        return None
    try:
        return int(value[len(prefix):])
    except ValueError:
        return None


def create_subscription(user, back_url, payer_email=None):
    payer_email = str(payer_email or user.email or "").strip().lower()
    if not payer_email:
        raise RuntimeError("Cadastre um e-mail antes de assinar o Premium.")
    return _request(
        "POST",
        "/preapproval",
        json={
            "reason": "GreenHunter Pro - mensal",
            "external_reference": user_reference(user.id),
            "payer_email": payer_email,
            "back_url": back_url,
            "auto_recurring": {
                "frequency": 1,
                "frequency_type": "months",
                "transaction_amount": 14.90,
                "currency_id": "BRL",
            },
            "status": "pending",
        },
    )


def get_subscription(subscription_id):
    return _request("GET", f"/preapproval/{subscription_id}")


def cancel_subscription(subscription_id):
    return _request("PUT", f"/preapproval/{subscription_id}", json={"status": "cancelled"})


def get_authorized_payment(payment_id):
    return _request("GET", f"/authorized_payments/{payment_id}")


def get_payment(payment_id):
    return _request("GET", f"/v1/payments/{payment_id}")
