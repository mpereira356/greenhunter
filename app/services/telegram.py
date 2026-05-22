import requests
import time
import threading

from .scraper import make_session

_SESSION_LOCAL = threading.local()


def _telegram_session():
    session = getattr(_SESSION_LOCAL, "session", None)
    if session is None:
        session = make_session()
        _SESSION_LOCAL.session = session
    return session


def _post_with_retry(session, url, payload):
    resp = session.post(url, data=payload, timeout=15)
    if resp.status_code == 429:
        retry_after = 1
        try:
            body = resp.json()
            retry_after = int((body.get("parameters") or {}).get("retry_after") or 1)
        except Exception:
            retry_after = 1
        time.sleep(min(max(retry_after, 1), 5))
        resp = session.post(url, data=payload, timeout=15)
    if resp.status_code >= 500:
        time.sleep(1)
        resp = session.post(url, data=payload, timeout=15)
    return resp


def send_message(token: str, chat_id: str, text: str):
    if not token or not chat_id:
        return False, "Token/chat_id ausente."
    session = _telegram_session()
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    fallback_payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    try:
        resp = _post_with_retry(session, url, payload)
        if resp.status_code != 200:
            # Fallback for markdown or formatting errors in dynamic text.
            resp = _post_with_retry(session, url, fallback_payload)
        if resp.status_code != 200:
            description = ""
            try:
                body = resp.json()
                description = (body.get("description") or "").strip()
            except Exception:
                description = ""
            if "chat not found" in description.lower():
                return (
                    False,
                    "Chat nao encontrado. Adicione o bot ao grupo e envie uma mensagem no grupo antes de testar.",
                )
            print(f"[telegram] falha ao enviar (HTTP {resp.status_code}): {resp.text[:180]}")
            return False, f"HTTP {resp.status_code}: {resp.text[:180]}"
        message_id = None
        try:
            body = resp.json()
            message_id = ((body.get("result") or {}).get("message_id"))
        except Exception:
            message_id = None
        return True, "ok", message_id
    except requests.RequestException as exc:
        return False, str(exc)


def edit_message_text(token: str, chat_id: str, message_id: int, text: str):
    if not token or not chat_id or not message_id:
        return False, "Token/chat_id/message_id ausente."
    session = _telegram_session()
    url = f"https://api.telegram.org/bot{token}/editMessageText"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    fallback_payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    try:
        resp = _post_with_retry(session, url, payload)
        if resp.status_code != 200:
            resp = _post_with_retry(session, url, fallback_payload)
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}: {resp.text[:180]}"
        return True, "ok"
    except requests.RequestException as exc:
        return False, str(exc)


def send_document(token: str, chat_id: str, file_path: str, caption: str | None = None):
    if not token or not chat_id:
        return False, "Token/chat_id ausente."
    if not file_path:
        return False, "Arquivo nao informado."
    session = _telegram_session()
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption
    try:
        with open(file_path, "rb") as handle:
            files = {"document": handle}
            resp = session.post(url, data=data, files=files, timeout=30)
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}"
        return True, "ok"
    except (requests.RequestException, OSError) as exc:
        return False, str(exc)
