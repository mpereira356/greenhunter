import requests

from .scraper import make_session


def send_message(token: str, chat_id: str, text: str):
    if not token or not chat_id:
        return False, "Token/chat_id ausente."
    session = make_session()
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    try:
        resp = session.post(url, data=payload, timeout=15)
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}"
        return True, "ok"
    except requests.RequestException as exc:
        return False, str(exc)


def send_document(token: str, chat_id: str, file_path: str, caption: str | None = None):
    if not token or not chat_id:
        return False, "Token/chat_id ausente."
    if not file_path:
        return False, "Arquivo nao informado."
    session = make_session()
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
