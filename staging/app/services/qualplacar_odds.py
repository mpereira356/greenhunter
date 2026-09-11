import json
import os
import re
import threading
import time
import unicodedata
from datetime import datetime
from difflib import SequenceMatcher

import requests


QUALPLACAR_ODDS_URL = os.environ.get(
    "QUALPLACAR_ODDS_URL", "https://prod.qualplacar.top/v3/jogos/listar"
)
QUALPLACAR_ODDS_TTL_SECONDS = int(os.environ.get("QUALPLACAR_ODDS_TTL_SECONDS", "900"))
QUALPLACAR_ODDS_TIMEOUT_SECONDS = float(os.environ.get("QUALPLACAR_ODDS_TIMEOUT_SECONDS", "5"))
QUALPLACAR_DETAIL_URL = os.environ.get(
    "QUALPLACAR_DETAIL_URL", "https://prod.qualplacar.top/v3/jogos/detalhes-jogo/{game_id}"
)
QUALPLACAR_DETAIL_TTL_SECONDS = int(os.environ.get("QUALPLACAR_DETAIL_TTL_SECONDS", "300"))
QUALPLACAR_ODDS_CACHE_DIR = os.environ.get(
    "MATCHDAY_CACHE_DIR", os.path.join("data", "matchday_cache")
)

_cache_lock = threading.Lock()
_CLUB_QUALIFIERS = {"afc", "cf", "city", "club", "fc", "fk", "sc", "sk", "united"}
_AMBIGUOUS_SINGLE_TOKENS = _CLUB_QUALIFIERS | {"athletic", "deportivo", "real", "sporting"}


def _cache_path(day: str) -> str:
    return os.path.join(QUALPLACAR_ODDS_CACHE_DIR, f"qualplacar-odds-{day}.json")


def _detail_cache_path(game_id: str) -> str:
    safe_id = re.sub(r"[^0-9A-Za-z_-]", "", str(game_id))
    return os.path.join(QUALPLACAR_ODDS_CACHE_DIR, f"qualplacar-bet365-{safe_id}.json")


def _normalized_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char)).casefold()
    tokens = re.sub(r"[^a-z0-9]+", " ", text).split()
    tokens = ["united" if token == "utd" else token for token in tokens]
    return " ".join(tokens)


def _valid_odd(value):
    try:
        odd = float(value)
    except (TypeError, ValueError):
        return None
    return round(odd, 2) if 1.0 <= odd <= 1000 else None


def _load_cache(day: str, allow_stale: bool = False) -> list[dict] | None:
    path = _cache_path(day)
    try:
        if not allow_stale and time.time() - os.stat(path).st_mtime > QUALPLACAR_ODDS_TTL_SECONDS:
            return None
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, TypeError, ValueError):
        return None
    if not isinstance(payload, dict) or payload.get("day") != day or payload.get("schema") != 2:
        return None
    games = payload.get("games")
    if not isinstance(games, list):
        return None
    # Caches anteriores à integração dos mercados não guardavam o id usado
    # pelo endpoint de detalhes. Recarregue-os uma única vez.
    if games and not any(game.get("id") for game in games if isinstance(game, dict)):
        return None
    return games


def _save_cache(day: str, games: list[dict]) -> None:
    if not games:
        return
    try:
        os.makedirs(QUALPLACAR_ODDS_CACHE_DIR, exist_ok=True)
        path = _cache_path(day)
        temporary = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump({"schema": 2, "day": day, "games": games}, handle, ensure_ascii=False, separators=(",", ":"))
        os.replace(temporary, path)
    except OSError:
        try:
            os.unlink(temporary)
        except (OSError, UnboundLocalError):
            pass


def _fetch_odds(day: str) -> list[dict]:
    parsed_day = datetime.strptime(day, "%Y-%m-%d")
    response = requests.post(
        QUALPLACAR_ODDS_URL,
        json={
            "data": parsed_day.strftime("%d/%m/%Y"),
            "id_filtro": None,
            "texto_pesquisa": "",
            "filtro_personalizado": None,
        },
        headers={"Accept": "application/json", "User-Agent": "GreenHunter/1.0"},
        timeout=QUALPLACAR_ODDS_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    body = response.json()
    if not body.get("ok") or not isinstance(body.get("result"), list):
        return []
    games = []
    for league in body["result"]:
        if not isinstance(league, dict):
            continue
        for game in league.get("jogos") or []:
            if not isinstance(game, dict):
                continue
            home_odd = _valid_odd(game.get("odd_mandante"))
            draw_odd = _valid_odd(game.get("odd_empate"))
            away_odd = _valid_odd(game.get("odd_visitante"))
            game_id = str(game.get("id") or "")
            home_name = str(game.get("mandante") or "")
            away_name = str(game.get("visitante") or "")
            # Mesmo sem o mercado 1x2, preserve a partida: o detalhe pode ter
            # gols, cantos ou ambos marcam em uma ou mais casas.
            if not game_id or not home_name or not away_name:
                continue
            games.append({
                "id": game_id,
                "league": str(league.get("liga") or ""),
                "home": home_name,
                "away": away_name,
                "time": str(game.get("data_descricao") or ""),
                "home_odd": home_odd,
                "draw_odd": draw_odd,
                "away_odd": away_odd,
            })
    return games


def get_qualplacar_odds(day: str) -> list[dict]:
    if os.environ.get("QUALPLACAR_ODDS_ENABLED", "1").strip().casefold() in {"0", "false", "no"}:
        return []
    try:
        datetime.strptime(day, "%Y-%m-%d")
    except (TypeError, ValueError):
        return []
    cached = _load_cache(day)
    if cached is not None:
        return cached
    with _cache_lock:
        cached = _load_cache(day)
        if cached is not None:
            return cached
        try:
            games = _fetch_odds(day)
        except (requests.RequestException, TypeError, ValueError):
            return _load_cache(day, allow_stale=True) or []
        _save_cache(day, games)
        return games


def _similarity(left: str, right: str) -> float:
    left_normalized = _normalized_name(left)
    right_normalized = _normalized_name(right)
    if not left_normalized or not right_normalized:
        return 0.0
    if left_normalized == right_normalized:
        return 1.0
    left_tokens = left_normalized.split()
    right_tokens = right_normalized.split()
    shorter, longer = (
        (left_tokens, right_tokens)
        if len(left_tokens) <= len(right_tokens)
        else (right_tokens, left_tokens)
    )
    # Fontes diferentes frequentemente acrescentam apenas a designação do
    # clube (Norwich/Norwich City, Birmingham/Birmingham City, Santos/Santos
    # FC). Aceite essa diferença sem ignorar palavras realmente distintivas.
    if shorter and longer[:len(shorter)] == shorter:
        extra_tokens = longer[len(shorter):]
        if extra_tokens and all(token in _CLUB_QUALIFIERS for token in extra_tokens):
            return 0.97
    return SequenceMatcher(None, left_normalized, right_normalized).ratio()


def _normalized_clock(value: str) -> str:
    matches = re.findall(r"(?<!\d)([01]?\d|2[0-3]):([0-5]\d)(?!\d)", str(value or ""))
    return f"{int(matches[-1][0]):02d}:{matches[-1][1]}" if matches else str(value or "").strip()


def _single_token_alias(left: str, right: str) -> bool:
    left_tokens = _normalized_name(left).split()
    right_tokens = _normalized_name(right).split()
    shorter, longer = (left_tokens, right_tokens) if len(left_tokens) <= len(right_tokens) else (right_tokens, left_tokens)
    return (
        len(shorter) == 1
        and len(shorter[0]) >= 5
        and shorter[0] not in _AMBIGUOUS_SINGLE_TOKENS
        and shorter[0] in longer
    )


def _find_odds(match: dict, candidates: list[dict]) -> dict | None:
    clock = _normalized_clock(match.get("time"))
    ranked = []
    for candidate in candidates:
        # Horário é um filtro exato e muito mais barato que comparar nomes.
        # Aplicá-lo primeiro evita milhares de SequenceMatcher desnecessários
        # ao abrir uma agenda grande.
        if clock != _normalized_clock(candidate.get("time")):
            continue
        home_score = _similarity(match.get("home_team"), candidate.get("home"))
        away_score = _similarity(match.get("away_team"), candidate.get("away"))
        if _single_token_alias(match.get("home_team"), candidate.get("home")):
            home_score = max(home_score, .92)
        if _single_token_alias(match.get("away_team"), candidate.get("away")):
            away_score = max(away_score, .92)
        if min(home_score, away_score) < 0.64:
            continue
        team_score = (home_score + away_score) / 2
        if team_score < 0.78:
            continue
        league_score = _similarity(match.get("league"), candidate.get("league"))
        ranked.append((team_score + league_score * 0.08, team_score, candidate))
    if not ranked:
        return None
    ranked.sort(key=lambda item: item[0], reverse=True)
    # Em caso de dois candidatos quase iguais, é mais seguro não mostrar odd.
    if len(ranked) > 1 and ranked[0][0] - ranked[1][0] < 0.025:
        return None
    return ranked[0][2]


def attach_qualplacar_odds(matches: list[dict], day: str) -> int:
    candidates = get_qualplacar_odds(day)
    # A página pode ter centenas de partidas. Indexar uma vez por HH:MM
    # transforma o cruzamento completo em pequenos grupos do mesmo horário.
    candidates_by_clock = {}
    for candidate in candidates:
        candidates_by_clock.setdefault(_normalized_clock(candidate.get("time")), []).append(candidate)
    attached = 0
    for match in matches:
        match.pop("home_win_odd", None)
        match.pop("away_win_odd", None)
        match.pop("qualplacar_id", None)
        compatible = candidates_by_clock.get(_normalized_clock(match.get("time")), ())
        candidate = _find_odds(match, compatible)
        if candidate is None:
            continue
        if candidate.get("home_odd") is not None:
            match["home_win_odd"] = candidate["home_odd"]
        if candidate.get("away_odd") is not None:
            match["away_win_odd"] = candidate["away_odd"]
        if candidate.get("id"):
            match["qualplacar_id"] = candidate["id"]
        if candidate.get("home_odd") is not None or candidate.get("away_odd") is not None:
            attached += 1
    return attached


def _load_detail_cache(game_id: str) -> dict | None:
    path = _detail_cache_path(game_id)
    try:
        if time.time() - os.stat(path).st_mtime > QUALPLACAR_DETAIL_TTL_SECONDS:
            return None
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, TypeError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _fetch_detail(game_id: str) -> dict:
    response = requests.get(
        QUALPLACAR_DETAIL_URL.format(game_id=game_id),
        headers={"Accept": "application/json", "User-Agent": "GreenHunter/1.0"},
        timeout=max(QUALPLACAR_ODDS_TIMEOUT_SECONDS, 8),
    )
    response.raise_for_status()
    body = response.json()
    return body if isinstance(body, dict) and body.get("ok") else {}


def _get_detail(game_id: str) -> dict:
    cached = _load_detail_cache(game_id)
    if cached is not None:
        return cached
    with _cache_lock:
        cached = _load_detail_cache(game_id)
        if cached is not None:
            return cached
        try:
            detail = _fetch_detail(game_id)
        except (requests.RequestException, TypeError, ValueError):
            return {}
        if detail:
            try:
                os.makedirs(QUALPLACAR_ODDS_CACHE_DIR, exist_ok=True)
                path = _detail_cache_path(game_id)
                temporary = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
                with open(temporary, "w", encoding="utf-8") as handle:
                    json.dump(detail, handle, ensure_ascii=False, separators=(",", ":"))
                os.replace(temporary, path)
            except OSError:
                pass
        return detail


def _bet365_markets(detail: dict) -> dict[int, list[dict]]:
    result = {}
    markets = (((detail.get("result") or {}).get("analise_pre_jogo") or {}).get("odds") or {}).get("data") or []
    for market in markets:
        try:
            market_id = int(market.get("id"))
        except (TypeError, ValueError, AttributeError):
            continue
        bookmakers = ((market.get("bookmaker") or {}).get("data") or []) if isinstance(market, dict) else []
        bet365 = next((item for item in bookmakers if str(item.get("name") or "").casefold() == "bet365"), None)
        if bet365:
            result[market_id] = ((bet365.get("odds") or {}).get("data") or [])
    return result


def _bookmaker_markets(detail: dict) -> dict[int, list[dict]]:
    result = {}
    markets = (((detail.get("result") or {}).get("analise_pre_jogo") or {}).get("odds") or {}).get("data") or []
    for market in markets:
        try:
            market_id = int(market.get("id"))
        except (TypeError, ValueError, AttributeError):
            continue
        bookmakers = ((market.get("bookmaker") or {}).get("data") or []) if isinstance(market, dict) else []
        result[market_id] = [
            {"name": str(bookmaker.get("name") or "").strip(), "odds": ((bookmaker.get("odds") or {}).get("data") or [])}
            for bookmaker in bookmakers if str(bookmaker.get("name") or "").strip()
        ]
    return result


def _selection_market(item: dict):
    key = str(item.get("marketKey") or item.get("generatedMarket") or "").casefold()
    direction = "under" if item.get("direction") == "under" or "under" in key else "over"
    try:
        line = float(item.get("selectedLine"))
    except (TypeError, ValueError):
        line = None
    if key in {"btts", "btts_yes"}:
        return 14, "yes", None
    if key in {"over15", "under15", "over25", "under25", "under35"} or key.startswith("goals_total"):
        return 80, direction, line
    if key.startswith("team_goals") and key.endswith("_home"):
        return 20, direction, line
    if key.startswith("team_goals") and key.endswith("_away"):
        return 21, direction, line
    if key.startswith("goals_1h") and key.endswith(("total", "1h")):
        return 28, direction, line
    if key.startswith("corners") and "1h" not in key and (key.endswith("_total") or key == "corners_avg"):
        # O feed expressa a linha europeia de cantos como o inteiro seguinte:
        # "9" corresponde ao bilhete "mais de 8,5".
        return 67, direction, int(line + .5) if line is not None else None
    return None, None, None


def bet365_selection_odds(match: dict, day: str, selections: list[dict]) -> dict[str, float]:
    """Return only exact, supported Bet365 prices, keyed by client selection id."""
    candidate = _find_odds(match, get_qualplacar_odds(day))
    if not candidate or not candidate.get("id"):
        return {}
    markets = _bet365_markets(_get_detail(candidate["id"]))
    found = {}
    for item in selections[:30]:
        market_id, label, total = _selection_market(item)
        if market_id is None:
            continue
        for odd in markets.get(market_id, []):
            if str(odd.get("label") or "").casefold() != label:
                continue
            if total is not None:
                try:
                    if abs(float(odd.get("total")) - float(total)) > .001:
                        continue
                except (TypeError, ValueError):
                    continue
            value = _valid_odd(odd.get("value"))
            selection_id = str(item.get("id") or "")
            if selection_id and value is not None:
                found[selection_id] = value
            break
    return found


def bookmaker_selection_odds(match: dict, day: str, selections: list[dict]) -> dict[str, list[dict]]:
    """Return comparable prices from every bookmaker for each exact selection."""
    candidate = _find_odds(match, get_qualplacar_odds(day))
    if not candidate or not candidate.get("id"):
        return {}
    markets = _bookmaker_markets(_get_detail(candidate["id"]))
    found = {}
    for item in selections[:30]:
        market_id, label, total = _selection_market(item)
        selection_id = str(item.get("id") or "")
        if market_id is None or not selection_id:
            continue
        prices = []
        for bookmaker in markets.get(market_id, []):
            for odd in bookmaker["odds"]:
                if str(odd.get("label") or "").casefold() != label:
                    continue
                if total is not None:
                    try:
                        if abs(float(odd.get("total")) - float(total)) > .001:
                            continue
                    except (TypeError, ValueError):
                        continue
                value = _valid_odd(odd.get("value"))
                if value is not None:
                    prices.append({"name": bookmaker["name"][:60], "odd": value})
                break
        if prices:
            found[selection_id] = sorted(prices, key=lambda entry: (-entry["odd"], entry["name"].casefold()))
    return found
