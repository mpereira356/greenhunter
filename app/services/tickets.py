import json
import re
import unicodedata
from ..extensions import db
from ..models import LiveGameState, SavedTicket
from ..utils.time import now_sp
from .telegram import send_message


def _number(value):
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _score(value):
    found = re.search(r"(\d+)\s*[-x:]\s*(\d+)", str(value or ""), re.I)
    return (int(found.group(1)), int(found.group(2))) if found else (None, None)


def _stat(stats, names, side):
    for name in names:
        row = stats.get(name)
        if not isinstance(row, dict):
            continue
        value = _number(row.get(side))
        if value is not None:
            return value
        if side == "total":
            value = _number(row.get("total"))
            if value is not None:
                return value
            home, away = _number(row.get("home")), _number(row.get("away"))
            if home is not None and away is not None:
                return home + away
    return None


def _has_first_half_goal(state):
    try:
        events = json.loads(state.events_json or "[]")
    except (TypeError, ValueError):
        return None
    found_timed_event = False
    for event in events if isinstance(events, list) else []:
        text = " ".join(str(value) for value in event.values()) if isinstance(event, dict) else str(event)
        minute = re.search(r"\b(\d{1,3})(?:\+\d+)?['’]?\b", text)
        if not minute:
            continue
        found_timed_event = True
        if int(minute.group(1)) <= 45 and any(word in text.casefold() for word in ("goal", "gol")):
            return True
    return False if found_timed_event else None


def _team_key(value):
    normalized = unicodedata.normalize("NFKD", str(value or "").casefold())
    return re.sub(r"[^a-z0-9]+", "", "".join(char for char in normalized if not unicodedata.combining(char)))


def _period_event_value(leg, state):
    matched = re.fullmatch(r"(goals|corners|cards|shots|shots_on_target)(?:_under)?_(1h|2h)(?:_(home|away))?", (leg.market_key or "").casefold())
    if not matched:
        return None
    market, period, side = matched.groups()
    try:
        events = json.loads(state.events_json or "[]")
    except (TypeError, ValueError):
        return None
    if not isinstance(events, list) or not events:
        return None
    kinds = {
        "goals": {"goal"},
        "corners": {"corner"},
        "cards": {"yellow_card", "red_card"},
        "shots": {"on_target", "off_target"},
        "shots_on_target": {"on_target"},
    }[market]
    target_team = _team_key(state.home_team if side == "home" else state.away_team if side == "away" else "")
    value = 0
    saw_timed_event = False
    for event in events:
        if not isinstance(event, dict) or event.get("kind") not in kinds or not isinstance(event.get("minute"), int):
            continue
        minute = int(event["minute"])
        if (period == "1h" and minute > 45) or (period == "2h" and minute <= 45):
            continue
        saw_timed_event = True
        if target_team and _team_key(event.get("team")) != target_team:
            continue
        value += 1
    return float(value) if saw_timed_event or events else None


def _leg_value(leg, state, stats):
    home, away = _score(state.score)
    if leg.market_key in {"over05", "over15", "over25", "under15", "under25", "under35"}:
        return None if home is None else home + away
    if leg.market_key in {"team_goals_home", "team_goals_under_home"}:
        return home
    if leg.market_key in {"team_goals_away", "team_goals_under_away"}:
        return away
    if leg.market_key == "goal_ht":
        value = _has_first_half_goal(state)
        return None if value is None else (1 if value else 0)
    key = leg.market_key.casefold()
    if re.fullmatch(r"(goals|corners|cards|shots|shots_on_target)_(1h|2h)(?:_(home|away))?", key):
        return _period_event_value(leg, state)
    if key == "corners_10_over1":
        try:
            events = json.loads(state.events_json or "[]")
        except (TypeError, ValueError):
            events = []
        if not isinstance(events, list) or not events:
            return None
        return float(sum(
            1 for event in events
            if isinstance(event, dict)
            and event.get("kind") == "corner"
            and isinstance(event.get("minute"), int)
            and int(event.get("minute")) <= 10
        ))
    if "corner" in key:
        return _stat(stats, ("Corners", "Corner Kicks", "Escanteios"), leg.target_side)
    if "card" in key:
        direct = _stat(stats, ("Cards", "Total Cards", "Cartões"), leg.target_side)
        if direct is not None:
            return direct
        yellow = _stat(stats, ("Yellow Card", "Yellow Cards", "Cartões Amarelos"), leg.target_side)
        red = _stat(stats, ("Red Card", "Red Cards", "Cartões Vermelhos"), leg.target_side)
        return None if yellow is None and red is None else (yellow or 0) + (red or 0)
    if "shots_on_target" in key:
        return _stat(stats, ("On Target", "Shots on Target", "Chutes ao Gol"), leg.target_side)
    if "shots" in key:
        direct = _stat(stats, ("Shots", "Total Shots", "Finalizações"), leg.target_side)
        if direct is not None:
            return direct
        on_target = _stat(stats, ("On Target", "Shots on Target", "Chutes ao Gol"), leg.target_side)
        off_target = _stat(stats, ("Off Target", "Shots off Target", "Chutes para Fora"), leg.target_side)
        return None if on_target is None and off_target is None else (on_target or 0) + (off_target or 0)
    if "foul" in key:
        return _stat(stats, ("Fouls", "Fouls Committed", "Faltas"), leg.target_side)
    if "offside" in key:
        return _stat(stats, ("Offsides", "Offside", "Impedimentos"), leg.target_side)
    return None


def _is_finished(state):
    text = str(state.time_text or "").strip().casefold()
    return text in {"ft", "finished", "ended", "encerrado", "ap"} or "full time" in text


def _is_under_market(leg):
    return (leg.market_key or "").casefold().startswith("under") or "_under_" in (leg.market_key or "").casefold()


def _early_corner_window_closed(state):
    if _is_finished(state):
        return True
    try:
        return int(state.minute or 0) > 10
    except (TypeError, ValueError):
        return False


def _first_half_closed(state):
    if _is_finished(state) or bool(getattr(state, "second_half_started", False)) or getattr(state, "ht_seen_at", None):
        return True
    text = str(state.time_text or "").strip().casefold()
    return text in {"ht", "half time", "intervalo"} or "2nd half" in text or "2º tempo" in text


def _leg_match_closed(leg, state):
    if _is_finished(state):
        return True
    # Alguns jogos arquivados permanecem com o ultimo marcador em 90' sem
    # que o provedor grave a sigla FT. No dia seguinte isso ja e terminal.
    try:
        reached_ninety = int(state.minute or 0) >= 90
    except (TypeError, ValueError):
        reached_ninety = False
    return reached_ninety and str(leg.game_day or "") < now_sp().strftime("%Y-%m-%d")


def _result_description(leg):
    if leg.result_value is None:
        return ""
    value = float(leg.result_value)
    shown = str(int(value)) if value.is_integer() else str(value).replace(".", ",")
    key = (leg.market_key or "").casefold()
    if key in {"over05", "over15", "over25", "under15", "under25", "under35", "goal_ht"} or key.startswith("goals_") or key.startswith("team_goals"):
        noun = "gol" if value == 1 else "gols"
    elif "corner" in key:
        noun = "escanteio" if value == 1 else "escanteios"
    elif "card" in key:
        noun = "cartão" if value == 1 else "cartões"
    elif "shots_on_target" in key:
        noun = "chute ao gol" if value == 1 else "chutes ao gol"
    elif "shots" in key:
        noun = "finalização" if value == 1 else "finalizações"
    elif "foul" in key:
        noun = "falta" if value == 1 else "faltas"
    elif "offside" in key:
        noun = "impedimento" if value == 1 else "impedimentos"
    else:
        noun = "apurado"
    return f"{shown} {noun}"


def _archived_refresh_due(legs, state, now):
    """Reconsulta jogos que já deveriam ter terminado e pararam de aparecer ao vivo."""
    if state is not None and _is_finished(state):
        return False
    sample = legs[0]
    day = str(sample.game_day or "")
    clock = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*", str(sample.game_time or ""))
    today = now.strftime("%Y-%m-%d")
    if day > today or not day:
        return False
    if day == today:
        if not clock:
            return False
        scheduled_minute = int(clock.group(1)) * 60 + int(clock.group(2))
        if now.hour * 60 + now.minute < scheduled_minute + 180:
            return False
    latest_check = max((leg.checked_at for leg in legs if leg.checked_at), default=None)
    if latest_check and (now.replace(tzinfo=None) - latest_check.replace(tzinfo=None)).total_seconds() < 1800:
        return False
    return True


def _refresh_archived_states(tickets, session, maximum=3):
    from .scraper import fetch_match_stats, make_session

    now = now_sp()
    legs_by_game = {}
    for ticket in tickets:
        for leg in ticket.legs:
            if leg.status == "pending":
                legs_by_game.setdefault(str(leg.game_id), []).append(leg)
    own_session = session is None
    session = session or make_session()
    refreshed = 0
    try:
        for game_id, legs in legs_by_game.items():
            state = LiveGameState.query.filter_by(game_id=game_id).first()
            state_stats = {}
            if state is not None:
                try:
                    state_stats = json.loads(state.stats_json or "{}")
                except (TypeError, ValueError):
                    state_stats = {}
            state_has_result = (
                any(key != "Minute" for key in state_stats) or _is_finished(state)
            ) if state is not None else False
            if str(legs[0].game_day or "") < now.strftime("%Y-%m-%d") and state is not None and not state_has_result:
                # Missing provider data is temporary, not a sporting result.
                continue
            if refreshed >= maximum or not _archived_refresh_due(legs, state, now):
                continue
            refreshed += 1
            checked_at = now_sp()
            for leg in legs:
                leg.checked_at = checked_at
            payload = fetch_match_stats(session, f"https://betsapi.com/r/{game_id}")
            payload_stats = (payload or {}).get("stats") or {}
            meaningful_stats = any(key != "Minute" for key in payload_stats)
            payload_time = str((payload or {}).get("time_text") or "").strip().casefold()
            payload_finished = payload_time in {"ft", "finished", "ended", "encerrado", "ap"} or "full time" in payload_time
            if not payload or (not meaningful_stats and not payload_finished):
                # Depois que o provedor remove uma partida antiga, sua pagina
                # pode virar um 404 contendo apenas um relogio de outro widget.
                # Isso nao e resultado esportivo e nunca deve sobrescrever o
                # ultimo estado valido nem manter a selecao pendente para sempre.
                db.session.commit()
                continue
            if state is None:
                state = LiveGameState(game_id=game_id)
                db.session.add(state)
            state.url = f"https://betsapi.com/r/{game_id}"
            state.league = payload.get("league") or legs[0].league
            state.home_team = payload.get("home_team") or legs[0].home_team
            state.away_team = payload.get("away_team") or legs[0].away_team
            state.time_text = payload.get("time_text") or ""
            state.minute = payload.get("minute")
            state.score = payload.get("score")
            state.stats_json = json.dumps(payload.get("stats") or {}, ensure_ascii=False)
            state.events_json = json.dumps(payload.get("events") or [], ensure_ascii=False)
            db.session.commit()
    finally:
        if own_session:
            session.close()


def resolve_saved_tickets(session=None, refresh_maximum=3):
    # A primeira perna RED encerra financeiramente o bilhete, mas as demais
    # continuam sendo apuradas para manter o histórico individual correto.
    tickets = SavedTicket.query.filter(SavedTicket.status.in_(("pending", "red", "unavailable"))).all()
    for ticket in tickets:
        recovered = False
        for leg in ticket.legs:
            if leg.status == "unavailable":
                leg.status = "pending"
                leg.checked_at = None
                recovered = True
        if ticket.status == "unavailable":
            ticket.status = "pending"
            ticket.profit = 0
            ticket.resolved_at = None
            ticket.telegram_notified_at = None
            recovered = True
        if recovered:
            db.session.commit()
    _refresh_archived_states(tickets, session, maximum=refresh_maximum)
    for ticket in tickets:
        changed = False
        for leg in ticket.legs:
            if leg.status != "pending":
                continue
            state = LiveGameState.query.filter_by(game_id=leg.game_id).first()
            if not state:
                continue
            try:
                stats = json.loads(state.stats_json or "{}")
            except (TypeError, ValueError):
                stats = {}
            value = _leg_value(leg, state, stats)
            if value is None:
                continue
            previous_status = leg.status
            previous_value = leg.result_value
            if leg.market_key == "goal_ht":
                if value >= 1:
                    leg.status = "green"
                elif _is_finished(state):
                    leg.status = "red"
            elif leg.market_key == "corners_10_over1":
                if value >= 2:
                    leg.status = "green"
                elif _early_corner_window_closed(state):
                    leg.status = "red"
            elif re.fullmatch(r"(goals|corners|cards|shots|shots_on_target)(?:_under)?_(1h|2h)(?:_(home|away))?", leg.market_key or ""):
                closed = _first_half_closed(state) if "_1h" in leg.market_key else _leg_match_closed(leg, state)
                if _is_under_market(leg):
                    if value >= float(leg.target_line or 0):
                        leg.status = "red"
                    elif closed:
                        leg.status = "green"
                elif value > float(leg.target_line or 0):
                    leg.status = "green"
                elif closed:
                    leg.status = "red"
            else:
                if _is_under_market(leg):
                    if value >= float(leg.target_line or 0):
                        leg.status = "red"
                    elif _leg_match_closed(leg, state):
                        leg.status = "green"
                elif value > float(leg.target_line or 0):
                    leg.status = "green"
                elif _leg_match_closed(leg, state):
                    leg.status = "red"
            if previous_value != value or previous_status != leg.status:
                leg.result_value = value
                leg.checked_at = now_sp()
                changed = True
        statuses = [leg.status for leg in ticket.legs]
        if "red" in statuses:
            ticket.status, ticket.profit = "red", -float(ticket.stake_amount)
        elif statuses and all(status == "green" for status in statuses):
            ticket.status = "green"
            ticket.profit = round(float(ticket.stake_amount) * (float(ticket.total_odd) - 1), 2)
        else:
            ticket.status = "pending"
            ticket.profit = 0
        if ticket.status != "pending" and not ticket.resolved_at:
            ticket.resolved_at = now_sp()
            changed = True
        if changed:
            db.session.commit()
        if ticket.status != "pending" and not ticket.telegram_notified_at:
            icon = "✅" if ticket.status == "green" else "❌" if ticket.status == "red" else "⚪"
            result = (
                f"Lucro líquido: R$ {ticket.profit:.2f}\nRetorno total: R$ {(ticket.stake_amount + ticket.profit):.2f}"
                if ticket.status == "green" else f"Prejuízo: R$ {abs(ticket.profit):.2f}"
                if ticket.status == "red" else "Resultado indisponível no provedor; lançamento financeiro não calculado."
            )
            status_icons = {"green": "✅", "red": "❌", "pending": "🟡", "unavailable": "⚪"}
            selections = "\n\n".join(
                f"{status_icons.get(leg.status, '🟡')} {leg.home_team} x {leg.away_team}\n"
                f"{leg.market_label} — {leg.status.upper()}"
                f"{' (' + _result_description(leg) + ')' if _result_description(leg) else ''}"
                for leg in ticket.legs
            )
            early = ticket.status == "green" and any(
                (state := LiveGameState.query.filter_by(game_id=leg.game_id).first()) and not _is_finished(state)
                for leg in ticket.legs
            )
            confirmation = "\n\nGreen confirmado assim que todas as seleções foram atingidas." if early else ""
            text = (
                f"{icon} {ticket.name.upper()} — {ticket.status.upper()}\n\n"
                f"{selections}\n\nOdd: {ticket.total_odd:.2f}\n"
                f"Valor apostado: R$ {ticket.stake_amount:.2f}\n{result}{confirmation}"
            )
            user = ticket.user
            if user.telegram_token and user.telegram_chat_id:
                result = send_message(user.telegram_token, user.telegram_chat_id, text)
                ok = bool(result and result[0])
                if ok:
                    ticket.telegram_notified_at = now_sp()
                    db.session.commit()
