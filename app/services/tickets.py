import json
import re
import unicodedata
from datetime import timedelta

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
    matched = re.fullmatch(r"(goals|corners|cards|shots|shots_on_target)_(1h|2h)(?:_(home|away))?", (leg.market_key or "").casefold())
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
    if leg.market_key in {"over15", "over25"}:
        return None if home is None else home + away
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


def _result_description(leg):
    if leg.result_value is None:
        return ""
    value = float(leg.result_value)
    shown = str(int(value)) if value.is_integer() else str(value).replace(".", ",")
    key = (leg.market_key or "").casefold()
    if key in {"over15", "over25", "goal_ht"} or key.startswith("goals_"):
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


def resolve_saved_tickets():
    # A primeira perna RED encerra financeiramente o bilhete, mas as demais
    # continuam sendo apuradas para manter o histórico individual correto.
    tickets = SavedTicket.query.filter(SavedTicket.status.in_(("pending", "red"))).filter(
        SavedTicket.created_at >= now_sp() - timedelta(days=4)
    ).limit(100).all()
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
            elif re.fullmatch(r"(goals|corners|cards|shots|shots_on_target)_(1h|2h)(?:_(home|away))?", leg.market_key or ""):
                if value > float(leg.target_line or 0):
                    leg.status = "green"
                elif "_1h" in leg.market_key and _first_half_closed(state):
                    leg.status = "red"
                elif "_2h" in leg.market_key and _is_finished(state):
                    leg.status = "red"
            else:
                if value > float(leg.target_line or 0):
                    leg.status = "green"
                elif _is_finished(state):
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
        if ticket.status != "pending" and not ticket.resolved_at:
            ticket.resolved_at = now_sp()
            changed = True
        if changed:
            db.session.commit()
        if ticket.status != "pending" and not ticket.telegram_notified_at:
            icon = "✅" if ticket.status == "green" else "❌"
            result = f"Lucro líquido: R$ {ticket.profit:.2f}\nRetorno total: R$ {(ticket.stake_amount + ticket.profit):.2f}" if ticket.status == "green" else f"Prejuízo: R$ {abs(ticket.profit):.2f}"
            status_icons = {"green": "✅", "red": "❌", "pending": "🟡"}
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
