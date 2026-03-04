import json
import os
import re
import difflib
import unicodedata
from datetime import datetime, timedelta

from flask import Blueprint, Response, flash, jsonify, redirect, render_template, request, url_for
from flask_login import current_user, login_required
from markupsafe import Markup, escape
from sqlalchemy import func

from ..extensions import db
from ..models import LiveGameState, MatchAlert, Rule, RuleCondition, RuleOutcomeCondition
from ..services.evaluator import evaluate_rule, history_confidence
from ..services.scraper import (
    fetch_live_games,
    fetch_match_history,
    fetch_match_stats,
    format_history_summary,
    is_first_half_extra_time,
    make_session,
    summarize_history,
)
from ..services.worker import parse_score
from ..services.undo import create_undo_action, snapshot_rule
from ..utils.time import now_sp

rules_bp = Blueprint("rules", __name__, url_prefix="/rules")
AI_HINT_CACHE = {}
IA_SHADOW_CONTROL_RULE_NAME = os.environ.get("IA_SHADOW_CONTROL_RULE_NAME", "REGRA IA SOMBRA (Sistema)")


def _ensure_ia_shadow_control_rule(user_id: int):
    existing = (
        Rule.query.filter_by(user_id=user_id, name=IA_SHADOW_CONTROL_RULE_NAME)
        .order_by(Rule.id.asc())
        .first()
    )
    if existing:
        return existing
    rule = Rule(
        user_id=user_id,
        name=IA_SHADOW_CONTROL_RULE_NAME,
        time_limit_min=90,
        message_template=None,
        is_active=True,
        second_half_only=True,
        follow_ht=False,
        follow_ft=False,
        notify_telegram=False,
    )
    db.session.add(rule)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return (
            Rule.query.filter_by(user_id=user_id, name=IA_SHADOW_CONTROL_RULE_NAME)
            .order_by(Rule.id.asc())
            .first()
        )
    return rule


def _normalize_hint_text(raw: str) -> str:
    text = (raw or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9x\s]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    typo_map = {
        "estanteio": "escanteio",
        "estanteios": "escanteios",
        "escantei": "escanteio",
        "cantos": "escanteios",
        "cartoes": "cartao",
        "amarelos": "cartao",
        "vermelhos": "cartao",
        "primeirotempo": "primeiro tempo",
        "segundotempo": "segundo tempo",
        "placarcorreto": "placar correto",
        "ambasmarcam": "ambas marcam",
        "finalizacao": "finalizacoes",
        "baliza": "chute no alvo",
    }
    for wrong, right in typo_map.items():
        text = text.replace(wrong, right)
    return text


def _contains_any_fuzzy(text: str, keywords: list[str], min_ratio: float = 0.84) -> bool:
    if not text:
        return False
    if any(k in text for k in keywords):
        return True
    tokens = text.split()
    for token in tokens:
        for key in keywords:
            for kt in key.split():
                if len(token) < 4 or len(kt) < 4:
                    continue
                if difflib.SequenceMatcher(None, token, kt).ratio() >= min_ratio:
                    return True
    return False


def _hint_market_from_text(compact: str) -> str:
    text = _normalize_hint_text(compact)
    if _contains_any_fuzzy(text, ["escanteio", "escanteios", "corner", "cantos"]):
        return "corners"
    if _contains_any_fuzzy(text, ["cartao", "card", "amarelo", "vermelho"]):
        return "cards"
    if _contains_any_fuzzy(text, ["ambas marcam", "btts", "gg"]):
        return "btts"
    if _contains_any_fuzzy(text, ["placar correto", "placar exato", "correct score", "exato"]):
        return "exact_score"
    if _contains_any_fuzzy(text, ["2 tempo", "2o tempo", "segundo tempo"]):
        return "goal_2h"
    if _contains_any_fuzzy(text, ["under", "menos de"]):
        return "under_goals"
    if _contains_any_fuzzy(text, ["over", "mais de"]):
        return "over_goals"
    if _contains_any_fuzzy(text, ["ht", "1 tempo", "1o tempo", "primeiro tempo"]):
        return "goal_ht"
    if _contains_any_fuzzy(text, ["gol", "gols"]):
        return "goals"
    return "generic"


def _hint_market_from_rule_name(name: str, result_minute: int | None) -> str:
    compact = _normalize_hint_text(name or "")
    market = _hint_market_from_text(compact)
    if market == "goals" and isinstance(result_minute, int) and result_minute <= 45:
        return "goal_ht"
    return market


def _hint_stat_total(stats_json: str | None, key: str) -> int | None:
    if not stats_json:
        return None
    try:
        stats = json.loads(stats_json)
    except Exception:
        return None
    if not isinstance(stats, dict):
        return None
    bucket = stats.get(key)
    if not isinstance(bucket, dict):
        return None
    value = bucket.get("total")
    try:
        if value in (None, "", "-"):
            return None
        return int(value)
    except Exception:
        return None


def _percentile(values: list[int], pct: float) -> int | None:
    nums = sorted(v for v in values if isinstance(v, (int, float)))
    if not nums:
        return None
    idx = int((len(nums) - 1) * pct)
    return int(round(nums[idx]))


def _global_market_learning(market_key: str, limit: int = 2400) -> dict | None:
    now = now_sp()
    cached = AI_HINT_CACHE.get(market_key)
    if cached and isinstance(cached, dict):
        seen_at = cached.get("seen_at")
        if isinstance(seen_at, datetime) and (now - seen_at) < timedelta(minutes=5):
            return cached.get("profile")

    rows = (
        db.session.query(MatchAlert, Rule)
        .join(Rule, Rule.id == MatchAlert.rule_id)
        .filter(MatchAlert.status.in_(("green", "red")))
        .order_by(MatchAlert.created_at.desc())
        .limit(limit)
        .all()
    )
    if not rows:
        return None

    market_rows = []
    for alert, rule in rows:
        mk = _hint_market_from_rule_name(rule.name if rule else "", alert.result_minute)
        if market_key != "generic" and mk != market_key:
            continue
        market_rows.append((alert, rule))
    if len(market_rows) < 30:
        return None

    greens = 0
    reds = 0
    green_minutes = []
    green_on_target = []
    green_dangerous = []
    green_corners = []
    for alert, _rule in market_rows:
        is_green = alert.status == "green"
        greens += 1 if is_green else 0
        reds += 0 if is_green else 1
        if not is_green:
            continue
        if isinstance(alert.alert_minute, int):
            green_minutes.append(alert.alert_minute)
        ot = _hint_stat_total(alert.initial_stats_json, "On Target")
        dg = _hint_stat_total(alert.initial_stats_json, "Dangerous Attacks")
        cr = _hint_stat_total(alert.initial_stats_json, "Corners")
        if isinstance(ot, int):
            green_on_target.append(ot)
        if isinstance(dg, int):
            green_dangerous.append(dg)
        if isinstance(cr, int):
            green_corners.append(cr)

    total = greens + reds
    if total == 0:
        return None
    profile = {
        "samples": total,
        "green": greens,
        "red": reds,
        "win_rate": round((greens / total) * 100, 2),
        "minute_p50": _percentile(green_minutes, 0.50),
        "on_target_p25": _percentile(green_on_target, 0.25),
        "dangerous_p25": _percentile(green_dangerous, 0.25),
        "corners_p25": _percentile(green_corners, 0.25),
    }
    AI_HINT_CACHE[market_key] = {"seen_at": now, "profile": profile}
    return profile


def _clean_league_name(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def _parse_allowed_leagues_json(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        data = json.loads(value)
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    leagues = []
    seen = set()
    for item in data:
        name = _clean_league_name(item)
        if not name:
            continue
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        leagues.append(name)
    return leagues


def _available_leagues(limit: int = 250) -> list[str]:
    leagues = set()
    for col in (LiveGameState.league, MatchAlert.league):
        try:
            rows = (
                db.session.query(col)
                .filter(col.isnot(None))
                .distinct()
                .limit(limit)
                .all()
            )
        except Exception:
            rows = []
        for (val,) in rows:
            name = _clean_league_name(val)
            if name:
                leagues.add(name)
    return sorted(leagues, key=lambda s: s.casefold())


def _parse_conditions(form):
    conditions = []
    def _normalize_operator(value: str) -> str:
        value = (value or "").strip()
        if value == "≥":
            return ">="
        if value == "≤":
            return "<="
        if value == "=":
            return "=="
        return value

    # New grouped format: group-<g>-cond-<i>-stat_key
    grouped_keys = [k for k in form.keys() if k.startswith("group-") and k.endswith("-stat_key")]
    if grouped_keys:
        for key in grouped_keys:
            parts = key.split("-")
            if len(parts) < 4:
                continue
            group_id = int(parts[1])
            index = int(parts[3])
            stat_key = form.get(f"group-{group_id}-cond-{index}-stat_key", "").strip()
            if stat_key.casefold() in ("league", "liga"):
                # League filter is stored at the Rule level (allowed_leagues_json), not as a numeric stat condition.
                continue
            side = form.get(f"group-{group_id}-cond-{index}-side", "").strip()
            if not side and stat_key.lower() in ("minute", "minutos", "minuto", "min"):
                side = "total"
            operator = _normalize_operator(form.get(f"group-{group_id}-cond-{index}-operator", ""))
            value_raw = form.get(f"group-{group_id}-cond-{index}-value", "").strip()
            if stat_key and side and operator and value_raw.isdigit():
                conditions.append(
                    RuleCondition(
                        stat_key=stat_key,
                        side=side,
                        operator=operator,
                        value=int(value_raw),
                        group_id=group_id,
                    )
                )
        return conditions

    # Legacy flat format
    index = 0
    while True:
        stat_key = form.get(f"conditions-{index}-stat_key")
        if stat_key is None:
            break
        stat_key = stat_key.strip()
        if stat_key.casefold() in ("league", "liga"):
            # League filter is stored at the Rule level (allowed_leagues_json), not as a numeric stat condition.
            index += 1
            continue
        side = form.get(f"conditions-{index}-side", "").strip()
        if not side and stat_key.lower() in ("minute", "minutos", "minuto", "min"):
            side = "total"
        operator = _normalize_operator(form.get(f"conditions-{index}-operator", ""))
        value_raw = form.get(f"conditions-{index}-value", "").strip()
        if stat_key and side and operator and value_raw.isdigit():
            conditions.append(
                RuleCondition(
                    stat_key=stat_key,
                    side=side,
                    operator=operator,
                    value=int(value_raw),
                    group_id=0,
                )
            )
        index += 1
    return conditions


def _parse_outcome_conditions(form, prefix):
    conditions = []
    def _normalize_operator(value: str) -> str:
        value = (value or "").strip()
        if value == "≥":
            return ">="
        if value == "≤":
            return "<="
        if value == "=":
            return "=="
        return value

    outcome_type = prefix.split("-")[-1]
    grouped_pattern = re.compile(rf"^{re.escape(prefix)}-group-(\d+)-cond-(\d+)-stat_key$")
    grouped_keys = [k for k in form.keys() if grouped_pattern.match(k)]
    if grouped_keys:
        for key in grouped_keys:
            match = grouped_pattern.match(key)
            if not match:
                continue
            group_id = int(match.group(1))
            index = int(match.group(2))
            stat_key = form.get(f"{prefix}-group-{group_id}-cond-{index}-stat_key", "").strip()
            side = form.get(f"{prefix}-group-{group_id}-cond-{index}-side", "").strip()
            if not side and stat_key.lower() in ("minute", "minutos", "minuto", "min"):
                side = "total"
            operator = _normalize_operator(form.get(f"{prefix}-group-{group_id}-cond-{index}-operator", ""))
            value_raw = form.get(f"{prefix}-group-{group_id}-cond-{index}-value", "").strip()
            if stat_key and side and operator and value_raw.isdigit():
                conditions.append(
                    RuleOutcomeCondition(
                        outcome_type=outcome_type,
                        stat_key=stat_key,
                        side=side,
                        operator=operator,
                        value=int(value_raw),
                        group_id=group_id,
                    )
                )
        return conditions

    index = 0
    while True:
        stat_key = form.get(f"{prefix}-{index}-stat_key")
        if stat_key is None:
            break
        stat_key = stat_key.strip()
        side = form.get(f"{prefix}-{index}-side", "").strip()
        if not side and stat_key.lower() in ("minute", "minutos", "minuto", "min"):
            side = "total"
        operator = _normalize_operator(form.get(f"{prefix}-{index}-operator", ""))
        value_raw = form.get(f"{prefix}-{index}-value", "").strip()
        if stat_key and side and operator and value_raw.isdigit():
            conditions.append(
                RuleOutcomeCondition(
                    outcome_type=outcome_type,
                    stat_key=stat_key,
                    side=side,
                    operator=operator,
                    value=int(value_raw),
                    group_id=0,
                )
            )
        index += 1
    return conditions


def _coerce_int(value, default=None):
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _normalize_import_operator(value: str) -> str:
    value = (value or "").strip()
    if value in ("?", "???"):
        return ">="
    if value in ("?", "???"):
        return "<="
    if value == "=":
        return "=="
    if value not in (">", ">=", "<", "<=", "==", "!="):
        return ">="
    return value


def _build_ai_hint(objective_text: str) -> dict:
    text = (objective_text or "").strip()
    compact = _normalize_hint_text(text)
    market_key = _hint_market_from_text(compact)
    profile = _global_market_learning(market_key)

    def learned(default_value: int, key: str, min_v: int, max_v: int) -> int:
        if not profile:
            return default_value
        raw = profile.get(key)
        if not isinstance(raw, int):
            return default_value
        return max(min_v, min(max_v, raw))

    learning_note = None
    if profile:
        learning_note = (
            f"Base global: {profile.get('samples')} entradas | "
            f"WR {profile.get('win_rate')}% ({profile.get('green')}G/{profile.get('red')}R)."
        )
    nums = [int(n) for n in re.findall(r"\d+", compact)]
    minute_hint = None
    for n in nums:
        if 5 <= n <= 45:
            minute_hint = n
            break
    minute_cap = minute_hint or learned(20, "minute_p50", 12, 32)
    goal_target = 1
    if "2 gol" in compact or "2 gols" in compact:
        goal_target = 2

    wants_ht = ("ht" in compact) or ("1 tempo" in compact) or ("1o tempo" in compact) or ("primeiro tempo" in compact)
    wants_ft = ("ft" in compact) or ("final" in compact) or ("full time" in compact) or ("jogo todo" in compact)
    wants_corners = ("escanteio" in compact) or ("corner" in compact)
    wants_cards = ("cartao" in compact) or ("card" in compact)
    wants_btts = ("ambas marcam" in compact) or ("btts" in compact) or ("gg" in compact)
    wants_second_half = ("2 tempo" in compact) or ("2o tempo" in compact) or ("segundo tempo" in compact)
    wants_exact_score = ("placar correto" in compact) or ("exato" in compact) or ("correct score" in compact)
    wants_over = ("over" in compact) or ("mais de" in compact)
    wants_under = ("under" in compact) or ("menos de" in compact)
    score_pairs = re.findall(r"(\d+)\s*x\s*(\d+)", compact)

    if wants_second_half and score_pairs:
        valid_pairs = []
        for a, b in score_pairs[:4]:
            try:
                h = int(a)
                aw = int(b)
            except Exception:
                continue
            if 0 <= h <= 7 and 0 <= aw <= 7:
                valid_pairs.append((h, aw))
        if valid_pairs:
            totals = sorted({h + aw for h, aw in valid_pairs})
            min_home = min(h for h, _ in valid_pairs)
            min_away = min(a for _, a in valid_pairs)
            targets_txt = " ou ".join(f"{h}x{a}" for h, a in valid_pairs)
            primary_total = totals[0]
            ot_cut = learned(5, "on_target_p25", 3, 9)
            da_cut = learned(45, "dangerous_p25", 30, 80)
            return {
                "title": f"Regra para buscar placar {targets_txt} no 2o tempo",
                "rationale": (
                    f"Para buscar {targets_txt} no 2o tempo, priorize jogos vivos apos 55 minutos. "
                    f"Pelo historico, jogos com pelo menos {ot_cut} chutes no alvo totais e {da_cut} ataques perigosos "
                    f"tem mais chance de chegar nesse perfil de placar."
                ),
                "suggestion": {
                    "name": f"2T BUSCAR {targets_txt}",
                    "message_template": (
                        "🚨 {rule}\n"
                        "{home_team} vs {away_team}\n"
                        "Min: {minute} | Placar: {score}\n"
                        "IA: {ai_commentary}\n"
                        "IA Entrada: {ai_verdict} ({ai_score}/100)\n"
                        "ML: {ml_verdict} ({ml_score}/100)\n"
                        "Link: {url}"
                    ),
                    "outcome_green_minute": 90,
                    "outcome_red_minute": 90,
                    "outcome_red_if_no_green": True,
                    "conditions": [
                        {"group_id": 0, "stat_key": "Minute", "side": "total", "operator": ">=", "value": 55},
                        {"group_id": 0, "stat_key": "on-target", "side": "total", "operator": ">=", "value": ot_cut},
                        {"group_id": 0, "stat_key": "dangerous-attacks", "side": "total", "operator": ">=", "value": da_cut},
                    ],
                    "outcome_green": [
                        {"stat_key": "goals", "side": "total", "operator": "==", "value": primary_total},
                        {"stat_key": "goals", "side": "home", "operator": ">=", "value": min_home},
                        {"stat_key": "goals", "side": "away", "operator": ">=", "value": min_away},
                        {"stat_key": "goals", "side": "total", "operator": ">=", "value": primary_total},
                    ],
                    "outcome_red": [],
                    "allowed_leagues": [],
                    "notes": [
                        learning_note or "Sem base global suficiente para calibrar esse mercado.",
                        "Como a condicao final da regra nao aceita dois placares exatos juntos, use a estrutura do placar (total de gols + gol dos dois lados).",
                        f"Se quiser ficar mais preciso, crie duas regras: uma para {valid_pairs[0][0]}x{valid_pairs[0][1]} e outra para {valid_pairs[-1][0]}x{valid_pairs[-1][1]}.",
                        "Deixe rodar por pelo menos 50 resultados antes de ajustar os filtros.",
                    ],
                },
            }

    if wants_ht and ("gol" in compact):
        return {
            "title": f"Regra inicial para buscar {goal_target} gol(s) no HT",
            "rationale": "Filtra jogos com pressao ofensiva cedo para buscar gol antes do intervalo.",
            "suggestion": {
                "name": f"CHUTES NO GOL ATE {minute_cap} MIN = BUSCAR {goal_target} GOL HT",
                "message_template": (
                    "🚨 {rule}\n"
                    "{home_team} vs {away_team}\n"
                    "Min: {minute} | Placar: {score}\n"
                    "Conf regra: {rule_confidence} | Conf liga: {league_rule_confidence}\n"
                    "H2H: {history_h2h}\n"
                    "IA: {ai_commentary}\n"
                    "IA Entrada: {ai_verdict} ({ai_score}/100)\n"
                    "ML: {ml_verdict} ({ml_score}/100) [{ml_samples} amostras]\n"
                    "Link: {url}"
                ),
                "outcome_green_minute": 45,
                "outcome_red_minute": 45,
                "outcome_red_if_no_green": True,
                "conditions": [
                    {"group_id": 0, "stat_key": "on-target", "side": "total", "operator": ">=", "value": learned(4 if goal_target == 1 else 5, "on_target_p25", 3, 8)},
                    {"group_id": 0, "stat_key": "dangerous-attacks", "side": "total", "operator": ">=", "value": learned(28 if goal_target == 1 else 35, "dangerous_p25", 20, 60)},
                    {"group_id": 0, "stat_key": "Minute", "side": "total", "operator": "<=", "value": minute_cap},
                ],
                "outcome_green": [
                    {"stat_key": "goals", "side": "total", "operator": ">=", "value": goal_target},
                ],
                "outcome_red": [],
                "allowed_leagues": [],
                "notes": [
                    learning_note or "Sem base global suficiente para calibrar esse mercado.",
                    "Se vier muito RED, aumente o filtro de chutes no alvo em +1.",
                    "Se vier poucos sinais, abra o minuto limite em +2/+3 e acompanhe o ML.",
                ],
            },
        }

    if wants_corners:
        corner_target = learned(8, "corners_p25", 6, 12)
        for n in nums:
            if 5 <= n <= 16:
                corner_target = n
                break
        second_half_corners = wants_second_half
        corner_minute_cond = 46 if second_half_corners else 30
        ot_needed = learned(2 if second_half_corners else 3, "on_target_p25", 1, 7)
        da_needed = learned(28 if second_half_corners else 35, "dangerous_p25", 18, 70)
        return {
            "title": "Regra inicial para escanteios no 2o tempo" if second_half_corners else "Regra inicial para escanteios",
            "rationale": (
                f"Para buscar {corner_target} escanteios no 2o tempo, filtre jogos que chegam vivos na etapa final."
                if second_half_corners
                else "Usa volume ofensivo para filtrar jogos com tendencia a corners."
            ),
            "suggestion": {
                "name": (
                    f"2T VIVO = BUSCAR {corner_target} ESCANTEIOS NO 2T"
                    if second_half_corners
                    else f"PRESSAO ATE 30 MIN = BUSCAR {corner_target}+ ESCANTEIOS FT"
                ),
                "message_template": (
                    "🚨 {rule}\n"
                    "{home_team} vs {away_team}\n"
                    "Min: {minute} | Placar: {score}\n"
                    "Escanteios: {corners_home}x{corners_away}\n"
                    "IA Entrada: {ai_verdict} ({ai_score}/100)\n"
                    "Link: {url}"
                ),
                "outcome_green_minute": 90,
                "outcome_red_minute": 90,
                "outcome_red_if_no_green": True,
                "second_half_only": second_half_corners,
                "conditions": [
                    {"group_id": 0, "stat_key": "dangerous-attacks", "side": "total", "operator": ">=", "value": da_needed},
                    {"group_id": 0, "stat_key": "on-target", "side": "total", "operator": ">=", "value": ot_needed},
                    {"group_id": 0, "stat_key": "Minute", "side": "total", "operator": ">=" if second_half_corners else "<=", "value": corner_minute_cond},
                ],
                "outcome_green": [
                    {"stat_key": "corners", "side": "total", "operator": ">=", "value": corner_target},
                ],
                "outcome_red": [],
                "allowed_leagues": [],
                "notes": [
                    learning_note or "Sem base global suficiente para calibrar esse mercado.",
                    (
                        f"Se quiser focar ainda mais no 2o tempo, deixe o modo 'Somente 2o tempo' ligado e use minuto >= {corner_minute_cond}."
                        if second_half_corners
                        else "Mercados de escanteio variam por liga; monitore por campeonato."
                    ),
                    "Depois de 30 a 50 resultados, ajuste os filtros pelos greens e reds.",
                ],
            },
        }

    if wants_cards:
        card_target = 4
        for n in nums:
            if 2 <= n <= 10:
                card_target = n
                break
        return {
            "title": "Regra inicial para cartoes",
            "rationale": "Combina faltas e jogo truncado para buscar cartoes no FT.",
            "suggestion": {
                "name": f"JOGO FISICO = BUSCAR {card_target}+ CARTOES FT",
                "message_template": (
                    "🚨 {rule}\n"
                    "{home_team} vs {away_team}\n"
                    "Min: {minute} | Placar: {score}\n"
                    "IA Entrada: {ai_verdict} ({ai_score}/100)\n"
                    "Link: {url}"
                ),
                "outcome_green_minute": 90,
                "outcome_red_minute": 90,
                "outcome_red_if_no_green": True,
                "second_half_only": True,
                "conditions": [
                    {"group_id": 0, "stat_key": "attacks", "side": "total", "operator": ">=", "value": 55},
                    {"group_id": 0, "stat_key": "Minute", "side": "total", "operator": "<=", "value": 35},
                ],
                "outcome_green": [
                    {"stat_key": "yellow-cards", "side": "total", "operator": ">=", "value": card_target},
                ],
                "outcome_red": [],
                "allowed_leagues": [],
                "notes": [
                    learning_note or "Sem base global suficiente para calibrar esse mercado.",
                    "Para cartoes, foque ligas de contato alto e arbitragens mais rigorosas.",
                ],
            },
        }

    if wants_btts:
        return {
            "title": "Regra inicial para ambas marcam",
            "rationale": "Procura jogo aberto dos dois lados para elevar chance de gol para casa e visitante.",
            "suggestion": {
                "name": "JOGO ABERTO DOS 2 LADOS = BUSCAR BTTS",
                "message_template": (
                    "🚨 {rule}\n"
                    "{home_team} vs {away_team}\n"
                    "Min: {minute} | Placar: {score}\n"
                    "IA Entrada: {ai_verdict} ({ai_score}/100)\n"
                    "Link: {url}"
                ),
                "outcome_green_minute": 90,
                "outcome_red_minute": 90,
                "outcome_red_if_no_green": True,
                "second_half_only": True,
                "conditions": [
                    {"group_id": 0, "stat_key": "on-target", "side": "home", "operator": ">=", "value": 2},
                    {"group_id": 0, "stat_key": "on-target", "side": "away", "operator": ">=", "value": 2},
                    {"group_id": 0, "stat_key": "Minute", "side": "total", "operator": "<=", "value": 60},
                ],
                "outcome_green": [
                    {"stat_key": "goals", "side": "home", "operator": ">=", "value": 1},
                    {"stat_key": "goals", "side": "away", "operator": ">=", "value": 1},
                ],
                "outcome_red": [],
                "allowed_leagues": [],
                "notes": [
                    learning_note or "Sem base global suficiente para calibrar esse mercado.",
                    "Se vier muito 1x0, endureca a condicao do visitante (on-target away >= 3).",
                ],
            },
        }

    if wants_second_half and ("gol" in compact):
        return {
            "title": "Regra inicial para gol no 2o tempo",
            "rationale": "Ativa em jogo vivo apos intervalo, buscando 1 gol no segundo tempo.",
            "suggestion": {
                "name": "2T VIVO = BUSCAR 1 GOL 2T",
                "message_template": (
                    "🚨 {rule}\n"
                    "{home_team} vs {away_team}\n"
                    "Min: {minute} | Placar: {score}\n"
                    "IA Entrada: {ai_verdict} ({ai_score}/100)\n"
                    "Link: {url}"
                ),
                "outcome_green_minute": 90,
                "outcome_red_minute": 90,
                "outcome_red_if_no_green": True,
                "conditions": [
                    {"group_id": 0, "stat_key": "on-target", "side": "total", "operator": ">=", "value": 4},
                    {"group_id": 0, "stat_key": "Minute", "side": "total", "operator": ">=", "value": 55},
                ],
                "outcome_green": [
                    {"stat_key": "goals", "side": "total", "operator": ">=", "value": 1},
                ],
                "outcome_red": [],
                "allowed_leagues": [],
                "notes": [
                    learning_note or "Sem base global suficiente para calibrar esse mercado.",
                    "Use junto com modo 2o tempo quando quiser medir apenas dinamica da etapa final.",
                ],
            },
        }

    if wants_over and ("gol" in compact):
        over_target = 2
        for n in nums:
            if 1 <= n <= 5:
                over_target = n
                break
        red_minute = 90 if wants_ft else 45
        return {
            "title": f"Regra inicial para over {over_target}.5 gols",
            "rationale": "Modela jogo com volume ofensivo para buscar linha de gols acima da media.",
            "suggestion": {
                "name": f"VOLUME OFENSIVO = OVER {over_target}.5 GOLS",
                "message_template": "🚨 {rule}\n{home_team} vs {away_team}\nMin: {minute} | {score}\nIA: {ai_commentary}\nLink: {url}",
                "outcome_green_minute": red_minute,
                "outcome_red_minute": red_minute,
                "outcome_red_if_no_green": True,
                "conditions": [
                    {"group_id": 0, "stat_key": "on-target", "side": "total", "operator": ">=", "value": 5},
                    {"group_id": 0, "stat_key": "dangerous-attacks", "side": "total", "operator": ">=", "value": 45},
                    {"group_id": 0, "stat_key": "Minute", "side": "total", "operator": "<=", "value": 35 if wants_ft else 25},
                ],
                "outcome_green": [
                    {"stat_key": "goals", "side": "total", "operator": ">=", "value": over_target + 1},
                ],
                "outcome_red": [],
                "allowed_leagues": [],
                "notes": [
                    learning_note or "Sem base global suficiente para calibrar esse mercado.",
                    "Valide por liga; algumas ligas inflacionam chutes sem conversao.",
                ],
            },
        }

    if wants_exact_score:
        return {
            "title": "Regra inicial para placar exato",
            "rationale": "Placar exato e mercado mais restritivo; comece com contexto de dominancia clara.",
            "suggestion": {
                "name": "DOMINANCIA MANDANTE = BUSCAR 2x1",
                "message_template": "🚨 {rule}\n{home_team} vs {away_team}\nMin: {minute} | {score}\nIA Entrada: {ai_verdict} ({ai_score}/100)\nLink: {url}",
                "outcome_green_minute": 90,
                "outcome_red_minute": 90,
                "outcome_red_if_no_green": True,
                "conditions": [
                    {"group_id": 0, "stat_key": "on-target", "side": "home", "operator": ">=", "value": 4},
                    {"group_id": 0, "stat_key": "on-target", "side": "away", "operator": "<=", "value": 2},
                    {"group_id": 0, "stat_key": "Minute", "side": "total", "operator": "<=", "value": 70},
                ],
                "outcome_green": [
                    {"stat_key": "goals", "side": "home", "operator": "==", "value": 2},
                    {"stat_key": "goals", "side": "away", "operator": "==", "value": 1},
                ],
                "outcome_red": [],
                "allowed_leagues": [],
                "notes": [
                    learning_note or "Sem base global suficiente para calibrar esse mercado.",
                    "Use stake baixa em placar exato e ajuste com historico longo.",
                ],
            },
        }

    if wants_under and ("gol" in compact):
        return {
            "title": "Regra inicial para under gols",
            "rationale": "Busca jogos de baixo ritmo para linhas de gols abaixo.",
            "suggestion": {
                "name": "RITMO BAIXO = UNDER 2.5 GOLS",
                "message_template": "🚨 {rule}\n{home_team} vs {away_team}\nMin: {minute} | {score}\nIA: {ai_commentary}\nLink: {url}",
                "outcome_green_minute": 90,
                "outcome_red_minute": 90,
                "outcome_red_if_no_green": True,
                "conditions": [
                    {"group_id": 0, "stat_key": "on-target", "side": "total", "operator": "<=", "value": 3},
                    {"group_id": 0, "stat_key": "dangerous-attacks", "side": "total", "operator": "<=", "value": 30},
                    {"group_id": 0, "stat_key": "Minute", "side": "total", "operator": "<=", "value": 35},
                ],
                "outcome_green": [
                    {"stat_key": "goals", "side": "total", "operator": "<=", "value": 2},
                ],
                "outcome_red": [],
                "allowed_leagues": [],
                "notes": [
                    learning_note or "Sem base global suficiente para calibrar esse mercado.",
                    "Evite ligas historicamente over quando operar under.",
                ],
            },
        }

    minute_le = learned(25 if not wants_ft else 35, "minute_p50", 12, 70)
    minute_ge = None
    m_ate = re.search(r"(?:ate|até)\s*(\d{1,2})", compact)
    m_depois = re.search(r"(?:depois|apos|ap[oó]s|a partir de)\s*(\d{1,2})", compact)
    if m_ate:
        minute_le = max(8, min(90, int(m_ate.group(1))))
    if m_depois:
        minute_ge = max(1, min(90, int(m_depois.group(1))))
    if wants_second_half and minute_ge is None:
        minute_ge = 46

    base_conditions = []
    if "chute" in compact or "baliza" in compact or "finaliza" in compact:
        base_conditions.append(
            {"group_id": 0, "stat_key": "on-target", "side": "total", "operator": ">=", "value": learned(3 if not wants_ft else 4, "on_target_p25", 2, 9)}
        )
    if "ataque" in compact:
        base_conditions.append(
            {"group_id": 0, "stat_key": "dangerous-attacks", "side": "total", "operator": ">=", "value": learned(28 if not wants_ft else 38, "dangerous_p25", 18, 85)}
        )
    if "escanteio" in compact or "corner" in compact:
        base_conditions.append(
            {"group_id": 0, "stat_key": "corners", "side": "total", "operator": ">=", "value": learned(3, "corners_p25", 1, 8)}
        )
    if not base_conditions:
        base_conditions = [
            {"group_id": 0, "stat_key": "on-target", "side": "total", "operator": ">=", "value": learned(3 if not wants_ft else 4, "on_target_p25", 2, 8)},
            {"group_id": 0, "stat_key": "dangerous-attacks", "side": "total", "operator": ">=", "value": learned(25 if not wants_ft else 35, "dangerous_p25", 18, 70)},
        ]
    if minute_ge is not None:
        base_conditions.append({"group_id": 0, "stat_key": "Minute", "side": "total", "operator": ">=", "value": minute_ge})
    else:
        base_conditions.append({"group_id": 0, "stat_key": "Minute", "side": "total", "operator": "<=", "value": minute_le})

    outcome_green = []
    title = "Sugestao inteligente de regra"
    if score_pairs:
        parsed_pairs = []
        for h_raw, a_raw in score_pairs[:4]:
            try:
                h, a = int(h_raw), int(a_raw)
            except Exception:
                continue
            if 0 <= h <= 7 and 0 <= a <= 7:
                parsed_pairs.append((h, a))
        if parsed_pairs:
            title = f"Sugestao para placares alvo ({' ou '.join([f'{h}x{a}' for h, a in parsed_pairs])})"
            totals = sorted({h + a for h, a in parsed_pairs})
            outcome_green = [
                {"stat_key": "goals", "side": "total", "operator": ">=", "value": totals[0]},
                {"stat_key": "goals", "side": "home", "operator": ">=", "value": min(h for h, _ in parsed_pairs)},
                {"stat_key": "goals", "side": "away", "operator": ">=", "value": min(a for _, a in parsed_pairs)},
            ]
    if not outcome_green and wants_corners:
        outcome_green = [{"stat_key": "corners", "side": "total", "operator": ">=", "value": learned(8, "corners_p25", 6, 12)}]
        title = "Sugestao para mercado de escanteios"
    if not outcome_green and wants_cards:
        outcome_green = [{"stat_key": "yellow-cards", "side": "total", "operator": ">=", "value": 4}]
        title = "Sugestao para mercado de cartoes"
    if not outcome_green and wants_under and ("gol" in compact):
        outcome_green = [{"stat_key": "goals", "side": "total", "operator": "<=", "value": 2}]
        title = "Sugestao para under gols"
    if not outcome_green and wants_over and ("gol" in compact):
        outcome_green = [{"stat_key": "goals", "side": "total", "operator": ">=", "value": 3 if wants_ft else 2}]
        title = "Sugestao para over gols"
    if not outcome_green and wants_btts:
        outcome_green = [
            {"stat_key": "goals", "side": "home", "operator": ">=", "value": 1},
            {"stat_key": "goals", "side": "away", "operator": ">=", "value": 1},
        ]
        title = "Sugestao para ambas marcam"
    if not outcome_green:
        outcome_green = [{"stat_key": "goals", "side": "total", "operator": ">=", "value": 1 if not wants_ft else 2}]

    red_minute = 90 if (wants_ft or wants_second_half or minute_ge is not None) else 45
    notes = [learning_note or "Sem base global suficiente para calibrar esse mercado."]
    if score_pairs:
        notes.append("Para placares como 2x1 e 1x2, o ideal e criar 2 regras separadas e comparar qual funciona melhor.")
    notes.append("Use o teste da regra no ao vivo e ajuste thresholds a cada 30-50 resultados.")

    return {
        "title": title,
        "rationale": f"Objetivo lido: '{text}'. A sugestao abaixo foi montada automaticamente com base no seu pedido.",
        "suggestion": {
            "name": f"Estrategia IA - {text[:36]}".strip(),
            "message_template": (
                "🚨 {rule}\n"
                "{home_team} vs {away_team}\n"
                "Min: {minute} | Placar: {score}\n"
                "IA: {ai_commentary}\n"
                "IA Entrada: {ai_verdict} ({ai_score}/100)\n"
                "Link: {url}"
            ),
            "outcome_green_minute": red_minute,
            "outcome_red_minute": red_minute,
            "outcome_red_if_no_green": True,
            "second_half_only": wants_second_half,
            "conditions": base_conditions,
            "outcome_green": outcome_green,
            "outcome_red": [],
            "allowed_leagues": [],
            "notes": notes,
        },
    }


def _serialize_rule(rule: Rule) -> dict:
    return {
        "name": rule.name,
        "time_limit_min": rule.time_limit_min,
        "message_template": rule.message_template,
        "is_active": rule.is_active,
        "second_half_only": rule.second_half_only,
        "notify_telegram": rule.notify_telegram,
        "alert_on_penalty": rule.alert_on_penalty,
        "follow_ht": rule.follow_ht,
        "follow_ft": rule.follow_ft,
        "outcome_green_stage": rule.outcome_green_stage,
        "outcome_red_stage": rule.outcome_red_stage,
        "outcome_green_minute": rule.outcome_green_minute,
        "outcome_red_minute": rule.outcome_red_minute,
        "outcome_red_if_no_green": rule.outcome_red_if_no_green,
        "score_home": rule.score_home,
        "score_away": rule.score_away,
        "allowed_leagues": _parse_allowed_leagues_json(getattr(rule, "allowed_leagues_json", None)),
        "conditions": [_condition_dict(c) for c in (rule.conditions or [])],
        "outcome_conditions": [_condition_dict(c) | {"outcome_type": c.outcome_type} for c in (rule.outcome_conditions or [])],
    }


def _deserialize_condition(item: dict):
    stat_key = str(item.get("stat_key", "")).strip()
    side = str(item.get("side", "")).strip()
    operator = _normalize_import_operator(item.get("operator", ""))
    value = _coerce_int(item.get("value"))
    group_id = _coerce_int(item.get("group_id"), default=0)
    if not stat_key or not side or value is None:
        return None
    return RuleCondition(
        stat_key=stat_key,
        side=side,
        operator=operator,
        value=value,
        group_id=group_id,
    )


def _deserialize_outcome_condition(item: dict):
    outcome_type = str(item.get("outcome_type", "")).strip().lower()
    stat_key = str(item.get("stat_key", "")).strip()
    side = str(item.get("side", "")).strip()
    operator = _normalize_import_operator(item.get("operator", ""))
    value = _coerce_int(item.get("value"))
    group_id = _coerce_int(item.get("group_id"), default=0)
    if outcome_type not in ("green", "red") or not stat_key or not side or value is None:
        return None
    return RuleOutcomeCondition(
        outcome_type=outcome_type,
        stat_key=stat_key,
        side=side,
        operator=operator,
        value=value,
        group_id=group_id,
    )


def _condition_dict(cond):
    data = {
        "stat_key": cond.stat_key,
        "side": cond.side,
        "operator": cond.operator,
        "value": cond.value,
    }
    if hasattr(cond, "group_id"):
        data["group_id"] = cond.group_id
    return data


def _normalize_text(value) -> str:
    return " ".join(str(value or "").strip().split())


def _name_key(value) -> str:
    return _normalize_text(value).casefold()


def _condition_signature(cond, include_outcome_type: bool = False):
    base = (
        _normalize_text(getattr(cond, "stat_key", "")).casefold(),
        _normalize_text(getattr(cond, "side", "")).lower(),
        _normalize_import_operator(getattr(cond, "operator", "")),
        _coerce_int(getattr(cond, "value", None), 0),
        _coerce_int(getattr(cond, "group_id", 0), 0),
    )
    if include_outcome_type:
        return (_normalize_text(getattr(cond, "outcome_type", "")).lower(),) + base
    return base


def _rule_signature(
    *,
    name,
    time_limit_min,
    message_template,
    is_active,
    second_half_only,
    notify_telegram,
    alert_on_penalty,
    follow_ht,
    follow_ft,
    outcome_green_stage,
    outcome_red_stage,
    outcome_green_minute,
    outcome_red_minute,
    outcome_red_if_no_green,
    score_home,
    score_away,
    allowed_leagues,
    conditions,
    outcome_conditions,
):
    return (
        _name_key(name),
        _coerce_int(time_limit_min, 90),
        _normalize_text(message_template) or None,
        _coerce_bool(is_active, True),
        _coerce_bool(second_half_only, False),
        _coerce_bool(notify_telegram, True),
        _coerce_bool(alert_on_penalty, False),
        _coerce_bool(follow_ht, True),
        _coerce_bool(follow_ft, True),
        _normalize_text(outcome_green_stage or "HT").upper(),
        _normalize_text(outcome_red_stage or "HT").upper(),
        _coerce_int(outcome_green_minute),
        _coerce_int(outcome_red_minute),
        _coerce_bool(outcome_red_if_no_green, False),
        _coerce_int(score_home),
        _coerce_int(score_away),
        tuple(sorted(_clean_league_name(x).casefold() for x in (allowed_leagues or []) if _clean_league_name(x))),
        tuple(sorted(_condition_signature(c) for c in (conditions or []))),
        tuple(sorted(_condition_signature(c, include_outcome_type=True) for c in (outcome_conditions or []))),
    )


def _rule_model_signature(rule: Rule):
    return _rule_signature(
        name=rule.name,
        time_limit_min=rule.time_limit_min,
        message_template=rule.message_template,
        is_active=rule.is_active,
        second_half_only=rule.second_half_only,
        notify_telegram=rule.notify_telegram,
        alert_on_penalty=rule.alert_on_penalty,
        follow_ht=rule.follow_ht,
        follow_ft=rule.follow_ft,
        outcome_green_stage=rule.outcome_green_stage,
        outcome_red_stage=rule.outcome_red_stage,
        outcome_green_minute=rule.outcome_green_minute,
        outcome_red_minute=rule.outcome_red_minute,
        outcome_red_if_no_green=rule.outcome_red_if_no_green,
        score_home=rule.score_home,
        score_away=rule.score_away,
        allowed_leagues=_parse_allowed_leagues_json(getattr(rule, "allowed_leagues_json", None)),
        conditions=rule.conditions or [],
        outcome_conditions=rule.outcome_conditions or [],
    )


def _build_form_context(form):
    allowed_leagues = _parse_allowed_leagues_json(form.get("allowed_leagues_json"))
    form_data = {
        "name": form.get("name", "").strip(),
        "message_template": form.get("message_template", "").strip(),
        "is_active": bool(form.get("is_active")),
        "second_half_only": bool(form.get("second_half_only")),
        "notify_telegram": bool(form.get("notify_telegram")),
        "alert_on_penalty": bool(form.get("alert_on_penalty")),
        "score_home": form.get("score_home", "").strip(),
        "score_away": form.get("score_away", "").strip(),
        "outcome_green_minute": form.get("outcome_green_minute", "").strip(),
        "outcome_red_minute": form.get("outcome_red_minute", "").strip(),
        "outcome_red_if_no_green": bool(form.get("outcome_red_if_no_green")),
    }
    conditions = [_condition_dict(c) for c in _parse_conditions(form)]
    outcome_green = [_condition_dict(c) for c in _parse_outcome_conditions(form, "outcome-green")]
    outcome_red = [_condition_dict(c) for c in _parse_outcome_conditions(form, "outcome-red")]
    return {
        "form_data": form_data,
        "form_conditions": conditions,
        "form_outcome_green": outcome_green,
        "form_outcome_red": outcome_red,
        "allowed_leagues": allowed_leagues,
    }


def _build_rule_context(rule):
    return {
        "form_conditions": [_condition_dict(c) for c in (rule.conditions or [])],
        "form_outcome_green": [
            _condition_dict(c)
            for c in (rule.outcome_conditions or [])
            if c.outcome_type == "green"
        ],
        "form_outcome_red": [
            _condition_dict(c)
            for c in (rule.outcome_conditions or [])
            if c.outcome_type == "red"
        ],
        "allowed_leagues": _parse_allowed_leagues_json(getattr(rule, "allowed_leagues_json", None)),
    }


@rules_bp.route("/")
@login_required
def list_rules():
    _ensure_ia_shadow_control_rule(current_user.id)
    stage_filter = (request.args.get("stage") or "all").strip().lower()
    if stage_filter not in ("all", "ht", "ft"):
        stage_filter = "all"
    rules_query = Rule.query.filter_by(user_id=current_user.id)
    if stage_filter == "ht":
        rules_query = rules_query.filter(Rule.second_half_only.is_(False))
    elif stage_filter == "ft":
        rules_query = rules_query.filter(Rule.second_half_only.is_(True))
    rules = rules_query.all()
    has_any_rules = Rule.query.filter_by(user_id=current_user.id).count() > 0
    rule_stats = {rule.id: {"green": 0, "red": 0} for rule in rules}
    rule_alert_counts = {rule.id: 0 for rule in rules}
    rule_rankings = {rule.id: {"leagues": {"green": [], "red": []}, "teams": {"green": [], "red": []}} for rule in rules}
    rule_ids = [rule.id for rule in rules]
    counts = (
        db.session.query(MatchAlert.rule_id, MatchAlert.status, func.count(MatchAlert.id))
        .filter(MatchAlert.user_id == current_user.id)
        .group_by(MatchAlert.rule_id, MatchAlert.status)
        .all()
    )
    for rule_id, status, total in counts:
        if rule_id in rule_stats and status in ("green", "red"):
            rule_stats[rule_id][status] = total
        if rule_id in rule_alert_counts:
            rule_alert_counts[rule_id] += total

    if rule_ids:
        alerts = (
            db.session.query(
                MatchAlert.rule_id,
                MatchAlert.status,
                MatchAlert.league,
                MatchAlert.home_team,
                MatchAlert.away_team,
            )
            .filter(MatchAlert.user_id == current_user.id)
            .filter(MatchAlert.rule_id.in_(rule_ids))
            .filter(MatchAlert.status.in_(("green", "red")))
            .all()
        )
        league_counts = {rid: {"green": {}, "red": {}} for rid in rule_ids}
        team_counts = {rid: {"green": {}, "red": {}} for rid in rule_ids}
        for rule_id, status, league, home_team, away_team in alerts:
            if rule_id not in league_counts:
                continue
            if league:
                league_counts[rule_id][status][league] = league_counts[rule_id][status].get(league, 0) + 1
            for team in (home_team, away_team):
                if team:
                    team_counts[rule_id][status][team] = team_counts[rule_id][status].get(team, 0) + 1

        def _top5(d):
            items = sorted(d.items(), key=lambda item: (-item[1], item[0]))
            return [{"name": name, "count": count} for name, count in items[:5]]

        for rid in rule_ids:
            rule_rankings[rid]["leagues"]["green"] = _top5(league_counts[rid]["green"])
            rule_rankings[rid]["leagues"]["red"] = _top5(league_counts[rid]["red"])
            rule_rankings[rid]["teams"]["green"] = _top5(team_counts[rid]["green"])
            rule_rankings[rid]["teams"]["red"] = _top5(team_counts[rid]["red"])
    rules = sorted(
        rules,
        key=lambda rule: (
            -(rule_stats.get(rule.id, {}).get("green", 0) - rule_stats.get(rule.id, {}).get("red", 0)),
            -rule_stats.get(rule.id, {}).get("green", 0),
            rule_stats.get(rule.id, {}).get("red", 0),
            -rule.id,
        ),
    )
    return render_template(
        "rules/list.html",
        rules=rules,
        rule_stats=rule_stats,
        rule_alert_counts=rule_alert_counts,
        rule_rankings=rule_rankings,
        stage_filter=stage_filter,
        has_any_rules=has_any_rules,
    )




@rules_bp.route("/export", methods=["GET"])
@login_required
def export_rules():
    rules = Rule.query.filter_by(user_id=current_user.id).order_by(Rule.id.asc()).all()
    payload = {
        "version": 1,
        "exported_at": now_sp().isoformat(),
        "user": current_user.username,
        "rules": [_serialize_rule(rule) for rule in rules],
    }
    data = json.dumps(payload, ensure_ascii=False, indent=2)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"regras_{current_user.username}_{stamp}.json"
    return Response(
        data,
        mimetype="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@rules_bp.route("/import", methods=["POST"])
@login_required
def import_rules():
    file = request.files.get("rules_file")
    if not file or not file.filename:
        flash("Selecione um arquivo JSON para importar.", "warning")
        return redirect(url_for("rules.list_rules"))

    try:
        payload = json.loads(file.read().decode("utf-8-sig"))
    except Exception:
        flash("Arquivo invalido. Envie um JSON exportado pelo sistema.", "danger")
        return redirect(url_for("rules.list_rules"))

    rule_items = payload.get("rules") if isinstance(payload, dict) else None
    if not isinstance(rule_items, list):
        flash("Formato invalido: campo 'rules' nao encontrado.", "danger")
        return redirect(url_for("rules.list_rules"))

    imported = 0
    skipped = 0
    duplicated = 0
    existing_name_keys = {
        _name_key(rule.name)
        for rule in Rule.query.filter_by(user_id=current_user.id).all()
    }
    existing_signatures = {
        _rule_model_signature(rule)
        for rule in Rule.query.filter_by(user_id=current_user.id).all()
    }

    for item in rule_items:
        if not isinstance(item, dict):
            skipped += 1
            continue

        name = str(item.get("name", "")).strip()
        if not name:
            skipped += 1
            continue
        if _name_key(name) in existing_name_keys:
            duplicated += 1
            continue

        conditions_raw = item.get("conditions") or []
        conditions = [_deserialize_condition(c) for c in conditions_raw if isinstance(c, dict)]
        conditions = [c for c in conditions if c]
        if not conditions:
            skipped += 1
            continue

        outcome_conditions_raw = item.get("outcome_conditions") or []
        outcome_conditions = [_deserialize_outcome_condition(c) for c in outcome_conditions_raw if isinstance(c, dict)]
        outcome_conditions = [c for c in outcome_conditions if c]

        allowed_leagues_raw = item.get("allowed_leagues") or []
        allowed_leagues = []
        if isinstance(allowed_leagues_raw, list):
            for x in allowed_leagues_raw:
                league_name = _clean_league_name(x)
                if league_name:
                    allowed_leagues.append(league_name)

        rule_data = {
            "time_limit_min": _coerce_int(item.get("time_limit_min"), 90),
            "message_template": item.get("message_template") or None,
            "is_active": _coerce_bool(item.get("is_active"), True),
            "second_half_only": _coerce_bool(item.get("second_half_only"), False),
            "notify_telegram": _coerce_bool(item.get("notify_telegram"), True),
            "alert_on_penalty": _coerce_bool(item.get("alert_on_penalty"), False),
            "follow_ht": _coerce_bool(item.get("follow_ht"), True),
            "follow_ft": _coerce_bool(item.get("follow_ft"), True),
            "outcome_green_stage": str(item.get("outcome_green_stage") or "HT"),
            "outcome_red_stage": str(item.get("outcome_red_stage") or "HT"),
            "outcome_green_minute": _coerce_int(item.get("outcome_green_minute")),
            "outcome_red_minute": _coerce_int(item.get("outcome_red_minute")),
            "outcome_red_if_no_green": _coerce_bool(item.get("outcome_red_if_no_green"), False),
            "score_home": _coerce_int(item.get("score_home")),
            "score_away": _coerce_int(item.get("score_away")),
            "allowed_leagues_json": json.dumps(allowed_leagues, ensure_ascii=False) if allowed_leagues else None,
        }
        signature_data = dict(rule_data)
        signature_data.pop("allowed_leagues_json", None)
        signature = _rule_signature(
            name=name,
            allowed_leagues=allowed_leagues,
            conditions=conditions,
            outcome_conditions=outcome_conditions,
            **signature_data,
        )
        if signature in existing_signatures:
            duplicated += 1
            continue

        rule = Rule(
            user_id=current_user.id,
            name=name,
            **rule_data,
        )

        db.session.add(rule)
        db.session.flush()
        for cond in conditions:
            cond.rule_id = rule.id
            db.session.add(cond)
        for cond in outcome_conditions:
            cond.rule_id = rule.id
            db.session.add(cond)

        try:
            db.session.commit()
            imported += 1
            existing_name_keys.add(_name_key(name))
            existing_signatures.add(signature)
        except Exception:
            db.session.rollback()
            skipped += 1

    if imported:
        flash(f"Importacao concluida: {imported} regra(s) importada(s).", "success")
    if duplicated:
        flash(f"{duplicated} regra(s) duplicada(s) foram ignoradas.", "warning")
    if skipped:
        flash(f"{skipped} item(ns) foram ignorados por formato invalido.", "warning")
    if not imported and not skipped and not duplicated:
        flash("Nenhuma regra encontrada para importar.", "warning")
    return redirect(url_for("rules.list_rules"))


@rules_bp.route("/new", methods=["GET", "POST"])
@login_required
def create_rule():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        time_limit_raw = request.form.get("time_limit_min", "").strip()
        message_template = request.form.get("message_template", "").strip()
        is_active = bool(request.form.get("is_active"))
        second_half_only = bool(request.form.get("second_half_only"))
        notify_telegram = bool(request.form.get("notify_telegram"))
        alert_on_penalty = bool(request.form.get("alert_on_penalty"))
        follow_ht = bool(request.form.get("follow_ht"))
        follow_ft = bool(request.form.get("follow_ft"))
        outcome_green_stage = request.form.get("outcome_green_stage", "HT")
        outcome_red_stage = request.form.get("outcome_red_stage", "HT")
        outcome_green_minute_raw = request.form.get("outcome_green_minute", "").strip()
        outcome_red_minute_raw = request.form.get("outcome_red_minute", "").strip()
        outcome_red_if_no_green = bool(request.form.get("outcome_red_if_no_green"))
        score_home_raw = request.form.get("score_home", "").strip()
        score_away_raw = request.form.get("score_away", "").strip()
        allowed_leagues = _parse_allowed_leagues_json(request.form.get("allowed_leagues_json"))

        if not name:
            flash("Nome e obrigatorio.", "warning")
            return render_template(
                "rules/form.html",
                rule=None,
                available_leagues=_available_leagues(),
                **_build_form_context(request.form),
            )
        if notify_telegram and (not current_user.telegram_token or not current_user.telegram_chat_id or not current_user.telegram_verified):
            flash("Configure e teste o Telegram antes de criar regras que avisam.", "warning")
            return redirect(url_for("settings.settings"))
        time_limit_min = int(time_limit_raw) if time_limit_raw.isdigit() else 90
        outcome_green_minute = int(outcome_green_minute_raw) if outcome_green_minute_raw.isdigit() else None
        outcome_red_minute = int(outcome_red_minute_raw) if outcome_red_minute_raw.isdigit() else None
        score_home = int(score_home_raw) if score_home_raw.isdigit() else None
        score_away = int(score_away_raw) if score_away_raw.isdigit() else None

        conditions = _parse_conditions(request.form)
        if not conditions:
            flash("Adicione ao menos uma condicao.", "warning")
            return render_template(
                "rules/form.html",
                rule=None,
                available_leagues=_available_leagues(),
                **_build_form_context(request.form),
            )

        outcome_green = _parse_outcome_conditions(request.form, "outcome-green")
        outcome_red = _parse_outcome_conditions(request.form, "outcome-red")
        if outcome_red_if_no_green and not outcome_green:
            flash("Adicione ao menos uma condicao de GREEN para usar o RED por tempo.", "warning")
            return render_template(
                "rules/form.html",
                rule=None,
                available_leagues=_available_leagues(),
                **_build_form_context(request.form),
            )
        if outcome_red_if_no_green and outcome_red_minute is None:
            flash("Defina o minuto limite para virar RED quando o GREEN nao ocorrer.", "warning")
            return render_template(
                "rules/form.html",
                rule=None,
                available_leagues=_available_leagues(),
                **_build_form_context(request.form),
            )

        rule = Rule(
            user_id=current_user.id,
            name=name,
            time_limit_min=time_limit_min,
            message_template=message_template or None,
            is_active=is_active,
            second_half_only=second_half_only,
            notify_telegram=notify_telegram,
            alert_on_penalty=alert_on_penalty,
            follow_ht=follow_ht,
            follow_ft=follow_ft,
            outcome_green_stage=outcome_green_stage,
            outcome_red_stage=outcome_red_stage,
            outcome_green_minute=outcome_green_minute,
            outcome_red_minute=outcome_red_minute,
            outcome_red_if_no_green=outcome_red_if_no_green,
            score_home=score_home,
            score_away=score_away,
            allowed_leagues_json=json.dumps(allowed_leagues, ensure_ascii=False) if allowed_leagues else None,
        )
        db.session.add(rule)
        db.session.flush()

        for cond in conditions:
            cond.rule_id = rule.id
            db.session.add(cond)
        for cond in outcome_green + outcome_red:
            cond.rule_id = rule.id
            db.session.add(cond)
        db.session.commit()
        flash("Regra criada.", "success")
        return redirect(url_for("rules.list_rules"))
    return render_template(
        "rules/form.html",
        rule=None,
        available_leagues=_available_leagues(),
        allowed_leagues=[],
    )


@rules_bp.route("/<int:rule_id>/edit", methods=["GET", "POST"])
@login_required
def edit_rule(rule_id):
    rule = Rule.query.filter_by(id=rule_id, user_id=current_user.id).first_or_404()
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        time_limit_raw = request.form.get("time_limit_min", "").strip()
        message_template = request.form.get("message_template", "").strip()
        is_active = bool(request.form.get("is_active"))
        second_half_only = bool(request.form.get("second_half_only"))
        notify_telegram = bool(request.form.get("notify_telegram"))
        alert_on_penalty = bool(request.form.get("alert_on_penalty"))
        follow_ht = bool(request.form.get("follow_ht"))
        follow_ft = bool(request.form.get("follow_ft"))
        outcome_green_stage = request.form.get("outcome_green_stage", "HT")
        outcome_red_stage = request.form.get("outcome_red_stage", "HT")
        outcome_green_minute_raw = request.form.get("outcome_green_minute", "").strip()
        outcome_red_minute_raw = request.form.get("outcome_red_minute", "").strip()
        outcome_red_if_no_green = bool(request.form.get("outcome_red_if_no_green"))
        score_home_raw = request.form.get("score_home", "").strip()
        score_away_raw = request.form.get("score_away", "").strip()
        allowed_leagues = _parse_allowed_leagues_json(request.form.get("allowed_leagues_json"))

        if not name:
            flash("Nome e obrigatorio.", "warning")
            return render_template(
                "rules/form.html",
                rule=rule,
                available_leagues=_available_leagues(),
                **_build_form_context(request.form),
            )
        if notify_telegram and (not current_user.telegram_token or not current_user.telegram_chat_id or not current_user.telegram_verified):
            flash("Configure e teste o Telegram antes de ativar avisos.", "warning")
            return redirect(url_for("settings.settings"))

        time_limit_min = int(time_limit_raw) if time_limit_raw.isdigit() else rule.time_limit_min
        outcome_green_minute = int(outcome_green_minute_raw) if outcome_green_minute_raw.isdigit() else None
        outcome_red_minute = int(outcome_red_minute_raw) if outcome_red_minute_raw.isdigit() else None
        score_home = int(score_home_raw) if score_home_raw.isdigit() else None
        score_away = int(score_away_raw) if score_away_raw.isdigit() else None
        rule.name = name
        rule.time_limit_min = time_limit_min
        rule.message_template = message_template or None
        rule.is_active = is_active
        rule.second_half_only = second_half_only
        rule.notify_telegram = notify_telegram
        rule.alert_on_penalty = alert_on_penalty
        rule.follow_ht = follow_ht
        rule.follow_ft = follow_ft
        rule.outcome_green_stage = outcome_green_stage
        rule.outcome_red_stage = outcome_red_stage
        rule.outcome_green_minute = outcome_green_minute
        rule.outcome_red_minute = outcome_red_minute
        rule.outcome_red_if_no_green = outcome_red_if_no_green
        rule.score_home = score_home
        rule.score_away = score_away
        rule.allowed_leagues_json = json.dumps(allowed_leagues, ensure_ascii=False) if allowed_leagues else None

        conditions = _parse_conditions(request.form)
        if not conditions:
            flash("Adicione ao menos uma condicao.", "warning")
            db.session.rollback()
            return render_template(
                "rules/form.html",
                rule=rule,
                available_leagues=_available_leagues(),
                **_build_form_context(request.form),
            )
        outcome_green = _parse_outcome_conditions(request.form, "outcome-green")
        outcome_red = _parse_outcome_conditions(request.form, "outcome-red")
        if outcome_red_if_no_green and not outcome_green:
            flash("Adicione ao menos uma condicao de GREEN para usar o RED por tempo.", "warning")
            return render_template(
                "rules/form.html",
                rule=rule,
                available_leagues=_available_leagues(),
                **_build_form_context(request.form),
            )
        if outcome_red_if_no_green and outcome_red_minute is None:
            flash("Defina o minuto limite para virar RED quando o GREEN nao ocorrer.", "warning")
            return render_template(
                "rules/form.html",
                rule=rule,
                available_leagues=_available_leagues(),
                **_build_form_context(request.form),
            )

        RuleCondition.query.filter_by(rule_id=rule.id).delete()
        RuleOutcomeCondition.query.filter_by(rule_id=rule.id).delete()
        for cond in conditions:
            cond.rule_id = rule.id
            db.session.add(cond)
        for cond in outcome_green + outcome_red:
            cond.rule_id = rule.id
            db.session.add(cond)
        db.session.commit()
        flash("Regra atualizada.", "success")
        return redirect(url_for("rules.list_rules"))
    return render_template(
        "rules/form.html",
        rule=rule,
        available_leagues=_available_leagues(),
        **_build_rule_context(rule),
    )


@rules_bp.route("/<int:rule_id>/delete", methods=["POST"])
@login_required
def delete_rule(rule_id):
    rule = Rule.query.filter_by(id=rule_id, user_id=current_user.id).first_or_404()
    alert_count = MatchAlert.query.filter_by(rule_id=rule.id).count()
    if alert_count > 20 and not current_user.is_admin_user:
        password = request.form.get("confirm_password", "").strip()
        if not current_user.check_password(password):
            flash("Regra com muitos jogos no histórico. Informe sua senha para confirmar.", "warning")
            return redirect(url_for("rules.list_rules"))
    rule_snapshot = snapshot_rule(rule)
    undo_token = create_undo_action(
        user_id=current_user.id,
        action_type="delete_rule",
        payload={"rule": rule_snapshot},
    )
    db.session.delete(rule)
    db.session.commit()
    undo_url = url_for("main.undo_action", token=undo_token, next=url_for("rules.list_rules"))
    flash(
        Markup(f"Regra removida. <a class='alert-link' href='{escape(undo_url)}'>Desfazer</a>"),
        "success",
    )
    return redirect(url_for("rules.list_rules"))


@rules_bp.route("/<int:rule_id>/toggle", methods=["POST"])
@login_required
def toggle_rule(rule_id):
    rule = Rule.query.filter_by(id=rule_id, user_id=current_user.id).first_or_404()
    rule.is_active = not rule.is_active
    db.session.commit()
    return redirect(url_for("rules.list_rules"))


@rules_bp.route("/ai-hint", methods=["POST"])
@login_required
def ai_hint():
    payload = request.get_json(silent=True) or {}
    objective = str(payload.get("objective") or "").strip()
    if len(objective) < 4:
        return jsonify({"ok": False, "message": "Descreva o objetivo com mais detalhes."}), 400
    hint = _build_ai_hint(objective)
    return jsonify({"ok": True, "hint": hint})


@rules_bp.route("/test", methods=["POST"])
@login_required
def test_rule():
    def is_youth_match(payload):
        if not payload:
            return False
        hay = f"{payload.get('league', '')} {payload.get('home_team', '')} {payload.get('away_team', '')}".lower()
        return any(
            token in hay
            for token in (
                "u19",
                "u-19",
                "u 19",
                "sub19",
                "sub-19",
                "sub 19",
                "under 19",
                "u20",
                "u-20",
                "u 20",
                "sub20",
                "sub-20",
                "sub 20",
                "under 20",
            )
        )

    conditions = _parse_conditions(request.form)
    if not conditions:
        return jsonify({"ok": False, "message": "Adicione condicoes antes de testar."}), 400
    allowed_leagues = _parse_allowed_leagues_json(request.form.get("allowed_leagues_json"))
    allowed_leagues_set = {name.casefold() for name in allowed_leagues}
    temp_rule = Rule(
        user_id=current_user.id,
        name=request.form.get("name", "Regra teste"),
        time_limit_min=90,
    )
    temp_rule.second_half_only = bool(request.form.get("second_half_only"))
    temp_rule.alert_on_penalty = bool(request.form.get("alert_on_penalty"))
    score_home_raw = request.form.get("score_home", "").strip()
    score_away_raw = request.form.get("score_away", "").strip()
    temp_rule.score_home = int(score_home_raw) if score_home_raw.isdigit() else None
    temp_rule.score_away = int(score_away_raw) if score_away_raw.isdigit() else None
    temp_rule.conditions = conditions

    session = make_session()
    games, status_code = fetch_live_games(session)
    if status_code != 200:
        return jsonify({"ok": False, "message": f"API OFF (HTTP {status_code})"}), 503
    matches = []
    for game in games:
        stats_payload = fetch_match_stats(session, game["url"])
        if not stats_payload:
            continue
        if is_youth_match(stats_payload):
            continue
        if allowed_leagues_set:
            league_name = _clean_league_name(stats_payload.get("league"))
            if league_name.casefold() not in allowed_leagues_set:
                continue
        minute = stats_payload.get("minute") or game["minute"]
        home_score, away_score = parse_score(stats_payload.get("score", ""))
        if temp_rule.score_home is not None and home_score != temp_rule.score_home:
            continue
        if temp_rule.score_away is not None and away_score != temp_rule.score_away:
            continue
        stats_for_rule = stats_payload.get("stats", {})
        if temp_rule.second_half_only:
            if is_first_half_extra_time(stats_payload.get("time_text", "")):
                continue
            if (minute or 0) < 46:
                continue
            stats_for_rule = {
                key: value.copy() if isinstance(value, dict) else value
                for key, value in stats_for_rule.items()
            }
            if minute is not None:
                minute_2h = max(0, minute - 45)
                stats_for_rule["Minute"] = {"home": minute_2h, "away": minute_2h, "total": minute_2h}
        # alert_on_penalty nao deve bloquear a regra no teste
        if evaluate_rule(temp_rule, stats_for_rule):
            history_meta = {}
            try:
                history = fetch_match_history(session, game["url"])
                h2h_summary = summarize_history(history.get("h2h", []))
                home_summary = summarize_history(history.get("home", []))
                away_summary = summarize_history(history.get("away", []))
                h2h_items = history.get("h2h", [])
                history_meta = {
                    "history_h2h": format_history_summary("H2H", h2h_summary) if h2h_summary else "Sem historico de um contra o outro",
                    "history_home": format_history_summary("Home", home_summary),
                    "history_away": format_history_summary("Away", away_summary),
                }
                confidence = history_confidence(conditions, h2h_items)
                history_meta["history_confidence"] = f"{confidence}%" if confidence is not None else "Sem historico de um contra o outro"
            except Exception:
                history_meta = {}
            matches.append(
                {
                    "league": stats_payload.get("league"),
                    "home_team": stats_payload.get("home_team"),
                    "away_team": stats_payload.get("away_team"),
                    "minute": stats_payload.get("minute"),
                    "score": stats_payload.get("score"),
                    "url": game["url"],
                    **history_meta,
                }
            )
        if len(matches) >= 5:
            break
    return jsonify({"ok": True, "matches": matches})
