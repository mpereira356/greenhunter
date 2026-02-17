import json

from .scraper import normalize_stat_key


def compare(op: str, left: int, right: int) -> bool:
    if op == ">=":
        return left >= right
    if op == ">":
        return left > right
    if op == "==":
        return left == right
    if op == "<=":
        return left <= right
    if op == "<":
        return left < right
    return False


def evaluate_conditions(conditions, stats: dict) -> bool:
    if not conditions:
        return False
    for cond in conditions:
        key = normalize_stat_key(cond.stat_key)
        if key not in stats:
            return False
        side_values = stats[key]
        if cond.side not in side_values:
            return False
        value = side_values[cond.side]
        if value is None:
            return False
        if not compare(cond.operator, value, cond.value):
            return False
    return True


def evaluate_rule(rule, stats: dict) -> bool:
    if not rule.conditions:
        return False
    groups = {}
    for cond in rule.conditions:
        gid = cond.group_id if cond.group_id is not None else 0
        groups.setdefault(gid, []).append(cond)
    for conds in groups.values():
        if evaluate_conditions(conds, stats):
            return True
    return False


class _SafeDict(dict):
    def __missing__(self, key):
        return ""


def _to_int(value, default=0):
    try:
        if value in (None, "", "-"):
            return default
        return int(value)
    except Exception:
        return default


def _parse_pct(value) -> int | None:
    if value in (None, "", "-", "N/A"):
        return None
    text = str(value)
    digits = []
    for ch in text:
        if ch.isdigit():
            digits.append(ch)
        elif ch == "%":
            break
    if not digits:
        return None
    try:
        return int("".join(digits))
    except Exception:
        return None


def _build_ai_assessment(rule, meta: dict) -> dict:
    minute = _to_int(meta.get("minute"), 0)
    goals_total = _to_int(meta.get("goals_total"), 0)
    on_target_total = _to_int(meta.get("on_target_total"), 0)
    dangerous_total = _to_int(meta.get("dangerous_attacks_total"), 0)
    corners_total = _to_int(meta.get("corners_total"), 0)

    live_score = 0
    if on_target_total >= 6:
        live_score += 20
    elif on_target_total >= 4:
        live_score += 12
    elif on_target_total >= 2:
        live_score += 6
    else:
        live_score -= 8

    if dangerous_total >= 80:
        live_score += 16
    elif dangerous_total >= 50:
        live_score += 10
    elif dangerous_total >= 30:
        live_score += 5
    else:
        live_score -= 6

    if corners_total >= 8:
        live_score += 6
    elif corners_total >= 5:
        live_score += 3

    rule_name = (getattr(rule, "name", "") or "").lower()
    rule_template = (getattr(rule, "message_template", "") or "").lower()
    ht_tokens = (" ht", "1o tempo", "1 tempo", "1st half", "half time", "intervalo")
    has_ht_hint = any(token in rule_name or token in rule_template for token in ht_tokens)
    is_ht_context = minute <= 45 or (has_ht_hint and minute <= 47)

    # If market is first-half oriented but window already passed, strongly discourage entry.
    if has_ht_hint and minute > 47:
        live_score -= 28

    if is_ht_context and goals_total == 0:
        if minute >= 40 and on_target_total >= 4 and dangerous_total >= 40:
            live_score += 8
        elif minute >= 40 and on_target_total <= 1 and dangerous_total <= 20:
            live_score -= 12

    hist_score = 0
    rule_conf = _parse_pct(meta.get("rule_confidence"))
    if rule_conf is not None:
        if rule_conf >= 65:
            hist_score += 18
        elif rule_conf >= 58:
            hist_score += 10
        elif rule_conf >= 52:
            hist_score += 4
        elif rule_conf < 45:
            hist_score -= 12

    league_conf = _parse_pct(meta.get("league_rule_confidence"))
    if league_conf is not None:
        if league_conf >= 62:
            hist_score += 12
        elif league_conf >= 55:
            hist_score += 7
        elif league_conf < 45:
            hist_score -= 8

    h2h_conf = _parse_pct(meta.get("history_confidence"))
    if h2h_conf is not None:
        if h2h_conf >= 60:
            hist_score += 8
        elif h2h_conf < 40:
            hist_score -= 6

    raw_score = 50 + live_score + hist_score
    final_score = max(0, min(100, raw_score))

    if final_score >= 68:
        verdict = "Boa entrada"
    elif final_score >= 52:
        verdict = "Entrada com cautela"
    else:
        verdict = "Melhor evitar"

    return {"score": final_score, "verdict": verdict, "is_ht_context": is_ht_context}


def _build_ai_commentary(rule, meta: dict) -> str:
    minute = _to_int(meta.get("minute"), 0)
    goals_total = _to_int(meta.get("goals_total"), 0)
    goals_home = _to_int(meta.get("goals_home"), 0)
    goals_away = _to_int(meta.get("goals_away"), 0)
    on_target_home = _to_int(meta.get("on_target_home"), 0)
    on_target_away = _to_int(meta.get("on_target_away"), 0)
    on_target_total = _to_int(meta.get("on_target_total"), 0)
    corners_home = _to_int(meta.get("corners_home"), 0)
    corners_away = _to_int(meta.get("corners_away"), 0)
    dangerous_home = _to_int(meta.get("dangerous_attacks_home"), 0)
    dangerous_away = _to_int(meta.get("dangerous_attacks_away"), 0)
    dangerous_total = _to_int(meta.get("dangerous_attacks_total"), 0)

    home_pressure = (on_target_home * 3) + corners_home + (dangerous_home // 3)
    away_pressure = (on_target_away * 3) + corners_away + (dangerous_away // 3)
    pressure_diff = home_pressure - away_pressure

    if pressure_diff >= 4:
        team_trend = "Mandante pressiona mais e cria volume acima do visitante."
    elif pressure_diff <= -4:
        team_trend = "Visitante pressiona mais e gera mais chances no momento."
    else:
        team_trend = "Partida equilibrada entre as duas equipes ate aqui."

    if on_target_total >= 6 or dangerous_total >= 70:
        pace = "Ritmo ofensivo alto."
    elif on_target_total <= 1 and dangerous_total <= 20:
        pace = "Ritmo ofensivo baixo."
    else:
        pace = "Ritmo ofensivo moderado."

    assessment = _build_ai_assessment(rule, meta)
    is_ht_context = bool(assessment.get("is_ht_context"))

    if is_ht_context:
        remaining = max(0, 45 - minute)
        if goals_total == 0:
            if on_target_total >= 4 and dangerous_total >= 40:
                market_note = f"Mercado gol HT segue vivo com pressao dos dois lados ({remaining} min para o intervalo)."
            elif on_target_total <= 1 and dangerous_total <= 20:
                market_note = f"Cenario fraco para gol HT ate agora ({remaining} min para o intervalo)."
            else:
                market_note = f"Cenario misto para gol HT ({remaining} min para o intervalo)."
        else:
            market_note = f"Ja houve gol no 1o tempo ({goals_home}x{goals_away}); monitorar continuidade ofensiva."
    else:
        if goals_total == 0 and on_target_total >= 4:
            market_note = "Pressao ofensiva relevante para buscar gol no decorrer do jogo."
        elif goals_total >= 2 and on_target_total >= 5:
            market_note = "Jogo aberto, com indicadores de mais eventos ofensivos."
        else:
            market_note = "Leitura de jogo neutra para o momento."

    return f"{pace} {team_trend} {market_note}"


def render_message(rule, meta: dict) -> str:
    meta_safe = dict(meta or {})
    assessment = _build_ai_assessment(rule, meta_safe)
    meta_safe["ai_score"] = assessment.get("score")
    meta_safe["ai_verdict"] = assessment.get("verdict")
    if not meta_safe.get("ai_commentary"):
        meta_safe["ai_commentary"] = _build_ai_commentary(rule, meta_safe)
    for key in (
        "rule_confidence",
        "history_confidence",
        "league_rule_confidence",
        "ml_verdict",
        "ml_score",
        "ml_prob_green",
        "ml_samples",
        "ml_feature_count",
        "ml_trained_at",
        "green_market_label",
        "green_market_target",
    ):
        if key not in meta_safe or meta_safe.get(key) in (None, "", "None"):
            meta_safe[key] = "N/A"

    default_msg = (
        f"Alerta: {rule.name}\n"
        f"{meta_safe.get('home_team')} vs {meta_safe.get('away_team')}\n"
        f"Min: {meta_safe.get('minute')} | Placar: {meta_safe.get('score')}\n"
        f"{meta_safe.get('url')}"
    )
    history_lines = []
    if meta_safe.get("rule_confidence"):
        history_lines.append(f"Conf regra: {meta_safe.get('rule_confidence')}")
    if meta_safe.get("history_confidence"):
        history_lines.append(f"Conf: {meta_safe.get('history_confidence')}")
    if meta_safe.get("league_rule_confidence"):
        history_lines.append(f"Conf liga: {meta_safe.get('league_rule_confidence')}")
    if meta_safe.get("history_h2h"):
        history_lines.append(meta_safe.get("history_h2h"))
    if meta_safe.get("history_home"):
        history_lines.append(meta_safe.get("history_home"))
    if meta_safe.get("history_away"):
        history_lines.append(meta_safe.get("history_away"))
    if history_lines:
        default_msg = f"{default_msg}\n" + "\n".join(history_lines)

    rendered = default_msg
    if rule.message_template:
        try:
            rendered = rule.message_template.format_map(_SafeDict(meta_safe))
        except Exception:
            rendered = default_msg

    if meta_safe.get("ai_commentary"):
        template_has_ai = bool(
            rule.message_template and any(
                marker in rule.message_template
                for marker in (
                    "{ai_commentary}",
                    "{ai_verdict}",
                    "{ai_score}",
                    "{ml_verdict}",
                    "{ml_score}",
                    "{ml_prob_green}",
                    "{ml_samples}",
                    "{ml_feature_count}",
                    "{ml_trained_at}",
                    "{green_market_label}",
                    "{green_market_target}",
                )
            )
        )
        if not template_has_ai:
            market_label = meta_safe.get("green_market_label") or "N/A"
            market_target = meta_safe.get("green_market_target") or "N/A"
            rendered = (
                f"{rendered}\n"
                f"Analise IA e ML\n"
                f"IA: {meta_safe.get('ai_commentary')}\n"
                f"Veredito IA: {meta_safe.get('ai_verdict')} ({meta_safe.get('ai_score')}/100)\n"
                f"Mercado alvo: {market_label} | {market_target}\n"
                f"Veredito ML: {meta_safe.get('ml_verdict')} ({meta_safe.get('ml_score')}/100)\n"
                f"Probabilidade ML (green): {meta_safe.get('ml_prob_green')}\n"
                f"Amostras ML: {meta_safe.get('ml_samples')}\n"
                f"Variaveis ML: {meta_safe.get('ml_feature_count')}\n"
                f"Ultimo treino ML: {meta_safe.get('ml_trained_at')}"
            )

    return rendered


def stats_to_json(stats: dict) -> str:
    return json.dumps(stats, ensure_ascii=True)


def _get_cond_attr(cond, name: str):
    if isinstance(cond, dict):
        return cond.get(name)
    return getattr(cond, name, None)


def history_confidence(conditions, history_items):
    if not conditions or not history_items:
        return None
    goals_conds = []
    for cond in conditions:
        stat_key = _get_cond_attr(cond, "stat_key")
        side = _get_cond_attr(cond, "side")
        operator = _get_cond_attr(cond, "operator")
        value = _get_cond_attr(cond, "value")
        if not stat_key or not side or operator is None or value is None:
            return None
        if normalize_stat_key(stat_key) != "Goals":
            return None
        if side not in ("home", "away", "total"):
            return None
        goals_conds.append((side, operator, int(value)))

    total = len(history_items)
    if total == 0:
        return None
    hits = 0
    for item in history_items:
        stats = {
            "home": item.get("home", 0),
            "away": item.get("away", 0),
            "total": item.get("total", 0),
        }
        if all(compare(op, stats[side], val) for side, op, val in goals_conds):
            hits += 1
    return round((hits / total) * 100)
