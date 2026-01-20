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


def render_message(rule, meta: dict) -> str:
    default_msg = (
        f"Alerta: {rule.name}\n"
        f"{meta.get('home_team')} vs {meta.get('away_team')}\n"
        f"Min: {meta.get('minute')} | Placar: {meta.get('score')}\n"
        f"{meta.get('url')}"
    )
    history_lines = []
    if meta.get("history_confidence"):
        history_lines.append(f"Conf: {meta.get('history_confidence')}")
    if meta.get("history_h2h"):
        history_lines.append(meta.get("history_h2h"))
    if meta.get("history_home"):
        history_lines.append(meta.get("history_home"))
    if meta.get("history_away"):
        history_lines.append(meta.get("history_away"))
    if history_lines:
        default_msg = f"{default_msg}\n" + "\n".join(history_lines)
    if not rule.message_template:
        return default_msg
    try:
        return rule.message_template.format_map(_SafeDict(meta))
    except Exception:
        return default_msg


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
