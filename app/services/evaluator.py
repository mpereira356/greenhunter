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
    if not rule.message_template:
        return default_msg
    try:
        return rule.message_template.format_map(_SafeDict(meta))
    except Exception:
        return default_msg


def stats_to_json(stats: dict) -> str:
    return json.dumps(stats, ensure_ascii=True)
