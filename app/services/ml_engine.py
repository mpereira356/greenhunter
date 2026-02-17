import json
import math
import os
import re
import threading
from datetime import timedelta

from app.models import MatchAlert
from app.utils.time import now_sp

MODEL_PATH = os.environ.get("ML_MODEL_PATH", "data/ml/model.json")
ML_MIN_SAMPLES = int(os.environ.get("ML_MIN_SAMPLES", "80"))
ML_TRAIN_INTERVAL_SECONDS = int(os.environ.get("ML_TRAIN_INTERVAL_SECONDS", "900"))
ML_MAX_TRAIN_SAMPLES = int(os.environ.get("ML_MAX_TRAIN_SAMPLES", "5000"))
ML_EPOCHS = int(os.environ.get("ML_EPOCHS", "180"))
ML_LEARNING_RATE = float(os.environ.get("ML_LEARNING_RATE", "0.08"))
ML_L2 = float(os.environ.get("ML_L2", "0.0005"))
ML_MAX_STAT_FEATURES = int(os.environ.get("ML_MAX_STAT_FEATURES", "240"))

_MODEL_LOCK = threading.Lock()
_MODEL_CACHE = None
_LAST_TRAIN_AT = None

MARKET_LABELS = {
    "goals": "Gols",
    "corners": "Escanteios",
    "yellow_cards": "Cartoes Amarelos",
    "red_cards": "Cartoes Vermelhos",
    "on_target": "Chutes no Alvo",
    "off_target": "Chutes para Fora",
    "dangerous_attacks": "Ataques Perigosos",
    "attacks": "Ataques",
    "possession": "Posse de Bola",
}


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


def _sigmoid(z: float) -> float:
    if z < -30:
        return 0.0
    if z > 30:
        return 1.0
    return 1.0 / (1.0 + math.exp(-z))


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value in (None, "", "-"):
            return default
        text = str(value).strip().replace("%", "")
        return float(text)
    except Exception:
        return default


def _safe_int(value, default: int = 0) -> int:
    return int(round(_safe_float(value, float(default))))


def _normalize_stat_key(raw: str) -> str:
    text = (raw or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def _normalize_operator(raw: str) -> str:
    text = (raw or "").strip()
    if text == "=":
        return "=="
    if text == "≥":
        return ">="
    if text == "≤":
        return "<="
    return text or ">="


def _scale_stat_value(value: float) -> float:
    if value <= 0:
        return 0.0
    # Compresses large ranges (e.g. dangerous attacks) while preserving small differences.
    return _clamp(math.log1p(value) / 6.0)


def _parse_score(score_text: str | None) -> tuple[int, int]:
    if not score_text:
        return 0, 0
    nums = re.findall(r"\d+", score_text)
    if len(nums) < 2:
        return 0, 0
    return int(nums[0]), int(nums[1])


def _build_delta_map(initial: dict | None, final: dict | None) -> dict[str, float]:
    base = _flatten_stats_map(initial)
    end = _flatten_stats_map(final)
    out = {}
    for key, end_val in end.items():
        start_val = _safe_float(base.get(key), 0.0)
        out[f"delta|{key}"] = max(0.0, _safe_float(end_val, 0.0) - start_val)
    return out


def _lookup_map_value(source: dict | None, key, default):
    if not isinstance(source, dict):
        return default
    if key in source:
        return source.get(key, default)
    key_s = str(key)
    if key_s in source:
        return source.get(key_s, default)
    return default


def _flatten_stats_map(stats: dict | None) -> dict[str, float]:
    if not isinstance(stats, dict):
        return {}
    flat = {}
    for key, value in stats.items():
        stat_key = _normalize_stat_key(str(key))
        if not stat_key:
            continue
        if isinstance(value, dict):
            for side, side_val in value.items():
                side_key = _normalize_stat_key(str(side))
                if not side_key:
                    continue
                flat[f"stat|{stat_key}|{side_key}"] = _safe_float(side_val, 0.0)
        else:
            flat[f"stat|{stat_key}|value"] = _safe_float(value, 0.0)
    return flat


def _format_target_text(label: str, side: str | None, operator: str | None, value: int | None) -> str:
    side_map = {"total": "total", "home": "casa", "away": "visitante"}
    side_txt = side_map.get((side or "").strip().lower(), "")
    op = (operator or "").strip() or ">="
    if value is None:
        return label
    if side_txt:
        return f"{label} ({side_txt}) {op} {value}"
    return f"{label} {op} {value}"


def infer_green_profile(rule) -> dict:
    if not rule:
        return {
            "outcome_signature": "goals|total|>=|*",
            "market_keys": ["goals"],
            "target_count": 1,
            "target_sides": ["total"],
            "target_operators": [">="],
            "market_key": "goals",
            "market_label": "Gols",
            "target_side": "total",
            "target_operator": ">=",
            "target_value": None,
            "target_text": "Gols",
        }

    green_conds = [c for c in (getattr(rule, "outcome_conditions", []) or []) if getattr(c, "outcome_type", "") == "green"]
    if not green_conds:
        return {
            "outcome_signature": "goals|total|>=|*",
            "market_keys": ["goals"],
            "target_count": 1,
            "target_sides": ["total"],
            "target_operators": [">="],
            "market_key": "goals",
            "market_label": "Gols",
            "target_side": "total",
            "target_operator": ">=",
            "target_value": None,
            "target_text": "Gols",
        }

    grouped = {}
    candidates = []  # for "dominant" market (backward-compatible display)
    market_keys = set()
    side_keys = set()
    operator_keys = set()
    for cond in green_conds:
        stat_raw = _normalize_stat_key(getattr(cond, "stat_key", ""))
        if not stat_raw or stat_raw in ("minute", "minuto", "minutos", "min"):
            continue
        gid = _safe_int(getattr(cond, "group_id", 0), 0)
        side = (_normalize_stat_key(getattr(cond, "side", "")) or "total")
        value = _safe_int(getattr(cond, "value", 0), 0)
        operator = _normalize_operator(getattr(cond, "operator", ""))
        grouped.setdefault(gid, []).append((stat_raw, side, operator, value))
        market_keys.add(stat_raw)
        side_keys.add(side)
        operator_keys.add(operator)
        # Prefer total-side and larger targets.
        score = (1 if side == "total" else 0, value)
        candidates.append((score, stat_raw, side, operator, value))

    if not grouped:
        return {
            "outcome_signature": "goals|total|>=|*",
            "market_keys": ["goals"],
            "target_count": 1,
            "target_sides": ["total"],
            "target_operators": [">="],
            "market_key": "goals",
            "market_label": "Gols",
            "target_side": "total",
            "target_operator": ">=",
            "target_value": None,
            "target_text": "Gols",
        }

    signature_groups = []
    text_groups = []
    for gid in sorted(grouped.keys()):
        conds = sorted(grouped[gid], key=lambda item: (item[0], item[1], item[2], item[3]))
        signature_groups.append(
            ",".join(f"{st}|{sd}|{op}|{val}" for st, sd, op, val in conds)
        )
        text_groups.append(
            " AND ".join(
                _format_target_text(MARKET_LABELS.get(st, st.replace("_", " ").title()), sd, op, val)
                for st, sd, op, val in conds
            )
        )

    outcome_signature = " OR ".join(signature_groups)
    target_text = " OR ".join(text_groups)

    if candidates:
        _, market_key, side, operator, value = max(candidates, key=lambda item: item[0])
    else:
        market_key, side, operator, value = "goals", "total", ">=", None

    label = MARKET_LABELS.get(market_key, market_key.replace("_", " ").title())
    return {
        "outcome_signature": outcome_signature,
        "market_keys": sorted(market_keys),
        "target_count": sum(len(v) for v in grouped.values()),
        "target_sides": sorted(side_keys),
        "target_operators": sorted(operator_keys),
        "market_key": market_key,
        "market_label": label,
        "target_side": side,
        "target_operator": operator,
        "target_value": value,
        "target_text": target_text or _format_target_text(label, side, operator, value),
    }


def infer_rule_market(rule) -> dict:
    # Backward-compatible alias used by older call sites.
    return infer_green_profile(rule)


def _win_rate_map(items):
    counts = {}
    greens = {}
    for key, is_green in items:
        if key in (None, "", 0):
            continue
        counts[key] = counts.get(key, 0) + 1
        if is_green:
            greens[key] = greens.get(key, 0) + 1
    rates = {}
    for key, total in counts.items():
        g = greens.get(key, 0)
        rates[key] = round((g / total), 6) if total else 0.0
    return rates, counts


def _extract_training_rows():
    alerts = (
        MatchAlert.query.filter(MatchAlert.status.in_(("green", "red")))
        .order_by(MatchAlert.created_at.desc())
        .limit(ML_MAX_TRAIN_SAMPLES)
        .all()
    )
    rows = []
    for alert in alerts:
        if not alert.initial_stats_json:
            continue
        try:
            stats = json.loads(alert.initial_stats_json)
        except Exception:
            continue
        if not isinstance(stats, dict):
            continue
        label = 1 if alert.status == "green" else 0
        h_score, a_score = _parse_score(alert.initial_score)
        final_stats = None
        if alert.ht_stats_json:
            try:
                parsed = json.loads(alert.ht_stats_json)
                if isinstance(parsed, dict):
                    final_stats = parsed
            except Exception:
                final_stats = None
        if final_stats is None and alert.ft_stats_json:
            try:
                parsed = json.loads(alert.ft_stats_json)
                if isinstance(parsed, dict):
                    final_stats = parsed
            except Exception:
                final_stats = None
        final_score_home, final_score_away = _parse_score(alert.ht_score or alert.ft_score or alert.last_score or alert.initial_score)
        result_minute = alert.result_minute if isinstance(alert.result_minute, int) else None
        alert_minute = alert.alert_minute if isinstance(alert.alert_minute, int) else 0
        elapsed_min = max(0, (result_minute - alert_minute)) if result_minute is not None else 0
        delta_flat = _build_delta_map(stats, final_stats)
        market_ctx = infer_green_profile(getattr(alert, "rule", None))
        rows.append(
            {
                "label": label,
                "minute": alert.alert_minute or 0,
                "result_minute": result_minute,
                "elapsed_min": elapsed_min,
                "rule_id": alert.rule_id,
                "league": alert.league or "",
                "score_home": h_score,
                "score_away": a_score,
                "score_final_home": final_score_home,
                "score_final_away": final_score_away,
                "stats": stats,
                "stats_flat": _flatten_stats_map(stats),
                "stats_final": final_stats if isinstance(final_stats, dict) else {},
                "delta_flat": delta_flat,
                "outcome_signature": market_ctx.get("outcome_signature"),
                "market_keys": market_ctx.get("market_keys") or [],
                "target_count": market_ctx.get("target_count"),
                "target_sides": market_ctx.get("target_sides") or [],
                "target_operators": market_ctx.get("target_operators") or [],
                "market_key": market_ctx.get("market_key"),
                "target_side": market_ctx.get("target_side"),
                "target_operator": market_ctx.get("target_operator"),
                "target_value": market_ctx.get("target_value"),
            }
        )
    return rows


def _compute_priors(rows: list[dict]) -> dict:
    total = len(rows)
    if total == 0:
        return {
            "global_rate": 0.5,
            "rule_rates": {},
            "league_rates": {},
            "rule_league_rates": {},
            "market_rates": {},
            "rule_market_rates": {},
            "signature_rates": {},
            "rule_signature_rates": {},
            "rule_counts": {},
            "league_counts": {},
            "rule_league_counts": {},
            "market_counts": {},
            "rule_market_counts": {},
            "signature_counts": {},
            "rule_signature_counts": {},
        }
    global_rate = sum(r.get("label", 0) for r in rows) / total
    rule_items = []
    league_items = []
    rule_league_items = []
    market_items = []
    rule_market_items = []
    signature_items = []
    rule_signature_items = []
    for r in rows:
        label = bool(r.get("label", 0))
        rule_id = r.get("rule_id")
        league = r.get("league") or ""
        market_key = r.get("market_key") or ""
        signature = r.get("outcome_signature") or ""
        rule_items.append((rule_id, label))
        league_items.append((league, label))
        if rule_id and league:
            rule_league_items.append((f"{rule_id}|{league}", label))
        if market_key:
            market_items.append((market_key, label))
        if rule_id and market_key:
            rule_market_items.append((f"{rule_id}|{market_key}", label))
        if signature:
            signature_items.append((signature, label))
        if rule_id and signature:
            rule_signature_items.append((f"{rule_id}|{signature}", label))

    rule_rates, rule_counts = _win_rate_map(rule_items)
    league_rates, league_counts = _win_rate_map(league_items)
    rule_league_rates, rule_league_counts = _win_rate_map(rule_league_items)
    market_rates, market_counts = _win_rate_map(market_items)
    rule_market_rates, rule_market_counts = _win_rate_map(rule_market_items)
    signature_rates, signature_counts = _win_rate_map(signature_items)
    rule_signature_rates, rule_signature_counts = _win_rate_map(rule_signature_items)
    evo_green_rows = [r for r in rows if r.get("label") == 1]
    evo_red_rows = [r for r in rows if r.get("label") == 0]

    def _avg(items, fn):
        vals = [fn(it) for it in items]
        vals = [v for v in vals if isinstance(v, (int, float))]
        return round((sum(vals) / len(vals)), 6) if vals else 0.0

    evo_global = {
        "green_elapsed_avg": _avg(evo_green_rows, lambda r: _safe_float(r.get("elapsed_min"), 0.0)),
        "red_elapsed_avg": _avg(evo_red_rows, lambda r: _safe_float(r.get("elapsed_min"), 0.0)),
        "green_goal_delta_avg": _avg(
            evo_green_rows,
            lambda r: max(
                0.0,
                (_safe_float(r.get("score_final_home"), 0.0) + _safe_float(r.get("score_final_away"), 0.0))
                - (_safe_float(r.get("score_home"), 0.0) + _safe_float(r.get("score_away"), 0.0)),
            ),
        ),
        "red_goal_delta_avg": _avg(
            evo_red_rows,
            lambda r: max(
                0.0,
                (_safe_float(r.get("score_final_home"), 0.0) + _safe_float(r.get("score_final_away"), 0.0))
                - (_safe_float(r.get("score_home"), 0.0) + _safe_float(r.get("score_away"), 0.0)),
            ),
        ),
        "green_on_target_delta_avg": _avg(evo_green_rows, lambda r: _safe_float((r.get("delta_flat") or {}).get("delta|stat|on_target|total"), 0.0)),
        "red_on_target_delta_avg": _avg(evo_red_rows, lambda r: _safe_float((r.get("delta_flat") or {}).get("delta|stat|on_target|total"), 0.0)),
        "green_dangerous_delta_avg": _avg(evo_green_rows, lambda r: _safe_float((r.get("delta_flat") or {}).get("delta|stat|dangerous_attacks|total"), 0.0)),
        "red_dangerous_delta_avg": _avg(evo_red_rows, lambda r: _safe_float((r.get("delta_flat") or {}).get("delta|stat|dangerous_attacks|total"), 0.0)),
        "green_corners_delta_avg": _avg(evo_green_rows, lambda r: _safe_float((r.get("delta_flat") or {}).get("delta|stat|corners|total"), 0.0)),
        "red_corners_delta_avg": _avg(evo_red_rows, lambda r: _safe_float((r.get("delta_flat") or {}).get("delta|stat|corners|total"), 0.0)),
    }

    rule_green_goal = {}
    rule_red_goal = {}
    rule_green_count = {}
    rule_red_count = {}
    for r in rows:
        rk = r.get("rule_id")
        if not rk:
            continue
        delta_goal = max(
            0.0,
            (_safe_float(r.get("score_final_home"), 0.0) + _safe_float(r.get("score_final_away"), 0.0))
            - (_safe_float(r.get("score_home"), 0.0) + _safe_float(r.get("score_away"), 0.0)),
        )
        if r.get("label") == 1:
            rule_green_goal[rk] = rule_green_goal.get(rk, 0.0) + delta_goal
            rule_green_count[rk] = rule_green_count.get(rk, 0) + 1
        else:
            rule_red_goal[rk] = rule_red_goal.get(rk, 0.0) + delta_goal
            rule_red_count[rk] = rule_red_count.get(rk, 0) + 1
    rule_evo = {
        "green_goal_delta_avg": {
            k: round(rule_green_goal[k] / max(1, rule_green_count.get(k, 1)), 6) for k in rule_green_goal
        },
        "red_goal_delta_avg": {
            k: round(rule_red_goal[k] / max(1, rule_red_count.get(k, 1)), 6) for k in rule_red_goal
        },
    }

    return {
        "global_rate": round(global_rate, 6),
        "rule_rates": rule_rates,
        "league_rates": league_rates,
        "rule_league_rates": rule_league_rates,
        "market_rates": market_rates,
        "rule_market_rates": rule_market_rates,
        "signature_rates": signature_rates,
        "rule_signature_rates": rule_signature_rates,
        "rule_counts": rule_counts,
        "league_counts": league_counts,
        "rule_league_counts": rule_league_counts,
        "market_counts": market_counts,
        "rule_market_counts": rule_market_counts,
        "signature_counts": signature_counts,
        "rule_signature_counts": rule_signature_counts,
        "evolution": evo_global,
        "rule_evolution": rule_evo,
    }


def _build_stat_vocab(rows: list[dict]) -> list[str]:
    freq = {}
    for row in rows:
        stats_flat = row.get("stats_flat") or {}
        for key, value in stats_flat.items():
            if value is None:
                continue
            if abs(float(value)) <= 0:
                continue
            freq[key] = freq.get(key, 0) + 1
    # Prefer features that appear more often for stability.
    ordered = sorted(freq.items(), key=lambda item: (-item[1], item[0]))
    return [k for k, _ in ordered[: max(1, ML_MAX_STAT_FEATURES)]]


def _to_features(row: dict, priors: dict, stat_vocab: list[str]) -> list[float]:
    minute = _safe_int(row.get("minute"), 0)
    score_home = _safe_int(row.get("score_home"), 0)
    score_away = _safe_int(row.get("score_away"), 0)
    score_total = score_home + score_away

    rule_key = row.get("rule_id")
    league_key = row.get("league") or ""
    rule_league_key = f"{rule_key}|{league_key}" if rule_key and league_key else ""
    market_key = (row.get("market_key") or "").strip()
    outcome_signature = (row.get("outcome_signature") or "").strip()
    market_keys = row.get("market_keys") or ([market_key] if market_key else [])
    target_sides = row.get("target_sides") or ([row.get("target_side")] if row.get("target_side") else [])
    target_operators = row.get("target_operators") or ([row.get("target_operator")] if row.get("target_operator") else [])
    target_side = (row.get("target_side") or "").strip()
    target_operator = _normalize_operator(row.get("target_operator") or "")
    target_value = _safe_float(row.get("target_value"), 0.0)
    target_count = _safe_float(row.get("target_count"), 1.0)
    rule_market_key = f"{rule_key}|{market_key}" if rule_key and market_key else ""
    rule_signature_key = f"{rule_key}|{outcome_signature}" if rule_key and outcome_signature else ""

    global_rate = priors.get("global_rate", 0.5)
    rule_rate = _lookup_map_value(priors.get("rule_rates", {}), rule_key, global_rate)
    league_rate = _lookup_map_value(priors.get("league_rates", {}), league_key, global_rate)
    rule_league_rate = _lookup_map_value(priors.get("rule_league_rates", {}), rule_league_key, global_rate)
    market_rate = _lookup_map_value(priors.get("market_rates", {}), market_key, global_rate)
    rule_market_rate = _lookup_map_value(priors.get("rule_market_rates", {}), rule_market_key, market_rate)
    signature_rate = _lookup_map_value(priors.get("signature_rates", {}), outcome_signature, market_rate)
    rule_signature_rate = _lookup_map_value(priors.get("rule_signature_rates", {}), rule_signature_key, signature_rate)

    evolution = priors.get("evolution", {}) if isinstance(priors, dict) else {}
    rule_evolution = priors.get("rule_evolution", {}) if isinstance(priors, dict) else {}
    evo_green_goal = _safe_float(_lookup_map_value(evolution, "green_goal_delta_avg", 0.0), 0.0)
    evo_red_goal = _safe_float(_lookup_map_value(evolution, "red_goal_delta_avg", 0.0), 0.0)
    evo_green_elapsed = _safe_float(_lookup_map_value(evolution, "green_elapsed_avg", 0.0), 0.0)
    evo_red_elapsed = _safe_float(_lookup_map_value(evolution, "red_elapsed_avg", 0.0), 0.0)
    evo_green_ot = _safe_float(_lookup_map_value(evolution, "green_on_target_delta_avg", 0.0), 0.0)
    evo_red_ot = _safe_float(_lookup_map_value(evolution, "red_on_target_delta_avg", 0.0), 0.0)
    evo_green_da = _safe_float(_lookup_map_value(evolution, "green_dangerous_delta_avg", 0.0), 0.0)
    evo_red_da = _safe_float(_lookup_map_value(evolution, "red_dangerous_delta_avg", 0.0), 0.0)
    evo_green_cr = _safe_float(_lookup_map_value(evolution, "green_corners_delta_avg", 0.0), 0.0)
    evo_red_cr = _safe_float(_lookup_map_value(evolution, "red_corners_delta_avg", 0.0), 0.0)

    rule_green_goal = _safe_float(
        _lookup_map_value(_lookup_map_value(rule_evolution, "green_goal_delta_avg", {}), rule_key, evo_green_goal),
        evo_green_goal,
    )
    rule_red_goal = _safe_float(
        _lookup_map_value(_lookup_map_value(rule_evolution, "red_goal_delta_avg", {}), rule_key, evo_red_goal),
        evo_red_goal,
    )

    base = [
        1.0,
        _clamp(minute / 120.0),
        _clamp(score_total / 8.0),
        _clamp(abs(score_home - score_away) / 6.0),
        _clamp(rule_rate),
        _clamp(league_rate),
        _clamp(rule_league_rate),
        _clamp(market_rate),
        _clamp(rule_market_rate),
        _clamp(signature_rate),
        _clamp(rule_signature_rate),
        _clamp(global_rate),
        _clamp(target_value / 20.0),
        _clamp(target_count / 8.0),
        1.0 if target_side == "total" else 0.0,
        1.0 if target_side == "home" else 0.0,
        1.0 if target_side == "away" else 0.0,
        1.0 if target_operator == ">=" else 0.0,
        1.0 if target_operator == "<=" else 0.0,
        1.0 if target_operator == "==" else 0.0,
        1.0 if "total" in target_sides else 0.0,
        1.0 if "home" in target_sides else 0.0,
        1.0 if "away" in target_sides else 0.0,
        1.0 if ">=" in target_operators else 0.0,
        1.0 if "<=" in target_operators else 0.0,
        1.0 if "==" in target_operators else 0.0,
        1.0 if "goals" in market_keys else 0.0,
        1.0 if "corners" in market_keys else 0.0,
        1.0 if "yellow_cards" in market_keys else 0.0,
        1.0 if "red_cards" in market_keys else 0.0,
        1.0 if "on_target" in market_keys else 0.0,
        1.0 if "dangerous_attacks" in market_keys else 0.0,
        1.0 if "off_target" in market_keys else 0.0,
        1.0 if "attacks" in market_keys else 0.0,
        1.0 if "possession" in market_keys else 0.0,
        _clamp(evo_green_goal / 3.0),
        _clamp(evo_red_goal / 3.0),
        _clamp(evo_green_elapsed / 45.0),
        _clamp(evo_red_elapsed / 45.0),
        _clamp(evo_green_ot / 8.0),
        _clamp(evo_red_ot / 8.0),
        _clamp(evo_green_da / 60.0),
        _clamp(evo_red_da / 60.0),
        _clamp(evo_green_cr / 10.0),
        _clamp(evo_red_cr / 10.0),
        _clamp(rule_green_goal / 3.0),
        _clamp(rule_red_goal / 3.0),
    ]

    stats_flat = row.get("stats_flat")
    if not isinstance(stats_flat, dict):
        stats_flat = _flatten_stats_map(row.get("stats"))

    dynamic = []
    for feature_key in stat_vocab:
        dynamic.append(_scale_stat_value(_safe_float(stats_flat.get(feature_key), 0.0)))
    return base + dynamic


def _train_logreg(rows: list[dict], priors: dict, stat_vocab: list[str]) -> list[float]:
    feature_size = len(_to_features(rows[0], priors, stat_vocab))
    weights = [0.0] * feature_size
    alpha = ML_LEARNING_RATE
    l2 = ML_L2
    epochs = max(1, ML_EPOCHS)
    for _ in range(epochs):
        for row in rows:
            x = _to_features(row, priors, stat_vocab)
            y = row["label"]
            z = sum(w * v for w, v in zip(weights, x))
            pred = _sigmoid(z)
            err = pred - y
            for i in range(feature_size):
                grad = (err * x[i]) + (l2 * weights[i] if i > 0 else 0.0)
                weights[i] -= alpha * grad
    return [round(w, 8) for w in weights]


def _build_model():
    rows = _extract_training_rows()
    if len(rows) < ML_MIN_SAMPLES:
        return None
    priors = _compute_priors(rows)
    stat_vocab = _build_stat_vocab(rows)
    weights = _train_logreg(rows, priors, stat_vocab)
    return {
        "kind": "logreg-v3-all-stats-with-outcome-evolution",
        "trained_at": now_sp().strftime("%Y-%m-%d %H:%M:%S"),
        "samples": len(rows),
        "weights": weights,
        "priors": priors,
        "stat_vocab": stat_vocab,
        "feature_count": len(weights),
    }


def _ensure_model_dir():
    folder = os.path.dirname(MODEL_PATH)
    if folder:
        os.makedirs(folder, exist_ok=True)


def _save_model(model: dict):
    _ensure_model_dir()
    with open(MODEL_PATH, "w", encoding="utf-8") as fh:
        json.dump(model, fh, ensure_ascii=False)


def _load_model_file():
    if not os.path.exists(MODEL_PATH):
        return None
    try:
        with open(MODEL_PATH, "r", encoding="utf-8") as fh:
            loaded = json.load(fh)
        if not isinstance(loaded, dict):
            return None
        return loaded
    except Exception:
        return None


def maybe_retrain_model(force: bool = False) -> bool:
    global _MODEL_CACHE, _LAST_TRAIN_AT
    with _MODEL_LOCK:
        now = now_sp()
        if not force and _LAST_TRAIN_AT and (now - _LAST_TRAIN_AT) < timedelta(seconds=ML_TRAIN_INTERVAL_SECONDS):
            return False
        model = _build_model()
        _LAST_TRAIN_AT = now
        if not model:
            return False
        _MODEL_CACHE = model
        try:
            _save_model(model)
        except Exception:
            pass
        return True


def _get_model():
    global _MODEL_CACHE
    with _MODEL_LOCK:
        if _MODEL_CACHE is not None:
            return _MODEL_CACHE
        loaded = _load_model_file()
        if loaded:
            _MODEL_CACHE = loaded
        return _MODEL_CACHE


def predict_alert_ml(
    rule_id: int | None,
    league: str | None,
    minute: int | None,
    stats: dict | None,
    score_text: str | None = None,
    outcome_signature: str | None = None,
    market_keys: list[str] | None = None,
    target_count: int | None = None,
    target_sides: list[str] | None = None,
    target_operators: list[str] | None = None,
    market_key: str | None = None,
    target_side: str | None = None,
    target_operator: str | None = None,
    target_value: int | None = None,
) -> dict | None:
    model = _get_model()
    if not model:
        return None
    weights = model.get("weights")
    priors = model.get("priors", {})
    stat_vocab = model.get("stat_vocab") or []
    if not isinstance(weights, list) or not weights:
        return None
    if not isinstance(stat_vocab, list):
        stat_vocab = []

    score_home, score_away = _parse_score(score_text)
    row = {
        "rule_id": rule_id,
        "league": league or "",
        "minute": minute or 0,
        "score_home": score_home,
        "score_away": score_away,
        "stats": stats if isinstance(stats, dict) else {},
        "stats_flat": _flatten_stats_map(stats if isinstance(stats, dict) else {}),
        "outcome_signature": (outcome_signature or "").strip(),
        "market_keys": [_normalize_stat_key(x) for x in (market_keys or []) if _normalize_stat_key(x)],
        "target_count": target_count if isinstance(target_count, int) else None,
        "target_sides": [_normalize_stat_key(x) for x in (target_sides or []) if _normalize_stat_key(x)],
        "target_operators": [_normalize_operator(x) for x in (target_operators or []) if _normalize_operator(x)],
        "market_key": _normalize_stat_key(market_key or ""),
        "target_side": _normalize_stat_key(target_side or ""),
        "target_operator": _normalize_operator(target_operator or ""),
        "target_value": target_value if isinstance(target_value, int) else None,
    }
    x = _to_features(row, priors, stat_vocab)
    if len(x) != len(weights):
        return None
    z = sum(float(w) * float(v) for w, v in zip(weights, x))
    prob_green = _sigmoid(z)
    score = int(round(prob_green * 100))
    if score >= 64:
        verdict = "Boa entrada"
    elif score >= 50:
        verdict = "Entrada com cautela"
    else:
        verdict = "Melhor evitar"
    return {
        "score": score,
        "verdict": verdict,
        "prob_green": round(prob_green, 4),
        "samples": int(model.get("samples") or 0),
        "trained_at": model.get("trained_at"),
        "feature_count": int(model.get("feature_count") or len(weights)),
    }
