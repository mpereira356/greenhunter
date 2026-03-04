import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Backtest em modo sombra para regra IA (sem alterar o fluxo atual)."
    )
    parser.add_argument("--db", default="app.db", help="Caminho para o sqlite (padrao: app.db)")
    parser.add_argument("--from-date", default=None, help="Data inicial YYYY-MM-DD")
    parser.add_argument("--to-date", default=None, help="Data final YYYY-MM-DD")
    parser.add_argument("--rule-id", type=int, default=None, help="Filtrar por regra especifica")
    parser.add_argument("--league-contains", default=None, help="Filtrar por trecho do nome da liga")
    parser.add_argument("--min-minute", type=int, default=1, help="Minuto minimo para entrada")
    parser.add_argument("--max-minute", type=int, default=90, help="Minuto maximo para entrada")
    parser.add_argument("--min-ai-score", type=int, default=60, help="Score minimo da IA heuristica (0-100)")
    parser.add_argument(
        "--min-ml-score",
        type=int,
        default=-1,
        help="Score minimo do ML (0-100). Use -1 para ignorar.",
    )
    parser.add_argument(
        "--min-ml-prob",
        type=float,
        default=-1.0,
        help="Probabilidade minima do ML (0-1). Use -1 para ignorar.",
    )
    parser.add_argument(
        "--min-on-target-total",
        type=int,
        default=0,
        help="Minimo de chutes no alvo (total) no momento do alerta",
    )
    parser.add_argument(
        "--min-dangerous-total",
        type=int,
        default=0,
        help="Minimo de ataques perigosos (total) no momento do alerta",
    )
    return parser.parse_args()


def parse_iso_dt(raw):
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d")
    except ValueError:
        return None


def parse_alert_dt(raw):
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def read_stat_total(stats_json, stat_name):
    if not stats_json:
        return 0
    try:
        payload = json.loads(stats_json)
    except Exception:
        return 0
    if not isinstance(payload, dict):
        return 0
    bucket = payload.get(stat_name)
    if not isinstance(bucket, dict):
        return 0
    value = bucket.get("total", 0)
    try:
        return int(value)
    except Exception:
        return 0


def parse_score(score_text):
    if not score_text:
        return 0, 0
    nums = []
    for ch in str(score_text):
        if ch.isdigit():
            nums.append(ch)
    # fallback robusto para formatos com separadores estranhos
    parts = [p for p in str(score_text).replace("-", "x").split("x") if p.strip().isdigit()]
    if len(parts) >= 2:
        return int(parts[0]), int(parts[1])
    if len(nums) >= 2:
        return int(nums[0]), int(nums[1])
    return 0, 0


def _parse_pct(value):
    if value in (None, "", "-", "N/A"):
        return None
    digits = []
    for ch in str(value):
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


def build_ai_assessment(meta):
    minute = int(meta.get("minute") or 0)
    goals_total = int(meta.get("goals_total") or 0)
    on_target_total = int(meta.get("on_target_total") or 0)
    dangerous_total = int(meta.get("dangerous_attacks_total") or 0)
    corners_total = int(meta.get("corners_total") or 0)

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

    if minute <= 47 and goals_total == 0:
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
    return {"score": int(final_score)}


def build_ai_score(row):
    stats_json = row["initial_stats_json"]
    try:
        stats = json.loads(stats_json) if stats_json else {}
    except Exception:
        stats = {}
    if not isinstance(stats, dict):
        stats = {}

    goals_home, goals_away = parse_score(row["initial_score"])
    goals_total = goals_home + goals_away
    minute = row["alert_minute"] or 0
    on_target_home = read_stat_total(stats_json, "On Target")
    on_target_away = 0
    if isinstance(stats.get("On Target"), dict):
        try:
            on_target_home = int(stats["On Target"].get("home", 0) or 0)
            on_target_away = int(stats["On Target"].get("away", 0) or 0)
        except Exception:
            pass
    dangerous_home = 0
    dangerous_away = 0
    if isinstance(stats.get("Dangerous Attacks"), dict):
        try:
            dangerous_home = int(stats["Dangerous Attacks"].get("home", 0) or 0)
            dangerous_away = int(stats["Dangerous Attacks"].get("away", 0) or 0)
        except Exception:
            pass

    meta = {
        "minute": minute,
        "goals_home": goals_home,
        "goals_away": goals_away,
        "goals_total": goals_total,
        "on_target_home": on_target_home,
        "on_target_away": on_target_away,
        "on_target_total": read_stat_total(stats_json, "On Target"),
        "dangerous_attacks_home": dangerous_home,
        "dangerous_attacks_away": dangerous_away,
        "dangerous_attacks_total": read_stat_total(stats_json, "Dangerous Attacks"),
        "corners_total": read_stat_total(stats_json, "Corners"),
        "rule_confidence": None,
        "league_rule_confidence": None,
        "history_confidence": None,
    }
    out = build_ai_assessment(meta)
    return int(out.get("score", 0) or 0)


def passes_shadow_rule(row, args):
    minute = row["alert_minute"] or 0
    ai_score = row["ai_score"] or 0
    ml_score = row["ml_pred_score"]
    ml_prob = row["ml_pred_prob_green"]
    if minute < args.min_minute or minute > args.max_minute:
        return False
    if int(ai_score) < args.min_ai_score:
        return False
    if args.min_ml_score >= 0:
        if ml_score is None or int(ml_score) < args.min_ml_score:
            return False
    if args.min_ml_prob >= 0:
        if ml_prob is None or float(ml_prob) < args.min_ml_prob:
            return False

    on_target_total = read_stat_total(row["initial_stats_json"], "On Target")
    dangerous_total = read_stat_total(row["initial_stats_json"], "Dangerous Attacks")
    if on_target_total < args.min_on_target_total:
        return False
    if dangerous_total < args.min_dangerous_total:
        return False
    return True


def safe_pct(num, den):
    if den <= 0:
        return 0.0
    return round((num / den) * 100.0, 2)


def print_summary(title, total, greens, reds):
    wr = safe_pct(greens, greens + reds)
    print(f"\n{title}")
    print("-" * len(title))
    print(f"Entradas: {total}")
    print(f"Greens:  {greens}")
    print(f"Reds:    {reds}")
    print(f"WinRate: {wr}%")


def main():
    args = parse_args()
    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"Banco nao encontrado: {db_path}")

    from_dt = parse_iso_dt(args.from_date)
    to_dt = parse_iso_dt(args.to_date)
    if args.from_date and from_dt is None:
        raise SystemExit("Formato invalido em --from-date. Use YYYY-MM-DD.")
    if args.to_date and to_dt is None:
        raise SystemExit("Formato invalido em --to-date. Use YYYY-MM-DD.")
    if to_dt is not None:
        to_dt = to_dt.replace(hour=23, minute=59, second=59)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    table_cols = {r[1] for r in cur.execute("PRAGMA table_info(match_alert)").fetchall()}
    has_ml_score = "ml_pred_score" in table_cols
    has_ml_prob = "ml_pred_prob_green" in table_cols

    query = """
        SELECT
            id,
            rule_id,
            league,
            alert_minute,
            created_at,
            status,
            initial_stats_json,
            initial_score,
            (SELECT name FROM rule WHERE rule.id = match_alert.rule_id) AS rule_name,
            (SELECT message_template FROM rule WHERE rule.id = match_alert.rule_id) AS rule_template,
            {ml_score_expr} AS ml_pred_score,
            {ml_prob_expr} AS ml_pred_prob_green
        FROM match_alert
        WHERE status IN ('green', 'red')
    """.format(
        ml_score_expr="ml_pred_score" if has_ml_score else "NULL",
        ml_prob_expr="ml_pred_prob_green" if has_ml_prob else "NULL",
    )
    params = []
    if args.rule_id is not None:
        query += " AND rule_id = ?"
        params.append(args.rule_id)
    if args.league_contains:
        query += " AND lower(coalesce(league, '')) LIKE ?"
        params.append(f"%{args.league_contains.lower()}%")
    query += " ORDER BY created_at ASC"

    rows = cur.execute(query, params).fetchall()
    conn.close()

    filtered = []
    for row in rows:
        dt = parse_alert_dt(row["created_at"])
        if from_dt and (dt is None or dt < from_dt):
            continue
        if to_dt and (dt is None or dt > to_dt):
            continue
        row_dict = dict(row)
        row_dict["ai_score"] = build_ai_score(row_dict)
        filtered.append(row_dict)

    if not filtered:
        raise SystemExit("Nenhum alerta encontrado com os filtros informados.")

    baseline_total = len(filtered)
    baseline_greens = sum(1 for r in filtered if r["status"] == "green")
    baseline_reds = baseline_total - baseline_greens
    print_summary("BASELINE (historico filtrado)", baseline_total, baseline_greens, baseline_reds)

    selected = [r for r in filtered if passes_shadow_rule(r, args)]
    selected_total = len(selected)
    selected_greens = sum(1 for r in selected if r["status"] == "green")
    selected_reds = selected_total - selected_greens
    print_summary("REGRA IA SOMBRA (thresholds atuais)", selected_total, selected_greens, selected_reds)

    base_wr = safe_pct(baseline_greens, baseline_greens + baseline_reds)
    rule_wr = safe_pct(selected_greens, selected_greens + selected_reds)
    lift = round(rule_wr - base_wr, 2)

    print("\nComparativo")
    print("-----------")
    print(f"WinRate baseline: {base_wr}%")
    print(f"WinRate regra IA: {rule_wr}%")
    print(f"Lift: {lift:+.2f} p.p.")
    print(f"Cobertura: {safe_pct(selected_total, baseline_total)}% dos alertas filtrados")

    by_rule = Counter()
    by_rule_green = Counter()
    by_day = defaultdict(lambda: {"g": 0, "r": 0, "n": 0})

    for r in selected:
        rid = r["rule_id"]
        by_rule[rid] += 1
        if r["status"] == "green":
            by_rule_green[rid] += 1
        dt = parse_alert_dt(r["created_at"])
        key = dt.strftime("%Y-%m-%d") if dt else "sem-data"
        by_day[key]["n"] += 1
        if r["status"] == "green":
            by_day[key]["g"] += 1
        else:
            by_day[key]["r"] += 1

    print("\nTop regras na selecao")
    print("---------------------")
    if not by_rule:
        print("Nenhuma entrada selecionada.")
    else:
        for rid, n in by_rule.most_common(10):
            g = by_rule_green[rid]
            wr = safe_pct(g, n)
            print(f"rule_id={rid}: entradas={n}, greens={g}, wr={wr}%")

    print("\nResumo diario da selecao")
    print("------------------------")
    days = sorted(by_day.keys())
    if not days:
        print("Sem entradas.")
    else:
        for day in days:
            item = by_day[day]
            wr = safe_pct(item["g"], item["n"])
            print(f"{day}: entradas={item['n']} | greens={item['g']} | reds={item['r']} | wr={wr}%")

    print("\nThresholds usados")
    print("-----------------")
    print(f"min_minute={args.min_minute}")
    print(f"max_minute={args.max_minute}")
    print(f"min_ai_score={args.min_ai_score}")
    print(f"min_ml_score={args.min_ml_score}")
    print(f"min_ml_prob={args.min_ml_prob}")
    print(f"min_on_target_total={args.min_on_target_total}")
    print(f"min_dangerous_total={args.min_dangerous_total}")
    print(f"ml_colunas_no_banco={'sim' if (has_ml_score and has_ml_prob) else 'nao'}")


if __name__ == "__main__":
    main()
