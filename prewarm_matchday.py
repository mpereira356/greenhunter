#!/usr/bin/env python3
import argparse
import fcntl
import os
import time
from datetime import timedelta

os.environ["DISABLE_WORKER"] = "1"

from app import create_app
from app.services.matchday import build_matchday_trend_index
from app.utils.time import now_sp


def main():
    parser = argparse.ArgumentParser(description="Prepara o índice compartilhado de tendências dos Jogos do Dia.")
    parser.add_argument("--days", type=int, default=2, help="Quantidade de dias a partir de hoje.")
    parser.add_argument("--resume", action="store_true", help="Preserva resultados já coletados e tenta apenas os ausentes.")
    args = parser.parse_args()

    lock_path = "/tmp/greenhunter-matchday-prewarm.lock"
    lock = open(lock_path, "w", encoding="utf-8")
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("[matchday_prewarm] outra execução já está ativa", flush=True)
        return 0

    app = create_app()
    with app.app_context():
        for offset in range(max(1, args.days)):
            day = (now_sp() + timedelta(days=offset)).strftime("%Y-%m-%d")
            print(f"[matchday_prewarm] iniciando índice leve de {day}", flush=True)

            def progress(done, total, failures):
                print(f"[matchday_prewarm] {day}: {done}/{total}, falhas={failures}", flush=True)

            payload = build_matchday_trend_index(
                day,
                force_refresh=not args.resume,
                progress_callback=progress,
            )
            for _ in range(2):
                if payload.get("complete"):
                    break
                print(f"[matchday_prewarm] {day}: repetindo somente itens ausentes", flush=True)
                time.sleep(30)
                payload = build_matchday_trend_index(
                    day,
                    force_refresh=False,
                    progress_callback=progress,
                )
            print(
                f"[matchday_prewarm] {day}: completo={payload.get('complete')} "
                f"entradas={len(payload.get('entries') or {})}/{payload.get('total')} "
                f"falhas={payload.get('failures')}",
                flush=True,
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
