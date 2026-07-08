import argparse
import json
import os

os.environ.setdefault("DISABLE_WORKER", "1")

from app import create_app
from app.extensions import db
from app.models import MatchAlert
from app.services.worker import _exact_score_matches
from app.utils.time import now_sp


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rule-id", type=int, default=22)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    app = create_app()
    counts = {"green": 0, "red": 0, "unchanged": 0, "without_final_score": 0}
    with app.app_context():
        alerts = MatchAlert.query.filter_by(rule_id=args.rule_id).order_by(MatchAlert.id).all()
        for alert in alerts:
            final_score = (alert.ft_score or "").strip()
            if not final_score:
                counts["without_final_score"] += 1
                continue
            expected = "green" if _exact_score_matches(alert.rule, final_score, alert) else "red"
            counts[expected] += 1
            if alert.status == expected and alert.result_minute == 90 and alert.last_score == final_score:
                counts["unchanged"] += 1
                continue
            if args.apply:
                alert.status = expected
                alert.result_minute = 90
                alert.result_time_hhmm = now_sp().strftime("%H:%M")
                alert.ht_score = final_score
                alert.last_score = final_score
                alert.last_score_minute = 90
        if args.apply:
            db.session.commit()

    print(json.dumps(counts, ensure_ascii=False))


if __name__ == "__main__":
    main()
