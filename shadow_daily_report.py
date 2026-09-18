#!/usr/bin/env python3
import argparse
import json
import os

os.environ.setdefault("DISABLE_WORKER", "1")

from app import create_app
from app.services.shadow_settlement import daily_shadow_report, settle_prospective_predictions


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("date")
    parser.add_argument("--settle", action="store_true")
    args = parser.parse_args()
    application = create_app()
    with application.app_context():
        if args.settle:
            settle_prospective_predictions(args.date)
        print(json.dumps(daily_shadow_report(args.date), ensure_ascii=False, indent=2))
