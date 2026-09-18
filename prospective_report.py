#!/usr/bin/env python3
import argparse
import json
import os

os.environ.setdefault("DISABLE_WORKER", "1")

from app import create_app
from app.services.prospective_reporting import daily_report, report_for_period


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date")
    parser.add_argument("--days", type=int, choices=(7, 14, 30))
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()
    app = create_app()
    with app.app_context():
        report = daily_report(args.date) if args.date else report_for_period(None if args.all else (args.days or 7))
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
