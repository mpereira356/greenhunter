#!/usr/bin/env python3
"""Administrative, explicit rebuild of chronological B2 Elo snapshots."""

import json
import os

os.environ.setdefault("DISABLE_WORKER", "1")

from app import create_app
from app.services.strength import StrengthEngine


if __name__ == "__main__":
    application = create_app()
    with application.app_context():
        print(json.dumps(StrengthEngine().rebuild(), ensure_ascii=False, sort_keys=True))
