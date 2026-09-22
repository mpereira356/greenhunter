#!/usr/bin/env python3
import json
from app.services.prospective_operations import prospective_health

print(json.dumps(prospective_health(), ensure_ascii=False, indent=2))
