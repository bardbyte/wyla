#!/usr/bin/env bash
# Calls the MDM API for the first configured entity and saves the response
# to tests/fixtures/sample_mdm_response.json for future test re-use.
set -euo pipefail

CONFIG="${1:-lumi_config.yaml}"
[ -f "$CONFIG" ] || { echo "ERROR: $CONFIG not found"; exit 2; }

python - <<PY
import os
import sys
from pathlib import Path

import yaml
import requests

cfg = yaml.safe_load(Path("$CONFIG").read_text())
endpoint = cfg["mdm"]["endpoint"].rstrip("/")
auth_env = cfg["mdm"].get("auth_env")
mapping = cfg["mdm"]["view_to_mdm_entity"]
if not mapping:
    print("ERROR: no view_to_mdm_entity entries")
    sys.exit(2)

first_entity = next(iter(mapping.values()))
url = f"{endpoint}/{first_entity}"
headers = {"Accept": "application/json"}
if auth_env:
    tok = os.environ.get(auth_env, "")
    if tok:
        headers["Authorization"] = f"Bearer {tok}"
    else:
        print(f"WARN: env var {auth_env} is empty — calling unauthenticated.")

print(f"==> GET {url}")
try:
    r = requests.get(url, headers=headers, timeout=15)
except requests.RequestException as e:
    print(f"ERROR: {e}"); sys.exit(1)
print(f"HTTP {r.status_code}")
if not r.ok:
    print(r.text[:400]); sys.exit(1)

out = Path("tests/fixtures/sample_mdm_response.json")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(r.text, encoding="utf-8")
print(f"Saved response to {out}")
PY
