#!/usr/bin/env bash
# Verifies every Python dep LUMI needs is installed and importable.
set -euo pipefail

echo "==> Python version"
python --version

echo "==> Importing required packages"
python - <<'PY'
mods = [
    "pydantic", "yaml", "requests", "dotenv",
    "openpyxl", "lkml", "sqlglot",
    "google.adk", "langchain_core",
]
missing = []
for m in mods:
    try:
        __import__(m)
    except ImportError as e:
        missing.append((m, str(e)))
if missing:
    print("MISSING:")
    for m, e in missing:
        print(f"  - {m}: {e}")
    raise SystemExit(1)
print("OK — all deps importable.")
PY

echo "==> Importing SafeChain (Amex-internal)"
python - <<'PY' || {
    echo "WARN: SafeChain adapter not importable. Install the Amex SafeChain"
    echo "      package before running \`python -m lumi\`. See BOOTSTRAP.md."
    exit 1
}
from src.adapters.model_adapter import get_model  # noqa: F401
print("OK — SafeChain importable.")
PY

echo "==> All deps OK."
