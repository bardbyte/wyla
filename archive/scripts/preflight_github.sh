#!/usr/bin/env bash
# Verifies git is installed and we can reach the configured LookML repo.
set -euo pipefail

CONFIG="${1:-lumi_config.yaml}"

if [ ! -f "$CONFIG" ]; then
    echo "ERROR: $CONFIG not found. Copy lumi_config.example.yaml and fill it in."
    exit 2
fi

echo "==> git version"
git --version

REPO=$(python - <<PY
import yaml
from pathlib import Path
cfg = yaml.safe_load(Path("$CONFIG").read_text())
print(cfg["git"]["repo"])
PY
)

echo "==> Probing $REPO"
if git ls-remote --exit-code --heads "$REPO" > /dev/null 2>&1; then
    echo "OK — repo reachable."
else
    echo "ERROR: cannot reach $REPO — check VPN / auth / repo URL."
    exit 1
fi
