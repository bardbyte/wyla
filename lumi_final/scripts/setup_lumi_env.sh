#!/usr/bin/env bash
# Source this to set up BOTH service-account credentials in one shell:
#   - GOOGLE_APPLICATION_CREDENTIALS  → Vertex SA (used by ADK + google-genai)
#   - LUMI_BQ_KEY_FILE                → BQ SA   (used by check_bq_access.py)
#
# Two SAs let us isolate IAM grants: the Vertex SA needs aiplatform.user
# and nothing else, the BQ SA needs bigquery.dataViewer + bigquery.jobUser
# and nothing else. Same shell, two credentials, no conflict.
#
# Usage:
#   source lumi_final/scripts/setup_lumi_env.sh \
#       --vertex ~/Downloads/vertex-sa.json \
#       --bq     ~/Downloads/bq-sa.json
#
#   # Or in any order; both required.
#
# After sourcing:
#   python lumi_final/scripts/check_vertex_gemini.py        # uses GOOGLE_APPLICATION_CREDENTIALS
#   python lumi_final/scripts/check_bq_access.py --auth-only # uses LUMI_BQ_KEY_FILE

VERTEX_KEY=""
BQ_KEY=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --vertex) VERTEX_KEY="$2"; shift 2 ;;
        --bq)     BQ_KEY="$2";     shift 2 ;;
        -h|--help)
            grep -E "^# " "${BASH_SOURCE[0]}" | sed 's/^# //'
            return 0 2>/dev/null || exit 0
            ;;
        *)
            echo "ERROR: unknown arg: $1" >&2
            echo "Usage: source setup_lumi_env.sh --vertex <path> --bq <path>" >&2
            return 1 2>/dev/null || exit 1
            ;;
    esac
done

if [[ -z "$VERTEX_KEY" || -z "$BQ_KEY" ]]; then
    echo "ERROR: both --vertex and --bq are required" >&2
    echo "Usage: source setup_lumi_env.sh --vertex <path> --bq <path>" >&2
    return 1 2>/dev/null || exit 1
fi

# Resolve to absolute paths and verify both files exist.
_abspath() {
    local p="$1"
    # Expand ~
    p="${p/#~/$HOME}"
    if [[ ! -f "$p" ]]; then
        echo "ERROR: file not found: $p" >&2
        return 1
    fi
    echo "$(cd "$(dirname "$p")" && pwd)/$(basename "$p")"
}

VERTEX_ABS="$(_abspath "$VERTEX_KEY")" || return 1 2>/dev/null || exit 1
BQ_ABS="$(_abspath "$BQ_KEY")" || return 1 2>/dev/null || exit 1

# Refuse to load any key from inside the repo (matches the rest of the codebase).
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
for k in "$VERTEX_ABS" "$BQ_ABS"; do
    case "$k" in
        "$REPO_ROOT"/*)
            echo "ERROR: refusing to load credentials from inside the repo: $k" >&2
            echo "       move the key outside the repo (e.g. ~/Downloads/) and re-run." >&2
            return 1 2>/dev/null || exit 1
            ;;
    esac
done

if [[ "$VERTEX_ABS" == "$BQ_ABS" ]]; then
    echo "WARN: --vertex and --bq point at the same file; that defeats the purpose of split SAs." >&2
fi

export GOOGLE_APPLICATION_CREDENTIALS="$VERTEX_ABS"
export GOOGLE_GENAI_USE_VERTEXAI=true
export GOOGLE_CLOUD_PROJECT=prj-d-ea-poc
export GOOGLE_CLOUD_LOCATION=global
export LUMI_BQ_KEY_FILE="$BQ_ABS"

echo "LUMI env configured:"
echo "  GOOGLE_APPLICATION_CREDENTIALS = $GOOGLE_APPLICATION_CREDENTIALS"
echo "  GOOGLE_GENAI_USE_VERTEXAI      = $GOOGLE_GENAI_USE_VERTEXAI"
echo "  GOOGLE_CLOUD_PROJECT           = $GOOGLE_CLOUD_PROJECT"
echo "  GOOGLE_CLOUD_LOCATION          = $GOOGLE_CLOUD_LOCATION"
echo "  LUMI_BQ_KEY_FILE               = $LUMI_BQ_KEY_FILE"
echo
echo "Now run any of:"
echo "  python lumi_final/scripts/check_vertex_gemini.py     # Vertex SA"
echo "  python lumi_final/scripts/check_bq_access.py --auth-only  # BQ SA"
echo "  adk web apps/                                        # Vertex SA"
