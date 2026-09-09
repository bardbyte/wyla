#!/usr/bin/env bash
# Source this before running `adk web` so the agent talks to your Vertex
# project via the service-account JSON.
#
#   source agent_test/setup_vertex_env.sh ~/Downloads/key.json
#   adk web apps/
#
# Args:
#   $1  Path to the service-account JSON. Default: $HOME/Downloads/key.json.

KEY_FILE="${1:-$HOME/Downloads/key.json}"

if [[ ! -f "$KEY_FILE" ]]; then
    echo "ERROR: service-account JSON not found at: $KEY_FILE" >&2
    echo "       pass a path:  source agent_test/setup_vertex_env.sh /path/to/key.json" >&2
    return 1 2>/dev/null || exit 1
fi

# Refuse to load a key from inside the repo (matches run.py's guard).
ABS_KEY="$(cd "$(dirname "$KEY_FILE")" && pwd)/$(basename "$KEY_FILE")"
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
case "$ABS_KEY" in
    "$REPO_ROOT"/*)
        echo "ERROR: refusing to load credentials from inside the repo: $ABS_KEY" >&2
        echo "       move the key outside the repo (e.g. ~/Downloads/) and re-run." >&2
        return 1 2>/dev/null || exit 1
        ;;
esac

export GOOGLE_APPLICATION_CREDENTIALS="$ABS_KEY"
export GOOGLE_GENAI_USE_VERTEXAI=true
export GOOGLE_CLOUD_PROJECT=prj-d-ea-poc
export GOOGLE_CLOUD_LOCATION=global

echo "Vertex env configured:"
echo "  GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS"
echo "  GOOGLE_GENAI_USE_VERTEXAI=$GOOGLE_GENAI_USE_VERTEXAI"
echo "  GOOGLE_CLOUD_PROJECT=$GOOGLE_CLOUD_PROJECT"
echo "  GOOGLE_CLOUD_LOCATION=$GOOGLE_CLOUD_LOCATION"
echo
echo "Now run:  adk web apps/"
