"""ADK web entry point — re-exports the smoke-test agent.

Wires the existing `agent_test/` agent definition into the layout `adk web`
expects (AGENTS_DIR/<agent_name>/agent.py with a module-level `root_agent`).

Single source of truth lives in `agent_test/agent.py`; this file just makes
the agent discoverable when the user runs:

    adk web apps/

from the repo root.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make the repo root importable so `from agent_test.agent import root_agent`
# resolves regardless of where adk web set sys.path.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from agent_test.agent import root_agent  # noqa: E402, F401
