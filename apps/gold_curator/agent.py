"""ADK web entry point — re-exports the gold_curator agent.

Single source of truth lives in `gold_curator/agent.py`. This file just
makes the agent discoverable when the user runs:

    adk web apps/

from the repo root.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make the repo root importable so `from gold_curator.agent import root_agent`
# resolves regardless of where adk web set sys.path.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from gold_curator.agent import root_agent  # noqa: E402, F401
