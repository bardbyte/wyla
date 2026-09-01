"""Agent Loop v1 — the Claude-Code-class harness (docs/specs/agent_loop_v1.md).

The model is the planner and the navigator; determinism lives inside
the tools; the contract, verifier, budgets, and disclosure are the
harness around it. This package is built in the spec's §9 order:
tools first (descriptions are the product), then the loop, then the
prompt, then the navigation evals, then the scout.
"""

from .loop import (LoopBudget, navigate_loop)             # noqa: F401
from .prompt import PROMPT_VERSION, system_prompt         # noqa: F401
from .scout import run_scout                              # noqa: F401
from .skills import Skill, list_skills, load_skills       # noqa: F401
from .tools import (LoopState, SnapshotRunner, ToolSpec,  # noqa: F401
                    render_tool_block, toolkit)
