"""What an assistant turn accumulates on top of the loop state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sahs.loop.tools import LoopState


@dataclass
class AssistantState(LoopState):
    queries_saved: int = 0
    artifacts_touched: list[str] = field(default_factory=list)
    facts: set[str] = field(default_factory=set)
    facts_log: list[dict[str, Any]] = field(default_factory=list)
    skills_loaded: list[str] = field(default_factory=list)
    chips: list[str] = field(default_factory=list)
    # the query handed to the person (propose_sql): ends the turn
    proposal: dict[str, Any] | None = None
    # propose_sql refuses an over-ceiling query once, so the model
    # narrows it; the second time it hands over with the warning
    ceiling_refusals: int = 0
