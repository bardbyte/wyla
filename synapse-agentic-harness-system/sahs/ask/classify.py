"""Turn classification (E18): what kind of turn is this?

new_question · mutate(slot) · explain · discover · govern

Temperature 0, strict JSON, and two deterministic shortcuts that pay
for themselves: the FIRST turn of a session is a new question by
construction (no model call), and a chip choice arrives already
structured (no model call). The model is asked only when a human
typed free text into an existing plan.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .plan import Plan

KINDS = ("new_question", "mutate", "explain", "discover", "govern")

SYSTEM = """You classify ONE turn of an analyst's conversation with a \
governed semantic layer. You never answer the question and you never \
invent metric names.

Return STRICT JSON:
{"kind": "new_question|mutate|explain|discover|govern",
 "question": "<the standalone question this turn is asking>",
 "edits": [{"slot": "<slot name>", "value": "<new value>"}],
 "why": "<one short sentence>"}

Rules:
- "mutate" ONLY when the turn changes part of the existing plan and \
you can name the exact slot from the slot list given. Follow-ups like \
"same for Canada" or "make it monthly" are mutations.
- edits is [] for every kind except mutate.
- A slot value must be a literal the analyst said; never a guess.
- "explain" = asking about the previous answer's meaning or evidence.
- "discover" = asking what exists (which metrics/tables are there).
- "govern" = a steward action (certify, deprecate, rename, approve).
- If the turn starts a different analysis, that is new_question."""


@dataclass
class Classification:
    kind: str = "new_question"
    question: str = ""
    edits: list[dict[str, Any]] = field(default_factory=list)
    why: str = ""
    degraded: bool = False        # the model gave nothing usable
    model_used: bool = False

    def to_event(self) -> dict[str, Any]:
        return {"kind": self.kind, "question": self.question,
                "edits": self.edits, "why": self.why,
                "degraded": self.degraded, "model_used": self.model_used}


def classify(model: Any, text: str, plan: Plan | None, *,
             choice: dict[str, Any] | None = None) -> Classification:
    """→ Classification. `choice` is a chip answer: already structured,
    so it costs nothing and cannot be misread."""
    if choice:
        slot = str(choice.get("slot") or "")
        return Classification(
            kind="mutate", question=(plan.question if plan else text),
            edits=[{"slot": slot, "value": choice.get("value")}],
            why=f"the analyst picked {choice.get('label') or slot}")

    if plan is None:
        # nothing to mutate: the first turn is a new question, and
        # saying so deterministically keeps it instant and free
        return Classification(kind="new_question", question=text,
                              why="first turn of the session")

    slot_list = "\n".join(f"  {name}: {value!r}"
                          for name, value in sorted(plan.slots().items()))
    prompt = (f"Current plan slots:\n{slot_list}\n\n"
              f"Previous question: {plan.question!r}\n"
              f"This turn: {text!r}\n\nClassify it.")
    answer = model.json(prompt, system=SYSTEM, temperature=0.0,
                        max_tokens=600)
    if not isinstance(answer, dict) or answer.get("kind") not in KINDS:
        # honest degradation: treat it as a fresh question and SAY so
        # in the event, rather than guessing at a mutation
        return Classification(kind="new_question", question=text,
                              why="the classifier returned no usable JSON",
                              degraded=True, model_used=True)

    edits = []
    known = set(plan.slots())
    for edit in (answer.get("edits") or [])[:3]:
        if not isinstance(edit, dict):
            continue
        slot = str(edit.get("slot") or "")
        # a mutation may only name a slot that exists, or add a filter
        if slot in known or slot.startswith("filters."):
            edits.append({"slot": slot, "value": edit.get("value")})
    kind = answer["kind"]
    if kind == "mutate" and not edits:
        kind = "new_question"       # it named nothing we can move
    return Classification(
        kind=kind, question=str(answer.get("question") or text),
        edits=edits, why=str(answer.get("why") or ""), model_used=True)
