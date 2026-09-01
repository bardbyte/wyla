"""Turn classification (E18, extended by E22): what kind of turn?

Data turns:  new_question · mutate(slot) · explain · discover · govern
Chat turns:  chat · meta · help · feedback · off_topic · mixed

Temperature 0, strict JSON, and deterministic shortcuts that pay for
themselves: greetings, thanks and capability questions are matched in
code before this module is reached (see converse.py), a chip choice
arrives already structured, and the first turn of a session that is
not conversational is a new question by construction. The model is
asked only when a human typed free text this code could not place.

The chat kinds create no plan version, call no resolver and emit no
number. ``mixed`` is the one that does both: it carries a chat half
and a data half, answered in that order inside one turn.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .plan import Plan

DATA_KINDS = ("new_question", "mutate", "explain", "discover", "govern")
CHAT_KINDS = ("chat", "meta", "help", "feedback", "off_topic")
KINDS = DATA_KINDS + CHAT_KINDS + ("mixed",)

SYSTEM = """You classify ONE turn of an analyst's conversation with a \
governed semantic layer. You never answer the question and you never \
invent metric names.

Return STRICT JSON:
{"kind": "<one kind from the list>",
 "question": "<the standalone DATA question, if this turn has one>",
 "chat": "<the conversational half, only for kind=mixed>",
 "edits": [{"slot": "<slot name>", "value": "<new value>"}],
 "why": "<one short sentence>"}

DATA kinds (these plan and answer):
- "mutate" ONLY when the turn changes part of the existing plan and \
you can name the exact slot from the slot list given. Follow-ups like \
"same for Canada" or "make it monthly" are mutations.
- "new_question" if the turn starts a different analysis.
- "explain" = asking about the previous answer's meaning or evidence.
- "discover" = asking what exists (which metrics/tables are there).
- "govern" = a steward action (certify, deprecate, rename, approve).

CHAT kinds (these must NOT plan; they touch no data):
- "chat" = greeting, small talk, thanks, pleasantry.
- "meta" = about this system: what it can do, how it works, how \
current it is, whether it can be trusted.
- "help" = how to ASK something ("how do I compare two quarters", \
"what does grain mean") rather than the question itself.
- "feedback" = reacting to an answer: wrong, right, not what I meant.
- "off_topic" = outside this system's data entirely.

- "mixed" = one turn carrying BOTH, e.g. "hi! can you show me spend \
in Canada". Put the pleasantry in "chat" and the data question in \
"question". Only use mixed when the data half is a real question.

Rules:
- edits is [] for every kind except mutate.
- A slot value must be a literal the analyst said; never a guess.
- When in doubt between a chat kind and a data kind, choose the data \
kind: a missed question is worse than an unnecessary plan."""


@dataclass
class Classification:
    kind: str = "new_question"
    question: str = ""
    edits: list[dict[str, Any]] = field(default_factory=list)
    why: str = ""
    degraded: bool = False        # the model gave nothing usable
    model_used: bool = False
    chat: str = ""                # the conversational half (mixed)

    @property
    def is_chat(self) -> bool:
        """True when this turn must never reach the resolver."""
        return self.kind in CHAT_KINDS

    def to_event(self) -> dict[str, Any]:
        return {"kind": self.kind, "question": self.question,
                "edits": self.edits, "why": self.why,
                "degraded": self.degraded, "model_used": self.model_used,
                "chat_turn": self.is_chat}


def classify(model: Any, text: str, plan: Plan | None, *,
             choice: dict[str, Any] | None = None,
             allow_chat: bool = False) -> Classification:
    """→ Classification. `choice` is a chip answer: already structured,
    so it costs nothing and cannot be misread.

    ``allow_chat`` asks the model to consider the E22 chat kinds too.
    The loop sets it once converse.pre_classify has declined the turn,
    so the free path is always tried first."""
    if choice:
        slot = str(choice.get("slot") or "")
        return Classification(
            kind="mutate", question=(plan.question if plan else text),
            edits=[{"slot": slot, "value": choice.get("value")}],
            why=f"the analyst picked {choice.get('label') or slot}")

    if plan is None:
        # Nothing to mutate: the first turn is a new question, and
        # saying so deterministically keeps it instant and free. E22
        # does NOT spend a model call here to catch a rare opener:
        # converse.pre_classify has already matched the conversational
        # ones in code, and someone opening a data tool is asking a
        # data question. The known cost is documented in converse.py.
        return Classification(kind="new_question", question=text,
                              why="first turn of the session")

    slot_list = "\n".join(f"  {name}: {value!r}"
                          for name, value in sorted(plan.slots().items())
                          ) if plan is not None else "  (no plan yet)"
    previous = f"{plan.question!r}" if plan is not None else "(none)"
    prompt = (f"Current plan slots:\n{slot_list}\n\n"
              f"Previous question: {previous}\n"
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
    known = set(plan.slots()) if plan is not None else set()
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
    if kind in CHAT_KINDS and not allow_chat:
        kind = "new_question"       # chat was not on the menu this turn
    question = str(answer.get("question") or text)
    if kind == "mixed" and not str(answer.get("question") or "").strip():
        kind = "chat"               # no data half: it was just chat
    return Classification(
        kind=kind, question=question, chat=str(answer.get("chat") or ""),
        edits=edits, why=str(answer.get("why") or ""), model_used=True)
