"""The conversational layer (E22): the half of a conversation that is
not a question about data.

"hi", "thanks", "what can you do", "that's wrong actually" all have to
land like a colleague answering, and NONE of them may touch the plan,
call the resolver, or emit a number. Two pins shape the whole module:

  * **No model call where code will do.** Greetings, thanks, farewells
    and "what can you do" are matched deterministically and answered
    from templates, so the most common turns in any chat cost nothing
    and cannot drift.
  * **The reality law applies to self-description.** Every fact in an
    answer about this system is read from the promoted build or from
    the served-capability list below. The system never claims a
    capability it does not have, and names the gated ones plainly.

The voice, pinned because tone is a product surface: warm, brief,
plain. A sharp colleague who knows the data cold and never overstates.
No gushing, no roleplayed feelings, no AI-mystique language.

KNOWN COST, stated rather than hidden: the FIRST turn of a session
still classifies as a data question with no model call (an E18 pin
worth keeping, since it is the most latency-sensitive turn there is).
So a conversational opener these matchers do not catch reaches the
resolver, finds nothing, and comes back with "nothing in the promoted
build matches that yet" plus candidate chips. That is honest and
recoverable, and it is why the matchers below cover greetings, thanks,
capability, how-it-works, build-freshness and help IN CODE: the fix
for a missed opener is another pattern here, never a model call on
every session's first turn.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any

# ── what we actually serve ───────────────────────────────────
# The single source of the product's own capability claims. A test
# pins this against the E19 capability matrix in BOTH directions, so
# a tier that ships without being claimed, or a claim without a tier,
# fails the build rather than reaching a user as a lie.
SERVES: tuple[tuple[str, str], ...] = (
    ("T1", "expand the acronyms and terms your teams actually use, "
           "and ask which one you mean when a scope is ambiguous"),
    ("T2", "bind a question to a certified metric and say which "
           "definition it used"),
    ("T3", "stop and ask when the evidence does not separate two "
           "candidates, with the evidence shown"),
    ("T4", "carry a follow-up like \"same for Canada\" by changing "
           "one part of the plan and nothing else"),
    ("T5", "write down what an answer must satisfy before doing any "
           "work, and check it afterwards"),
    ("T6", "judge whether a join can double-count before the query "
           "runs"),
    ("T7", "show the SQL, the grain, and the limits behind every "
           "number"),
    ("T10", "say I cannot answer, and why, instead of guessing"),
)

# Named honestly when someone asks what this cannot do yet. Each entry
# is a capability the design has and the build does NOT.
GATED: tuple[str, ...] = (
    "composing a new metric from raw columns when none exists yet",
    "open-ended exploratory analysis in code",
    "joins beyond the paths the build has witnesses for",
    "running queries against live data (every query here is validated, "
    "not executed, unless this machine is configured for it)",
)

# ── deterministic matchers ───────────────────────────────────
# Tight patterns on purpose: a false positive here answers a data
# question with small talk, which is far worse than paying for one
# model call. Anchored, and short-text-only for the ambiguous ones.
_GREETING = re.compile(
    r"^\s*(hi|hey|hello|yo|(good\s+)?(morning|afternoon|evening)|"
    r"how('s| is| are)\s+(it going|you|things)|how do you do)"
    r"( there| team| all| everyone| folks)?"
    r"[\s!.,?]*$", re.I)
# a greeting glued to a real request: "hi! can you show me spend in
# Canada". Split in CODE, so E22's canonical mixed opener costs zero
# model calls and the first-turn determinism pin survives intact.
_GREETING_PREFIX = re.compile(
    r"^\s*(?:hi|hey|hello|(?:good\s+)?(?:morning|afternoon|evening))\b"
    r"(?: there| team| all| everyone| folks)?[\s!,.:;-]+", re.I)
_THANKS = re.compile(
    r"^\s*(thanks?|thank\s+you|ta|cheers|nice|perfect|great|"
    r"awesome|lovely|brilliant)"
    r"([\s!.,]*(so much|a lot|mate|very much))?[\s!.,]*$", re.I)
_FAREWELL = re.compile(
    r"^\s*(bye|goodbye|see\s+(you|ya)|later|good\s?night|that's all|"
    r"that is all|i'm done|im done)[\s!.,]*$", re.I)
_CAPABILITY = re.compile(
    r"\b(what can you do|what do you do|what are you( able)? "
    r"(to do|capable of)|what can i ask|how can you help|"
    r"what are your capabilities|who are you|what are you)\b", re.I)
_HOW_IT_WORKS = re.compile(
    r"\b(how do you work|how does (this|it) work|how are you built|"
    r"can i trust (this|you)|where do (your|the) (numbers|answers) "
    r"come from)\b", re.I)
_WHICH_BUILD = re.compile(
    r"\b(which build|what build|build id|how fresh|how current|"
    r"when was this (built|compiled)|what do you know)\b", re.I)
_HELP = re.compile(
    r"\b(how (do|should) i (ask|phrase|word|query|compare|search)|"
    r"what (does|do) (grain|the meridian line|certified|unreviewed|"
    r"witness(es)?|pending|a plan|the contract) mean|"
    r"how do i (use|get started)|what kind of questions?)\b", re.I)
_NEGATIVE = re.compile(
    r"^\s*(that'?s? (wrong|not right|incorrect)|not what i meant|"
    r"wrong|nope,? wrong|that'?s not it|no,? that'?s wrong)"
    r"[\s!.,]*$", re.I)

CHAT_KINDS = ("chat", "meta", "help", "feedback", "off_topic", "mixed")


@dataclass
class ChatTurn:
    """A turn that produces prose and nothing else."""

    kind: str = "chat"
    text: str = ""                    # the reply to stream
    model_used: bool = False
    feedback: dict[str, Any] | None = None   # recorded, when negative
    question: str = ""                # the data half of a mixed turn
    facts: dict[str, Any] = field(default_factory=dict)


# ── grounding ────────────────────────────────────────────────
def world(build: Any) -> dict[str, Any]:
    """The facts any answer about this system may use, read from the
    promoted build. When the compiler's SYNAPSE.md digest lands (E21
    Step 2) it becomes the richer source and slots in here; until then
    these come straight from the indexes, and NOTHING here is
    hand-written."""
    facts: dict[str, Any] = {
        "build_id": getattr(build, "version", ""),
        "tables": len(getattr(build, "schema", {}) or {}),
        "metrics": len(getattr(build, "metrics", []) or []),
        "bindings": len(getattr(build, "bindings", []) or []),
        "joins": len(getattr(build, "joins", []) or []),
    }
    certified = 0
    for row in getattr(build, "metrics", []) or []:
        if (row.get("status_served") or row.get("status")) == "certified":
            certified += 1
    facts["certified"] = certified
    digest = getattr(build, "root", None)
    if digest is not None:
        path = digest / "SYNAPSE.md"          # E21 Step 2, when it lands
        if path.exists():
            facts["digest"] = path.read_text(encoding="utf-8")[:8000]
    return facts


def _pick(options: tuple[str, ...], seed: str) -> str:
    """Light variation, deterministically: the same turn always reads
    the same way, so a transcript is reproducible and a test can pin
    it, but a session does not sound like a recording."""
    index = int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:8], 16)
    return options[index % len(options)]


# ── the deterministic replies ────────────────────────────────
def _greeting(facts: dict[str, Any], seed: str) -> str:
    scale = (f"{facts['tables']} tables and {facts['metrics']} metrics"
             if facts.get("tables") else "the company's governed data")
    return _pick((
        f"Hi. I answer questions over {scale}, with the definition "
        f"shown on every number. What are you looking at?",
        f"Hello. I work over {scale}, and every number comes back with "
        f"the definition behind it. What do you need?",
        f"Hi there. {scale.capitalize()} are compiled and ready, each "
        f"answer carrying its own receipts. What are we looking into?",
    ), seed)


def _thanks(seed: str) -> str:
    return _pick((
        "Any time. What else?",
        "You're welcome. Anything else you want to look at?",
        "Glad it helped. What next?",
    ), seed)


def _farewell(seed: str) -> str:
    return _pick((
        "Cheers. The session is saved, so you can pick it back up.",
        "See you. Your plan and its versions are kept for next time.",
    ), seed)


def _capabilities(facts: dict[str, Any]) -> str:
    lines = [f"I answer questions over the compiled build "
             f"`{facts.get('build_id', '?')}`: "
             f"{facts.get('tables', 0)} tables, "
             f"{facts.get('metrics', 0)} metrics "
             f"({facts.get('certified', 0)} certified). What I can do:",
             ""]
    lines += [f"- {what}" for _tier, what in SERVES]
    lines += ["", "Not yet, and I will say so rather than improvise:", ""]
    lines += [f"- {what}" for what in GATED]
    lines += ["", "Ask me something and you will see the whole chain: "
              "what I bound, what I checked, and what I could not."]
    return "\n".join(lines)


def _how_it_works(facts: dict[str, Any]) -> str:
    return (
        "Every answer is built from a compiled snapshot of the "
        f"company's semantic graph (this one is `{facts.get('build_id')}`), "
        "never from the live truth store, so the same question against "
        "the same build gives the same answer with the same provenance."
        "\n\nA turn goes: read what you asked, bind it to a definition "
        "on record, write down what the answer must satisfy, compose "
        "the query, then check it with a separate pass that starts from "
        "the assumption that everything failed. Whatever that check "
        "could not prove is shown on the answer as a limit, not hidden."
        "\n\nSo: trust the numbers as far as the receipts go, and the "
        "receipts are on every one of them.")


def _which_build(facts: dict[str, Any]) -> str:
    return (
        f"This is build `{facts.get('build_id', '?')}`: "
        f"{facts.get('tables', 0)} tables, {facts.get('metrics', 0)} "
        f"metrics ({facts.get('certified', 0)} certified), "
        f"{facts.get('bindings', 0)} concept bindings and "
        f"{facts.get('joins', 0)} join paths on record. A build is a "
        "snapshot: when a steward changes a definition it takes a "
        "recompile to reach me, and the build id changes with it.")


def _help() -> str:
    return (
        "Ask for a measure and how you want it cut: \"spend by month "
        "for Canada\", \"transaction count by day\". Two words that "
        "carry weight here:"
        "\n\n- **grain** is what one row of the answer means. I will "
        "ask if it is not on record, and every option I offer is a "
        "grain some other metric on that table already declares."
        "\n- **certified** means a steward signed off on that "
        "definition. Anything else is labelled with where its "
        "evidence came from instead."
        "\n\nFollow-ups work: ask one thing, then say \"same for "
        "Canada\" and I change that one part and nothing else.")


def pre_classify(text: str, facts: dict[str, Any], *,
                 first_turn: bool) -> ChatTurn | None:
    """The deterministic half. Returns a fully-formed ChatTurn when the
    turn is unmistakably conversational, or None to let the model
    classifier decide. Runs BEFORE the first-turn shortcut, because
    "hi" as an opener is a greeting, never a question."""
    stripped = (text or "").strip()
    if not stripped:
        return None
    seed = stripped.lower()

    if _GREETING.match(stripped):
        return ChatTurn(kind="chat", text=_greeting(facts, seed),
                        facts=facts)
    if _THANKS.match(stripped):
        return ChatTurn(kind="chat", text=_thanks(seed), facts=facts)
    if _FAREWELL.match(stripped):
        return ChatTurn(kind="chat", text=_farewell(seed), facts=facts)
    prefixed = _GREETING_PREFIX.match(stripped)
    if prefixed:
        rest = stripped[prefixed.end():].strip()
        if len(rest) >= 10:
            # mixed, split deterministically: a short hello, then the
            # data half runs the full pipeline
            return ChatTurn(kind="mixed",
                            text=_pick(("Hi.", "Hello.", "Morning."),
                                       seed),
                            question=rest, facts=facts)
    if _NEGATIVE.match(stripped):
        return ChatTurn(
            kind="feedback",
            text="Noted, and recorded against that answer. Tell me what "
                 "it should have been and I will change that one part "
                 "of the plan, or say what looked wrong and a steward "
                 "will see it.",
            feedback={"vote": "down", "subject": "answer",
                      "note": stripped[:2000]}, facts=facts)
    # the highest-stakes self-descriptions are code, not prompt: a
    # capability claim is exactly the sentence that must never drift
    if _HELP.search(stripped):
        return ChatTurn(kind="help", text=_help(), facts=facts)
    if _CAPABILITY.search(stripped):
        return ChatTurn(kind="meta", text=_capabilities(facts),
                        facts=facts)
    if _HOW_IT_WORKS.search(stripped):
        return ChatTurn(kind="meta", text=_how_it_works(facts),
                        facts=facts)
    if _WHICH_BUILD.search(stripped):
        return ChatTurn(kind="meta", text=_which_build(facts),
                        facts=facts)
    return None


# ── the model-backed half ────────────────────────────────────
CHAT_SYSTEM = """You are the conversational voice of a governed \
analytics system. You are talking to an analyst.

VOICE: warm, brief, plain. A sharp colleague who knows the data cold \
and never overstates. Two or three sentences. No gushing, no \
roleplayed feelings, no talk of being an AI, no exclamation stacking.

HARD RULES:
- You have NO access to data in this turn. Never state a number, a \
metric value, or a fact about the company's data.
- Never claim a capability. The capability list is written by the \
system, not by you.
- If the turn actually wants data, say so in one sentence and stop; \
the system will route it.

Return STRICT JSON: {"reply": "<your message>"}"""


def chat_prompt(kind: str, text: str, facts: dict[str, Any]) -> str:
    known = (f"build {facts.get('build_id')}, {facts.get('tables')} "
             f"tables, {facts.get('metrics')} metrics")
    guide = {
        "help": "The analyst is asking HOW to ask something. Explain "
                "the shape of a good question here in plain words.",
        "off_topic": "The analyst asked about something outside this "
                     "system's data. Decline in one sentence, warmly, "
                     "and offer the door back to their data.",
        "feedback": "The analyst is reacting to an answer. Acknowledge "
                    "it plainly and say what happens next.",
    }.get(kind, "Reply conversationally.")
    return (f"Context you may reference (facts only): {known}\n"
            f"Situation: {guide}\nThe analyst said: {text!r}")
