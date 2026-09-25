"""One harness, several engines: what each Gemini model accepts, and
how the depth dial folds onto it.

The chassis is shared — one loop, one prompt, one tool kit. What
differs per model is the engine map: how it takes its depth (a
``thinkingLevel`` on 3.x, a token budget on the retiring 2.5s), WHICH
levels it accepts (3.7 Flash refuses minimal; 3.5 Flash starts at
medium; Flash Lite goes all the way down), its output ceiling, and
where it belongs in the harness. The dial has five stops for every
model; a stop the model does not know folds onto the nearest level it
does, toward the deeper side on a tie, so "Minimal" never becomes a
400 and never silently becomes "Standard" either.

The table below is what Google's model pages say (September 2026) and
what the laptop's ``gateway_check.py --all-models`` proved; the
``--levels`` probe asks the gateway itself and prints the
``GATEWAY_MODEL_LEVELS`` line to paste when the two disagree. Nothing
here names an enterprise host: models, levels and caps only.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

# the API's levels, shallow to deep; "max" is kept so a model that
# grows one can be named in GATEWAY_MODEL_LEVELS without a code change
LEVEL_RANK: dict[str, int] = {"minimal": 0, "low": 1, "medium": 2,
                              "high": 3, "max": 4}
# the dial's five stops as the loop spells them, plus the one-shot
# JSON calls (judge, title, memory), which want the shallowest level
DIAL_STOPS = ("minimal", "low", "medium", "high", "max")
JSON_STOP = "json"

FAMILY_GEMINI_3 = "gemini-3"
FAMILY_GEMINI_25 = "gemini-2.5"
FAMILY_OTHER = "other"

THINKING_LEVEL = "level"
THINKING_BUDGET = "budget"
THINKING_NONE = "none"

DEFAULT_CAP = 65536

# ── the skill-retrieval budget ────────────────────────────────
# A skill over the whole-load ceiling reaches the prompt as its table
# of contents plus the chunks that match the ask (sahs.loop.skill_index),
# under a per-engine budget in CHARACTERS (≈ 4 chars a token). The
# numbers are deliberately conservative: a prompt is not a library,
# and the model can always skill_search for more. The depth dial
# folds onto the budget through RETRIEVAL_FOLD, keyed by the level the
# engine actually runs at (so Quick on 3.5 Flash, which folds to
# medium, gets medium's share — the fold is the engine's, not the
# dial's). No env knob: the table is the place to tune an engine.
DEFAULT_SKILL_BUDGET = 32_000          # an engine the table does not know
# level the engine runs at → (chunks in the prompt, share of the budget)
RETRIEVAL_FOLD: dict[str, tuple[int, float]] = {
    "minimal": (2, 0.25), "low": (3, 0.40), "medium": (5, 0.60),
    "high": (8, 1.00), "max": (8, 1.00)}

# ── the whole-load budget ─────────────────────────────────────
# Whole-load is the correctness path: a skill that fits loads whole,
# verbatim. What fits is the engine's context window (tokens, from
# the model pages) times the harness's 4-chars-a-token estimate times
# the share a skill may take of it, leaving the rest for the digest,
# the conversation, the tool results and the answer. A 1M-token
# engine takes a 650,000-character bundle whole; an engine the table
# does not know is assumed small. SAHS_MAX_SKILL_CHARS stays the
# global ceiling on top (skills_loader.whole_load_limit takes the min).
CHARS_PER_TOKEN = 4
WHOLE_LOAD_SHARE = 0.5
CONTEXT_GEMINI_3 = 1_048_576
CONTEXT_GEMINI_25 = 1_048_576
DEFAULT_CONTEXT_TOKENS = 131_072       # an engine the table does not know


@dataclass(frozen=True)
class ModelProfile:
    model: str
    family: str
    thinking: str                  # level | budget | none
    accepts: tuple[str, ...]       # thinkingLevel values, shallow → deep
    cap: int = DEFAULT_CAP
    fit: str = ""                  # when to pick it, for the product's people
    source: str = "family"         # docs | probe | family | env
    facts: str = ""                # the engineer's line: dialect, ceiling, status
    skill_budget: int = DEFAULT_SKILL_BUDGET   # chars of searchable skills
    context_tokens: int = DEFAULT_CONTEXT_TOKENS  # the engine's window

    @property
    def whole_load_chars(self) -> int:
        """The longest skill this engine takes whole: its share of
        the context window, in characters."""
        return int(self.context_tokens * CHARS_PER_TOKEN * WHOLE_LOAD_SHARE)

    def retrieval(self, stop: str) -> tuple[int, int]:
        """(chunks, chars) of searchable-skill context for a dial
        stop: the fold's share of this engine's budget at the level
        the stop lands on."""
        level = self.level_for(stop)
        k, share = RETRIEVAL_FOLD.get(level, RETRIEVAL_FOLD["medium"])
        return k, int(self.skill_budget * share)

    def level_for(self, stop: str) -> str:
        """The dial stop (or "json") as this model spells it: the
        nearest accepted level, the deeper one on a tie; a level model
        with nothing accepted, or a model that does not think, gets
        the stop back unchanged for the caller to decide."""
        key = (stop or "").strip().lower()
        if key == JSON_STOP:
            return self.accepts[0] if self.accepts else "low"
        if not self.accepts or key not in LEVEL_RANK:
            return key
        wanted = LEVEL_RANK[key]
        best = min(self.accepts,
                   key=lambda lvl: (abs(LEVEL_RANK.get(lvl, 99) - wanted),
                                    -LEVEL_RANK.get(lvl, 99)))
        return best

    @property
    def depth_levels(self) -> dict[str, str]:
        """Every dial stop, and "json", folded onto this model."""
        return {stop: self.level_for(stop) for stop in (*DIAL_STOPS, JSON_STOP)}

    def as_row(self) -> dict[str, Any]:
        return {"model": self.model, "family": self.family,
                "thinking": self.thinking, "levels": list(self.accepts),
                "cap": self.cap, "fit": self.fit, "facts": self.facts,
                "source": self.source,
                "skill_budget": self.skill_budget,
                "context_tokens": self.context_tokens,
                "whole_load_chars": self.whole_load_chars}


# ── the known engines ─────────────────────────────────────────
# keyed by the model's stem: gemini-3.1-pro-preview → gemini-3.1-pro.
# ``fit`` is the line under the model's name in the composer's picker,
# written for the people who pick: risk analysts asking metric, SQL and
# dashboard questions, stewards curating knowledge, admins. One plain
# sentence on when to pick it. ``facts`` is the engineer's line — the
# thinking dialect, the levels, the ceiling, the status — and rides the
# hover title and the "?" explainer, never the row.
PROFILES: dict[str, ModelProfile] = {
    "gemini-3.1-pro": ModelProfile(
        "gemini-3.1-pro", FAMILY_GEMINI_3, THINKING_LEVEL,
        ("low", "medium", "high"),
        fit="Most careful. Pick for complex metric questions, multi-step "
            "SQL and dashboards. Slower.",
        source="docs",
        facts="Thinks by level (low, medium, high); Deep and Extra deep "
              "fold onto high; streams on Vertex; 65,536-token ceiling.",
        skill_budget=120_000, context_tokens=CONTEXT_GEMINI_3),
    "gemini-3.7-flash": ModelProfile(
        "gemini-3.7-flash", FAMILY_GEMINI_3, THINKING_LEVEL,
        ("low", "medium", "high"),          # minimal is refused
        fit="Fast everyday answers: definitions, quick lookups, simple "
            "queries.",
        source="docs",
        facts="Thinks by level (low, medium, high); Minimal folds onto "
              "low; the gateway's default; 65,536-token ceiling.",
        skill_budget=80_000, context_tokens=CONTEXT_GEMINI_3),
    "gemini-3.5-flash": ModelProfile(
        "gemini-3.5-flash", FAMILY_GEMINI_3, THINKING_LEVEL,
        ("medium", "high"),                 # nothing shallower is listed
        fit="As fast as 3.7 Flash; use when 3.7 is not offered in your "
            "environment.",
        source="docs",
        facts="Thinks by level (medium, high); Minimal and Quick fold "
              "onto medium; 65,536-token ceiling.",
        skill_budget=80_000, context_tokens=CONTEXT_GEMINI_3),
    "gemini-3.1-flash-lite": ModelProfile(
        "gemini-3.1-flash-lite", FAMILY_GEMINI_3, THINKING_LEVEL,
        ("minimal", "low", "medium", "high"),
        fit="Fastest and lightest: short factual questions and quick "
            "checks, not multi-step analysis.",
        source="docs",
        facts="Thinks by level (minimal to high); also serves the "
              "harness's one-shot JSON calls (classify, judge, reviews, "
              "suggestions); 65,536-token ceiling.",
        skill_budget=40_000, context_tokens=CONTEXT_GEMINI_3),
}

# the family fallbacks, for a model the table does not name
_FAMILY_DEFAULTS: dict[str, ModelProfile] = {
    FAMILY_GEMINI_3: ModelProfile(
        "", FAMILY_GEMINI_3, THINKING_LEVEL, ("low", "medium", "high"),
        fit="A newer Gemini 3 model: fine for everyday questions until "
            "it has been tried here.",
        facts="Not in the engine table: the common three levels (low, "
              "medium, high) until the probe says otherwise.",
        skill_budget=80_000, context_tokens=CONTEXT_GEMINI_3),
    FAMILY_GEMINI_25: ModelProfile(
        "", FAMILY_GEMINI_25, THINKING_BUDGET, (),
        fit="Older model kept for compatibility; prefer 3.1 Pro or 3.7 "
            "Flash.",
        facts="Retiring: thinks by token budget under the output cap; "
              "kept only for an environment that still names it.",
        skill_budget=40_000, context_tokens=CONTEXT_GEMINI_25),
    FAMILY_OTHER: ModelProfile(
        "", FAMILY_OTHER, THINKING_BUDGET, (),
        fit="A model outside the Gemini family: use only if your "
            "environment names it.",
        facts="Unknown to the engine table: treated as a budget model."),
}


def family_of(model: str) -> str:
    name = (model or "").strip().lower()
    if name.startswith("gemini-3"):
        return FAMILY_GEMINI_3
    if name.startswith("gemini-2.5"):
        return FAMILY_GEMINI_25
    return FAMILY_OTHER


def _stem(model: str) -> str:
    """The table key for a model id: the longest known key the id
    starts with (gemini-3.1-pro-preview → gemini-3.1-pro)."""
    name = (model or "").strip().lower()
    hits = [key for key in PROFILES if name == key or name.startswith(key + "-")]
    return max(hits, key=len) if hits else ""


def _pairs(raw: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in (raw or "").split(","):
        key, sep, value = item.strip().partition(":")
        if sep and key.strip() and value.strip():
            out[key.strip()] = value.strip()
    return out


def _levels_list(raw: str) -> tuple[str, ...]:
    """"minimal|low|medium" (or space separated) → the known levels,
    shallow to deep, unknown words dropped."""
    words = [w.strip().lower() for w in raw.replace("|", " ").split()]
    known = [w for w in words if w in LEVEL_RANK]
    return tuple(sorted(dict.fromkeys(known), key=LEVEL_RANK.__getitem__))


def profile_for(model: str, env: dict[str, str] | None = None) -> ModelProfile:
    """The engine map for one model: the table's row (by stem), else
    the family's default; then the environment's word on top —
    GATEWAY_MODEL_THINKING=model:kind, GATEWAY_MODEL_LEVELS=model:l1|l2
    (the probe's finding), GATEWAY_MODEL_CAPS=model:tokens."""
    env = dict(os.environ if env is None else env)
    name = (model or "").strip()
    stem = _stem(name)
    base = PROFILES.get(stem) or _FAMILY_DEFAULTS[family_of(name)]
    thinking, accepts, cap, source = base.thinking, base.accepts, base.cap, base.source
    kind = _pairs(env.get("GATEWAY_MODEL_THINKING") or "").get(name, "").lower()
    if kind in (THINKING_LEVEL, THINKING_BUDGET, THINKING_NONE):
        thinking, source = kind, "env"
    levels = _pairs(env.get("GATEWAY_MODEL_LEVELS") or "").get(name, "")
    if levels:
        given = _levels_list(levels)
        if given:
            accepts, source = given, "env"
    raw_cap = _pairs(env.get("GATEWAY_MODEL_CAPS") or "").get(name, "")
    if raw_cap.isdigit():
        cap = int(raw_cap)
    return ModelProfile(name or base.model, base.family, thinking, accepts,
                        cap=cap, fit=base.fit, source=source, facts=base.facts,
                        skill_budget=base.skill_budget,
                        context_tokens=base.context_tokens)


def skill_retrieval_for(model: str, stop: str,
                        env: dict[str, str] | None = None) -> tuple[int, int]:
    """(chunks, chars) of searchable-skill context one turn may put
    in the prompt on this engine at this dial stop — the profile's
    fold (see RETRIEVAL_FOLD)."""
    return profile_for(model, env).retrieval(stop)


def whole_load_chars_for(model: str,
                         env: dict[str, str] | None = None) -> int:
    """The longest skill this engine takes whole, in characters (its
    share of the context window); the global SAHS_MAX_SKILL_CHARS
    ceiling is applied on top by the loader."""
    return profile_for(model, env).whole_load_chars


# ── the sampling policy ───────────────────────────────────────
# Google's Gemini 3 guidance: keep the temperature at the model's
# default (1.0); lowering it on a thinking model invites loops and
# weaker reasoning, on structured one-shots too. The harness's JSON
# calls were tuned at 0.0–0.3 for 2.5, so on a Gemini 3 engine they
# leave the field out. SAHS_TEMPERATURE_POLICY=explicit sends the
# caller's number anyway (to compare on the evals).
TEMPERATURE_POLICIES = ("default", "explicit")


def temperature_for(model: str, wanted: float | None,
                    env: dict[str, str] | None = None) -> float | None:
    env = dict(os.environ if env is None else env)
    policy = (env.get("SAHS_TEMPERATURE_POLICY") or "default").strip().lower()
    if policy == "explicit":
        return wanted
    if family_of(model) == FAMILY_GEMINI_3:
        return None
    return wanted


# ── the prompt style per family ───────────────────────────────
# Gemini 3 answers a direct brief best and is terse by default; asking
# it to "think step by step" adds nothing to a model that already
# thinks. One short section, stable across turns so the prefix caches.
STYLES: dict[str, str] = {
    FAMILY_GEMINI_3: (
        "Lead with the answer: no preamble, no restating the ask, no "
        "recap of what you did. Match the length to the question — a "
        "short question gets a short answer, a deliverable gets the "
        "whole thing. When tool calls are independent, make them in "
        "the same turn. Do not narrate a plan before acting and do not "
        "describe your own reasoning; the tool rows and the receipts "
        "show the work."),
}


def prompt_style(model: str) -> str:
    """The style section for a model's family; empty when the family
    has none (the shared prompt already speaks the older dialect)."""
    return STYLES.get(family_of(model), "")
