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


@dataclass(frozen=True)
class ModelProfile:
    model: str
    family: str
    thinking: str                  # level | budget | none
    accepts: tuple[str, ...]       # thinkingLevel values, shallow → deep
    cap: int = DEFAULT_CAP
    fit: str = ""                  # where it belongs in the harness
    source: str = "family"         # docs | probe | family | env

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
                "cap": self.cap, "fit": self.fit, "source": self.source}


# ── the known engines ─────────────────────────────────────────
# keyed by the model's stem: gemini-3.1-pro-preview → gemini-3.1-pro
PROFILES: dict[str, ModelProfile] = {
    "gemini-3.1-pro": ModelProfile(
        "gemini-3.1-pro", FAMILY_GEMINI_3, THINKING_LEVEL,
        ("low", "medium", "high"),
        fit="Deep and Extra deep; the multi-step SQL and python turns; "
            "streams on Vertex", source="docs"),
    "gemini-3.7-flash": ModelProfile(
        "gemini-3.7-flash", FAMILY_GEMINI_3, THINKING_LEVEL,
        ("low", "medium", "high"),          # minimal is refused
        fit="Everyday chat at Standard; Quick autopilot at high",
        source="docs"),
    "gemini-3.5-flash": ModelProfile(
        "gemini-3.5-flash", FAMILY_GEMINI_3, THINKING_LEVEL,
        ("medium", "high"),                 # nothing shallower is listed
        fit="The alternate workhorse when 3.7 Flash is not served",
        source="docs"),
    "gemini-3.1-flash-lite": ModelProfile(
        "gemini-3.1-flash-lite", FAMILY_GEMINI_3, THINKING_LEVEL,
        ("minimal", "low", "medium", "high"),
        fit="The one-shot JSON calls (classify, judge, reviews, "
            "suggestions) and Minimal depth", source="docs"),
}

# the family fallbacks, for a model the table does not name
_FAMILY_DEFAULTS: dict[str, ModelProfile] = {
    FAMILY_GEMINI_3: ModelProfile(
        "", FAMILY_GEMINI_3, THINKING_LEVEL, ("low", "medium", "high"),
        fit="A Gemini 3 model the table does not know: the common "
            "three levels until the probe says otherwise"),
    FAMILY_GEMINI_25: ModelProfile(
        "", FAMILY_GEMINI_25, THINKING_BUDGET, (),
        fit="Retiring: a thinking budget under the cap; kept only for "
            "an environment that still names it"),
    FAMILY_OTHER: ModelProfile(
        "", FAMILY_OTHER, THINKING_BUDGET, (),
        fit="Unknown to the table: treated as a budget model"),
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
                        cap, base.fit, source)


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
