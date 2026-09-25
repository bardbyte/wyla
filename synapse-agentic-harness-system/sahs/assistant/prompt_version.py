"""A deterministic fingerprint of the assembled system prompt.

The system prompt is built from parts (``loop.system_prompt``: an
identity sentence, the chain, the mode, a style section for the
model's family, the graph digest, the skills, the memories and the
session block). ``ASSISTANT_VERSION`` names the prose the code ships;
this fingerprint says which exact prompt a turn ran under — so two
turns are comparable only when their versions match, and a memory
added or a skill pinned is visible as a moved part hash.

The record keeps the prompt text truncated on purpose
(``model_prompt`` n=0 carries the first 12 000 characters), so the
fingerprint is computed here, once, from the whole text and rides on
the same record. Everything is a pure function of the text.
"""

from __future__ import annotations

import hashlib
import re

# the stable prefix ends where the per-turn material begins
PREFIX_BOUNDARY = "skills"
_SECTION = re.compile(r"<([a-z_]+)>\n(.*?)\n</\1>", re.DOTALL)


def _short_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]


def prompt_parts(system: str) -> dict[str, str]:
    """The ``<tag>`` sections of an assembled prompt, in order, tag →
    body. Text outside any section is ignored."""
    return {m.group(1): m.group(2) for m in _SECTION.finditer(system or "")}


def prompt_fingerprint(system: str, version: str) -> dict[str, object]:
    """``prompt_version`` (the version string plus a short hash of the
    stable prefix before ``<skills>``), ``prompt_prefix`` (that hash on
    its own) and ``prompt_parts`` (a short hash per section)."""
    parts = prompt_parts(system)
    text = system or ""
    cut = text.find(f"<{PREFIX_BOUNDARY}>")
    if cut < 0:
        # no skills section this turn: the prefix runs up to memory,
        # the first per-turn part, so the hash stays comparable
        cut = text.find("<memory>")
    prefix = text if cut < 0 else text[:cut]
    prefix_hash = _short_hash(prefix.rstrip())
    return {"prompt_version": f"{version}+{prefix_hash}",
            "prompt_prefix": prefix_hash,
            "prompt_parts": {tag: _short_hash(body)
                             for tag, body in parts.items()}}


__all__ = ["PREFIX_BOUNDARY", "prompt_fingerprint", "prompt_parts"]
