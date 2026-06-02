"""Parse LLM YAML output into CurationProposal.

Defensive: strips markdown fences if the model added them; tolerates a
leading prose preamble; raises ValueError with the offending text on
schema violation."""

from __future__ import annotations

import datetime as _dt
import re

import yaml
from pydantic import ValidationError

from synapse.registry import CurationProposal


_FENCE_RE = re.compile(r"```(?:yaml|yml)?\s*\n(.*?)```", re.DOTALL)


def _strip_fences(text: str) -> str:
    """If the model wrapped its YAML in ```yaml fences, extract the body."""
    m = _FENCE_RE.search(text)
    if m:
        return m.group(1).strip()
    return text.strip()


def _strip_preamble(text: str) -> str:
    """Find the first line that looks like the start of our YAML
    (`proposed_entities:` at column 0) and drop everything before it."""
    for i, line in enumerate(text.splitlines()):
        if line.strip().startswith("proposed_entities"):
            return "\n".join(text.splitlines()[i:])
    return text


def parse_llm_output(
    raw: str, *, model_used: str = "", prompt_sha: str = "",
) -> CurationProposal:
    """Parse a raw LLM response string into a typed CurationProposal."""
    body = _strip_fences(raw)
    body = _strip_preamble(body)

    try:
        data = yaml.safe_load(body)
    except yaml.YAMLError as e:
        raise ValueError(
            f"LLM output is not valid YAML: {e}\n"
            f"---first 500 chars---\n{body[:500]}"
        ) from e

    if not isinstance(data, dict):
        raise ValueError(
            f"LLM output must be a top-level mapping, got {type(data).__name__}"
        )

    # Tolerate missing optional top-level keys
    data.setdefault("ambiguities_flagged", [])
    data.setdefault("scope_observations", [])
    if "proposed_entities" not in data:
        raise ValueError("LLM output missing required 'proposed_entities' key")

    try:
        proposal = CurationProposal(
            proposed_entities=data["proposed_entities"],
            ambiguities_flagged=data["ambiguities_flagged"],
            scope_observations=data["scope_observations"],
            generated_at=_dt.datetime.now(_dt.timezone.utc).isoformat(),
            model_used=model_used,
            prompt_sha256=prompt_sha,
        )
    except ValidationError as e:
        raise ValueError(
            f"LLM output does not match CurationProposal schema:\n{e}\n"
            f"---first 500 chars---\n{body[:500]}"
        ) from e

    return proposal
