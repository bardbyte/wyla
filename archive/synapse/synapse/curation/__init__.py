"""Curation — assemble evidence, prompt LLM, parse + render for review.

Five modules:
    bundle.py    — assemble EvidenceBundle from all seven sources
    prompt.py    — build the structured prompt (deterministic)
    llm.py       — call Vertex Gemini (gated to env; dry-run otherwise)
    parser.py    — parse LLM YAML output into CurationProposal
    review.py    — render ENTITIES_FOR_REVIEW.md for the human
"""

from synapse.curation.bundle import assemble_evidence_bundle
from synapse.curation.prompt import build_prompt, prompt_sha256
from synapse.curation.parser import parse_llm_output
from synapse.curation.review import render_review_markdown

__all__ = [
    "assemble_evidence_bundle",
    "build_prompt",
    "prompt_sha256",
    "parse_llm_output",
    "render_review_markdown",
]
