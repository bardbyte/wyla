"""Vertex Gemini client for the enrichment pass — synapse-native.

Implements the ``LLMClient`` protocol the enricher expects, so
`pipeline.py --enrich` needs no sys.path reach into semantic-graph.

Design notes carried over from the proven semantic-graph client:
  * `response_mime_type="application/json"` but deliberately NOT
    `response_schema` — Vertex rejects pydantic's $ref/anyOf output.
  * tolerant parse: strip markdown fences, find the outermost JSON
    object, coerce into EnrichmentBundle with defaults filled.
  * one table batch failing returns an EMPTY bundle with the error in
    `requires_steward_attention` — the run continues; failures are
    in-band data, never exceptions.

Env: the four standard GOOGLE_* vars + GEMINI_MODEL.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

from synapse.enrichment.schemas import EnrichmentBundle, SelfAssessment

_PROMPT_TEMPLATE = """{skill_md}

────────────────────────────────────────────
TABLE: {table_name}
BATCH: columns {chunk} of {of} (this batch: {n_cols} columns)

CONTEXT (the ONLY evidence you may use):
{context_json}
────────────────────────────────────────────

Emit ONE JSON object matching the EnrichmentBundle schema described in
the skill. JSON only — no prose, no markdown fences.
"""


def _empty_bundle(table_name: str, note: str) -> EnrichmentBundle:
    return EnrichmentBundle(
        table_name=table_name,
        self_assessment=SelfAssessment(
            tables_skipped_for_lack_of_signal=[],
            columns_marked_ambiguous=0,
            proposed_entities_with_low_evidence=[],
            requires_steward_attention=[note] if note else [],
        ),
    )


def parse_bundle_text(text: str, table_name: str) -> EnrichmentBundle:
    """LLM output text → EnrichmentBundle, tolerantly.

    Strips fences/preamble, extracts the outermost JSON object, fills the
    required self_assessment when the model omitted it, forces
    table_name. Unparseable → empty bundle with the reason attached.
    """
    cleaned = re.sub(r"```(?:json)?", "", text or "").strip().strip("`")
    start, end = cleaned.find("{"), cleaned.rfind("}")
    if start < 0 or end <= start:
        return _empty_bundle(table_name, "llm returned no JSON object")
    try:
        payload = json.loads(cleaned[start:end + 1])
    except json.JSONDecodeError as exc:
        return _empty_bundle(table_name, f"llm JSON unparseable: {exc}")
    if not isinstance(payload, dict):
        return _empty_bundle(table_name, "llm JSON was not an object")
    payload["table_name"] = table_name
    payload.setdefault("self_assessment", {
        "tables_skipped_for_lack_of_signal": [],
        "columns_marked_ambiguous": 0,
        "proposed_entities_with_low_evidence": [],
        "requires_steward_attention": [],
    })
    try:
        return EnrichmentBundle.model_validate(payload)
    except Exception as exc:  # schema drift → keep the run alive
        return _empty_bundle(
            table_name, f"bundle failed validation: {str(exc)[:200]}")


class VertexLLMClient:
    """Real Gemini-on-Vertex implementation of the LLMClient protocol."""

    def __init__(self, model: str | None = None,
                 temperature: float = 0.0) -> None:
        try:
            from google import genai  # lazy — laptop dependency
            from google.genai import types
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "google-genai is required for --enrich: pip install "
                "google-genai (plus GOOGLE_GENAI_USE_VERTEXAI=true and "
                "the GOOGLE_CLOUD_* env vars)"
            ) from exc
        self._types = types
        self._client = genai.Client()
        self.model = model or os.environ.get(
            "GEMINI_MODEL", "gemini-3.1-pro-preview")
        self.temperature = temperature

    def enrich(self, *, skill_md: str, context: dict[str, Any],
               table_name: str) -> EnrichmentBundle:
        batch = context.get("batch") or {}
        prompt = _PROMPT_TEMPLATE.format(
            skill_md=skill_md,
            table_name=table_name,
            chunk=batch.get("chunk", 1),
            of=batch.get("of", 1),
            n_cols=batch.get("columns_in_chunk", "all"),
            context_json=json.dumps(context, indent=1, default=str)[:60_000],
        )
        try:
            response = self._client.models.generate_content(
                model=self.model,
                contents=prompt,
                config=self._types.GenerateContentConfig(
                    temperature=self.temperature,
                    response_mime_type="application/json",
                ),
            )
            text = response.text or ""
        except Exception as exc:  # network/model error → in-band failure
            return _empty_bundle(
                table_name, f"vertex call failed: {str(exc)[:200]}")
        return parse_bundle_text(text, table_name)
