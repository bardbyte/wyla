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

Gemini 3.1 Pro capabilities actually used (not left on the table):
  * THINKING — `thinking_config` enabled by default (dynamic budget);
    enrichment is a reasoning task (calibrated confidence, evidence
    citation), not extraction. `GEMINI_THINKING_BUDGET=0` disables;
    endpoints that reject thinking degrade gracefully (one retry
    without, then thinking stays off for the run).
  * LONG CONTEXT — evidence window defaults to 400K chars (~100K
    tokens) instead of starving a 1M-token model; truncation, when it
    happens, is EXPLICIT (an in-prompt marker tells the model evidence
    was cut so it abstains instead of guessing).
    `GEMINI_MAX_CONTEXT_CHARS` overrides.
  * CORRECTIVE RETRY — skill.md promises "you'll be re-prompted with
    the validation error"; this client honors it: exactly one
    re-prompt carrying the parse/validation failure, then the original
    in-band failure stands.

Env: the four standard GOOGLE_* vars + GEMINI_MODEL,
GEMINI_THINKING_BUDGET, GEMINI_MAX_CONTEXT_CHARS.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from synapse.enrichment.schemas import EnrichmentBundle, SelfAssessment


def _tls_mode() -> str:
    """TLS handling for the corporate-proxy path, decided from env.

    Corporate networks MITM all HTTPS; Python's bundled CAs don't trust
    the corp root, so Vertex calls die with 'certificate verify failed'
    (probe_vertex_readiness.py tells you if you're affected).

      GEMINI_CA_BUNDLE=<pem>   verify against the corporate root CA (safe)
      GEMINI_TLS_INSECURE=1    disable verification — trusted intranet
                               only; same bypass as the probe's --insecure
      (default)                system trust, plus `truststore` (the OS
                               keychain, where corp roots live) if installed
    """
    if os.environ.get("GEMINI_CA_BUNDLE"):
        return "ca-bundle"
    if (os.environ.get("GEMINI_TLS_INSECURE") or "").lower() in (
            "1", "true", "yes"):
        return "insecure"
    return "default"


def _apply_tls(mode: str) -> None:
    """Apply the chosen TLS mode process-wide (idempotent).

    The insecure patch set is lifted from the proven
    scripts/check_vertex_gemini.py: stdlib ssl + httpx (google-genai's
    transport) + google-auth's token-exchange session."""
    if mode == "ca-bundle":
        bundle = str(
            Path(os.environ["GEMINI_CA_BUNDLE"]).expanduser().resolve())
        for var in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE",
                    "CURL_CA_BUNDLE", "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH"):
            os.environ[var] = bundle
        return
    if mode != "insecure":
        try:  # best effort — makes the OS keychain the trust source
            import truststore
            truststore.inject_into_ssl()
        except ImportError:
            pass
        return

    import ssl
    import warnings
    ssl._create_default_https_context = ssl._create_unverified_context  # type: ignore[assignment]
    os.environ["PYTHONHTTPSVERIFY"] = "0"
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except ImportError:
        pass
    try:
        import httpx
        if not getattr(httpx.Client, "_synapse_no_verify", False):
            orig_client, orig_async = httpx.Client, httpx.AsyncClient

            def _client_no_verify(*args: Any, **kwargs: Any) -> Any:
                kwargs.setdefault("verify", False)
                return orig_client(*args, **kwargs)

            def _async_no_verify(*args: Any, **kwargs: Any) -> Any:
                kwargs.setdefault("verify", False)
                return orig_async(*args, **kwargs)

            _client_no_verify._synapse_no_verify = True  # type: ignore[attr-defined]
            _async_no_verify._synapse_no_verify = True  # type: ignore[attr-defined]
            httpx.Client = _client_no_verify  # type: ignore[misc]
            httpx.AsyncClient = _async_no_verify  # type: ignore[misc]
    except ImportError:
        pass
    try:
        import google.auth.transport.requests as gat
        if not getattr(gat.AuthorizedSession, "_synapse_no_verify", False):
            _orig_session = gat.AuthorizedSession

            class _NoVerifyAuthorizedSession(_orig_session):  # type: ignore[misc, valid-type]
                _synapse_no_verify = True

                def __init__(self, *args: Any, **kwargs: Any) -> None:
                    super().__init__(*args, **kwargs)
                    self.verify = False

            gat.AuthorizedSession = _NoVerifyAuthorizedSession  # type: ignore[misc]
    except ImportError:
        pass
    warnings.warn(
        "GEMINI_TLS_INSECURE=1 — TLS verification disabled for Vertex "
        "calls; only safe behind a trusted corporate proxy.",
        stacklevel=2)

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


_DEFAULT_MAX_CONTEXT_CHARS = 400_000   # ~100K tokens; Gemini 3.1 Pro takes 1M
_TRUNCATION_MARKER = (
    "\n...[EVIDENCE TRUNCATED HERE — anything not shown above is ABSENT, "
    "not implied; abstain rather than guess about unseen columns]"
)

# notes parse_bundle_text attaches on failure — the corrective-retry trigger
_FAILURE_PREFIXES = (
    "llm returned no JSON object",
    "llm JSON unparseable",
    "llm JSON was not an object",
    "bundle failed validation",
)


def _serialize_context(context: dict[str, Any], max_chars: int) -> str:
    """Context → JSON for the prompt. Truncation is explicit, never silent."""
    text = json.dumps(context, indent=1, default=str)
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + _TRUNCATION_MARKER


def _failure_note(bundle: EnrichmentBundle) -> str | None:
    """The parse/validation error carried by an empty bundle, if any."""
    if (bundle.column_observations or bundle.candidate_synonyms
            or bundle.candidate_code_resolutions
            or bundle.table_description_proposal):
        return None
    for note in bundle.self_assessment.requires_steward_attention:
        if note.startswith(_FAILURE_PREFIXES):
            return note
    return None


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
        self.tls_mode = _tls_mode()
        _apply_tls(self.tls_mode)     # BEFORE the client builds its httpx
        self._client = genai.Client()
        self.model = model or os.environ.get(
            "GEMINI_MODEL", "gemini-3.1-pro-preview")
        self.temperature = temperature
        # -1 = dynamic (model decides how hard to think); 0 = off
        self.thinking_budget = int(
            os.environ.get("GEMINI_THINKING_BUDGET", "-1"))
        self.max_context_chars = int(os.environ.get(
            "GEMINI_MAX_CONTEXT_CHARS", str(_DEFAULT_MAX_CONTEXT_CHARS)))
        self.stats = {"calls": 0, "corrective_retries": 0,
                      "thinking_fallbacks": 0}

    def _generate(self, prompt: str) -> str:
        """One model call. Thinking on by default; endpoints that reject
        it get one retry without, and thinking stays off for the run."""
        config_kwargs: dict[str, Any] = {
            "temperature": self.temperature,
            "response_mime_type": "application/json",
        }
        if self.thinking_budget != 0:
            config_kwargs["thinking_config"] = self._types.ThinkingConfig(
                thinking_budget=self.thinking_budget)
        try:
            response = self._client.models.generate_content(
                model=self.model, contents=prompt,
                config=self._types.GenerateContentConfig(**config_kwargs),
            )
        except Exception as exc:
            if "thinking" in str(exc).lower() and "thinking_config" in config_kwargs:
                self.stats["thinking_fallbacks"] += 1
                self.thinking_budget = 0        # don't re-fail every call
                config_kwargs.pop("thinking_config")
                response = self._client.models.generate_content(
                    model=self.model, contents=prompt,
                    config=self._types.GenerateContentConfig(**config_kwargs),
                )
            else:
                raise
        return response.text or ""

    def enrich(self, *, skill_md: str, context: dict[str, Any],
               table_name: str) -> EnrichmentBundle:
        batch = context.get("batch") or {}
        prompt = _PROMPT_TEMPLATE.format(
            skill_md=skill_md,
            table_name=table_name,
            chunk=batch.get("chunk", 1),
            of=batch.get("of", 1),
            n_cols=batch.get("columns_in_chunk", "all"),
            context_json=_serialize_context(context, self.max_context_chars),
        )
        self.stats["calls"] += 1
        try:
            text = self._generate(prompt)
        except Exception as exc:  # network/model error → in-band failure
            return _empty_bundle(
                table_name, f"vertex call failed: {str(exc)[:200]}")
        bundle = parse_bundle_text(text, table_name)

        # skill.md's promise, honored: ONE corrective re-prompt carrying
        # the exact parse/validation error. Recovers total losses; a
        # second failure keeps the original in-band note.
        note = _failure_note(bundle)
        if note is None:
            return bundle
        self.stats["corrective_retries"] += 1
        self.stats["calls"] += 1
        retry_prompt = (
            f"{prompt}\n\nYOUR PREVIOUS RESPONSE FAILED VALIDATION: {note}\n"
            "Return ONLY the corrected JSON object matching the "
            "EnrichmentBundle schema. No prose, no fences."
        )
        try:
            retry_text = self._generate(retry_prompt)
        except Exception as exc:
            bundle.self_assessment.requires_steward_attention.append(
                f"corrective retry also failed: {str(exc)[:160]}")
            return bundle
        retry_bundle = parse_bundle_text(retry_text, table_name)
        if _failure_note(retry_bundle) is None:
            retry_bundle.self_assessment.requires_steward_attention.append(
                f"recovered after corrective retry ({note})")
            return retry_bundle
        return bundle


class TieredLLMClient:
    """Route column chunks across the Gemini family — PRO where reasoning
    lives, FLASH where breadth lives.

    Chunk 1 of every table carries the semantic load: the table
    description, cross-table relates_to, synonyms, and the scope digest
    against which they must ground → PRO. Chunks 2..N of wide tables are
    mechanical per-column observation work → FLASH (≈10x cheaper/faster,
    and the grounding gate catches its mistakes the same way). Narrow
    tables (single chunk) go entirely to PRO — they're the semantically
    dense ones.

    Concretely: risk_pers_acct_history (1,404 columns, 36 chunks) costs
    1 PRO + 35 FLASH calls instead of 36 PRO calls.

    Same LLMClient protocol — the enricher never knows. Routing reads the
    `batch` metadata the enricher already sends. With no flash model
    configured, everything falls back to PRO (probe first, then set
    GEMINI_MODEL_FLASH from its recommendation).
    """

    def __init__(self, pro_model: str | None = None,
                 flash_model: str | None = None,
                 temperature: float = 0.0) -> None:
        self.pro_model = (pro_model
                          or os.environ.get("GEMINI_MODEL_PRO")
                          or os.environ.get("GEMINI_MODEL",
                                            "gemini-3.1-pro-preview"))
        self.flash_model = (flash_model
                            or os.environ.get("GEMINI_MODEL_FLASH"))
        self._pro = VertexLLMClient(model=self.pro_model,
                                    temperature=temperature)
        self._flash = (VertexLLMClient(model=self.flash_model,
                                       temperature=temperature)
                       if self.flash_model else self._pro)
        self.tls_mode = self._pro.tls_mode

    @property
    def stats(self) -> dict[str, int]:
        merged = {
            key: self._pro.stats[key] + (
                self._flash.stats[key] if self._flash is not self._pro else 0)
            for key in self._pro.stats
        }
        merged["pro_calls"] = self._pro.stats["calls"]
        merged["flash_calls"] = (self._flash.stats["calls"]
                                 if self._flash is not self._pro else 0)
        return merged

    def enrich(self, *, skill_md: str, context: dict[str, Any],
               table_name: str) -> EnrichmentBundle:
        chunk = (context.get("batch") or {}).get("chunk", 1)
        client = self._pro if chunk == 1 else self._flash
        return client.enrich(skill_md=skill_md, context=context,
                             table_name=table_name)
