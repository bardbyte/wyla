"""Service-account auth helpers.

The enterprise pattern: two separate SA keys, one per service
(Vertex / BQ). Loaded explicitly per use instead of the global
``GOOGLE_APPLICATION_CREDENTIALS`` env var — that way one process
can hit both with different identities.

Env vars (read in this order; first hit wins per service):

    Vertex:
        LUMI_VERTEX_SA_KEY            explicit Vertex SA key path
        GOOGLE_APPLICATION_CREDENTIALS  legacy single-key fallback

    BQ:
        LUMI_BQ_SA_KEY                explicit BQ SA key path
        GOOGLE_APPLICATION_CREDENTIALS  legacy single-key fallback

Both helpers return either a usable ``service_account.Credentials`` or
``None`` (caller decides whether None is fatal). They never raise on
missing creds — the caller renders the diagnostic.

TLS: every consumer is expected to call ``inject_truststore()`` at
process start, BEFORE any ``google.*`` import. The function is a
no-op on machines without the corporate root CA in the keychain.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger("synapse.utils.auth")


# ─── TLS ─────────────────────────────────────────────────────


def inject_truststore() -> tuple[bool, str]:
    """Try to inject the OS truststore for corporate-MITM TLS.

    Returns (ok, message). Safe to call multiple times. No-op when
    truststore isn't installed (returns ok=True with a 'not installed'
    note — many machines don't need it)."""
    try:
        import truststore  # type: ignore[import-not-found]
        truststore.inject_into_ssl()
        return True, "truststore.inject_into_ssl() succeeded"
    except ImportError:
        return True, "truststore not installed (fine if not on corporate proxy)"
    except Exception as e:  # noqa: BLE001
        return False, f"truststore inject failed: {type(e).__name__}: {e}"


# ─── Credential resolution ───────────────────────────────────


def resolve_vertex_key_path() -> Path | None:
    """Return the configured Vertex SA key path, or None."""
    for env in ("LUMI_VERTEX_SA_KEY", "GOOGLE_APPLICATION_CREDENTIALS"):
        v = os.environ.get(env)
        if v and Path(v).exists():
            return Path(v)
    return None


def resolve_bq_key_path() -> Path | None:
    """Return the configured BQ SA key path, or None."""
    for env in ("LUMI_BQ_SA_KEY", "GOOGLE_APPLICATION_CREDENTIALS"):
        v = os.environ.get(env)
        if v and Path(v).exists():
            return Path(v)
    return None


def load_vertex_credentials() -> Any | None:
    """Load service-account credentials for Vertex AI. None if missing."""
    p = resolve_vertex_key_path()
    if p is None:
        return None
    try:
        from google.oauth2 import service_account  # type: ignore[import-not-found]
    except ImportError as e:
        logger.warning("google-auth not installed: %s", e)
        return None
    return service_account.Credentials.from_service_account_file(str(p))


def load_bq_credentials() -> Any | None:
    """Load service-account credentials for BigQuery. None if missing."""
    p = resolve_bq_key_path()
    if p is None:
        return None
    try:
        from google.oauth2 import service_account  # type: ignore[import-not-found]
    except ImportError as e:
        logger.warning("google-auth not installed: %s", e)
        return None
    return service_account.Credentials.from_service_account_file(str(p))


# ─── Client factories ────────────────────────────────────────


def build_bq_client(project: str | None = None) -> Any:
    """Build a `google.cloud.bigquery.Client` with the BQ SA key.

    Falls back to ADC if no key path is set. Raises if the bigquery
    package isn't installed."""
    try:
        from google.cloud import bigquery  # type: ignore[import-not-found]
    except ImportError as e:
        raise RuntimeError(
            "google-cloud-bigquery not installed. "
            "Install with: pip install google-cloud-bigquery"
        ) from e

    creds = load_bq_credentials()
    proj = project or os.environ.get("LUMI_BQ_PROJECT")
    if creds is not None:
        return bigquery.Client(project=proj, credentials=creds)
    return bigquery.Client(project=proj)


def build_vertex_genai_client(
    project: str | None = None, location: str | None = None,
) -> Any:
    """Build a google.genai client pointed at Vertex with the Vertex SA key.

    Caller MUST have called `inject_truststore()` before this on
    corporate networks. Raises if google-genai isn't installed."""
    try:
        from google import genai  # type: ignore[import-not-found]
    except ImportError as e:
        raise RuntimeError(
            "google-genai not installed. Install with: pip install google-genai"
        ) from e

    # Hint the SDK to use Vertex (vs the Gemini-API backend)
    os.environ.setdefault("GOOGLE_GENAI_USE_VERTEXAI", "true")

    proj = project or os.environ.get(
        "LUMI_VERTEX_PROJECT", "your-vertex-project",
    )
    loc = location or os.environ.get("LUMI_VERTEX_LOCATION", "global")

    # If we have an explicit Vertex SA key, point GOOGLE_APPLICATION_CREDENTIALS
    # at it for the duration of this client's life (google-genai picks it up
    # from the env). This preserves the single-key fallback path AND lets
    # callers use a Vertex-specific key without rewriting their environment.
    vertex_key = resolve_vertex_key_path()
    if vertex_key is not None:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(vertex_key)

    return genai.Client(vertexai=True, project=proj, location=loc)
