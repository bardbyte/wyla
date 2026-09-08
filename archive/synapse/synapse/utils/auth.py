"""Service-account auth helpers (Vertex + BigQuery, enterprise-network safe).

Two-SA-key pattern — Vertex and BQ keys are loaded explicitly per use
rather than via the global GOOGLE_APPLICATION_CREDENTIALS env var, so
one process can hit both with different identities.

ENTERPRISE NETWORK HANDLING (matches the production BQ probe pattern):

    Endpoint:
        BIGQUERY_API_BASE_URL    primary REST endpoint override
        BIGQUERY_URL             fallback REST endpoint override
        (default: bigquery.googleapis.com — public)

    Region:
        BQ_LOCATION              region for jobReference (default "US")

    Project:
        BQ_PROJECT_ID            primary BQ execution project
        LUMI_BQ_PROJECT          fallback synapse-convention name
        GOOGLE_CLOUD_PROJECT     standard GCP env var (last)

    Proxy:
        BQ_FORCE_PROXY=1         keep using whatever proxy env vars say
                                 (skips NO_PROXY injection)
        BQ_DISABLE_PROXY=1       set trust_env=False on token-refresh
                                 session (use when proxy returns 407)
        NO_PROXY / no_proxy      we MERGE Google hosts in unless
                                 BQ_FORCE_PROXY says otherwise

    TLS:
        REQUESTS_CA_BUNDLE       custom CA bundle (preferred)
        SSL_CERT_FILE            CA bundle fallback
        + call inject_truststore() at process start for corporate MITM

OAuth scopes:
        Vertex:  default scope chain (genai SDK handles it)
        BQ:      explicit "https://www.googleapis.com/auth/bigquery"
                 attached to the SA credentials

Both Vertex + BQ key resolvers return a usable Path / None; never raise
on missing creds — the caller decides whether None is fatal.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger("synapse.utils.auth")


# ─── Constants ───────────────────────────────────────────────


# Enterprise-PSC endpoint — what works inside corporate networks.
# Public Google endpoint (bigquery.googleapis.com) is unreachable from
# many enterprise gateways. Override via BIGQUERY_API_BASE_URL when you
# need the public path (e.g. running from a public-internet machine).
DEFAULT_BQ_ENDPOINT = "https://bigquery-prod.p.googleapis.com"
DEFAULT_BQ_LOCATION = "US"
BQ_SCOPE = "https://www.googleapis.com/auth/bigquery"

# Vertex default — overridable via VERTEX_API_BASE_URL. The google-genai
# SDK uses aiplatform.googleapis.com by default; PSC users override.
DEFAULT_VERTEX_ENDPOINT = "https://aiplatform.googleapis.com"
DEFAULT_OAUTH_ENDPOINT = "https://oauth2.googleapis.com"

# Google hosts that should bypass enterprise proxies for auth + API calls
_BYPASS_HOSTS = (
    "oauth2.googleapis.com",
    "oauth2-dev.p.googleapis.com",
    "oauth2-prod.p.googleapis.com",
    "bigquery.googleapis.com",
    "bigquery-dev.p.googleapis.com",
    "bigquery-prod.p.googleapis.com",
    "aiplatform.googleapis.com",
    "generativelanguage.googleapis.com",
)


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
    """Load BQ service-account credentials scoped to BigQuery.

    Explicit scope = `https://www.googleapis.com/auth/bigquery`. The
    bigquery client library would scope when needed via the default
    chain, but pinning here makes the token's intent unambiguous and
    safer against credential reuse."""
    p = resolve_bq_key_path()
    if p is None:
        return None
    try:
        from google.oauth2 import service_account  # type: ignore[import-not-found]
    except ImportError as e:
        logger.warning("google-auth not installed: %s", e)
        return None
    return service_account.Credentials.from_service_account_file(
        str(p), scopes=[BQ_SCOPE],
    )


# ─── BQ network / endpoint resolution (enterprise) ──────────


def _truthy(v: str | None) -> bool:
    return bool(v) and v.strip().lower() in {"1", "true", "yes", "on"}


def resolve_bq_project() -> str | None:
    """BQ_PROJECT_ID → LUMI_BQ_PROJECT → GOOGLE_CLOUD_PROJECT, first hit."""
    for env in ("BQ_PROJECT_ID", "LUMI_BQ_PROJECT", "GOOGLE_CLOUD_PROJECT"):
        v = os.environ.get(env)
        if v:
            return v.strip()
    return None


def resolve_bq_endpoint() -> str:
    """BIGQUERY_API_BASE_URL → BIGQUERY_URL → DEFAULT_BQ_ENDPOINT.

    Trailing slashes stripped so callers can string-concat paths.
    Default is enterprise PSC (bigquery-prod.p.googleapis.com); set
    BIGQUERY_API_BASE_URL=https://bigquery.googleapis.com to use public."""
    for env in ("BIGQUERY_API_BASE_URL", "BIGQUERY_URL"):
        v = os.environ.get(env)
        if v:
            return v.strip().rstrip("/")
    return DEFAULT_BQ_ENDPOINT


def resolve_vertex_endpoint() -> str:
    """VERTEX_API_BASE_URL → DEFAULT_VERTEX_ENDPOINT."""
    v = os.environ.get("VERTEX_API_BASE_URL")
    if v:
        return v.strip().rstrip("/")
    return DEFAULT_VERTEX_ENDPOINT


def resolve_bq_location() -> str:
    return (os.environ.get("BQ_LOCATION") or DEFAULT_BQ_LOCATION).strip()


def setup_bq_network_env(*, verbose: bool = False) -> dict[str, Any]:
    """Apply enterprise BQ network conventions to os.environ.

    Mutations performed:
      - NO_PROXY / no_proxy get the Google host list MERGED in
        (skipped if BQ_FORCE_PROXY=1)
      - If BQ_DISABLE_PROXY=1: signals to downstream code (returned in
        the dict). The actual `requests.Session.trust_env = False` is
        applied by callers that build a Session.
      - If REQUESTS_CA_BUNDLE is unset but SSL_CERT_FILE is set, copy
        SSL_CERT_FILE → REQUESTS_CA_BUNDLE so `requests`-based
        libraries see the bundle.

    Returns a state dict the preflight uses to show resolved config.
    """
    force_proxy = _truthy(os.environ.get("BQ_FORCE_PROXY"))
    disable_proxy = _truthy(os.environ.get("BQ_DISABLE_PROXY"))

    endpoint = resolve_bq_endpoint()
    endpoint_host = urlparse(endpoint).hostname

    bypass_hosts = list(_BYPASS_HOSTS)
    if endpoint_host and endpoint_host not in bypass_hosts:
        bypass_hosts.append(endpoint_host)

    no_proxy_applied: list[str] = []
    if not force_proxy:
        existing = os.environ.get("NO_PROXY") or os.environ.get("no_proxy") or ""
        existing_set = {h.strip() for h in existing.split(",") if h.strip()}
        merged = sorted(existing_set | set(bypass_hosts))
        merged_str = ",".join(merged)
        os.environ["NO_PROXY"] = merged_str
        os.environ["no_proxy"] = merged_str
        no_proxy_applied = merged

    # CA bundle: REQUESTS_CA_BUNDLE wins; if only SSL_CERT_FILE set,
    # mirror it so `requests` library sees it.
    ca_bundle = os.environ.get("REQUESTS_CA_BUNDLE")
    if not ca_bundle:
        ssl_cert = os.environ.get("SSL_CERT_FILE")
        if ssl_cert and Path(ssl_cert).exists():
            os.environ["REQUESTS_CA_BUNDLE"] = ssl_cert
            ca_bundle = ssl_cert

    state = {
        "endpoint": endpoint,
        "endpoint_host": endpoint_host,
        "location": resolve_bq_location(),
        "project": resolve_bq_project(),
        "force_proxy": force_proxy,
        "disable_proxy_for_auth": disable_proxy,
        "ca_bundle": ca_bundle,
        "no_proxy_hosts": no_proxy_applied,
    }
    if verbose:
        logger.info("BQ network state: %s", state)
    return state


# ─── Client factories ────────────────────────────────────────


def build_bq_client(
    project: str | None = None,
    *,
    apply_network_env: bool = True,
) -> Any:
    """Build a scoped `google.cloud.bigquery.Client`.

    - Applies enterprise NO_PROXY + CA bundle env defaults via
      `setup_bq_network_env()` (set apply_network_env=False to skip).
    - Routes to custom REST endpoint when BIGQUERY_API_BASE_URL is set.
    - Pins `location` from BQ_LOCATION (or "US" default).
    - Project precedence: explicit arg > BQ_PROJECT_ID > LUMI_BQ_PROJECT
      > GOOGLE_CLOUD_PROJECT.
    - Credentials carry the explicit BigQuery OAuth scope.
    """
    try:
        from google.cloud import bigquery  # type: ignore[import-not-found]
    except ImportError as e:
        raise RuntimeError(
            "google-cloud-bigquery not installed. "
            "Install with: pip install google-cloud-bigquery"
        ) from e

    if apply_network_env:
        setup_bq_network_env()

    proj = project or resolve_bq_project()
    location = resolve_bq_location()
    endpoint = resolve_bq_endpoint()
    creds = load_bq_credentials()

    kwargs: dict[str, Any] = {"project": proj, "location": location}
    if creds is not None:
        kwargs["credentials"] = creds

    # Honor custom endpoint only when overridden — default routes via
    # the standard public host.
    if endpoint and endpoint != DEFAULT_BQ_ENDPOINT:
        try:
            from google.api_core.client_options import (  # type: ignore[import-not-found]
                ClientOptions,
            )
            kwargs["client_options"] = ClientOptions(api_endpoint=endpoint)
        except ImportError:
            logger.warning(
                "google.api_core.client_options unavailable — "
                "BIGQUERY_API_BASE_URL ignored",
            )

    return bigquery.Client(**kwargs)


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
