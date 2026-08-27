"""SVC-ID connectivity — the silo's one sanctioned carry-over, re-created.

Meridian talks to exactly two Google surfaces: BigQuery (dry-run grading,
the gated live path) and Vertex (the L5 model, later). Access is through
service-account key files resolved from the SAME environment variables the
laptop already has configured — the contract carries over; the code is
fresh and owns nothing else.

Resolution order (first hit wins), matching the existing laptop setup:

    BQ key       LUMI_BQ_SA_KEY      → GOOGLE_APPLICATION_CREDENTIALS
    Vertex key   LUMI_VERTEX_SA_KEY  → GOOGLE_APPLICATION_CREDENTIALS
    BQ project   BQ_PROJECT_ID → LUMI_BQ_PROJECT → GOOGLE_CLOUD_PROJECT
    BQ endpoint  BIGQUERY_API_BASE_URL → BIGQUERY_URL → enterprise PSC default
    BQ location  BQ_LOCATION → "US"

Vertex resolution honors the PROVEN laptop contract (the ADK apps and
check_vertex_gemini.py that already ran against prj-d-ea-poc): the
standard GOOGLE_* names, location "global" (Vertex's globally-routed
endpoint — the right default for the Gemini previews), GEMINI_MODEL,
and the GEMINI_* TLS knobs. Silo-first names win when both are set:

    Vertex project   VERTEX_PROJECT_ID → LUMI_VERTEX_PROJECT
                     → GOOGLE_CLOUD_PROJECT      (never a BQ_* var)
    Vertex location  VERTEX_LOCATION → LUMI_VERTEX_LOCATION
                     → GOOGLE_CLOUD_LOCATION → "global"
    Vertex model     VERTEX_MODEL → LUMI_VERTEX_MODEL → GEMINI_MODEL
                     → "gemini-3.1-pro-preview" (the proven default)
    Vertex URL       VERTEX_API_BASE_URL → derived from location
                     (global → aiplatform.googleapis.com; regional →
                      {location}-aiplatform.googleapis.com)
    Vertex TLS       GEMINI_CA_BUNDLE → REQUESTS_CA_BUNDLE →
                     SSL_CERT_FILE; insecure opt-in via
                     GEMINI_TLS_INSECURE=1 or BQ_SSL_NO_VERIFY=1;
                     `truststore` (the OS keychain, where corporate
                     roots live) engages best-effort in every mode

No global credential mutation happens on import; callers ask for what they
need. Missing configuration is reported as a typed error (exit code 3 —
env/auth — per the E10 console contract), never a stack trace.
"""

from __future__ import annotations

import os
import ssl
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

DEFAULT_BQ_ENDPOINT = "https://bigquery-prod.p.googleapis.com"
DEFAULT_VERTEX_ENDPOINT = "https://aiplatform.googleapis.com"
DEFAULT_BQ_LOCATION = "US"


class AuthError(RuntimeError):
    """Environment/auth misconfiguration — maps to exit code 3."""


def load_dotenv(path: Path | None = None) -> list[str]:
    """Read a ``.env`` file into ``os.environ`` (the laptop keeps its
    three BQ variables there — same flow as the proven bq_connect.py).
    NEVER overrides variables already exported in the shell. Search
    order: explicit path → $SAHS_ENV_FILE → <silo root>/.env → ./.env.
    Returns the variable names that were loaded."""
    candidates = [path] if path else []
    if os.environ.get("SAHS_ENV_FILE"):
        candidates.append(Path(os.environ["SAHS_ENV_FILE"]))
    candidates += [Path(__file__).resolve().parents[2] / ".env",
                   Path(".env")]
    loaded: list[str] = []
    for candidate in candidates:
        if candidate is None or not candidate.is_file():
            continue
        for line in candidate.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip().removeprefix("export ").strip()
            value = value.strip().strip("'\"")
            if key and key not in os.environ:
                os.environ[key] = value
                loaded.append(key)
        break                       # first .env found wins
    return loaded


def configure_network(endpoint: str) -> dict[str, str]:
    """Corporate-proxy handling, same contract as the laptop's
    bq_connect.py: inject the Google hostnames into NO_PROXY so BQ and
    OAuth calls go DIRECT (the corporate proxy breaks the handshake
    against Google's private endpoints). Overrides: BQ_FORCE_PROXY=1
    skips the injection (everything through the proxy);
    BQ_DISABLE_PROXY=1 drops the proxy entirely for this process.
    Returns a summary for display."""
    summary: dict[str, str] = {}
    if os.environ.get("BQ_DISABLE_PROXY") == "1":
        for name in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy",
                     "https_proxy"):
            os.environ.pop(name, None)
        summary["proxy"] = "disabled (BQ_DISABLE_PROXY=1)"
        return summary
    if os.environ.get("BQ_FORCE_PROXY") == "1":
        summary["proxy"] = "forced through proxy (BQ_FORCE_PROXY=1)"
        return summary
    host = urlparse(endpoint).hostname or ""
    hosts = {host, "oauth2.googleapis.com", "www.googleapis.com",
             "googleapis.com"}
    hosts.discard("")
    for name in ("NO_PROXY", "no_proxy"):
        existing = [h for h in os.environ.get(name, "").split(",") if h]
        merged = existing + sorted(h for h in hosts if h not in existing)
        os.environ[name] = ",".join(merged)
    summary["proxy"] = f"direct for: {', '.join(sorted(hosts))}"
    return summary


def resolve_ssl() -> tuple[bool, str | None]:
    """→ (verify, ca_bundle). ``BQ_SSL_NO_VERIFY=1`` disables TLS
    verification entirely (explicit opt-in for corporate TLS
    interception when the CA bundle isn't available — the CA-bundle
    route via REQUESTS_CA_BUNDLE / SSL_CERT_FILE is always preferred
    when you have the cert)."""
    if os.environ.get("BQ_SSL_NO_VERIFY") == "1":
        return False, None
    bundle = _first_env("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE")
    return True, bundle


def _first_env(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value and value.strip():
            return value.strip()
    return None


def resolve_bq_key_path() -> Path | None:
    v = _first_env("LUMI_BQ_SA_KEY", "GOOGLE_APPLICATION_CREDENTIALS")
    return Path(v).expanduser() if v else None


def resolve_vertex_key_path() -> Path | None:
    v = _first_env("LUMI_VERTEX_SA_KEY", "GOOGLE_APPLICATION_CREDENTIALS")
    return Path(v).expanduser() if v else None


def resolve_bq_project() -> str | None:
    return _first_env("BQ_PROJECT_ID", "LUMI_BQ_PROJECT",
                      "GOOGLE_CLOUD_PROJECT")


def resolve_bq_endpoint() -> str:
    v = _first_env("BIGQUERY_API_BASE_URL", "BIGQUERY_URL")
    return (v or DEFAULT_BQ_ENDPOINT).rstrip("/")


DEFAULT_VERTEX_LOCATION = "global"     # the proven laptop default —
                                       # Vertex's globally-routed
                                       # endpoint, right for the
                                       # Gemini previews
DEFAULT_VERTEX_MODEL = "gemini-3.1-pro-preview"   # proven reachable
                                                  # from prj-d-ea-poc


def resolve_vertex_endpoint(location: str = "") -> str:
    """VERTEX_API_BASE_URL wins (PSC); otherwise derived from the
    location — REST requires the REGIONAL host for a regional
    location, while "global" uses the plain host."""
    v = _first_env("VERTEX_API_BASE_URL")
    if v:
        return v.rstrip("/")
    location = (location or "").strip().lower()
    if location and location != "global":
        return f"https://{location}-aiplatform.googleapis.com"
    return DEFAULT_VERTEX_ENDPOINT


def resolve_vertex_project() -> str | None:
    """Never a BQ_* variable: the laptop's Vertex SVC-ID lives in a
    DIFFERENT project (prj-d-ea-poc) than the BQ dry-run one, and a
    silent BQ fallback would bill (and fail) against the wrong
    project. GOOGLE_CLOUD_PROJECT is accepted because the proven ADK
    setup already sets it to the VERTEX project."""
    return _first_env("VERTEX_PROJECT_ID", "LUMI_VERTEX_PROJECT",
                      "GOOGLE_CLOUD_PROJECT")


def resolve_vertex_location() -> str:
    return _first_env("VERTEX_LOCATION", "LUMI_VERTEX_LOCATION",
                      "GOOGLE_CLOUD_LOCATION") or DEFAULT_VERTEX_LOCATION


def resolve_vertex_model() -> str:
    return _first_env("VERTEX_MODEL", "LUMI_VERTEX_MODEL",
                      "GEMINI_MODEL") or DEFAULT_VERTEX_MODEL


def resolve_vertex_ssl() -> tuple[bool, str | None]:
    """Vertex layers the proven GEMINI_* knobs over the shared ones:
    GEMINI_TLS_INSECURE=1 (or BQ_SSL_NO_VERIFY=1) disables
    verification; GEMINI_CA_BUNDLE → REQUESTS_CA_BUNDLE →
    SSL_CERT_FILE names the corporate root."""
    insecure = (os.environ.get("GEMINI_TLS_INSECURE") or "").lower()
    if insecure in ("1", "true", "yes") \
            or os.environ.get("BQ_SSL_NO_VERIFY") == "1":
        return False, None
    bundle = _first_env("GEMINI_CA_BUNDLE", "REQUESTS_CA_BUNDLE",
                        "SSL_CERT_FILE")
    return True, bundle


def resolve_bq_location() -> str:
    return _first_env("BQ_LOCATION") or DEFAULT_BQ_LOCATION


@dataclass(frozen=True)
class BQConnection:
    """Everything the dry-run substrate / sandbox needs to reach BigQuery."""

    project: str
    endpoint: str
    location: str
    key_path: Path | None
    ssl_verify: bool = True
    ca_bundle: str | None = None

    @classmethod
    def from_env(cls) -> "BQConnection":
        """The full laptop bootstrap, mirroring the proven bq_connect
        flow: .env → validate → resolve endpoint → NO_PROXY injection →
        SSL settings. Fails fast with a typed error (exit 3)."""
        load_dotenv()
        project = resolve_bq_project()
        if not project:
            raise AuthError(
                "no BigQuery project configured — set BQ_PROJECT_ID (or "
                "LUMI_BQ_PROJECT / GOOGLE_CLOUD_PROJECT), e.g. in .env")
        key = resolve_bq_key_path()
        if key is None:
            raise AuthError(
                "no SA key configured — set GOOGLE_APPLICATION_"
                "CREDENTIALS (or LUMI_BQ_SA_KEY) to the key-file path, "
                "e.g. in .env")
        if not key.exists():
            raise AuthError(f"BigQuery SA key not found on disk: {key}")
        endpoint = resolve_bq_endpoint()
        configure_network(endpoint)
        verify, bundle = resolve_ssl()
        return cls(project=project, endpoint=endpoint,
                   location=resolve_bq_location(), key_path=key,
                   ssl_verify=verify, ca_bundle=bundle)

    def ssl_context(self) -> ssl.SSLContext:
        """Context for urllib calls: default verified (with the custom
        CA bundle when configured), or unverified under the explicit
        BQ_SSL_NO_VERIFY=1 opt-in."""
        if not self.ssl_verify:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            return context
        return ssl.create_default_context(cafile=self.ca_bundle)

    def token_session(self):
        """requests.Session for the OAuth token refresh, honoring the
        same verify/CA settings (used only against oauth2.googleapis.com
        — the laptop contract's Step 6)."""
        import requests                          # ships with google-auth
        session = requests.Session()
        session.verify = (self.ca_bundle or True) if self.ssl_verify \
            else False
        return session


@dataclass(frozen=True)
class VertexConnection:
    """Everything the B1 enricher needs to reach the Vertex model.

    A SEPARATE contract from BQConnection on purpose: the laptop's
    Vertex SVC-ID and project are different from the BQ dry-run ones.
    The key resolution keeps GOOGLE_APPLICATION_CREDENTIALS as a
    fallback (one shared key file is a valid setup), but project and
    model never borrow from the BQ side."""

    project: str
    location: str
    model: str
    endpoint: str
    key_path: Path | None
    ssl_verify: bool = True
    ca_bundle: str | None = None
    truststore_active: bool = False

    @classmethod
    def from_env(cls) -> "VertexConnection":
        """Same bootstrap shape as BQConnection: .env → validate →
        resolve endpoint → NO_PROXY injection → SSL settings. Fails
        fast with a typed error (exit 3). ``truststore`` (the OS
        keychain, where corporate root CAs actually live) engages
        best-effort in every mode — the field lesson: it is the clean
        fix for corporate TLS interception, strictly better than
        disabling verification."""
        load_dotenv()
        project = resolve_vertex_project()
        if not project:
            raise AuthError(
                "no Vertex project configured — set VERTEX_PROJECT_ID "
                "(or LUMI_VERTEX_PROJECT / GOOGLE_CLOUD_PROJECT), "
                "e.g. in .env. This is a DIFFERENT project than the "
                "BQ one and is never borrowed from a BQ_* variable")
        key = resolve_vertex_key_path()
        if key is None:
            raise AuthError(
                "no Vertex SA key configured — set LUMI_VERTEX_SA_KEY "
                "(or GOOGLE_APPLICATION_CREDENTIALS) to the key-file "
                "path, e.g. in .env")
        if not key.exists():
            raise AuthError(f"Vertex SA key not found on disk: {key}")
        truststore_active = False
        try:
            import truststore                    # type: ignore
            truststore.inject_into_ssl()
            truststore_active = True
        except ImportError:
            pass
        location = resolve_vertex_location()
        endpoint = resolve_vertex_endpoint(location)
        configure_network(endpoint)
        verify, bundle = resolve_vertex_ssl()
        return cls(project=project, location=location,
                   model=resolve_vertex_model(), endpoint=endpoint,
                   key_path=key, ssl_verify=verify, ca_bundle=bundle,
                   truststore_active=truststore_active)

    def ssl_context(self) -> ssl.SSLContext:
        if not self.ssl_verify:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            return context
        return ssl.create_default_context(cafile=self.ca_bundle)

    def token_session(self):
        import requests                          # ships with google-auth
        session = requests.Session()
        session.verify = (self.ca_bundle or True) if self.ssl_verify \
            else False
        return session
