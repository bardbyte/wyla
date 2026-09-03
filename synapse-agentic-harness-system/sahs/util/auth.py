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
    BQ data proj BQ_DATA_PROJECT → LUMI_BQ_DATA_PROJECT → the BQ project
                 (the project that HOSTS the tables, e.g. axp-lumi, when
                 it differs from the one that runs and bills the query)

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

import base64
import os
import ssl
import urllib.request
from typing import Any
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse, unquote

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


def env_proxies() -> dict[str, str]:
    """The corporate proxy as the environment declares it (HTTPS_PROXY /
    HTTP_PROXY), for the plane that rides it. Read, never written."""
    out: dict[str, str] = {}
    https = _first_env("HTTPS_PROXY", "https_proxy")
    http = _first_env("HTTP_PROXY", "http_proxy")
    if https:
        out["https"] = https
    if http:
        out["http"] = http
    return out


def bq_proxies() -> dict[str, str]:
    """The BigQuery plane's route: DIRECT by default — the PSC endpoint
    and its OAuth token endpoint resolve privately and the corporate
    proxy breaks the handshake (the laptop's bq_connect contract).
    ``BQ_FORCE_PROXY=1`` rides the proxy; ``BQ_DISABLE_PROXY=1`` is
    direct as well."""
    if os.environ.get("BQ_DISABLE_PROXY") == "1":
        return {}
    if os.environ.get("BQ_FORCE_PROXY") == "1":
        return env_proxies()
    return {}


def vertex_proxies() -> dict[str, str]:
    """The Vertex plane's route: VIA the corporate proxy as the
    environment declares it (the proven contract: token call and model
    call both ride it). ``VERTEX_DISABLE_PROXY=1`` and
    ``VERTEX_NO_PROXY_GOOGLE=1`` are direct — every Vertex host is a
    Google host, so "direct for Google" is direct."""
    if os.environ.get("VERTEX_DISABLE_PROXY") == "1" \
            or os.environ.get("VERTEX_NO_PROXY_GOOGLE") == "1":
        return {}
    return env_proxies()


def configure_network(endpoint: str) -> dict[str, str]:
    """The BigQuery plane's route, for display. NOTHING is written to
    the process environment any more: the route is pinned on the
    connection (``BQConnection.proxies`` / ``opener()``), so it can
    never leak into the Vertex plane. It used to: the first dry run
    injected googleapis.com into NO_PROXY, and every model call after
    it went DIRECT to Vertex — which the corporate network blackholes —
    so the turn hung right after a successful dry run."""
    if os.environ.get("BQ_DISABLE_PROXY") == "1":
        return {"proxy": "disabled (BQ_DISABLE_PROXY=1): direct"}
    if os.environ.get("BQ_FORCE_PROXY") == "1":
        proxy = env_proxies().get("https", "")
        return {"proxy": "forced through proxy "
                         + (redact_url(proxy) if proxy
                            else "(none configured: direct)")
                         + " (BQ_FORCE_PROXY=1)"}
    host = urlparse(endpoint).hostname or ""
    hosts = sorted(h for h in {host, "oauth2.googleapis.com"} if h)
    return {"proxy": f"direct for: {', '.join(hosts)} (pinned on the "
                     "connection; NO_PROXY untouched)"}


def configure_vertex_network(endpoint: str) -> dict[str, str]:
    """The Vertex plane's route, for display: the PROVEN contract
    (check_vertex_gemini.py / the ADK apps that ran against
    prj-d-ea-poc) rides the corporate proxy for the OAuth token call
    and the model call, with truststore fixing the MITM chain.

    This is deliberately the OPPOSITE of the BigQuery plane, and the
    two no longer touch: each connection carries its own route and its
    opener never consults NO_PROXY, so the BigQuery plane's direct
    setting cannot send the Vertex OAuth call direct to
    oauth2.googleapis.com any more (the field symptom: a 120s timeout
    at the auth step, then the same hang on every model call after the
    first dry run).

    Knobs for other topologies: ``VERTEX_DISABLE_PROXY=1`` (direct
    egress) and ``VERTEX_NO_PROXY_GOOGLE=1`` (private DNS where
    googleapis resolves to restricted VIPs) — both direct."""
    if os.environ.get("VERTEX_DISABLE_PROXY") == "1":
        return {"proxy": "disabled (VERTEX_DISABLE_PROXY=1): direct"}
    if os.environ.get("VERTEX_NO_PROXY_GOOGLE") == "1":
        return {"proxy": "direct for Google hosts "
                         "(VERTEX_NO_PROXY_GOOGLE=1)"}
    proxy = env_proxies().get("https", "")
    return {"proxy": (f"via corporate proxy {redact_url(proxy)} "
                      "(the proven contract; pinned on the connection, "
                      "NO_PROXY never consulted)" if proxy
                      else "no proxy configured: direct")}


class PinnedProxyHandler(urllib.request.ProxyHandler):
    """A proxy decision made per connection, never by the environment.

    The stdlib handler asks ``proxy_bypass(host)`` on every request,
    which reads NO_PROXY from the process environment — so one plane's
    direct-connection list silently rerouted the other plane. This
    handler routes exactly what it was given: an empty mapping is a
    direct connection, a mapping is that proxy (credentials in the URL
    become the Proxy-authorization header), and NO_PROXY is never
    consulted."""

    def proxy_open(self, req, proxy, type):            # noqa: A002
        orig_type = req.type
        proxy_type, user, password, hostport = \
            urllib.request._parse_proxy(proxy)         # noqa: SLF001
        if proxy_type is None:
            proxy_type = orig_type
        if user and password:
            user_pass = f"{unquote(user)}:{unquote(password)}"
            creds = base64.b64encode(user_pass.encode()).decode("ascii")
            req.add_header("Proxy-authorization", "Basic " + creds)
        req.set_proxy(unquote(hostport), proxy_type)
        if orig_type == proxy_type or orig_type == "https":
            return None
        return self.parent.open(req, timeout=req.timeout)


def plane_opener(proxies: dict[str, str],
                 context: ssl.SSLContext) -> urllib.request.OpenerDirector:
    """A urllib opener with one plane's route and TLS pinned. Passing
    the handlers replaces build_opener's environment-derived ones."""
    return urllib.request.build_opener(
        PinnedProxyHandler(dict(proxies)),
        urllib.request.HTTPSHandler(context=context))


def describe_route(proxies: dict[str, str]) -> str:
    """'direct' or 'via <proxy>' (credentials redacted), for display."""
    proxy = proxies.get("https") or proxies.get("http") or ""
    return f"via {redact_url(proxy)}" if proxy else "direct"


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


def redact_url(url: str) -> str:
    """Strip embedded credentials from a URL for display —
    ``user:pass@host`` is common in corporate HTTPS_PROXY values and
    must NEVER reach logs, console output, or screenshots."""
    url = (url or "").strip()
    if "@" not in url:
        return url
    scheme, sep, rest = url.partition("://")
    host = rest.rsplit("@", 1)[-1] if sep else url.rsplit("@", 1)[-1]
    return f"{scheme}://{host}" if sep else host


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


def resolve_bq_data_project() -> str | None:
    """The project that HOSTS the tables (``axp-lumi.dw.<table>``),
    when it is not the project that runs and bills the query. The
    graph names tables ``dataset.table``; the sandbox qualifies them
    with this project before any dry run or execution, so a query
    written from the cards resolves where the data actually lives."""
    return _first_env("BQ_DATA_PROJECT", "LUMI_BQ_DATA_PROJECT")


_BQ_CREDENTIALS: dict[str, Any] = {}
_BQ_SCOPES = ["https://www.googleapis.com/auth/bigquery"]


def bq_access_token(connection: "BQConnection", *,
                    make_credentials: Any = None,
                    refresh: Any = None) -> str:
    key = str(connection.key_path)
    creds = _BQ_CREDENTIALS.get(key)
    if creds is None:
        if make_credentials is not None:
            creds = make_credentials()
        else:
            from google.oauth2 import service_account      # type: ignore
            creds = service_account.Credentials.from_service_account_file(
                key, scopes=_BQ_SCOPES)
        _BQ_CREDENTIALS[key] = creds
    if not getattr(creds, "valid", False):
        if refresh is not None:
            refresh(creds)
        else:
            from google.auth.transport.requests import Request  # type: ignore
            creds.refresh(Request(session=connection.token_session()))
    return str(creds.token)


@dataclass(frozen=True)
class BQConnection:
    """Everything the dry-run substrate / sandbox needs to reach BigQuery."""

    project: str
    endpoint: str
    location: str
    key_path: Path | None
    ssl_verify: bool = True
    ca_bundle: str | None = None
    # where the tables live; defaults to the query project
    data_project: str = ""
    # THIS plane's route: {} is direct (the PSC contract), a mapping is
    # the proxy. Pinned here and in opener(); never the environment's
    proxies: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "BQConnection":
        """The full laptop bootstrap, mirroring the proven bq_connect
        flow: .env → validate → resolve endpoint → the route (direct,
        pinned on the connection) → SSL settings. Fails fast with a
        typed error (exit 3). The process environment is read, never
        written."""
        load_dotenv()
        project = resolve_bq_project()
        if not project:
            raise AuthError(
                "no BigQuery project configured: set BQ_PROJECT_ID (or "
                "LUMI_BQ_PROJECT / GOOGLE_CLOUD_PROJECT), e.g. in .env")
        key = resolve_bq_key_path()
        if key is None:
            raise AuthError(
                "no SA key configured: set GOOGLE_APPLICATION_"
                "CREDENTIALS (or LUMI_BQ_SA_KEY) to the key-file path, "
                "e.g. in .env")
        if not key.exists():
            raise AuthError(f"BigQuery SA key not found on disk: {key}")
        endpoint = resolve_bq_endpoint()
        verify, bundle = resolve_ssl()
        return cls(project=project, endpoint=endpoint,
                   location=resolve_bq_location(), key_path=key,
                   ssl_verify=verify, ca_bundle=bundle,
                   data_project=resolve_bq_data_project() or project,
                   proxies=bq_proxies())

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

    def route(self) -> str:
        return describe_route(self.proxies)

    def opener(self) -> urllib.request.OpenerDirector:
        """urllib opener with this plane's route and TLS pinned; the
        environment's NO_PROXY is never consulted."""
        return plane_opener(self.proxies, self.ssl_context())

    def token_session(self):
        """requests.Session for the OAuth token refresh, on this
        plane's route with the same verify/CA settings (used only
        against oauth2.googleapis.com — the laptop contract's Step 6).
        trust_env is off: the route is the connection's, never the
        environment's."""
        import requests                          # ships with google-auth
        session = requests.Session()
        session.trust_env = False
        session.proxies = dict(self.proxies)
        session.verify = (self.ca_bundle or True) if self.ssl_verify \
            else False
        return session

    def token(self, *, make_credentials: Any = None,
              refresh: Any = None) -> str:
        """The OAuth access token, cached per key file and refreshed
        only when expired: a turn with six dry runs makes one token
        trip through the proxy, not six. ``make_credentials`` and
        ``refresh`` are injection points for tests."""
        return bq_access_token(self, make_credentials=make_credentials,
                               refresh=refresh)


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
    # THIS plane's route: the corporate proxy as the environment
    # declares it (the proven contract). Pinned here and in opener()
    proxies: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "VertexConnection":
        """Same bootstrap shape as BQConnection — .env → validate →
        resolve endpoint → network → SSL — but the network step is the
        PROVEN proxy-riding contract, never the BQ NO_PROXY injection.
        Fails fast with a typed error (exit 3). ``truststore`` (the OS
        keychain, where corporate root CAs actually live) engages
        best-effort in every mode — the field lesson: it is the clean
        fix for corporate TLS interception, strictly better than
        disabling verification."""
        load_dotenv()
        project = resolve_vertex_project()
        if not project:
            raise AuthError(
                "no Vertex project configured: set VERTEX_PROJECT_ID "
                "(or LUMI_VERTEX_PROJECT / GOOGLE_CLOUD_PROJECT), "
                "e.g. in .env. This is a DIFFERENT project than the "
                "BQ one and is never borrowed from a BQ_* variable")
        key = resolve_vertex_key_path()
        if key is None:
            raise AuthError(
                "no Vertex SA key configured: set LUMI_VERTEX_SA_KEY "
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
        # the Vertex plane rides the proxy by default (the proven
        # contract) — pinned on the connection, see vertex_proxies
        verify, bundle = resolve_vertex_ssl()
        return cls(project=project, location=location,
                   model=resolve_vertex_model(), endpoint=endpoint,
                   key_path=key, ssl_verify=verify, ca_bundle=bundle,
                   truststore_active=truststore_active,
                   proxies=vertex_proxies())

    def ssl_context(self) -> ssl.SSLContext:
        if not self.ssl_verify:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            return context
        return ssl.create_default_context(cafile=self.ca_bundle)

    def route(self) -> str:
        return describe_route(self.proxies)

    def opener(self) -> urllib.request.OpenerDirector:
        """urllib opener with this plane's route and TLS pinned; the
        environment's NO_PROXY is never consulted, so a BigQuery
        connection made earlier in the process cannot reroute the
        model calls."""
        return plane_opener(self.proxies, self.ssl_context())

    def token_session(self):
        import requests                          # ships with google-auth
        session = requests.Session()
        session.trust_env = False        # the route is the connection's
        session.proxies = dict(self.proxies)
        session.verify = (self.ca_bundle or True) if self.ssl_verify \
            else False
        return session
