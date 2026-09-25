"""Pinned network routing for the Google-facing planes.

The process environment is read, never rewritten. Each caller pins the
returned proxy mapping onto its own HTTP client/opener so BigQuery,
Vertex, and gateway traffic cannot silently reroute each other through
NO_PROXY side effects.
"""

from __future__ import annotations

import os
from urllib.parse import urlparse

from sahs.constants import (OAUTH_TOKEN_ENDPOINTS_BY_ENV,
                            SPANNER_ENDPOINTS_BY_ENV,
                            VERTEX_ENDPOINTS_BY_ENV)

EPAAS_ENV_KEYS = ("EPAAS_ENV", "SAHS_ENV", "SAHS_ENVIRONMENT", "APP_ENV",
                  "ENVIRONMENT")


def _env_value(env: dict[str, str] | None, *names: str) -> str:
    values = os.environ if env is None else env
    for name in names:
        value = values.get(name)
        if value and value.strip():
            return str(value).strip()
    return ""


def epaas_env(env: dict[str, str] | None = None) -> str:
    """Deployment environment name, with EPAAS_ENV as the source of truth."""
    return _env_value(env, *EPAAS_ENV_KEYS).lower()


def vertex_endpoint_for_env(env: dict[str, str] | None = None) -> str:
    """The enterprise Vertex endpoint selected by EPAAS_ENV/e1-e3."""
    return VERTEX_ENDPOINTS_BY_ENV.get(epaas_env(env), "")


def oauth_token_endpoint_for_env(env: dict[str, str] | None = None) -> str:
    """The enterprise OAuth token endpoint selected by EPAAS_ENV/e1-e3."""
    return OAUTH_TOKEN_ENDPOINTS_BY_ENV.get(epaas_env(env), "")


def spanner_endpoint_for_env(env: dict[str, str] | None = None) -> str:
    """The enterprise Spanner PSC endpoint selected by EPAAS_ENV/e1-e3."""
    return SPANNER_ENDPOINTS_BY_ENV.get(epaas_env(env), "")


def env_proxies(env: dict[str, str] | None = None) -> dict[str, str]:
    """The corporate proxy as the environment declares it."""
    out: dict[str, str] = {}
    https = _env_value(env, "HTTPS_PROXY", "https_proxy")
    http = _env_value(env, "HTTP_PROXY", "http_proxy")
    if https:
        out["https"] = https
    if http:
        out["http"] = http
    return out


def bq_proxies(env: dict[str, str] | None = None) -> dict[str, str]:
    """BigQuery route: direct by default, proxy only by explicit opt-in."""
    if _env_value(env, "BQ_DISABLE_PROXY") == "1":
        return {}
    if _env_value(env, "BQ_FORCE_PROXY") == "1":
        return env_proxies(env)
    return {}


def spanner_proxies(env: dict[str, str] | None = None) -> dict[str, str]:
    """Spanner route: direct unless the deployment explicitly forces proxying."""
    if _env_value(env, "SPANNER_DISABLE_PROXY") == "1":
        return {}
    if _env_value(env, "SPANNER_FORCE_PROXY") == "1":
        return env_proxies(env)
    return {}


def apply_grpc_proxy(proxies: dict[str, str]) -> None:
    """Export the pinned proxy so grpc's own HTTP-CONNECT support picks it
    up — grpc channels read http_proxy/https_proxy/grpc_proxy straight from
    the process environment, never from an application-level dict."""
    proxy = proxies.get("https") or proxies.get("http")
    if not proxy:
        return
    os.environ["grpc_proxy"] = proxy
    os.environ.setdefault("https_proxy", proxy)
    os.environ.setdefault("HTTPS_PROXY", proxy)


def vertex_proxies(endpoint: str = "",
                   env: dict[str, str] | None = None) -> dict[str, str]:
    """Vertex route: proxy by default unless a direct topology is selected."""
    if _env_value(env, "VERTEX_DISABLE_PROXY") == "1" \
            or _env_value(env, "VERTEX_NO_PROXY_GOOGLE") == "1":
        return {}
    return env_proxies(env)


def redact_url(url: str) -> str:
    """Strip embedded credentials from a URL for display."""
    url = (url or "").strip()
    if "@" not in url:
        return url
    scheme, sep, rest = url.partition("://")
    host = rest.rsplit("@", 1)[-1] if sep else url.rsplit("@", 1)[-1]
    return f"{scheme}://{host}" if sep else host


def describe_route(proxies: dict[str, str]) -> str:
    """'direct' or 'via <proxy>' with credentials redacted."""
    proxy = proxies.get("https") or proxies.get("http") or ""
    return f"via {redact_url(proxy)}" if proxy else "direct"


def configure_network(endpoint: str,
                      env: dict[str, str] | None = None) -> dict[str, str]:
    """BigQuery route summary for display."""
    if _env_value(env, "BQ_DISABLE_PROXY") == "1":
        return {"proxy": "disabled (BQ_DISABLE_PROXY=1): direct"}
    if _env_value(env, "BQ_FORCE_PROXY") == "1":
        proxy = env_proxies(env).get("https", "")
        return {"proxy": "forced through proxy "
                         + (redact_url(proxy) if proxy
                            else "(none configured: direct)")
                         + " (BQ_FORCE_PROXY=1)"}
    host = urlparse(endpoint).hostname or ""
    hosts = sorted(h for h in {host, "oauth2.googleapis.com"} if h)
    return {"proxy": f"direct for: {', '.join(hosts)} (pinned on the "
                     "connection; NO_PROXY untouched)"}


def configure_vertex_network(endpoint: str,
                             env: dict[str, str] | None = None
                             ) -> dict[str, str]:
    """Vertex route summary for display."""
    if _env_value(env, "VERTEX_DISABLE_PROXY") == "1":
        return {"proxy": "disabled (VERTEX_DISABLE_PROXY=1): direct"}
    if _env_value(env, "VERTEX_NO_PROXY_GOOGLE") == "1":
        return {"proxy": "direct for Google hosts "
                         "(VERTEX_NO_PROXY_GOOGLE=1)"}
    proxy = env_proxies(env).get("https", "")
    return {"proxy": (f"via corporate proxy {redact_url(proxy)} "
                      "(the proven contract; pinned on the connection, "
                      "NO_PROXY never consulted)" if proxy
                      else "no proxy configured: direct")}
