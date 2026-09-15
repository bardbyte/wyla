"""The catalog plane: Knowledge Catalog search on its own service account.

A separate contract from the model planes on purpose: the key that may
search the catalog is not the key that may call the model, and neither
is borrowed from the other. One POST in the sample's shape
(``semanticSearch`` on, fifty rows), the sample's row shape, and the
retry ladder the model planes use for transient refusals. The search
is always ``locations/global``: that is the API's rule, not a setting.
"""

from __future__ import annotations

import functools
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .env import (ConfigError, describe_route, env_flag, env_int, first_env,
                  google_proxies)
from .transport import (Http, ServiceAccountToken, TransportError,
                        error_reason, error_text, http_call,
                        inject_truststore, json_body, plane_opener,
                        resolve_tls, ssl_context, token_session)

DEFAULT_ENDPOINT = "https://dataplex.googleapis.com"
DEFAULT_PAGE_SIZE = 50
NO_HEADER = "none"                  # KC_QUOTA_PROJECT=none: no quota header
RETRY_STATUSES = {429, 500, 502, 503, 504}
BACKOFFS = (2, 4, 8, 16)
CALL_TIMEOUT = 60.0


class CatalogError(RuntimeError):
    """The catalog refused. ``status`` is the HTTP status (0 when no
    answer came), ``reason`` the machine reason when Google gave one,
    ``message`` the server's own words."""

    def __init__(self, status: int, reason: str, message: str) -> None:
        super().__init__(f"the catalog refused (HTTP {status}): {message}"
                         if status else message)
        self.status = status
        self.reason = reason
        self.message = message


@dataclass(frozen=True)
class CatalogConnection:
    """Everything a search needs to reach the catalog."""

    project: str
    key_path: Path | None
    endpoint: str = DEFAULT_ENDPOINT
    quota_project: str = ""          # "" sends no X-Goog-User-Project
    search_scope: str = ""           # "" lets the API pick the org scope
    page_size: int = DEFAULT_PAGE_SIZE
    semantic: bool = True
    # THIS connection's route: {} is direct, a mapping is that proxy.
    # Pinned here and in opener(); the environment's NO_PROXY is never read
    proxies: dict[str, str] = field(default_factory=dict)
    ssl_verify: bool = True
    ca_bundle: str | None = None
    truststore_active: bool = False

    @classmethod
    def from_env(cls) -> "CatalogConnection":
        """Validate → endpoint → route → TLS. Fails fast with a typed
        error that names the variable. Reads the environment only; the
        scripts load the ``.env`` files before calling this."""
        project = first_env("KC_PROJECT_ID")
        if not project:
            raise ConfigError(
                "no catalog project configured: set KC_PROJECT_ID (the "
                "consumer project the search runs in), e.g. in .env")
        key = first_env("KC_SA_KEY")
        if not key:
            raise ConfigError(
                "no catalog key configured: set KC_SA_KEY to a "
                "service-account key file with dataplex.projects.search "
                "and serviceusage.services.use. The catalog never borrows "
                "the model plane's key or GOOGLE_APPLICATION_CREDENTIALS")
        key_path = Path(key).expanduser()
        if not key_path.is_file():
            raise ConfigError(
                f"catalog key not found on disk: {key_path} (KC_SA_KEY)")
        quota = first_env("KC_QUOTA_PROJECT") or project
        if quota.lower() == NO_HEADER:
            quota = ""
        active = inject_truststore()
        verify, bundle = resolve_tls("KC_CA_BUNDLE",
                                     insecure_var="KC_TLS_INSECURE")
        return cls(
            project=project, key_path=key_path,
            endpoint=(first_env("KC_API_BASE_URL")
                      or DEFAULT_ENDPOINT).rstrip("/"),
            quota_project=quota,
            search_scope=first_env("KC_SEARCH_SCOPE") or "",
            page_size=max(1, env_int("KC_PAGE_SIZE", DEFAULT_PAGE_SIZE)),
            semantic=env_flag("KC_SEMANTIC_SEARCH", True),
            proxies=google_proxies("KC_DISABLE_PROXY"),
            ssl_verify=verify, ca_bundle=bundle, truststore_active=active)

    @property
    def parent(self) -> str:
        return f"projects/{self.project}/locations/global"

    def url(self) -> str:
        return f"{self.endpoint}/v1/{self.parent}:searchEntries"

    def ssl_context(self):
        return ssl_context(self.ssl_verify, self.ca_bundle)

    def opener(self):
        return plane_opener(self.proxies, self.ssl_context())

    def route(self) -> str:
        return describe_route(self.proxies)

    def token_session(self):
        return token_session(self.proxies,
                             (self.ca_bundle or True) if self.ssl_verify
                             else False)

    def describe(self) -> dict[str, Any]:
        """The resolved configuration, never a secret."""
        return {"project": self.project, "parent": self.parent,
                "quota_project": self.quota_project or "(no header)",
                "search_scope": self.search_scope or "(the API default: "
                                                     "the project's org)",
                "endpoint": self.endpoint, "page_size": self.page_size,
                "semantic_search": self.semantic,
                "key_file": str(self.key_path),
                "key_exists": bool(self.key_path and self.key_path.is_file()),
                "route": self.route(),
                "tls": ("verification DISABLED (KC_TLS_INSECURE)"
                        if not self.ssl_verify else
                        f"bundle {self.ca_bundle}" if self.ca_bundle else
                        "system default")
                       + (" + truststore (OS keychain)"
                          if self.truststore_active else "")}


def shape_row(result: dict[str, Any]) -> dict[str, str]:
    """One search result → the sample's four keys, plus the fields the
    ranking step reads. Absent fields are empty strings, never missing."""
    entry = result.get("dataplexEntry") or {}
    source = entry.get("entrySource") or {}
    return {
        "entry_name": str(entry.get("name") or ""),
        "system": str(source.get("system") or ""),
        "resource_id": str(source.get("resource") or ""),
        "display_name": str(source.get("displayName") or ""),
        "description": str(source.get("description") or ""),
        "entry_type": str(entry.get("entryType") or ""),
        "fully_qualified_name": str(entry.get("fullyQualifiedName") or ""),
    }


def shape_response(payload: dict[str, Any]) -> dict[str, Any]:
    return {"results": [shape_row(r) for r in payload.get("results") or []
                        if isinstance(r, dict)],
            "total_size": int(payload.get("totalSize") or 0),
            "next_page_token": str(payload.get("nextPageToken") or ""),
            "unreachable": [str(u) for u in payload.get("unreachable") or []]}


@dataclass
class CatalogClient:
    """``searchEntries`` over urllib with the service-account token.
    ``http`` and ``token`` are injectable so tests never touch a
    network."""

    connection: CatalogConnection
    http: Http | None = None
    token: Callable[[], str] | None = None
    sleep: Callable[[float], None] = time.sleep
    log: Callable[[str], None] | None = None
    usage: dict[str, int] = field(default_factory=lambda: {
        "calls": 0, "rows": 0})

    def __post_init__(self) -> None:
        if self.http is None:
            self.http = functools.partial(http_call, self.connection.opener())
        if self.token is None:
            self.token = ServiceAccountToken(self.connection.key_path,
                                             self.connection.token_session)

    def _note(self, message: str) -> None:
        if self.log is not None:
            self.log(f"    [catalog] {message}")

    def search(self, query: str, *, page_size: int | None = None,
               semantic: bool | None = None, page_token: str = "",
               scope: str | None = None, order_by: str = "") -> dict[str, Any]:
        """One search → ``{"results", "total_size", "next_page_token",
        "unreachable"}``. Transient refusals (429, 5xx) and transport
        failures ride the backoff ladder; any other refusal is a typed
        ``CatalogError`` with the server's message and reason."""
        c = self.connection
        body: dict[str, Any] = {
            "query": str(query),
            "pageSize": int(page_size or c.page_size),
            "semanticSearch": c.semantic if semantic is None
            else bool(semantic)}
        if page_token:
            body["pageToken"] = page_token
        scope = c.search_scope if scope is None else scope
        if scope:
            body["scope"] = scope
        if order_by:
            body["orderBy"] = order_by
        data = json.dumps(body).encode("utf-8")
        headers = {"Content-Type": "application/json",
                   "Accept": "application/json"}
        if c.quota_project:
            headers["X-Goog-User-Project"] = c.quota_project

        last = "no attempt made"
        last_status, last_reason = 0, "UNREACHABLE"
        for attempt, backoff in enumerate(BACKOFFS + (None,)):
            # a token failure is immediate by design: it never rides the
            # ladder, because a dead token endpoint is a dead network
            token = self.token()
            try:
                status, answer_headers, raw = self.http(
                    "POST", c.url(),
                    {**headers, "Authorization": f"Bearer {token}"},
                    data, timeout=CALL_TIMEOUT)
            except TransportError as e:
                last = str(e)
                if backoff is None:
                    break
                self._note(f"{last} — retrying in {backoff}s (attempt "
                           f"{attempt + 2}/{len(BACKOFFS) + 1})")
                self.sleep(backoff)
                continue
            if status == 200:
                payload = json_body(raw)
                if not isinstance(payload, dict):
                    raise CatalogError(status, "", "the catalog answered "
                                       "200 with a body that is not JSON")
                shaped = shape_response(payload)
                self.usage["calls"] += 1
                self.usage["rows"] += len(shaped["results"])
                return shaped
            message = error_text(status, raw, answer_headers)
            if status in RETRY_STATUSES:
                last, last_status = message, status
                last_reason = error_reason(raw) or "UNAVAILABLE"
                if backoff is None:
                    break
                self._note(f"{message} — retrying in {backoff}s (attempt "
                           f"{attempt + 2}/{len(BACKOFFS) + 1})")
                self.sleep(backoff)
                continue
            raise CatalogError(status, error_reason(raw), message)
        raise CatalogError(
            last_status, last_reason,
            f"the catalog did not answer after {len(BACKOFFS) + 1} "
            f"attempts — last: {last}")
