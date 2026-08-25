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
    Vertex URL   VERTEX_API_BASE_URL → public default
    BQ location  BQ_LOCATION → "US"

No global credential mutation happens on import; callers ask for what they
need. Missing configuration is reported as a typed error (exit code 3 —
env/auth — per the E10 console contract), never a stack trace.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_BQ_ENDPOINT = "https://bigquery-prod.p.googleapis.com"
DEFAULT_VERTEX_ENDPOINT = "https://aiplatform.googleapis.com"
DEFAULT_BQ_LOCATION = "US"


class AuthError(RuntimeError):
    """Environment/auth misconfiguration — maps to exit code 3."""


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


def resolve_vertex_endpoint() -> str:
    v = _first_env("VERTEX_API_BASE_URL")
    return (v or DEFAULT_VERTEX_ENDPOINT).rstrip("/")


def resolve_bq_location() -> str:
    return _first_env("BQ_LOCATION") or DEFAULT_BQ_LOCATION


@dataclass(frozen=True)
class BQConnection:
    """Everything the dry-run substrate / sandbox needs to reach BigQuery."""

    project: str
    endpoint: str
    location: str
    key_path: Path | None

    @classmethod
    def from_env(cls) -> "BQConnection":
        project = resolve_bq_project()
        if not project:
            raise AuthError(
                "no BigQuery project configured — set BQ_PROJECT_ID (or "
                "LUMI_BQ_PROJECT / GOOGLE_CLOUD_PROJECT)")
        key = resolve_bq_key_path()
        if key is not None and not key.exists():
            raise AuthError(f"BigQuery SA key not found: {key}")
        return cls(project=project, endpoint=resolve_bq_endpoint(),
                   location=resolve_bq_location(), key_path=key)
