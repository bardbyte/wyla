"""Per-environment endpoints for the Google-facing planes.

The private endpoints are not literals in this module: the three
maps are filled from the environment, one variable per plane and
environment:

    SAHS_VERTEX_ENDPOINT_E1 / _E2 / _E3
    SAHS_OAUTH_TOKEN_ENDPOINT_E1 / _E2 / _E3
    SAHS_SPANNER_ENDPOINT_E1 / _E2 / _E3

An unset variable leaves that environment out of the map, which is what
``sahs.util.network`` treats as "no pinned endpoint": the public Google
endpoint applies. Keys are the lower-case environment names ``e1``,
``e2``, ``e3`` that ``epaas_env()`` returns.
"""

from __future__ import annotations

import os

ENVIRONMENTS = ("e1", "e2", "e3")


def _endpoints(prefix: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for env in ENVIRONMENTS:
        value = os.environ.get(f"{prefix}_{env.upper()}", "").strip()
        if value:
            out[env] = value
    return out


VERTEX_ENDPOINTS_BY_ENV: dict[str, str] = _endpoints("SAHS_VERTEX_ENDPOINT")
OAUTH_TOKEN_ENDPOINTS_BY_ENV: dict[str, str] = _endpoints("SAHS_OAUTH_TOKEN_ENDPOINT")
SPANNER_ENDPOINTS_BY_ENV: dict[str, str] = _endpoints("SAHS_SPANNER_ENDPOINT")

__all__ = ["ENVIRONMENTS", "OAUTH_TOKEN_ENDPOINTS_BY_ENV",
           "SPANNER_ENDPOINTS_BY_ENV", "VERTEX_ENDPOINTS_BY_ENV"]
