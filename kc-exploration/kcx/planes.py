"""Which door: the plane a question rides, read from the environment.

Two planes, whether or not this machine can ride them: Vertex (Gemini
3.1 Pro Preview on a service-account key, streamed) and the gateway
(Gemini 2.5 Pro behind an identity-service token, whole calls). Read
from the environment each time, never cached: the ``.env`` is the
switchboard.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable

from .env import ConfigError
from .gateway import GatewayConfig, GatewayModel
from .vertex import DEFAULT_MODEL as VERTEX_DEFAULT_MODEL
from .vertex import VertexConnection, VertexModel

PLANE_VAR = "KC_MODEL_PLANE"           # vertex | gateway | auto
PLANE_IDS = ("vertex", "gateway")


class ModelUnavailable(ConfigError):
    """This machine cannot ride that plane: its contract is missing, or
    the name is not a plane."""


def vertex_configured(env: dict[str, str] | None = None) -> bool:
    """The Vertex contract is present: a key file that exists and a
    project."""
    env = dict(os.environ if env is None else env)
    key = (env.get("VERTEX_SA_KEY") or env.get("SYNAPSE_VERTEX_SA_KEY")
           or env.get("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    project = (env.get("VERTEX_PROJECT_ID")
               or env.get("GOOGLE_CLOUD_PROJECT") or "").strip()
    return bool(key and project and Path(key).expanduser().is_file())


def gateway_reason(env: dict[str, str] | None = None) -> str:
    """Why the gateway plane cannot start, or "" when it can."""
    try:
        GatewayConfig.from_env(env).validate()
    except ConfigError as e:
        return str(e)
    return ""


def gateway_configured(env: dict[str, str] | None = None) -> bool:
    return not gateway_reason(env)


def model_plane(env: dict[str, str] | None = None) -> str:
    """The plane a question rides by default. KC_MODEL_PLANE names one
    (SAHS_MODEL_PLANE is read as an alias, so a shared ``.env`` keeps its
    meaning); ``auto`` lands on Vertex whenever its contract is present
    and on the gateway only when the gateway alone is configured."""
    env = dict(os.environ if env is None else env)
    wanted = (env.get(PLANE_VAR) or env.get("SAHS_MODEL_PLANE")
              or "auto").strip().lower()
    if wanted in PLANE_IDS:
        return wanted
    if gateway_configured(env) and not vertex_configured(env):
        return "gateway"
    return "vertex"


def plane_note(env: dict[str, str] | None = None) -> str:
    """Why the plane is what it is, for the check scripts."""
    env = dict(os.environ if env is None else env)
    wanted = (env.get(PLANE_VAR) or env.get("SAHS_MODEL_PLANE")
              or "auto").strip().lower()
    if wanted in PLANE_IDS:
        return f"{PLANE_VAR}={wanted}"
    if vertex_configured(env):
        return f"{PLANE_VAR} unset: the Vertex contract is present"
    if gateway_configured(env):
        return (f"{PLANE_VAR} unset: the gateway is configured and no "
                "Vertex key is")
    return f"{PLANE_VAR} unset: no Vertex key and no gateway contract"


def pretty_model(raw: str) -> str:
    """gemini-2.5-pro → Gemini 2.5 Pro; gemini-3.1-pro-preview → Gemini
    3.1 Pro Preview."""
    return " ".join(w.capitalize() if w.isalpha() else w
                    for w in (raw or "").replace("_", "-").split("-")
                    if w)


def plane_catalog(env: dict[str, str] | None = None) -> list[dict[str, Any]]:
    """Both planes: id, label, model, availability with the reason when
    not, and which one a question starts on."""
    env = dict(os.environ if env is None else env)
    default = model_plane(env)
    vertex_model = (env.get("VERTEX_MODEL") or env.get("GEMINI_MODEL")
                    or VERTEX_DEFAULT_MODEL).strip()
    vertex_ok = vertex_configured(env)
    gateway_cfg = GatewayConfig.from_env(env)
    gateway_why = gateway_reason(env)
    return [
        {"id": "vertex",
         "label": pretty_model(vertex_model),
         "plane_name": "Vertex",
         "model": vertex_model,
         "available": vertex_ok,
         "reason": "" if vertex_ok else
         "needs the Vertex contract in .env: VERTEX_SA_KEY (a key file "
         "that exists) and VERTEX_PROJECT_ID",
         "means": "Google Cloud's Vertex AI with the service-account key. "
                  "Thinking streams as it happens and the answer arrives "
                  "word by word.",
         "feel": "streams",
         "default": default == "vertex"},
        {"id": "gateway",
         "label": pretty_model(gateway_cfg.model),
         "plane_name": "Gateway",
         "model": gateway_cfg.model,
         "available": not gateway_why,
         "reason": gateway_why,
         "means": "An enterprise gateway with an identity-service token "
                  "that renews itself. Each call lands whole: a thinking "
                  "pause, then the text at once. No streaming.",
         "feel": "whole calls",
         "default": default == "gateway"},
    ]


def model_for(plane: str = "", log: Callable[[str], None] | None = None
              ) -> VertexModel | GatewayModel:
    """The model on a named plane, or on the environment's default when
    the name is empty. An unknown name is a typed error, not a silent
    fallback."""
    plane = (plane or "").strip().lower() or model_plane()
    if plane == "gateway":
        try:
            return GatewayModel.from_env(log=log)
        except ConfigError as e:
            raise ModelUnavailable(str(e)) from e
    if plane == "vertex":
        try:
            return VertexModel(VertexConnection.from_env(), log=log)
        except ConfigError as e:
            raise ModelUnavailable(
                f"{e}: the Vertex plane needs VERTEX_SA_KEY and "
                "VERTEX_PROJECT_ID (VERTEX_LOCATION and VERTEX_MODEL "
                "optional)") from e
    raise ModelUnavailable(f"no model plane called {plane!r}: the planes "
                           "are vertex and gateway")
